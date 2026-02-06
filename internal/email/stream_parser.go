package email

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/mail"
	"strings"
)

// Limits for MIME structure to guard against "part bomb" emails.
const (
	MaxParts          = 100 // Maximum total parts (leaf + container combined)
	MaxMultipartDepth = 10  // Maximum levels of multipart nesting
)

// Sentinel errors for part bomb detection.
var (
	ErrTooManyParts  = errors.New("too many MIME parts")
	ErrNestingTooDeep = errors.New("MIME nesting too deep")
)

// BlobUploader abstracts blob uploads for dependency inversion.
type BlobUploader interface {
	Upload(ctx context.Context, accountID, parentBlobID, contentType string, body io.Reader) (blobID string, size int64, err error)
}

// ParseRFC5322Stream parses an RFC5322 message from a stream, generating blob IDs for body parts.
// For single-part identity-encoded messages (7bit, 8bit, binary), it returns byte-range blob IDs.
// For multipart messages, all leaf parts are uploaded via the BlobUploader.
// For non-identity encoded parts (base64, quoted-printable), content is decoded and uploaded.
//
// This function streams the email without buffering the entire message in memory.
// Only headers are buffered; body content flows through to blob storage or is discarded.
func ParseRFC5322Stream(
	ctx context.Context,
	r io.Reader,
	emailBlobID string,
	accountID string,
	uploader BlobUploader,
) (*ParsedEmail, error) {
	// Wrap in CountingReader to track total bytes
	cr := NewCountingReader(r)

	// Read headers only - body stays in the stream
	headerData, bodyReader, err := readHeaderBytes(cr)
	if err != nil {
		return nil, fmt.Errorf("failed to read headers: %w", err)
	}

	headerSize := int64(len(headerData))

	// Parse headers via mail.ReadMessage
	msg, err := mail.ReadMessage(bytes.NewReader(headerData))
	if err != nil {
		return nil, fmt.Errorf("failed to parse message: %w", err)
	}

	parsed := &ParsedEmail{
		Sender:      []EmailAddress{},
		To:          []EmailAddress{},
		CC:          []EmailAddress{},
		Bcc:         []EmailAddress{},
		ReplyTo:     []EmailAddress{},
		MessageID:   []string{},
		InReplyTo:   []string{},
		References:  []string{},
		TextBody:    []string{},
		HTMLBody:    []string{},
		Attachments: []string{},
	}

	parseHeaders(parsed, msg.Header)
	parsed.HeaderSize = headerSize

	// Determine content type
	contentType := msg.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "text/plain"
	}
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		mediaType = "text/plain"
		params = nil
	}

	encoding := msg.Header.Get("Content-Transfer-Encoding")
	previewCapture := NewPreviewCapture(256)
	partCounter := 0

	if strings.HasPrefix(mediaType, "multipart/") {
		boundary, ok := params["boundary"]
		if !ok {
			return nil, fmt.Errorf("multipart message missing boundary parameter")
		}

		partCounter++
		parsed.BodyStructure = BodyPart{
			PartID: fmt.Sprintf("%d", partCounter),
			Type:   mediaType,
		}

		subParts, err := parseMultipartStreaming(
			ctx, bodyReader, boundary,
			emailBlobID, accountID, uploader, &partCounter, previewCapture, 1,
		)
		if err != nil {
			return nil, err
		}
		parsed.BodyStructure.SubParts = subParts
	} else {
		// Single-part message
		partCounter++
		part, err := parseSinglePartStreaming(
			ctx, bodyReader, headerSize, cr,
			mediaType, encoding, emailBlobID, accountID, uploader, previewCapture,
		)
		if err != nil {
			return nil, err
		}
		part.PartID = fmt.Sprintf("%d", partCounter)
		part.Type = mediaType
		if charset, ok := params["charset"]; ok {
			part.Charset = charset
		}
		parsed.BodyStructure = part
	}

	// Drain any remaining bytes to get accurate total size
	_, _ = io.Copy(io.Discard, bodyReader)
	parsed.Size = cr.BytesRead()
	parsed.Preview = previewCapture.Preview()

	// Collect text/html body parts and attachments
	collectParts(parsed, &parsed.BodyStructure)

	return parsed, nil
}

// parseHeaders extracts header fields into ParsedEmail.
func parseHeaders(parsed *ParsedEmail, header mail.Header) {
	parsed.Subject = decodeHeader(header.Get("Subject"))
	parsed.From = parseAddressList(header.Get("From"))
	parsed.Sender = parseAddressList(header.Get("Sender"))
	parsed.To = parseAddressList(header.Get("To"))
	parsed.CC = parseAddressList(header.Get("Cc"))
	parsed.Bcc = parseAddressList(header.Get("Bcc"))
	parsed.ReplyTo = parseAddressList(header.Get("Reply-To"))

	if dateStr := header.Get("Date"); dateStr != "" {
		if t, err := mail.ParseDate(dateStr); err == nil {
			parsed.SentAt = t.UTC()
		}
	}

	if msgID := header.Get("Message-Id"); msgID != "" {
		parsed.MessageID = []string{msgID}
	}

	if inReplyTo := header.Get("In-Reply-To"); inReplyTo != "" {
		parsed.InReplyTo = parseMessageIDList(inReplyTo)
	}

	if refs := header.Get("References"); refs != "" {
		parsed.References = parseMessageIDList(refs)
	}
}

// isIdentityEncoding returns true for encodings that don't transform the content.
// Per RFC 8621, unknown encodings should be treated as identity.
func isIdentityEncoding(encoding string) bool {
	enc := strings.ToLower(strings.TrimSpace(encoding))
	switch enc {
	case "7bit", "8bit", "binary", "":
		return true
	case "base64", "quoted-printable":
		return false
	default:
		// Unknown encoding treated as identity per RFC 8621
		return true
	}
}

// getDecoder returns a decoding reader for the given encoding.
func getDecoder(encoding string, r io.Reader) io.Reader {
	enc := strings.ToLower(strings.TrimSpace(encoding))
	switch enc {
	case "base64":
		return base64.NewDecoder(base64.StdEncoding, r)
	case "quoted-printable":
		return quotedprintable.NewReader(r)
	default:
		return r
	}
}

// readHeaderBytes reads from r until it finds the header/body separator (CRLFCRLF or LFLF),
// returning the raw header bytes (including the separator) and a reader for the remaining body.
// The returned body reader yields any excess bytes read past the separator, followed by the
// remaining unread data from r.
func readHeaderBytes(r io.Reader) ([]byte, io.Reader, error) {
	const chunkSize = 4096
	var headerBuf []byte
	readBuf := make([]byte, chunkSize)

	for {
		n, err := r.Read(readBuf)
		if n > 0 {
			headerBuf = append(headerBuf, readBuf[:n]...)

			// Scan for separator in the accumulated buffer.
			// We need to check a window that spans the previous chunk boundary,
			// so scan from max(0, len(headerBuf)-n-3) to cover overlap.
			scanStart := len(headerBuf) - n - 3
			if scanStart < 0 {
				scanStart = 0
			}

			for i := scanStart; i < len(headerBuf)-1; i++ {
				// Check for \n\n (LF LF)
				if headerBuf[i] == '\n' && headerBuf[i+1] == '\n' {
					sepEnd := i + 2
					excess := headerBuf[sepEnd:]
					bodyReader := io.MultiReader(bytes.NewReader(excess), r)
					return headerBuf[:sepEnd], bodyReader, nil
				}
				// Check for \r\n\r\n (CRLF CRLF)
				if i+3 < len(headerBuf) &&
					headerBuf[i] == '\r' && headerBuf[i+1] == '\n' &&
					headerBuf[i+2] == '\r' && headerBuf[i+3] == '\n' {
					sepEnd := i + 4
					excess := headerBuf[sepEnd:]
					bodyReader := io.MultiReader(bytes.NewReader(excess), r)
					return headerBuf[:sepEnd], bodyReader, nil
				}
			}
		}
		if err == io.EOF {
			// No separator found; treat entire input as headers
			return headerBuf, bytes.NewReader(nil), nil
		}
		if err != nil {
			return nil, nil, fmt.Errorf("reading headers: %w", err)
		}
	}
}

// parseSinglePartStreaming processes a single (non-multipart) body part from a stream.
// For identity-encoded parts, it uses byte-range blob IDs.
// For non-identity encoded parts, it decodes and uploads via uploader.
// Text/plain content is teed to previewCapture for preview generation.
func parseSinglePartStreaming(
	ctx context.Context,
	bodyReader io.Reader,
	headerSize int64,
	cr *CountingReader,
	mediaType string,
	encoding string,
	emailBlobID string,
	accountID string,
	uploader BlobUploader,
	previewCapture *PreviewCapture,
) (BodyPart, error) {
	var part BodyPart
	isText := strings.HasPrefix(mediaType, "text/plain")

	if isIdentityEncoding(encoding) {
		// Identity encoding: drain body to count bytes, optionally capturing preview
		startByte := headerSize
		var w io.Writer = io.Discard
		if isText && !previewCapture.Full() {
			w = io.MultiWriter(io.Discard, previewCapture)
		}
		n, err := io.Copy(w, bodyReader)
		if err != nil {
			return part, fmt.Errorf("draining identity body: %w", err)
		}
		endByte := startByte + n
		part.BlobID = fmt.Sprintf("%s,%d,%d", emailBlobID, startByte, endByte)
		part.Size = n
	} else {
		// Non-identity encoding: decode and upload
		decodedReader := getDecoder(encoding, bodyReader)

		var uploadReader io.Reader = decodedReader
		if isText && !previewCapture.Full() {
			uploadReader = io.TeeReader(decodedReader, previewCapture)
		}

		blobID, size, err := uploader.Upload(ctx, accountID, emailBlobID, mediaType, uploadReader)
		if err != nil {
			return part, fmt.Errorf("uploading decoded part: %w", err)
		}
		part.BlobID = blobID
		part.Size = size
	}

	return part, nil
}

// parseMultipartStreaming parses a multipart body from a stream using mime/multipart.Reader.
// All leaf parts are uploaded via uploader. First text/plain part is teed to previewCapture.
// depth tracks the current multipart nesting level (1 = top-level multipart).
func parseMultipartStreaming(
	ctx context.Context,
	bodyReader io.Reader,
	boundary string,
	emailBlobID string,
	accountID string,
	uploader BlobUploader,
	counter *int,
	previewCapture *PreviewCapture,
	depth int,
) ([]BodyPart, error) {
	if depth > MaxMultipartDepth {
		return nil, ErrNestingTooDeep
	}

	mr := multipart.NewReader(bodyReader, boundary)
	var subParts []BodyPart

	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return subParts, fmt.Errorf("reading multipart: %w", err)
		}

		*counter++
		if *counter > MaxParts {
			return nil, ErrTooManyParts
		}
		partID := fmt.Sprintf("%d", *counter)

		contentType := p.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "text/plain"
		}
		partMediaType, partParams, err := mime.ParseMediaType(contentType)
		if err != nil {
			partMediaType = "text/plain"
			partParams = nil
		}

		subPart := BodyPart{
			PartID: partID,
			Type:   partMediaType,
		}
		if charset, ok := partParams["charset"]; ok {
			subPart.Charset = charset
		}

		// Check for disposition
		disposition := p.Header.Get("Content-Disposition")
		if disposition != "" {
			dispType, dispParams, _ := mime.ParseMediaType(disposition)
			subPart.Disposition = dispType
			if filename, ok := dispParams["filename"]; ok {
				subPart.Name = filename
			}
		}
		if subPart.Name == "" {
			if name, ok := partParams["name"]; ok {
				subPart.Name = name
			}
		}

		if strings.HasPrefix(partMediaType, "multipart/") {
			innerBoundary, ok := partParams["boundary"]
			if ok {
				nested, err := parseMultipartStreaming(
					ctx, p, innerBoundary,
					emailBlobID, accountID, uploader, counter, previewCapture, depth+1,
				)
				if err != nil {
					return nil, err
				}
				subPart.SubParts = nested
			}
		} else {
			partEncoding := p.Header.Get("Content-Transfer-Encoding")
			leaf, err := processLeafPartStreaming(
				ctx, p, partMediaType, partEncoding,
				emailBlobID, accountID, uploader, previewCapture,
			)
			if err != nil {
				return nil, err
			}
			subPart.BlobID = leaf.BlobID
			subPart.Size = leaf.Size
		}

		subParts = append(subParts, subPart)
	}

	return subParts, nil
}

// processLeafPartStreaming processes a single leaf part within a multipart message.
// ALL leaf parts in multipart messages are uploaded via uploader (both identity and
// non-identity encoded), since mime/multipart.Reader's internal buffering makes
// exact byte-offset tracking impossible for sub-parts.
//
// This is the right tradeoff: multipart emails benefit most from streaming, and
// the BlobUploader interface provides the injection point for future optimizations
// like signed-URL uploads — no parser changes needed.
func processLeafPartStreaming(
	ctx context.Context,
	partReader io.Reader,
	mediaType string,
	encoding string,
	emailBlobID string,
	accountID string,
	uploader BlobUploader,
	previewCapture *PreviewCapture,
) (BodyPart, error) {
	var part BodyPart
	isText := strings.HasPrefix(mediaType, "text/plain")

	// Decode if non-identity encoding
	var contentReader io.Reader = partReader
	if !isIdentityEncoding(encoding) {
		contentReader = getDecoder(encoding, partReader)
	}

	// Tee text/plain to preview capture
	var uploadReader io.Reader = contentReader
	if isText && !previewCapture.Full() {
		uploadReader = io.TeeReader(contentReader, previewCapture)
	}

	blobID, size, err := uploader.Upload(ctx, accountID, emailBlobID, mediaType, uploadReader)
	if err != nil {
		return part, fmt.Errorf("uploading multipart leaf: %w", err)
	}
	part.BlobID = blobID
	part.Size = size

	return part, nil
}
