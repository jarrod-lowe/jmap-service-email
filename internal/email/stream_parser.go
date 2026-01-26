package email

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"mime"
	"mime/quotedprintable"
	"net/mail"
	"net/textproto"
	"strings"
)

// BlobUploader abstracts blob uploads for dependency inversion.
type BlobUploader interface {
	Upload(ctx context.Context, accountID, parentBlobID, contentType string, body io.Reader) (blobID string, size int64, err error)
}

// ParseRFC5322Stream parses an RFC5322 message from a reader, generating blob IDs for body parts.
// For identity-encoded parts (7bit, 8bit, binary), it returns byte-range blob IDs.
// For non-identity encoded parts (base64, quoted-printable), it decodes and uploads the content.
func ParseRFC5322Stream(
	ctx context.Context,
	r io.Reader,
	emailBlobID string,
	accountID string,
	uploader BlobUploader,
) (*ParsedEmail, error) {
	// Read the full email to enable byte offset tracking
	// TODO: For very large emails, consider chunked processing
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read email: %w", err)
	}

	msg, err := mail.ReadMessage(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to parse message: %w", err)
	}

	parsed := &ParsedEmail{
		Size:        int64(len(data)),
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

	// Parse headers
	parseHeaders(parsed, msg.Header)

	// Find body start position (after headers + blank line)
	bodyStart := findBodyStart(data)

	// Store header size for later retrieval (e.g., header:* properties)
	parsed.HeaderSize = int64(bodyStart)

	// Parse body structure
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

	partCounter := 0
	parsed.BodyStructure, err = parsePartStream(
		ctx,
		data[bodyStart:],
		int64(bodyStart),
		mediaType,
		params,
		encoding,
		emailBlobID,
		accountID,
		uploader,
		&partCounter,
	)
	if err != nil {
		return nil, err
	}

	// Collect text/html body parts and attachments
	collectParts(parsed, &parsed.BodyStructure)

	// Generate preview
	parsed.Preview = generatePreviewFromData(parsed, &parsed.BodyStructure, data[bodyStart:])

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

// findBodyStart finds the byte offset where the body starts (after headers + CRLF or LF).
func findBodyStart(data []byte) int {
	// Look for double newline (CRLFCRLF or LFLF)
	for i := 0; i < len(data)-1; i++ {
		if data[i] == '\n' {
			if data[i+1] == '\n' {
				return i + 2
			}
			if i+2 < len(data) && data[i+1] == '\r' && data[i+2] == '\n' {
				return i + 3
			}
		}
		if i+3 < len(data) && data[i] == '\r' && data[i+1] == '\n' && data[i+2] == '\r' && data[i+3] == '\n' {
			return i + 4
		}
	}
	return len(data)
}

// parsePartStream parses a MIME part, handling encoding and generating blob IDs.
func parsePartStream(
	ctx context.Context,
	partData []byte,
	absoluteOffset int64,
	mediaType string,
	params map[string]string,
	encoding string,
	emailBlobID string,
	accountID string,
	uploader BlobUploader,
	counter *int,
) (BodyPart, error) {
	*counter++
	partID := fmt.Sprintf("%d", *counter)

	part := BodyPart{
		PartID: partID,
		Type:   mediaType,
	}

	if charset, ok := params["charset"]; ok {
		part.Charset = charset
	}

	// Handle multipart
	if strings.HasPrefix(mediaType, "multipart/") {
		boundary, ok := params["boundary"]
		if !ok {
			return part, nil
		}

		subParts, err := parseMultipartStream(
			ctx,
			partData,
			absoluteOffset,
			boundary,
			emailBlobID,
			accountID,
			uploader,
			counter,
		)
		if err != nil {
			return part, err
		}
		part.SubParts = subParts
		// Multipart containers don't have their own blob ID
		return part, nil
	}

	// Handle leaf part
	if isIdentityEncoding(encoding) {
		// Identity encoding: use byte-range blob ID
		startByte := absoluteOffset
		endByte := absoluteOffset + int64(len(partData))
		part.BlobID = fmt.Sprintf("%s,%d,%d", emailBlobID, startByte, endByte)
		part.Size = int64(len(partData))
	} else {
		// Non-identity encoding: decode and upload
		decodedReader := getDecoder(encoding, bytes.NewReader(partData))

		blobID, size, err := uploader.Upload(ctx, accountID, emailBlobID, mediaType, decodedReader)
		if err != nil {
			return part, fmt.Errorf("failed to upload decoded part: %w", err)
		}
		part.BlobID = blobID
		part.Size = size
	}

	return part, nil
}

// parseMultipartStream parses multipart content and returns sub-parts.
func parseMultipartStream(
	ctx context.Context,
	data []byte,
	baseOffset int64,
	boundary string,
	emailBlobID string,
	accountID string,
	uploader BlobUploader,
	counter *int,
) ([]BodyPart, error) {
	var subParts []BodyPart

	// Parse the multipart content to find boundaries and part positions
	boundaryBytes := []byte("--" + boundary)

	// Find all boundary positions
	parts := findMultipartParts(data, boundaryBytes)

	for _, partInfo := range parts {
		// Parse part headers
		partReader := bytes.NewReader(partInfo.data)
		tp := textproto.NewReader(bufio.NewReader(partReader))
		header, err := tp.ReadMIMEHeader()
		if err != nil && err != io.EOF {
			continue
		}

		// Find where body starts within this part
		partBodyStart := findBodyStart(partInfo.data)
		if partBodyStart >= len(partInfo.data) {
			continue
		}

		partBody := partInfo.data[partBodyStart:]
		partAbsoluteOffset := baseOffset + int64(partInfo.offset) + int64(partBodyStart)

		// Parse content type
		contentType := header.Get("Content-Type")
		if contentType == "" {
			contentType = "text/plain"
		}
		partMediaType, partParams, err := mime.ParseMediaType(contentType)
		if err != nil {
			partMediaType = "text/plain"
			partParams = nil
		}

		partEncoding := header.Get("Content-Transfer-Encoding")

		subPart, err := parsePartStream(
			ctx,
			partBody,
			partAbsoluteOffset,
			partMediaType,
			partParams,
			partEncoding,
			emailBlobID,
			accountID,
			uploader,
			counter,
		)
		if err != nil {
			return nil, err
		}

		// Check for disposition
		disposition := header.Get("Content-Disposition")
		if disposition != "" {
			dispType, dispParams, _ := mime.ParseMediaType(disposition)
			subPart.Disposition = dispType
			if filename, ok := dispParams["filename"]; ok {
				subPart.Name = filename
			}
		}

		// Also check Content-Type name parameter
		if subPart.Name == "" {
			if name, ok := partParams["name"]; ok {
				subPart.Name = name
			}
		}

		subParts = append(subParts, subPart)
	}

	return subParts, nil
}

// partRange represents a part's data and its offset within the multipart content.
type partRange struct {
	data   []byte
	offset int
}

// findMultipartParts finds all parts between boundaries.
func findMultipartParts(data []byte, boundary []byte) []partRange {
	var parts []partRange

	// Find the first boundary
	idx := bytes.Index(data, boundary)
	if idx == -1 {
		return parts
	}

	// Skip past first boundary and its trailing CRLF/LF
	pos := idx + len(boundary)
	pos = skipLineEnding(data, pos)

	for {
		// Find the next boundary
		nextIdx := bytes.Index(data[pos:], boundary)
		if nextIdx == -1 {
			break
		}

		// Extract part data (everything before the boundary, minus trailing CRLF)
		partEnd := pos + nextIdx
		// Remove trailing line ending before boundary
		if partEnd > 0 && data[partEnd-1] == '\n' {
			partEnd--
		}
		if partEnd > 0 && data[partEnd-1] == '\r' {
			partEnd--
		}

		if partEnd > pos {
			parts = append(parts, partRange{
				data:   data[pos:partEnd],
				offset: pos,
			})
		}

		// Move past this boundary
		pos = pos + nextIdx + len(boundary)

		// Check for closing boundary (--)
		if pos+2 <= len(data) && data[pos] == '-' && data[pos+1] == '-' {
			break
		}

		pos = skipLineEnding(data, pos)
	}

	return parts
}

// skipLineEnding advances past CRLF or LF.
func skipLineEnding(data []byte, pos int) int {
	if pos < len(data) && data[pos] == '\r' {
		pos++
	}
	if pos < len(data) && data[pos] == '\n' {
		pos++
	}
	return pos
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

// generatePreviewFromData creates a preview string from raw body data.
func generatePreviewFromData(parsed *ParsedEmail, rootPart *BodyPart, bodyData []byte) string {
	var text string

	// For simple non-multipart messages, use the body directly
	if !strings.HasPrefix(rootPart.Type, "multipart/") {
		text = string(bodyData)
	}

	// Clean up whitespace
	text = strings.TrimSpace(text)
	text = strings.ReplaceAll(text, "\r\n", " ")
	text = strings.ReplaceAll(text, "\n", " ")

	// Collapse multiple spaces
	for strings.Contains(text, "  ") {
		text = strings.ReplaceAll(text, "  ", " ")
	}

	// Truncate to ~256 chars
	const maxPreview = 256
	if len(text) > maxPreview {
		text = text[:maxPreview]
		if lastSpace := strings.LastIndex(text, " "); lastSpace > maxPreview-50 {
			text = text[:lastSpace]
		}
		text += "..."
	}

	return text
}
