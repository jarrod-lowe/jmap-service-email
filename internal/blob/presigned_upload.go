package blob

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/jarrod-lowe/jmap-service-libs/tracing"
	"go.opentelemetry.io/otel/trace"
)

const multipartPartSize = 10 * 1024 * 1024 // 10MB

type partURL struct {
	PartNumber int32  `json:"partNumber"`
	URL        string `json:"url"`
}

type completedPart struct {
	PartNumber int32  `json:"partNumber"`
	ETag       string `json:"etag"`
}

// PresignedUploadClient uploads blobs using a multipart presigned URL flow:
// 1. Call Blob/allocate via JMAP to get a blobID + presigned part URLs
// 2. PUT body chunks to presigned URLs, collecting ETags
// 3. Call Blob/complete to finalize the multipart upload
type PresignedUploadClient struct {
	baseURL      string   // API Gateway base URL
	signedClient HTTPDoer // SigV4-signed client for Blob/allocate JMAP POST
	plainClient  HTTPDoer // Plain client for presigned URL PUT (no signing)
}

// NewPresignedUploadClient creates a new PresignedUploadClient.
func NewPresignedUploadClient(baseURL string, signedClient, plainClient HTTPDoer) *PresignedUploadClient {
	return &PresignedUploadClient{
		baseURL:      baseURL,
		signedClient: signedClient,
		plainClient:  plainClient,
	}
}

// jmapRequest is the structure for a JMAP request.
type jmapRequest struct {
	Using       []string `json:"using"`
	MethodCalls []any    `json:"methodCalls"`
}

// allocateCreated holds the created entry from a Blob/allocate response.
type allocateCreated struct {
	ID    string    `json:"id"`
	Parts []partURL `json:"parts"`
}

// countingReader wraps a reader and counts bytes read.
type countingReader struct {
	reader    io.Reader
	bytesRead int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.reader.Read(p)
	cr.bytesRead += int64(n)
	return n, err
}

// Upload implements BlobUploader using the presigned URL flow.
func (c *PresignedUploadClient) Upload(ctx context.Context, accountID, parentBlobID, contentType string, body io.Reader) (string, int64, error) {
	tracer := tracing.Tracer("jmap-blob-client")
	ctx, span := tracer.Start(ctx, "blob.PresignedUpload",
		trace.WithAttributes(
			tracing.AccountID(accountID),
			tracing.ParentBlobID(parentBlobID),
			tracing.ContentType(contentType),
		))
	defer span.End()

	// Step 1: Call Blob/allocate to get blobID + presigned part URLs
	blobID, parts, err := c.allocate(ctx, accountID, contentType)
	if err != nil {
		tracing.RecordError(span, err)
		return "", 0, err
	}

	// Step 2: Upload body in chunks to presigned part URLs
	cr := &countingReader{reader: body}
	completedParts, err := c.uploadParts(ctx, parts, contentType, cr)
	if err != nil {
		tracing.RecordError(span, err)
		return "", 0, err
	}

	// Step 3: Complete the multipart upload
	if err := c.complete(ctx, accountID, blobID, completedParts); err != nil {
		tracing.RecordError(span, err)
		return "", 0, err
	}

	return blobID, cr.bytesRead, nil
}

// allocate sends a Blob/allocate JMAP request and returns the blobID and presigned part URLs.
func (c *PresignedUploadClient) allocate(ctx context.Context, accountID, contentType string) (string, []partURL, error) {
	reqBody := jmapRequest{
		Using: []string{"https://jmap.rrod.net/extensions/upload-put"},
		MethodCalls: []any{
			[]any{
				"Blob/allocate",
				map[string]any{
					"accountId": accountID,
					"create": map[string]any{
						"c0": map[string]any{
							"type":      contentType,
							"size":      0,
							"multipart": true,
						},
					},
				},
				"c0",
			},
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", nil, fmt.Errorf("%w: %v", ErrInvalidArguments, err)
	}

	url := c.baseURL + "/jmap-iam/" + accountID
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", nil, fmt.Errorf("%w: %v", ErrInvalidArguments, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.signedClient.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("%w: %v", ErrServerFail, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		return "", nil, fmt.Errorf("%w: allocate returned status %d", ErrServerFail, resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		return "", nil, fmt.Errorf("%w: allocate returned status %d", ErrInvalidArguments, resp.StatusCode)
	}

	// Parse JMAP response
	var jmapResp struct {
		MethodResponses []json.RawMessage `json:"methodResponses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&jmapResp); err != nil {
		return "", nil, fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}

	if len(jmapResp.MethodResponses) == 0 {
		return "", nil, fmt.Errorf("%w: empty methodResponses", ErrInvalidResponse)
	}

	// Parse the first method response tuple [name, args, clientId]
	var tuple []json.RawMessage
	if err := json.Unmarshal(jmapResp.MethodResponses[0], &tuple); err != nil {
		return "", nil, fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}
	if len(tuple) < 2 {
		return "", nil, fmt.Errorf("%w: method response tuple too short", ErrInvalidResponse)
	}

	var methodName string
	if err := json.Unmarshal(tuple[0], &methodName); err != nil {
		return "", nil, fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}

	if methodName == "error" {
		return "", nil, fmt.Errorf("%w: Blob/allocate returned JMAP error", ErrServerFail)
	}

	// Parse the response args
	var allocateResp struct {
		Created    map[string]allocateCreated `json:"created"`
		NotCreated map[string]any             `json:"notCreated"`
	}
	if err := json.Unmarshal(tuple[1], &allocateResp); err != nil {
		return "", nil, fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}

	// Check for notCreated
	if nc, ok := allocateResp.NotCreated["c0"]; ok {
		return "", nil, fmt.Errorf("%w: Blob/allocate notCreated: %v", ErrServerFail, nc)
	}

	created, ok := allocateResp.Created["c0"]
	if !ok {
		return "", nil, fmt.Errorf("%w: no 'c0' in created", ErrInvalidResponse)
	}

	if len(created.Parts) == 0 {
		return "", nil, fmt.Errorf("%w: no parts in allocate response", ErrInvalidResponse)
	}

	return created.ID, created.Parts, nil
}

// putPart PUTs a single chunk to a presigned part URL and returns the ETag.
func (c *PresignedUploadClient) putPart(ctx context.Context, url, contentType string, data []byte) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidArguments, err)
	}
	req.Header.Set("Content-Type", contentType)

	resp, err := c.plainClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrServerFail, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("%w: part PUT returned status %d", ErrServerFail, resp.StatusCode)
	}

	etag := resp.Header.Get("ETag")
	if etag == "" {
		return "", fmt.Errorf("%w: part PUT response missing ETag header", ErrInvalidResponse)
	}

	return etag, nil
}

// uploadParts reads body in chunks and PUTs each to the corresponding presigned part URL.
func (c *PresignedUploadClient) uploadParts(ctx context.Context, parts []partURL, contentType string, body io.Reader) ([]completedPart, error) {
	buf := make([]byte, multipartPartSize)
	var completed []completedPart

	for _, part := range parts {
		n, err := io.ReadFull(body, buf)
		if err == io.EOF {
			break
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("%w: %v", ErrServerFail, err)
		}

		etag, putErr := c.putPart(ctx, part.URL, contentType, buf[:n])
		if putErr != nil {
			return nil, putErr
		}

		completed = append(completed, completedPart{
			PartNumber: part.PartNumber,
			ETag:       etag,
		})

		if err == io.ErrUnexpectedEOF {
			break
		}
	}

	// Verify body is exhausted
	extra := make([]byte, 1)
	if n, _ := body.Read(extra); n > 0 {
		return nil, fmt.Errorf("%w: body exceeds maximum upload size (%d parts)", ErrInvalidArguments, len(parts))
	}

	return completed, nil
}

// complete sends a Blob/complete JMAP request to finalize the multipart upload.
func (c *PresignedUploadClient) complete(ctx context.Context, accountID, blobID string, parts []completedPart) error {
	reqBody := jmapRequest{
		Using: []string{"https://jmap.rrod.net/extensions/upload-put"},
		MethodCalls: []any{
			[]any{
				"Blob/complete",
				map[string]any{
					"accountId": accountID,
					"id":        blobID,
					"parts":     parts,
				},
				"c0",
			},
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArguments, err)
	}

	url := c.baseURL + "/jmap-iam/" + accountID
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArguments, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.signedClient.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrServerFail, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		return fmt.Errorf("%w: complete returned status %d", ErrServerFail, resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("%w: complete returned status %d", ErrInvalidArguments, resp.StatusCode)
	}

	// Parse JMAP response to check for errors
	var jmapResp struct {
		MethodResponses []json.RawMessage `json:"methodResponses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&jmapResp); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}

	if len(jmapResp.MethodResponses) == 0 {
		return fmt.Errorf("%w: empty methodResponses from complete", ErrInvalidResponse)
	}

	var tuple []json.RawMessage
	if err := json.Unmarshal(jmapResp.MethodResponses[0], &tuple); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}
	if len(tuple) < 1 {
		return fmt.Errorf("%w: method response tuple too short", ErrInvalidResponse)
	}

	var methodName string
	if err := json.Unmarshal(tuple[0], &methodName); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}

	if methodName == "error" {
		return fmt.Errorf("%w: Blob/complete returned JMAP error", ErrServerFail)
	}

	return nil
}
