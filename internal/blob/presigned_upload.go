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

// PresignedUploadClient uploads blobs using a two-step presigned URL flow:
// 1. Call Blob/allocate via JMAP to get a blobID + presigned S3 PUT URL
// 2. PUT the streaming body directly to the presigned URL
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
	ID  string `json:"id"`
	URL string `json:"url"`
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

	// Step 1: Call Blob/allocate to get blobID + presigned URL
	blobID, presignedURL, err := c.allocate(ctx, accountID, contentType)
	if err != nil {
		tracing.RecordError(span, err)
		return "", 0, err
	}

	// Step 2: PUT body to presigned URL
	cr := &countingReader{reader: body}
	if err := c.putToPresignedURL(ctx, presignedURL, contentType, cr); err != nil {
		tracing.RecordError(span, err)
		return "", 0, err
	}

	return blobID, cr.bytesRead, nil
}

// allocate sends a Blob/allocate JMAP request and returns the blobID and presigned URL.
func (c *PresignedUploadClient) allocate(ctx context.Context, accountID, contentType string) (string, string, error) {
	reqBody := jmapRequest{
		Using: []string{"https://jmap.rrod.net/extensions/upload-put"},
		MethodCalls: []any{
			[]any{
				"Blob/allocate",
				map[string]any{
					"accountId": accountID,
					"create": map[string]any{
						"c0": map[string]any{
							"type": contentType,
							"size": 0,
						},
					},
				},
				"c0",
			},
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrInvalidArguments, err)
	}

	url := c.baseURL + "/jmap-iam/" + accountID
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrInvalidArguments, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.signedClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrServerFail, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		return "", "", fmt.Errorf("%w: allocate returned status %d", ErrServerFail, resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		return "", "", fmt.Errorf("%w: allocate returned status %d", ErrInvalidArguments, resp.StatusCode)
	}

	// Parse JMAP response
	var jmapResp struct {
		MethodResponses []json.RawMessage `json:"methodResponses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&jmapResp); err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}

	if len(jmapResp.MethodResponses) == 0 {
		return "", "", fmt.Errorf("%w: empty methodResponses", ErrInvalidResponse)
	}

	// Parse the first method response tuple [name, args, clientId]
	var tuple []json.RawMessage
	if err := json.Unmarshal(jmapResp.MethodResponses[0], &tuple); err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}
	if len(tuple) < 2 {
		return "", "", fmt.Errorf("%w: method response tuple too short", ErrInvalidResponse)
	}

	var methodName string
	if err := json.Unmarshal(tuple[0], &methodName); err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}

	if methodName == "error" {
		return "", "", fmt.Errorf("%w: Blob/allocate returned JMAP error", ErrServerFail)
	}

	// Parse the response args
	var allocateResp struct {
		Created    map[string]allocateCreated `json:"created"`
		NotCreated map[string]any             `json:"notCreated"`
	}
	if err := json.Unmarshal(tuple[1], &allocateResp); err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}

	// Check for notCreated
	if nc, ok := allocateResp.NotCreated["c0"]; ok {
		return "", "", fmt.Errorf("%w: Blob/allocate notCreated: %v", ErrServerFail, nc)
	}

	created, ok := allocateResp.Created["c0"]
	if !ok {
		return "", "", fmt.Errorf("%w: no 'c0' in created", ErrInvalidResponse)
	}

	return created.ID, created.URL, nil
}

// putToPresignedURL PUTs the body to the presigned S3 URL.
func (c *PresignedUploadClient) putToPresignedURL(ctx context.Context, presignedURL, contentType string, body io.Reader) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, presignedURL, body)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArguments, err)
	}
	req.Header.Set("Content-Type", contentType)

	resp, err := c.plainClient.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrServerFail, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("%w: presigned PUT returned status %d", ErrServerFail, resp.StatusCode)
	}

	return nil
}
