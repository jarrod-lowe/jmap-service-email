package blob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Error types for blob operations.
var (
	ErrBlobNotFound      = errors.New("blob not found")
	ErrForbidden         = errors.New("forbidden")
	ErrServerFail        = errors.New("server error")
	ErrInvalidArguments  = errors.New("invalid arguments")
	ErrInvalidResponse   = errors.New("invalid response")
)

// BlobStreamer abstracts streaming blob downloads for dependency inversion.
type BlobStreamer interface {
	Stream(ctx context.Context, accountID, blobID string) (io.ReadCloser, error)
}

// BlobUploader abstracts blob uploads for dependency inversion.
type BlobUploader interface {
	Upload(ctx context.Context, accountID, parentBlobID, contentType string, body io.Reader) (blobID string, size int64, err error)
}

// HTTPDoer abstracts HTTP client operations for dependency inversion.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// HTTPBlobClient fetches blobs via HTTP.
type HTTPBlobClient struct {
	baseURL    string
	httpClient HTTPDoer
	maxRetries int
	baseDelay  time.Duration
	sleepFunc  func(time.Duration)
}

// NewHTTPBlobClient creates a new HTTPBlobClient with default settings.
func NewHTTPBlobClient(baseURL string, httpClient HTTPDoer) *HTTPBlobClient {
	return &HTTPBlobClient{
		baseURL:    baseURL,
		httpClient: httpClient,
		maxRetries: 2,
		baseDelay:  100 * time.Millisecond,
		sleepFunc:  time.Sleep,
	}
}

// blobURL constructs the download URL for a blob.
func (c *HTTPBlobClient) blobURL(accountID, blobID string) string {
	return c.baseURL + "/download-iam/" + accountID + "/" + blobID
}

// FetchBlob fetches a blob by account ID and blob ID.
func (c *HTTPBlobClient) FetchBlob(ctx context.Context, accountID, blobID string) ([]byte, error) {
	url := c.blobURL(accountID, blobID)

	maxAttempts := c.maxRetries + 1
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Check context before each attempt
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Sleep before retry (not before first attempt)
		if attempt > 0 && c.sleepFunc != nil && c.baseDelay > 0 {
			delay := c.baseDelay * time.Duration(1<<(attempt-1)) // exponential: 1x, 2x, 4x, ...
			c.sleepFunc(delay)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return nil, ErrBlobNotFound
		}
		if resp.StatusCode == http.StatusForbidden {
			resp.Body.Close()
			return nil, ErrForbidden
		}
		if resp.StatusCode >= 500 {
			resp.Body.Close()
			lastErr = ErrServerFail
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		return body, nil
	}

	return nil, lastErr
}

// Stream returns a streaming reader for a blob. The caller is responsible for closing the reader.
func (c *HTTPBlobClient) Stream(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
	url := c.blobURL(accountID, blobID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, ErrBlobNotFound
	}
	if resp.StatusCode == http.StatusForbidden {
		resp.Body.Close()
		return nil, ErrForbidden
	}
	if resp.StatusCode >= 500 {
		resp.Body.Close()
		return nil, ErrServerFail
	}

	return resp.Body, nil
}

// uploadURL constructs the upload URL for a blob.
func (c *HTTPBlobClient) uploadURL(accountID string) string {
	return c.baseURL + "/upload-iam/" + accountID
}

// uploadResponse represents the JSON response from a blob upload.
type uploadResponse struct {
	BlobID string `json:"blobId"`
	Size   int64  `json:"size"`
}

// Upload uploads content as a new blob and returns the blob ID and size.
func (c *HTTPBlobClient) Upload(ctx context.Context, accountID, parentBlobID, contentType string, body io.Reader) (string, int64, error) {
	url := c.uploadURL(accountID)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return "", 0, err
	}

	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-Parent", parentBlobID)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		return "", 0, ErrServerFail
	}
	if resp.StatusCode >= 400 {
		return "", 0, fmt.Errorf("%w: status %d", ErrInvalidArguments, resp.StatusCode)
	}

	var uploadResp uploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return "", 0, fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}

	return uploadResp.BlobID, uploadResp.Size, nil
}
