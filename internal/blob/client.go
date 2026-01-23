package blob

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"
)

// Error types for blob operations.
var (
	ErrBlobNotFound = errors.New("blob not found")
	ErrForbidden    = errors.New("forbidden")
	ErrServerFail   = errors.New("server error")
)

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
