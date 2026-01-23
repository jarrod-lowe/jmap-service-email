package blob

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"
)

// fakeHTTPDoer implements HTTPDoer for testing.
type fakeHTTPDoer struct {
	doFunc func(req *http.Request) (*http.Response, error)
}

func (f *fakeHTTPDoer) Do(req *http.Request) (*http.Response, error) {
	if f.doFunc != nil {
		return f.doFunc(req)
	}
	return nil, nil
}

func TestFetchBlob_ConstructsCorrectURL(t *testing.T) {
	var capturedURL string
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			capturedURL = req.URL.String()
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	_, _ = client.FetchBlob(context.Background(), "user-123", "blob-456")

	expected := "https://api.example.com/download-iam/user-123/blob-456"
	if capturedURL != expected {
		t.Errorf("URL = %q, want %q", capturedURL, expected)
	}
}

func TestFetchBlob_ReturnsBodyOn200(t *testing.T) {
	expectedBody := []byte("email content here")
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(expectedBody)),
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	body, err := client.FetchBlob(context.Background(), "user-123", "blob-456")
	if err != nil {
		t.Fatalf("FetchBlob error = %v, want nil", err)
	}
	if !bytes.Equal(body, expectedBody) {
		t.Errorf("body = %q, want %q", body, expectedBody)
	}
}

func TestFetchBlob_Returns404AsErrBlobNotFound(t *testing.T) {
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	_, err := client.FetchBlob(context.Background(), "user-123", "blob-456")
	if !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("error = %v, want ErrBlobNotFound", err)
	}
}

func TestFetchBlob_Returns403AsErrForbidden(t *testing.T) {
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusForbidden,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	_, err := client.FetchBlob(context.Background(), "user-123", "blob-456")
	if !errors.Is(err, ErrForbidden) {
		t.Errorf("error = %v, want ErrForbidden", err)
	}
}

func TestFetchBlob_Returns5xxAsErrServerFail(t *testing.T) {
	statusCodes := []int{500, 502, 503}

	for _, code := range statusCodes {
		t.Run(http.StatusText(code), func(t *testing.T) {
			fake := &fakeHTTPDoer{
				doFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: code,
						Body:       http.NoBody,
					}, nil
				},
			}

			client := &HTTPBlobClient{
				baseURL:    "https://api.example.com",
				httpClient: fake,
			}

			_, err := client.FetchBlob(context.Background(), "user-123", "blob-456")
			if !errors.Is(err, ErrServerFail) {
				t.Errorf("status %d: error = %v, want ErrServerFail", code, err)
			}
		})
	}
}

func TestFetchBlob_RetriesOn5xx_SucceedsOnRetry(t *testing.T) {
	attempts := 0
	expectedBody := []byte("success on retry")
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			attempts++
			if attempts == 1 {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       http.NoBody,
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(expectedBody)),
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
		maxRetries: 2,
	}

	body, err := client.FetchBlob(context.Background(), "user-123", "blob-456")
	if err != nil {
		t.Fatalf("FetchBlob error = %v, want nil", err)
	}
	if !bytes.Equal(body, expectedBody) {
		t.Errorf("body = %q, want %q", body, expectedBody)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2", attempts)
	}
}

func TestFetchBlob_MaxRetriesExhausted_ReturnsError(t *testing.T) {
	attempts := 0
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			attempts++
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
		maxRetries: 2,
	}

	_, err := client.FetchBlob(context.Background(), "user-123", "blob-456")
	if !errors.Is(err, ErrServerFail) {
		t.Errorf("error = %v, want ErrServerFail", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3 (1 initial + 2 retries)", attempts)
	}
}

func TestFetchBlob_DoesNotRetryOn4xx(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		wantErr    error
	}{
		{"404", http.StatusNotFound, ErrBlobNotFound},
		{"403", http.StatusForbidden, ErrForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attempts := 0
			fake := &fakeHTTPDoer{
				doFunc: func(req *http.Request) (*http.Response, error) {
					attempts++
					return &http.Response{
						StatusCode: tt.statusCode,
						Body:       http.NoBody,
					}, nil
				},
			}

			client := &HTTPBlobClient{
				baseURL:    "https://api.example.com",
				httpClient: fake,
				maxRetries: 2,
			}

			_, err := client.FetchBlob(context.Background(), "user-123", "blob-456")
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("error = %v, want %v", err, tt.wantErr)
			}
			if attempts != 1 {
				t.Errorf("attempts = %d, want 1 (should not retry on %d)", attempts, tt.statusCode)
			}
		})
	}
}

func TestFetchBlob_RetriesOnNetworkError_SucceedsOnRetry(t *testing.T) {
	attempts := 0
	expectedBody := []byte("success after network error")
	networkErr := errors.New("connection refused")
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			attempts++
			if attempts == 1 {
				return nil, networkErr
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(expectedBody)),
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
		maxRetries: 2,
	}

	body, err := client.FetchBlob(context.Background(), "user-123", "blob-456")
	if err != nil {
		t.Fatalf("FetchBlob error = %v, want nil", err)
	}
	if !bytes.Equal(body, expectedBody) {
		t.Errorf("body = %q, want %q", body, expectedBody)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2", attempts)
	}
}

func TestFetchBlob_ExponentialBackoff(t *testing.T) {
	attempts := 0
	var delays []time.Duration
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			attempts++
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
		maxRetries: 2,
		baseDelay:  100 * time.Millisecond,
		sleepFunc: func(d time.Duration) {
			delays = append(delays, d)
		},
	}

	_, _ = client.FetchBlob(context.Background(), "user-123", "blob-456")

	// Should have 2 delays (before 2nd and 3rd attempts)
	if len(delays) != 2 {
		t.Fatalf("delay count = %d, want 2", len(delays))
	}
	// First delay should be 100ms
	if delays[0] != 100*time.Millisecond {
		t.Errorf("first delay = %v, want 100ms", delays[0])
	}
	// Second delay should be 200ms (doubled)
	if delays[1] != 200*time.Millisecond {
		t.Errorf("second delay = %v, want 200ms", delays[1])
	}
}

func TestFetchBlob_ContextCancelled_ReturnsImmediately(t *testing.T) {
	attempts := 0
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			attempts++
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
		maxRetries: 2,
		baseDelay:  100 * time.Millisecond,
		sleepFunc:  func(d time.Duration) {},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := client.FetchBlob(ctx, "user-123", "blob-456")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
	if attempts != 0 {
		t.Errorf("attempts = %d, want 0 (should not attempt with cancelled context)", attempts)
	}
}

func TestBlobURL_ConstructsCorrectURL(t *testing.T) {
	client := &HTTPBlobClient{
		baseURL: "https://api.example.com",
	}

	url := client.blobURL("user-123", "blob-456")

	expected := "https://api.example.com/download-iam/user-123/blob-456"
	if url != expected {
		t.Errorf("blobURL() = %q, want %q", url, expected)
	}
}

func TestNewHTTPBlobClient_SetsDefaults(t *testing.T) {
	fake := &fakeHTTPDoer{}

	client := NewHTTPBlobClient("https://api.example.com", fake)

	if client.baseURL != "https://api.example.com" {
		t.Errorf("baseURL = %q, want %q", client.baseURL, "https://api.example.com")
	}
	if client.httpClient != fake {
		t.Errorf("httpClient not set correctly")
	}
	if client.maxRetries != 2 {
		t.Errorf("maxRetries = %d, want 2", client.maxRetries)
	}
	if client.baseDelay != 100*time.Millisecond {
		t.Errorf("baseDelay = %v, want 100ms", client.baseDelay)
	}
	if client.sleepFunc == nil {
		t.Errorf("sleepFunc should not be nil")
	}
}
