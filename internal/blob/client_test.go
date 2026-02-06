package blob

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
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

// Tests for Stream method

func TestStream_ConstructsCorrectURL(t *testing.T) {
	var capturedURL string
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			capturedURL = req.URL.String()
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte("content"))),
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	rc, err := client.Stream(context.Background(), "user-123", "blob-456")
	if err != nil {
		t.Fatalf("Stream error = %v, want nil", err)
	}
	defer rc.Close()

	expected := "https://api.example.com/download-iam/user-123/blob-456"
	if capturedURL != expected {
		t.Errorf("URL = %q, want %q", capturedURL, expected)
	}
}

func TestStream_ReturnsReadCloserOn200(t *testing.T) {
	expectedContent := []byte("streamed content")
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(expectedContent)),
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	rc, err := client.Stream(context.Background(), "user-123", "blob-456")
	if err != nil {
		t.Fatalf("Stream error = %v, want nil", err)
	}
	defer rc.Close()

	content, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll error = %v", err)
	}
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("content = %q, want %q", content, expectedContent)
	}
}

func TestStream_Returns404AsErrBlobNotFound(t *testing.T) {
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

	_, err := client.Stream(context.Background(), "user-123", "blob-456")
	if !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("error = %v, want ErrBlobNotFound", err)
	}
}

func TestStream_Returns403AsErrForbidden(t *testing.T) {
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

	_, err := client.Stream(context.Background(), "user-123", "blob-456")
	if !errors.Is(err, ErrForbidden) {
		t.Errorf("error = %v, want ErrForbidden", err)
	}
}

func TestStream_Returns5xxAsErrServerFail(t *testing.T) {
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	_, err := client.Stream(context.Background(), "user-123", "blob-456")
	if !errors.Is(err, ErrServerFail) {
		t.Errorf("error = %v, want ErrServerFail", err)
	}
}

// Tests for Upload method

func TestUpload_ConstructsCorrectURL(t *testing.T) {
	var capturedURL string
	var capturedMethod string
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			capturedURL = req.URL.String()
			capturedMethod = req.Method
			return &http.Response{
				StatusCode: http.StatusCreated,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"blobId":"new-blob-123","size":100}`))),
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("content")))
	if err != nil {
		t.Fatalf("Upload error = %v, want nil", err)
	}

	expectedURL := "https://api.example.com/upload-iam/user-123"
	if capturedURL != expectedURL {
		t.Errorf("URL = %q, want %q", capturedURL, expectedURL)
	}
	if capturedMethod != http.MethodPost {
		t.Errorf("Method = %q, want %q", capturedMethod, http.MethodPost)
	}
}

func TestUpload_SetsRequiredHeaders(t *testing.T) {
	var capturedContentType string
	var capturedParentHeader string
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			capturedContentType = req.Header.Get("Content-Type")
			capturedParentHeader = req.Header.Get("X-Parent")
			return &http.Response{
				StatusCode: http.StatusCreated,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"blobId":"new-blob-123","size":100}`))),
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob-456", "application/pdf", bytes.NewReader([]byte("content")))
	if err != nil {
		t.Fatalf("Upload error = %v, want nil", err)
	}

	if capturedContentType != "application/pdf" {
		t.Errorf("Content-Type = %q, want %q", capturedContentType, "application/pdf")
	}
	if capturedParentHeader != "parent-blob-456" {
		t.Errorf("X-Parent = %q, want %q", capturedParentHeader, "parent-blob-456")
	}
}

func TestUpload_StreamsBodyDirectly(t *testing.T) {
	var capturedBody []byte
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			capturedBody, _ = io.ReadAll(req.Body)
			return &http.Response{
				StatusCode: http.StatusCreated,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"blobId":"new-blob-123","size":7}`))),
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	content := []byte("content")
	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Upload error = %v, want nil", err)
	}

	if !bytes.Equal(capturedBody, content) {
		t.Errorf("body = %q, want %q", capturedBody, content)
	}
}

func TestUpload_ParsesResponseForBlobID(t *testing.T) {
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusCreated,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"blobId":"uploaded-blob-789","size":42}`))),
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	blobID, size, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("test")))
	if err != nil {
		t.Fatalf("Upload error = %v, want nil", err)
	}

	if blobID != "uploaded-blob-789" {
		t.Errorf("blobID = %q, want %q", blobID, "uploaded-blob-789")
	}
	if size != 42 {
		t.Errorf("size = %d, want 42", size)
	}
}

func TestUpload_Returns4xxAsInvalidArguments(t *testing.T) {
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusBadRequest,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"type":"invalidArguments","description":"bad request"}`))),
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("test")))
	if err == nil {
		t.Fatal("Upload should return error for 4xx")
	}
}

func TestUpload_Returns5xxAsServerFail(t *testing.T) {
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("test")))
	if !errors.Is(err, ErrServerFail) {
		t.Errorf("error = %v, want ErrServerFail", err)
	}
}

// Tests for Delete method

func TestDelete_ConstructsCorrectURL(t *testing.T) {
	var capturedURL string
	var capturedMethod string
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			capturedURL = req.URL.String()
			capturedMethod = req.Method
			return &http.Response{
				StatusCode: http.StatusNoContent,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	err := client.Delete(context.Background(), "user-123", "blob-456")
	if err != nil {
		t.Fatalf("Delete error = %v, want nil", err)
	}

	expectedURL := "https://api.example.com/delete-iam/user-123/blob-456"
	if capturedURL != expectedURL {
		t.Errorf("URL = %q, want %q", capturedURL, expectedURL)
	}
	if capturedMethod != http.MethodDelete {
		t.Errorf("Method = %q, want %q", capturedMethod, http.MethodDelete)
	}
}

func TestDelete_204ReturnsNil(t *testing.T) {
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusNoContent,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	err := client.Delete(context.Background(), "user-123", "blob-456")
	if err != nil {
		t.Errorf("Delete error = %v, want nil", err)
	}
}

func TestDelete_404ReturnsNil(t *testing.T) {
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

	err := client.Delete(context.Background(), "user-123", "blob-456")
	if err != nil {
		t.Errorf("Delete error = %v, want nil (404 should be ignored)", err)
	}
}

func TestDelete_5xxReturnsError(t *testing.T) {
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	err := client.Delete(context.Background(), "user-123", "blob-456")
	if !errors.Is(err, ErrServerFail) {
		t.Errorf("Delete error = %v, want ErrServerFail", err)
	}
}

func TestDelete_NetworkErrorReturnsError(t *testing.T) {
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("connection refused")
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	err := client.Delete(context.Background(), "user-123", "blob-456")
	if err == nil {
		t.Error("Delete should return error on network failure")
	}
}

func TestUpload_ReturnsNetworkError(t *testing.T) {
	networkErr := errors.New("connection refused")
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return nil, networkErr
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("test")))
	if err == nil {
		t.Fatal("Upload should return error for network failure")
	}
}

// setupTestTracer creates a test tracer provider and returns the span recorder.
func setupTestTracer(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	prev := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
	})
	return recorder
}

// findSpan finds a span by name in the recorded spans.
func findSpan(recorder *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	for _, span := range recorder.Ended() {
		if span.Name() == name {
			return span
		}
	}
	return nil
}

// hasAttribute checks if a span has an attribute with the given key and value.
func hasAttribute(span sdktrace.ReadOnlySpan, key, value string) bool {
	for _, attr := range span.Attributes() {
		if string(attr.Key) == key && attr.Value.AsString() == value {
			return true
		}
	}
	return false
}

// Tests for OTel tracing in blob client methods

func TestStream_CreatesSpanWithAttributes(t *testing.T) {
	recorder := setupTestTracer(t)
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte("content"))),
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	rc, err := client.Stream(context.Background(), "user-123", "blob-456")
	if err != nil {
		t.Fatalf("Stream error = %v, want nil", err)
	}
	rc.Close()

	span := findSpan(recorder, "blob.Stream")
	if span == nil {
		t.Fatal("Expected span 'blob.Stream' not found")
	}

	if !hasAttribute(span, "account_id", "user-123") {
		t.Error("Span missing attribute account_id=user-123")
	}
	if !hasAttribute(span, "blob_id", "blob-456") {
		t.Error("Span missing attribute blob_id=blob-456")
	}
}

func TestStream_RecordsErrorOnSpan(t *testing.T) {
	recorder := setupTestTracer(t)
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

	_, err := client.Stream(context.Background(), "user-123", "blob-456")
	if err == nil {
		t.Fatal("Stream should return error for 404")
	}

	span := findSpan(recorder, "blob.Stream")
	if span == nil {
		t.Fatal("Expected span 'blob.Stream' not found")
	}

	// Check span recorded the error
	if span.Status().Code == 0 {
		t.Error("Span should have error status set")
	}
}

func TestUpload_CreatesSpanWithAttributes(t *testing.T) {
	recorder := setupTestTracer(t)
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusCreated,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"blobId":"new-blob-123","size":100}`))),
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob-456", "application/pdf", bytes.NewReader([]byte("content")))
	if err != nil {
		t.Fatalf("Upload error = %v, want nil", err)
	}

	span := findSpan(recorder, "blob.Upload")
	if span == nil {
		t.Fatal("Expected span 'blob.Upload' not found")
	}

	if !hasAttribute(span, "account_id", "user-123") {
		t.Error("Span missing attribute account_id=user-123")
	}
	if !hasAttribute(span, "parent_blob_id", "parent-blob-456") {
		t.Error("Span missing attribute parent_blob_id=parent-blob-456")
	}
	if !hasAttribute(span, "content_type", "application/pdf") {
		t.Error("Span missing attribute content_type=application/pdf")
	}
}

func TestUpload_RecordsErrorOnSpan(t *testing.T) {
	recorder := setupTestTracer(t)
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("test")))
	if err == nil {
		t.Fatal("Upload should return error for 500")
	}

	span := findSpan(recorder, "blob.Upload")
	if span == nil {
		t.Fatal("Expected span 'blob.Upload' not found")
	}

	if span.Status().Code == 0 {
		t.Error("Span should have error status set")
	}
}

func TestDelete_CreatesSpanWithAttributes(t *testing.T) {
	recorder := setupTestTracer(t)
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusNoContent,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	err := client.Delete(context.Background(), "user-123", "blob-456")
	if err != nil {
		t.Fatalf("Delete error = %v, want nil", err)
	}

	span := findSpan(recorder, "blob.Delete")
	if span == nil {
		t.Fatal("Expected span 'blob.Delete' not found")
	}

	if !hasAttribute(span, "account_id", "user-123") {
		t.Error("Span missing attribute account_id=user-123")
	}
	if !hasAttribute(span, "blob_id", "blob-456") {
		t.Error("Span missing attribute blob_id=blob-456")
	}
}

func TestDelete_RecordsErrorOnSpan(t *testing.T) {
	recorder := setupTestTracer(t)
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	err := client.Delete(context.Background(), "user-123", "blob-456")
	if err == nil {
		t.Fatal("Delete should return error for 500")
	}

	span := findSpan(recorder, "blob.Delete")
	if span == nil {
		t.Fatal("Expected span 'blob.Delete' not found")
	}

	if span.Status().Code == 0 {
		t.Error("Span should have error status set")
	}
}

func TestFetchBlob_CreatesSpanWithAttributes(t *testing.T) {
	recorder := setupTestTracer(t)
	fake := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte("content"))),
			}, nil
		},
	}

	client := &HTTPBlobClient{
		baseURL:    "https://api.example.com",
		httpClient: fake,
	}

	_, err := client.FetchBlob(context.Background(), "user-123", "blob-456")
	if err != nil {
		t.Fatalf("FetchBlob error = %v, want nil", err)
	}

	span := findSpan(recorder, "blob.Fetch")
	if span == nil {
		t.Fatal("Expected span 'blob.Fetch' not found")
	}

	if !hasAttribute(span, "account_id", "user-123") {
		t.Error("Span missing attribute account_id=user-123")
	}
	if !hasAttribute(span, "blob_id", "blob-456") {
		t.Error("Span missing attribute blob_id=blob-456")
	}
}

func TestFetchBlob_RecordsErrorOnSpan(t *testing.T) {
	recorder := setupTestTracer(t)
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
	if err == nil {
		t.Fatal("FetchBlob should return error for 404")
	}

	span := findSpan(recorder, "blob.Fetch")
	if span == nil {
		t.Fatal("Expected span 'blob.Fetch' not found")
	}

	if span.Status().Code == 0 {
		t.Error("Span should have error status set")
	}
}

// Ensure attribute import is used
var _ = attribute.String("test", "test")
