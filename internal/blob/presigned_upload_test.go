package blob

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"go.opentelemetry.io/otel"
)

// allocateSuccessResponse builds a JMAP response body for a successful Blob/allocate.
func allocateSuccessResponse(blobID, presignedURL string) string {
	resp := map[string]any{
		"methodResponses": []any{
			[]any{
				"Blob/allocate",
				map[string]any{
					"accountId": "user-123",
					"created": map[string]any{
						"c0": map[string]any{
							"id":  blobID,
							"url": presignedURL,
						},
					},
				},
				"c0",
			},
		},
	}
	b, _ := json.Marshal(resp)
	return string(b)
}

func TestPresignedUpload_ConstructsCorrectAllocateRequest(t *testing.T) {
	var capturedReq *http.Request
	var capturedBody []byte

	signedClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			capturedBody, _ = io.ReadAll(req.Body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte(allocateSuccessResponse("blob-new", "https://s3.example.com/put")))),
			}, nil
		},
	}
	plainClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := NewPresignedUploadClient("https://api.example.com", signedClient, plainClient)
	_, _, _ = client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("hello")))

	if capturedReq == nil {
		t.Fatal("signedClient was not called")
	}

	// Verify URL
	expectedURL := "https://api.example.com/jmap-iam/user-123"
	if capturedReq.URL.String() != expectedURL {
		t.Errorf("URL = %q, want %q", capturedReq.URL.String(), expectedURL)
	}

	// Verify method is POST
	if capturedReq.Method != http.MethodPost {
		t.Errorf("Method = %q, want POST", capturedReq.Method)
	}

	// Verify Content-Type
	if ct := capturedReq.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	// Parse the JMAP request body
	var jmapReq map[string]any
	if err := json.Unmarshal(capturedBody, &jmapReq); err != nil {
		t.Fatalf("Failed to parse JMAP request: %v", err)
	}

	// Verify using capability
	using, ok := jmapReq["using"].([]any)
	if !ok || len(using) == 0 {
		t.Fatal("missing 'using' in JMAP request")
	}
	found := false
	for _, u := range using {
		if u == "https://jmap.rrod.net/extensions/upload-put" {
			found = true
		}
	}
	if !found {
		t.Errorf("using = %v, want to contain 'https://jmap.rrod.net/extensions/upload-put'", using)
	}

	// Verify methodCalls
	calls, ok := jmapReq["methodCalls"].([]any)
	if !ok || len(calls) == 0 {
		t.Fatal("missing 'methodCalls' in JMAP request")
	}
	call := calls[0].([]any)
	if call[0] != "Blob/allocate" {
		t.Errorf("method = %v, want Blob/allocate", call[0])
	}
	args := call[1].(map[string]any)
	if args["accountId"] != "user-123" {
		t.Errorf("accountId = %v, want user-123", args["accountId"])
	}
	// Verify create.c0 contains size and type
	create, ok := args["create"].(map[string]any)
	if !ok {
		t.Fatal("missing 'create' in method args")
	}
	c0, ok := create["c0"].(map[string]any)
	if !ok {
		t.Fatal("missing 'c0' in create")
	}
	// size should be 0 for unknown-size streaming
	if size, ok := c0["size"].(float64); !ok || size != 0 {
		t.Errorf("size = %v, want 0", c0["size"])
	}
	if c0["type"] != "text/plain" {
		t.Errorf("type = %v, want text/plain", c0["type"])
	}
}

func TestPresignedUpload_PutsBodyToPresignedURL(t *testing.T) {
	signedClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte(allocateSuccessResponse("blob-new", "https://s3.example.com/presigned-put")))),
			}, nil
		},
	}

	var capturedPutReq *http.Request
	var capturedPutBody []byte
	plainClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			capturedPutReq = req
			capturedPutBody, _ = io.ReadAll(req.Body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := NewPresignedUploadClient("https://api.example.com", signedClient, plainClient)
	content := []byte("email body content")
	_, _, _ = client.Upload(context.Background(), "user-123", "parent-blob", "message/rfc822", bytes.NewReader(content))

	if capturedPutReq == nil {
		t.Fatal("plainClient was not called")
	}

	// Verify URL is the presigned URL
	if capturedPutReq.URL.String() != "https://s3.example.com/presigned-put" {
		t.Errorf("PUT URL = %q, want %q", capturedPutReq.URL.String(), "https://s3.example.com/presigned-put")
	}

	// Verify method is PUT
	if capturedPutReq.Method != http.MethodPut {
		t.Errorf("Method = %q, want PUT", capturedPutReq.Method)
	}

	// Verify Content-Type
	if ct := capturedPutReq.Header.Get("Content-Type"); ct != "message/rfc822" {
		t.Errorf("Content-Type = %q, want message/rfc822", ct)
	}

	// Verify body was sent
	if !bytes.Equal(capturedPutBody, content) {
		t.Errorf("PUT body = %q, want %q", capturedPutBody, content)
	}
}

func TestPresignedUpload_ReturnsBlobIDAndSize(t *testing.T) {
	signedClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte(allocateSuccessResponse("blob-abc-123", "https://s3.example.com/put")))),
			}, nil
		},
	}
	plainClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			// Consume request body so countingReader tracks bytes
			_, _ = io.Copy(io.Discard, req.Body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := NewPresignedUploadClient("https://api.example.com", signedClient, plainClient)
	content := []byte("hello world 12345")
	blobID, size, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Upload error = %v, want nil", err)
	}

	if blobID != "blob-abc-123" {
		t.Errorf("blobID = %q, want %q", blobID, "blob-abc-123")
	}
	if size != int64(len(content)) {
		t.Errorf("size = %d, want %d", size, len(content))
	}
}

func TestPresignedUpload_AllocateHTTPError(t *testing.T) {
	signedClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(bytes.NewReader([]byte("Internal Server Error"))),
			}, nil
		},
	}
	plainClient := &fakeHTTPDoer{}

	client := NewPresignedUploadClient("https://api.example.com", signedClient, plainClient)
	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("test")))
	if err == nil {
		t.Fatal("Upload should return error for 5xx allocate response")
	}
	if !errors.Is(err, ErrServerFail) {
		t.Errorf("error = %v, want ErrServerFail", err)
	}
}

func TestPresignedUpload_AllocateJMAPError(t *testing.T) {
	jmapErrorResp := `{"methodResponses":[["error",{"type":"serverFail","description":"something broke"},"c0"]]}`
	signedClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte(jmapErrorResp))),
			}, nil
		},
	}
	plainClient := &fakeHTTPDoer{}

	client := NewPresignedUploadClient("https://api.example.com", signedClient, plainClient)
	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("test")))
	if err == nil {
		t.Fatal("Upload should return error for JMAP error response")
	}
	if !errors.Is(err, ErrServerFail) {
		t.Errorf("error = %v, want ErrServerFail", err)
	}
}

func TestPresignedUpload_AllocateNotCreated(t *testing.T) {
	notCreatedResp := `{"methodResponses":[["Blob/allocate",{"accountId":"user-123","created":{},"notCreated":{"c0":{"type":"overQuota","description":"quota exceeded"}}},"c0"]]}`
	signedCalled := false
	signedClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			signedCalled = true
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte(notCreatedResp))),
			}, nil
		},
	}
	plainClient := &fakeHTTPDoer{}

	client := NewPresignedUploadClient("https://api.example.com", signedClient, plainClient)
	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("test")))
	if !signedCalled {
		t.Fatal("signedClient should have been called for allocate")
	}
	if err == nil {
		t.Fatal("Upload should return error for notCreated response")
	}
}

func TestPresignedUpload_PutFailure(t *testing.T) {
	signedClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte(allocateSuccessResponse("blob-new", "https://s3.example.com/put")))),
			}, nil
		},
	}
	plainCalled := false
	plainClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			plainCalled = true
			return &http.Response{
				StatusCode: http.StatusForbidden,
				Body:       io.NopCloser(bytes.NewReader([]byte("Access Denied"))),
			}, nil
		},
	}

	client := NewPresignedUploadClient("https://api.example.com", signedClient, plainClient)
	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("test")))
	if !plainCalled {
		t.Fatal("plainClient should have been called for PUT")
	}
	if err == nil {
		t.Fatal("Upload should return error for failed PUT")
	}
}

func TestPresignedUpload_CreatesSpan(t *testing.T) {
	recorder := setupTestTracer(t)

	signedClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte(allocateSuccessResponse("blob-new", "https://s3.example.com/put")))),
			}, nil
		},
	}
	plainClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       http.NoBody,
			}, nil
		},
	}

	client := NewPresignedUploadClient("https://api.example.com", signedClient, plainClient)
	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob-456", "application/pdf", bytes.NewReader([]byte("content")))
	if err != nil {
		t.Fatalf("Upload error = %v, want nil", err)
	}

	span := findSpan(recorder, "blob.PresignedUpload")
	if span == nil {
		t.Fatal("Expected span 'blob.PresignedUpload' not found")
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

// Ensure otel import is used.
var _ = otel.GetTracerProvider
