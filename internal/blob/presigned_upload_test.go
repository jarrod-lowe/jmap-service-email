package blob

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"

	"go.opentelemetry.io/otel"
)

// allocateMultipartSuccessResponse builds a JMAP response body for a successful Blob/allocate
// with multipart part URLs.
func allocateMultipartSuccessResponse(blobID string, partURLs []string) string {
	parts := make([]map[string]any, len(partURLs))
	for i, u := range partURLs {
		parts[i] = map[string]any{
			"partNumber": float64(i + 1),
			"url":        u,
		}
	}
	resp := map[string]any{
		"methodResponses": []any{
			[]any{
				"Blob/allocate",
				map[string]any{
					"accountId": "user-123",
					"created": map[string]any{
						"c0": map[string]any{
							"id":    blobID,
							"parts": parts,
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

// completeSuccessResponse builds a JMAP response body for a successful Blob/complete.
func completeSuccessResponse() string {
	resp := map[string]any{
		"methodResponses": []any{
			[]any{
				"Blob/complete",
				map[string]any{
					"accountId": "user-123",
				},
				"c0",
			},
		},
	}
	b, _ := json.Marshal(resp)
	return string(b)
}

// makePartURLs generates n part URLs with a prefix.
func makePartURLs(n int) []string {
	urls := make([]string, n)
	for i := range urls {
		urls[i] = fmt.Sprintf("https://s3.example.com/part-%d", i+1)
	}
	return urls
}

// newMultipartMocks creates signed and plain clients for multipart upload tests.
// signedClient handles allocate (1st call) and complete (2nd call).
// plainClient handles part PUTs, returning ETags based on part number.
func newMultipartMocks(blobID string, numParts int) (signed *fakeHTTPDoer, plain *fakeHTTPDoer) {
	partURLs := makePartURLs(numParts)
	signedCallCount := 0

	signed = &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			signedCallCount++
			if signedCallCount == 1 {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader([]byte(allocateMultipartSuccessResponse(blobID, partURLs)))),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte(completeSuccessResponse()))),
			}, nil
		},
	}

	plain = &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			_, _ = io.ReadAll(req.Body)
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Etag": []string{`"etag-default"`}},
				Body:       http.NoBody,
			}
			return resp, nil
		},
	}

	return signed, plain
}

func TestPresignedUpload_AllocateRequestIncludesMultipartTrue(t *testing.T) {
	var capturedBody []byte

	signedClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			capturedBody, _ = io.ReadAll(req.Body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte(allocateMultipartSuccessResponse("blob-new", makePartURLs(100))))),
			}, nil
		},
	}
	plainClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			_, _ = io.ReadAll(req.Body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Etag": []string{`"etag-1"`}},
				Body:       http.NoBody,
			}, nil
		},
	}

	// Complete response for the 2nd signed call
	origFunc := signedClient.doFunc
	callCount := 0
	signedClient.doFunc = func(req *http.Request) (*http.Response, error) {
		callCount++
		if callCount == 1 {
			return origFunc(req)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte(completeSuccessResponse()))),
		}, nil
	}

	client := NewPresignedUploadClient("https://api.example.com", signedClient, plainClient)
	_, _, _ = client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("hello")))

	// Parse the JMAP request body
	var jmapReq map[string]any
	if err := json.Unmarshal(capturedBody, &jmapReq); err != nil {
		t.Fatalf("Failed to parse JMAP request: %v", err)
	}

	calls, ok := jmapReq["methodCalls"].([]any)
	if !ok || len(calls) == 0 {
		t.Fatal("missing 'methodCalls' in JMAP request")
	}
	call := calls[0].([]any)
	args := call[1].(map[string]any)
	create := args["create"].(map[string]any)
	c0 := create["c0"].(map[string]any)

	multipart, ok := c0["multipart"].(bool)
	if !ok || !multipart {
		t.Errorf("multipart = %v, want true", c0["multipart"])
	}
}

func TestPresignedUpload_ConstructsCorrectAllocateRequest(t *testing.T) {
	var capturedReq *http.Request
	var capturedBody []byte

	signed, plain := newMultipartMocks("blob-new", 100)
	origFunc := signed.doFunc
	wrapCallCount := 0
	signed.doFunc = func(req *http.Request) (*http.Response, error) {
		wrapCallCount++
		if wrapCallCount == 1 {
			capturedReq = req
			capturedBody, _ = io.ReadAll(req.Body)
			req.Body = io.NopCloser(bytes.NewReader(capturedBody))
		}
		return origFunc(req)
	}

	client := NewPresignedUploadClient("https://api.example.com", signed, plain)
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
	create, ok := args["create"].(map[string]any)
	if !ok {
		t.Fatal("missing 'create' in method args")
	}
	c0, ok := create["c0"].(map[string]any)
	if !ok {
		t.Fatal("missing 'c0' in create")
	}
	if size, ok := c0["size"].(float64); !ok || size != 0 {
		t.Errorf("size = %v, want 0", c0["size"])
	}
	if c0["type"] != "text/plain" {
		t.Errorf("type = %v, want text/plain", c0["type"])
	}
}

func TestPresignedUpload_SmallBody_UploadsSinglePart(t *testing.T) {
	signed, _ := newMultipartMocks("blob-new", 100)

	var capturedPutReq *http.Request
	var capturedPutBody []byte
	plainClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			capturedPutReq = req
			capturedPutBody, _ = io.ReadAll(req.Body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Etag": []string{`"etag-1"`}},
				Body:       http.NoBody,
			}, nil
		},
	}

	client := NewPresignedUploadClient("https://api.example.com", signed, plainClient)
	content := []byte("hello")
	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "message/rfc822", bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Upload error = %v, want nil", err)
	}

	if capturedPutReq == nil {
		t.Fatal("plainClient was not called")
	}

	// Verify URL is the first part URL
	if capturedPutReq.URL.String() != "https://s3.example.com/part-1" {
		t.Errorf("PUT URL = %q, want %q", capturedPutReq.URL.String(), "https://s3.example.com/part-1")
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

func TestPresignedUpload_SmallBody_CallsCompleteWithCorrectParts(t *testing.T) {
	signedCallCount := 0
	var capturedCompleteBody []byte

	signedClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			signedCallCount++
			if signedCallCount == 1 {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader([]byte(allocateMultipartSuccessResponse("blob-new", makePartURLs(100))))),
				}, nil
			}
			capturedCompleteBody, _ = io.ReadAll(req.Body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte(completeSuccessResponse()))),
			}, nil
		},
	}
	plainClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			_, _ = io.ReadAll(req.Body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Etag": []string{`"etag-abc"`}},
				Body:       http.NoBody,
			}, nil
		},
	}

	client := NewPresignedUploadClient("https://api.example.com", signedClient, plainClient)
	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("Upload error = %v, want nil", err)
	}

	if capturedCompleteBody == nil {
		t.Fatal("complete was not called")
	}

	// Parse the complete request
	var jmapReq map[string]any
	if err := json.Unmarshal(capturedCompleteBody, &jmapReq); err != nil {
		t.Fatalf("Failed to parse complete request: %v", err)
	}

	calls := jmapReq["methodCalls"].([]any)
	call := calls[0].([]any)
	if call[0] != "Blob/complete" {
		t.Errorf("method = %v, want Blob/complete", call[0])
	}
	args := call[1].(map[string]any)
	if args["id"] != "blob-new" {
		t.Errorf("id = %v, want blob-new", args["id"])
	}
	parts := args["parts"].([]any)
	if len(parts) != 1 {
		t.Fatalf("parts count = %d, want 1", len(parts))
	}
	part := parts[0].(map[string]any)
	if pn := part["partNumber"].(float64); pn != 1 {
		t.Errorf("partNumber = %v, want 1", pn)
	}
	if etag := part["etag"].(string); etag != `"etag-abc"` {
		t.Errorf("etag = %v, want \"etag-abc\"", etag)
	}
}

func TestPresignedUpload_SmallBody_ReturnsBlobIDAndSize(t *testing.T) {
	signed, plain := newMultipartMocks("blob-abc-123", 100)

	client := NewPresignedUploadClient("https://api.example.com", signed, plain)
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

func TestPresignedUpload_LargeBody_UploadsMultipleParts(t *testing.T) {
	signed, _ := newMultipartMocks("blob-large", 100)

	var putURLs []string
	var putSizes []int
	plainClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			putURLs = append(putURLs, req.URL.String())
			body, _ := io.ReadAll(req.Body)
			putSizes = append(putSizes, len(body))
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Etag": []string{fmt.Sprintf(`"etag-%d"`, len(putURLs))}},
				Body:       http.NoBody,
			}, nil
		},
	}

	client := NewPresignedUploadClient("https://api.example.com", signed, plainClient)
	// 25MB body → 3 parts: 10MB, 10MB, 5MB
	content := make([]byte, 25*1024*1024)
	for i := range content {
		content[i] = byte(i % 256)
	}
	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "application/octet-stream", bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Upload error = %v, want nil", err)
	}

	if len(putURLs) != 3 {
		t.Fatalf("PUT count = %d, want 3", len(putURLs))
	}

	// Verify URLs are sequential part URLs
	for i, url := range putURLs {
		expected := fmt.Sprintf("https://s3.example.com/part-%d", i+1)
		if url != expected {
			t.Errorf("PUT[%d] URL = %q, want %q", i, url, expected)
		}
	}

	// Verify sizes: 10MB, 10MB, 5MB
	if putSizes[0] != 10*1024*1024 {
		t.Errorf("PUT[0] size = %d, want %d", putSizes[0], 10*1024*1024)
	}
	if putSizes[1] != 10*1024*1024 {
		t.Errorf("PUT[1] size = %d, want %d", putSizes[1], 10*1024*1024)
	}
	if putSizes[2] != 5*1024*1024 {
		t.Errorf("PUT[2] size = %d, want %d", putSizes[2], 5*1024*1024)
	}
}

func TestPresignedUpload_LargeBody_CallsCompleteWithAllParts(t *testing.T) {
	signedCallCount := 0
	var capturedCompleteBody []byte

	signedClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			signedCallCount++
			if signedCallCount == 1 {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader([]byte(allocateMultipartSuccessResponse("blob-large", makePartURLs(100))))),
				}, nil
			}
			capturedCompleteBody, _ = io.ReadAll(req.Body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte(completeSuccessResponse()))),
			}, nil
		},
	}
	partPutCount := 0
	plainClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			partPutCount++
			_, _ = io.ReadAll(req.Body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Etag": []string{fmt.Sprintf(`"etag-%d"`, partPutCount)}},
				Body:       http.NoBody,
			}, nil
		},
	}

	client := NewPresignedUploadClient("https://api.example.com", signedClient, plainClient)
	content := make([]byte, 25*1024*1024)
	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "application/octet-stream", bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Upload error = %v, want nil", err)
	}

	var jmapReq map[string]any
	if err := json.Unmarshal(capturedCompleteBody, &jmapReq); err != nil {
		t.Fatalf("Failed to parse complete request: %v", err)
	}

	calls := jmapReq["methodCalls"].([]any)
	args := calls[0].([]any)[1].(map[string]any)
	parts := args["parts"].([]any)
	if len(parts) != 3 {
		t.Fatalf("parts count = %d, want 3", len(parts))
	}

	for i, p := range parts {
		part := p.(map[string]any)
		expectedPN := float64(i + 1)
		if part["partNumber"].(float64) != expectedPN {
			t.Errorf("part[%d] partNumber = %v, want %v", i, part["partNumber"], expectedPN)
		}
		expectedETag := fmt.Sprintf(`"etag-%d"`, i+1)
		if part["etag"].(string) != expectedETag {
			t.Errorf("part[%d] etag = %v, want %v", i, part["etag"], expectedETag)
		}
	}
}

func TestPresignedUpload_ExactPartSize_TwoParts(t *testing.T) {
	signed, _ := newMultipartMocks("blob-exact", 100)

	var putSizes []int
	plainClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body)
			putSizes = append(putSizes, len(body))
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Etag": []string{`"etag-x"`}},
				Body:       http.NoBody,
			}, nil
		},
	}

	client := NewPresignedUploadClient("https://api.example.com", signed, plainClient)
	// Exactly 20MB → 2 parts of 10MB each
	content := make([]byte, 20*1024*1024)
	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "application/octet-stream", bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Upload error = %v, want nil", err)
	}

	if len(putSizes) != 2 {
		t.Fatalf("PUT count = %d, want 2", len(putSizes))
	}
	if putSizes[0] != 10*1024*1024 {
		t.Errorf("PUT[0] size = %d, want %d", putSizes[0], 10*1024*1024)
	}
	if putSizes[1] != 10*1024*1024 {
		t.Errorf("PUT[1] size = %d, want %d", putSizes[1], 10*1024*1024)
	}
}

func TestPresignedUpload_CapturesETagFromResponse(t *testing.T) {
	signedCallCount := 0
	var capturedCompleteBody []byte

	signedClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			signedCallCount++
			if signedCallCount == 1 {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader([]byte(allocateMultipartSuccessResponse("blob-etag", makePartURLs(100))))),
				}, nil
			}
			capturedCompleteBody, _ = io.ReadAll(req.Body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte(completeSuccessResponse()))),
			}, nil
		},
	}
	plainClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			_, _ = io.ReadAll(req.Body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Etag": []string{`"specific-etag-value"`}},
				Body:       http.NoBody,
			}, nil
		},
	}

	client := NewPresignedUploadClient("https://api.example.com", signedClient, plainClient)
	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("Upload error = %v, want nil", err)
	}

	var jmapReq map[string]any
	if err := json.Unmarshal(capturedCompleteBody, &jmapReq); err != nil {
		t.Fatalf("Failed to parse complete request: %v", err)
	}

	calls := jmapReq["methodCalls"].([]any)
	args := calls[0].([]any)[1].(map[string]any)
	parts := args["parts"].([]any)
	part := parts[0].(map[string]any)
	if etag := part["etag"].(string); etag != `"specific-etag-value"` {
		t.Errorf("etag = %q, want %q", etag, `"specific-etag-value"`)
	}
}

func TestPresignedUpload_PartPutFailure_ReturnsError(t *testing.T) {
	signed, _ := newMultipartMocks("blob-fail", 100)

	plainClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			_, _ = io.ReadAll(req.Body)
			return &http.Response{
				StatusCode: http.StatusForbidden,
				Body:       io.NopCloser(bytes.NewReader([]byte("Access Denied"))),
			}, nil
		},
	}

	client := NewPresignedUploadClient("https://api.example.com", signed, plainClient)
	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("test")))
	if err == nil {
		t.Fatal("Upload should return error for failed part PUT")
	}
	if !errors.Is(err, ErrServerFail) {
		t.Errorf("error = %v, want ErrServerFail", err)
	}
}

func TestPresignedUpload_PartPutMissingETag_ReturnsError(t *testing.T) {
	signed, _ := newMultipartMocks("blob-noetag", 100)

	plainClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			_, _ = io.ReadAll(req.Body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       http.NoBody,
				// No ETag header
			}, nil
		},
	}

	client := NewPresignedUploadClient("https://api.example.com", signed, plainClient)
	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("test")))
	if err == nil {
		t.Fatal("Upload should return error for missing ETag")
	}
	if !errors.Is(err, ErrInvalidResponse) {
		t.Errorf("error = %v, want ErrInvalidResponse", err)
	}
}

func TestPresignedUpload_CompleteFailure_ReturnsError(t *testing.T) {
	signedCallCount := 0
	signedClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			signedCallCount++
			if signedCallCount == 1 {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader([]byte(allocateMultipartSuccessResponse("blob-fail", makePartURLs(100))))),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(bytes.NewReader([]byte("Internal Server Error"))),
			}, nil
		},
	}
	plainClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			_, _ = io.ReadAll(req.Body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Etag": []string{`"etag-1"`}},
				Body:       http.NoBody,
			}, nil
		},
	}

	client := NewPresignedUploadClient("https://api.example.com", signedClient, plainClient)
	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("test")))
	if err == nil {
		t.Fatal("Upload should return error for failed complete")
	}
	if !errors.Is(err, ErrServerFail) {
		t.Errorf("error = %v, want ErrServerFail", err)
	}
}

func TestPresignedUpload_CompleteJMAPError_ReturnsError(t *testing.T) {
	jmapErrorResp := `{"methodResponses":[["error",{"type":"serverFail","description":"complete failed"},"c0"]]}`
	signedCallCount := 0
	signedClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			signedCallCount++
			if signedCallCount == 1 {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader([]byte(allocateMultipartSuccessResponse("blob-fail", makePartURLs(100))))),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte(jmapErrorResp))),
			}, nil
		},
	}
	plainClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			_, _ = io.ReadAll(req.Body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Etag": []string{`"etag-1"`}},
				Body:       http.NoBody,
			}, nil
		},
	}

	client := NewPresignedUploadClient("https://api.example.com", signedClient, plainClient)
	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("test")))
	if err == nil {
		t.Fatal("Upload should return error for JMAP error from complete")
	}
	if !errors.Is(err, ErrServerFail) {
		t.Errorf("error = %v, want ErrServerFail", err)
	}
}

func TestPresignedUpload_AllocateEmptyParts_ReturnsError(t *testing.T) {
	// Response with empty parts array
	emptyPartsResp := `{"methodResponses":[["Blob/allocate",{"accountId":"user-123","created":{"c0":{"id":"blob-empty","parts":[]}}},"c0"]]}`
	signedClient := &fakeHTTPDoer{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte(emptyPartsResp))),
			}, nil
		},
	}
	plainClient := &fakeHTTPDoer{}

	client := NewPresignedUploadClient("https://api.example.com", signedClient, plainClient)
	_, _, err := client.Upload(context.Background(), "user-123", "parent-blob", "text/plain", bytes.NewReader([]byte("test")))
	if err == nil {
		t.Fatal("Upload should return error for empty parts")
	}
	if !errors.Is(err, ErrInvalidResponse) {
		t.Errorf("error = %v, want ErrInvalidResponse", err)
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

func TestPresignedUpload_CreatesSpan(t *testing.T) {
	recorder := setupTestTracer(t)

	signed, plain := newMultipartMocks("blob-new", 100)

	client := NewPresignedUploadClient("https://api.example.com", signed, plain)
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
