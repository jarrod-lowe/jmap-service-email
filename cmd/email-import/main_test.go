package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/blob"
)

// Simple test email content
const testEmailContent = `From: Alice <alice@example.com>
To: Bob <bob@example.com>
Subject: Test Email
Date: Sat, 20 Jan 2024 10:00:00 +0000
Message-ID: <test-msg@example.com>

This is a test email body.
`

// mockBlobStreamer implements the BlobStreamer interface for testing.
type mockBlobStreamer struct {
	streamFunc func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error)
}

func (m *mockBlobStreamer) Stream(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
	if m.streamFunc != nil {
		return m.streamFunc(ctx, accountID, blobID)
	}
	return io.NopCloser(strings.NewReader(testEmailContent)), nil
}

// mockBlobUploader implements the BlobUploader interface for testing.
type mockBlobUploader struct {
	uploadFunc func(ctx context.Context, accountID, parentBlobID, contentType string, body io.Reader) (string, int64, error)
}

func (m *mockBlobUploader) Upload(ctx context.Context, accountID, parentBlobID, contentType string, body io.Reader) (string, int64, error) {
	if m.uploadFunc != nil {
		return m.uploadFunc(ctx, accountID, parentBlobID, contentType, body)
	}
	// Default: consume the body and return a mock blob ID
	content, _ := io.ReadAll(body)
	return "uploaded-blob-id", int64(len(content)), nil
}

// mockEmailRepository implements the EmailRepository interface for testing.
type mockEmailRepository struct {
	createFunc func(ctx context.Context, email *emailItem) error
}

func (m *mockEmailRepository) CreateEmail(ctx context.Context, email *emailItem) error {
	if m.createFunc != nil {
		return m.createFunc(ctx, email)
	}
	return nil
}

func TestHandler_SingleEmailImport(t *testing.T) {
	var capturedEmail *emailItem
	mockRepo := &mockEmailRepository{
		createFunc: func(ctx context.Context, email *emailItem) error {
			capturedEmail = email
			return nil
		},
	}
	mockStreamer := &mockBlobStreamer{}
	mockUploader := &mockBlobUploader{}

	h := newHandler(mockRepo, mockStreamer, mockUploader)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId": "blob-456",
					"mailboxIds": map[string]any{
						"inbox-id": true,
					},
					"keywords": map[string]any{
						"$seen": true,
					},
					"receivedAt": "2024-01-20T10:00:00Z",
				},
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should return Email/import response
	if response.MethodResponse.Name != "Email/import" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/import")
	}

	// Should have accountId in args
	accountID, ok := response.MethodResponse.Args["accountId"].(string)
	if !ok || accountID != "user-123" {
		t.Errorf("accountId = %v, want %q", response.MethodResponse.Args["accountId"], "user-123")
	}

	// Should have created entry
	created, ok := response.MethodResponse.Args["created"].(map[string]any)
	if !ok {
		t.Fatal("created should be a map")
	}

	createdEmail, ok := created["client-ref-1"].(map[string]any)
	if !ok {
		t.Fatal("created should contain client-ref-1")
	}

	// Should have id in created email
	if _, ok := createdEmail["id"].(string); !ok {
		t.Error("created email should have id")
	}

	// Should have blobId in created email
	if blobID, ok := createdEmail["blobId"].(string); !ok || blobID != "blob-456" {
		t.Errorf("blobId = %v, want %q", createdEmail["blobId"], "blob-456")
	}

	// Verify repository was called with correct data
	if capturedEmail == nil {
		t.Fatal("CreateEmail was not called")
	}
	if capturedEmail.AccountID != "user-123" {
		t.Errorf("AccountID = %q, want %q", capturedEmail.AccountID, "user-123")
	}
	if capturedEmail.BlobID != "blob-456" {
		t.Errorf("BlobID = %q, want %q", capturedEmail.BlobID, "blob-456")
	}
	if capturedEmail.Subject != "Test Email" {
		t.Errorf("Subject = %q, want %q", capturedEmail.Subject, "Test Email")
	}
}

func TestHandler_BlobStreamError(t *testing.T) {
	mockRepo := &mockEmailRepository{}
	mockStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return nil, blob.ErrBlobNotFound
		},
	}
	mockUploader := &mockBlobUploader{}

	h := newHandler(mockRepo, mockStreamer, mockUploader)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId": "blob-456",
					"mailboxIds": map[string]any{
						"inbox-id": true,
					},
				},
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should have notCreated entry
	notCreated, ok := response.MethodResponse.Args["notCreated"].(map[string]any)
	if !ok {
		t.Fatal("notCreated should be a map")
	}

	errorEntry, ok := notCreated["client-ref-1"].(map[string]any)
	if !ok {
		t.Fatal("notCreated should contain client-ref-1")
	}

	// Error type should be blobNotFound
	if errorType, ok := errorEntry["type"].(string); !ok || errorType != "blobNotFound" {
		t.Errorf("error type = %v, want %q", errorEntry["type"], "blobNotFound")
	}
}

func TestHandler_RepositoryError(t *testing.T) {
	mockRepo := &mockEmailRepository{
		createFunc: func(ctx context.Context, email *emailItem) error {
			return errors.New("database error")
		},
	}
	mockStreamer := &mockBlobStreamer{}
	mockUploader := &mockBlobUploader{}

	h := newHandler(mockRepo, mockStreamer, mockUploader)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId": "blob-456",
					"mailboxIds": map[string]any{
						"inbox-id": true,
					},
				},
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should have notCreated entry
	notCreated, ok := response.MethodResponse.Args["notCreated"].(map[string]any)
	if !ok {
		t.Fatal("notCreated should be a map")
	}

	errorEntry, ok := notCreated["client-ref-1"].(map[string]any)
	if !ok {
		t.Fatal("notCreated should contain client-ref-1")
	}

	// Error type should be serverFail
	if errorType, ok := errorEntry["type"].(string); !ok || errorType != "serverFail" {
		t.Errorf("error type = %v, want %q", errorEntry["type"], "serverFail")
	}
}

func TestHandler_MultipleEmails(t *testing.T) {
	createCount := 0
	mockRepo := &mockEmailRepository{
		createFunc: func(ctx context.Context, email *emailItem) error {
			createCount++
			return nil
		},
	}
	mockStreamer := &mockBlobStreamer{}
	mockUploader := &mockBlobUploader{}

	h := newHandler(mockRepo, mockStreamer, mockUploader)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"ref-1": map[string]any{
					"blobId":     "blob-1",
					"mailboxIds": map[string]any{"inbox": true},
				},
				"ref-2": map[string]any{
					"blobId":     "blob-2",
					"mailboxIds": map[string]any{"inbox": true},
				},
				"ref-3": map[string]any{
					"blobId":     "blob-3",
					"mailboxIds": map[string]any{"inbox": true},
				},
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should have created 3 emails
	if createCount != 3 {
		t.Errorf("CreateEmail called %d times, want 3", createCount)
	}

	// Should have created entries for all 3
	created, ok := response.MethodResponse.Args["created"].(map[string]any)
	if !ok {
		t.Fatal("created should be a map")
	}
	if len(created) != 3 {
		t.Errorf("created count = %d, want 3", len(created))
	}
}

func TestHandler_InvalidMethod(t *testing.T) {
	mockRepo := &mockEmailRepository{}
	mockStreamer := &mockBlobStreamer{}
	mockUploader := &mockBlobUploader{}

	h := newHandler(mockRepo, mockStreamer, mockUploader)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args:      map[string]any{},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should return error response
	if response.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "error")
	}

	errorType, ok := response.MethodResponse.Args["type"].(string)
	if !ok || errorType != "unknownMethod" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "unknownMethod")
	}
}

func TestHandler_Base64Attachment_UploadsCalled(t *testing.T) {
	// Email with base64 encoded content
	emailWithBase64 := `From: Alice <alice@example.com>
To: Bob <bob@example.com>
Subject: With Attachment
MIME-Version: 1.0
Content-Type: application/octet-stream; name="test.bin"
Content-Transfer-Encoding: base64
Content-Disposition: attachment; filename="test.bin"

SGVsbG8gV29ybGQ=
`
	uploadCount := 0
	mockRepo := &mockEmailRepository{}
	mockStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(emailWithBase64)), nil
		},
	}
	mockUploader := &mockBlobUploader{
		uploadFunc: func(ctx context.Context, accountID, parentBlobID, contentType string, body io.Reader) (string, int64, error) {
			uploadCount++
			content, _ := io.ReadAll(body)
			return "decoded-blob", int64(len(content)), nil
		},
	}

	h := newHandler(mockRepo, mockStreamer, mockUploader)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId": "email-blob-123",
					"mailboxIds": map[string]any{
						"inbox-id": true,
					},
				},
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should succeed
	if response.MethodResponse.Name != "Email/import" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/import")
	}

	// Should have uploaded the decoded content
	if uploadCount != 1 {
		t.Errorf("uploadCount = %d, want 1", uploadCount)
	}
}
