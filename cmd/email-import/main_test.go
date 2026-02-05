package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jarrod-lowe/jmap-service-libs/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/blob"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailbox"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
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
	createFunc               func(ctx context.Context, email *emailItem) error
	findByMessageIDFunc      func(ctx context.Context, accountID, messageID string) (*emailItem, error)
	buildCreateEmailItemsFunc func(email *emailItem) []types.TransactWriteItem
}

func (m *mockEmailRepository) CreateEmail(ctx context.Context, email *emailItem) error {
	if m.createFunc != nil {
		return m.createFunc(ctx, email)
	}
	return nil
}

func (m *mockEmailRepository) FindByMessageID(ctx context.Context, accountID, messageID string) (*emailItem, error) {
	if m.findByMessageIDFunc != nil {
		return m.findByMessageIDFunc(ctx, accountID, messageID)
	}
	return nil, nil
}

func (m *mockEmailRepository) BuildCreateEmailItems(email *emailItem) []types.TransactWriteItem {
	if m.buildCreateEmailItemsFunc != nil {
		return m.buildCreateEmailItemsFunc(email)
	}
	// Default: return 2 items (email + 1 membership for simplicity)
	return []types.TransactWriteItem{
		{Put: &types.Put{}}, // Email item
		{Put: &types.Put{}}, // Membership item
	}
}

// mockMailboxRepository implements the MailboxRepository interface for testing.
type mockMailboxRepository struct {
	existsFunc                 func(ctx context.Context, accountID, mailboxID string) (bool, error)
	incrementCountFunc         func(ctx context.Context, accountID, mailboxID string, incrementUnread bool) error
	buildIncrementCountsItemsFunc func(accountID, mailboxID string, incrementUnread bool) types.TransactWriteItem
}

func (m *mockMailboxRepository) MailboxExists(ctx context.Context, accountID, mailboxID string) (bool, error) {
	if m.existsFunc != nil {
		return m.existsFunc(ctx, accountID, mailboxID)
	}
	return true, nil
}

func (m *mockMailboxRepository) IncrementCounts(ctx context.Context, accountID, mailboxID string, incrementUnread bool) error {
	if m.incrementCountFunc != nil {
		return m.incrementCountFunc(ctx, accountID, mailboxID, incrementUnread)
	}
	return nil
}

func (m *mockMailboxRepository) BuildIncrementCountsItems(accountID, mailboxID string, incrementUnread bool) types.TransactWriteItem {
	if m.buildIncrementCountsItemsFunc != nil {
		return m.buildIncrementCountsItemsFunc(accountID, mailboxID, incrementUnread)
	}
	// Default: return an Update item
	return types.TransactWriteItem{Update: &types.Update{}}
}

// mockStateRepository implements the StateRepository interface for testing.
type mockStateRepository struct {
	incrementFunc           func(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error)
	getCurrentStateFunc     func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
	buildStateChangeItemsFunc func(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem)
	buildStateChangeItemsMultiFunc func(accountID string, objectType state.ObjectType, currentState int64, objectIDs []string, changeType state.ChangeType) (int64, []types.TransactWriteItem)
}

func (m *mockStateRepository) IncrementStateAndLogChange(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error) {
	if m.incrementFunc != nil {
		return m.incrementFunc(ctx, accountID, objectType, objectID, changeType)
	}
	return 1, nil
}

func (m *mockStateRepository) GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
	if m.getCurrentStateFunc != nil {
		return m.getCurrentStateFunc(ctx, accountID, objectType)
	}
	return 0, nil
}

func (m *mockStateRepository) BuildStateChangeItems(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem) {
	if m.buildStateChangeItemsFunc != nil {
		return m.buildStateChangeItemsFunc(accountID, objectType, currentState, objectID, changeType)
	}
	// Default: return state+1 and 2 items (state update + change log)
	newState := currentState + 1
	return newState, []types.TransactWriteItem{
		{Update: &types.Update{}}, // State update
		{Put: &types.Put{}},       // Change log
	}
}

func (m *mockStateRepository) BuildStateChangeItemsMulti(accountID string, objectType state.ObjectType, currentState int64, objectIDs []string, changeType state.ChangeType) (int64, []types.TransactWriteItem) {
	if m.buildStateChangeItemsMultiFunc != nil {
		return m.buildStateChangeItemsMultiFunc(accountID, objectType, currentState, objectIDs, changeType)
	}
	n := int64(len(objectIDs))
	items := make([]types.TransactWriteItem, 0, n+1)
	if n > 0 {
		items = append(items, types.TransactWriteItem{Update: &types.Update{}})
		for range objectIDs {
			items = append(items, types.TransactWriteItem{Put: &types.Put{}})
		}
	}
	return currentState + n, items
}

// mockBlobDeletePublisher implements BlobDeletePublisher for testing.
type mockBlobDeletePublisher struct {
	publishFunc func(ctx context.Context, accountID string, blobIDs []string, apiURL string) error
}

func (m *mockBlobDeletePublisher) PublishBlobDeletions(ctx context.Context, accountID string, blobIDs []string, apiURL string) error {
	if m.publishFunc != nil {
		return m.publishFunc(ctx, accountID, blobIDs, apiURL)
	}
	return nil
}

// mockBlobClientFactory wraps mock streamer and uploader in a factory for testing.
func mockBlobClientFactory(s BlobStreamer, u BlobUploader) func(string) (BlobStreamer, BlobUploader) {
	return func(_ string) (BlobStreamer, BlobUploader) { return s, u }
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
	mockMailboxRepo := &mockMailboxRepository{} // Default: mailbox exists

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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
	// Version should be 1 for newly imported emails
	if capturedEmail.Version != 1 {
		t.Errorf("Version = %d, want 1", capturedEmail.Version)
	}
}

func TestHandler_SingleEmailImport_VerifiesFromField(t *testing.T) {
	var capturedEmail *emailItem
	mockRepo := &mockEmailRepository{
		createFunc: func(ctx context.Context, email *emailItem) error {
			capturedEmail = email
			return nil
		},
	}
	mockStreamer := &mockBlobStreamer{}
	mockUploader := &mockBlobUploader{}
	mockMailboxRepo := &mockMailboxRepository{} // Default: mailbox exists

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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

	if response.MethodResponse.Name != "Email/import" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/import")
	}

	// Verify From field was parsed and stored
	if capturedEmail == nil {
		t.Fatal("CreateEmail was not called")
	}
	if len(capturedEmail.From) != 1 {
		t.Fatalf("From length = %d, want 1; From=%v", len(capturedEmail.From), capturedEmail.From)
	}
	if capturedEmail.From[0].Name != "Alice" {
		t.Errorf("From[0].Name = %q, want %q", capturedEmail.From[0].Name, "Alice")
	}
	if capturedEmail.From[0].Email != "alice@example.com" {
		t.Errorf("From[0].Email = %q, want %q", capturedEmail.From[0].Email, "alice@example.com")
	}

	// Also verify To for comparison
	if len(capturedEmail.To) != 1 {
		t.Fatalf("To length = %d, want 1; To=%v", len(capturedEmail.To), capturedEmail.To)
	}
	if capturedEmail.To[0].Email != "bob@example.com" {
		t.Errorf("To[0].Email = %q, want %q", capturedEmail.To[0].Email, "bob@example.com")
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
	mockMailboxRepo := &mockMailboxRepository{} // Default: mailbox exists

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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
	mockMailboxRepo := &mockMailboxRepository{} // Default: mailbox exists

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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
	mockMailboxRepo := &mockMailboxRepository{} // Default: mailbox exists

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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
	mockMailboxRepo := &mockMailboxRepository{} // Default: mailbox exists

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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
	mockMailboxRepo := &mockMailboxRepository{} // Default: mailbox exists

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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

// Test: Import with invalid mailbox ID returns invalidMailboxId error
func TestHandler_InvalidMailboxId(t *testing.T) {
	mockRepo := &mockEmailRepository{}
	mockStreamer := &mockBlobStreamer{}
	mockUploader := &mockBlobUploader{}
	mockMailboxRepo := &mockMailboxRepository{
		existsFunc: func(ctx context.Context, accountID, mailboxID string) (bool, error) {
			return false, nil // Mailbox does not exist
		},
	}

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId": "blob-456",
					"mailboxIds": map[string]any{
						"nonexistent-mailbox": true,
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

	// Error type should be invalidMailboxId
	if errorType, ok := errorEntry["type"].(string); !ok || errorType != "invalidMailboxId" {
		t.Errorf("error type = %v, want %q", errorEntry["type"], "invalidMailboxId")
	}
}

// Test: Import with valid mailbox ID succeeds and increments counts
func TestHandler_ValidMailboxIdIncrementsCounts(t *testing.T) {
	var incrementCalls []struct {
		mailboxID       string
		incrementUnread bool
	}

	mockRepo := &mockEmailRepository{}
	mockStreamer := &mockBlobStreamer{}
	mockUploader := &mockBlobUploader{}
	mockMailboxRepo := &mockMailboxRepository{
		existsFunc: func(ctx context.Context, accountID, mailboxID string) (bool, error) {
			return true, nil
		},
		incrementCountFunc: func(ctx context.Context, accountID, mailboxID string, incrementUnread bool) error {
			incrementCalls = append(incrementCalls, struct {
				mailboxID       string
				incrementUnread bool
			}{mailboxID, incrementUnread})
			return nil
		},
	}

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId": "blob-456",
					"mailboxIds": map[string]any{
						"inbox": true,
					},
					"keywords": map[string]any{
						"$seen": true,
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
	created, ok := response.MethodResponse.Args["created"].(map[string]any)
	if !ok {
		t.Fatal("created should be a map")
	}
	if _, ok := created["client-ref-1"]; !ok {
		t.Fatal("created should contain client-ref-1")
	}

	// Should have incremented counts for inbox
	if len(incrementCalls) != 1 {
		t.Errorf("IncrementCounts called %d times, want 1", len(incrementCalls))
	}
	if len(incrementCalls) > 0 {
		if incrementCalls[0].mailboxID != "inbox" {
			t.Errorf("mailboxID = %q, want %q", incrementCalls[0].mailboxID, "inbox")
		}
		// $seen is present, so should NOT increment unread
		if incrementCalls[0].incrementUnread != false {
			t.Error("incrementUnread should be false when $seen is present")
		}
	}
}

// Test: Import without $seen keyword increments unread count
func TestHandler_NoSeenKeywordIncrementsUnread(t *testing.T) {
	var incrementCalls []struct {
		mailboxID       string
		incrementUnread bool
	}

	mockRepo := &mockEmailRepository{}
	mockStreamer := &mockBlobStreamer{}
	mockUploader := &mockBlobUploader{}
	mockMailboxRepo := &mockMailboxRepository{
		existsFunc: func(ctx context.Context, accountID, mailboxID string) (bool, error) {
			return true, nil
		},
		incrementCountFunc: func(ctx context.Context, accountID, mailboxID string, incrementUnread bool) error {
			incrementCalls = append(incrementCalls, struct {
				mailboxID       string
				incrementUnread bool
			}{mailboxID, incrementUnread})
			return nil
		},
	}

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId": "blob-456",
					"mailboxIds": map[string]any{
						"inbox": true,
					},
					// No $seen keyword
				},
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should succeed
	created, ok := response.MethodResponse.Args["created"].(map[string]any)
	if !ok {
		t.Fatal("created should be a map")
	}
	if _, ok := created["client-ref-1"]; !ok {
		t.Fatal("created should contain client-ref-1")
	}

	// Should have incremented counts for inbox with unread
	if len(incrementCalls) != 1 {
		t.Errorf("IncrementCounts called %d times, want 1", len(incrementCalls))
	}
	if len(incrementCalls) > 0 {
		// No $seen, so should increment unread
		if incrementCalls[0].incrementUnread != true {
			t.Error("incrementUnread should be true when $seen is not present")
		}
	}
}

// Test: Import with multiple mailboxes increments all
func TestHandler_MultipleMailboxesIncrementAll(t *testing.T) {
	var incrementCalls []string

	mockRepo := &mockEmailRepository{}
	mockStreamer := &mockBlobStreamer{}
	mockUploader := &mockBlobUploader{}
	mockMailboxRepo := &mockMailboxRepository{
		existsFunc: func(ctx context.Context, accountID, mailboxID string) (bool, error) {
			return true, nil
		},
		incrementCountFunc: func(ctx context.Context, accountID, mailboxID string, incrementUnread bool) error {
			incrementCalls = append(incrementCalls, mailboxID)
			return nil
		},
	}

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId": "blob-456",
					"mailboxIds": map[string]any{
						"inbox":   true,
						"archive": true,
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
	created, ok := response.MethodResponse.Args["created"].(map[string]any)
	if !ok {
		t.Fatal("created should be a map")
	}
	if _, ok := created["client-ref-1"]; !ok {
		t.Fatal("created should contain client-ref-1")
	}

	// Should have incremented counts for both mailboxes
	if len(incrementCalls) != 2 {
		t.Errorf("IncrementCounts called %d times, want 2", len(incrementCalls))
	}
}

// Test: Mailbox check error returns serverFail
func TestHandler_MailboxCheckError(t *testing.T) {
	mockRepo := &mockEmailRepository{}
	mockStreamer := &mockBlobStreamer{}
	mockUploader := &mockBlobUploader{}
	mockMailboxRepo := &mockMailboxRepository{
		existsFunc: func(ctx context.Context, accountID, mailboxID string) (bool, error) {
			return false, errors.New("database error")
		},
	}

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId": "blob-456",
					"mailboxIds": map[string]any{
						"inbox": true,
					},
				},
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	notCreated, ok := response.MethodResponse.Args["notCreated"].(map[string]any)
	if !ok {
		t.Fatal("notCreated should be a map")
	}

	errorEntry, ok := notCreated["client-ref-1"].(map[string]any)
	if !ok {
		t.Fatal("notCreated should contain client-ref-1")
	}

	if errorType, ok := errorEntry["type"].(string); !ok || errorType != "serverFail" {
		t.Errorf("error type = %v, want %q", errorEntry["type"], "serverFail")
	}
}

// Test: Increment count error still succeeds (non-fatal)
func TestHandler_IncrementCountError(t *testing.T) {
	mockRepo := &mockEmailRepository{}
	mockStreamer := &mockBlobStreamer{}
	mockUploader := &mockBlobUploader{}
	mockMailboxRepo := &mockMailboxRepository{
		existsFunc: func(ctx context.Context, accountID, mailboxID string) (bool, error) {
			return true, nil
		},
		incrementCountFunc: func(ctx context.Context, accountID, mailboxID string, incrementUnread bool) error {
			return mailbox.ErrMailboxNotFound // Mailbox was deleted after check
		},
	}

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId": "blob-456",
					"mailboxIds": map[string]any{
						"inbox": true,
					},
				},
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should still succeed - increment error is logged but not fatal
	created, ok := response.MethodResponse.Args["created"].(map[string]any)
	if !ok {
		t.Fatal("created should be a map")
	}
	if _, ok := created["client-ref-1"]; !ok {
		t.Fatal("created should contain client-ref-1 even when increment fails")
	}
}

// Email without In-Reply-To header
const testEmailNoReply = `From: Alice <alice@example.com>
To: Bob <bob@example.com>
Subject: New Thread
Date: Sat, 20 Jan 2024 10:00:00 +0000
Message-ID: <new-msg@example.com>

This starts a new thread.
`

// Email with In-Reply-To header
const testEmailWithReply = `From: Bob <bob@example.com>
To: Alice <alice@example.com>
Subject: Re: New Thread
Date: Sat, 20 Jan 2024 11:00:00 +0000
Message-ID: <reply-msg@example.com>
In-Reply-To: <parent-msg@example.com>

This is a reply.
`

// Test: Email without In-Reply-To gets new threadId
func TestHandler_Threading_NoInReplyTo_NewThread(t *testing.T) {
	var capturedEmail *emailItem
	mockRepo := &mockEmailRepository{
		createFunc: func(ctx context.Context, email *emailItem) error {
			capturedEmail = email
			return nil
		},
		findByMessageIDFunc: func(ctx context.Context, accountID, messageID string) (*emailItem, error) {
			t.Error("FindByMessageID should not be called when there's no In-Reply-To")
			return nil, nil
		},
	}
	mockStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(testEmailNoReply)), nil
		},
	}
	mockUploader := &mockBlobUploader{}
	mockMailboxRepo := &mockMailboxRepository{}

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId":     "blob-456",
					"mailboxIds": map[string]any{"inbox": true},
				},
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Email/import" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/import")
	}

	// Verify email was created with a threadId
	if capturedEmail == nil {
		t.Fatal("CreateEmail was not called")
	}
	if capturedEmail.ThreadID == "" {
		t.Error("ThreadID should not be empty")
	}
	// ThreadID should equal EmailID when no parent exists (our implementation)
	if capturedEmail.ThreadID != capturedEmail.EmailID {
		t.Errorf("ThreadID = %q, expected to match EmailID %q for new thread", capturedEmail.ThreadID, capturedEmail.EmailID)
	}
}

// Test: Email with In-Reply-To finds parent and inherits threadId
func TestHandler_Threading_InReplyTo_InheritsThread(t *testing.T) {
	parentThreadID := "parent-thread-123"
	var capturedEmail *emailItem

	mockRepo := &mockEmailRepository{
		createFunc: func(ctx context.Context, email *emailItem) error {
			capturedEmail = email
			return nil
		},
		findByMessageIDFunc: func(ctx context.Context, accountID, messageID string) (*emailItem, error) {
			if messageID == "<parent-msg@example.com>" {
				return &emailItem{
					EmailID:  "parent-email-id",
					ThreadID: parentThreadID,
				}, nil
			}
			return nil, nil
		},
	}
	mockStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(testEmailWithReply)), nil
		},
	}
	mockUploader := &mockBlobUploader{}
	mockMailboxRepo := &mockMailboxRepository{}

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId":     "blob-456",
					"mailboxIds": map[string]any{"inbox": true},
				},
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Email/import" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/import")
	}

	// Verify email inherited parent's threadId
	if capturedEmail == nil {
		t.Fatal("CreateEmail was not called")
	}
	if capturedEmail.ThreadID != parentThreadID {
		t.Errorf("ThreadID = %q, want %q (inherited from parent)", capturedEmail.ThreadID, parentThreadID)
	}
}

// Test: Email with In-Reply-To but parent not found gets new threadId
func TestHandler_Threading_InReplyTo_ParentNotFound(t *testing.T) {
	var capturedEmail *emailItem

	mockRepo := &mockEmailRepository{
		createFunc: func(ctx context.Context, email *emailItem) error {
			capturedEmail = email
			return nil
		},
		findByMessageIDFunc: func(ctx context.Context, accountID, messageID string) (*emailItem, error) {
			return nil, nil // Parent not found
		},
	}
	mockStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(testEmailWithReply)), nil
		},
	}
	mockUploader := &mockBlobUploader{}
	mockMailboxRepo := &mockMailboxRepository{}

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId":     "blob-456",
					"mailboxIds": map[string]any{"inbox": true},
				},
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Email/import" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/import")
	}

	// Verify email was created with a new threadId
	if capturedEmail == nil {
		t.Fatal("CreateEmail was not called")
	}
	if capturedEmail.ThreadID == "" {
		t.Error("ThreadID should not be empty")
	}
}

// Test: Thread assignment error is non-fatal - falls back to new thread
func TestHandler_Threading_LookupError_FallsBackToNewThread(t *testing.T) {
	var capturedEmail *emailItem

	mockRepo := &mockEmailRepository{
		createFunc: func(ctx context.Context, email *emailItem) error {
			capturedEmail = email
			return nil
		},
		findByMessageIDFunc: func(ctx context.Context, accountID, messageID string) (*emailItem, error) {
			return nil, errors.New("database error")
		},
	}
	mockStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(testEmailWithReply)), nil
		},
	}
	mockUploader := &mockBlobUploader{}
	mockMailboxRepo := &mockMailboxRepository{}

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId":     "blob-456",
					"mailboxIds": map[string]any{"inbox": true},
				},
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should still succeed with a new thread
	if response.MethodResponse.Name != "Email/import" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/import")
	}

	if capturedEmail == nil {
		t.Fatal("CreateEmail was not called")
	}
	if capturedEmail.ThreadID == "" {
		t.Error("ThreadID should not be empty even when lookup fails")
	}
}

func TestHandler_StateTracking(t *testing.T) {
	mockRepo := &mockEmailRepository{
		createFunc: func(ctx context.Context, email *emailItem) error {
			return nil
		},
	}
	mockStreamer := &mockBlobStreamer{}
	mockUploader := &mockBlobUploader{}
	mockMailboxRepo := &mockMailboxRepository{}

	// Track state changes
	var stateChanges []struct {
		objectType state.ObjectType
		objectID   string
		changeType state.ChangeType
	}
	mockStateRepo := &mockStateRepository{
		incrementFunc: func(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error) {
			stateChanges = append(stateChanges, struct {
				objectType state.ObjectType
				objectID   string
				changeType state.ChangeType
			}{objectType, objectID, changeType})
			return 1, nil
		},
	}

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, mockStateRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId": "blob-456",
					"mailboxIds": map[string]any{
						"inbox":   true,
						"archive": true,
					},
				},
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Email/import" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/import")
	}

	// Should have 4 state changes: 1 Email created + 1 Thread updated + 2 Mailboxes updated
	if len(stateChanges) != 4 {
		t.Fatalf("stateChanges count = %d, want 4", len(stateChanges))
	}

	// First should be Email created
	emailCreated := false
	for _, change := range stateChanges {
		if change.objectType == state.ObjectTypeEmail && change.changeType == state.ChangeTypeCreated {
			emailCreated = true
			break
		}
	}
	if !emailCreated {
		t.Error("Expected Email/created state change")
	}

	// Should have 2 Mailbox updated changes
	mailboxUpdates := 0
	for _, change := range stateChanges {
		if change.objectType == state.ObjectTypeMailbox && change.changeType == state.ChangeTypeUpdated {
			mailboxUpdates++
		}
	}
	if mailboxUpdates != 2 {
		t.Errorf("mailbox update count = %d, want 2", mailboxUpdates)
	}
}

func TestHandler_StateTracking_ThreadUpdated_ForReply(t *testing.T) {
	parentThreadID := "existing-thread-123"

	mockRepo := &mockEmailRepository{
		createFunc: func(ctx context.Context, email *emailItem) error {
			return nil
		},
		findByMessageIDFunc: func(ctx context.Context, accountID, messageID string) (*emailItem, error) {
			// Parent email found - reply joins existing thread
			if messageID == "<parent-msg@example.com>" {
				return &emailItem{
					EmailID:  "parent-email-id",
					ThreadID: parentThreadID,
				}, nil
			}
			return nil, nil
		},
	}
	// Use email with In-Reply-To to join existing thread
	mockStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(testEmailWithReply)), nil
		},
	}
	mockUploader := &mockBlobUploader{}
	mockMailboxRepo := &mockMailboxRepository{}

	// Track state changes
	var stateChanges []struct {
		objectType state.ObjectType
		objectID   string
		changeType state.ChangeType
	}
	mockStateRepo := &mockStateRepository{
		incrementFunc: func(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error) {
			stateChanges = append(stateChanges, struct {
				objectType state.ObjectType
				objectID   string
				changeType state.ChangeType
			}{objectType, objectID, changeType})
			return 1, nil
		},
	}

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, mockStateRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId": "blob-456",
					"mailboxIds": map[string]any{
						"inbox": true,
					},
				},
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Email/import" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/import")
	}

	// Should have Thread updated (not created) since email joins existing thread
	threadUpdated := false
	var threadChange struct {
		objectType state.ObjectType
		objectID   string
		changeType state.ChangeType
	}
	for _, change := range stateChanges {
		if change.objectType == state.ObjectTypeThread {
			threadChange = change
			if change.changeType == state.ChangeTypeUpdated {
				threadUpdated = true
			}
			break
		}
	}
	if !threadUpdated {
		t.Errorf("Expected Thread/updated state change for reply, got changes: %v", stateChanges)
	}
	if threadChange.objectID != parentThreadID {
		t.Errorf("Thread objectID = %q, want %q (parent thread)", threadChange.objectID, parentThreadID)
	}
}

func TestHandler_SingleEmailImport_StoresHeaderSize(t *testing.T) {
	var capturedEmail *emailItem
	mockRepo := &mockEmailRepository{
		createFunc: func(ctx context.Context, email *emailItem) error {
			capturedEmail = email
			return nil
		},
	}
	mockStreamer := &mockBlobStreamer{}
	mockUploader := &mockBlobUploader{}
	mockMailboxRepo := &mockMailboxRepository{}

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{})

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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

	if response.MethodResponse.Name != "Email/import" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/import")
	}

	// Verify HeaderSize was stored (testEmailContent headers end at position 133)
	if capturedEmail == nil {
		t.Fatal("CreateEmail was not called")
	}
	if capturedEmail.HeaderSize == 0 {
		t.Error("HeaderSize should not be zero for a valid email")
	}
	// testEmailContent has headers ending before "This is a test email body."
	// Headers: From, To, Subject, Date, Message-ID, blank line
	if capturedEmail.HeaderSize < 100 {
		t.Errorf("HeaderSize = %d, expected > 100 for testEmailContent", capturedEmail.HeaderSize)
	}
}

func TestHandler_StateTracking_IncludesThread(t *testing.T) {
	mockRepo := &mockEmailRepository{
		createFunc: func(ctx context.Context, email *emailItem) error {
			return nil
		},
	}
	// Use email without In-Reply-To to create a new thread
	mockStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(testEmailNoReply)), nil
		},
	}
	mockUploader := &mockBlobUploader{}
	mockMailboxRepo := &mockMailboxRepository{}

	// Track state changes
	var stateChanges []struct {
		objectType state.ObjectType
		objectID   string
		changeType state.ChangeType
	}
	mockStateRepo := &mockStateRepository{
		incrementFunc: func(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error) {
			stateChanges = append(stateChanges, struct {
				objectType state.ObjectType
				objectID   string
				changeType state.ChangeType
			}{objectType, objectID, changeType})
			return 1, nil
		},
	}

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, mockStateRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId": "blob-456",
					"mailboxIds": map[string]any{
						"inbox": true,
					},
				},
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Email/import" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/import")
	}

	// Should have state changes: Email created, Thread created (new thread), Mailbox updated
	// New threads (no In-Reply-To) should be logged as "created"
	threadCreated := false
	for _, change := range stateChanges {
		if change.objectType == state.ObjectTypeThread && change.changeType == state.ChangeTypeCreated {
			threadCreated = true
			break
		}
	}
	if !threadCreated {
		t.Errorf("Expected Thread/created state change for new thread, got changes: %v", stateChanges)
	}
}

// mockTransactWriter captures transaction writes for verification.
type mockTransactWriter struct {
	transactFunc func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	capturedInput *dynamodb.TransactWriteItemsInput
}

func (m *mockTransactWriter) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	m.capturedInput = input
	if m.transactFunc != nil {
		return m.transactFunc(ctx, input, opts...)
	}
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

// Test: Import with transactor uses atomic transaction with all items
func TestHandler_ImportWithTransactor_UsesAtomicTransaction(t *testing.T) {
	mockRepo := &mockEmailRepository{
		createFunc: func(ctx context.Context, email *emailItem) error {
			t.Error("CreateEmail should not be called when transactor is available")
			return nil
		},
	}
	mockStreamer := &mockBlobStreamer{}
	mockUploader := &mockBlobUploader{}
	mockMailboxRepo := &mockMailboxRepository{
		existsFunc: func(ctx context.Context, accountID, mailboxID string) (bool, error) {
			return true, nil
		},
	}
	mockStateRepo := &mockStateRepository{}
	mockTransactor := &mockTransactWriter{}

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, mockStateRepo, mockTransactor)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId": "blob-456",
					"mailboxIds": map[string]any{
						"inbox": true,
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
	created, ok := response.MethodResponse.Args["created"].(map[string]any)
	if !ok {
		t.Fatal("created should be a map")
	}
	if _, ok := created["client-ref-1"]; !ok {
		t.Fatal("created should contain client-ref-1")
	}

	// Should have called TransactWriteItems
	if mockTransactor.capturedInput == nil {
		t.Fatal("TransactWriteItems was not called")
	}

	// Expected item count for 1 mailbox:
	// - 2 from BuildCreateEmailItems (email + 1 membership)
	// - 2 from BuildStateChangeItems(Email) (state update + change log)
	// - 2 from BuildStateChangeItems(Thread) (state update + change log)
	// - 1 from BuildIncrementCountsItems (mailbox counter)
	// - 2 from BuildStateChangeItems(Mailbox) (state update + change log)
	// = 9 total items
	expectedItems := 9
	actualItems := len(mockTransactor.capturedInput.TransactItems)
	if actualItems != expectedItems {
		t.Errorf("Transaction item count = %d, want %d", actualItems, expectedItems)
	}
}

// Test: Transaction failure returns error and triggers blob cleanup
func TestHandler_TransactionFailure_ReturnsErrorAndCleansUpBlobs(t *testing.T) {
	var publishedBlobIDs []string

	// Email with a base64 attachment that gets uploaded as a separate blob
	emailWithAttachment := `From: Alice <alice@example.com>
To: Bob <bob@example.com>
Subject: With Attachment
MIME-Version: 1.0
Content-Type: application/octet-stream; name="test.bin"
Content-Transfer-Encoding: base64
Content-Disposition: attachment; filename="test.bin"

SGVsbG8gV29ybGQ=
`
	mockRepo := &mockEmailRepository{}
	mockStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(emailWithAttachment)), nil
		},
	}
	mockUploader := &mockBlobUploader{
		uploadFunc: func(ctx context.Context, accountID, parentBlobID, contentType string, body io.Reader) (string, int64, error) {
			content, _ := io.ReadAll(body)
			return "uploaded-part-blob", int64(len(content)), nil
		},
	}
	mockMailboxRepo := &mockMailboxRepository{
		existsFunc: func(ctx context.Context, accountID, mailboxID string) (bool, error) {
			return true, nil
		},
	}
	mockStateRepo := &mockStateRepository{}
	mockBlobPub := &mockBlobDeletePublisher{
		publishFunc: func(ctx context.Context, accountID string, blobIDs []string, apiURL string) error {
			publishedBlobIDs = append(publishedBlobIDs, blobIDs...)
			return nil
		},
	}
	mockTransactor := &mockTransactWriter{
		transactFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			return nil, errors.New("transaction failed")
		},
	}

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, mockStateRepo, mockTransactor, mockBlobPub)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
		Args: map[string]any{
			"accountId": "user-123",
			"emails": map[string]any{
				"client-ref-1": map[string]any{
					"blobId": "blob-456",
					"mailboxIds": map[string]any{
						"inbox": true,
					},
				},
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should have notCreated entry with serverFail error
	notCreated, ok := response.MethodResponse.Args["notCreated"].(map[string]any)
	if !ok {
		t.Fatal("notCreated should be a map")
	}

	errorEntry, ok := notCreated["client-ref-1"].(map[string]any)
	if !ok {
		t.Fatal("notCreated should contain client-ref-1")
	}

	if errorType, ok := errorEntry["type"].(string); !ok || errorType != "serverFail" {
		t.Errorf("error type = %v, want %q", errorEntry["type"], "serverFail")
	}

	// Should have published blob cleanup for the uploaded part blob
	if len(publishedBlobIDs) == 0 {
		t.Error("expected blob cleanup to be published on transaction failure")
	}
}

// Test: CreateEmail failure publishes blob cleanup
func TestHandler_RepositoryError_PublishesBlobCleanup(t *testing.T) {
	var publishedBlobIDs []string

	// Email with a base64 attachment that gets uploaded as a separate blob
	emailWithAttachment := `From: Alice <alice@example.com>
To: Bob <bob@example.com>
Subject: With Attachment
MIME-Version: 1.0
Content-Type: application/octet-stream; name="test.bin"
Content-Transfer-Encoding: base64
Content-Disposition: attachment; filename="test.bin"

SGVsbG8gV29ybGQ=
`
	mockRepo := &mockEmailRepository{
		createFunc: func(ctx context.Context, email *emailItem) error {
			return errors.New("database error")
		},
	}
	mockStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(emailWithAttachment)), nil
		},
	}
	mockUploader := &mockBlobUploader{
		uploadFunc: func(ctx context.Context, accountID, parentBlobID, contentType string, body io.Reader) (string, int64, error) {
			content, _ := io.ReadAll(body)
			return "uploaded-part-blob", int64(len(content)), nil
		},
	}
	mockMailboxRepo := &mockMailboxRepository{}
	mockBlobPub := &mockBlobDeletePublisher{
		publishFunc: func(ctx context.Context, accountID string, blobIDs []string, apiURL string) error {
			publishedBlobIDs = append(publishedBlobIDs, blobIDs...)
			return nil
		},
	}

	h := newHandler(mockRepo, mockBlobClientFactory(mockStreamer, mockUploader), mockMailboxRepo, &mockStateRepository{}, mockBlobPub)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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
	if _, ok := notCreated["client-ref-1"]; !ok {
		t.Fatal("notCreated should contain client-ref-1")
	}

	// Should have published blob cleanup for the uploaded part blob
	if len(publishedBlobIDs) == 0 {
		t.Error("expected blob cleanup to be published on CreateEmail failure")
	}
}
