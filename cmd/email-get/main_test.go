package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
)

// emailItem is an alias for the internal email.EmailItem type.
type emailItem = email.EmailItem

// mockEmailRepository implements the EmailRepository interface for testing.
type mockEmailRepository struct {
	getFunc func(ctx context.Context, accountID, emailID string) (*emailItem, error)
}

func (m *mockEmailRepository) GetEmail(ctx context.Context, accountID, emailID string) (*emailItem, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, accountID, emailID)
	}
	return nil, email.ErrEmailNotFound
}

// mockStateRepository implements the StateRepository interface for testing.
type mockStateRepository struct {
	getCurrentStateFunc func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
}

func (m *mockStateRepository) GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
	if m.getCurrentStateFunc != nil {
		return m.getCurrentStateFunc(ctx, accountID, objectType)
	}
	return 0, nil
}

// Helper to create a test email item.
func testEmailItem(accountID, emailID string) *emailItem {
	return &emailItem{
		AccountID:     accountID,
		EmailID:       emailID,
		BlobID:        "blob-" + emailID,
		ThreadID:      "thread-" + emailID,
		MailboxIDs:    map[string]bool{"inbox": true},
		Keywords:      map[string]bool{"$seen": true},
		Size:          1234,
		ReceivedAt:    time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC),
		HasAttachment: false,
		Subject:       "Test Subject",
		From:          []email.EmailAddress{{Name: "Alice", Email: "alice@example.com"}},
		To:            []email.EmailAddress{{Name: "Bob", Email: "bob@example.com"}},
		CC:            nil,
		ReplyTo:       nil,
		SentAt:        time.Date(2024, 1, 20, 9, 0, 0, 0, time.UTC),
		MessageID:     []string{"<msg-123@example.com>"},
		InReplyTo:     nil,
		References:    nil,
		Preview:       "This is a preview...",
		BodyStructure: email.BodyPart{PartID: "1", Type: "text/plain", Size: 100},
		TextBody:      []string{"1"},
		HTMLBody:      nil,
		Attachments:   nil,
	}
}

func TestHandler_SingleIDFound(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			if accountID == "user-123" && emailID == "email-1" {
				return testEmail, nil
			}
			return nil, email.ErrEmailNotFound
		},
	}

	h := newHandler(mockRepo, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"email-1"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should return Email/get response
	if response.MethodResponse.Name != "Email/get" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/get")
	}

	// Should have accountId in args
	accountID, ok := response.MethodResponse.Args["accountId"].(string)
	if !ok || accountID != "user-123" {
		t.Errorf("accountId = %v, want %q", response.MethodResponse.Args["accountId"], "user-123")
	}

	// Should have state
	state, ok := response.MethodResponse.Args["state"].(string)
	if !ok || state != "0" {
		t.Errorf("state = %v, want %q", response.MethodResponse.Args["state"], "0")
	}

	// Should have list with one email
	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok {
		t.Fatal("list should be a slice")
	}
	if len(list) != 1 {
		t.Fatalf("list length = %d, want 1", len(list))
	}

	// Check email in list
	emailMap, ok := list[0].(map[string]any)
	if !ok {
		t.Fatal("list[0] should be a map")
	}
	if emailMap["id"] != "email-1" {
		t.Errorf("id = %v, want %q", emailMap["id"], "email-1")
	}
	if emailMap["blobId"] != "blob-email-1" {
		t.Errorf("blobId = %v, want %q", emailMap["blobId"], "blob-email-1")
	}
	if emailMap["subject"] != "Test Subject" {
		t.Errorf("subject = %v, want %q", emailMap["subject"], "Test Subject")
	}

	// Should have empty notFound
	notFound, ok := response.MethodResponse.Args["notFound"].([]any)
	if !ok {
		t.Fatal("notFound should be a slice")
	}
	if len(notFound) != 0 {
		t.Errorf("notFound length = %d, want 0", len(notFound))
	}
}

func TestHandler_SingleIDNotFound(t *testing.T) {
	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return nil, email.ErrEmailNotFound
		},
	}

	h := newHandler(mockRepo, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"nonexistent-id"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should return Email/get response (not error)
	if response.MethodResponse.Name != "Email/get" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/get")
	}

	// Should have empty list
	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok {
		t.Fatal("list should be a slice")
	}
	if len(list) != 0 {
		t.Errorf("list length = %d, want 0", len(list))
	}

	// Should have notFound with the ID
	notFound, ok := response.MethodResponse.Args["notFound"].([]any)
	if !ok {
		t.Fatal("notFound should be a slice")
	}
	if len(notFound) != 1 {
		t.Fatalf("notFound length = %d, want 1", len(notFound))
	}
	if notFound[0] != "nonexistent-id" {
		t.Errorf("notFound[0] = %v, want %q", notFound[0], "nonexistent-id")
	}
}

func TestHandler_MultipleIDsMixedResults(t *testing.T) {
	email1 := testEmailItem("user-123", "email-1")
	email3 := testEmailItem("user-123", "email-3")

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			switch emailID {
			case "email-1":
				return email1, nil
			case "email-3":
				return email3, nil
			default:
				return nil, email.ErrEmailNotFound
			}
		},
	}

	h := newHandler(mockRepo, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"email-1", "email-2", "email-3"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should have list with two emails
	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok {
		t.Fatal("list should be a slice")
	}
	if len(list) != 2 {
		t.Fatalf("list length = %d, want 2", len(list))
	}

	// Should have notFound with one ID
	notFound, ok := response.MethodResponse.Args["notFound"].([]any)
	if !ok {
		t.Fatal("notFound should be a slice")
	}
	if len(notFound) != 1 {
		t.Fatalf("notFound length = %d, want 1", len(notFound))
	}
	if notFound[0] != "email-2" {
		t.Errorf("notFound[0] = %v, want %q", notFound[0], "email-2")
	}
}

func TestHandler_PropertyFiltering(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	h := newHandler(mockRepo, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":  "user-123",
			"ids":        []any{"email-1"},
			"properties": []any{"id", "subject", "from"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should return Email/get response
	if response.MethodResponse.Name != "Email/get" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/get")
	}

	// Should have list with one email
	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok {
		t.Fatal("list should be a slice")
	}
	if len(list) != 1 {
		t.Fatalf("list length = %d, want 1", len(list))
	}

	// Check email has only requested properties
	emailMap, ok := list[0].(map[string]any)
	if !ok {
		t.Fatal("list[0] should be a map")
	}

	// Should have requested properties
	if emailMap["id"] != "email-1" {
		t.Errorf("id = %v, want %q", emailMap["id"], "email-1")
	}
	if emailMap["subject"] != "Test Subject" {
		t.Errorf("subject = %v, want %q", emailMap["subject"], "Test Subject")
	}
	if _, ok := emailMap["from"]; !ok {
		t.Error("from should be present")
	}

	// Should NOT have unrequested properties
	if _, ok := emailMap["blobId"]; ok {
		t.Error("blobId should NOT be present (not requested)")
	}
	if _, ok := emailMap["threadId"]; ok {
		t.Error("threadId should NOT be present (not requested)")
	}
	if _, ok := emailMap["size"]; ok {
		t.Error("size should NOT be present (not requested)")
	}
}

func TestHandler_HeaderPropertyRejection(t *testing.T) {
	mockRepo := &mockEmailRepository{}

	h := newHandler(mockRepo, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":  "user-123",
			"ids":        []any{"email-1"},
			"properties": []any{"id", "header:X-Custom-Header"},
		},
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
	if !ok || errorType != "invalidArguments" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "invalidArguments")
	}

	// Description should mention header
	description, _ := response.MethodResponse.Args["description"].(string)
	if description == "" {
		t.Error("description should be present")
	}
}

func TestHandler_MissingIDs(t *testing.T) {
	mockRepo := &mockEmailRepository{}

	h := newHandler(mockRepo, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			// No "ids" field
		},
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
	if !ok || errorType != "invalidArguments" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "invalidArguments")
	}
}

func TestHandler_EmptyIDsArray(t *testing.T) {
	mockRepo := &mockEmailRepository{}

	h := newHandler(mockRepo, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should return valid Email/get response (not an error)
	if response.MethodResponse.Name != "Email/get" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/get")
	}

	// Should have empty list
	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok {
		t.Fatal("list should be a slice")
	}
	if len(list) != 0 {
		t.Errorf("list length = %d, want 0", len(list))
	}

	// Should have empty notFound
	notFound, ok := response.MethodResponse.Args["notFound"].([]any)
	if !ok {
		t.Fatal("notFound should be a slice")
	}
	if len(notFound) != 0 {
		t.Errorf("notFound length = %d, want 0", len(notFound))
	}

	// Should have state
	state, ok := response.MethodResponse.Args["state"].(string)
	if !ok || state != "0" {
		t.Errorf("state = %v, want %q", response.MethodResponse.Args["state"], "0")
	}
}

func TestHandler_RepositoryError(t *testing.T) {
	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return nil, errors.New("database connection failed")
		},
	}

	h := newHandler(mockRepo, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"email-1"},
		},
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
	if !ok || errorType != "serverFail" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "serverFail")
	}
}

func TestHandler_InvalidMethod(t *testing.T) {
	mockRepo := &mockEmailRepository{}

	h := newHandler(mockRepo, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/import",
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

func TestHandler_BodyValuesAlwaysEmpty(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	h := newHandler(mockRepo, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"email-1"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok || len(list) == 0 {
		t.Fatal("list should have one email")
	}

	emailMap, ok := list[0].(map[string]any)
	if !ok {
		t.Fatal("list[0] should be a map")
	}

	// bodyValues should be present and be an empty map
	bodyValues, ok := emailMap["bodyValues"].(map[string]any)
	if !ok {
		t.Fatal("bodyValues should be a map")
	}
	if len(bodyValues) != 0 {
		t.Errorf("bodyValues should be empty, got %v", bodyValues)
	}
}

func TestHandler_FromFieldValue(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	h := newHandler(mockRepo, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"email-1"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok || len(list) == 0 {
		t.Fatal("list should have one email")
	}

	emailMap, ok := list[0].(map[string]any)
	if !ok {
		t.Fatal("list[0] should be a map")
	}

	// Verify from field is present and has correct value
	from, ok := emailMap["from"].([]map[string]any)
	if !ok {
		t.Fatalf("from should be []map[string]any, got %T: %v", emailMap["from"], emailMap["from"])
	}
	if len(from) != 1 {
		t.Fatalf("from length = %d, want 1", len(from))
	}
	if from[0]["name"] != "Alice" {
		t.Errorf("from[0].name = %q, want %q", from[0]["name"], "Alice")
	}
	if from[0]["email"] != "alice@example.com" {
		t.Errorf("from[0].email = %q, want %q", from[0]["email"], "alice@example.com")
	}

	// Also verify 'to' for comparison
	to, ok := emailMap["to"].([]map[string]any)
	if !ok {
		t.Fatalf("to should be []map[string]any, got %T: %v", emailMap["to"], emailMap["to"])
	}
	if len(to) != 1 {
		t.Fatalf("to length = %d, want 1", len(to))
	}
	if to[0]["email"] != "bob@example.com" {
		t.Errorf("to[0].email = %q, want %q", to[0]["email"], "bob@example.com")
	}
}

func TestHandler_InvalidIDType(t *testing.T) {
	mockRepo := &mockEmailRepository{}

	h := newHandler(mockRepo, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       "not-an-array", // Wrong type - should be array
		},
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
	if !ok || errorType != "invalidArguments" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "invalidArguments")
	}
}

func TestHandler_ReturnsActualState(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}
	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			if objectType != state.ObjectTypeEmail {
				t.Errorf("objectType = %q, want %q", objectType, state.ObjectTypeEmail)
			}
			return 42, nil
		},
	}

	h := newHandler(mockRepo, mockStateRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"email-1"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Email/get" {
		t.Fatalf("Name = %q, want %q", response.MethodResponse.Name, "Email/get")
	}

	stateVal, ok := response.MethodResponse.Args["state"].(string)
	if !ok {
		t.Fatal("state should be a string")
	}
	if stateVal != "42" {
		t.Errorf("state = %q, want %q", stateVal, "42")
	}
}

func TestHandler_StateRepositoryError(t *testing.T) {
	mockRepo := &mockEmailRepository{}
	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 0, errors.New("state lookup failed")
		},
	}

	h := newHandler(mockRepo, mockStateRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "error" {
		t.Fatalf("Name = %q, want %q", response.MethodResponse.Name, "error")
	}

	errorType, ok := response.MethodResponse.Args["type"].(string)
	if !ok || errorType != "serverFail" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "serverFail")
	}
}

func TestHandler_SenderFieldPresent(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	testEmail.Sender = []email.EmailAddress{{Name: "Secretary", Email: "secretary@example.com"}}

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	h := newHandler(mockRepo, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"email-1"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok || len(list) == 0 {
		t.Fatal("list should have one email")
	}

	emailMap, ok := list[0].(map[string]any)
	if !ok {
		t.Fatal("list[0] should be a map")
	}

	// Verify sender field is present and has correct value
	sender, ok := emailMap["sender"].([]map[string]any)
	if !ok {
		t.Fatalf("sender should be []map[string]any, got %T: %v", emailMap["sender"], emailMap["sender"])
	}
	if len(sender) != 1 {
		t.Fatalf("sender length = %d, want 1", len(sender))
	}
	if sender[0]["name"] != "Secretary" {
		t.Errorf("sender[0].name = %q, want %q", sender[0]["name"], "Secretary")
	}
	if sender[0]["email"] != "secretary@example.com" {
		t.Errorf("sender[0].email = %q, want %q", sender[0]["email"], "secretary@example.com")
	}
}

func TestHandler_BccFieldPresent(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	testEmail.Bcc = []email.EmailAddress{
		{Name: "Secret", Email: "secret@example.com"},
		{Name: "Hidden", Email: "hidden@example.com"},
	}

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	h := newHandler(mockRepo, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"email-1"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok || len(list) == 0 {
		t.Fatal("list should have one email")
	}

	emailMap, ok := list[0].(map[string]any)
	if !ok {
		t.Fatal("list[0] should be a map")
	}

	// Verify bcc field is present and has correct value
	bcc, ok := emailMap["bcc"].([]map[string]any)
	if !ok {
		t.Fatalf("bcc should be []map[string]any, got %T: %v", emailMap["bcc"], emailMap["bcc"])
	}
	if len(bcc) != 2 {
		t.Fatalf("bcc length = %d, want 2", len(bcc))
	}
	if bcc[0]["name"] != "Secret" {
		t.Errorf("bcc[0].name = %q, want %q", bcc[0]["name"], "Secret")
	}
	if bcc[0]["email"] != "secret@example.com" {
		t.Errorf("bcc[0].email = %q, want %q", bcc[0]["email"], "secret@example.com")
	}
}

func TestHandler_SenderNullWhenEmpty(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	// Sender is nil (empty) - should return null per RFC 8621

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	h := newHandler(mockRepo, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"email-1"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok || len(list) == 0 {
		t.Fatal("list should have one email")
	}

	emailMap, ok := list[0].(map[string]any)
	if !ok {
		t.Fatal("list[0] should be a map")
	}

	// Per RFC 8621, empty address lists should be null, not empty array
	sender := emailMap["sender"]
	if sender != nil {
		t.Errorf("sender should be nil when empty, got %T: %v", sender, sender)
	}
}

func TestHandler_BccNullWhenEmpty(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	// Bcc is nil (empty) - should return null per RFC 8621

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	h := newHandler(mockRepo, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"email-1"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok || len(list) == 0 {
		t.Fatal("list should have one email")
	}

	emailMap, ok := list[0].(map[string]any)
	if !ok {
		t.Fatal("list[0] should be a map")
	}

	// Per RFC 8621, empty address lists should be null, not empty array
	bcc := emailMap["bcc"]
	if bcc != nil {
		t.Errorf("bcc should be nil when empty, got %T: %v", bcc, bcc)
	}
}
