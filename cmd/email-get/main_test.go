package main

import (
	"context"
	"errors"
	"io"
	"strings"
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

// mockBlobStreamer implements the BlobStreamer interface for testing.
type mockBlobStreamer struct {
	streamFunc func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error)
}

func (m *mockBlobStreamer) Stream(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
	if m.streamFunc != nil {
		return m.streamFunc(ctx, accountID, blobID)
	}
	return io.NopCloser(strings.NewReader("")), nil
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

	h := newHandler(mockRepo, nil, nil)

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

	h := newHandler(mockRepo, nil, nil)

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

	h := newHandler(mockRepo, nil, nil)

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

	h := newHandler(mockRepo, nil, nil)

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

func TestHandler_HeaderPropertySupported(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	testEmail.HeaderSize = 150 // Simulate stored header size

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	// Raw email headers for blob streaming
	rawHeaders := "From: Alice <alice@example.com>\r\n" +
		"To: Bob <bob@example.com>\r\n" +
		"Subject: Test Subject\r\n" +
		"X-Custom-Header: custom-value\r\n" +
		"\r\n"

	mockBlobStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(rawHeaders)), nil
		},
	}

	h := newHandler(mockRepo, nil, mockBlobStreamer)

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

	// Should return Email/get response (not error)
	if response.MethodResponse.Name != "Email/get" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/get")
	}

	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok || len(list) == 0 {
		t.Fatal("list should have one email")
	}

	emailMap, ok := list[0].(map[string]any)
	if !ok {
		t.Fatal("list[0] should be a map")
	}

	// Should have header:X-Custom-Header property
	headerVal, ok := emailMap["header:X-Custom-Header"]
	if !ok {
		t.Error("header:X-Custom-Header should be present")
	}
	if headerVal != "custom-value" {
		t.Errorf("header:X-Custom-Header = %v, want %q", headerVal, "custom-value")
	}
}

func TestHandler_HeaderPropertyInvalidForm(t *testing.T) {
	mockRepo := &mockEmailRepository{}
	mockBlobStreamer := &mockBlobStreamer{}

	h := newHandler(mockRepo, nil, mockBlobStreamer)

	// Try to use asDate form on Subject (not allowed)
	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":  "user-123",
			"ids":        []any{"email-1"},
			"properties": []any{"id", "header:Subject:asDate"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should return error response for invalid form
	if response.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "error")
	}

	errorType, ok := response.MethodResponse.Args["type"].(string)
	if !ok || errorType != "invalidArguments" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "invalidArguments")
	}
}

func TestHandler_HeaderPropertyWithForm(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	testEmail.HeaderSize = 200

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	rawHeaders := "From: Alice <alice@example.com>\r\n" +
		"Subject: =?UTF-8?Q?Hello_World?=\r\n" +
		"\r\n"

	mockBlobStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(rawHeaders)), nil
		},
	}

	h := newHandler(mockRepo, nil, mockBlobStreamer)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":  "user-123",
			"ids":        []any{"email-1"},
			"properties": []any{"id", "header:Subject:asText"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Email/get" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/get")
	}

	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok || len(list) == 0 {
		t.Fatal("list should have one email")
	}

	emailMap, ok := list[0].(map[string]any)
	if !ok {
		t.Fatal("list[0] should be a map")
	}

	// Subject should be decoded from RFC 2047
	headerVal, ok := emailMap["header:Subject:asText"]
	if !ok {
		t.Error("header:Subject:asText should be present")
	}
	if headerVal != "Hello World" {
		t.Errorf("header:Subject:asText = %v, want %q", headerVal, "Hello World")
	}
}

func TestHandler_MissingIDs(t *testing.T) {
	mockRepo := &mockEmailRepository{}

	h := newHandler(mockRepo, nil, nil)

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

	h := newHandler(mockRepo, nil, nil)

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

	h := newHandler(mockRepo, nil, nil)

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

	h := newHandler(mockRepo, nil, nil)

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

	h := newHandler(mockRepo, nil, nil)

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

	h := newHandler(mockRepo, nil, nil)

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

	h := newHandler(mockRepo, nil, nil)

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

	h := newHandler(mockRepo, mockStateRepo, nil)

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

	h := newHandler(mockRepo, mockStateRepo, nil)

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

	h := newHandler(mockRepo, nil, nil)

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

	h := newHandler(mockRepo, nil, nil)

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

	h := newHandler(mockRepo, nil, nil)

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

	h := newHandler(mockRepo, nil, nil)

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

func TestHandler_PropertyFiltering_IDAlwaysReturned(t *testing.T) {
	// RFC 8621 Section 4.1:
	// "The id property is always returned, regardless of whether it is in the properties argument."
	testEmail := testEmailItem("user-123", "email-1")
	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	h := newHandler(mockRepo, nil, nil)

	// Request specific properties WITHOUT "id"
	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":  "user-123",
			"ids":        []any{"email-1"},
			"properties": []any{"threadId", "from", "subject"}, // Note: "id" is NOT in this list
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Email/get" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/get")
	}

	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("list should have one email, got %v", list)
	}

	emailMap, ok := list[0].(map[string]any)
	if !ok {
		t.Fatal("list[0] should be a map")
	}

	// RFC 8621: id MUST always be returned even if not in properties list
	if emailMap["id"] != "email-1" {
		t.Errorf("id = %v, want %q (id must always be returned per RFC 8621)", emailMap["id"], "email-1")
	}

	// Should have requested properties
	if emailMap["threadId"] != "thread-email-1" {
		t.Errorf("threadId = %v, want %q", emailMap["threadId"], "thread-email-1")
	}
	if emailMap["subject"] != "Test Subject" {
		t.Errorf("subject = %v, want %q", emailMap["subject"], "Test Subject")
	}
	if _, ok := emailMap["from"]; !ok {
		t.Error("from should be present")
	}

	// Should NOT have unrequested properties (other than id)
	if _, ok := emailMap["blobId"]; ok {
		t.Error("blobId should NOT be present (not requested)")
	}
	if _, ok := emailMap["size"]; ok {
		t.Error("size should NOT be present (not requested)")
	}
}

func TestHandler_KeywordsEmptyObjectWhenNil(t *testing.T) {
	// RFC 8621 Section 4.1: keywords default is {} (empty object), not null.
	// A nil Keywords map should serialize to {} in JSON, not null.
	// This prevents client crashes like: "TypeError can't access property '$seen', keywords is null"

	testEmail := testEmailItem("user-123", "email-1")
	testEmail.Keywords = nil // Explicitly set to nil

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	h := newHandler(mockRepo, nil, nil)

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

	// The keywords field must be a non-nil map so that it serializes to {} in JSON, not null.
	// A nil map[string]bool would serialize to "null" in JSON, causing client crashes.
	keywords := emailMap["keywords"]
	if keywords == nil {
		t.Fatal("keywords should not be nil (would serialize to null in JSON)")
	}

	// Verify it's a map and is empty
	keywordsMap, ok := keywords.(map[string]bool)
	if !ok {
		t.Fatalf("keywords should be map[string]bool, got %T: %v", keywords, keywords)
	}
	if keywordsMap == nil {
		t.Fatal("keywords map should not be nil (would serialize to null in JSON)")
	}
	if len(keywordsMap) != 0 {
		t.Errorf("keywords should be empty, got %v", keywordsMap)
	}
}

func TestHandler_FetchTextBodyValues(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	testEmail.TextBody = []string{"1", "3"} // Two text body parts

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	h := newHandler(mockRepo, nil, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":           "user-123",
			"ids":                 []any{"email-1"},
			"fetchTextBodyValues": true,
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

	bodyValues, ok := emailMap["bodyValues"].(map[string]any)
	if !ok {
		t.Fatal("bodyValues should be a map")
	}

	// Should have entries for partId "1" and "3"
	for _, partID := range []string{"1", "3"} {
		entry, ok := bodyValues[partID].(map[string]any)
		if !ok {
			t.Fatalf("bodyValues[%q] should be a map, got %T", partID, bodyValues[partID])
		}

		// value should be empty string
		if entry["value"] != "" {
			t.Errorf("bodyValues[%q].value = %v, want empty string", partID, entry["value"])
		}

		// isTruncated should be true
		if entry["isTruncated"] != true {
			t.Errorf("bodyValues[%q].isTruncated = %v, want true", partID, entry["isTruncated"])
		}

		// isEncodingProblem should be false
		if entry["isEncodingProblem"] != false {
			t.Errorf("bodyValues[%q].isEncodingProblem = %v, want false", partID, entry["isEncodingProblem"])
		}
	}
}

func TestHandler_FetchHTMLBodyValues(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	testEmail.HTMLBody = []string{"2", "4"} // Two HTML body parts

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	h := newHandler(mockRepo, nil, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":           "user-123",
			"ids":                 []any{"email-1"},
			"fetchHTMLBodyValues": true,
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

	bodyValues, ok := emailMap["bodyValues"].(map[string]any)
	if !ok {
		t.Fatal("bodyValues should be a map")
	}

	// Should have entries for partId "2" and "4"
	for _, partID := range []string{"2", "4"} {
		entry, ok := bodyValues[partID].(map[string]any)
		if !ok {
			t.Fatalf("bodyValues[%q] should be a map, got %T", partID, bodyValues[partID])
		}

		// value should be empty string
		if entry["value"] != "" {
			t.Errorf("bodyValues[%q].value = %v, want empty string", partID, entry["value"])
		}

		// isTruncated should be true
		if entry["isTruncated"] != true {
			t.Errorf("bodyValues[%q].isTruncated = %v, want true", partID, entry["isTruncated"])
		}

		// isEncodingProblem should be false
		if entry["isEncodingProblem"] != false {
			t.Errorf("bodyValues[%q].isEncodingProblem = %v, want false", partID, entry["isEncodingProblem"])
		}
	}
}

func TestHandler_FetchAllBodyValues(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	// Create a multipart structure with multiple text/* parts
	testEmail.BodyStructure = email.BodyPart{
		PartID: "0",
		Type:   "multipart/alternative",
		SubParts: []email.BodyPart{
			{PartID: "1", Type: "text/plain", Size: 100},
			{PartID: "2", Type: "text/html", Size: 200},
			{PartID: "3", Type: "image/png", Size: 1000}, // Not text/*, should be excluded
		},
	}

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	h := newHandler(mockRepo, nil, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":          "user-123",
			"ids":                []any{"email-1"},
			"fetchAllBodyValues": true,
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

	bodyValues, ok := emailMap["bodyValues"].(map[string]any)
	if !ok {
		t.Fatal("bodyValues should be a map")
	}

	// Should have entries for text/* parts (1 and 2), but not image/png (3)
	for _, partID := range []string{"1", "2"} {
		entry, ok := bodyValues[partID].(map[string]any)
		if !ok {
			t.Fatalf("bodyValues[%q] should be a map, got %T", partID, bodyValues[partID])
		}

		// value should be empty string
		if entry["value"] != "" {
			t.Errorf("bodyValues[%q].value = %v, want empty string", partID, entry["value"])
		}

		// isTruncated should be true
		if entry["isTruncated"] != true {
			t.Errorf("bodyValues[%q].isTruncated = %v, want true", partID, entry["isTruncated"])
		}
	}

	// Should NOT have entry for non-text part
	if _, ok := bodyValues["3"]; ok {
		t.Error("bodyValues should not contain non-text/* part (partId 3)")
	}
}

func TestHandler_FetchBodyValuesNoFlags(t *testing.T) {
	// When no fetch*BodyValues flags are set, bodyValues should be empty
	testEmail := testEmailItem("user-123", "email-1")
	testEmail.TextBody = []string{"1"}
	testEmail.HTMLBody = []string{"2"}

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	h := newHandler(mockRepo, nil, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"email-1"},
			// No fetch*BodyValues flags
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

	bodyValues, ok := emailMap["bodyValues"].(map[string]any)
	if !ok {
		t.Fatal("bodyValues should be a map")
	}

	// Should be empty when no flags are set
	if len(bodyValues) != 0 {
		t.Errorf("bodyValues should be empty when no fetch flags set, got %v", bodyValues)
	}
}

func TestHandler_FetchHTMLBodyValuesFallsBackToTextBody(t *testing.T) {
	// When fetchHTMLBodyValues is true but the email has no HTML body parts,
	// we should fall back to populating bodyValues for the text body parts.
	// This matches what users expect - they want to see body content regardless of format.

	testEmail := testEmailItem("user-123", "email-1")
	testEmail.TextBody = []string{"1"} // Plain-text only email
	testEmail.HTMLBody = nil           // No HTML body parts

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	h := newHandler(mockRepo, nil, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":           "user-123",
			"ids":                 []any{"email-1"},
			"fetchHTMLBodyValues": true, // Request HTML body values
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

	bodyValues, ok := emailMap["bodyValues"].(map[string]any)
	if !ok {
		t.Fatal("bodyValues should be a map")
	}

	// When fetchHTMLBodyValues is true but no HTML parts exist,
	// should fall back to text body parts
	if len(bodyValues) == 0 {
		t.Fatal("bodyValues should not be empty - should fall back to text body parts")
	}

	// Should have entry for text body part "1"
	entry, ok := bodyValues["1"].(map[string]any)
	if !ok {
		t.Fatalf("bodyValues[\"1\"] should be a map, got %T", bodyValues["1"])
	}

	// Verify structure
	if entry["value"] != "" {
		t.Errorf("bodyValues[\"1\"].value = %v, want empty string", entry["value"])
	}
	if entry["isTruncated"] != true {
		t.Errorf("bodyValues[\"1\"].isTruncated = %v, want true", entry["isTruncated"])
	}
	if entry["isEncodingProblem"] != false {
		t.Errorf("bodyValues[\"1\"].isEncodingProblem = %v, want false", entry["isEncodingProblem"])
	}
}

func TestHandler_FetchHTMLBodyValuesDoesNotFallbackWhenHTMLExists(t *testing.T) {
	// When fetchHTMLBodyValues is true and HTML body parts exist,
	// we should NOT include text body parts in bodyValues

	testEmail := testEmailItem("user-123", "email-1")
	testEmail.TextBody = []string{"1"}  // Text part
	testEmail.HTMLBody = []string{"2"}  // HTML part exists

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	h := newHandler(mockRepo, nil, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":           "user-123",
			"ids":                 []any{"email-1"},
			"fetchHTMLBodyValues": true, // Request HTML body values only
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

	bodyValues, ok := emailMap["bodyValues"].(map[string]any)
	if !ok {
		t.Fatal("bodyValues should be a map")
	}

	// Should have HTML part "2"
	if _, ok := bodyValues["2"]; !ok {
		t.Error("bodyValues should contain HTML part \"2\"")
	}

	// Should NOT have text part "1" (because HTML exists, no fallback needed)
	if _, ok := bodyValues["1"]; ok {
		t.Error("bodyValues should NOT contain text part \"1\" when HTML exists (no fallback)")
	}
}

func TestHandler_HeaderAllModifierReturnEmptyArrayForMissingHeader(t *testing.T) {
	// RFC 8621 Section 4.1.3:
	// "If no header fields exist in the message with the requested name,
	// the value is null if fetching a single instance or an empty array
	// if requesting :all."

	testEmail := testEmailItem("user-123", "email-1")
	testEmail.HeaderSize = 100

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	// Raw email with only standard headers - no X-Nonexistent header
	rawHeaders := "From: Alice <alice@example.com>\r\n" +
		"To: Bob <bob@example.com>\r\n" +
		"Subject: Test Subject\r\n" +
		"\r\n"

	mockBlobStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(rawHeaders)), nil
		},
	}

	h := newHandler(mockRepo, nil, mockBlobStreamer)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"email-1"},
			"properties": []any{
				"id",
				"header:X-Nonexistent",      // Without :all - should return null
				"header:X-Nonexistent:all",  // With :all - should return []
				"header:X-Also-Missing:all", // Another missing header with :all
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Email/get" {
		t.Fatalf("Name = %q, want %q", response.MethodResponse.Name, "Email/get")
	}

	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok || len(list) == 0 {
		t.Fatal("list should have one email")
	}

	emailMap, ok := list[0].(map[string]any)
	if !ok {
		t.Fatal("list[0] should be a map")
	}

	// Without :all modifier, missing header should return null
	headerSingle := emailMap["header:X-Nonexistent"]
	if headerSingle != nil {
		t.Errorf("header:X-Nonexistent (without :all) = %v (%T), want nil", headerSingle, headerSingle)
	}

	// With :all modifier, missing header should return empty array []
	headerAll, ok := emailMap["header:X-Nonexistent:all"]
	if !ok {
		t.Fatal("header:X-Nonexistent:all should be present in response")
	}
	headerAllSlice, ok := headerAll.([]any)
	if !ok {
		t.Fatalf("header:X-Nonexistent:all should be []any, got %T: %v", headerAll, headerAll)
	}
	if len(headerAllSlice) != 0 {
		t.Errorf("header:X-Nonexistent:all = %v, want empty array []", headerAllSlice)
	}

	// Another missing header with :all should also return []
	headerAlsoMissing, ok := emailMap["header:X-Also-Missing:all"]
	if !ok {
		t.Fatal("header:X-Also-Missing:all should be present in response")
	}
	headerAlsoMissingSlice, ok := headerAlsoMissing.([]any)
	if !ok {
		t.Fatalf("header:X-Also-Missing:all should be []any, got %T: %v", headerAlsoMissing, headerAlsoMissing)
	}
	if len(headerAlsoMissingSlice) != 0 {
		t.Errorf("header:X-Also-Missing:all = %v, want empty array []", headerAlsoMissingSlice)
	}
}

func TestHandler_SoftDeletedEmailIsNotFound(t *testing.T) {
	now := time.Now()
	testEmail := testEmailItem("user-123", "email-1")
	testEmail.DeletedAt = &now

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			if emailID == "email-1" {
				return testEmail, nil
			}
			return nil, email.ErrEmailNotFound
		},
	}

	h := newHandler(mockRepo, nil, nil)

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

	// Soft-deleted email should appear in notFound, not in list
	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok {
		t.Fatal("list should be a slice")
	}
	if len(list) != 0 {
		t.Errorf("list length = %d, want 0 (soft-deleted email should not appear)", len(list))
	}

	notFound, ok := response.MethodResponse.Args["notFound"].([]any)
	if !ok {
		t.Fatal("notFound should be a slice")
	}
	if len(notFound) != 1 {
		t.Fatalf("notFound length = %d, want 1", len(notFound))
	}
	if notFound[0] != "email-1" {
		t.Errorf("notFound[0] = %v, want email-1", notFound[0])
	}
}
