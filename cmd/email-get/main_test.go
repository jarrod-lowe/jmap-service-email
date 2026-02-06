package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jarrod-lowe/jmap-service-libs/plugincontract"
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

// mockBlobFactory wraps a BlobStreamer in a factory function for testing.
func mockBlobFactory(bs BlobStreamer) func(string) BlobStreamer {
	return func(_ string) BlobStreamer { return bs }
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

func TestHandler_FactoryReceivesAPIURL(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	var capturedURL string
	mock := &mockBlobStreamer{}
	factory := func(baseURL string) BlobStreamer {
		capturedURL = baseURL
		return mock
	}

	h := newHandler(mockRepo, nil, factory, defaultMaxBodyValueBytes)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		APIURL:    "https://api.example.com/prod",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"email-1"},
		},
	}

	_, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if capturedURL != "https://api.example.com/prod" {
		t.Errorf("factory received URL %q, want %q", capturedURL, "https://api.example.com/prod")
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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, nil, mockBlobFactory(mockBlobStreamer), defaultMaxBodyValueBytes)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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

	h := newHandler(mockRepo, nil, mockBlobFactory(mockBlobStreamer), defaultMaxBodyValueBytes)

	// Try to use asDate form on Subject (not allowed)
	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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

	h := newHandler(mockRepo, nil, mockBlobFactory(mockBlobStreamer), defaultMaxBodyValueBytes)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, mockStateRepo, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, mockStateRepo, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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
	testEmail.BodyStructure = email.BodyPart{
		PartID: "0",
		Type:   "multipart/mixed",
		SubParts: []email.BodyPart{
			{PartID: "1", Type: "text/plain", BlobID: "blob-1", Charset: "utf-8"},
			{PartID: "2", Type: "image/png", BlobID: "blob-2"},
			{PartID: "3", Type: "text/plain", BlobID: "blob-3", Charset: "utf-8"},
		},
	}

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	mockBlobStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			switch blobID {
			case "blob-1":
				return io.NopCloser(strings.NewReader("Content 1")), nil
			case "blob-3":
				return io.NopCloser(strings.NewReader("Content 3")), nil
			default:
				return nil, errors.New("blob not found")
			}
		},
	}

	h := newHandler(mockRepo, nil, mockBlobFactory(mockBlobStreamer), defaultMaxBodyValueBytes)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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
	expectedContent := map[string]string{"1": "Content 1", "3": "Content 3"}
	for partID, expectedValue := range expectedContent {
		entry, ok := bodyValues[partID].(map[string]any)
		if !ok {
			t.Fatalf("bodyValues[%q] should be a map, got %T", partID, bodyValues[partID])
		}

		// value should be actual content
		if entry["value"] != expectedValue {
			t.Errorf("bodyValues[%q].value = %v, want %q", partID, entry["value"], expectedValue)
		}

		// isTruncated should be false (content fits)
		if entry["isTruncated"] != false {
			t.Errorf("bodyValues[%q].isTruncated = %v, want false", partID, entry["isTruncated"])
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
	testEmail.BodyStructure = email.BodyPart{
		PartID: "0",
		Type:   "multipart/mixed",
		SubParts: []email.BodyPart{
			{PartID: "1", Type: "text/plain", BlobID: "blob-1", Charset: "utf-8"},
			{PartID: "2", Type: "text/html", BlobID: "blob-2", Charset: "utf-8"},
			{PartID: "3", Type: "image/png", BlobID: "blob-3"},
			{PartID: "4", Type: "text/html", BlobID: "blob-4", Charset: "utf-8"},
		},
	}

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	mockBlobStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			switch blobID {
			case "blob-2":
				return io.NopCloser(strings.NewReader("<p>HTML 2</p>")), nil
			case "blob-4":
				return io.NopCloser(strings.NewReader("<p>HTML 4</p>")), nil
			default:
				return nil, errors.New("blob not found")
			}
		},
	}

	h := newHandler(mockRepo, nil, mockBlobFactory(mockBlobStreamer), defaultMaxBodyValueBytes)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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
	expectedContent := map[string]string{"2": "<p>HTML 2</p>", "4": "<p>HTML 4</p>"}
	for partID, expectedValue := range expectedContent {
		entry, ok := bodyValues[partID].(map[string]any)
		if !ok {
			t.Fatalf("bodyValues[%q] should be a map, got %T", partID, bodyValues[partID])
		}

		// value should be actual content
		if entry["value"] != expectedValue {
			t.Errorf("bodyValues[%q].value = %v, want %q", partID, entry["value"], expectedValue)
		}

		// isTruncated should be false
		if entry["isTruncated"] != false {
			t.Errorf("bodyValues[%q].isTruncated = %v, want false", partID, entry["isTruncated"])
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
			{PartID: "1", Type: "text/plain", Size: 100, BlobID: "blob-1", Charset: "utf-8"},
			{PartID: "2", Type: "text/html", Size: 200, BlobID: "blob-2", Charset: "utf-8"},
			{PartID: "3", Type: "image/png", Size: 1000, BlobID: "blob-3"}, // Not text/*, should be excluded
		},
	}

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	mockBlobStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			switch blobID {
			case "blob-1":
				return io.NopCloser(strings.NewReader("Plain text")), nil
			case "blob-2":
				return io.NopCloser(strings.NewReader("<p>HTML</p>")), nil
			default:
				return nil, errors.New("blob not found")
			}
		},
	}

	h := newHandler(mockRepo, nil, mockBlobFactory(mockBlobStreamer), defaultMaxBodyValueBytes)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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
	expectedContent := map[string]string{"1": "Plain text", "2": "<p>HTML</p>"}
	for partID, expectedValue := range expectedContent {
		entry, ok := bodyValues[partID].(map[string]any)
		if !ok {
			t.Fatalf("bodyValues[%q] should be a map, got %T", partID, bodyValues[partID])
		}

		// value should be actual content
		if entry["value"] != expectedValue {
			t.Errorf("bodyValues[%q].value = %v, want %q", partID, entry["value"], expectedValue)
		}

		// isTruncated should be false
		if entry["isTruncated"] != false {
			t.Errorf("bodyValues[%q].isTruncated = %v, want false", partID, entry["isTruncated"])
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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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
	testEmail.BodyStructure = email.BodyPart{
		PartID:  "1",
		Type:    "text/plain",
		BlobID:  "blob-1",
		Charset: "utf-8",
		Size:    20,
	}

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	mockBlobStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			if blobID == "blob-1" {
				return io.NopCloser(strings.NewReader("Fallback text content")), nil
			}
			return nil, errors.New("blob not found")
		},
	}

	h := newHandler(mockRepo, nil, mockBlobFactory(mockBlobStreamer), defaultMaxBodyValueBytes)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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

	// Verify structure - should have actual content
	if entry["value"] != "Fallback text content" {
		t.Errorf("bodyValues[\"1\"].value = %v, want %q", entry["value"], "Fallback text content")
	}
	if entry["isTruncated"] != false {
		t.Errorf("bodyValues[\"1\"].isTruncated = %v, want false", entry["isTruncated"])
	}
	if entry["isEncodingProblem"] != false {
		t.Errorf("bodyValues[\"1\"].isEncodingProblem = %v, want false", entry["isEncodingProblem"])
	}
}

func TestHandler_FetchHTMLBodyValuesDoesNotFallbackWhenHTMLExists(t *testing.T) {
	// When fetchHTMLBodyValues is true and HTML body parts exist,
	// we should NOT include text body parts in bodyValues

	testEmail := testEmailItem("user-123", "email-1")
	testEmail.TextBody = []string{"1"} // Text part
	testEmail.HTMLBody = []string{"2"} // HTML part exists
	testEmail.BodyStructure = email.BodyPart{
		PartID: "0",
		Type:   "multipart/alternative",
		SubParts: []email.BodyPart{
			{PartID: "1", Type: "text/plain", BlobID: "blob-1", Charset: "utf-8"},
			{PartID: "2", Type: "text/html", BlobID: "blob-2", Charset: "utf-8"},
		},
	}

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	mockBlobStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			switch blobID {
			case "blob-2":
				return io.NopCloser(strings.NewReader("<p>HTML content</p>")), nil
			default:
				return nil, errors.New("blob not found")
			}
		},
	}

	h := newHandler(mockRepo, nil, mockBlobFactory(mockBlobStreamer), defaultMaxBodyValueBytes)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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

	h := newHandler(mockRepo, nil, mockBlobFactory(mockBlobStreamer), defaultMaxBodyValueBytes)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

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

func TestFindBodyPart_TopLevel(t *testing.T) {
	root := email.BodyPart{
		PartID: "1",
		Type:   "text/plain",
		BlobID: "blob-1",
	}

	part := findBodyPart(root, "1")
	if part == nil {
		t.Fatal("expected to find part 1")
	}
	if part.BlobID != "blob-1" {
		t.Errorf("BlobID = %q, want %q", part.BlobID, "blob-1")
	}
}

func TestFindBodyPart_Nested(t *testing.T) {
	root := email.BodyPart{
		PartID: "0",
		Type:   "multipart/alternative",
		SubParts: []email.BodyPart{
			{PartID: "1", Type: "text/plain", BlobID: "blob-1", Charset: "utf-8"},
			{PartID: "2", Type: "text/html", BlobID: "blob-2", Charset: "utf-8"},
		},
	}

	// Find nested part
	part := findBodyPart(root, "2")
	if part == nil {
		t.Fatal("expected to find part 2")
	}
	if part.BlobID != "blob-2" {
		t.Errorf("BlobID = %q, want %q", part.BlobID, "blob-2")
	}
	if part.Type != "text/html" {
		t.Errorf("Type = %q, want %q", part.Type, "text/html")
	}
}

func TestFindBodyPart_DeeplyNested(t *testing.T) {
	root := email.BodyPart{
		PartID: "0",
		Type:   "multipart/mixed",
		SubParts: []email.BodyPart{
			{
				PartID: "1",
				Type:   "multipart/alternative",
				SubParts: []email.BodyPart{
					{PartID: "1.1", Type: "text/plain", BlobID: "blob-1.1"},
					{PartID: "1.2", Type: "text/html", BlobID: "blob-1.2"},
				},
			},
			{PartID: "2", Type: "image/png", BlobID: "blob-2"},
		},
	}

	// Find deeply nested part
	part := findBodyPart(root, "1.2")
	if part == nil {
		t.Fatal("expected to find part 1.2")
	}
	if part.BlobID != "blob-1.2" {
		t.Errorf("BlobID = %q, want %q", part.BlobID, "blob-1.2")
	}
}

func TestFindBodyPart_NotFound(t *testing.T) {
	root := email.BodyPart{
		PartID: "1",
		Type:   "text/plain",
	}

	part := findBodyPart(root, "nonexistent")
	if part != nil {
		t.Errorf("expected nil for nonexistent part, got %+v", part)
	}
}

func TestHandler_FetchTextBodyValues_ReturnsActualContent(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	testEmail.TextBody = []string{"1"}
	testEmail.BodyStructure = email.BodyPart{
		PartID:  "1",
		Type:    "text/plain",
		BlobID:  "part-blob-1",
		Charset: "utf-8",
		Size:    13,
	}

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	// Mock blob streamer returns actual content
	mockBlobStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			if blobID == "part-blob-1" {
				return io.NopCloser(strings.NewReader("Hello, World!")), nil
			}
			return nil, errors.New("blob not found")
		},
	}

	h := newHandler(mockRepo, nil, mockBlobFactory(mockBlobStreamer), defaultMaxBodyValueBytes)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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

	entry, ok := bodyValues["1"].(map[string]any)
	if !ok {
		t.Fatalf("bodyValues[\"1\"] should be a map, got %T", bodyValues["1"])
	}

	// Value should be actual content, not empty string
	if entry["value"] != "Hello, World!" {
		t.Errorf("bodyValues[\"1\"].value = %q, want %q", entry["value"], "Hello, World!")
	}

	// isTruncated should be false (content fits)
	if entry["isTruncated"] != false {
		t.Errorf("bodyValues[\"1\"].isTruncated = %v, want false", entry["isTruncated"])
	}

	// isEncodingProblem should be false
	if entry["isEncodingProblem"] != false {
		t.Errorf("bodyValues[\"1\"].isEncodingProblem = %v, want false", entry["isEncodingProblem"])
	}
}

func TestHandler_FetchBodyValues_Truncation(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	testEmail.TextBody = []string{"1"}
	testEmail.BodyStructure = email.BodyPart{
		PartID:  "1",
		Type:    "text/plain",
		BlobID:  "part-blob-1",
		Charset: "utf-8",
		Size:    100,
	}

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	// Return content longer than maxBodyValueBytes
	longContent := strings.Repeat("A", 100)
	mockBlobStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(longContent)), nil
		},
	}

	h := newHandler(mockRepo, nil, mockBlobFactory(mockBlobStreamer), defaultMaxBodyValueBytes)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
		Args: map[string]any{
			"accountId":           "user-123",
			"ids":                 []any{"email-1"},
			"fetchTextBodyValues": true,
			"maxBodyValueBytes":   float64(10), // Limit to 10 bytes
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

	entry, ok := bodyValues["1"].(map[string]any)
	if !ok {
		t.Fatalf("bodyValues[\"1\"] should be a map, got %T", bodyValues["1"])
	}

	// Value should be truncated to maxBodyValueBytes
	value, ok := entry["value"].(string)
	if !ok {
		t.Fatalf("value should be string, got %T", entry["value"])
	}
	if len(value) != 10 {
		t.Errorf("value length = %d, want 10", len(value))
	}

	// isTruncated should be true
	if entry["isTruncated"] != true {
		t.Errorf("bodyValues[\"1\"].isTruncated = %v, want true", entry["isTruncated"])
	}
}

func TestHandler_FetchBodyValues_CharsetDecoding(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	testEmail.TextBody = []string{"1"}
	testEmail.BodyStructure = email.BodyPart{
		PartID:  "1",
		Type:    "text/plain",
		BlobID:  "part-blob-1",
		Charset: "iso-8859-1",
		Size:    10,
	}

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	// ISO-8859-1 content: "café" where é = 0xE9
	iso8859Content := []byte{'c', 'a', 'f', 0xE9}
	mockBlobStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(string(iso8859Content))), nil
		},
	}

	h := newHandler(mockRepo, nil, mockBlobFactory(mockBlobStreamer), defaultMaxBodyValueBytes)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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

	entry, ok := bodyValues["1"].(map[string]any)
	if !ok {
		t.Fatalf("bodyValues[\"1\"] should be a map, got %T", bodyValues["1"])
	}

	// Value should be decoded to UTF-8: "café"
	if entry["value"] != "café" {
		t.Errorf("bodyValues[\"1\"].value = %q, want %q", entry["value"], "café")
	}

	// isEncodingProblem should be false (successful decode)
	if entry["isEncodingProblem"] != false {
		t.Errorf("bodyValues[\"1\"].isEncodingProblem = %v, want false", entry["isEncodingProblem"])
	}
}

func TestHandler_FetchBodyValues_MissingBlob(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	testEmail.TextBody = []string{"1"}
	testEmail.BodyStructure = email.BodyPart{
		PartID:  "1",
		Type:    "text/plain",
		BlobID:  "missing-blob",
		Charset: "utf-8",
		Size:    100,
	}

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	// Blob not found
	mockBlobStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return nil, errors.New("blob not found")
		},
	}

	h := newHandler(mockRepo, nil, mockBlobFactory(mockBlobStreamer), defaultMaxBodyValueBytes)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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

	entry, ok := bodyValues["1"].(map[string]any)
	if !ok {
		t.Fatalf("bodyValues[\"1\"] should be a map, got %T", bodyValues["1"])
	}

	// Value should be empty for missing blob
	if entry["value"] != "" {
		t.Errorf("bodyValues[\"1\"].value = %q, want empty string", entry["value"])
	}

	// isEncodingProblem should be true for missing blob
	if entry["isEncodingProblem"] != true {
		t.Errorf("bodyValues[\"1\"].isEncodingProblem = %v, want true", entry["isEncodingProblem"])
	}
}

func TestHandler_FetchHTMLBodyValues_ReturnsActualContent(t *testing.T) {
	testEmail := testEmailItem("user-123", "email-1")
	testEmail.HTMLBody = []string{"2"}
	testEmail.BodyStructure = email.BodyPart{
		PartID: "0",
		Type:   "multipart/alternative",
		SubParts: []email.BodyPart{
			{PartID: "1", Type: "text/plain", BlobID: "part-blob-1", Charset: "utf-8"},
			{PartID: "2", Type: "text/html", BlobID: "part-blob-2", Charset: "utf-8"},
		},
	}

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	mockBlobStreamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			if blobID == "part-blob-2" {
				return io.NopCloser(strings.NewReader("<html><body>Hello</body></html>")), nil
			}
			return nil, errors.New("blob not found")
		},
	}

	h := newHandler(mockRepo, nil, mockBlobFactory(mockBlobStreamer), defaultMaxBodyValueBytes)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		APIURL:    "https://api.test.com",
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

	entry, ok := bodyValues["2"].(map[string]any)
	if !ok {
		t.Fatalf("bodyValues[\"2\"] should be a map, got %T", bodyValues["2"])
	}

	// Value should be actual HTML content
	if entry["value"] != "<html><body>Hello</body></html>" {
		t.Errorf("bodyValues[\"2\"].value = %q, want %q", entry["value"], "<html><body>Hello</body></html>")
	}
}

func TestResolveBodyPartRefs_AttachmentWithFullProperties(t *testing.T) {
	bodyStructure := email.BodyPart{
		PartID: "0",
		Type:   "multipart/mixed",
		SubParts: []email.BodyPart{
			{PartID: "1", Type: "text/plain", BlobID: "blob-1", Size: 100, Charset: "utf-8"},
			{PartID: "2", Type: "image/png", BlobID: "blob-2", Size: 50000, Name: "photo.png", Disposition: "attachment"},
		},
	}

	result := resolveBodyPartRefs([]string{"2"}, bodyStructure, nil)
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}

	part := result[0]
	if part["partId"] != "2" {
		t.Errorf("partId = %v, want %q", part["partId"], "2")
	}
	if part["type"] != "image/png" {
		t.Errorf("type = %v, want %q", part["type"], "image/png")
	}
	if part["blobId"] != "blob-2" {
		t.Errorf("blobId = %v, want %q", part["blobId"], "blob-2")
	}
	if part["size"] != int64(50000) {
		t.Errorf("size = %v, want %d", part["size"], 50000)
	}
	if part["name"] != "photo.png" {
		t.Errorf("name = %v, want %q", part["name"], "photo.png")
	}
	if part["disposition"] != "attachment" {
		t.Errorf("disposition = %v, want %q", part["disposition"], "attachment")
	}
	// Should not have subParts
	if _, ok := part["subParts"]; ok {
		t.Error("subParts should not be present in leaf part references")
	}
}

func TestResolveBodyPartRefs_TextBodyWithCharset(t *testing.T) {
	bodyStructure := email.BodyPart{
		PartID: "1",
		Type:   "text/plain",
		BlobID: "blob-1",
		Size:   200,
		Charset: "iso-8859-1",
	}

	result := resolveBodyPartRefs([]string{"1"}, bodyStructure, nil)
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}

	part := result[0]
	if part["partId"] != "1" {
		t.Errorf("partId = %v, want %q", part["partId"], "1")
	}
	if part["charset"] != "iso-8859-1" {
		t.Errorf("charset = %v, want %q", part["charset"], "iso-8859-1")
	}
	if part["type"] != "text/plain" {
		t.Errorf("type = %v, want %q", part["type"], "text/plain")
	}
}

func TestResolveBodyPartRefs_NilInput(t *testing.T) {
	bodyStructure := email.BodyPart{PartID: "1", Type: "text/plain"}
	result := resolveBodyPartRefs(nil, bodyStructure, nil)
	if result != nil {
		t.Errorf("expected nil for nil input, got %v", result)
	}
}

func TestResolveBodyPartRefs_FallbackForNotFound(t *testing.T) {
	bodyStructure := email.BodyPart{PartID: "1", Type: "text/plain"}

	result := resolveBodyPartRefs([]string{"nonexistent"}, bodyStructure, nil)
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}

	part := result[0]
	if part["partId"] != "nonexistent" {
		t.Errorf("partId = %v, want %q", part["partId"], "nonexistent")
	}
	// Should only have partId for not-found parts
	if len(part) != 1 {
		t.Errorf("expected only partId key for not-found part, got %d keys: %v", len(part), part)
	}
}

func TestResolveBodyPartRefs_FiltersByBodyProperties(t *testing.T) {
	bodyStructure := email.BodyPart{
		PartID:      "1",
		Type:        "text/plain",
		BlobID:      "blob-1",
		Size:        200,
		Charset:     "utf-8",
		Disposition: "inline",
		Name:        "message.txt",
	}

	// Only request partId and type
	bodyProperties := []string{"partId", "type"}
	result := resolveBodyPartRefs([]string{"1"}, bodyStructure, bodyProperties)
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}

	part := result[0]
	if part["partId"] != "1" {
		t.Errorf("partId = %v, want %q", part["partId"], "1")
	}
	if part["type"] != "text/plain" {
		t.Errorf("type = %v, want %q", part["type"], "text/plain")
	}
	// Should NOT have properties not in bodyProperties
	if _, ok := part["blobId"]; ok {
		t.Error("blobId should not be present when not in bodyProperties")
	}
	if _, ok := part["size"]; ok {
		t.Error("size should not be present when not in bodyProperties")
	}
	if _, ok := part["charset"]; ok {
		t.Error("charset should not be present when not in bodyProperties")
	}
	if _, ok := part["name"]; ok {
		t.Error("name should not be present when not in bodyProperties")
	}
}

func TestHandler_BodyProperties_FiltersBodyPartOutput(t *testing.T) {
	// When bodyProperties is specified in request args, textBody/htmlBody/attachments
	// should only include those properties
	testEmail := testEmailItem("user-123", "email-1")
	testEmail.TextBody = []string{"1"}
	testEmail.Attachments = []string{"2"}
	testEmail.BodyStructure = email.BodyPart{
		PartID: "0",
		Type:   "multipart/mixed",
		SubParts: []email.BodyPart{
			{PartID: "1", Type: "text/plain", BlobID: "blob-1", Size: 100, Charset: "utf-8"},
			{PartID: "2", Type: "image/png", BlobID: "blob-2", Size: 50000, Name: "photo.png", Disposition: "attachment"},
		},
	}

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":      "user-123",
			"ids":            []any{"email-1"},
			"properties":     []any{"textBody", "attachments"},
			"bodyProperties": []any{"partId", "type", "name"},
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

	// Check attachments have filtered properties
	attachments, ok := emailMap["attachments"].([]map[string]any)
	if !ok {
		t.Fatalf("attachments should be []map[string]any, got %T: %v", emailMap["attachments"], emailMap["attachments"])
	}
	if len(attachments) != 1 {
		t.Fatalf("attachments length = %d, want 1", len(attachments))
	}
	att := attachments[0]
	if att["partId"] != "2" {
		t.Errorf("attachment partId = %v, want %q", att["partId"], "2")
	}
	if att["type"] != "image/png" {
		t.Errorf("attachment type = %v, want %q", att["type"], "image/png")
	}
	if att["name"] != "photo.png" {
		t.Errorf("attachment name = %v, want %q", att["name"], "photo.png")
	}
	// blobId should NOT be present (not in bodyProperties)
	if _, ok := att["blobId"]; ok {
		t.Error("attachment blobId should not be present when not in bodyProperties")
	}
	// size should NOT be present
	if _, ok := att["size"]; ok {
		t.Error("attachment size should not be present when not in bodyProperties")
	}
}

func TestHandler_TextBody_ReturnsFullBodyParts(t *testing.T) {
	// textBody should return full EmailBodyPart objects, not just {"partId": ref}
	testEmail := testEmailItem("user-123", "email-1")
	testEmail.TextBody = []string{"1"}
	testEmail.BodyStructure = email.BodyPart{
		PartID:  "1",
		Type:    "text/plain",
		BlobID:  "blob-text-1",
		Size:    500,
		Charset: "utf-8",
	}

	mockRepo := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*emailItem, error) {
			return testEmail, nil
		},
	}

	h := newHandler(mockRepo, nil, nil, defaultMaxBodyValueBytes)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":  "user-123",
			"ids":        []any{"email-1"},
			"properties": []any{"textBody"},
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

	textBody, ok := emailMap["textBody"].([]map[string]any)
	if !ok {
		t.Fatalf("textBody should be []map[string]any, got %T: %v", emailMap["textBody"], emailMap["textBody"])
	}
	if len(textBody) != 1 {
		t.Fatalf("textBody length = %d, want 1", len(textBody))
	}

	part := textBody[0]
	if part["partId"] != "1" {
		t.Errorf("textBody[0].partId = %v, want %q", part["partId"], "1")
	}
	if part["type"] != "text/plain" {
		t.Errorf("textBody[0].type = %v, want %q", part["type"], "text/plain")
	}
	if part["blobId"] != "blob-text-1" {
		t.Errorf("textBody[0].blobId = %v, want %q", part["blobId"], "blob-text-1")
	}
	if part["size"] != int64(500) {
		t.Errorf("textBody[0].size = %v, want %d", part["size"], 500)
	}
	if part["charset"] != "utf-8" {
		t.Errorf("textBody[0].charset = %v, want %q", part["charset"], "utf-8")
	}
}

func TestResolveBodyPartRefs_DefaultBodyProperties(t *testing.T) {
	// When bodyProperties is nil, all default leaf-part properties should be included
	bodyStructure := email.BodyPart{
		PartID:      "1",
		Type:        "text/plain",
		BlobID:      "blob-1",
		Size:        200,
		Charset:     "utf-8",
		Disposition: "inline",
		Name:        "message.txt",
	}

	result := resolveBodyPartRefs([]string{"1"}, bodyStructure, nil)
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}

	part := result[0]
	// All set properties should be present with default bodyProperties
	expectedKeys := []string{"partId", "type", "blobId", "size", "charset", "disposition", "name"}
	for _, key := range expectedKeys {
		if _, ok := part[key]; !ok {
			t.Errorf("expected key %q to be present with default bodyProperties", key)
		}
	}
}
