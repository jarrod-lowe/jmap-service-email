package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-libs/plugincontract"
)

// mockEmailRepository implements the EmailRepository interface for testing.
type mockEmailRepository struct {
	getFunc func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error)
}

func (m *mockEmailRepository) GetEmail(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, accountID, emailID)
	}
	return nil, email.ErrEmailNotFound
}

func testEmailItem(accountID, emailID, subject, preview string) *email.EmailItem {
	return &email.EmailItem{
		AccountID:  accountID,
		EmailID:    emailID,
		Subject:    subject,
		Preview:    preview,
		ReceivedAt: time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC),
		MailboxIDs: map[string]bool{"inbox": true},
	}
}

func TestWrongMethod(t *testing.T) {
	h := newHandler(&mockEmailRepository{})
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:   "Email/get",
		ClientID: "c0",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MethodResponse.Name != "error" {
		t.Errorf("expected error response, got %q", resp.MethodResponse.Name)
	}
	if resp.MethodResponse.Args["type"] != "unknownMethod" {
		t.Errorf("expected unknownMethod, got %v", resp.MethodResponse.Args["type"])
	}
}

func TestMissingEmailIds(t *testing.T) {
	h := newHandler(&mockEmailRepository{})
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:   "SearchSnippet/get",
		ClientID: "c0",
		Args: plugincontract.Args{
			"filter": map[string]any{"text": "hello"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MethodResponse.Name != "error" {
		t.Errorf("expected error response, got %q", resp.MethodResponse.Name)
	}
	if resp.MethodResponse.Args["type"] != "invalidArguments" {
		t.Errorf("expected invalidArguments, got %v", resp.MethodResponse.Args["type"])
	}
}

func TestRequestTooLarge(t *testing.T) {
	h := newHandler(&mockEmailRepository{})
	// Create 101 email IDs
	ids := make([]any, 101)
	for i := range ids {
		ids[i] = "email-" + string(rune('a'+i%26))
	}
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:   "SearchSnippet/get",
		ClientID: "c0",
		Args: plugincontract.Args{
			"emailIds": ids,
			"filter":   map[string]any{"text": "hello"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MethodResponse.Name != "error" {
		t.Errorf("expected error response, got %q", resp.MethodResponse.Name)
	}
	if resp.MethodResponse.Args["type"] != "requestTooLarge" {
		t.Errorf("expected requestTooLarge, got %v", resp.MethodResponse.Args["type"])
	}
}

func TestUnsupportedFilter(t *testing.T) {
	h := newHandler(&mockEmailRepository{})
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:   "SearchSnippet/get",
		ClientID: "c0",
		Args: plugincontract.Args{
			"emailIds": []any{"email-1"},
			"filter":   map[string]any{"header": "X-Custom"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MethodResponse.Name != "error" {
		t.Errorf("expected error response, got %q", resp.MethodResponse.Name)
	}
	if resp.MethodResponse.Args["type"] != "unsupportedFilter" {
		t.Errorf("expected unsupportedFilter, got %v", resp.MethodResponse.Args["type"])
	}
}

func TestBasicTextFilter(t *testing.T) {
	mockRepo := &mockEmailRepository{
		getFunc: func(_ context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return testEmailItem(accountID, emailID, "Hello World", "This is a test email"), nil
		},
	}
	h := newHandler(mockRepo)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:    "SearchSnippet/get",
		AccountID: "user-1",
		ClientID:  "c0",
		Args: plugincontract.Args{
			"emailIds": []any{"email-1"},
			"filter":   map[string]any{"text": "Hello"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MethodResponse.Name != "SearchSnippet/get" {
		t.Fatalf("expected SearchSnippet/get, got %q", resp.MethodResponse.Name)
	}

	list, ok := resp.MethodResponse.Args["list"].([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("expected list of 1, got %v", resp.MethodResponse.Args["list"])
	}

	snippet, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", list[0])
	}
	if snippet["emailId"] != "email-1" {
		t.Errorf("expected emailId email-1, got %v", snippet["emailId"])
	}
	// "Hello" appears in subject
	subject, ok := snippet["subject"].(*string)
	if !ok || subject == nil {
		t.Fatalf("expected non-nil subject, got %v", snippet["subject"])
	}
	if *subject != "<mark>Hello</mark> World" {
		t.Errorf("unexpected subject snippet: %q", *subject)
	}
}

func TestSubjectFilter(t *testing.T) {
	mockRepo := &mockEmailRepository{
		getFunc: func(_ context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return testEmailItem(accountID, emailID, "Hello World", "Something else"), nil
		},
	}
	h := newHandler(mockRepo)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:    "SearchSnippet/get",
		AccountID: "user-1",
		ClientID:  "c0",
		Args: plugincontract.Args{
			"emailIds": []any{"email-1"},
			"filter":   map[string]any{"subject": "Hello"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MethodResponse.Name != "SearchSnippet/get" {
		t.Fatalf("expected SearchSnippet/get, got %q", resp.MethodResponse.Name)
	}

	list, ok := resp.MethodResponse.Args["list"].([]any)
	if !ok || len(list) == 0 {
		t.Fatalf("expected non-empty list, got %v", resp.MethodResponse.Args["list"])
	}
	snippet, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", list[0])
	}

	// "Hello" only goes to subjectTerms, not previewTerms
	subject, ok := snippet["subject"].(*string)
	if !ok || subject == nil || *subject != "<mark>Hello</mark> World" {
		t.Errorf("expected highlighted subject, got %v", snippet["subject"])
	}

	// Preview should be nil since "Hello" is not a preview term
	if snippet["preview"] != (*string)(nil) {
		t.Errorf("expected nil preview, got %v", snippet["preview"])
	}
}

func TestBodyFilter(t *testing.T) {
	mockRepo := &mockEmailRepository{
		getFunc: func(_ context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return testEmailItem(accountID, emailID, "Hello World", "Something else"), nil
		},
	}
	h := newHandler(mockRepo)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:    "SearchSnippet/get",
		AccountID: "user-1",
		ClientID:  "c0",
		Args: plugincontract.Args{
			"emailIds": []any{"email-1"},
			"filter":   map[string]any{"body": "Something"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MethodResponse.Name != "SearchSnippet/get" {
		t.Fatalf("expected SearchSnippet/get, got %q", resp.MethodResponse.Name)
	}

	list, ok := resp.MethodResponse.Args["list"].([]any)
	if !ok || len(list) == 0 {
		t.Fatalf("expected non-empty list, got %v", resp.MethodResponse.Args["list"])
	}
	snippet, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", list[0])
	}

	// Subject should be nil since "Something" is not a subject term
	if snippet["subject"] != (*string)(nil) {
		t.Errorf("expected nil subject, got %v", snippet["subject"])
	}

	// Preview should be highlighted
	preview, ok := snippet["preview"].(*string)
	if !ok || preview == nil || *preview != "<mark>Something</mark> else" {
		t.Errorf("expected highlighted preview, got %v", snippet["preview"])
	}
}

func TestEmailNotFound(t *testing.T) {
	mockRepo := &mockEmailRepository{
		getFunc: func(_ context.Context, _, _ string) (*email.EmailItem, error) {
			return nil, email.ErrEmailNotFound
		},
	}
	h := newHandler(mockRepo)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:    "SearchSnippet/get",
		AccountID: "user-1",
		ClientID:  "c0",
		Args: plugincontract.Args{
			"emailIds": []any{"email-missing"},
			"filter":   map[string]any{"text": "hello"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MethodResponse.Name != "SearchSnippet/get" {
		t.Fatalf("expected SearchSnippet/get, got %q", resp.MethodResponse.Name)
	}

	list, ok := resp.MethodResponse.Args["list"].([]any)
	if !ok {
		t.Fatalf("expected list, got %v", resp.MethodResponse.Args["list"])
	}
	if len(list) != 0 {
		t.Errorf("expected empty list, got %v", list)
	}

	notFound, ok := resp.MethodResponse.Args["notFound"].([]string)
	if !ok {
		t.Fatalf("expected []string notFound, got %T", resp.MethodResponse.Args["notFound"])
	}
	if len(notFound) != 1 || notFound[0] != "email-missing" {
		t.Errorf("expected [email-missing], got %v", notFound)
	}
}

func TestSoftDeletedEmail(t *testing.T) {
	now := time.Now()
	mockRepo := &mockEmailRepository{
		getFunc: func(_ context.Context, accountID, emailID string) (*email.EmailItem, error) {
			item := testEmailItem(accountID, emailID, "Hello", "World")
			item.DeletedAt = &now
			return item, nil
		},
	}
	h := newHandler(mockRepo)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:    "SearchSnippet/get",
		AccountID: "user-1",
		ClientID:  "c0",
		Args: plugincontract.Args{
			"emailIds": []any{"email-deleted"},
			"filter":   map[string]any{"text": "hello"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MethodResponse.Name != "SearchSnippet/get" {
		t.Fatalf("expected SearchSnippet/get, got %q", resp.MethodResponse.Name)
	}

	notFound, ok := resp.MethodResponse.Args["notFound"].([]string)
	if !ok {
		t.Fatalf("expected []string notFound, got %T", resp.MethodResponse.Args["notFound"])
	}
	if len(notFound) != 1 || notFound[0] != "email-deleted" {
		t.Errorf("expected [email-deleted] in notFound, got %v", notFound)
	}
}

func TestNoTextFilters(t *testing.T) {
	// Structural-only filter — no text terms, so subject/preview should be null
	mockRepo := &mockEmailRepository{
		getFunc: func(_ context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return testEmailItem(accountID, emailID, "Hello World", "Preview text"), nil
		},
	}
	h := newHandler(mockRepo)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:    "SearchSnippet/get",
		AccountID: "user-1",
		ClientID:  "c0",
		Args: plugincontract.Args{
			"emailIds": []any{"email-1"},
			"filter":   map[string]any{"inMailbox": "inbox-1"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MethodResponse.Name != "SearchSnippet/get" {
		t.Fatalf("expected SearchSnippet/get, got %q", resp.MethodResponse.Name)
	}

	list, ok := resp.MethodResponse.Args["list"].([]any)
	if !ok || len(list) == 0 {
		t.Fatalf("expected non-empty list, got %v", resp.MethodResponse.Args["list"])
	}
	snippet, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", list[0])
	}
	if snippet["subject"] != (*string)(nil) {
		t.Errorf("expected nil subject, got %v", snippet["subject"])
	}
	if snippet["preview"] != (*string)(nil) {
		t.Errorf("expected nil preview, got %v", snippet["preview"])
	}
}

func TestNullFilter(t *testing.T) {
	// Null filter — no text terms, subject/preview should be null
	mockRepo := &mockEmailRepository{
		getFunc: func(_ context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return testEmailItem(accountID, emailID, "Hello World", "Preview text"), nil
		},
	}
	h := newHandler(mockRepo)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:    "SearchSnippet/get",
		AccountID: "user-1",
		ClientID:  "c0",
		Args: plugincontract.Args{
			"emailIds": []any{"email-1"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MethodResponse.Name != "SearchSnippet/get" {
		t.Fatalf("expected SearchSnippet/get, got %q", resp.MethodResponse.Name)
	}

	list, ok := resp.MethodResponse.Args["list"].([]any)
	if !ok || len(list) == 0 {
		t.Fatalf("expected non-empty list, got %v", resp.MethodResponse.Args["list"])
	}
	snippet, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", list[0])
	}
	if snippet["subject"] != (*string)(nil) {
		t.Errorf("expected nil subject, got %v", snippet["subject"])
	}
	if snippet["preview"] != (*string)(nil) {
		t.Errorf("expected nil preview, got %v", snippet["preview"])
	}
}

func TestMultipleEmailIds(t *testing.T) {
	emails := map[string]*email.EmailItem{
		"email-1": testEmailItem("user-1", "email-1", "Hello World", "First email"),
		"email-2": testEmailItem("user-1", "email-2", "Hello There", "Second email"),
	}
	mockRepo := &mockEmailRepository{
		getFunc: func(_ context.Context, _, emailID string) (*email.EmailItem, error) {
			if item, ok := emails[emailID]; ok {
				return item, nil
			}
			return nil, email.ErrEmailNotFound
		},
	}
	h := newHandler(mockRepo)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:    "SearchSnippet/get",
		AccountID: "user-1",
		ClientID:  "c0",
		Args: plugincontract.Args{
			"emailIds": []any{"email-1", "email-2", "email-missing"},
			"filter":   map[string]any{"text": "Hello"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MethodResponse.Name != "SearchSnippet/get" {
		t.Fatalf("expected SearchSnippet/get, got %q", resp.MethodResponse.Name)
	}

	list, ok := resp.MethodResponse.Args["list"].([]any)
	if !ok {
		t.Fatalf("expected list, got %v", resp.MethodResponse.Args["list"])
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 snippets, got %d", len(list))
	}

	notFound, ok := resp.MethodResponse.Args["notFound"].([]string)
	if !ok {
		t.Fatalf("expected []string notFound, got %T", resp.MethodResponse.Args["notFound"])
	}
	if len(notFound) != 1 || notFound[0] != "email-missing" {
		t.Errorf("expected [email-missing], got %v", notFound)
	}
}

func TestAllFoundNotFoundIsNull(t *testing.T) {
	mockRepo := &mockEmailRepository{
		getFunc: func(_ context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return testEmailItem(accountID, emailID, "Hello", "World"), nil
		},
	}
	h := newHandler(mockRepo)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:    "SearchSnippet/get",
		AccountID: "user-1",
		ClientID:  "c0",
		Args: plugincontract.Args{
			"emailIds": []any{"email-1"},
			"filter":   map[string]any{"text": "Hello"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// When all found, notFound should be nil (JSON null)
	if resp.MethodResponse.Args["notFound"] != nil {
		t.Errorf("expected nil notFound, got %v", resp.MethodResponse.Args["notFound"])
	}
}

func TestServerError(t *testing.T) {
	mockRepo := &mockEmailRepository{
		getFunc: func(_ context.Context, _, _ string) (*email.EmailItem, error) {
			return nil, errors.New("dynamo error")
		},
	}
	h := newHandler(mockRepo)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:    "SearchSnippet/get",
		AccountID: "user-1",
		ClientID:  "c0",
		Args: plugincontract.Args{
			"emailIds": []any{"email-1"},
			"filter":   map[string]any{"text": "hello"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MethodResponse.Name != "error" {
		t.Errorf("expected error response, got %q", resp.MethodResponse.Name)
	}
	if resp.MethodResponse.Args["type"] != "serverFail" {
		t.Errorf("expected serverFail, got %v", resp.MethodResponse.Args["type"])
	}
}

func TestANDFilterWithTextSearch(t *testing.T) {
	mockRepo := &mockEmailRepository{
		getFunc: func(_ context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return testEmailItem(accountID, emailID, "Hello World", "Test email body"), nil
		},
	}
	h := newHandler(mockRepo)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:    "SearchSnippet/get",
		AccountID: "user-1",
		ClientID:  "c0",
		Args: plugincontract.Args{
			"emailIds": []any{"email-1"},
			"filter": map[string]any{
				"operator": "AND",
				"conditions": []any{
					map[string]any{"inMailbox": "inbox-1"},
					map[string]any{"text": "Hello"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MethodResponse.Name != "SearchSnippet/get" {
		t.Fatalf("expected SearchSnippet/get, got %q", resp.MethodResponse.Name)
	}

	list, ok := resp.MethodResponse.Args["list"].([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("expected list of 1, got %v", resp.MethodResponse.Args["list"])
	}

	snippet, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", list[0])
	}
	subject, ok := snippet["subject"].(*string)
	if !ok || subject == nil {
		t.Fatalf("expected non-nil subject, got %v", snippet["subject"])
	}
	if *subject != "<mark>Hello</mark> World" {
		t.Errorf("unexpected subject snippet: %q", *subject)
	}
}

func TestAercCompoundFilter(t *testing.T) {
	mockRepo := &mockEmailRepository{
		getFunc: func(_ context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return testEmailItem(accountID, emailID, "Hello World", "Preview text"), nil
		},
	}
	h := newHandler(mockRepo)

	// aerc-style: AND(inMailboxOtherThan:[junk,trash], OR(to:jmap-test))
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:    "SearchSnippet/get",
		AccountID: "user-1",
		ClientID:  "c0",
		Args: plugincontract.Args{
			"emailIds": []any{"email-1"},
			"filter": map[string]any{
				"operator": "AND",
				"conditions": []any{
					map[string]any{"inMailboxOtherThan": []any{"junk", "trash"}},
					map[string]any{
						"operator": "OR",
						"conditions": []any{
							map[string]any{"to": "jmap-test"},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MethodResponse.Name != "SearchSnippet/get" {
		t.Fatalf("expected SearchSnippet/get, got %q", resp.MethodResponse.Name)
	}

	list, ok := resp.MethodResponse.Args["list"].([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("expected list of 1, got %v", resp.MethodResponse.Args["list"])
	}

	// No text filters, so subject/preview should be null
	snippet, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", list[0])
	}
	if snippet["subject"] != (*string)(nil) {
		t.Errorf("expected nil subject, got %v", snippet["subject"])
	}
}

func TestORMultipleConditions_UnsupportedFilter(t *testing.T) {
	h := newHandler(&mockEmailRepository{})
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:   "SearchSnippet/get",
		ClientID: "c0",
		Args: plugincontract.Args{
			"emailIds": []any{"email-1"},
			"filter": map[string]any{
				"operator": "OR",
				"conditions": []any{
					map[string]any{"from": "alice"},
					map[string]any{"from": "bob"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MethodResponse.Name != "error" {
		t.Errorf("expected error response, got %q", resp.MethodResponse.Name)
	}
	if resp.MethodResponse.Args["type"] != "unsupportedFilter" {
		t.Errorf("expected unsupportedFilter, got %v", resp.MethodResponse.Args["type"])
	}
}

func TestAccountIdFromArgs(t *testing.T) {
	var capturedAccountID string
	mockRepo := &mockEmailRepository{
		getFunc: func(_ context.Context, accountID, _ string) (*email.EmailItem, error) {
			capturedAccountID = accountID
			return testEmailItem(accountID, "email-1", "Hello", "World"), nil
		},
	}
	h := newHandler(mockRepo)
	_, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:    "SearchSnippet/get",
		AccountID: "default-account",
		ClientID:  "c0",
		Args: plugincontract.Args{
			"accountId": "override-account",
			"emailIds":  []any{"email-1"},
			"filter":    map[string]any{"text": "Hello"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedAccountID != "override-account" {
		t.Errorf("expected override-account, got %q", capturedAccountID)
	}
}
