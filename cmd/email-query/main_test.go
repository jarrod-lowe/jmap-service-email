package main

import (
	"context"
	"testing"

	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
)

// mockEmailRepository implements the EmailRepository interface for testing.
type mockEmailRepository struct {
	queryFunc func(ctx context.Context, accountID string, req *email.QueryRequest) (*email.QueryResult, error)
	getFunc   func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error)
}

func (m *mockEmailRepository) QueryEmails(ctx context.Context, accountID string, req *email.QueryRequest) (*email.QueryResult, error) {
	if m.queryFunc != nil {
		return m.queryFunc(ctx, accountID, req)
	}
	return &email.QueryResult{IDs: []string{}, Position: 0, QueryState: ""}, nil
}

func (m *mockEmailRepository) GetEmail(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, accountID, emailID)
	}
	return nil, email.ErrEmailNotFound
}

// mockMailboxRepository implements MailboxChecker for testing.
type mockMailboxRepository struct {
	existsFunc func(ctx context.Context, accountID, mailboxID string) (bool, error)
}

func (m *mockMailboxRepository) MailboxExists(ctx context.Context, accountID, mailboxID string) (bool, error) {
	if m.existsFunc != nil {
		return m.existsFunc(ctx, accountID, mailboxID)
	}
	return false, nil
}

func TestHandler_BasicQuery(t *testing.T) {
	mockEmail := &mockEmailRepository{
		queryFunc: func(ctx context.Context, accountID string, req *email.QueryRequest) (*email.QueryResult, error) {
			return &email.QueryResult{
				IDs:        []string{"email-1", "email-2"},
				Position:   0,
				QueryState: "state-123",
			}, nil
		},
	}
	mockMailbox := &mockMailboxRepository{
		existsFunc: func(ctx context.Context, accountID, mailboxID string) (bool, error) {
			return true, nil
		},
	}

	h := newHandler(mockEmail, mockMailbox)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"filter":    map[string]any{"inMailbox": "inbox-id"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Email/query" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/query")
	}

	// Should have ids array
	ids, ok := response.MethodResponse.Args["ids"].([]string)
	if !ok {
		t.Fatalf("ids should be []string, got %T", response.MethodResponse.Args["ids"])
	}
	if len(ids) != 2 {
		t.Errorf("ids length = %d, want 2", len(ids))
	}

	// Should have position
	position, ok := response.MethodResponse.Args["position"].(int)
	if !ok {
		t.Fatalf("position should be int, got %T", response.MethodResponse.Args["position"])
	}
	if position != 0 {
		t.Errorf("position = %d, want 0", position)
	}

	// Should have queryState
	queryState, ok := response.MethodResponse.Args["queryState"].(string)
	if !ok {
		t.Fatalf("queryState should be string, got %T", response.MethodResponse.Args["queryState"])
	}
	if queryState != "state-123" {
		t.Errorf("queryState = %q, want %q", queryState, "state-123")
	}

	// canCalculateChanges should be false
	canCalculateChanges, ok := response.MethodResponse.Args["canCalculateChanges"].(bool)
	if !ok || canCalculateChanges != false {
		t.Errorf("canCalculateChanges = %v, want false", response.MethodResponse.Args["canCalculateChanges"])
	}
}

func TestHandler_UnsupportedFilter(t *testing.T) {
	mockEmail := &mockEmailRepository{}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"filter":    map[string]any{"hasKeyword": "$seen"}, // Unsupported
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "error")
	}

	errorType, ok := response.MethodResponse.Args["type"].(string)
	if !ok || errorType != "unsupportedFilter" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "unsupportedFilter")
	}
}

func TestHandler_UnsupportedSort(t *testing.T) {
	mockEmail := &mockEmailRepository{}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"sort":      []any{map[string]any{"property": "subject"}}, // Unsupported
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "error")
	}

	errorType, ok := response.MethodResponse.Args["type"].(string)
	if !ok || errorType != "unsupportedSort" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "unsupportedSort")
	}
}

func TestHandler_MailboxNotFound(t *testing.T) {
	mockEmail := &mockEmailRepository{}
	mockMailbox := &mockMailboxRepository{
		existsFunc: func(ctx context.Context, accountID, mailboxID string) (bool, error) {
			return false, nil // Mailbox doesn't exist
		},
	}

	h := newHandler(mockEmail, mockMailbox)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"filter":    map[string]any{"inMailbox": "nonexistent-mailbox"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "error")
	}

	errorType, ok := response.MethodResponse.Args["type"].(string)
	if !ok || errorType != "invalidArguments" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "invalidArguments")
	}
}

func TestHandler_AnchorNotFound(t *testing.T) {
	mockEmail := &mockEmailRepository{
		getFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return nil, email.ErrEmailNotFound
		},
	}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"anchor":    "nonexistent-email",
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "error")
	}

	errorType, ok := response.MethodResponse.Args["type"].(string)
	if !ok || errorType != "anchorNotFound" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "anchorNotFound")
	}
}

func TestHandler_DefaultLimit(t *testing.T) {
	var capturedReq *email.QueryRequest
	mockEmail := &mockEmailRepository{
		queryFunc: func(ctx context.Context, accountID string, req *email.QueryRequest) (*email.QueryResult, error) {
			capturedReq = req
			return &email.QueryResult{IDs: []string{}, Position: 0}, nil
		},
	}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			// No limit specified
		},
	}

	_, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if capturedReq.Limit != 25 {
		t.Errorf("Limit = %d, want 25", capturedReq.Limit)
	}
}

func TestHandler_MaxLimit(t *testing.T) {
	var capturedReq *email.QueryRequest
	mockEmail := &mockEmailRepository{
		queryFunc: func(ctx context.Context, accountID string, req *email.QueryRequest) (*email.QueryResult, error) {
			capturedReq = req
			return &email.QueryResult{IDs: []string{}, Position: 0}, nil
		},
	}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"limit":     float64(500), // Exceeds max
		},
	}

	_, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if capturedReq.Limit != 100 {
		t.Errorf("Limit = %d, want 100 (max)", capturedReq.Limit)
	}
}

func TestHandler_CollapseThreadsAlwaysFalse(t *testing.T) {
	mockEmail := &mockEmailRepository{
		queryFunc: func(ctx context.Context, accountID string, req *email.QueryRequest) (*email.QueryResult, error) {
			return &email.QueryResult{IDs: []string{}, Position: 0}, nil
		},
	}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":       "user-123",
			"collapseThreads": true, // Request true, but we always return false
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	collapseThreads, ok := response.MethodResponse.Args["collapseThreads"].(bool)
	if !ok || collapseThreads != false {
		t.Errorf("collapseThreads = %v, want false", response.MethodResponse.Args["collapseThreads"])
	}
}

func TestHandler_InvalidMethod(t *testing.T) {
	mockEmail := &mockEmailRepository{}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox)

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

	if response.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "error")
	}

	errorType, ok := response.MethodResponse.Args["type"].(string)
	if !ok || errorType != "unknownMethod" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "unknownMethod")
	}
}

func TestHandler_SortReceivedAtAscending(t *testing.T) {
	var capturedReq *email.QueryRequest
	mockEmail := &mockEmailRepository{
		queryFunc: func(ctx context.Context, accountID string, req *email.QueryRequest) (*email.QueryResult, error) {
			capturedReq = req
			return &email.QueryResult{IDs: []string{}, Position: 0}, nil
		},
	}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"sort":      []any{map[string]any{"property": "receivedAt", "isAscending": true}},
		},
	}

	_, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if len(capturedReq.Sort) != 1 {
		t.Fatalf("Sort length = %d, want 1", len(capturedReq.Sort))
	}
	if capturedReq.Sort[0].Property != "receivedAt" {
		t.Errorf("Sort[0].Property = %q, want %q", capturedReq.Sort[0].Property, "receivedAt")
	}
	if !capturedReq.Sort[0].IsAscending {
		t.Error("Sort[0].IsAscending should be true")
	}
}

func TestHandler_EmptyFilter(t *testing.T) {
	var capturedReq *email.QueryRequest
	mockEmail := &mockEmailRepository{
		queryFunc: func(ctx context.Context, accountID string, req *email.QueryRequest) (*email.QueryResult, error) {
			capturedReq = req
			return &email.QueryResult{IDs: []string{"email-1"}, Position: 0}, nil
		},
	}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			// No filter - should query all emails
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Email/query" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/query")
	}

	// Filter should be nil for empty filter case
	if capturedReq.Filter != nil {
		t.Errorf("Filter should be nil for empty filter, got %+v", capturedReq.Filter)
	}
}
