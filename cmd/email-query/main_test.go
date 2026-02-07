package main

import (
	"context"
	"testing"

	"github.com/jarrod-lowe/jmap-service-libs/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailbox"
	"github.com/jarrod-lowe/jmap-service-email/internal/search"
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
	existsFunc     func(ctx context.Context, accountID, mailboxID string) (bool, error)
	getMailboxFunc func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error)
}

func (m *mockMailboxRepository) MailboxExists(ctx context.Context, accountID, mailboxID string) (bool, error) {
	if m.existsFunc != nil {
		return m.existsFunc(ctx, accountID, mailboxID)
	}
	return false, nil
}

func (m *mockMailboxRepository) GetMailbox(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
	if m.getMailboxFunc != nil {
		return m.getMailboxFunc(ctx, accountID, mailboxID)
	}
	return nil, mailbox.ErrMailboxNotFound
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

	h := newHandler(mockEmail, mockMailbox, nil, nil, nil)

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

func TestHandler_UnsupportedFilter_Header(t *testing.T) {
	mockEmail := &mockEmailRepository{}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox, nil, nil, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"filter":    map[string]any{"header": []any{"X-Custom"}}, // Unsupported
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

func TestHandler_UnsupportedFilter_ThreadKeyword(t *testing.T) {
	mockEmail := &mockEmailRepository{}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox, nil, nil, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"filter":    map[string]any{"allInThreadHaveKeyword": "$seen"},
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

func TestHandler_UnsupportedFilter_Unknown(t *testing.T) {
	mockEmail := &mockEmailRepository{}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox, nil, nil, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"filter":    map[string]any{"nonexistentProperty": "foo"},
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

	h := newHandler(mockEmail, mockMailbox, nil, nil, nil)

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

	h := newHandler(mockEmail, mockMailbox, nil, nil, nil)

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

	h := newHandler(mockEmail, mockMailbox, nil, nil, nil)

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

	h := newHandler(mockEmail, mockMailbox, nil, nil, nil)

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

	h := newHandler(mockEmail, mockMailbox, nil, nil, nil)

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

func TestHandler_CollapseThreadsTrue_PassedToRepository(t *testing.T) {
	var capturedReq *email.QueryRequest
	mockEmail := &mockEmailRepository{
		queryFunc: func(ctx context.Context, accountID string, req *email.QueryRequest) (*email.QueryResult, error) {
			capturedReq = req
			return &email.QueryResult{IDs: []string{}, Position: 0}, nil
		},
	}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox, nil, nil, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":       "user-123",
			"collapseThreads": true,
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should pass collapseThreads=true to repository
	if !capturedReq.CollapseThreads {
		t.Error("CollapseThreads should be true in query request")
	}

	// Response should echo back the requested value
	collapseThreads, ok := response.MethodResponse.Args["collapseThreads"].(bool)
	if !ok || !collapseThreads {
		t.Errorf("collapseThreads = %v, want true", response.MethodResponse.Args["collapseThreads"])
	}
}

func TestHandler_Total_IncludedWhenInMailboxFilter(t *testing.T) {
	mockEmail := &mockEmailRepository{
		queryFunc: func(ctx context.Context, accountID string, req *email.QueryRequest) (*email.QueryResult, error) {
			return &email.QueryResult{IDs: []string{"email-1", "email-2"}, Position: 0}, nil
		},
	}
	mockMailbox := &mockMailboxRepository{
		existsFunc: func(ctx context.Context, accountID, mailboxID string) (bool, error) {
			return true, nil
		},
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return &mailbox.MailboxItem{
				MailboxID:   "inbox-id",
				TotalEmails: 42,
			}, nil
		},
	}

	h := newHandler(mockEmail, mockMailbox, nil, nil, nil)

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

	total, ok := response.MethodResponse.Args["total"].(int)
	if !ok {
		t.Fatalf("total should be int, got %T", response.MethodResponse.Args["total"])
	}
	if total != 42 {
		t.Errorf("total = %d, want 42", total)
	}
}

func TestHandler_Total_NotIncludedWithoutInMailboxFilter(t *testing.T) {
	mockEmail := &mockEmailRepository{
		queryFunc: func(ctx context.Context, accountID string, req *email.QueryRequest) (*email.QueryResult, error) {
			return &email.QueryResult{IDs: []string{"email-1"}, Position: 0}, nil
		},
	}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox, nil, nil, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			// No filter - query all emails
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Total should not be present when no inMailbox filter
	if _, ok := response.MethodResponse.Args["total"]; ok {
		t.Error("total should not be present when no inMailbox filter")
	}
}

func TestHandler_InvalidMethod(t *testing.T) {
	mockEmail := &mockEmailRepository{}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox, nil, nil, nil)

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

	h := newHandler(mockEmail, mockMailbox, nil, nil, nil)

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

	h := newHandler(mockEmail, mockMailbox, nil, nil, nil)

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

func TestHandler_HasKeywordFilter(t *testing.T) {
	var capturedReq *email.QueryRequest
	mockEmail := &mockEmailRepository{
		queryFunc: func(ctx context.Context, accountID string, req *email.QueryRequest) (*email.QueryResult, error) {
			capturedReq = req
			return &email.QueryResult{IDs: []string{}, Position: 0}, nil
		},
	}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox, nil, nil, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"filter":    map[string]any{"hasKeyword": "$seen"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Email/query" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/query")
	}
	if capturedReq.Filter == nil || capturedReq.Filter.HasKeyword != "$seen" {
		t.Errorf("Filter.HasKeyword = %v, want '$seen'", capturedReq.Filter)
	}
}

func TestHandler_MultipleFilters(t *testing.T) {
	var capturedReq *email.QueryRequest
	mockEmail := &mockEmailRepository{
		queryFunc: func(ctx context.Context, accountID string, req *email.QueryRequest) (*email.QueryResult, error) {
			capturedReq = req
			return &email.QueryResult{IDs: []string{}, Position: 0}, nil
		},
	}
	mockMailbox := &mockMailboxRepository{
		existsFunc: func(ctx context.Context, accountID, mailboxID string) (bool, error) {
			return true, nil
		},
	}

	h := newHandler(mockEmail, mockMailbox, nil, nil, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"filter": map[string]any{
				"inMailbox":     "inbox-id",
				"hasKeyword":    "$flagged",
				"from":          "alice",
				"hasAttachment": true,
				"minSize":       float64(1024),
				"after":         "2024-01-01T00:00:00Z",
			},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Email/query" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/query")
	}

	f := capturedReq.Filter
	if f == nil {
		t.Fatal("Filter should not be nil")
	}
	if f.InMailbox != "inbox-id" {
		t.Errorf("InMailbox = %q, want %q", f.InMailbox, "inbox-id")
	}
	if f.HasKeyword != "$flagged" {
		t.Errorf("HasKeyword = %q, want %q", f.HasKeyword, "$flagged")
	}
	if f.From != "alice" {
		t.Errorf("From = %q, want %q", f.From, "alice")
	}
	if f.HasAttachment == nil || *f.HasAttachment != true {
		t.Errorf("HasAttachment = %v, want true", f.HasAttachment)
	}
	if f.MinSize == nil || *f.MinSize != 1024 {
		t.Errorf("MinSize = %v, want 1024", f.MinSize)
	}
	if f.After == nil {
		t.Error("After should not be nil")
	}
}

// mockVectorSearcher implements VectorSearcher for testing.
type mockVectorSearcher struct {
	searchFunc func(ctx context.Context, accountID string, filter *email.QueryFilter, position, limit int) (*search.SearchResult, error)
	searchCalls int
}

func (m *mockVectorSearcher) Search(ctx context.Context, accountID string, filter *email.QueryFilter, position, limit int) (*search.SearchResult, error) {
	m.searchCalls++
	if m.searchFunc != nil {
		return m.searchFunc(ctx, accountID, filter, position, limit)
	}
	return &search.SearchResult{IDs: []string{}, Position: position}, nil
}

// mockTokenQuerier implements TokenQuerier for testing.
type mockTokenQuerier struct {
	queryFunc  func(ctx context.Context, accountID string, field email.TokenField, tokenPrefix string, limit int32, scanForward bool) ([]email.TokenQueryResult, error)
	queryCalls int
}

func (m *mockTokenQuerier) QueryTokens(ctx context.Context, accountID string, field email.TokenField, tokenPrefix string, limit int32, scanForward bool) ([]email.TokenQueryResult, error) {
	m.queryCalls++
	if m.queryFunc != nil {
		return m.queryFunc(ctx, accountID, field, tokenPrefix, limit, scanForward)
	}
	return nil, nil
}

// mockEmailFilter implements EmailFilter for testing.
type mockEmailFilter struct {
	filterFunc func(ctx context.Context, accountID string, emailIDs []string, filter *email.QueryFilter) ([]string, error)
}

func (m *mockEmailFilter) FilterEmailIDs(ctx context.Context, accountID string, emailIDs []string, filter *email.QueryFilter) ([]string, error) {
	if m.filterFunc != nil {
		return m.filterFunc(ctx, accountID, emailIDs, filter)
	}
	return emailIDs, nil
}

func TestHandler_TextFilter_UsesVectorSearch(t *testing.T) {
	vectorSearcher := &mockVectorSearcher{
		searchFunc: func(ctx context.Context, accountID string, filter *email.QueryFilter, position, limit int) (*search.SearchResult, error) {
			return &search.SearchResult{
				IDs:      []string{"email-1", "email-2"},
				Position: 0,
			}, nil
		},
	}

	mockEmail := &mockEmailRepository{}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox, vectorSearcher, nil, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"filter":    map[string]any{"text": "hello world"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Email/query" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/query")
	}

	ids, ok := response.MethodResponse.Args["ids"].([]string)
	if !ok {
		t.Fatalf("ids should be []string, got %T", response.MethodResponse.Args["ids"])
	}
	if len(ids) != 2 {
		t.Errorf("ids length = %d, want 2", len(ids))
	}

	// Vector searcher should have been called
	if vectorSearcher.searchCalls != 1 {
		t.Errorf("vectorSearcher.searchCalls = %d, want 1", vectorSearcher.searchCalls)
	}
}

func TestHandler_FromFilter_UsesTokenQuery(t *testing.T) {
	tokenQuerier := &mockTokenQuerier{
		queryFunc: func(ctx context.Context, accountID string, field email.TokenField, tokenPrefix string, limit int32, scanForward bool) ([]email.TokenQueryResult, error) {
			return []email.TokenQueryResult{
				{EmailID: "email-1"},
				{EmailID: "email-2"},
			}, nil
		},
	}

	emailFilter := &mockEmailFilter{
		filterFunc: func(ctx context.Context, accountID string, emailIDs []string, filter *email.QueryFilter) ([]string, error) {
			return emailIDs, nil
		},
	}

	mockEmail := &mockEmailRepository{}
	mockMailbox := &mockMailboxRepository{}

	h := newHandler(mockEmail, mockMailbox, nil, tokenQuerier, emailFilter)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"filter":    map[string]any{"from": "alice"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Email/query" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/query")
	}

	ids, ok := response.MethodResponse.Args["ids"].([]string)
	if !ok {
		t.Fatalf("ids should be []string, got %T", response.MethodResponse.Args["ids"])
	}
	if len(ids) != 2 {
		t.Errorf("ids length = %d, want 2", len(ids))
	}

	// Token querier should have been called
	if tokenQuerier.queryCalls != 1 {
		t.Errorf("tokenQuerier.queryCalls = %d, want 1", tokenQuerier.queryCalls)
	}
}

func TestHandler_StructuralFilter_UsesDynamoDB(t *testing.T) {
	var queryCallCount int
	mockEmail := &mockEmailRepository{
		queryFunc: func(ctx context.Context, accountID string, req *email.QueryRequest) (*email.QueryResult, error) {
			queryCallCount++
			return &email.QueryResult{IDs: []string{"email-1"}, Position: 0}, nil
		},
	}
	mockMailbox := &mockMailboxRepository{
		existsFunc: func(ctx context.Context, accountID, mailboxID string) (bool, error) {
			return true, nil
		},
	}

	vectorSearcher := &mockVectorSearcher{}
	tokenQuerier := &mockTokenQuerier{}

	h := newHandler(mockEmail, mockMailbox, vectorSearcher, tokenQuerier, nil)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/query",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"filter":    map[string]any{"inMailbox": "inbox-id", "hasKeyword": "$seen"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Email/query" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Email/query")
	}

	// DynamoDB query should have been used (not vector search or token query)
	if queryCallCount != 1 {
		t.Errorf("queryCallCount = %d, want 1", queryCallCount)
	}
	if vectorSearcher.searchCalls != 0 {
		t.Errorf("vectorSearcher should not have been called, got %d", vectorSearcher.searchCalls)
	}
	if tokenQuerier.queryCalls != 0 {
		t.Errorf("tokenQuerier should not have been called, got %d", tokenQuerier.queryCalls)
	}
}
