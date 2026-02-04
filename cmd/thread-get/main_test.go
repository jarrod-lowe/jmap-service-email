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

// mockEmailRepository implements the EmailRepository interface for testing.
type mockEmailRepository struct {
	findByThreadIDFunc func(ctx context.Context, accountID, threadID string) ([]*email.EmailItem, error)
}

func (m *mockEmailRepository) FindByThreadID(ctx context.Context, accountID, threadID string) ([]*email.EmailItem, error) {
	if m.findByThreadIDFunc != nil {
		return m.findByThreadIDFunc(ctx, accountID, threadID)
	}
	return nil, nil
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

func TestHandler_SingleThread(t *testing.T) {
	mockRepo := &mockEmailRepository{
		findByThreadIDFunc: func(ctx context.Context, accountID, threadID string) ([]*email.EmailItem, error) {
			if threadID == "thread-123" {
				return []*email.EmailItem{
					{EmailID: "email-1", ThreadID: "thread-123", ReceivedAt: time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)},
					{EmailID: "email-2", ThreadID: "thread-123", ReceivedAt: time.Date(2024, 1, 20, 11, 0, 0, 0, time.UTC)},
				}, nil
			}
			return nil, nil
		},
	}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Thread/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"thread-123"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should return Thread/get response
	if response.MethodResponse.Name != "Thread/get" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Thread/get")
	}

	// Should have accountId in args
	accountID, ok := response.MethodResponse.Args["accountId"].(string)
	if !ok || accountID != "user-123" {
		t.Errorf("accountId = %v, want %q", response.MethodResponse.Args["accountId"], "user-123")
	}

	// Should have list with one thread
	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok {
		t.Fatal("list should be an array")
	}
	if len(list) != 1 {
		t.Fatalf("list length = %d, want 1", len(list))
	}

	// Verify thread structure
	thread, ok := list[0].(map[string]any)
	if !ok {
		t.Fatal("thread should be a map")
	}
	if thread["id"] != "thread-123" {
		t.Errorf("thread id = %v, want %q", thread["id"], "thread-123")
	}

	// Verify emailIds are in order (sorted by receivedAt)
	emailIds, ok := thread["emailIds"].([]string)
	if !ok {
		t.Fatal("emailIds should be a string array")
	}
	if len(emailIds) != 2 {
		t.Fatalf("emailIds length = %d, want 2", len(emailIds))
	}
	if emailIds[0] != "email-1" {
		t.Errorf("emailIds[0] = %q, want %q", emailIds[0], "email-1")
	}
	if emailIds[1] != "email-2" {
		t.Errorf("emailIds[1] = %q, want %q", emailIds[1], "email-2")
	}

	// Should have empty notFound
	notFound, ok := response.MethodResponse.Args["notFound"].([]any)
	if !ok {
		t.Fatal("notFound should be an array")
	}
	if len(notFound) != 0 {
		t.Errorf("notFound length = %d, want 0", len(notFound))
	}
}

func TestHandler_ThreadNotFound(t *testing.T) {
	mockRepo := &mockEmailRepository{
		findByThreadIDFunc: func(ctx context.Context, accountID, threadID string) ([]*email.EmailItem, error) {
			return []*email.EmailItem{}, nil // Empty result = thread not found
		},
	}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Thread/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"nonexistent-thread"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Thread/get" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Thread/get")
	}

	// Should have empty list
	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok {
		t.Fatal("list should be an array")
	}
	if len(list) != 0 {
		t.Errorf("list length = %d, want 0", len(list))
	}

	// Should have notFound with the thread ID
	notFound, ok := response.MethodResponse.Args["notFound"].([]any)
	if !ok {
		t.Fatal("notFound should be an array")
	}
	if len(notFound) != 1 {
		t.Fatalf("notFound length = %d, want 1", len(notFound))
	}
	if notFound[0] != "nonexistent-thread" {
		t.Errorf("notFound[0] = %v, want %q", notFound[0], "nonexistent-thread")
	}
}

func TestHandler_MultipleThreads(t *testing.T) {
	mockRepo := &mockEmailRepository{
		findByThreadIDFunc: func(ctx context.Context, accountID, threadID string) ([]*email.EmailItem, error) {
			switch threadID {
			case "thread-1":
				return []*email.EmailItem{
					{EmailID: "email-1", ThreadID: "thread-1"},
				}, nil
			case "thread-2":
				return []*email.EmailItem{
					{EmailID: "email-2", ThreadID: "thread-2"},
					{EmailID: "email-3", ThreadID: "thread-2"},
				}, nil
			default:
				return []*email.EmailItem{}, nil
			}
		},
	}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Thread/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"thread-1", "thread-2", "nonexistent"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should have 2 threads in list
	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok {
		t.Fatal("list should be an array")
	}
	if len(list) != 2 {
		t.Errorf("list length = %d, want 2", len(list))
	}

	// Should have 1 notFound
	notFound, ok := response.MethodResponse.Args["notFound"].([]any)
	if !ok {
		t.Fatal("notFound should be an array")
	}
	if len(notFound) != 1 {
		t.Fatalf("notFound length = %d, want 1", len(notFound))
	}
	if notFound[0] != "nonexistent" {
		t.Errorf("notFound[0] = %v, want %q", notFound[0], "nonexistent")
	}
}

func TestHandler_QueryError(t *testing.T) {
	mockRepo := &mockEmailRepository{
		findByThreadIDFunc: func(ctx context.Context, accountID, threadID string) ([]*email.EmailItem, error) {
			return nil, errors.New("database error")
		},
	}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Thread/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"thread-123"},
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

	h := newHandler(mockRepo)

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

func TestHandler_MissingIds(t *testing.T) {
	mockRepo := &mockEmailRepository{}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Thread/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			// ids missing
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

func TestHandler_ReturnsActualState(t *testing.T) {
	mockRepo := &mockEmailRepository{
		findByThreadIDFunc: func(ctx context.Context, accountID, threadID string) ([]*email.EmailItem, error) {
			return []*email.EmailItem{
				{EmailID: "email-1", ThreadID: "thread-123"},
			}, nil
		},
	}

	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			if objectType != state.ObjectTypeThread {
				t.Errorf("Expected ObjectTypeThread, got %v", objectType)
			}
			return 42, nil // Return a specific state to verify
		},
	}

	h := newHandlerWithState(mockRepo, mockStateRepo, 5)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Thread/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"thread-123"},
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Thread/get" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Thread/get")
	}

	// Verify state is returned as "42" (not hardcoded "0")
	stateValue, ok := response.MethodResponse.Args["state"].(string)
	if !ok {
		t.Fatalf("state should be a string, got %T", response.MethodResponse.Args["state"])
	}
	if stateValue != "42" {
		t.Errorf("state = %q, want %q", stateValue, "42")
	}
}

func TestHandler_SoftDeletedEmailsExcludedFromThread(t *testing.T) {
	now := time.Now()
	mockRepo := &mockEmailRepository{
		findByThreadIDFunc: func(ctx context.Context, accountID, threadID string) ([]*email.EmailItem, error) {
			return []*email.EmailItem{
				{EmailID: "email-1", ThreadID: "thread-1", ReceivedAt: now},
				{EmailID: "email-2", ThreadID: "thread-1", ReceivedAt: now, DeletedAt: &now},
			}, nil
		},
	}

	h := newHandler(mockRepo)
	response, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-1",
		Method:    "Thread/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-1",
			"ids":       []any{"thread-1"},
		},
	})
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	list, ok := response.MethodResponse.Args["list"].([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("expected 1 thread in list, got %v", list)
	}
	thread := list[0].(map[string]any)
	emailIds := thread["emailIds"].([]string)
	if len(emailIds) != 1 || emailIds[0] != "email-1" {
		t.Errorf("emailIds = %v, want [email-1]", emailIds)
	}
}

func TestHandler_AllSoftDeletedThreadIsNotFound(t *testing.T) {
	now := time.Now()
	mockRepo := &mockEmailRepository{
		findByThreadIDFunc: func(ctx context.Context, accountID, threadID string) ([]*email.EmailItem, error) {
			return []*email.EmailItem{
				{EmailID: "email-1", ThreadID: "thread-1", ReceivedAt: now, DeletedAt: &now},
			}, nil
		},
	}

	h := newHandler(mockRepo)
	response, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-1",
		Method:    "Thread/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-1",
			"ids":       []any{"thread-1"},
		},
	})
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	notFound, ok := response.MethodResponse.Args["notFound"].([]any)
	if !ok || len(notFound) != 1 {
		t.Fatalf("expected 1 in notFound, got %v", notFound)
	}
}

func TestHandler_StateError_ReturnsServerFail(t *testing.T) {
	mockRepo := &mockEmailRepository{}

	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 0, errors.New("database error")
		},
	}

	h := newHandlerWithState(mockRepo, mockStateRepo, 5)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Thread/get",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			"ids":       []any{"thread-123"},
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

func TestHandler_ConcurrentThreadLookups(t *testing.T) {
	// Test that multiple threads are fetched and results preserve order
	mockRepo := &mockEmailRepository{
		findByThreadIDFunc: func(ctx context.Context, accountID, threadID string) ([]*email.EmailItem, error) {
			// Return different emails for different threads
			return []*email.EmailItem{
				{EmailID: "email-" + threadID, ThreadID: threadID},
			}, nil
		},
	}

	h := newHandler(mockRepo)

	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		Method:    "Thread/get",
		AccountID: "user-123",
		Args: map[string]any{
			"ids": []any{"thread-1", "thread-2", "thread-3"},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}
	if resp.MethodResponse.Name != "Thread/get" {
		t.Errorf("Name = %q, want %q", resp.MethodResponse.Name, "Thread/get")
	}

	list, ok := resp.MethodResponse.Args["list"].([]any)
	if !ok {
		t.Fatal("list should be an array")
	}
	if len(list) != 3 {
		t.Fatalf("list length = %d, want 3", len(list))
	}

	// Verify order is preserved
	for i, expected := range []string{"thread-1", "thread-2", "thread-3"} {
		thread, ok := list[i].(map[string]any)
		if !ok {
			t.Fatalf("list[%d] should be a map", i)
		}
		if thread["id"] != expected {
			t.Errorf("list[%d] id = %v, want %q", i, thread["id"], expected)
		}
	}
}
