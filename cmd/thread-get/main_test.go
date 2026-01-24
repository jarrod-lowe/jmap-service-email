package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
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
