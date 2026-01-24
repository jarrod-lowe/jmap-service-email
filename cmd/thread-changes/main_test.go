package main

import (
	"context"
	"errors"
	"testing"

	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
)

// mockStateRepository implements the StateRepository interface for testing.
type mockStateRepository struct {
	getCurrentStateFunc         func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
	queryChangesFunc            func(ctx context.Context, accountID string, objectType state.ObjectType, sinceState int64, maxChanges int) ([]state.ChangeRecord, error)
	getOldestAvailableStateFunc func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
}

func (m *mockStateRepository) GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
	if m.getCurrentStateFunc != nil {
		return m.getCurrentStateFunc(ctx, accountID, objectType)
	}
	return 0, nil
}

func (m *mockStateRepository) QueryChanges(ctx context.Context, accountID string, objectType state.ObjectType, sinceState int64, maxChanges int) ([]state.ChangeRecord, error) {
	if m.queryChangesFunc != nil {
		return m.queryChangesFunc(ctx, accountID, objectType, sinceState, maxChanges)
	}
	return nil, nil
}

func (m *mockStateRepository) GetOldestAvailableState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
	if m.getOldestAvailableStateFunc != nil {
		return m.getOldestAvailableStateFunc(ctx, accountID, objectType)
	}
	return 0, nil
}

func TestHandler_BasicChanges(t *testing.T) {
	mockRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			if objectType != state.ObjectTypeThread {
				t.Errorf("Expected ObjectTypeThread, got %v", objectType)
			}
			return 5, nil
		},
		getOldestAvailableStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 1, nil
		},
		queryChangesFunc: func(ctx context.Context, accountID string, objectType state.ObjectType, sinceState int64, maxChanges int) ([]state.ChangeRecord, error) {
			// Thread changes can be created (new thread) or updated (reply joined)
			return []state.ChangeRecord{
				{ObjectID: "thread-1", ChangeType: state.ChangeTypeCreated, State: 4},
				{ObjectID: "thread-2", ChangeType: state.ChangeTypeUpdated, State: 5},
			}, nil
		},
	}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Thread/changes",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":  "user-123",
			"sinceState": "3",
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should return Thread/changes response
	if response.MethodResponse.Name != "Thread/changes" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Thread/changes")
	}

	// Should have accountId
	accountID, ok := response.MethodResponse.Args["accountId"].(string)
	if !ok || accountID != "user-123" {
		t.Errorf("accountId = %v, want %q", response.MethodResponse.Args["accountId"], "user-123")
	}

	// Should have oldState matching sinceState
	oldState, ok := response.MethodResponse.Args["oldState"].(string)
	if !ok || oldState != "3" {
		t.Errorf("oldState = %v, want %q", response.MethodResponse.Args["oldState"], "3")
	}

	// Should have newState matching current state
	newState, ok := response.MethodResponse.Args["newState"].(string)
	if !ok || newState != "5" {
		t.Errorf("newState = %v, want %q", response.MethodResponse.Args["newState"], "5")
	}

	// Should have hasMoreChanges
	hasMore, ok := response.MethodResponse.Args["hasMoreChanges"].(bool)
	if !ok || hasMore != false {
		t.Errorf("hasMoreChanges = %v, want false", response.MethodResponse.Args["hasMoreChanges"])
	}

	// Thread/changes should categorize by change type like Email/changes
	// thread-1 has ChangeTypeCreated → should be in created
	// thread-2 has ChangeTypeUpdated → should be in updated
	created, ok := response.MethodResponse.Args["created"].([]string)
	if !ok {
		t.Fatalf("created should be []string, got %T", response.MethodResponse.Args["created"])
	}
	if len(created) != 1 {
		t.Errorf("created length = %d, want 1", len(created))
	}
	if len(created) > 0 && created[0] != "thread-1" {
		t.Errorf("created[0] = %q, want %q", created[0], "thread-1")
	}

	updated, ok := response.MethodResponse.Args["updated"].([]string)
	if !ok {
		t.Fatalf("updated should be []string, got %T", response.MethodResponse.Args["updated"])
	}
	if len(updated) != 1 {
		t.Errorf("updated length = %d, want 1", len(updated))
	}
	if len(updated) > 0 && updated[0] != "thread-2" {
		t.Errorf("updated[0] = %q, want %q", updated[0], "thread-2")
	}

	destroyed, ok := response.MethodResponse.Args["destroyed"].([]string)
	if !ok {
		t.Fatalf("destroyed should be []string, got %T", response.MethodResponse.Args["destroyed"])
	}
	if len(destroyed) != 0 {
		t.Errorf("destroyed length = %d, want 0", len(destroyed))
	}
}

func TestHandler_DeduplicatesThreadChanges(t *testing.T) {
	mockRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 10, nil
		},
		getOldestAvailableStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 1, nil
		},
		queryChangesFunc: func(ctx context.Context, accountID string, objectType state.ObjectType, sinceState int64, maxChanges int) ([]state.ChangeRecord, error) {
			// Same thread appears multiple times (e.g., multiple emails added to same thread)
			return []state.ChangeRecord{
				{ObjectID: "thread-1", ChangeType: state.ChangeTypeUpdated, State: 8},
				{ObjectID: "thread-1", ChangeType: state.ChangeTypeUpdated, State: 9},
				{ObjectID: "thread-2", ChangeType: state.ChangeTypeUpdated, State: 10},
			}, nil
		},
	}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Thread/changes",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":  "user-123",
			"sinceState": "7",
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Thread/changes" {
		t.Fatalf("Name = %q, want %q", response.MethodResponse.Name, "Thread/changes")
	}

	// Should deduplicate - thread-1 should appear only once
	updated, ok := response.MethodResponse.Args["updated"].([]string)
	if !ok {
		t.Fatalf("updated should be []string, got %T", response.MethodResponse.Args["updated"])
	}
	if len(updated) != 2 {
		t.Errorf("updated length = %d, want 2 (thread-1 deduplicated)", len(updated))
	}

	// Verify thread-1 appears exactly once
	count := 0
	for _, id := range updated {
		if id == "thread-1" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("thread-1 appears %d times, want 1", count)
	}
}

func TestHandler_CannotCalculateChanges_GapDetected(t *testing.T) {
	mockRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 100, nil
		},
		getOldestAvailableStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			// Oldest available is 50, but sinceState is 10 - gap detected
			return 50, nil
		},
	}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Thread/changes",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":  "user-123",
			"sinceState": "10",
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should return cannotCalculateChanges error
	if response.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "error")
	}

	errorType, ok := response.MethodResponse.Args["type"].(string)
	if !ok || errorType != "cannotCalculateChanges" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "cannotCalculateChanges")
	}
}

func TestHandler_MissingSinceState(t *testing.T) {
	mockRepo := &mockStateRepository{}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Thread/changes",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
			// No sinceState
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should return invalidArguments error
	if response.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "error")
	}

	errorType, ok := response.MethodResponse.Args["type"].(string)
	if !ok || errorType != "invalidArguments" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "invalidArguments")
	}
}

func TestHandler_InvalidMethod(t *testing.T) {
	mockRepo := &mockStateRepository{}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Email/changes",
		ClientID:  "c0",
		Args:      map[string]any{},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should return unknownMethod error
	if response.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "error")
	}

	errorType, ok := response.MethodResponse.Args["type"].(string)
	if !ok || errorType != "unknownMethod" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "unknownMethod")
	}
}

func TestHandler_NoChanges(t *testing.T) {
	mockRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 5, nil
		},
		getOldestAvailableStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 1, nil
		},
		queryChangesFunc: func(ctx context.Context, accountID string, objectType state.ObjectType, sinceState int64, maxChanges int) ([]state.ChangeRecord, error) {
			return []state.ChangeRecord{}, nil
		},
	}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Thread/changes",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":  "user-123",
			"sinceState": "5",
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Thread/changes" {
		t.Fatalf("Name = %q, want %q", response.MethodResponse.Name, "Thread/changes")
	}

	// oldState and newState should be the same
	oldState, ok := response.MethodResponse.Args["oldState"].(string)
	if !ok {
		t.Fatalf("oldState should be string, got %T", response.MethodResponse.Args["oldState"])
	}
	newState, ok := response.MethodResponse.Args["newState"].(string)
	if !ok {
		t.Fatalf("newState should be string, got %T", response.MethodResponse.Args["newState"])
	}
	if oldState != "5" || newState != "5" {
		t.Errorf("states = %q/%q, want 5/5", oldState, newState)
	}

	// All lists should be empty
	created, ok := response.MethodResponse.Args["created"].([]string)
	if !ok {
		t.Fatalf("created should be []string, got %T", response.MethodResponse.Args["created"])
	}
	updated, ok := response.MethodResponse.Args["updated"].([]string)
	if !ok {
		t.Fatalf("updated should be []string, got %T", response.MethodResponse.Args["updated"])
	}
	destroyed, ok := response.MethodResponse.Args["destroyed"].([]string)
	if !ok {
		t.Fatalf("destroyed should be []string, got %T", response.MethodResponse.Args["destroyed"])
	}

	if len(created) != 0 || len(updated) != 0 || len(destroyed) != 0 {
		t.Errorf("expected empty lists, got created=%v updated=%v destroyed=%v", created, updated, destroyed)
	}
}

func TestHandler_MaxChanges(t *testing.T) {
	mockRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 100, nil
		},
		getOldestAvailableStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 1, nil
		},
		queryChangesFunc: func(ctx context.Context, accountID string, objectType state.ObjectType, sinceState int64, maxChanges int) ([]state.ChangeRecord, error) {
			// Verify maxChanges was passed
			if maxChanges != 10 {
				t.Errorf("maxChanges = %d, want 10", maxChanges)
			}
			// Return exactly maxChanges items
			changes := make([]state.ChangeRecord, 10)
			for i := 0; i < 10; i++ {
				changes[i] = state.ChangeRecord{
					ObjectID:   "thread-" + string(rune('a'+i)),
					ChangeType: state.ChangeTypeUpdated,
					State:      int64(i + 1),
				}
			}
			return changes, nil
		},
	}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Thread/changes",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":  "user-123",
			"sinceState": "0",
			"maxChanges": float64(10), // JSON numbers come as float64
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Thread/changes" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Thread/changes")
	}

	// hasMoreChanges should be true since there are more changes
	hasMore, ok := response.MethodResponse.Args["hasMoreChanges"].(bool)
	if !ok || !hasMore {
		t.Errorf("hasMoreChanges = %v, want true", hasMore)
	}
}

func TestHandler_RepositoryError(t *testing.T) {
	mockRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 0, errors.New("database error")
		},
	}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Thread/changes",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":  "user-123",
			"sinceState": "0",
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should return serverFail error
	if response.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "error")
	}

	errorType, ok := response.MethodResponse.Args["type"].(string)
	if !ok || errorType != "serverFail" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "serverFail")
	}
}

func TestHandler_CannotCalculateChanges_FutureState(t *testing.T) {
	mockRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 50, nil // current state is 50
		},
		getOldestAvailableStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 1, nil
		},
	}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Thread/changes",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":  "user-123",
			"sinceState": "100", // sinceState > currentState (future state)
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should return cannotCalculateChanges error for future state
	if response.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "error")
	}

	errorType, ok := response.MethodResponse.Args["type"].(string)
	if !ok || errorType != "cannotCalculateChanges" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "cannotCalculateChanges")
	}
}

func TestHandler_CannotCalculateChanges_UnparseableSinceState(t *testing.T) {
	mockRepo := &mockStateRepository{}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Thread/changes",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":  "user-123",
			"sinceState": "not-a-number", // Invalid state (not parseable as integer)
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Per RFC 8620 section 5.2, invalid sinceState must return cannotCalculateChanges
	if response.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "error")
	}

	errorType, ok := response.MethodResponse.Args["type"].(string)
	if !ok || errorType != "cannotCalculateChanges" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "cannotCalculateChanges")
	}
}
