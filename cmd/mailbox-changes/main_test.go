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
			// Verify correct object type is used
			if objectType != state.ObjectTypeMailbox {
				t.Errorf("objectType = %q, want %q", objectType, state.ObjectTypeMailbox)
			}
			return 5, nil
		},
		getOldestAvailableStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 1, nil
		},
		queryChangesFunc: func(ctx context.Context, accountID string, objectType state.ObjectType, sinceState int64, maxChanges int) ([]state.ChangeRecord, error) {
			return []state.ChangeRecord{
				{ObjectID: "mailbox-1", ChangeType: state.ChangeTypeCreated, State: 4},
				{ObjectID: "mailbox-2", ChangeType: state.ChangeTypeUpdated, State: 5},
			}, nil
		},
	}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Mailbox/changes",
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

	// Should return Mailbox/changes response
	if response.MethodResponse.Name != "Mailbox/changes" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "Mailbox/changes")
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

	// Should have created list with one mailbox
	created, ok := response.MethodResponse.Args["created"].([]string)
	if !ok {
		t.Fatalf("created should be []string, got %T", response.MethodResponse.Args["created"])
	}
	if len(created) != 1 {
		t.Errorf("created length = %d, want 1", len(created))
	}

	// Should have updated list with one mailbox
	updated, ok := response.MethodResponse.Args["updated"].([]string)
	if !ok {
		t.Fatalf("updated should be []string, got %T", response.MethodResponse.Args["updated"])
	}
	if len(updated) != 1 {
		t.Errorf("updated length = %d, want 1", len(updated))
	}
}

func TestHandler_CannotCalculateChanges_GapDetected(t *testing.T) {
	mockRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 100, nil
		},
		getOldestAvailableStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 50, nil
		},
	}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Mailbox/changes",
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
		Method:    "Mailbox/changes",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId": "user-123",
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

	if response.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "error")
	}

	errorType, ok := response.MethodResponse.Args["type"].(string)
	if !ok || errorType != "unknownMethod" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "unknownMethod")
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
		Method:    "Mailbox/changes",
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

	if response.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "error")
	}

	errorType, ok := response.MethodResponse.Args["type"].(string)
	if !ok || errorType != "serverFail" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "serverFail")
	}
}

func TestHandler_ResponseIncludesUpdatedProperties(t *testing.T) {
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
		Method:    "Mailbox/changes",
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

	if response.MethodResponse.Name != "Mailbox/changes" {
		t.Fatalf("Name = %q, want %q", response.MethodResponse.Name, "Mailbox/changes")
	}

	// RFC 8621 requires updatedProperties field (null if server cannot determine which properties changed)
	_, hasUpdatedProperties := response.MethodResponse.Args["updatedProperties"]
	if !hasUpdatedProperties {
		t.Error("response should include updatedProperties field (even if null)")
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
		Method:    "Mailbox/changes",
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
		Method:    "Mailbox/changes",
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

	// Per RFC 8620 §5.2, invalid sinceState must return cannotCalculateChanges
	if response.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", response.MethodResponse.Name, "error")
	}

	errorType, ok := response.MethodResponse.Args["type"].(string)
	if !ok || errorType != "cannotCalculateChanges" {
		t.Errorf("error type = %v, want %q", response.MethodResponse.Args["type"], "cannotCalculateChanges")
	}
}

func TestHandler_DestroyedMailbox(t *testing.T) {
	mockRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 10, nil
		},
		getOldestAvailableStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 1, nil
		},
		queryChangesFunc: func(ctx context.Context, accountID string, objectType state.ObjectType, sinceState int64, maxChanges int) ([]state.ChangeRecord, error) {
			return []state.ChangeRecord{
				{ObjectID: "mailbox-1", ChangeType: state.ChangeTypeDestroyed, State: 10},
			}, nil
		},
	}

	h := newHandler(mockRepo)

	request := plugincontract.PluginInvocationRequest{
		RequestID: "req-123",
		AccountID: "user-123",
		Method:    "Mailbox/changes",
		ClientID:  "c0",
		Args: map[string]any{
			"accountId":  "user-123",
			"sinceState": "9",
		},
	}

	response, err := h.handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if response.MethodResponse.Name != "Mailbox/changes" {
		t.Fatalf("Name = %q, want %q", response.MethodResponse.Name, "Mailbox/changes")
	}

	destroyed, ok := response.MethodResponse.Args["destroyed"].([]string)
	if !ok {
		t.Fatalf("destroyed should be []string, got %T", response.MethodResponse.Args["destroyed"])
	}
	if len(destroyed) != 1 || destroyed[0] != "mailbox-1" {
		t.Errorf("destroyed = %v, want [mailbox-1]", destroyed)
	}
}
