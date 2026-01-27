package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
)

type mockEmailRepository struct {
	updateEmailMailboxesFunc  func(ctx context.Context, accountID, emailID string, newMailboxIDs map[string]bool) (map[string]bool, *email.EmailItem, error)
	getEmailFunc              func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error)
	updateEmailKeywordsFunc   func(ctx context.Context, accountID, emailID string, newKeywords map[string]bool, expectedVersion int) (*email.EmailItem, error)
	buildDeleteEmailItemsFunc func(emailItem *email.EmailItem) []types.TransactWriteItem
}

func (m *mockEmailRepository) UpdateEmailMailboxes(ctx context.Context, accountID, emailID string, newMailboxIDs map[string]bool) (map[string]bool, *email.EmailItem, error) {
	if m.updateEmailMailboxesFunc != nil {
		return m.updateEmailMailboxesFunc(ctx, accountID, emailID, newMailboxIDs)
	}
	return nil, nil, email.ErrEmailNotFound
}

func (m *mockEmailRepository) GetEmail(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
	if m.getEmailFunc != nil {
		return m.getEmailFunc(ctx, accountID, emailID)
	}
	return nil, email.ErrEmailNotFound
}

func (m *mockEmailRepository) UpdateEmailKeywords(ctx context.Context, accountID, emailID string, newKeywords map[string]bool, expectedVersion int) (*email.EmailItem, error) {
	if m.updateEmailKeywordsFunc != nil {
		return m.updateEmailKeywordsFunc(ctx, accountID, emailID, newKeywords, expectedVersion)
	}
	return nil, email.ErrEmailNotFound
}

func (m *mockEmailRepository) BuildDeleteEmailItems(emailItem *email.EmailItem) []types.TransactWriteItem {
	if m.buildDeleteEmailItemsFunc != nil {
		return m.buildDeleteEmailItemsFunc(emailItem)
	}
	return []types.TransactWriteItem{}
}

type mockMailboxRepository struct {
	mailboxExistsFunc            func(ctx context.Context, accountID, mailboxID string) (bool, error)
	incrementCountsFunc          func(ctx context.Context, accountID, mailboxID string, incrementUnread bool) error
	decrementCountsFunc          func(ctx context.Context, accountID, mailboxID string, decrementUnread bool) error
	buildDecrementCountsItemsFunc func(accountID, mailboxID string, decrementUnread bool) types.TransactWriteItem
}

func (m *mockMailboxRepository) MailboxExists(ctx context.Context, accountID, mailboxID string) (bool, error) {
	if m.mailboxExistsFunc != nil {
		return m.mailboxExistsFunc(ctx, accountID, mailboxID)
	}
	return true, nil
}

func (m *mockMailboxRepository) IncrementCounts(ctx context.Context, accountID, mailboxID string, incrementUnread bool) error {
	if m.incrementCountsFunc != nil {
		return m.incrementCountsFunc(ctx, accountID, mailboxID, incrementUnread)
	}
	return nil
}

func (m *mockMailboxRepository) DecrementCounts(ctx context.Context, accountID, mailboxID string, decrementUnread bool) error {
	if m.decrementCountsFunc != nil {
		return m.decrementCountsFunc(ctx, accountID, mailboxID, decrementUnread)
	}
	return nil
}

func (m *mockMailboxRepository) BuildDecrementCountsItems(accountID, mailboxID string, decrementUnread bool) types.TransactWriteItem {
	if m.buildDecrementCountsItemsFunc != nil {
		return m.buildDecrementCountsItemsFunc(accountID, mailboxID, decrementUnread)
	}
	return types.TransactWriteItem{}
}

type mockStateRepository struct {
	getCurrentStateFunc            func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
	incrementStateAndLogChangeFunc func(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error)
	buildStateChangeItemsFunc      func(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem)
}

func (m *mockStateRepository) GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
	if m.getCurrentStateFunc != nil {
		return m.getCurrentStateFunc(ctx, accountID, objectType)
	}
	return 0, nil
}

func (m *mockStateRepository) IncrementStateAndLogChange(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error) {
	if m.incrementStateAndLogChangeFunc != nil {
		return m.incrementStateAndLogChangeFunc(ctx, accountID, objectType, objectID, changeType)
	}
	return 0, nil
}

func (m *mockStateRepository) BuildStateChangeItems(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem) {
	if m.buildStateChangeItemsFunc != nil {
		return m.buildStateChangeItemsFunc(accountID, objectType, currentState, objectID, changeType)
	}
	return currentState + 1, []types.TransactWriteItem{{}, {}}
}

type mockBlobDeletePublisher struct {
	publishFunc func(ctx context.Context, accountID string, blobIDs []string) error
}

func (m *mockBlobDeletePublisher) PublishBlobDeletions(ctx context.Context, accountID string, blobIDs []string) error {
	if m.publishFunc != nil {
		return m.publishFunc(ctx, accountID, blobIDs)
	}
	return nil
}

type mockTransactWriter struct {
	transactWriteItemsFunc func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

func (m *mockTransactWriter) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if m.transactWriteItemsFunc != nil {
		return m.transactWriteItemsFunc(ctx, input, opts...)
	}
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

// Test: Wrong method returns unknownMethod error
func TestHandler_InvalidMethod(t *testing.T) {
	h := newHandler(&mockEmailRepository{}, &mockMailboxRepository{}, nil, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/get", // Wrong method
		Args:      map[string]any{},
		ClientID:  "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if resp.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", resp.MethodResponse.Name, "error")
	}
	if resp.MethodResponse.Args["type"] != "unknownMethod" {
		t.Errorf("type = %v, want %q", resp.MethodResponse.Args["type"], "unknownMethod")
	}
}

// Test: Full replacement of mailboxIds
func TestHandler_UpdateMailboxIds_FullReplacement(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)
	var capturedNewMailboxIDs map[string]bool

	mockEmailRepo := &mockEmailRepository{
		updateEmailMailboxesFunc: func(ctx context.Context, accountID, emailID string, newMailboxIDs map[string]bool) (map[string]bool, *email.EmailItem, error) {
			capturedNewMailboxIDs = newMailboxIDs
			return map[string]bool{"inbox-id": true}, &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				Keywords:   map[string]bool{"$seen": true},
			}, nil
		},
	}

	h := newHandler(mockEmailRepo, &mockMailboxRepository{}, nil, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"update": map[string]any{
				"email-456": map[string]any{
					"mailboxIds": map[string]any{
						"inbox-id":   true,
						"archive-id": true,
					},
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if resp.MethodResponse.Name != "Email/set" {
		t.Errorf("Name = %q, want %q", resp.MethodResponse.Name, "Email/set")
	}

	updated, ok := resp.MethodResponse.Args["updated"].(map[string]any)
	if !ok {
		t.Fatalf("updated not a map: %T", resp.MethodResponse.Args["updated"])
	}
	if _, ok := updated["email-456"]; !ok {
		t.Error("email-456 should be in updated")
	}

	// Verify the new mailboxIds were passed correctly
	if capturedNewMailboxIDs == nil {
		t.Fatal("UpdateEmailMailboxes was not called")
	}
	if len(capturedNewMailboxIDs) != 2 {
		t.Errorf("capturedNewMailboxIDs = %v, want 2 entries", capturedNewMailboxIDs)
	}
	if !capturedNewMailboxIDs["inbox-id"] || !capturedNewMailboxIDs["archive-id"] {
		t.Errorf("capturedNewMailboxIDs = %v, want inbox-id and archive-id", capturedNewMailboxIDs)
	}
}

// Test: Patch syntax - add mailbox
func TestHandler_UpdateMailboxIds_PatchAdd(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)
	var capturedNewMailboxIDs map[string]bool

	mockEmailRepo := &mockEmailRepository{
		// GetEmail is called first to get current mailboxIds for patch mode
		getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				MailboxIDs: map[string]bool{"inbox-id": true}, // Currently in inbox only
			}, nil
		},
		updateEmailMailboxesFunc: func(ctx context.Context, accountID, emailID string, newMailboxIDs map[string]bool) (map[string]bool, *email.EmailItem, error) {
			capturedNewMailboxIDs = newMailboxIDs
			// Return old mailboxIds (email was in inbox only)
			return map[string]bool{"inbox-id": true}, &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				MailboxIDs: newMailboxIDs,
			}, nil
		},
	}

	h := newHandler(mockEmailRepo, &mockMailboxRepository{}, nil, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"update": map[string]any{
				"email-456": map[string]any{
					"mailboxIds/archive-id": true, // Patch: add archive-id
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	updated, ok := resp.MethodResponse.Args["updated"].(map[string]any)
	if !ok {
		t.Fatalf("updated not a map: %T", resp.MethodResponse.Args["updated"])
	}
	if _, ok := updated["email-456"]; !ok {
		t.Error("email-456 should be in updated")
	}

	// Verify the patch was applied correctly (inbox + archive)
	if capturedNewMailboxIDs == nil {
		t.Fatal("UpdateEmailMailboxes was not called")
	}
	if len(capturedNewMailboxIDs) != 2 {
		t.Errorf("capturedNewMailboxIDs = %v, want 2 entries", capturedNewMailboxIDs)
	}
}

// Test: Patch syntax - remove mailbox (null value)
func TestHandler_UpdateMailboxIds_PatchRemove(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)
	var capturedNewMailboxIDs map[string]bool

	mockEmailRepo := &mockEmailRepository{
		// GetEmail is called first to get current mailboxIds for patch mode
		getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				MailboxIDs: map[string]bool{"inbox-id": true, "archive-id": true}, // Currently in inbox + archive
			}, nil
		},
		updateEmailMailboxesFunc: func(ctx context.Context, accountID, emailID string, newMailboxIDs map[string]bool) (map[string]bool, *email.EmailItem, error) {
			capturedNewMailboxIDs = newMailboxIDs
			// Return old mailboxIds (email was in inbox + archive)
			return map[string]bool{"inbox-id": true, "archive-id": true}, &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				MailboxIDs: newMailboxIDs,
			}, nil
		},
	}

	h := newHandler(mockEmailRepo, &mockMailboxRepository{}, nil, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"update": map[string]any{
				"email-456": map[string]any{
					"mailboxIds/archive-id": nil, // Patch: remove archive-id
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	updated, ok := resp.MethodResponse.Args["updated"].(map[string]any)
	if !ok {
		t.Fatalf("updated not a map: %T", resp.MethodResponse.Args["updated"])
	}
	if _, ok := updated["email-456"]; !ok {
		t.Error("email-456 should be in updated")
	}

	// Verify the patch was applied correctly (only inbox)
	if capturedNewMailboxIDs == nil {
		t.Fatal("UpdateEmailMailboxes was not called")
	}
	if len(capturedNewMailboxIDs) != 1 || !capturedNewMailboxIDs["inbox-id"] {
		t.Errorf("capturedNewMailboxIDs = %v, want {inbox-id: true}", capturedNewMailboxIDs)
	}
}

// Test: Update non-existent email returns notFound
func TestHandler_UpdateEmailNotFound(t *testing.T) {
	mockEmailRepo := &mockEmailRepository{
		updateEmailMailboxesFunc: func(ctx context.Context, accountID, emailID string, newMailboxIDs map[string]bool) (map[string]bool, *email.EmailItem, error) {
			return nil, nil, email.ErrEmailNotFound
		},
	}

	h := newHandler(mockEmailRepo, &mockMailboxRepository{}, nil, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"update": map[string]any{
				"nonexistent": map[string]any{
					"mailboxIds": map[string]any{"inbox-id": true},
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notUpdated, ok := resp.MethodResponse.Args["notUpdated"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated not a map: %T", resp.MethodResponse.Args["notUpdated"])
	}
	item, ok := notUpdated["nonexistent"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated[nonexistent] not a map: %T", notUpdated["nonexistent"])
	}
	if item["type"] != "notFound" {
		t.Errorf("type = %v, want %q", item["type"], "notFound")
	}
}

// Test: Update with invalid mailbox ID returns invalidProperties
func TestHandler_UpdateInvalidMailbox(t *testing.T) {
	mockEmailRepo := &mockEmailRepository{}
	mockMailboxRepo := &mockMailboxRepository{
		mailboxExistsFunc: func(ctx context.Context, accountID, mailboxID string) (bool, error) {
			return false, nil // Mailbox doesn't exist
		},
	}

	h := newHandler(mockEmailRepo, mockMailboxRepo, nil, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"update": map[string]any{
				"email-456": map[string]any{
					"mailboxIds": map[string]any{"nonexistent-mailbox": true},
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notUpdated, ok := resp.MethodResponse.Args["notUpdated"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated not a map: %T", resp.MethodResponse.Args["notUpdated"])
	}
	item, ok := notUpdated["email-456"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated[email-456] not a map: %T", notUpdated["email-456"])
	}
	if item["type"] != "invalidProperties" {
		t.Errorf("type = %v, want %q", item["type"], "invalidProperties")
	}
}

// Test: Update removing all mailboxes returns error
func TestHandler_UpdateNoMailboxes(t *testing.T) {
	h := newHandler(&mockEmailRepository{}, &mockMailboxRepository{}, nil, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"update": map[string]any{
				"email-456": map[string]any{
					"mailboxIds": map[string]any{}, // Empty mailboxIds
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notUpdated, ok := resp.MethodResponse.Args["notUpdated"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated not a map: %T", resp.MethodResponse.Args["notUpdated"])
	}
	item, ok := notUpdated["email-456"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated[email-456] not a map: %T", notUpdated["email-456"])
	}
	if item["type"] != "invalidProperties" {
		t.Errorf("type = %v, want %q", item["type"], "invalidProperties")
	}
}

// Test: Mailbox counters are updated correctly
func TestHandler_UpdateMailboxCounters(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)
	var incrementedMailboxes []string
	var decrementedMailboxes []string

	mockEmailRepo := &mockEmailRepository{
		updateEmailMailboxesFunc: func(ctx context.Context, accountID, emailID string, newMailboxIDs map[string]bool) (map[string]bool, *email.EmailItem, error) {
			// Return old mailboxIds
			return map[string]bool{"inbox-id": true}, &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				MailboxIDs: newMailboxIDs,
				Keywords:   map[string]bool{}, // Seen, so don't affect unread
			}, nil
		},
	}

	mockMailboxRepo := &mockMailboxRepository{
		incrementCountsFunc: func(ctx context.Context, accountID, mailboxID string, incrementUnread bool) error {
			incrementedMailboxes = append(incrementedMailboxes, mailboxID)
			return nil
		},
		decrementCountsFunc: func(ctx context.Context, accountID, mailboxID string, decrementUnread bool) error {
			decrementedMailboxes = append(decrementedMailboxes, mailboxID)
			return nil
		},
	}

	h := newHandler(mockEmailRepo, mockMailboxRepo, nil, nil, nil)
	_, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"update": map[string]any{
				"email-456": map[string]any{
					"mailboxIds": map[string]any{
						"archive-id": true, // Move to archive (remove from inbox)
					},
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	// Should have incremented archive-id
	if len(incrementedMailboxes) != 1 || incrementedMailboxes[0] != "archive-id" {
		t.Errorf("incrementedMailboxes = %v, want [archive-id]", incrementedMailboxes)
	}

	// Should have decremented inbox-id
	if len(decrementedMailboxes) != 1 || decrementedMailboxes[0] != "inbox-id" {
		t.Errorf("decrementedMailboxes = %v, want [inbox-id]", decrementedMailboxes)
	}
}

// Test: State tracking on update
func TestHandler_StateTracking(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)
	var stateChanges []struct {
		objectType state.ObjectType
		objectID   string
		changeType state.ChangeType
	}

	mockEmailRepo := &mockEmailRepository{
		updateEmailMailboxesFunc: func(ctx context.Context, accountID, emailID string, newMailboxIDs map[string]bool) (map[string]bool, *email.EmailItem, error) {
			return map[string]bool{"inbox-id": true}, &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				MailboxIDs: newMailboxIDs,
			}, nil
		},
	}

	currentState := int64(10)
	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return currentState, nil
		},
		incrementStateAndLogChangeFunc: func(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error) {
			stateChanges = append(stateChanges, struct {
				objectType state.ObjectType
				objectID   string
				changeType state.ChangeType
			}{objectType, objectID, changeType})
			currentState++
			return currentState, nil
		},
	}

	h := newHandler(mockEmailRepo, &mockMailboxRepository{}, mockStateRepo, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"update": map[string]any{
				"email-456": map[string]any{
					"mailboxIds": map[string]any{
						"inbox-id":   true,
						"archive-id": true,
					},
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	// Check state values in response
	oldState, ok := resp.MethodResponse.Args["oldState"].(string)
	if !ok || oldState != "10" {
		t.Errorf("oldState = %v, want %q", resp.MethodResponse.Args["oldState"], "10")
	}

	// Should have Email updated + 1 Mailbox updated (archive added)
	// Email state change + affected mailbox state changes
	hasEmailUpdate := false
	hasMailboxUpdate := false
	for _, change := range stateChanges {
		if change.objectType == state.ObjectTypeEmail && change.changeType == state.ChangeTypeUpdated {
			hasEmailUpdate = true
		}
		if change.objectType == state.ObjectTypeMailbox && change.changeType == state.ChangeTypeUpdated {
			hasMailboxUpdate = true
		}
	}
	if !hasEmailUpdate {
		t.Error("expected Email state change")
	}
	if !hasMailboxUpdate {
		t.Error("expected Mailbox state change for affected mailboxes")
	}
}

// Test: ifInState mismatch returns stateMismatch (string value)
func TestHandler_IfInStateMismatch(t *testing.T) {
	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 5, nil
		},
	}

	h := newHandler(&mockEmailRepository{}, &mockMailboxRepository{}, mockStateRepo, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"ifInState": "10", // Server is at state 5
			"update": map[string]any{
				"email-456": map[string]any{
					"mailboxIds": map[string]any{"inbox-id": true},
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if resp.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", resp.MethodResponse.Name, "error")
	}
	if resp.MethodResponse.Args["type"] != "stateMismatch" {
		t.Errorf("type = %v, want %q", resp.MethodResponse.Args["type"], "stateMismatch")
	}
}

// Test: ifInState mismatch returns stateMismatch (numeric value)
// JSON numbers unmarshal as float64 in Go. This test verifies numeric ifInState values are handled.
func TestHandler_IfInStateMismatch_NumericValue(t *testing.T) {
	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 5, nil
		},
	}

	h := newHandler(&mockEmailRepository{}, &mockMailboxRepository{}, mockStateRepo, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"ifInState": float64(10), // Numeric value (JSON unmarshals numbers as float64)
			"update": map[string]any{
				"email-456": map[string]any{
					"mailboxIds": map[string]any{"inbox-id": true},
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if resp.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", resp.MethodResponse.Name, "error")
	}
	if resp.MethodResponse.Args["type"] != "stateMismatch" {
		t.Errorf("type = %v, want %q", resp.MethodResponse.Args["type"], "stateMismatch")
	}
}

// Test: ifInState with invalid (non-numeric) string returns stateMismatch
func TestHandler_IfInStateMismatch_InvalidString(t *testing.T) {
	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 5, nil
		},
	}

	h := newHandler(&mockEmailRepository{}, &mockMailboxRepository{}, mockStateRepo, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"ifInState": "wrong-state-value-12345", // Invalid non-numeric state
			"update": map[string]any{
				"email-456": map[string]any{
					"mailboxIds": map[string]any{"inbox-id": true},
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if resp.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", resp.MethodResponse.Name, "error")
	}
	if resp.MethodResponse.Args["type"] != "stateMismatch" {
		t.Errorf("type = %v, want %q", resp.MethodResponse.Args["type"], "stateMismatch")
	}
}

// Test: Repository transaction error returns serverFail
func TestHandler_TransactionError(t *testing.T) {
	mockEmailRepo := &mockEmailRepository{
		updateEmailMailboxesFunc: func(ctx context.Context, accountID, emailID string, newMailboxIDs map[string]bool) (map[string]bool, *email.EmailItem, error) {
			return nil, nil, errors.New("transaction failed")
		},
	}

	h := newHandler(mockEmailRepo, &mockMailboxRepository{}, nil, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"update": map[string]any{
				"email-456": map[string]any{
					"mailboxIds": map[string]any{"inbox-id": true},
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notUpdated, ok := resp.MethodResponse.Args["notUpdated"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated not a map: %T", resp.MethodResponse.Args["notUpdated"])
	}
	item, ok := notUpdated["email-456"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated[email-456] not a map: %T", notUpdated["email-456"])
	}
	if item["type"] != "serverFail" {
		t.Errorf("type = %v, want %q", item["type"], "serverFail")
	}
}

// Test: Keywords update - full replacement
func TestHandler_UpdateKeywords_FullReplacement(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)
	var capturedNewKeywords map[string]bool
	var capturedVersion int

	mockEmailRepo := &mockEmailRepository{
		getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				MailboxIDs: map[string]bool{"inbox-id": true},
				Keywords:   map[string]bool{"$seen": true},
				Version:    1,
			}, nil
		},
		updateEmailKeywordsFunc: func(ctx context.Context, accountID, emailID string, newKeywords map[string]bool, expectedVersion int) (*email.EmailItem, error) {
			capturedNewKeywords = newKeywords
			capturedVersion = expectedVersion
			return &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				MailboxIDs: map[string]bool{"inbox-id": true},
				Keywords:   newKeywords,
				Version:    expectedVersion + 1,
			}, nil
		},
	}

	h := newHandler(mockEmailRepo, &mockMailboxRepository{}, nil, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"update": map[string]any{
				"email-456": map[string]any{
					"keywords": map[string]any{"$seen": true, "$flagged": true},
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if resp.MethodResponse.Name != "Email/set" {
		t.Errorf("Name = %q, want %q", resp.MethodResponse.Name, "Email/set")
	}

	updated, ok := resp.MethodResponse.Args["updated"].(map[string]any)
	if !ok {
		t.Fatalf("updated not a map: %T", resp.MethodResponse.Args["updated"])
	}
	if _, ok := updated["email-456"]; !ok {
		t.Error("email-456 should be in updated")
	}

	// Verify keywords were passed correctly
	if capturedNewKeywords == nil {
		t.Fatal("UpdateEmailKeywords was not called")
	}
	if !capturedNewKeywords["$seen"] || !capturedNewKeywords["$flagged"] {
		t.Errorf("capturedNewKeywords = %v, want $seen and $flagged", capturedNewKeywords)
	}
	if capturedVersion != 1 {
		t.Errorf("capturedVersion = %d, want 1", capturedVersion)
	}
}

// Test: Keywords update - patch add
func TestHandler_UpdateKeywords_PatchAdd(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)
	var capturedNewKeywords map[string]bool

	mockEmailRepo := &mockEmailRepository{
		getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				MailboxIDs: map[string]bool{"inbox-id": true},
				Keywords:   map[string]bool{"$seen": true}, // Currently has $seen
				Version:    1,
			}, nil
		},
		updateEmailKeywordsFunc: func(ctx context.Context, accountID, emailID string, newKeywords map[string]bool, expectedVersion int) (*email.EmailItem, error) {
			capturedNewKeywords = newKeywords
			return &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				MailboxIDs: map[string]bool{"inbox-id": true},
				Keywords:   newKeywords,
				Version:    expectedVersion + 1,
			}, nil
		},
	}

	h := newHandler(mockEmailRepo, &mockMailboxRepository{}, nil, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"update": map[string]any{
				"email-456": map[string]any{
					"keywords/$flagged": true, // Patch: add $flagged
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	updated, ok := resp.MethodResponse.Args["updated"].(map[string]any)
	if !ok {
		t.Fatalf("updated not a map: %T", resp.MethodResponse.Args["updated"])
	}
	if _, ok := updated["email-456"]; !ok {
		t.Error("email-456 should be in updated")
	}

	// Verify patch was applied: should have both $seen and $flagged
	if capturedNewKeywords == nil {
		t.Fatal("UpdateEmailKeywords was not called")
	}
	if !capturedNewKeywords["$seen"] || !capturedNewKeywords["$flagged"] {
		t.Errorf("capturedNewKeywords = %v, want $seen and $flagged", capturedNewKeywords)
	}
}

// Test: Keywords update - patch remove
func TestHandler_UpdateKeywords_PatchRemove(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)
	var capturedNewKeywords map[string]bool

	mockEmailRepo := &mockEmailRepository{
		getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				MailboxIDs: map[string]bool{"inbox-id": true},
				Keywords:   map[string]bool{"$seen": true, "$flagged": true}, // Currently has both
				Version:    1,
			}, nil
		},
		updateEmailKeywordsFunc: func(ctx context.Context, accountID, emailID string, newKeywords map[string]bool, expectedVersion int) (*email.EmailItem, error) {
			capturedNewKeywords = newKeywords
			return &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				MailboxIDs: map[string]bool{"inbox-id": true},
				Keywords:   newKeywords,
				Version:    expectedVersion + 1,
			}, nil
		},
	}

	h := newHandler(mockEmailRepo, &mockMailboxRepository{}, nil, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"update": map[string]any{
				"email-456": map[string]any{
					"keywords/$flagged": nil, // Patch: remove $flagged
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	updated, ok := resp.MethodResponse.Args["updated"].(map[string]any)
	if !ok {
		t.Fatalf("updated not a map: %T", resp.MethodResponse.Args["updated"])
	}
	if _, ok := updated["email-456"]; !ok {
		t.Error("email-456 should be in updated")
	}

	// Verify patch was applied: should have only $seen
	if capturedNewKeywords == nil {
		t.Fatal("UpdateEmailKeywords was not called")
	}
	if len(capturedNewKeywords) != 1 || !capturedNewKeywords["$seen"] {
		t.Errorf("capturedNewKeywords = %v, want only $seen", capturedNewKeywords)
	}
}

// Test: Keywords update with invalid keyword
func TestHandler_UpdateKeywords_InvalidKeyword(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	mockEmailRepo := &mockEmailRepository{
		getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				MailboxIDs: map[string]bool{"inbox-id": true},
				Keywords:   map[string]bool{},
				Version:    1,
			}, nil
		},
	}

	h := newHandler(mockEmailRepo, &mockMailboxRepository{}, nil, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"update": map[string]any{
				"email-456": map[string]any{
					"keywords": map[string]any{"invalid(keyword": true}, // Invalid character
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notUpdated, ok := resp.MethodResponse.Args["notUpdated"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated not a map: %T", resp.MethodResponse.Args["notUpdated"])
	}
	item, ok := notUpdated["email-456"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated[email-456] not a map: %T", notUpdated["email-456"])
	}
	if item["type"] != "invalidProperties" {
		t.Errorf("type = %v, want %q", item["type"], "invalidProperties")
	}
}

// Test: Keywords update with version conflict and retry
func TestHandler_UpdateKeywords_VersionConflictRetry(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)
	callCount := 0

	mockEmailRepo := &mockEmailRepository{
		getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			callCount++
			// Simulate another update happening between reads
			version := callCount
			return &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				MailboxIDs: map[string]bool{"inbox-id": true},
				Keywords:   map[string]bool{"$seen": true},
				Version:    version,
			}, nil
		},
		updateEmailKeywordsFunc: func(ctx context.Context, accountID, emailID string, newKeywords map[string]bool, expectedVersion int) (*email.EmailItem, error) {
			// First two attempts fail with version conflict, third succeeds
			if expectedVersion < 3 {
				return nil, email.ErrVersionConflict
			}
			return &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				MailboxIDs: map[string]bool{"inbox-id": true},
				Keywords:   newKeywords,
				Version:    expectedVersion + 1,
			}, nil
		},
	}

	h := newHandler(mockEmailRepo, &mockMailboxRepository{}, nil, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"update": map[string]any{
				"email-456": map[string]any{
					"keywords": map[string]any{"$seen": true, "$flagged": true},
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	// Should succeed after retries
	updated, ok := resp.MethodResponse.Args["updated"].(map[string]any)
	if !ok {
		t.Fatalf("updated not a map: %T", resp.MethodResponse.Args["updated"])
	}
	if _, ok := updated["email-456"]; !ok {
		t.Error("email-456 should be in updated after retry")
	}

	// Should have called GetEmail 3 times (initial + 2 retries)
	if callCount != 3 {
		t.Errorf("GetEmail called %d times, want 3", callCount)
	}
}

// Test: Keywords update - invalid nested path returns invalidPatch
func TestHandler_UpdateKeywords_InvalidNestedPath(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	mockEmailRepo := &mockEmailRepository{
		getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				MailboxIDs: map[string]bool{"inbox-id": true},
				Keywords:   map[string]bool{"$seen": true},
				Version:    1,
			}, nil
		},
	}

	h := newHandler(mockEmailRepo, &mockMailboxRepository{}, nil, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"update": map[string]any{
				"email-456": map[string]any{
					"keywords/nested/deep": true, // Invalid: nested path
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notUpdated, ok := resp.MethodResponse.Args["notUpdated"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated not a map: %T", resp.MethodResponse.Args["notUpdated"])
	}
	item, ok := notUpdated["email-456"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated[email-456] not a map: %T", notUpdated["email-456"])
	}
	if item["type"] != "invalidPatch" {
		t.Errorf("type = %v, want %q", item["type"], "invalidPatch")
	}
	desc, _ := item["description"].(string)
	if !strings.Contains(desc, "keywords/nested/deep") {
		t.Errorf("description = %q, want it to contain the invalid path", desc)
	}
}

// Test: MailboxIds update - invalid nested path returns invalidPatch
func TestHandler_UpdateMailboxIds_InvalidNestedPath(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	mockEmailRepo := &mockEmailRepository{
		getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return &email.EmailItem{
				EmailID:    emailID,
				AccountID:  accountID,
				ReceivedAt: receivedAt,
				MailboxIDs: map[string]bool{"inbox-id": true},
				Keywords:   map[string]bool{},
				Version:    1,
			}, nil
		},
	}

	h := newHandler(mockEmailRepo, &mockMailboxRepository{}, nil, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"update": map[string]any{
				"email-456": map[string]any{
					"mailboxIds/folder/subfolder": true, // Invalid: nested path
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notUpdated, ok := resp.MethodResponse.Args["notUpdated"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated not a map: %T", resp.MethodResponse.Args["notUpdated"])
	}
	item, ok := notUpdated["email-456"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated[email-456] not a map: %T", notUpdated["email-456"])
	}
	if item["type"] != "invalidPatch" {
		t.Errorf("type = %v, want %q", item["type"], "invalidPatch")
	}
	desc, _ := item["description"].(string)
	if !strings.Contains(desc, "mailboxIds/folder/subfolder") {
		t.Errorf("description = %q, want it to contain the invalid path", desc)
	}
}

// Test: create not supported
func TestHandler_CreateNotSupported(t *testing.T) {
	h := newHandler(&mockEmailRepository{}, &mockMailboxRepository{}, nil, nil, nil)

	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"create": map[string]any{
				"c0": map[string]any{},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notCreated, ok := resp.MethodResponse.Args["notCreated"].(map[string]any)
	if !ok {
		t.Fatalf("notCreated not a map: %T", resp.MethodResponse.Args["notCreated"])
	}
	item, ok := notCreated["c0"].(map[string]any)
	if !ok {
		t.Fatalf("notCreated[c0] not a map: %T", notCreated["c0"])
	}
	if item["type"] != "forbidden" {
		t.Errorf("type = %v, want %q", item["type"], "forbidden")
	}
}

// Test: Destroy email successfully
func TestHandler_DestroyEmail_Success(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	mockEmailRepo := &mockEmailRepository{
		getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return &email.EmailItem{
				EmailID:    "email-456",
				AccountID:  accountID,
				BlobID:     "blob-root",
				ThreadID:   "thread-1",
				ReceivedAt: receivedAt,
				MailboxIDs: map[string]bool{"inbox-id": true},
				Keywords:   map[string]bool{"$seen": true},
				Version:    2,
				BodyStructure: email.BodyPart{
					PartID: "1",
					BlobID: "blob-part-1",
					Type:   "text/plain",
				},
			}, nil
		},
		buildDeleteEmailItemsFunc: func(emailItem *email.EmailItem) []types.TransactWriteItem {
			return []types.TransactWriteItem{{}, {}} // 2 dummy items
		},
	}

	currentState := int64(10)
	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return currentState, nil
		},
		buildStateChangeItemsFunc: func(accountID string, objectType state.ObjectType, cs int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem) {
			return cs + 1, []types.TransactWriteItem{{}, {}}
		},
	}

	var publishedBlobIDs []string
	mockBlobPub := &mockBlobDeletePublisher{
		publishFunc: func(ctx context.Context, accountID string, blobIDs []string) error {
			publishedBlobIDs = append(publishedBlobIDs, blobIDs...)
			return nil
		},
	}

	mockTransactor := &mockTransactWriter{}

	h := newHandler(mockEmailRepo, &mockMailboxRepository{}, mockStateRepo, mockBlobPub, mockTransactor)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"destroy": []any{"email-456"},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	if resp.MethodResponse.Name != "Email/set" {
		t.Errorf("Name = %q, want %q", resp.MethodResponse.Name, "Email/set")
	}

	destroyed, ok := resp.MethodResponse.Args["destroyed"].([]any)
	if !ok {
		t.Fatalf("destroyed not a slice: %T", resp.MethodResponse.Args["destroyed"])
	}
	if len(destroyed) != 1 || destroyed[0] != "email-456" {
		t.Errorf("destroyed = %v, want [email-456]", destroyed)
	}

	// Verify blobs were published for deletion (root + body part)
	if len(publishedBlobIDs) != 2 {
		t.Errorf("published blob count = %d, want 2", len(publishedBlobIDs))
	}
}

// Test: Destroy email not found
func TestHandler_DestroyEmail_NotFound(t *testing.T) {
	mockEmailRepo := &mockEmailRepository{
		getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return nil, email.ErrEmailNotFound
		},
	}

	h := newHandler(mockEmailRepo, &mockMailboxRepository{}, nil, nil, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"destroy": []any{"nonexistent"},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notDestroyed, ok := resp.MethodResponse.Args["notDestroyed"].(map[string]any)
	if !ok {
		t.Fatalf("notDestroyed not a map: %T", resp.MethodResponse.Args["notDestroyed"])
	}
	item, ok := notDestroyed["nonexistent"].(map[string]any)
	if !ok {
		t.Fatalf("notDestroyed[nonexistent] not a map: %T", notDestroyed["nonexistent"])
	}
	if item["type"] != "notFound" {
		t.Errorf("type = %v, want %q", item["type"], "notFound")
	}
}

// Test: Destroy email - transaction failure
func TestHandler_DestroyEmail_TransactionFailed(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	mockEmailRepo := &mockEmailRepository{
		getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return &email.EmailItem{
				EmailID:    "email-456",
				AccountID:  accountID,
				BlobID:     "blob-root",
				ThreadID:   "thread-1",
				ReceivedAt: receivedAt,
				MailboxIDs: map[string]bool{"inbox-id": true},
				Keywords:   map[string]bool{"$seen": true},
				Version:    1,
			}, nil
		},
		buildDeleteEmailItemsFunc: func(emailItem *email.EmailItem) []types.TransactWriteItem {
			return []types.TransactWriteItem{{}}
		},
	}

	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 5, nil
		},
		buildStateChangeItemsFunc: func(accountID string, objectType state.ObjectType, cs int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem) {
			return cs + 1, []types.TransactWriteItem{{}, {}}
		},
	}

	mockTransactor := &mockTransactWriter{
		transactWriteItemsFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			return nil, errors.New("transaction failed")
		},
	}

	h := newHandler(mockEmailRepo, &mockMailboxRepository{}, mockStateRepo, nil, mockTransactor)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"destroy": []any{"email-456"},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notDestroyed, ok := resp.MethodResponse.Args["notDestroyed"].(map[string]any)
	if !ok {
		t.Fatalf("notDestroyed not a map: %T", resp.MethodResponse.Args["notDestroyed"])
	}
	item, ok := notDestroyed["email-456"].(map[string]any)
	if !ok {
		t.Fatalf("notDestroyed[email-456] not a map: %T", notDestroyed["email-456"])
	}
	if item["type"] != "serverFail" {
		t.Errorf("type = %v, want %q", item["type"], "serverFail")
	}
}

// Test: Destroy with blob delete error is best-effort (still succeeds)
func TestHandler_DestroyEmail_BlobDeleteError_StillSucceeds(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	mockEmailRepo := &mockEmailRepository{
		getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return &email.EmailItem{
				EmailID:    "email-456",
				AccountID:  accountID,
				BlobID:     "blob-root",
				ThreadID:   "thread-1",
				ReceivedAt: receivedAt,
				MailboxIDs: map[string]bool{"inbox-id": true},
				Keywords:   map[string]bool{},
				Version:    1,
			}, nil
		},
		buildDeleteEmailItemsFunc: func(emailItem *email.EmailItem) []types.TransactWriteItem {
			return []types.TransactWriteItem{{}}
		},
	}

	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 5, nil
		},
		buildStateChangeItemsFunc: func(accountID string, objectType state.ObjectType, cs int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem) {
			return cs + 1, []types.TransactWriteItem{{}, {}}
		},
	}

	mockBlobPub := &mockBlobDeletePublisher{
		publishFunc: func(ctx context.Context, accountID string, blobIDs []string) error {
			return errors.New("sqs publish failed")
		},
	}

	mockTransactor := &mockTransactWriter{}

	h := newHandler(mockEmailRepo, &mockMailboxRepository{}, mockStateRepo, mockBlobPub, mockTransactor)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set",
		Args: map[string]any{
			"destroy": []any{"email-456"},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	// Should still succeed despite blob delete failure
	destroyed, ok := resp.MethodResponse.Args["destroyed"].([]any)
	if !ok {
		t.Fatalf("destroyed not a slice: %T", resp.MethodResponse.Args["destroyed"])
	}
	if len(destroyed) != 1 || destroyed[0] != "email-456" {
		t.Errorf("destroyed = %v, want [email-456]", destroyed)
	}
}
