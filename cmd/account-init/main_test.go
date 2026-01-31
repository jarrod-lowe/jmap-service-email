package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailbox"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
)

// mockMailboxRepository implements MailboxRepository for testing.
type mockMailboxRepository struct {
	buildCreateMailboxItemFunc func(mbox *mailbox.MailboxItem) types.TransactWriteItem
	getMailboxFunc             func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error)
}

func (m *mockMailboxRepository) BuildCreateMailboxItem(mbox *mailbox.MailboxItem) types.TransactWriteItem {
	if m.buildCreateMailboxItemFunc != nil {
		return m.buildCreateMailboxItemFunc(mbox)
	}
	return types.TransactWriteItem{Put: &types.Put{}}
}

func (m *mockMailboxRepository) GetMailbox(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
	if m.getMailboxFunc != nil {
		return m.getMailboxFunc(ctx, accountID, mailboxID)
	}
	return nil, mailbox.ErrMailboxNotFound
}

// mockStateRepository implements StateRepository for testing.
type mockStateRepository struct {
	getCurrentStateFunc          func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
	buildStateChangeItemsMultiFunc func(accountID string, objectType state.ObjectType, currentState int64, objectIDs []string, changeType state.ChangeType) (int64, []types.TransactWriteItem)
}

func (m *mockStateRepository) GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
	if m.getCurrentStateFunc != nil {
		return m.getCurrentStateFunc(ctx, accountID, objectType)
	}
	return 0, nil
}

func (m *mockStateRepository) BuildStateChangeItemsMulti(accountID string, objectType state.ObjectType, currentState int64, objectIDs []string, changeType state.ChangeType) (int64, []types.TransactWriteItem) {
	if m.buildStateChangeItemsMultiFunc != nil {
		return m.buildStateChangeItemsMultiFunc(accountID, objectType, currentState, objectIDs, changeType)
	}
	return currentState + int64(len(objectIDs)), nil
}

// mockTransactWriter implements TransactWriter for testing.
type mockTransactWriter struct {
	transactWriteItemsFunc func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

func (m *mockTransactWriter) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if m.transactWriteItemsFunc != nil {
		return m.transactWriteItemsFunc(ctx, input, opts...)
	}
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func makeAccountCreatedMessage(accountID string) events.SQSMessage {
	payload := EventPayload{
		EventType:  "account.created",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		AccountID:  accountID,
	}
	body, _ := json.Marshal(payload)
	return events.SQSMessage{
		MessageId: "msg-1",
		Body:      string(body),
	}
}

func makeEventMessage(eventType, accountID string) events.SQSMessage {
	payload := EventPayload{
		EventType:  eventType,
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		AccountID:  accountID,
	}
	body, _ := json.Marshal(payload)
	return events.SQSMessage{
		MessageId: "msg-1",
		Body:      string(body),
	}
}

// Test: Happy path - creates all 6 special mailboxes
func TestHandler_CreatesAllSpecialMailboxes(t *testing.T) {
	var createdMailboxes []string

	h := newHandler(
		&mockMailboxRepository{
			buildCreateMailboxItemFunc: func(mbox *mailbox.MailboxItem) types.TransactWriteItem {
				createdMailboxes = append(createdMailboxes, mbox.MailboxID)
				return types.TransactWriteItem{Put: &types.Put{}}
			},
			getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
				return nil, mailbox.ErrMailboxNotFound
			},
		},
		&mockStateRepository{},
		&mockTransactWriter{},
	)

	resp, err := h.handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{makeAccountCreatedMessage("user-123")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected 0 failures, got %d", len(resp.BatchItemFailures))
	}

	// Verify all 6 mailboxes were created
	expectedMailboxes := []string{"inbox", "drafts", "sent", "trash", "junk", "archive"}
	if len(createdMailboxes) != len(expectedMailboxes) {
		t.Errorf("expected %d mailboxes, got %d: %v", len(expectedMailboxes), len(createdMailboxes), createdMailboxes)
	}

	mailboxSet := make(map[string]bool)
	for _, id := range createdMailboxes {
		mailboxSet[id] = true
	}
	for _, expected := range expectedMailboxes {
		if !mailboxSet[expected] {
			t.Errorf("expected mailbox %q to be created", expected)
		}
	}
}

// Test: Idempotency - skips if mailboxes already exist
func TestHandler_IdempotentWhenMailboxesExist(t *testing.T) {
	var transactCalled bool

	h := newHandler(
		&mockMailboxRepository{
			getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
				// All mailboxes already exist
				return &mailbox.MailboxItem{
					AccountID: accountID,
					MailboxID: mailboxID,
					Name:      mailboxID,
					Role:      mailboxID,
				}, nil
			},
		},
		&mockStateRepository{},
		&mockTransactWriter{
			transactWriteItemsFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
				transactCalled = true
				return &dynamodb.TransactWriteItemsOutput{}, nil
			},
		},
	)

	resp, err := h.handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{makeAccountCreatedMessage("user-123")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected 0 failures, got %d", len(resp.BatchItemFailures))
	}

	if transactCalled {
		t.Error("transaction should not be called when all mailboxes already exist")
	}
}

// Test: Ignores non-account.created events
func TestHandler_IgnoresNonAccountCreatedEvents(t *testing.T) {
	var transactCalled bool

	h := newHandler(
		&mockMailboxRepository{},
		&mockStateRepository{},
		&mockTransactWriter{
			transactWriteItemsFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
				transactCalled = true
				return &dynamodb.TransactWriteItemsOutput{}, nil
			},
		},
	)

	resp, err := h.handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{makeEventMessage("account.deleted", "user-123")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected 0 failures, got %d", len(resp.BatchItemFailures))
	}

	if transactCalled {
		t.Error("transaction should not be called for non-account.created events")
	}
}

// Test: Handles malformed JSON gracefully
func TestHandler_MalformedJSON(t *testing.T) {
	h := newHandler(
		&mockMailboxRepository{},
		&mockStateRepository{},
		&mockTransactWriter{},
	)

	resp, err := h.handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{
			{MessageId: "msg-bad", Body: "not valid json"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.BatchItemFailures) != 1 {
		t.Errorf("expected 1 failure for malformed JSON, got %d", len(resp.BatchItemFailures))
	}
}

// Test: Transaction failure reports batch item failure
func TestHandler_TransactionFailure(t *testing.T) {
	h := newHandler(
		&mockMailboxRepository{
			getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
				return nil, mailbox.ErrMailboxNotFound
			},
		},
		&mockStateRepository{},
		&mockTransactWriter{
			transactWriteItemsFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
				return nil, errors.New("transaction failed")
			},
		},
	)

	resp, err := h.handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{makeAccountCreatedMessage("user-123")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.BatchItemFailures) != 1 {
		t.Errorf("expected 1 failure for transaction error, got %d", len(resp.BatchItemFailures))
	}
}

// Test: Verifies correct mailbox properties
func TestHandler_CorrectMailboxProperties(t *testing.T) {
	var createdMailboxes []*mailbox.MailboxItem

	h := newHandler(
		&mockMailboxRepository{
			buildCreateMailboxItemFunc: func(mbox *mailbox.MailboxItem) types.TransactWriteItem {
				createdMailboxes = append(createdMailboxes, mbox)
				return types.TransactWriteItem{Put: &types.Put{}}
			},
			getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
				return nil, mailbox.ErrMailboxNotFound
			},
		},
		&mockStateRepository{},
		&mockTransactWriter{},
	)

	_, err := h.handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{makeAccountCreatedMessage("user-123")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify properties of each mailbox
	expectedProps := map[string]struct {
		Name      string
		Role      string
		SortOrder int
	}{
		"inbox":   {Name: "Inbox", Role: "inbox", SortOrder: 0},
		"drafts":  {Name: "Drafts", Role: "drafts", SortOrder: 1},
		"sent":    {Name: "Sent", Role: "sent", SortOrder: 2},
		"trash":   {Name: "Trash", Role: "trash", SortOrder: 3},
		"junk":    {Name: "Junk", Role: "junk", SortOrder: 4},
		"archive": {Name: "Archive", Role: "archive", SortOrder: 5},
	}

	for _, mbox := range createdMailboxes {
		expected, ok := expectedProps[mbox.MailboxID]
		if !ok {
			t.Errorf("unexpected mailbox created: %s", mbox.MailboxID)
			continue
		}

		if mbox.Name != expected.Name {
			t.Errorf("mailbox %s: expected name %q, got %q", mbox.MailboxID, expected.Name, mbox.Name)
		}
		if mbox.Role != expected.Role {
			t.Errorf("mailbox %s: expected role %q, got %q", mbox.MailboxID, expected.Role, mbox.Role)
		}
		if mbox.SortOrder != expected.SortOrder {
			t.Errorf("mailbox %s: expected sortOrder %d, got %d", mbox.MailboxID, expected.SortOrder, mbox.SortOrder)
		}
		if mbox.AccountID != "user-123" {
			t.Errorf("mailbox %s: expected accountID %q, got %q", mbox.MailboxID, "user-123", mbox.AccountID)
		}
		if !mbox.IsSubscribed {
			t.Errorf("mailbox %s: expected IsSubscribed to be true", mbox.MailboxID)
		}
	}
}

// Test: State tracking items are built with state repository
func TestHandler_StateTrackingBuiltCorrectly(t *testing.T) {
	var stateChangeObjectIDs []string

	h := newHandler(
		&mockMailboxRepository{
			getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
				return nil, mailbox.ErrMailboxNotFound
			},
		},
		&mockStateRepository{
			buildStateChangeItemsMultiFunc: func(accountID string, objectType state.ObjectType, currentState int64, objectIDs []string, changeType state.ChangeType) (int64, []types.TransactWriteItem) {
				stateChangeObjectIDs = objectIDs
				if objectType != state.ObjectTypeMailbox {
					t.Errorf("expected object type Mailbox, got %s", objectType)
				}
				if changeType != state.ChangeTypeCreated {
					t.Errorf("expected change type created, got %s", changeType)
				}
				return currentState + int64(len(objectIDs)), nil
			},
		},
		&mockTransactWriter{},
	)

	_, err := h.handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{makeAccountCreatedMessage("user-123")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(stateChangeObjectIDs) != 6 {
		t.Errorf("expected 6 state change object IDs, got %d", len(stateChangeObjectIDs))
	}
}
