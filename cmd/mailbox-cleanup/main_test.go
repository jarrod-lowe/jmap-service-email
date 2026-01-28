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
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailboxcleanup"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
)

// mockEmailRepository implements EmailRepository for testing.
type mockEmailRepository struct {
	queryEmailsByMailboxFunc          func(ctx context.Context, accountID, mailboxID string) ([]string, error)
	getEmailFunc                      func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error)
	buildDeleteEmailItemsFunc         func(emailItem *email.EmailItem) []types.TransactWriteItem
	buildUpdateEmailMailboxesItemsFunc func(emailItem *email.EmailItem, newMailboxIDs map[string]bool) ([]string, []string, []types.TransactWriteItem)
}

func (m *mockEmailRepository) QueryEmailsByMailbox(ctx context.Context, accountID, mailboxID string) ([]string, error) {
	if m.queryEmailsByMailboxFunc != nil {
		return m.queryEmailsByMailboxFunc(ctx, accountID, mailboxID)
	}
	return nil, nil
}

func (m *mockEmailRepository) GetEmail(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
	if m.getEmailFunc != nil {
		return m.getEmailFunc(ctx, accountID, emailID)
	}
	return nil, email.ErrEmailNotFound
}

func (m *mockEmailRepository) BuildDeleteEmailItems(emailItem *email.EmailItem) []types.TransactWriteItem {
	if m.buildDeleteEmailItemsFunc != nil {
		return m.buildDeleteEmailItemsFunc(emailItem)
	}
	return nil
}

func (m *mockEmailRepository) BuildUpdateEmailMailboxesItems(emailItem *email.EmailItem, newMailboxIDs map[string]bool) ([]string, []string, []types.TransactWriteItem) {
	if m.buildUpdateEmailMailboxesItemsFunc != nil {
		return m.buildUpdateEmailMailboxesItemsFunc(emailItem, newMailboxIDs)
	}
	return nil, nil, nil
}

// mockStateRepository implements StateRepository for testing.
type mockStateRepository struct {
	getCurrentStateFunc           func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
	buildStateChangeItemsFunc     func(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem)
	buildStateChangeItemsMultiFunc func(accountID string, objectType state.ObjectType, currentState int64, objectIDs []string, changeType state.ChangeType) (int64, []types.TransactWriteItem)
}

func (m *mockStateRepository) GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
	if m.getCurrentStateFunc != nil {
		return m.getCurrentStateFunc(ctx, accountID, objectType)
	}
	return 0, nil
}

func (m *mockStateRepository) BuildStateChangeItems(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem) {
	if m.buildStateChangeItemsFunc != nil {
		return m.buildStateChangeItemsFunc(accountID, objectType, currentState, objectID, changeType)
	}
	return currentState + 1, nil
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

// mockBlobDeletePublisher implements BlobDeletePublisher for testing.
type mockBlobDeletePublisher struct {
	publishFunc func(ctx context.Context, accountID string, blobIDs []string) error
}

func (m *mockBlobDeletePublisher) PublishBlobDeletions(ctx context.Context, accountID string, blobIDs []string) error {
	if m.publishFunc != nil {
		return m.publishFunc(ctx, accountID, blobIDs)
	}
	return nil
}

func makeMessage(accountID, mailboxID string) events.SQSMessage {
	msg := mailboxcleanup.MailboxCleanupMessage{
		AccountID: accountID,
		MailboxID: mailboxID,
	}
	body, _ := json.Marshal(msg)
	return events.SQSMessage{
		MessageId: "msg-1",
		Body:      string(body),
	}
}

// Test: Empty mailbox — no emails to clean up
func TestHandler_EmptyMailbox(t *testing.T) {
	h := newHandler(
		&mockEmailRepository{
			queryEmailsByMailboxFunc: func(ctx context.Context, accountID, mailboxID string) ([]string, error) {
				return nil, nil
			},
		},
		&mockStateRepository{},
		&mockBlobDeletePublisher{},
		&mockTransactWriter{},
	)

	resp, err := h.handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{makeMessage("user-1", "mbox-1")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected 0 failures, got %d", len(resp.BatchItemFailures))
	}
}

// Test: Orphaned email (only in destroyed mailbox) gets deleted + blob cleanup published
func TestHandler_OrphanedEmailDeleted(t *testing.T) {
	var deletedEmails []string
	var publishedBlobIDs []string

	emailItem := &email.EmailItem{
		AccountID:  "user-1",
		EmailID:    "email-1",
		BlobID:     "blob-1",
		ThreadID:   "thread-1",
		MailboxIDs: map[string]bool{"mbox-1": true},
		ReceivedAt: time.Now(),
	}

	h := newHandler(
		&mockEmailRepository{
			queryEmailsByMailboxFunc: func(ctx context.Context, accountID, mailboxID string) ([]string, error) {
				return []string{"email-1"}, nil
			},
			getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
				return emailItem, nil
			},
			buildDeleteEmailItemsFunc: func(item *email.EmailItem) []types.TransactWriteItem {
				deletedEmails = append(deletedEmails, item.EmailID)
				return []types.TransactWriteItem{{Delete: &types.Delete{}}}
			},
		},
		&mockStateRepository{},
		&mockBlobDeletePublisher{
			publishFunc: func(ctx context.Context, accountID string, blobIDs []string) error {
				publishedBlobIDs = append(publishedBlobIDs, blobIDs...)
				return nil
			},
		},
		&mockTransactWriter{},
	)

	resp, err := h.handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{makeMessage("user-1", "mbox-1")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected 0 failures, got %d", len(resp.BatchItemFailures))
	}
	if len(deletedEmails) != 1 || deletedEmails[0] != "email-1" {
		t.Errorf("deleted emails = %v, want [email-1]", deletedEmails)
	}
	if len(publishedBlobIDs) == 0 {
		t.Error("expected blob deletion to be published")
	}
}

// Test: Multi-mailbox email has destroyed mailbox removed
func TestHandler_MultiMailboxEmailUpdated(t *testing.T) {
	var updatedNewMailboxIDs map[string]bool

	emailItem := &email.EmailItem{
		AccountID:  "user-1",
		EmailID:    "email-1",
		BlobID:     "blob-1",
		ThreadID:   "thread-1",
		MailboxIDs: map[string]bool{"mbox-1": true, "mbox-2": true},
		ReceivedAt: time.Now(),
	}

	h := newHandler(
		&mockEmailRepository{
			queryEmailsByMailboxFunc: func(ctx context.Context, accountID, mailboxID string) ([]string, error) {
				return []string{"email-1"}, nil
			},
			getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
				return emailItem, nil
			},
			buildUpdateEmailMailboxesItemsFunc: func(item *email.EmailItem, newMailboxIDs map[string]bool) ([]string, []string, []types.TransactWriteItem) {
				updatedNewMailboxIDs = newMailboxIDs
				return nil, []string{"mbox-1"}, []types.TransactWriteItem{{Put: &types.Put{}}}
			},
		},
		&mockStateRepository{},
		&mockBlobDeletePublisher{},
		&mockTransactWriter{},
	)

	resp, err := h.handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{makeMessage("user-1", "mbox-1")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected 0 failures, got %d", len(resp.BatchItemFailures))
	}
	if updatedNewMailboxIDs == nil {
		t.Fatal("expected email mailbox update")
	}
	if updatedNewMailboxIDs["mbox-1"] {
		t.Error("mbox-1 should have been removed from email's mailboxIds")
	}
	if !updatedNewMailboxIDs["mbox-2"] {
		t.Error("mbox-2 should still be in email's mailboxIds")
	}
}

// Test: Query failure reports batch item failure
func TestHandler_QueryFailureReportsBatchItemFailure(t *testing.T) {
	h := newHandler(
		&mockEmailRepository{
			queryEmailsByMailboxFunc: func(ctx context.Context, accountID, mailboxID string) ([]string, error) {
				return nil, errors.New("query failed")
			},
		},
		&mockStateRepository{},
		&mockBlobDeletePublisher{},
		&mockTransactWriter{},
	)

	resp, err := h.handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{makeMessage("user-1", "mbox-1")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.BatchItemFailures) != 1 {
		t.Errorf("expected 1 failure, got %d", len(resp.BatchItemFailures))
	}
}

// Test: Invalid JSON message reports batch item failure
func TestHandler_InvalidJSON(t *testing.T) {
	h := newHandler(
		&mockEmailRepository{},
		&mockStateRepository{},
		&mockBlobDeletePublisher{},
		&mockTransactWriter{},
	)

	resp, err := h.handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{
			{MessageId: "msg-bad", Body: "not json"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.BatchItemFailures) != 1 {
		t.Errorf("expected 1 failure, got %d", len(resp.BatchItemFailures))
	}
}

// Test: Transaction failure on email delete reports batch item failure
func TestHandler_TransactionFailure(t *testing.T) {
	emailItem := &email.EmailItem{
		AccountID:  "user-1",
		EmailID:    "email-1",
		BlobID:     "blob-1",
		ThreadID:   "thread-1",
		MailboxIDs: map[string]bool{"mbox-1": true},
		ReceivedAt: time.Now(),
	}

	h := newHandler(
		&mockEmailRepository{
			queryEmailsByMailboxFunc: func(ctx context.Context, accountID, mailboxID string) ([]string, error) {
				return []string{"email-1"}, nil
			},
			getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
				return emailItem, nil
			},
			buildDeleteEmailItemsFunc: func(item *email.EmailItem) []types.TransactWriteItem {
				return []types.TransactWriteItem{{Delete: &types.Delete{}}}
			},
		},
		&mockStateRepository{},
		&mockBlobDeletePublisher{},
		&mockTransactWriter{
			transactWriteItemsFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
				return nil, errors.New("transaction failed")
			},
		},
	)

	resp, err := h.handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{makeMessage("user-1", "mbox-1")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.BatchItemFailures) != 1 {
		t.Errorf("expected 1 failure, got %d", len(resp.BatchItemFailures))
	}
}
