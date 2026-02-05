package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
)

// mockEmailRepository implements EmailRepository for testing.
type mockEmailRepository struct {
	getEmailFunc              func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error)
	buildDeleteEmailItemsFunc func(emailItem *email.EmailItem) []types.TransactWriteItem
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
	publishFunc func(ctx context.Context, accountID string, blobIDs []string, apiURL string) error
}

func (m *mockBlobDeletePublisher) PublishBlobDeletions(ctx context.Context, accountID string, blobIDs []string, apiURL string) error {
	if m.publishFunc != nil {
		return m.publishFunc(ctx, accountID, blobIDs, apiURL)
	}
	return nil
}

func makeDynamoDBEvent(oldHasDeletedAt, newHasDeletedAt bool) events.DynamoDBEvent {
	return makeDynamoDBEventWithAPIURL(oldHasDeletedAt, newHasDeletedAt, "")
}

func makeDynamoDBEventWithAPIURL(oldHasDeletedAt, newHasDeletedAt bool, apiURL string) events.DynamoDBEvent {
	oldImage := map[string]events.DynamoDBAttributeValue{
		email.AttrAccountID: events.NewStringAttribute("user-1"),
		email.AttrEmailID:   events.NewStringAttribute("email-1"),
	}
	newImage := map[string]events.DynamoDBAttributeValue{
		email.AttrAccountID: events.NewStringAttribute("user-1"),
		email.AttrEmailID:   events.NewStringAttribute("email-1"),
	}
	if oldHasDeletedAt {
		oldImage[email.AttrDeletedAt] = events.NewStringAttribute("2024-01-15T12:00:00Z")
	}
	if newHasDeletedAt {
		newImage[email.AttrDeletedAt] = events.NewStringAttribute("2024-01-15T12:00:00Z")
	}
	if apiURL != "" {
		newImage[email.AttrAPIURL] = events.NewStringAttribute(apiURL)
	}

	return events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{
				EventName: "MODIFY",
				Change: events.DynamoDBStreamRecord{
					OldImage: oldImage,
					NewImage: newImage,
				},
			},
		},
	}
}

// Test: MODIFY with new deletedAt triggers cleanup
func TestHandler_SoftDeleteTriggersCleanup(t *testing.T) {
	now := time.Now()
	var deletedEmails []string
	var publishedBlobIDs []string

	h := newHandler(
		&mockEmailRepository{
			getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
				deletedAt := now
				return &email.EmailItem{
					AccountID:  "user-1",
					EmailID:    "email-1",
					BlobID:     "blob-1",
					ThreadID:   "thread-1",
					MailboxIDs: map[string]bool{"mbox-1": true},
					ReceivedAt: now,
					DeletedAt:  &deletedAt,
					Version:    2,
				}, nil
			},
			buildDeleteEmailItemsFunc: func(emailItem *email.EmailItem) []types.TransactWriteItem {
				deletedEmails = append(deletedEmails, emailItem.EmailID)
				return []types.TransactWriteItem{{Delete: &types.Delete{}}}
			},
		},
		&mockBlobDeletePublisher{
			publishFunc: func(ctx context.Context, accountID string, blobIDs []string, apiURL string) error {
				publishedBlobIDs = append(publishedBlobIDs, blobIDs...)
				return nil
			},
		},
		&mockTransactWriter{},
	)

	err := h.handle(context.Background(), makeDynamoDBEvent(false, true))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(deletedEmails) != 1 || deletedEmails[0] != "email-1" {
		t.Errorf("deleted emails = %v, want [email-1]", deletedEmails)
	}
	if len(publishedBlobIDs) == 0 {
		t.Error("expected blob deletion to be published")
	}
}

// Test: MODIFY where deletedAt already existed is skipped
func TestHandler_AlreadyDeletedIsSkipped(t *testing.T) {
	getCalled := false
	h := newHandler(
		&mockEmailRepository{
			getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
				getCalled = true
				return nil, email.ErrEmailNotFound
			},
		},
		nil,
		&mockTransactWriter{},
	)

	err := h.handle(context.Background(), makeDynamoDBEvent(true, true))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if getCalled {
		t.Error("GetEmail should not have been called for already-deleted record")
	}
}

// Test: MODIFY without deletedAt in new image is skipped
func TestHandler_NoDeletedAtIsSkipped(t *testing.T) {
	getCalled := false
	h := newHandler(
		&mockEmailRepository{
			getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
				getCalled = true
				return nil, email.ErrEmailNotFound
			},
		},
		nil,
		&mockTransactWriter{},
	)

	err := h.handle(context.Background(), makeDynamoDBEvent(false, false))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if getCalled {
		t.Error("GetEmail should not have been called for record without deletedAt")
	}
}

// Test: Transaction failure returns error
func TestHandler_TransactionFailureReturnsError(t *testing.T) {
	now := time.Now()
	h := newHandler(
		&mockEmailRepository{
			getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
				return &email.EmailItem{
					AccountID:  "user-1",
					EmailID:    "email-1",
					BlobID:     "blob-1",
					MailboxIDs: map[string]bool{"mbox-1": true},
					ReceivedAt: now,
					Version:    1,
				}, nil
			},
			buildDeleteEmailItemsFunc: func(emailItem *email.EmailItem) []types.TransactWriteItem {
				return []types.TransactWriteItem{{Delete: &types.Delete{}}}
			},
		},
		nil,
		&mockTransactWriter{
			transactWriteItemsFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
				return nil, errors.New("transaction failed")
			},
		},
	)

	err := h.handle(context.Background(), makeDynamoDBEvent(false, true))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// Test: INSERT and REMOVE events are ignored
func TestHandler_NonModifyEventsIgnored(t *testing.T) {
	getCalled := false
	h := newHandler(
		&mockEmailRepository{
			getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
				getCalled = true
				return nil, nil
			},
		},
		nil,
		&mockTransactWriter{},
	)

	err := h.handle(context.Background(), events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{EventName: "INSERT"},
			{EventName: "REMOVE"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if getCalled {
		t.Error("GetEmail should not have been called for non-MODIFY events")
	}
}

// Test: apiUrl from stream newImage is passed to blob delete publisher
func TestHandler_APIURLFromStreamPassedToPublisher(t *testing.T) {
	now := time.Now()
	var capturedAPIURL string

	h := newHandler(
		&mockEmailRepository{
			getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
				deletedAt := now
				return &email.EmailItem{
					AccountID:  "user-1",
					EmailID:    "email-1",
					BlobID:     "blob-1",
					ThreadID:   "thread-1",
					MailboxIDs: map[string]bool{"mbox-1": true},
					ReceivedAt: now,
					DeletedAt:  &deletedAt,
					Version:    2,
				}, nil
			},
			buildDeleteEmailItemsFunc: func(emailItem *email.EmailItem) []types.TransactWriteItem {
				return []types.TransactWriteItem{{Delete: &types.Delete{}}}
			},
		},
		&mockBlobDeletePublisher{
			publishFunc: func(ctx context.Context, accountID string, blobIDs []string, apiURL string) error {
				capturedAPIURL = apiURL
				return nil
			},
		},
		&mockTransactWriter{},
	)

	err := h.handle(context.Background(), makeDynamoDBEventWithAPIURL(false, true, "https://api.example.com/stage"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedAPIURL != "https://api.example.com/stage" {
		t.Errorf("apiURL = %q, want %q", capturedAPIURL, "https://api.example.com/stage")
	}
}
