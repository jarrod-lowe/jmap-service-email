package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/jarrod-lowe/jmap-service-email/internal/blobdelete"
)

type mockBlobDeleter struct {
	deleteFunc func(ctx context.Context, accountID, blobID string) error
}

func (m *mockBlobDeleter) Delete(ctx context.Context, accountID, blobID string) error {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, accountID, blobID)
	}
	return nil
}

func makeRecord(accountID string, blobIDs []string) events.SQSMessage {
	msg := blobdelete.BlobDeleteMessage{
		AccountID: accountID,
		BlobIDs:   blobIDs,
	}
	body, _ := json.Marshal(msg)
	return events.SQSMessage{
		MessageId: "msg-1",
		Body:      string(body),
	}
}

func TestHandler_SuccessfulDeletion(t *testing.T) {
	var deletedBlobs []string
	mock := &mockBlobDeleter{
		deleteFunc: func(ctx context.Context, accountID, blobID string) error {
			deletedBlobs = append(deletedBlobs, blobID)
			return nil
		},
	}

	h := newHandler(mock)
	resp, err := h.handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{
			makeRecord("user-123", []string{"blob-1", "blob-2"}),
		},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected no failures, got %d", len(resp.BatchItemFailures))
	}
	if len(deletedBlobs) != 2 {
		t.Errorf("deleted %d blobs, want 2", len(deletedBlobs))
	}
}

func TestHandler_BlobDeleteError_ReportsFailure(t *testing.T) {
	mock := &mockBlobDeleter{
		deleteFunc: func(ctx context.Context, accountID, blobID string) error {
			return errors.New("delete failed")
		},
	}

	h := newHandler(mock)
	resp, err := h.handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{
			makeRecord("user-123", []string{"blob-1"}),
		},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.BatchItemFailures) != 1 {
		t.Errorf("expected 1 failure, got %d", len(resp.BatchItemFailures))
	}
}

func TestHandler_InvalidJSON_ReportsFailure(t *testing.T) {
	mock := &mockBlobDeleter{}

	h := newHandler(mock)
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

func TestHandler_PartialFailure(t *testing.T) {
	callCount := 0
	mock := &mockBlobDeleter{
		deleteFunc: func(ctx context.Context, accountID, blobID string) error {
			callCount++
			if blobID == "blob-fail" {
				return errors.New("delete failed")
			}
			return nil
		},
	}

	h := newHandler(mock)
	resp, err := h.handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{
			makeRecord("user-123", []string{"blob-ok"}),
			makeRecord("user-123", []string{"blob-fail"}),
		},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Only the second message should fail
	if len(resp.BatchItemFailures) != 1 {
		t.Errorf("expected 1 failure, got %d", len(resp.BatchItemFailures))
	}
}

func TestHandler_MultipleBlobs_OneFailsReportsWholeMessage(t *testing.T) {
	mock := &mockBlobDeleter{
		deleteFunc: func(ctx context.Context, accountID, blobID string) error {
			if blobID == "blob-2" {
				return errors.New("delete failed")
			}
			return nil
		},
	}

	h := newHandler(mock)
	resp, err := h.handle(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{
			makeRecord("user-123", []string{"blob-1", "blob-2", "blob-3"}),
		},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Entire message should be reported as failed
	if len(resp.BatchItemFailures) != 1 {
		t.Errorf("expected 1 failure, got %d", len(resp.BatchItemFailures))
	}
}
