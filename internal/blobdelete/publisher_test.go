package blobdelete

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// mockSQSSender implements SQSSender for testing.
type mockSQSSender struct {
	sendFunc func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

func (m *mockSQSSender) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if m.sendFunc != nil {
		return m.sendFunc(ctx, params, optFns...)
	}
	return &sqs.SendMessageOutput{}, nil
}

func TestSQSPublisher_PublishBlobDeletions_Success(t *testing.T) {
	var capturedBody string
	mock := &mockSQSSender{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			capturedBody = *params.MessageBody
			return &sqs.SendMessageOutput{}, nil
		},
	}

	pub := NewSQSPublisher(mock, "https://sqs.example.com/queue")
	err := pub.PublishBlobDeletions(context.Background(), "user-123", []string{"blob-1", "blob-2"}, "https://api.example.com/stage")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify JSON body
	var msg BlobDeleteMessage
	if err := json.Unmarshal([]byte(capturedBody), &msg); err != nil {
		t.Fatalf("failed to parse message body: %v", err)
	}
	if msg.AccountID != "user-123" {
		t.Errorf("AccountID = %q, want %q", msg.AccountID, "user-123")
	}
	if len(msg.BlobIDs) != 2 || msg.BlobIDs[0] != "blob-1" || msg.BlobIDs[1] != "blob-2" {
		t.Errorf("BlobIDs = %v, want [blob-1, blob-2]", msg.BlobIDs)
	}
	if msg.APIURL != "https://api.example.com/stage" {
		t.Errorf("APIURL = %q, want %q", msg.APIURL, "https://api.example.com/stage")
	}
}

func TestSQSPublisher_PublishBlobDeletions_SQSError(t *testing.T) {
	mock := &mockSQSSender{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			return nil, errors.New("sqs send failed")
		},
	}

	pub := NewSQSPublisher(mock, "https://sqs.example.com/queue")
	err := pub.PublishBlobDeletions(context.Background(), "user-123", []string{"blob-1"}, "https://api.example.com/stage")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestSQSPublisher_PublishBlobDeletions_EmptyBlobIDs(t *testing.T) {
	sendCalled := false
	mock := &mockSQSSender{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			sendCalled = true
			return &sqs.SendMessageOutput{}, nil
		},
	}

	pub := NewSQSPublisher(mock, "https://sqs.example.com/queue")
	err := pub.PublishBlobDeletions(context.Background(), "user-123", []string{}, "https://api.example.com/stage")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sendCalled {
		t.Error("SQS should not be called for empty blob IDs")
	}
}

func TestSQSPublisher_PublishBlobDeletions_NilBlobIDs(t *testing.T) {
	sendCalled := false
	mock := &mockSQSSender{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			sendCalled = true
			return &sqs.SendMessageOutput{}, nil
		},
	}

	pub := NewSQSPublisher(mock, "https://sqs.example.com/queue")
	err := pub.PublishBlobDeletions(context.Background(), "user-123", nil, "https://api.example.com/stage")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sendCalled {
		t.Error("SQS should not be called for nil blob IDs")
	}
}

func TestSQSPublisher_CorrectQueueURL(t *testing.T) {
	var capturedQueueURL string
	mock := &mockSQSSender{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			capturedQueueURL = *params.QueueUrl
			return &sqs.SendMessageOutput{}, nil
		},
	}

	pub := NewSQSPublisher(mock, "https://sqs.example.com/my-queue")
	_ = pub.PublishBlobDeletions(context.Background(), "user-123", []string{"blob-1"}, "https://api.example.com/stage")

	if capturedQueueURL != "https://sqs.example.com/my-queue" {
		t.Errorf("QueueUrl = %q, want %q", capturedQueueURL, "https://sqs.example.com/my-queue")
	}
}
