package searchindex

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

func TestSQSPublisher_PublishIndexRequest_Success(t *testing.T) {
	var capturedBody string
	mock := &mockSQSSender{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			capturedBody = *params.MessageBody
			return &sqs.SendMessageOutput{}, nil
		},
	}

	pub := NewSQSPublisher(mock, "https://sqs.example.com/queue")
	err := pub.PublishIndexRequest(context.Background(), "user-123", "email-456", ActionIndex, "https://api.example.com", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var msg Message
	if err := json.Unmarshal([]byte(capturedBody), &msg); err != nil {
		t.Fatalf("failed to parse message body: %v", err)
	}
	if msg.AccountID != "user-123" {
		t.Errorf("AccountID = %q, want %q", msg.AccountID, "user-123")
	}
	if msg.EmailID != "email-456" {
		t.Errorf("EmailID = %q, want %q", msg.EmailID, "email-456")
	}
	if msg.Action != ActionIndex {
		t.Errorf("Action = %q, want %q", msg.Action, ActionIndex)
	}
	if msg.APIURL != "https://api.example.com" {
		t.Errorf("APIURL = %q, want %q", msg.APIURL, "https://api.example.com")
	}
}

func TestSQSPublisher_PublishIndexRequest_DeleteAction(t *testing.T) {
	var capturedBody string
	mock := &mockSQSSender{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			capturedBody = *params.MessageBody
			return &sqs.SendMessageOutput{}, nil
		},
	}

	pub := NewSQSPublisher(mock, "https://sqs.example.com/queue")
	err := pub.PublishIndexRequest(context.Background(), "user-123", "email-456", ActionDelete, "https://api.example.com", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var msg Message
	if err := json.Unmarshal([]byte(capturedBody), &msg); err != nil {
		t.Fatalf("failed to parse message body: %v", err)
	}
	if msg.Action != ActionDelete {
		t.Errorf("Action = %q, want %q", msg.Action, ActionDelete)
	}
}

func TestSQSPublisher_PublishIndexRequest_SQSError(t *testing.T) {
	mock := &mockSQSSender{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			return nil, errors.New("sqs send failed")
		},
	}

	pub := NewSQSPublisher(mock, "https://sqs.example.com/queue")
	err := pub.PublishIndexRequest(context.Background(), "user-123", "email-456", ActionIndex, "https://api.example.com", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
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
	_ = pub.PublishIndexRequest(context.Background(), "user-123", "email-456", ActionIndex, "https://api.example.com", nil) //nolint:errcheck // Test only

	if capturedQueueURL != "https://sqs.example.com/my-queue" {
		t.Errorf("QueueUrl = %q, want %q", capturedQueueURL, "https://sqs.example.com/my-queue")
	}
}

func TestSQSPublisher_PublishIndexRequest_WithDeleteMetadata(t *testing.T) {
	var capturedBody string
	mock := &mockSQSSender{
		sendFunc: func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			capturedBody = *params.MessageBody
			return &sqs.SendMessageOutput{}, nil
		},
	}

	pub := NewSQSPublisher(mock, "https://sqs.example.com/queue")
	metadata := &DeleteMetadata{
		SearchChunks: 2,
		Summary:      "Test summary",
		From: []EmailAddress{
			{Name: "Alice", Email: "alice@example.com"},
		},
	}
	err := pub.PublishIndexRequest(context.Background(), "user-123", "email-456", ActionDelete, "https://api.example.com", metadata)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var msg Message
	if err := json.Unmarshal([]byte(capturedBody), &msg); err != nil {
		t.Fatalf("failed to parse message body: %v", err)
	}
	if msg.DeleteMetadata == nil {
		t.Fatal("DeleteMetadata is nil")
	}
	if msg.DeleteMetadata.SearchChunks != 2 {
		t.Errorf("SearchChunks = %d, want 2", msg.DeleteMetadata.SearchChunks)
	}
	if msg.DeleteMetadata.Summary != "Test summary" {
		t.Errorf("Summary = %q, want %q", msg.DeleteMetadata.Summary, "Test summary")
	}
	if len(msg.DeleteMetadata.From) != 1 {
		t.Fatalf("From length = %d, want 1", len(msg.DeleteMetadata.From))
	}
	if msg.DeleteMetadata.From[0].Name != "Alice" {
		t.Errorf("From[0].Name = %q, want %q", msg.DeleteMetadata.From[0].Name, "Alice")
	}
}
