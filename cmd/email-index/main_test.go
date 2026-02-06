package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/searchindex"
	"github.com/jarrod-lowe/jmap-service-email/internal/vectorstore"
)

// mockEmailReader implements EmailReader for testing.
type mockEmailReader struct {
	getFunc func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error)
}

func (m *mockEmailReader) GetEmail(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, accountID, emailID)
	}
	return nil, errors.New("not implemented")
}

// mockEmailUpdater implements EmailUpdater for testing.
type mockEmailUpdater struct {
	updateFunc func(ctx context.Context, accountID, emailID string, searchChunks int) error
}

func (m *mockEmailUpdater) UpdateSearchChunks(ctx context.Context, accountID, emailID string, searchChunks int) error {
	if m.updateFunc != nil {
		return m.updateFunc(ctx, accountID, emailID, searchChunks)
	}
	return nil
}

// mockBlobStreamer implements BlobStreamer for testing.
type mockBlobStreamer struct {
	streamFunc func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error)
}

func (m *mockBlobStreamer) Stream(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
	if m.streamFunc != nil {
		return m.streamFunc(ctx, accountID, blobID)
	}
	return nil, errors.New("not implemented")
}

// mockEmbeddingClient implements EmbeddingClient for testing.
type mockEmbeddingClient struct {
	generateFunc func(ctx context.Context, text string) ([]float32, error)
}

func (m *mockEmbeddingClient) GenerateEmbedding(ctx context.Context, text string) ([]float32, error) {
	if m.generateFunc != nil {
		return m.generateFunc(ctx, text)
	}
	return []float32{0.1, 0.2, 0.3}, nil
}

// mockVectorStore implements VectorStore for testing.
type mockVectorStore struct {
	ensureFunc func(ctx context.Context, accountID string) error
	putFunc    func(ctx context.Context, accountID string, vector vectorstore.Vector) error
	deleteFunc func(ctx context.Context, accountID string, keys []string) error
	putCalls   []vectorstore.Vector
}

func (m *mockVectorStore) EnsureIndex(ctx context.Context, accountID string) error {
	if m.ensureFunc != nil {
		return m.ensureFunc(ctx, accountID)
	}
	return nil
}

func (m *mockVectorStore) PutVector(ctx context.Context, accountID string, vector vectorstore.Vector) error {
	m.putCalls = append(m.putCalls, vector)
	if m.putFunc != nil {
		return m.putFunc(ctx, accountID, vector)
	}
	return nil
}

func (m *mockVectorStore) DeleteVectors(ctx context.Context, accountID string, keys []string) error {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, accountID, keys)
	}
	return nil
}

func makeSQSEvent(msgs ...searchindex.Message) events.SQSEvent {
	var records []events.SQSMessage
	for i, msg := range msgs {
		body, _ := json.Marshal(msg)
		records = append(records, events.SQSMessage{
			MessageId: "msg-" + string(rune('0'+i)),
			Body:      string(body),
		})
	}
	return events.SQSEvent{Records: records}
}

func TestHandler_IndexEmail_Success(t *testing.T) {
	emailItem := &email.EmailItem{
		AccountID: "user-123",
		EmailID:   "email-456",
		Subject:   "Test Subject",
		From:      []email.EmailAddress{{Email: "alice@example.com"}},
		To:        []email.EmailAddress{{Email: "bob@example.com"}},
		TextBody:  []string{"1"},
		BodyStructure: email.BodyPart{
			PartID: "1",
			Type:   "text/plain",
			BlobID: "blob-part-1",
		},
	}

	reader := &mockEmailReader{
		getFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return emailItem, nil
		},
	}

	var streamedBlobIDs []string
	streamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			streamedBlobIDs = append(streamedBlobIDs, blobID)
			return io.NopCloser(strings.NewReader("Hello, this is the email body text.")), nil
		},
	}

	var updatedChunks int
	updater := &mockEmailUpdater{
		updateFunc: func(ctx context.Context, accountID, emailID string, searchChunks int) error {
			updatedChunks = searchChunks
			return nil
		},
	}

	embedder := &mockEmbeddingClient{}
	store := &mockVectorStore{}

	h := newHandler(reader, updater, nil, embedder, store)
	h.blobClientFactory = func(baseURL string) BlobStreamer {
		return streamer
	}

	event := makeSQSEvent(searchindex.Message{
		AccountID: "user-123",
		EmailID:   "email-456",
		Action:    searchindex.ActionIndex,
		APIURL:    "https://api.example.com",
	})

	resp, err := h.handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected no failures, got %d", len(resp.BatchItemFailures))
	}

	// Should have stored at least one vector
	if len(store.putCalls) == 0 {
		t.Error("expected at least one vector to be stored")
	}
	if store.putCalls[0].Key != "email-456#0" {
		t.Errorf("vector key = %q, want %q", store.putCalls[0].Key, "email-456#0")
	}

	// Should have updated searchChunks
	if updatedChunks != 1 {
		t.Errorf("searchChunks = %d, want 1", updatedChunks)
	}

	// Should have streamed using the blob ID, not the part ID
	if len(streamedBlobIDs) != 1 {
		t.Fatalf("expected 1 stream call, got %d", len(streamedBlobIDs))
	}
	if streamedBlobIDs[0] != "blob-part-1" {
		t.Errorf("streamed blobID = %q, want %q", streamedBlobIDs[0], "blob-part-1")
	}
}

func TestHandler_IndexEmail_HTMLBody(t *testing.T) {
	emailItem := &email.EmailItem{
		AccountID: "user-123",
		EmailID:   "email-789",
		Subject:   "HTML Email",
		HTMLBody:  []string{"1"},
		BodyStructure: email.BodyPart{
			PartID: "1",
			Type:   "text/html",
			BlobID: "blob-html-1",
		},
	}

	reader := &mockEmailReader{
		getFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return emailItem, nil
		},
	}

	streamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("<p>Hello <b>world</b></p>")), nil
		},
	}

	var embeddedTexts []string
	embedder := &mockEmbeddingClient{
		generateFunc: func(ctx context.Context, text string) ([]float32, error) {
			embeddedTexts = append(embeddedTexts, text)
			return []float32{0.1, 0.2}, nil
		},
	}

	store := &mockVectorStore{}

	h := newHandler(reader, &mockEmailUpdater{}, nil, embedder, store)
	h.blobClientFactory = func(baseURL string) BlobStreamer {
		return streamer
	}

	event := makeSQSEvent(searchindex.Message{
		AccountID: "user-123",
		EmailID:   "email-789",
		Action:    searchindex.ActionIndex,
		APIURL:    "https://api.example.com",
	})

	resp, err := h.handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected no failures, got %d", len(resp.BatchItemFailures))
	}

	// Embedded text should contain stripped HTML
	if len(embeddedTexts) == 0 {
		t.Fatal("expected at least one embedding call")
	}
	if !strings.Contains(embeddedTexts[0], "Hello world") {
		t.Errorf("embedded text should contain stripped HTML, got %q", embeddedTexts[0])
	}
}

func TestHandler_DeleteEmail_Success(t *testing.T) {
	emailItem := &email.EmailItem{
		AccountID:    "user-123",
		EmailID:      "email-456",
		SearchChunks: 3,
	}

	reader := &mockEmailReader{
		getFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return emailItem, nil
		},
	}

	var deletedKeys []string
	store := &mockVectorStore{
		deleteFunc: func(ctx context.Context, accountID string, keys []string) error {
			deletedKeys = keys
			return nil
		},
	}

	h := newHandler(reader, &mockEmailUpdater{}, nil, &mockEmbeddingClient{}, store)

	event := makeSQSEvent(searchindex.Message{
		AccountID: "user-123",
		EmailID:   "email-456",
		Action:    searchindex.ActionDelete,
		APIURL:    "https://api.example.com",
	})

	resp, err := h.handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected no failures, got %d", len(resp.BatchItemFailures))
	}

	if len(deletedKeys) != 3 {
		t.Fatalf("expected 3 deleted keys, got %d", len(deletedKeys))
	}
	if deletedKeys[0] != "email-456#0" || deletedKeys[1] != "email-456#1" || deletedKeys[2] != "email-456#2" {
		t.Errorf("unexpected keys: %v", deletedKeys)
	}
}

func TestHandler_InvalidJSON(t *testing.T) {
	event := events.SQSEvent{
		Records: []events.SQSMessage{
			{MessageId: "bad-msg", Body: "not json"},
		},
	}

	h := newHandler(&mockEmailReader{}, &mockEmailUpdater{}, nil, &mockEmbeddingClient{}, &mockVectorStore{})
	resp, err := h.handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.BatchItemFailures) != 1 {
		t.Errorf("expected 1 failure, got %d", len(resp.BatchItemFailures))
	}
}

func TestHandler_EmailNotFound(t *testing.T) {
	reader := &mockEmailReader{
		getFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return nil, email.ErrEmailNotFound
		},
	}

	h := newHandler(reader, &mockEmailUpdater{}, nil, &mockEmbeddingClient{}, &mockVectorStore{})

	event := makeSQSEvent(searchindex.Message{
		AccountID: "user-123",
		EmailID:   "email-456",
		Action:    searchindex.ActionIndex,
		APIURL:    "https://api.example.com",
	})

	resp, err := h.handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Email not found should NOT be a failure (email was deleted before indexing)
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected no failures for not-found email, got %d", len(resp.BatchItemFailures))
	}
}

func TestHandler_VectorMetadata(t *testing.T) {
	emailItem := &email.EmailItem{
		AccountID:     "user-123",
		EmailID:       "email-456",
		Subject:       "Test",
		MailboxIDs:    map[string]bool{"inbox-1": true},
		Keywords:      map[string]bool{"$seen": true},
		HasAttachment: true,
		Size:          1234,
		From:          []email.EmailAddress{{Email: "alice@example.com"}},
		To:            []email.EmailAddress{{Email: "bob@example.com"}},
		TextBody:      []string{"1"},
		BodyStructure: email.BodyPart{PartID: "1", Type: "text/plain", BlobID: "blob-part-1"},
	}

	reader := &mockEmailReader{
		getFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return emailItem, nil
		},
	}

	streamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("body text")), nil
		},
	}

	store := &mockVectorStore{}
	h := newHandler(reader, &mockEmailUpdater{}, nil, &mockEmbeddingClient{}, store)
	h.blobClientFactory = func(baseURL string) BlobStreamer {
		return streamer
	}

	event := makeSQSEvent(searchindex.Message{
		AccountID: "user-123",
		EmailID:   "email-456",
		Action:    searchindex.ActionIndex,
		APIURL:    "https://api.example.com",
	})

	_, _ = h.handle(context.Background(), event)

	if len(store.putCalls) == 0 {
		t.Fatal("expected at least one vector")
	}

	meta := store.putCalls[0].Metadata
	if meta["emailId"] != "email-456" {
		t.Errorf("metadata emailId = %v, want email-456", meta["emailId"])
	}
	if meta["subject"] != "Test" {
		t.Errorf("metadata subject = %v, want Test", meta["subject"])
	}
	if meta["hasAttachment"] != true {
		t.Errorf("metadata hasAttachment = %v, want true", meta["hasAttachment"])
	}
}
