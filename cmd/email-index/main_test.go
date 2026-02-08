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
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
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
	updateFunc        func(ctx context.Context, accountID, emailID string, searchChunks int) error
	updateSummaryFunc func(ctx context.Context, accountID, emailID, summary string, overwritePreview bool) error
	summaryUpdates    []summaryUpdate
}

type summaryUpdate struct {
	accountID        string
	emailID          string
	summary          string
	overwritePreview bool
}

func (m *mockEmailUpdater) UpdateSearchChunks(ctx context.Context, accountID, emailID string, searchChunks int) error {
	if m.updateFunc != nil {
		return m.updateFunc(ctx, accountID, emailID, searchChunks)
	}
	return nil
}

func (m *mockEmailUpdater) UpdateSummary(ctx context.Context, accountID, emailID, summary string, overwritePreview bool) error {
	m.summaryUpdates = append(m.summaryUpdates, summaryUpdate{accountID, emailID, summary, overwritePreview})
	if m.updateSummaryFunc != nil {
		return m.updateSummaryFunc(ctx, accountID, emailID, summary, overwritePreview)
	}
	return nil
}

// mockSummarizer implements Summarizer for testing.
type mockSummarizer struct {
	summarizeFunc func(ctx context.Context, subject, from, bodyText string) (string, error)
	calls         []summarizerCall
}

type summarizerCall struct {
	subject  string
	from     string
	bodyText string
}

func (m *mockSummarizer) Summarize(ctx context.Context, subject, from, bodyText string) (string, error) {
	m.calls = append(m.calls, summarizerCall{subject, from, bodyText})
	if m.summarizeFunc != nil {
		return m.summarizeFunc(ctx, subject, from, bodyText)
	}
	return "Test summary", nil
}

// mockStateChanger implements StateChanger for testing.
type mockStateChanger struct {
	incrementFunc func(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error)
	calls         int
}

func (m *mockStateChanger) IncrementStateAndLogChange(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error) {
	m.calls++
	if m.incrementFunc != nil {
		return m.incrementFunc(ctx, accountID, objectType, objectID, changeType)
	}
	return int64(m.calls), nil
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

// mockTokenWriter implements TokenWriter for testing.
type mockTokenWriter struct {
	writeFunc  func(ctx context.Context, emailItem *email.EmailItem) error
	deleteFunc func(ctx context.Context, emailItem *email.EmailItem) error
	writeCalls []*email.EmailItem
	deleteCalls []*email.EmailItem
}

func (m *mockTokenWriter) WriteTokens(ctx context.Context, emailItem *email.EmailItem) error {
	m.writeCalls = append(m.writeCalls, emailItem)
	if m.writeFunc != nil {
		return m.writeFunc(ctx, emailItem)
	}
	return nil
}

func (m *mockTokenWriter) DeleteTokens(ctx context.Context, emailItem *email.EmailItem) error {
	m.deleteCalls = append(m.deleteCalls, emailItem)
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, emailItem)
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

	tokenWriter := &mockTokenWriter{}
	h := newHandler(reader, updater, nil, embedder, store, tokenWriter)
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

	// Should have written address tokens
	if len(tokenWriter.writeCalls) != 1 {
		t.Fatalf("expected 1 WriteTokens call, got %d", len(tokenWriter.writeCalls))
	}
	if tokenWriter.writeCalls[0].EmailID != "email-456" {
		t.Errorf("WriteTokens emailID = %q, want %q", tokenWriter.writeCalls[0].EmailID, "email-456")
	}

	// Should have created a subject vector (body chunk + subject = 2 putCalls)
	if len(store.putCalls) != 2 {
		t.Fatalf("expected 2 vectors (1 body + 1 subject), got %d", len(store.putCalls))
	}
	if store.putCalls[1].Key != "email-456#subject" {
		t.Errorf("subject vector key = %q, want %q", store.putCalls[1].Key, "email-456#subject")
	}
	if store.putCalls[1].Metadata["type"] != "subject" {
		t.Errorf("subject vector type = %v, want %q", store.putCalls[1].Metadata["type"], "subject")
	}
	// Body vector should have type "body"
	if store.putCalls[0].Metadata["type"] != "body" {
		t.Errorf("body vector type = %v, want %q", store.putCalls[0].Metadata["type"], "body")
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

	h := newHandler(reader, &mockEmailUpdater{}, nil, embedder, store, &mockTokenWriter{})
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

	tokenWriter := &mockTokenWriter{}
	h := newHandler(reader, &mockEmailUpdater{}, nil, &mockEmbeddingClient{}, store, tokenWriter)

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

	if len(deletedKeys) != 5 {
		t.Fatalf("expected 5 deleted keys (3 body + subject + summary), got %d", len(deletedKeys))
	}
	if deletedKeys[0] != "email-456#0" || deletedKeys[1] != "email-456#1" || deletedKeys[2] != "email-456#2" {
		t.Errorf("unexpected body chunk keys: %v", deletedKeys[:3])
	}
	if deletedKeys[3] != "email-456#subject" {
		t.Errorf("unexpected subject key: %v", deletedKeys[3])
	}
	if deletedKeys[4] != "email-456#summary" {
		t.Errorf("unexpected summary key: %v", deletedKeys[4])
	}

	// Should have deleted address tokens
	if len(tokenWriter.deleteCalls) != 1 {
		t.Fatalf("expected 1 DeleteTokens call, got %d", len(tokenWriter.deleteCalls))
	}
	if tokenWriter.deleteCalls[0].EmailID != "email-456" {
		t.Errorf("DeleteTokens emailID = %q, want %q", tokenWriter.deleteCalls[0].EmailID, "email-456")
	}
}

func TestHandler_InvalidJSON(t *testing.T) {
	event := events.SQSEvent{
		Records: []events.SQSMessage{
			{MessageId: "bad-msg", Body: "not json"},
		},
	}

	h := newHandler(&mockEmailReader{}, &mockEmailUpdater{}, nil, &mockEmbeddingClient{}, &mockVectorStore{}, &mockTokenWriter{})
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

	h := newHandler(reader, &mockEmailUpdater{}, nil, &mockEmbeddingClient{}, &mockVectorStore{}, &mockTokenWriter{})

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
	h := newHandler(reader, &mockEmailUpdater{}, nil, &mockEmbeddingClient{}, store, &mockTokenWriter{})
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
	if meta["type"] != "body" {
		t.Errorf("metadata type = %v, want body", meta["type"])
	}

	// Check address token lists
	fromTokens, ok := meta["fromTokens"].([]string)
	if !ok {
		t.Fatalf("metadata fromTokens not []string, got %T", meta["fromTokens"])
	}
	foundAlice := false
	for _, tok := range fromTokens {
		if tok == "alice@example.com" {
			foundAlice = true
		}
	}
	if !foundAlice {
		t.Errorf("fromTokens %v should contain alice@example.com", fromTokens)
	}

	toTokens, ok := meta["toTokens"].([]string)
	if !ok {
		t.Fatalf("metadata toTokens not []string, got %T", meta["toTokens"])
	}
	foundBob := false
	for _, tok := range toTokens {
		if tok == "bob@example.com" {
			foundBob = true
		}
	}
	if !foundBob {
		t.Errorf("toTokens %v should contain bob@example.com", toTokens)
	}
}

func TestHandler_IndexEmail_WithSummarizer(t *testing.T) {
	emailItem := &email.EmailItem{
		AccountID: "user-123",
		EmailID:   "email-456",
		Subject:   "50% Off Furniture",
		From:      []email.EmailAddress{{Email: "deals@furniture.com"}},
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

	streamer := &mockBlobStreamer{
		streamFunc: func(ctx context.Context, accountID, blobID string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("Everything is 50% off this weekend only!")), nil
		},
	}

	updater := &mockEmailUpdater{}
	embedder := &mockEmbeddingClient{}
	store := &mockVectorStore{}
	tokenWriter := &mockTokenWriter{}
	summarizer := &mockSummarizer{
		summarizeFunc: func(ctx context.Context, subject, from, bodyText string) (string, error) {
			if subject != "50% Off Furniture" {
				t.Errorf("summarizer subject = %q, want %q", subject, "50% Off Furniture")
			}
			if from != "deals@furniture.com" {
				t.Errorf("summarizer from = %q, want %q", from, "deals@furniture.com")
			}
			if bodyText != "Everything is 50% off this weekend only!" {
				t.Errorf("summarizer bodyText = %q, want body text", bodyText)
			}
			return "Ad: 50% off furniture this weekend", nil
		},
	}
	stateChanger := &mockStateChanger{}

	h := newHandler(reader, updater, nil, embedder, store, tokenWriter)
	h.blobClientFactory = func(baseURL string) BlobStreamer { return streamer }
	h.summarizer = summarizer
	h.stateChanger = stateChanger

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

	// Summarizer should have been called
	if len(summarizer.calls) != 1 {
		t.Fatalf("expected 1 summarizer call, got %d", len(summarizer.calls))
	}

	// Summary should have been persisted
	if len(updater.summaryUpdates) != 1 {
		t.Fatalf("expected 1 summary update, got %d", len(updater.summaryUpdates))
	}
	if updater.summaryUpdates[0].summary != "Ad: 50% off furniture this weekend" {
		t.Errorf("summary = %q, want %q", updater.summaryUpdates[0].summary, "Ad: 50% off furniture this weekend")
	}

	// State change should have been logged
	if stateChanger.calls != 1 {
		t.Errorf("expected 1 state change call, got %d", stateChanger.calls)
	}

	// Should have a summary vector (body + subject + summary = 3 vectors)
	if len(store.putCalls) != 3 {
		t.Fatalf("expected 3 vectors (body + subject + summary), got %d", len(store.putCalls))
	}
	summaryVec := store.putCalls[2]
	if summaryVec.Key != "email-456#summary" {
		t.Errorf("summary vector key = %q, want %q", summaryVec.Key, "email-456#summary")
	}
	if summaryVec.Metadata["type"] != "summary" {
		t.Errorf("summary vector type = %v, want %q", summaryVec.Metadata["type"], "summary")
	}
	if summaryVec.Metadata["summary"] != "Ad: 50% off furniture this weekend" {
		t.Errorf("summary metadata = %v, want summary text", summaryVec.Metadata["summary"])
	}
}

func TestHandler_IndexEmail_SummarizerFailure_ContinuesIndexing(t *testing.T) {
	emailItem := &email.EmailItem{
		AccountID: "user-123",
		EmailID:   "email-456",
		Subject:   "Test",
		TextBody:  []string{"1"},
		BodyStructure: email.BodyPart{
			PartID: "1",
			Type:   "text/plain",
			BlobID: "blob-1",
		},
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

	updater := &mockEmailUpdater{}
	store := &mockVectorStore{}
	summarizer := &mockSummarizer{
		summarizeFunc: func(ctx context.Context, subject, from, bodyText string) (string, error) {
			return "", errors.New("bedrock error")
		},
	}

	h := newHandler(reader, updater, nil, &mockEmbeddingClient{}, store, &mockTokenWriter{})
	h.blobClientFactory = func(baseURL string) BlobStreamer { return streamer }
	h.summarizer = summarizer

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
	// Indexing should succeed even when summarizer fails
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected no failures, got %d", len(resp.BatchItemFailures))
	}

	// No summary update should have been made
	if len(updater.summaryUpdates) != 0 {
		t.Errorf("expected 0 summary updates, got %d", len(updater.summaryUpdates))
	}

	// Body + subject vectors should still exist (no summary vector)
	if len(store.putCalls) != 2 {
		t.Errorf("expected 2 vectors (body + subject), got %d", len(store.putCalls))
	}
}

func TestHandler_IndexEmail_OverwritePreview(t *testing.T) {
	emailItem := &email.EmailItem{
		AccountID: "user-123",
		EmailID:   "email-456",
		Subject:   "Test",
		Preview:   "Original preview text",
		TextBody:  []string{"1"},
		BodyStructure: email.BodyPart{
			PartID: "1",
			Type:   "text/plain",
			BlobID: "blob-1",
		},
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

	updater := &mockEmailUpdater{}

	h := newHandler(reader, updater, nil, &mockEmbeddingClient{}, &mockVectorStore{}, &mockTokenWriter{})
	h.blobClientFactory = func(baseURL string) BlobStreamer { return streamer }
	h.summarizer = &mockSummarizer{}
	h.overwritePreview = true

	event := makeSQSEvent(searchindex.Message{
		AccountID: "user-123",
		EmailID:   "email-456",
		Action:    searchindex.ActionIndex,
		APIURL:    "https://api.example.com",
	})

	_, _ = h.handle(context.Background(), event)

	if len(updater.summaryUpdates) != 1 {
		t.Fatalf("expected 1 summary update, got %d", len(updater.summaryUpdates))
	}
	if !updater.summaryUpdates[0].overwritePreview {
		t.Error("expected overwritePreview to be true")
	}
}

func TestHandler_DeleteEmail_WithSummaryVector(t *testing.T) {
	emailItem := &email.EmailItem{
		AccountID:    "user-123",
		EmailID:      "email-456",
		SearchChunks: 2,
		Summary:      "Test summary",
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

	h := newHandler(reader, &mockEmailUpdater{}, nil, &mockEmbeddingClient{}, store, &mockTokenWriter{})

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

	// Should delete body chunks + subject + summary vectors
	if len(deletedKeys) != 4 {
		t.Fatalf("expected 4 deleted keys, got %d: %v", len(deletedKeys), deletedKeys)
	}
	if deletedKeys[0] != "email-456#0" || deletedKeys[1] != "email-456#1" {
		t.Errorf("unexpected body chunk keys: %v", deletedKeys[:2])
	}
	if deletedKeys[2] != "email-456#subject" {
		t.Errorf("unexpected subject key: %v", deletedKeys[2])
	}
	if deletedKeys[3] != "email-456#summary" {
		t.Errorf("unexpected summary key: %v", deletedKeys[3])
	}
}
