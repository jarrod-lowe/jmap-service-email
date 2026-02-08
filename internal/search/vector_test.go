package search

import (
	"context"
	"sort"
	"testing"

	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/vectorstore"
)

// mockEmbedder implements Embedder for testing.
type mockEmbedder struct {
	generateFunc func(ctx context.Context, text string) ([]float32, error)
	calls        []string
}

func (m *mockEmbedder) GenerateEmbedding(ctx context.Context, text string) ([]float32, error) {
	m.calls = append(m.calls, text)
	if m.generateFunc != nil {
		return m.generateFunc(ctx, text)
	}
	return []float32{0.1, 0.2, 0.3}, nil
}

// mockVectorStore implements VectorQuerier for testing.
type mockVectorStore struct {
	queryFunc func(ctx context.Context, accountID string, req vectorstore.QueryRequest) ([]vectorstore.QueryResult, error)
	queryCalls []vectorstore.QueryRequest
}

func (m *mockVectorStore) QueryVectors(ctx context.Context, accountID string, req vectorstore.QueryRequest) ([]vectorstore.QueryResult, error) {
	m.queryCalls = append(m.queryCalls, req)
	if m.queryFunc != nil {
		return m.queryFunc(ctx, accountID, req)
	}
	return nil, nil
}

func TestVectorSearch_TextFilter_BasicQuery(t *testing.T) {
	embedder := &mockEmbedder{}
	store := &mockVectorStore{
		queryFunc: func(ctx context.Context, accountID string, req vectorstore.QueryRequest) ([]vectorstore.QueryResult, error) {
			return []vectorstore.QueryResult{
				{
					Key:      "email-1#0",
					Distance: 0.1,
					Metadata: map[string]any{
						"emailId":    "email-1",
						"receivedAt": "2024-01-20T10:00:00Z",
					},
				},
				{
					Key:      "email-2#0",
					Distance: 0.2,
					Metadata: map[string]any{
						"emailId":    "email-2",
						"receivedAt": "2024-01-20T11:00:00Z",
					},
				},
			}, nil
		},
	}

	vs := NewVectorSearcher(embedder, store)
	result, err := vs.Search(context.Background(), "user-123", &email.QueryFilter{
		Text: "hello world",
	}, 0, 25)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	// Should return 2 email IDs sorted by receivedAt descending
	if len(result.IDs) != 2 {
		t.Fatalf("IDs length = %d, want 2", len(result.IDs))
	}
	// Descending order: email-2 (11:00) before email-1 (10:00)
	if result.IDs[0] != "email-2" {
		t.Errorf("IDs[0] = %q, want %q", result.IDs[0], "email-2")
	}
	if result.IDs[1] != "email-1" {
		t.Errorf("IDs[1] = %q, want %q", result.IDs[1], "email-1")
	}

	// Should have called embedder with the search text
	if len(embedder.calls) != 1 || embedder.calls[0] != "hello world" {
		t.Errorf("embedder calls = %v, want [\"hello world\"]", embedder.calls)
	}
}

func TestVectorSearch_BodyFilter_TypeMetadata(t *testing.T) {
	store := &mockVectorStore{
		queryFunc: func(ctx context.Context, accountID string, req vectorstore.QueryRequest) ([]vectorstore.QueryResult, error) {
			return nil, nil
		},
	}

	vs := NewVectorSearcher(&mockEmbedder{}, store)
	_, _ = vs.Search(context.Background(), "user-123", &email.QueryFilter{
		Body: "test query",
	}, 0, 25)

	// Body filter should include type = "body" metadata filter
	if len(store.queryCalls) != 1 {
		t.Fatalf("query calls = %d, want 1", len(store.queryCalls))
	}
	filter := store.queryCalls[0].Filter
	if filter == nil {
		t.Fatal("expected metadata filter for body query")
	}
	typeFilter, ok := filter["type"]
	if !ok {
		t.Fatal("expected type in metadata filter")
	}
	eqMap, ok := typeFilter.(map[string]any)
	if !ok {
		t.Fatalf("type filter should be map, got %T", typeFilter)
	}
	if eqMap["$eq"] != "body" {
		t.Errorf("type filter $eq = %v, want \"body\"", eqMap["$eq"])
	}
}

func TestVectorSearch_SubjectFilter_TypeMetadata(t *testing.T) {
	store := &mockVectorStore{
		queryFunc: func(ctx context.Context, accountID string, req vectorstore.QueryRequest) ([]vectorstore.QueryResult, error) {
			return nil, nil
		},
	}

	vs := NewVectorSearcher(&mockEmbedder{}, store)
	_, _ = vs.Search(context.Background(), "user-123", &email.QueryFilter{
		Subject: "PTO request",
	}, 0, 25)

	if len(store.queryCalls) != 1 {
		t.Fatalf("query calls = %d, want 1", len(store.queryCalls))
	}
	filter := store.queryCalls[0].Filter
	if filter == nil {
		t.Fatal("expected metadata filter for subject query")
	}
	typeFilter := filter["type"].(map[string]any)
	if typeFilter["$eq"] != "subject" {
		t.Errorf("type filter $eq = %v, want \"subject\"", typeFilter["$eq"])
	}
}

func TestVectorSearch_DeduplicatesByEmailID(t *testing.T) {
	store := &mockVectorStore{
		queryFunc: func(ctx context.Context, accountID string, req vectorstore.QueryRequest) ([]vectorstore.QueryResult, error) {
			return []vectorstore.QueryResult{
				{Key: "email-1#0", Distance: 0.1, Metadata: map[string]any{"emailId": "email-1", "receivedAt": "2024-01-20T10:00:00Z"}},
				{Key: "email-1#1", Distance: 0.15, Metadata: map[string]any{"emailId": "email-1", "receivedAt": "2024-01-20T10:00:00Z"}},
				{Key: "email-2#0", Distance: 0.2, Metadata: map[string]any{"emailId": "email-2", "receivedAt": "2024-01-20T11:00:00Z"}},
			}, nil
		},
	}

	vs := NewVectorSearcher(&mockEmbedder{}, store)
	result, err := vs.Search(context.Background(), "user-123", &email.QueryFilter{Text: "test"}, 0, 25)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	// Should deduplicate: email-1 appears twice but only counted once
	if len(result.IDs) != 2 {
		t.Fatalf("IDs length = %d, want 2", len(result.IDs))
	}
}

func TestVectorSearch_Pagination(t *testing.T) {
	store := &mockVectorStore{
		queryFunc: func(ctx context.Context, accountID string, req vectorstore.QueryRequest) ([]vectorstore.QueryResult, error) {
			results := []vectorstore.QueryResult{
				{Key: "email-1#0", Metadata: map[string]any{"emailId": "email-1", "receivedAt": "2024-01-20T13:00:00Z"}},
				{Key: "email-2#0", Metadata: map[string]any{"emailId": "email-2", "receivedAt": "2024-01-20T12:00:00Z"}},
				{Key: "email-3#0", Metadata: map[string]any{"emailId": "email-3", "receivedAt": "2024-01-20T11:00:00Z"}},
				{Key: "email-4#0", Metadata: map[string]any{"emailId": "email-4", "receivedAt": "2024-01-20T10:00:00Z"}},
			}
			return results, nil
		},
	}

	vs := NewVectorSearcher(&mockEmbedder{}, store)

	// Get page 2 (position=2, limit=2)
	result, err := vs.Search(context.Background(), "user-123", &email.QueryFilter{Text: "test"}, 2, 2)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	// Sorted descending: email-1, email-2, email-3, email-4
	// Page 2 (position=2, limit=2): email-3, email-4
	if len(result.IDs) != 2 {
		t.Fatalf("IDs length = %d, want 2", len(result.IDs))
	}
	if result.IDs[0] != "email-3" {
		t.Errorf("IDs[0] = %q, want %q", result.IDs[0], "email-3")
	}
	if result.IDs[1] != "email-4" {
		t.Errorf("IDs[1] = %q, want %q", result.IDs[1], "email-4")
	}
	if result.Position != 2 {
		t.Errorf("Position = %d, want 2", result.Position)
	}
}

func TestVectorSearch_InMailboxMetadataFilter(t *testing.T) {
	store := &mockVectorStore{
		queryFunc: func(ctx context.Context, accountID string, req vectorstore.QueryRequest) ([]vectorstore.QueryResult, error) {
			return nil, nil
		},
	}

	vs := NewVectorSearcher(&mockEmbedder{}, store)
	_, _ = vs.Search(context.Background(), "user-123", &email.QueryFilter{
		Text:      "test",
		InMailbox: "inbox-123",
	}, 0, 25)

	if len(store.queryCalls) != 1 {
		t.Fatalf("query calls = %d, want 1", len(store.queryCalls))
	}
	filter := store.queryCalls[0].Filter
	if filter == nil {
		t.Fatal("expected metadata filter")
	}

	// Should have mailboxIds metadata filter
	mailboxFilter, ok := filter["mailboxIds"]
	if !ok {
		t.Fatal("expected mailboxIds in metadata filter")
	}
	eqMap := mailboxFilter.(map[string]any)
	if eqMap["$eq"] != "inbox-123" {
		t.Errorf("mailboxIds $eq = %v, want \"inbox-123\"", eqMap["$eq"])
	}
}

func TestVectorSearch_AddressMetadataFilter(t *testing.T) {
	store := &mockVectorStore{
		queryFunc: func(ctx context.Context, accountID string, req vectorstore.QueryRequest) ([]vectorstore.QueryResult, error) {
			return nil, nil
		},
	}

	vs := NewVectorSearcher(&mockEmbedder{}, store)
	_, _ = vs.Search(context.Background(), "user-123", &email.QueryFilter{
		Text: "test",
		From: "alice",
	}, 0, 25)

	if len(store.queryCalls) != 1 {
		t.Fatalf("query calls = %d, want 1", len(store.queryCalls))
	}
	filter := store.queryCalls[0].Filter
	if filter == nil {
		t.Fatal("expected metadata filter")
	}

	fromFilter, ok := filter["fromTokens"]
	if !ok {
		t.Fatal("expected fromTokens in metadata filter")
	}
	eqMap := fromFilter.(map[string]any)
	// Address search should use normalized token
	if eqMap["$eq"] != "alice" {
		t.Errorf("fromTokens $eq = %v, want \"alice\"", eqMap["$eq"])
	}
}

func TestBuildMetadataFilter(t *testing.T) {
	hasAttachment := true
	minSize := int64(1000)

	filter := &email.QueryFilter{
		InMailbox:     "inbox-123",
		HasKeyword:    "$flagged",
		HasAttachment: &hasAttachment,
		MinSize:       &minSize,
		From:          "Alice",
		To:            "Bob",
	}

	meta := BuildMetadataFilter(filter)

	// Check mailbox filter
	mailbox := meta["mailboxIds"].(map[string]any)
	if mailbox["$eq"] != "inbox-123" {
		t.Errorf("mailboxIds $eq = %v", mailbox["$eq"])
	}

	// Check keyword filter
	keywords := meta["keywords"].(map[string]any)
	if keywords["$eq"] != "$flagged" {
		t.Errorf("keywords $eq = %v", keywords["$eq"])
	}

	// Check hasAttachment
	hasAtt := meta["hasAttachment"].(map[string]any)
	if hasAtt["$eq"] != true {
		t.Errorf("hasAttachment $eq = %v", hasAtt["$eq"])
	}

	// Check minSize
	size := meta["size"].(map[string]any)
	if size["$gte"] != int64(1000) {
		t.Errorf("size $gte = %v", size["$gte"])
	}

	// Check address tokens (should be normalized/lowercased)
	from := meta["fromTokens"].(map[string]any)
	if from["$eq"] != "alice" {
		t.Errorf("fromTokens $eq = %v", from["$eq"])
	}
	to := meta["toTokens"].(map[string]any)
	if to["$eq"] != "bob" {
		t.Errorf("toTokens $eq = %v", to["$eq"])
	}
}

// Verify sort helper works correctly
func TestSortByReceivedAtDescending(t *testing.T) {
	items := []searchResult{
		{emailID: "email-1", receivedAt: "2024-01-20T10:00:00Z"},
		{emailID: "email-3", receivedAt: "2024-01-20T12:00:00Z"},
		{emailID: "email-2", receivedAt: "2024-01-20T11:00:00Z"},
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].receivedAt > items[j].receivedAt // descending
	})

	if items[0].emailID != "email-3" {
		t.Errorf("items[0] = %q, want email-3", items[0].emailID)
	}
	if items[1].emailID != "email-2" {
		t.Errorf("items[1] = %q, want email-2", items[1].emailID)
	}
	if items[2].emailID != "email-1" {
		t.Errorf("items[2] = %q, want email-1", items[2].emailID)
	}
}

func TestExtractSearchParams_Summary(t *testing.T) {
	filter := &email.QueryFilter{Summary: "spam emails"}
	text, typeFilter := extractSearchParams(filter)
	if text != "spam emails" {
		t.Errorf("searchText = %q, want %q", text, "spam emails")
	}
	if typeFilter != "summary" {
		t.Errorf("typeFilter = %q, want %q", typeFilter, "summary")
	}
}

func TestExtractSearchParams_SummaryPrecedence(t *testing.T) {
	// Summary should take precedence over subject/body/text
	filter := &email.QueryFilter{Summary: "summary query", Subject: "subject query", Body: "body query"}
	text, typeFilter := extractSearchParams(filter)
	if text != "summary query" {
		t.Errorf("searchText = %q, want %q", text, "summary query")
	}
	if typeFilter != "summary" {
		t.Errorf("typeFilter = %q, want %q", typeFilter, "summary")
	}
}

func TestNeedsVectorSearch_IncludesSummary(t *testing.T) {
	filter := &email.QueryFilter{Summary: "test"}
	if !filter.NeedsVectorSearch() {
		t.Error("NeedsVectorSearch() = false, want true when Summary is set")
	}
}
