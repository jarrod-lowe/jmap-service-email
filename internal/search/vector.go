// Package search provides search orchestration for Email/query.
package search

import (
	"context"
	"fmt"
	"sort"

	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/vectorstore"
)

// Embedder generates vector embeddings from text.
type Embedder interface {
	GenerateEmbedding(ctx context.Context, text string) ([]float32, error)
}

// VectorQuerier queries vectors from the vector store.
type VectorQuerier interface {
	QueryVectors(ctx context.Context, accountID string, req vectorstore.QueryRequest) ([]vectorstore.QueryResult, error)
}

// VectorSearcher orchestrates semantic search: embed query → query vectors → dedup → sort → paginate.
type VectorSearcher struct {
	embedder Embedder
	store    VectorQuerier
}

// NewVectorSearcher creates a new VectorSearcher.
func NewVectorSearcher(embedder Embedder, store VectorQuerier) *VectorSearcher {
	return &VectorSearcher{
		embedder: embedder,
		store:    store,
	}
}

// SearchResult contains the results of a vector search.
type SearchResult struct {
	IDs      []string
	Position int
}

// searchResult is an internal type for sorting results.
type searchResult struct {
	emailID    string
	receivedAt string
}

// Search performs a vector-based search for emails matching the filter.
// It embeds the query text, queries the vector store with metadata filters,
// deduplicates by emailId, sorts by receivedAt descending, and applies pagination.
func (vs *VectorSearcher) Search(ctx context.Context, accountID string, filter *email.QueryFilter, position, limit int) (*SearchResult, error) {
	// Determine the search text and vector type filter
	searchText, typeFilter := extractSearchParams(filter)
	if searchText == "" {
		return &SearchResult{IDs: []string{}, Position: position}, nil
	}

	// Generate embedding for the search query
	vector, err := vs.embedder.GenerateEmbedding(ctx, searchText)
	if err != nil {
		return nil, fmt.Errorf("generate embedding: %w", err)
	}

	// Build metadata filter for structural/address conditions
	metaFilter := BuildMetadataFilter(filter)
	if typeFilter != "" {
		metaFilter["type"] = map[string]any{"$eq": typeFilter}
	}

	// Query vectors with headroom for deduplication and pagination
	topK := int32((position + limit) * 3)
	if topK < 50 {
		topK = 50
	}

	results, err := vs.store.QueryVectors(ctx, accountID, vectorstore.QueryRequest{
		Vector: vector,
		TopK:   topK,
		Filter: metaFilter,
	})
	if err != nil {
		return nil, fmt.Errorf("query vectors: %w", err)
	}

	// Deduplicate by emailId and collect receivedAt for sorting
	seen := make(map[string]bool)
	var items []searchResult
	for _, r := range results {
		emailID, _ := r.Metadata["emailId"].(string)
		if emailID == "" || seen[emailID] {
			continue
		}
		seen[emailID] = true
		receivedAt, _ := r.Metadata["receivedAt"].(string)
		items = append(items, searchResult{
			emailID:    emailID,
			receivedAt: receivedAt,
		})
	}

	// Sort by receivedAt descending
	sort.Slice(items, func(i, j int) bool {
		return items[i].receivedAt > items[j].receivedAt
	})

	// Apply position/limit pagination
	startIdx := position
	if startIdx > len(items) {
		startIdx = len(items)
	}
	endIdx := startIdx + limit
	if endIdx > len(items) {
		endIdx = len(items)
	}

	ids := make([]string, 0, endIdx-startIdx)
	for _, item := range items[startIdx:endIdx] {
		ids = append(ids, item.emailID)
	}

	return &SearchResult{IDs: ids, Position: position}, nil
}

// extractSearchParams determines the search text and optional vector type filter
// from the query filter.
func extractSearchParams(filter *email.QueryFilter) (searchText string, typeFilter string) {
	if filter.Subject != "" {
		return filter.Subject, "subject"
	}
	if filter.Body != "" {
		return filter.Body, "body"
	}
	if filter.Text != "" {
		// text searches both body and subject (no type filter)
		return filter.Text, ""
	}
	return "", ""
}

// BuildMetadataFilter constructs an S3 Vectors metadata filter from a QueryFilter.
// This translates structural and address filter conditions into S3 Vectors metadata
// filter operators ($eq, $gte, $lte, $in, etc.).
func BuildMetadataFilter(filter *email.QueryFilter) map[string]any {
	meta := make(map[string]any)

	// Mailbox filter: $eq on list metadata matches if value equals any element
	if filter.InMailbox != "" {
		meta["mailboxIds"] = map[string]any{"$eq": filter.InMailbox}
	}

	// Keyword filters
	if filter.HasKeyword != "" {
		meta["keywords"] = map[string]any{"$eq": filter.HasKeyword}
	}

	// HasAttachment filter
	if filter.HasAttachment != nil {
		meta["hasAttachment"] = map[string]any{"$eq": *filter.HasAttachment}
	}

	// Size filters
	if filter.MinSize != nil {
		meta["size"] = map[string]any{"$gte": *filter.MinSize}
	}
	if filter.MaxSize != nil {
		if existing, ok := meta["size"].(map[string]any); ok {
			existing["$lte"] = *filter.MaxSize
		} else {
			meta["size"] = map[string]any{"$lte": *filter.MaxSize}
		}
	}

	// Before/After date filters
	if filter.Before != nil {
		meta["receivedAt"] = map[string]any{"$lt": filter.Before.UTC().Format("2006-01-02T15:04:05Z")}
	}
	if filter.After != nil {
		if existing, ok := meta["receivedAt"].(map[string]any); ok {
			existing["$gte"] = filter.After.UTC().Format("2006-01-02T15:04:05Z")
		} else {
			meta["receivedAt"] = map[string]any{"$gte": filter.After.UTC().Format("2006-01-02T15:04:05Z")}
		}
	}

	// Address token filters (normalized + lowercased)
	if filter.From != "" {
		meta["fromTokens"] = map[string]any{"$eq": email.NormalizeSearchQuery(filter.From)}
	}
	if filter.To != "" {
		meta["toTokens"] = map[string]any{"$eq": email.NormalizeSearchQuery(filter.To)}
	}
	if filter.CC != "" {
		meta["ccTokens"] = map[string]any{"$eq": email.NormalizeSearchQuery(filter.CC)}
	}
	if filter.Bcc != "" {
		meta["bccTokens"] = map[string]any{"$eq": email.NormalizeSearchQuery(filter.Bcc)}
	}

	return meta
}
