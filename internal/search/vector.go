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

const (
	// subjectBoost multiplies similarity scores for subject vectors in Text queries.
	subjectBoost = 1.5
	// relevanceWeight controls the blend between relevance and recency in Text queries.
	// 0.6 means 60% relevance, 40% recency.
	relevanceWeight = 0.6
)

// searchResult is an internal type for sorting results.
type searchResult struct {
	emailID    string
	receivedAt string
	score      float32
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

	// Query vectors with headroom for deduplication and pagination.
	// Text queries (no type filter) use a higher multiplier to give subject
	// vectors more room to survive the top-K cutoff.
	isTextQuery := typeFilter == ""
	multiplier := 3
	if isTextQuery {
		multiplier = 5
	}
	topK := int32((position + limit) * multiplier)
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

	// Deduplicate by emailId, tracking the best score per email.
	// For Text queries, subject vectors get a similarity boost.
	seen := make(map[string]int) // emailID → index in items
	var items []searchResult
	for _, r := range results {
		emailID, _ := r.Metadata["emailId"].(string)
		if emailID == "" {
			continue
		}

		similarity := 1 - r.Distance
		if isTextQuery {
			vecType, _ := r.Metadata["type"].(string)
			if vecType == "subject" {
				similarity *= subjectBoost
				if similarity > 1.0 {
					similarity = 1.0
				}
			}
		}

		if idx, exists := seen[emailID]; exists {
			// Max-wins: keep the higher boosted score
			if similarity > items[idx].score {
				items[idx].score = similarity
			}
			continue
		}

		seen[emailID] = len(items)
		receivedAt, _ := r.Metadata["receivedAt"].(string)
		items = append(items, searchResult{
			emailID:    emailID,
			receivedAt: receivedAt,
			score:      similarity,
		})
	}

	if isTextQuery && len(items) > 0 {
		// Compute recency scores normalised across the result set
		minTime := items[0].receivedAt
		maxTime := items[0].receivedAt
		for _, item := range items[1:] {
			if item.receivedAt < minTime {
				minTime = item.receivedAt
			}
			if item.receivedAt > maxTime {
				maxTime = item.receivedAt
			}
		}

		for i := range items {
			var recency float32 = 1.0
			if minTime != maxTime {
				recency = timestampToRecency(items[i].receivedAt, minTime, maxTime)
			}
			items[i].score = float32(relevanceWeight)*items[i].score + float32(1-relevanceWeight)*recency
		}

		// Sort by blended score descending
		sort.Slice(items, func(i, j int) bool {
			return items[i].score > items[j].score
		})
	} else {
		// Sort by receivedAt descending for type-filtered queries
		sort.Slice(items, func(i, j int) bool {
			return items[i].receivedAt > items[j].receivedAt
		})
	}

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

// timestampToRecency normalises a timestamp string to a 0.0–1.0 recency score
// relative to the min and max timestamps in the result set. Newest = 1.0, oldest = 0.0.
// Uses byte-level comparison which works correctly for RFC3339 timestamps.
func timestampToRecency(ts, minTS, maxTS string) float32 {
	// Convert to comparable numeric values using the timestamp bytes directly.
	// Since timestamps are fixed-format RFC3339 (e.g. "2024-01-20T10:00:00Z"),
	// lexicographic order equals chronological order.
	tsVal := timestampToFloat(ts)
	minVal := timestampToFloat(minTS)
	maxVal := timestampToFloat(maxTS)
	if maxVal == minVal {
		return 1.0
	}
	return float32((tsVal - minVal) / (maxVal - minVal))
}

// timestampToFloat converts an RFC3339 timestamp to a float64 for normalisation.
// Extracts numeric components to produce a comparable value.
func timestampToFloat(ts string) float64 {
	// Parse "2024-01-20T10:00:00Z" into a numeric value.
	// We only need relative ordering, so we can use a simple positional extraction.
	var val float64
	for _, b := range []byte(ts) {
		if b >= '0' && b <= '9' {
			val = val*10 + float64(b-'0')
		}
	}
	return val
}
