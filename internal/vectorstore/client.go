// Package vectorstore provides vector storage operations via S3 Vectors.
package vectorstore

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors"
	s3vdocument "github.com/aws/aws-sdk-go-v2/service/s3vectors/document"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/types"
)

const (
	// IndexDimensions is the vector dimension count for Titan Embeddings v2.
	IndexDimensions = 1024
	// IndexPrefix is the prefix for per-account index names.
	IndexPrefix = "acct-"
)

// Vector represents a vector to store.
type Vector struct {
	Key      string
	Data     []float32
	Metadata map[string]any
}

// QueryRequest represents a vector query request.
type QueryRequest struct {
	Vector []float32
	TopK   int32
	Filter map[string]any // S3 Vectors metadata filter (optional)
}

// QueryResult represents a single result from a vector query.
type QueryResult struct {
	Key      string
	Distance float32
	Metadata map[string]any
}

// Store defines the interface for vector storage operations.
type Store interface {
	EnsureIndex(ctx context.Context, accountID string) error
	PutVector(ctx context.Context, accountID string, vector Vector) error
	DeleteVectors(ctx context.Context, accountID string, keys []string) error
	QueryVectors(ctx context.Context, accountID string, req QueryRequest) ([]QueryResult, error)
}

// S3VectorsAPI abstracts S3 Vectors operations for dependency inversion.
type S3VectorsAPI interface {
	CreateIndex(ctx context.Context, params *s3vectors.CreateIndexInput, optFns ...func(*s3vectors.Options)) (*s3vectors.CreateIndexOutput, error)
	PutVectors(ctx context.Context, params *s3vectors.PutVectorsInput, optFns ...func(*s3vectors.Options)) (*s3vectors.PutVectorsOutput, error)
	DeleteVectors(ctx context.Context, params *s3vectors.DeleteVectorsInput, optFns ...func(*s3vectors.Options)) (*s3vectors.DeleteVectorsOutput, error)
	QueryVectors(ctx context.Context, params *s3vectors.QueryVectorsInput, optFns ...func(*s3vectors.Options)) (*s3vectors.QueryVectorsOutput, error)
}

// S3VectorsClient implements Store using AWS S3 Vectors.
type S3VectorsClient struct {
	client     S3VectorsAPI
	bucketName string
	tags       map[string]string
	mu         sync.Mutex
	knownIndex map[string]bool
}

// NewS3VectorsClient creates a new S3VectorsClient.
func NewS3VectorsClient(client S3VectorsAPI, bucketName string, tags map[string]string) *S3VectorsClient {
	return &S3VectorsClient{
		client:     client,
		bucketName: bucketName,
		tags:       tags,
		knownIndex: make(map[string]bool),
	}
}

// indexName returns the S3 Vectors index name for an account.
func indexName(accountID string) string {
	return IndexPrefix + accountID
}

// EnsureIndex creates the per-account vector index if it doesn't already exist.
// Known indexes are cached in-memory to avoid repeated CreateIndex calls.
func (c *S3VectorsClient) EnsureIndex(ctx context.Context, accountID string) error {
	name := indexName(accountID)

	c.mu.Lock()
	if c.knownIndex[name] {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	dim := int32(IndexDimensions)
	_, err := c.client.CreateIndex(ctx, &s3vectors.CreateIndexInput{
		VectorBucketName: &c.bucketName,
		IndexName:        &name,
		Dimension:        &dim,
		DataType:         types.DataTypeFloat32,
		DistanceMetric:   types.DistanceMetricCosine,
		Tags:             c.tags,
	})
	if err != nil {
		// If the index already exists, that's fine
		var conflictErr *types.ConflictException
		if errors.As(err, &conflictErr) {
			c.mu.Lock()
			c.knownIndex[name] = true
			c.mu.Unlock()
			return nil
		}
		return fmt.Errorf("create index %s: %w", name, err)
	}

	c.mu.Lock()
	c.knownIndex[name] = true
	c.mu.Unlock()
	return nil
}

// PutVector stores a single vector in the per-account index.
func (c *S3VectorsClient) PutVector(ctx context.Context, accountID string, vector Vector) error {
	name := indexName(accountID)

	vectors := []types.PutInputVector{
		{
			Key:  &vector.Key,
			Data: &types.VectorDataMemberFloat32{Value: vector.Data},
		},
	}

	if vector.Metadata != nil {
		vectors[0].Metadata = s3vdocument.NewLazyDocument(vector.Metadata)
	}

	_, err := c.client.PutVectors(ctx, &s3vectors.PutVectorsInput{
		VectorBucketName: &c.bucketName,
		IndexName:        aws.String(name),
		Vectors:          vectors,
	})
	if err != nil {
		return fmt.Errorf("put vector %s: %w", vector.Key, err)
	}
	return nil
}

// DeleteVectors deletes vectors by key from the per-account index.
func (c *S3VectorsClient) DeleteVectors(ctx context.Context, accountID string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	name := indexName(accountID)
	_, err := c.client.DeleteVectors(ctx, &s3vectors.DeleteVectorsInput{
		VectorBucketName: &c.bucketName,
		IndexName:        aws.String(name),
		Keys:             keys,
	})
	if err != nil {
		return fmt.Errorf("delete vectors: %w", err)
	}
	return nil
}

// QueryVectors performs an approximate nearest neighbor search in the per-account index.
// Returns results with key, distance, and metadata.
func (c *S3VectorsClient) QueryVectors(ctx context.Context, accountID string, req QueryRequest) ([]QueryResult, error) {
	name := indexName(accountID)

	input := &s3vectors.QueryVectorsInput{
		VectorBucketName: &c.bucketName,
		IndexName:        aws.String(name),
		QueryVector:      &types.VectorDataMemberFloat32{Value: req.Vector},
		TopK:             &req.TopK,
		ReturnMetadata:   true,
		ReturnDistance:    true,
	}

	if req.Filter != nil {
		input.Filter = s3vdocument.NewLazyDocument(req.Filter)
	}

	output, err := c.client.QueryVectors(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("query vectors: %w", err)
	}

	results := make([]QueryResult, 0, len(output.Vectors))
	for _, v := range output.Vectors {
		result := QueryResult{}
		if v.Key != nil {
			result.Key = *v.Key
		}
		if v.Distance != nil {
			result.Distance = *v.Distance
		}
		if v.Metadata != nil {
			var meta map[string]any
			if err := v.Metadata.UnmarshalSmithyDocument(&meta); err == nil {
				result.Metadata = meta
			}
		}
		results = append(results, result)
	}

	return results, nil
}
