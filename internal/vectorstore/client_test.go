package vectorstore

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/types"
)

// mockS3VectorsAPI implements S3VectorsAPI for testing.
type mockS3VectorsAPI struct {
	createIndexFunc   func(ctx context.Context, params *s3vectors.CreateIndexInput, optFns ...func(*s3vectors.Options)) (*s3vectors.CreateIndexOutput, error)
	putVectorsFunc    func(ctx context.Context, params *s3vectors.PutVectorsInput, optFns ...func(*s3vectors.Options)) (*s3vectors.PutVectorsOutput, error)
	deleteVectorsFunc func(ctx context.Context, params *s3vectors.DeleteVectorsInput, optFns ...func(*s3vectors.Options)) (*s3vectors.DeleteVectorsOutput, error)
	queryVectorsFunc  func(ctx context.Context, params *s3vectors.QueryVectorsInput, optFns ...func(*s3vectors.Options)) (*s3vectors.QueryVectorsOutput, error)
}

func (m *mockS3VectorsAPI) CreateIndex(ctx context.Context, params *s3vectors.CreateIndexInput, optFns ...func(*s3vectors.Options)) (*s3vectors.CreateIndexOutput, error) {
	if m.createIndexFunc != nil {
		return m.createIndexFunc(ctx, params, optFns...)
	}
	return &s3vectors.CreateIndexOutput{}, nil
}

func (m *mockS3VectorsAPI) PutVectors(ctx context.Context, params *s3vectors.PutVectorsInput, optFns ...func(*s3vectors.Options)) (*s3vectors.PutVectorsOutput, error) {
	if m.putVectorsFunc != nil {
		return m.putVectorsFunc(ctx, params, optFns...)
	}
	return &s3vectors.PutVectorsOutput{}, nil
}

func (m *mockS3VectorsAPI) DeleteVectors(ctx context.Context, params *s3vectors.DeleteVectorsInput, optFns ...func(*s3vectors.Options)) (*s3vectors.DeleteVectorsOutput, error) {
	if m.deleteVectorsFunc != nil {
		return m.deleteVectorsFunc(ctx, params, optFns...)
	}
	return &s3vectors.DeleteVectorsOutput{}, nil
}

func (m *mockS3VectorsAPI) QueryVectors(ctx context.Context, params *s3vectors.QueryVectorsInput, optFns ...func(*s3vectors.Options)) (*s3vectors.QueryVectorsOutput, error) {
	if m.queryVectorsFunc != nil {
		return m.queryVectorsFunc(ctx, params, optFns...)
	}
	return &s3vectors.QueryVectorsOutput{Vectors: []types.QueryOutputVector{}}, nil
}

func TestS3VectorsClient_EnsureIndex_CreatesNew(t *testing.T) {
	var capturedInput *s3vectors.CreateIndexInput
	mock := &mockS3VectorsAPI{
		createIndexFunc: func(ctx context.Context, params *s3vectors.CreateIndexInput, optFns ...func(*s3vectors.Options)) (*s3vectors.CreateIndexOutput, error) {
			capturedInput = params
			return &s3vectors.CreateIndexOutput{IndexArn: aws.String("arn:aws:s3vectors:us-east-1:123:bucket/index")}, nil
		},
	}

	tags := map[string]string{
		"Project":     "jmap-service-email",
		"Environment": "test",
	}
	client := NewS3VectorsClient(mock, "my-vector-bucket", tags)
	err := client.EnsureIndex(context.Background(), "user-123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedInput == nil {
		t.Fatal("CreateIndex was not called")
	}
	if *capturedInput.IndexName != "acct-user-123" {
		t.Errorf("IndexName = %q, want %q", *capturedInput.IndexName, "acct-user-123")
	}
	if *capturedInput.VectorBucketName != "my-vector-bucket" {
		t.Errorf("VectorBucketName = %q, want %q", *capturedInput.VectorBucketName, "my-vector-bucket")
	}
	if *capturedInput.Dimension != 1024 {
		t.Errorf("Dimension = %d, want 1024", *capturedInput.Dimension)
	}
	if capturedInput.DistanceMetric != types.DistanceMetricCosine {
		t.Errorf("DistanceMetric = %v, want cosine", capturedInput.DistanceMetric)
	}
	if len(capturedInput.Tags) != 2 {
		t.Fatalf("Tags length = %d, want 2", len(capturedInput.Tags))
	}
	if capturedInput.Tags["Project"] != "jmap-service-email" {
		t.Errorf("Tags[Project] = %q, want %q", capturedInput.Tags["Project"], "jmap-service-email")
	}
	if capturedInput.Tags["Environment"] != "test" {
		t.Errorf("Tags[Environment] = %q, want %q", capturedInput.Tags["Environment"], "test")
	}
}

func TestS3VectorsClient_EnsureIndex_CachesKnownIndex(t *testing.T) {
	callCount := 0
	mock := &mockS3VectorsAPI{
		createIndexFunc: func(ctx context.Context, params *s3vectors.CreateIndexInput, optFns ...func(*s3vectors.Options)) (*s3vectors.CreateIndexOutput, error) {
			callCount++
			return &s3vectors.CreateIndexOutput{}, nil
		},
	}

	client := NewS3VectorsClient(mock, "my-vector-bucket", nil)

	// First call should create
	if err := client.EnsureIndex(context.Background(), "user-123"); err != nil {
		t.Fatalf("first call error: %v", err)
	}
	// Second call should use cache
	if err := client.EnsureIndex(context.Background(), "user-123"); err != nil {
		t.Fatalf("second call error: %v", err)
	}

	if callCount != 1 {
		t.Errorf("CreateIndex called %d times, want 1", callCount)
	}
}

func TestS3VectorsClient_EnsureIndex_AlreadyExists(t *testing.T) {
	mock := &mockS3VectorsAPI{
		createIndexFunc: func(ctx context.Context, params *s3vectors.CreateIndexInput, optFns ...func(*s3vectors.Options)) (*s3vectors.CreateIndexOutput, error) {
			return nil, &types.ConflictException{Message: aws.String("Index already exists")}
		},
	}

	client := NewS3VectorsClient(mock, "my-vector-bucket", nil)
	err := client.EnsureIndex(context.Background(), "user-123")
	if err != nil {
		t.Fatalf("unexpected error for already-existing index: %v", err)
	}
}

func TestS3VectorsClient_PutVector_Success(t *testing.T) {
	var capturedInput *s3vectors.PutVectorsInput
	mock := &mockS3VectorsAPI{
		putVectorsFunc: func(ctx context.Context, params *s3vectors.PutVectorsInput, optFns ...func(*s3vectors.Options)) (*s3vectors.PutVectorsOutput, error) {
			capturedInput = params
			return &s3vectors.PutVectorsOutput{}, nil
		},
	}

	client := NewS3VectorsClient(mock, "my-vector-bucket", nil)
	v := Vector{
		Key:       "email-1#0",
		Data:      []float32{0.1, 0.2, 0.3},
		Metadata:  map[string]any{"emailId": "email-1"},
	}
	err := client.PutVector(context.Background(), "user-123", v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedInput == nil {
		t.Fatal("PutVectors was not called")
	}
	if *capturedInput.IndexName != "acct-user-123" {
		t.Errorf("IndexName = %q, want %q", *capturedInput.IndexName, "acct-user-123")
	}
}

func TestS3VectorsClient_PutVector_Error(t *testing.T) {
	mock := &mockS3VectorsAPI{
		putVectorsFunc: func(ctx context.Context, params *s3vectors.PutVectorsInput, optFns ...func(*s3vectors.Options)) (*s3vectors.PutVectorsOutput, error) {
			return nil, errors.New("put failed")
		},
	}

	client := NewS3VectorsClient(mock, "my-vector-bucket", nil)
	err := client.PutVector(context.Background(), "user-123", Vector{Key: "k", Data: []float32{0.1}})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestS3VectorsClient_DeleteVectors_Success(t *testing.T) {
	var capturedInput *s3vectors.DeleteVectorsInput
	mock := &mockS3VectorsAPI{
		deleteVectorsFunc: func(ctx context.Context, params *s3vectors.DeleteVectorsInput, optFns ...func(*s3vectors.Options)) (*s3vectors.DeleteVectorsOutput, error) {
			capturedInput = params
			return &s3vectors.DeleteVectorsOutput{}, nil
		},
	}

	client := NewS3VectorsClient(mock, "my-vector-bucket", nil)
	err := client.DeleteVectors(context.Background(), "user-123", []string{"email-1#0", "email-1#1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedInput == nil {
		t.Fatal("DeleteVectors was not called")
	}
	if len(capturedInput.Keys) != 2 {
		t.Errorf("Keys length = %d, want 2", len(capturedInput.Keys))
	}
}

func TestS3VectorsClient_DeleteVectors_Empty(t *testing.T) {
	callCount := 0
	mock := &mockS3VectorsAPI{
		deleteVectorsFunc: func(ctx context.Context, params *s3vectors.DeleteVectorsInput, optFns ...func(*s3vectors.Options)) (*s3vectors.DeleteVectorsOutput, error) {
			callCount++
			return &s3vectors.DeleteVectorsOutput{}, nil
		},
	}

	client := NewS3VectorsClient(mock, "my-vector-bucket", nil)
	err := client.DeleteVectors(context.Background(), "user-123", []string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if callCount != 0 {
		t.Error("DeleteVectors should not be called for empty keys")
	}
}

func TestS3VectorsClient_QueryVectors_Success(t *testing.T) {
	var capturedInput *s3vectors.QueryVectorsInput
	mock := &mockS3VectorsAPI{
		queryVectorsFunc: func(ctx context.Context, params *s3vectors.QueryVectorsInput, optFns ...func(*s3vectors.Options)) (*s3vectors.QueryVectorsOutput, error) {
			capturedInput = params
			return &s3vectors.QueryVectorsOutput{
				Vectors: []types.QueryOutputVector{
					{Key: aws.String("email-1#0"), Distance: aws.Float32(0.1)},
					{Key: aws.String("email-2#0"), Distance: aws.Float32(0.3)},
				},
			}, nil
		},
	}

	client := NewS3VectorsClient(mock, "my-vector-bucket", nil)
	results, err := client.QueryVectors(context.Background(), "user-123", QueryRequest{
		Vector: []float32{0.1, 0.2, 0.3},
		TopK:   10,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedInput == nil {
		t.Fatal("QueryVectors was not called")
	}
	if *capturedInput.IndexName != "acct-user-123" {
		t.Errorf("IndexName = %q, want %q", *capturedInput.IndexName, "acct-user-123")
	}
	if *capturedInput.TopK != 10 {
		t.Errorf("TopK = %d, want 10", *capturedInput.TopK)
	}
	if !capturedInput.ReturnMetadata {
		t.Error("ReturnMetadata should be true")
	}

	if len(results) != 2 {
		t.Fatalf("results length = %d, want 2", len(results))
	}
	if results[0].Key != "email-1#0" {
		t.Errorf("results[0].Key = %q, want %q", results[0].Key, "email-1#0")
	}
}

func TestS3VectorsClient_QueryVectors_WithFilter(t *testing.T) {
	var capturedInput *s3vectors.QueryVectorsInput
	mock := &mockS3VectorsAPI{
		queryVectorsFunc: func(ctx context.Context, params *s3vectors.QueryVectorsInput, optFns ...func(*s3vectors.Options)) (*s3vectors.QueryVectorsOutput, error) {
			capturedInput = params
			return &s3vectors.QueryVectorsOutput{Vectors: []types.QueryOutputVector{}}, nil
		},
	}

	client := NewS3VectorsClient(mock, "my-vector-bucket", nil)
	filter := map[string]any{
		"type": map[string]any{"$eq": "body"},
	}
	_, err := client.QueryVectors(context.Background(), "user-123", QueryRequest{
		Vector: []float32{0.1, 0.2},
		TopK:   5,
		Filter: filter,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedInput.Filter == nil {
		t.Error("Filter should be set")
	}
}

func TestS3VectorsClient_QueryVectors_Error(t *testing.T) {
	mock := &mockS3VectorsAPI{
		queryVectorsFunc: func(ctx context.Context, params *s3vectors.QueryVectorsInput, optFns ...func(*s3vectors.Options)) (*s3vectors.QueryVectorsOutput, error) {
			return nil, errors.New("query failed")
		},
	}

	client := NewS3VectorsClient(mock, "my-vector-bucket", nil)
	_, err := client.QueryVectors(context.Background(), "user-123", QueryRequest{
		Vector: []float32{0.1},
		TopK:   5,
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
