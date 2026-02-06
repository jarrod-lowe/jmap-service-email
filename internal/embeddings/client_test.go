package embeddings

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
)

// mockBedrockInvoker implements BedrockInvoker for testing.
type mockBedrockInvoker struct {
	invokeFunc func(ctx context.Context, params *bedrockruntime.InvokeModelInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error)
}

func (m *mockBedrockInvoker) InvokeModel(ctx context.Context, params *bedrockruntime.InvokeModelInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error) {
	if m.invokeFunc != nil {
		return m.invokeFunc(ctx, params, optFns...)
	}
	return nil, errors.New("not implemented")
}

func TestBedrockClient_GenerateEmbedding_Success(t *testing.T) {
	expectedVector := []float32{0.1, 0.2, 0.3}

	mock := &mockBedrockInvoker{
		invokeFunc: func(ctx context.Context, params *bedrockruntime.InvokeModelInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error) {
			// Verify model ID
			if *params.ModelId != ModelTitanEmbedV2 {
				t.Errorf("ModelId = %q, want %q", *params.ModelId, ModelTitanEmbedV2)
			}

			// Verify request body
			var req titanRequest
			if err := json.Unmarshal(params.Body, &req); err != nil {
				t.Fatalf("failed to parse request body: %v", err)
			}
			if req.InputText != "hello world" {
				t.Errorf("InputText = %q, want %q", req.InputText, "hello world")
			}

			// Return mock response
			resp := titanResponse{Embedding: expectedVector}
			body, _ := json.Marshal(resp)
			return &bedrockruntime.InvokeModelOutput{Body: body}, nil
		},
	}

	client := NewBedrockClient(mock)
	vector, err := client.GenerateEmbedding(context.Background(), "hello world")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(vector) != 3 {
		t.Fatalf("vector length = %d, want 3", len(vector))
	}
	for i, v := range expectedVector {
		if vector[i] != v {
			t.Errorf("vector[%d] = %f, want %f", i, vector[i], v)
		}
	}
}

func TestBedrockClient_GenerateEmbedding_Error(t *testing.T) {
	mock := &mockBedrockInvoker{
		invokeFunc: func(ctx context.Context, params *bedrockruntime.InvokeModelInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error) {
			return nil, errors.New("bedrock invoke failed")
		},
	}

	client := NewBedrockClient(mock)
	_, err := client.GenerateEmbedding(context.Background(), "hello")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestBedrockClient_GenerateEmbedding_InvalidResponse(t *testing.T) {
	mock := &mockBedrockInvoker{
		invokeFunc: func(ctx context.Context, params *bedrockruntime.InvokeModelInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error) {
			return &bedrockruntime.InvokeModelOutput{Body: []byte("not json")}, nil
		},
	}

	client := NewBedrockClient(mock)
	_, err := client.GenerateEmbedding(context.Background(), "hello")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
