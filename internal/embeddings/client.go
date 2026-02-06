// Package embeddings provides vector embedding generation via Amazon Bedrock.
package embeddings

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
)

// ModelTitanEmbedV2 is the model ID for Amazon Titan Embeddings v2.
const ModelTitanEmbedV2 = "amazon.titan-embed-text-v2:0"

// Client generates vector embeddings from text.
type Client interface {
	GenerateEmbedding(ctx context.Context, text string) ([]float32, error)
}

// BedrockInvoker abstracts Bedrock model invocation for dependency inversion.
type BedrockInvoker interface {
	InvokeModel(ctx context.Context, params *bedrockruntime.InvokeModelInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error)
}

// BedrockClient generates embeddings via Amazon Bedrock Titan Embeddings v2.
type BedrockClient struct {
	client BedrockInvoker
}

// NewBedrockClient creates a new BedrockClient.
func NewBedrockClient(client BedrockInvoker) *BedrockClient {
	return &BedrockClient{client: client}
}

// titanRequest is the request body for Titan Embeddings v2.
type titanRequest struct {
	InputText string `json:"inputText"`
}

// titanResponse is the response body from Titan Embeddings v2.
type titanResponse struct {
	Embedding []float32 `json:"embedding"`
}

// GenerateEmbedding generates a vector embedding for the given text.
func (c *BedrockClient) GenerateEmbedding(ctx context.Context, text string) ([]float32, error) {
	reqBody, err := json.Marshal(titanRequest{InputText: text})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	modelID := ModelTitanEmbedV2
	output, err := c.client.InvokeModel(ctx, &bedrockruntime.InvokeModelInput{
		ModelId: &modelID,
		Body:    reqBody,
	})
	if err != nil {
		return nil, fmt.Errorf("invoke model: %w", err)
	}

	var resp titanResponse
	if err := json.Unmarshal(output.Body, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	return resp.Embedding, nil
}
