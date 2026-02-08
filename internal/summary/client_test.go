package summary

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
)

// mockInvoker implements BedrockInvoker for testing.
type mockInvoker struct {
	invokeFunc func(ctx context.Context, params *bedrockruntime.InvokeModelInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error)
}

func (m *mockInvoker) InvokeModel(ctx context.Context, params *bedrockruntime.InvokeModelInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error) {
	return m.invokeFunc(ctx, params, optFns...)
}

func TestSummarize_Success(t *testing.T) {
	invoker := &mockInvoker{
		invokeFunc: func(ctx context.Context, params *bedrockruntime.InvokeModelInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error) {
			// Verify the model ID
			if *params.ModelId != "us.anthropic.claude-haiku-4-5-20251001-v1:0" {
				t.Errorf("model ID = %q, want default haiku model", *params.ModelId)
			}

			// Verify the request body is valid Claude Messages API format
			var req claudeRequest
			if err := json.Unmarshal(params.Body, &req); err != nil {
				t.Fatalf("failed to parse request body: %v", err)
			}
			if req.MaxTokens != 256 {
				t.Errorf("max_tokens = %d, want 256", req.MaxTokens)
			}
			if len(req.Messages) != 1 {
				t.Fatalf("messages count = %d, want 1", len(req.Messages))
			}
			if req.Messages[0].Role != "user" {
				t.Errorf("message role = %q, want user", req.Messages[0].Role)
			}

			resp := claudeResponse{
				Content: []contentBlock{{Type: "text", Text: "Ad: 50% off furniture this weekend"}},
			}
			body, _ := json.Marshal(resp)
			return &bedrockruntime.InvokeModelOutput{Body: body}, nil
		},
	}

	client := NewBedrockSummarizer(invoker, Config{})
	summary, err := client.Summarize(context.Background(), "Big Sale!", "deals@furniture.com", "Everything is 50% off this weekend only!")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if summary != "Ad: 50% off furniture this weekend" {
		t.Errorf("summary = %q, want %q", summary, "Ad: 50% off furniture this weekend")
	}
}

func TestSummarize_TruncatesLongOutput(t *testing.T) {
	longSummary := "This is a very long summary that exceeds the maximum allowed length and should be truncated at a word boundary with an ellipsis appended to indicate that the text has been cut short"

	invoker := &mockInvoker{
		invokeFunc: func(ctx context.Context, params *bedrockruntime.InvokeModelInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error) {
			resp := claudeResponse{
				Content: []contentBlock{{Type: "text", Text: longSummary}},
			}
			body, _ := json.Marshal(resp)
			return &bedrockruntime.InvokeModelOutput{Body: body}, nil
		},
	}

	client := NewBedrockSummarizer(invoker, Config{MaxLength: 50})
	summary, err := client.Summarize(context.Background(), "Test", "test@test.com", "body text")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(summary) > 50 {
		t.Errorf("summary length = %d, want <= 50", len(summary))
	}
	if summary[len(summary)-3:] != "..." {
		t.Errorf("truncated summary should end with '...', got %q", summary)
	}
}

func TestSummarize_TruncatesBodyInput(t *testing.T) {
	// Create body text longer than maxBodyInput
	longBody := make([]byte, 5000)
	for i := range longBody {
		longBody[i] = 'a'
	}

	var capturedBody string
	invoker := &mockInvoker{
		invokeFunc: func(ctx context.Context, params *bedrockruntime.InvokeModelInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error) {
			var req claudeRequest
			if err := json.Unmarshal(params.Body, &req); err != nil {
				t.Fatalf("failed to parse request: %v", err)
			}
			capturedBody = req.Messages[0].Content
			resp := claudeResponse{
				Content: []contentBlock{{Type: "text", Text: "Test summary"}},
			}
			body, _ := json.Marshal(resp)
			return &bedrockruntime.InvokeModelOutput{Body: body}, nil
		},
	}

	client := NewBedrockSummarizer(invoker, Config{})
	_, err := client.Summarize(context.Background(), "Test", "test@test.com", string(longBody))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The prompt should contain the body text, truncated to ~4000 chars
	// Total prompt is larger due to instructions + subject/from, but body portion should be capped
	if len(capturedBody) > 5000 {
		t.Errorf("prompt too long: %d chars, body input should be truncated", len(capturedBody))
	}
}

func TestSummarize_CustomModelID(t *testing.T) {
	customModel := "us.anthropic.claude-sonnet-4-5-20250929-v1:0"

	invoker := &mockInvoker{
		invokeFunc: func(ctx context.Context, params *bedrockruntime.InvokeModelInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error) {
			if *params.ModelId != customModel {
				t.Errorf("model ID = %q, want %q", *params.ModelId, customModel)
			}
			resp := claudeResponse{
				Content: []contentBlock{{Type: "text", Text: "Summary"}},
			}
			body, _ := json.Marshal(resp)
			return &bedrockruntime.InvokeModelOutput{Body: body}, nil
		},
	}

	client := NewBedrockSummarizer(invoker, Config{ModelID: customModel})
	_, err := client.Summarize(context.Background(), "Test", "test@test.com", "body")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSummarize_EmptyResponse(t *testing.T) {
	invoker := &mockInvoker{
		invokeFunc: func(ctx context.Context, params *bedrockruntime.InvokeModelInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error) {
			resp := claudeResponse{
				Content: []contentBlock{},
			}
			body, _ := json.Marshal(resp)
			return &bedrockruntime.InvokeModelOutput{Body: body}, nil
		},
	}

	client := NewBedrockSummarizer(invoker, Config{})
	summary, err := client.Summarize(context.Background(), "Test", "test@test.com", "body")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if summary != "" {
		t.Errorf("summary = %q, want empty string", summary)
	}
}

func TestSummarize_InvokeError(t *testing.T) {
	invoker := &mockInvoker{
		invokeFunc: func(ctx context.Context, params *bedrockruntime.InvokeModelInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error) {
			return nil, context.DeadlineExceeded
		},
	}

	client := NewBedrockSummarizer(invoker, Config{})
	_, err := client.Summarize(context.Background(), "Test", "test@test.com", "body")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestTruncateAtWordBoundary(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		maxLen   int
		expected string
	}{
		{
			name:     "short string unchanged",
			input:    "Hello world",
			maxLen:   50,
			expected: "Hello world",
		},
		{
			name:     "truncates at word boundary",
			input:    "Hello beautiful world today",
			maxLen:   20,
			expected: "Hello beautiful...",
		},
		{
			name:     "single long word",
			input:    "Supercalifragilisticexpialidocious",
			maxLen:   10,
			expected: "Superca...",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncateAtWordBoundary(tt.input, tt.maxLen)
			if result != tt.expected {
				t.Errorf("truncateAtWordBoundary(%q, %d) = %q, want %q", tt.input, tt.maxLen, result, tt.expected)
			}
			if len(result) > tt.maxLen {
				t.Errorf("result length %d exceeds max %d", len(result), tt.maxLen)
			}
		})
	}
}
