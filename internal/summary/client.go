// Package summary provides AI-generated email summarization via Amazon Bedrock.
package summary

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
)

const (
	// DefaultModelID is the default Bedrock model for summarization.
	DefaultModelID = "anthropic.claude-haiku-4-5-20251001-v1:0"
	// DefaultMaxLength is the default maximum summary length in characters.
	DefaultMaxLength = 256
	// maxBodyInput is the maximum body text chars sent to the model.
	maxBodyInput = 4000
	// anthropicVersion is the required API version for Claude on Bedrock.
	anthropicVersion = "bedrock-2023-05-31"
)

// Summarizer generates a short summary of an email.
type Summarizer interface {
	Summarize(ctx context.Context, subject, from, bodyText string) (string, error)
}

// BedrockInvoker abstracts Bedrock model invocation for dependency inversion.
type BedrockInvoker interface {
	InvokeModel(ctx context.Context, params *bedrockruntime.InvokeModelInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error)
}

// Config holds configuration for the summarizer.
type Config struct {
	ModelID   string
	MaxLength int
}

// BedrockSummarizer generates email summaries via Amazon Bedrock Claude models.
type BedrockSummarizer struct {
	client    BedrockInvoker
	modelID   string
	maxLength int
}

// NewBedrockSummarizer creates a new BedrockSummarizer.
func NewBedrockSummarizer(client BedrockInvoker, cfg Config) *BedrockSummarizer {
	modelID := cfg.ModelID
	if modelID == "" {
		modelID = DefaultModelID
	}
	maxLength := cfg.MaxLength
	if maxLength <= 0 {
		maxLength = DefaultMaxLength
	}
	return &BedrockSummarizer{
		client:    client,
		modelID:   modelID,
		maxLength: maxLength,
	}
}

// claudeRequest is the Claude Messages API request format for Bedrock.
type claudeRequest struct {
	AnthropicVersion string    `json:"anthropic_version"`
	MaxTokens        int       `json:"max_tokens"`
	Messages         []message `json:"messages"`
}

// message represents a message in the Claude Messages API.
type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// claudeResponse is the Claude Messages API response format.
type claudeResponse struct {
	Content []contentBlock `json:"content"`
}

// contentBlock represents a content block in the response.
type contentBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

const promptTemplate = `Write a brief summary of this email in a short phrase.

- For spam or phishing: start with "Spam:" and a brief description
- For advertising or marketing: start with "Ad:" and describe what's being promoted
- For automated notifications: describe the event concisely
- For personal or business emails: describe the key point or action needed
- Maximum 100 characters. Be specific, not vague.
- Output ONLY the summary phrase. No quotes, no preamble, no "This email is about".

Subject: %s
From: %s
---
%s`

// Summarize generates a short summary of an email.
func (s *BedrockSummarizer) Summarize(ctx context.Context, subject, from, bodyText string) (string, error) {
	// Truncate body text input
	if len(bodyText) > maxBodyInput {
		bodyText = bodyText[:maxBodyInput]
	}

	prompt := fmt.Sprintf(promptTemplate, subject, from, bodyText)

	reqBody, err := json.Marshal(claudeRequest{
		AnthropicVersion: anthropicVersion,
		MaxTokens:        s.maxLength,
		Messages: []message{
			{Role: "user", Content: prompt},
		},
	})
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}

	modelID := s.modelID
	output, err := s.client.InvokeModel(ctx, &bedrockruntime.InvokeModelInput{
		ModelId: &modelID,
		Body:    reqBody,
	})
	if err != nil {
		return "", fmt.Errorf("invoke model: %w", err)
	}

	var resp claudeResponse
	if err := json.Unmarshal(output.Body, &resp); err != nil {
		return "", fmt.Errorf("unmarshal response: %w", err)
	}

	if len(resp.Content) == 0 {
		return "", nil
	}

	summary := strings.TrimSpace(resp.Content[0].Text)
	return truncateAtWordBoundary(summary, s.maxLength), nil
}

// truncateAtWordBoundary truncates text to maxLen characters at a word boundary,
// appending "..." if truncated.
func truncateAtWordBoundary(text string, maxLen int) string {
	if len(text) <= maxLen {
		return text
	}

	// Reserve space for "..."
	cutoff := maxLen - 3
	if cutoff <= 0 {
		return text[:maxLen]
	}

	// Find last space before cutoff
	lastSpace := strings.LastIndex(text[:cutoff], " ")
	if lastSpace > 0 {
		return text[:lastSpace] + "..."
	}

	// No space found; hard truncate
	return text[:cutoff] + "..."
}
