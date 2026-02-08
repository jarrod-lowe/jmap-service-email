package searchindex

import (
	"encoding/json"
	"testing"
	"time"
)

// TestMessage_MarshalWithDeleteMetadata tests that Message correctly marshals DeleteMetadata.
func TestMessage_MarshalWithDeleteMetadata(t *testing.T) {
	receivedAt := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	msg := Message{
		AccountID: "user-123",
		EmailID:   "email-456",
		Action:    ActionDelete,
		APIURL:    "https://api.example.com",
		DeleteMetadata: &DeleteMetadata{
			SearchChunks: 2,
			Summary:      "Test summary",
			From: []EmailAddress{
				{Name: "Alice", Email: "alice@example.com"},
			},
			To: []EmailAddress{
				{Email: "bob@example.com"},
			},
			CC: []EmailAddress{
				{Email: "charlie@example.com"},
			},
			Bcc:        []EmailAddress{},
			ReceivedAt: receivedAt,
		},
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Unmarshal to verify round-trip
	var decoded Message
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded.AccountID != "user-123" {
		t.Errorf("AccountID = %q, want %q", decoded.AccountID, "user-123")
	}
	if decoded.EmailID != "email-456" {
		t.Errorf("EmailID = %q, want %q", decoded.EmailID, "email-456")
	}
	if decoded.Action != ActionDelete {
		t.Errorf("Action = %q, want %q", decoded.Action, ActionDelete)
	}
	if decoded.DeleteMetadata == nil {
		t.Fatal("DeleteMetadata is nil")
	}
	if decoded.DeleteMetadata.SearchChunks != 2 {
		t.Errorf("SearchChunks = %d, want 2", decoded.DeleteMetadata.SearchChunks)
	}
	if decoded.DeleteMetadata.Summary != "Test summary" {
		t.Errorf("Summary = %q, want %q", decoded.DeleteMetadata.Summary, "Test summary")
	}
	if len(decoded.DeleteMetadata.From) != 1 {
		t.Fatalf("From length = %d, want 1", len(decoded.DeleteMetadata.From))
	}
	if decoded.DeleteMetadata.From[0].Name != "Alice" {
		t.Errorf("From[0].Name = %q, want %q", decoded.DeleteMetadata.From[0].Name, "Alice")
	}
	if decoded.DeleteMetadata.From[0].Email != "alice@example.com" {
		t.Errorf("From[0].Email = %q, want %q", decoded.DeleteMetadata.From[0].Email, "alice@example.com")
	}
	if !decoded.DeleteMetadata.ReceivedAt.Equal(receivedAt) {
		t.Errorf("ReceivedAt = %v, want %v", decoded.DeleteMetadata.ReceivedAt, receivedAt)
	}
}

// TestMessage_MarshalWithoutDeleteMetadata tests that Message omits deleteMetadata when nil.
func TestMessage_MarshalWithoutDeleteMetadata(t *testing.T) {
	msg := Message{
		AccountID:      "user-123",
		EmailID:        "email-456",
		Action:         ActionIndex,
		APIURL:         "https://api.example.com",
		DeleteMetadata: nil,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Verify deleteMetadata is omitted from JSON
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("Unmarshal to map failed: %v", err)
	}

	if _, exists := raw["deleteMetadata"]; exists {
		t.Error("deleteMetadata field should be omitted when nil")
	}
}
