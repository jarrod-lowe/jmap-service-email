package email

import (
	"testing"
	"time"
)

func TestTokenEntry_SK(t *testing.T) {
	entry := TokenEntry{
		AccountID:  "user-123",
		Field:      TokenFieldFrom,
		Token:      "john",
		ReceivedAt: time.Date(2024, 1, 20, 10, 30, 45, 0, time.UTC),
		EmailID:    "email-456",
	}

	got := entry.SK()
	want := "TOK#FROM#john#RCVD#2024-01-20T10:30:45Z#email-456"
	if got != want {
		t.Errorf("SK() = %q, want %q", got, want)
	}
}

func TestTokenEntry_SK_Fields(t *testing.T) {
	base := TokenEntry{
		AccountID:  "user-123",
		Token:      "alice",
		ReceivedAt: time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC),
		EmailID:    "email-1",
	}

	tests := []struct {
		field TokenField
		want  string
	}{
		{TokenFieldFrom, "TOK#FROM#alice#RCVD#2024-01-20T10:00:00Z#email-1"},
		{TokenFieldTo, "TOK#TO#alice#RCVD#2024-01-20T10:00:00Z#email-1"},
		{TokenFieldCC, "TOK#CC#alice#RCVD#2024-01-20T10:00:00Z#email-1"},
		{TokenFieldBcc, "TOK#BCC#alice#RCVD#2024-01-20T10:00:00Z#email-1"},
	}

	for _, tt := range tests {
		base.Field = tt.field
		got := base.SK()
		if got != tt.want {
			t.Errorf("SK() for %s = %q, want %q", tt.field, got, tt.want)
		}
	}
}

func TestBuildTokenEntries(t *testing.T) {
	e := &EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		ReceivedAt: time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC),
		From:       []EmailAddress{{Name: "John Smith", Email: "john@example.com"}},
		To:         []EmailAddress{{Email: "bob@example.com"}},
	}

	entries := buildTokenEntries(e)

	if len(entries) == 0 {
		t.Fatal("expected at least one token entry")
	}

	// Check that we have FROM entries
	fromCount := 0
	toCount := 0
	for _, entry := range entries {
		if entry.Field == TokenFieldFrom {
			fromCount++
		}
		if entry.Field == TokenFieldTo {
			toCount++
		}
		if entry.AccountID != "user-123" {
			t.Errorf("entry.AccountID = %q, want %q", entry.AccountID, "user-123")
		}
		if entry.EmailID != "email-456" {
			t.Errorf("entry.EmailID = %q, want %q", entry.EmailID, "email-456")
		}
	}

	// "John Smith" + "john@example.com" -> tokens: john, smith, john@example.com, john, example.com
	// deduplicated: john, smith, john@example.com, example.com = 4 FROM tokens
	if fromCount != 4 {
		t.Errorf("FROM token count = %d, want 4", fromCount)
	}

	// bob@example.com -> tokens: bob@example.com, bob, example.com = 3 TO tokens
	if toCount != 3 {
		t.Errorf("TO token count = %d, want 3", toCount)
	}
}

func TestBuildTokenEntries_Empty(t *testing.T) {
	e := &EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		ReceivedAt: time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC),
	}

	entries := buildTokenEntries(e)
	if len(entries) != 0 {
		t.Errorf("expected no entries for email with no addresses, got %d", len(entries))
	}
}
