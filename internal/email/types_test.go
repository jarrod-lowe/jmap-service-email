package email

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEmailItem_Keys(t *testing.T) {
	email := EmailItem{
		AccountID: "user-123",
		EmailID:   "email-456",
	}

	pk := email.PK()
	sk := email.SK()

	if pk != "ACCOUNT#user-123" {
		t.Errorf("PK() = %q, want %q", pk, "ACCOUNT#user-123")
	}
	if sk != "EMAIL#email-456" {
		t.Errorf("SK() = %q, want %q", sk, "EMAIL#email-456")
	}
}

func TestMailboxMembershipItem_Keys(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)
	membership := MailboxMembershipItem{
		AccountID:  "user-123",
		MailboxID:  "inbox-789",
		ReceivedAt: receivedAt,
		EmailID:    "email-456",
	}

	pk := membership.PK()
	sk := membership.SK()

	if pk != "ACCOUNT#user-123" {
		t.Errorf("PK() = %q, want %q", pk, "ACCOUNT#user-123")
	}
	expectedSK := "MBOX#inbox-789#EMAIL#2024-01-20T10:00:00Z#email-456"
	if sk != expectedSK {
		t.Errorf("SK() = %q, want %q", sk, expectedSK)
	}
}

func TestEmailAddress_JSON(t *testing.T) {
	addr := EmailAddress{
		Name:  "John Doe",
		Email: "john@example.com",
	}

	data, err := json.Marshal(addr)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var parsed EmailAddress
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if parsed.Name != addr.Name || parsed.Email != addr.Email {
		t.Errorf("Round-trip failed: got %+v, want %+v", parsed, addr)
	}
}

func TestBodyPart_JSON(t *testing.T) {
	part := BodyPart{
		PartID:      "1",
		BlobID:      "blob-123",
		Size:        1024,
		Type:        "text/plain",
		Charset:     "utf-8",
		Disposition: "inline",
		Name:        "",
		SubParts:    nil,
	}

	data, err := json.Marshal(part)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var parsed BodyPart
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if parsed.PartID != part.PartID || parsed.Size != part.Size {
		t.Errorf("Round-trip failed: got %+v, want %+v", parsed, part)
	}
}

func TestEmailItem_FullStruct(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)
	sentAt := time.Date(2024, 1, 20, 9, 55, 0, 0, time.UTC)

	email := EmailItem{
		AccountID: "user-123",
		EmailID:   "email-456",
		BlobID:    "blob-789",
		ThreadID:  "email-456",
		MailboxIDs: map[string]bool{
			"inbox-id":    true,
			"projects-id": true,
		},
		Keywords: map[string]bool{
			"$seen": true,
		},
		ReceivedAt:    receivedAt,
		Size:          2048,
		HasAttachment: false,
		Subject:       "Test Subject",
		From: []EmailAddress{
			{Name: "Alice", Email: "alice@example.com"},
		},
		To: []EmailAddress{
			{Name: "Bob", Email: "bob@example.com"},
		},
		CC:        []EmailAddress{},
		ReplyTo:   []EmailAddress{},
		SentAt:    sentAt,
		MessageID: []string{"<msg-123@example.com>"},
		InReplyTo: []string{},
		References: []string{},
		Preview:   "This is a preview of the email body...",
		BodyStructure: BodyPart{
			PartID: "1",
			Type:   "text/plain",
			Size:   256,
		},
		TextBody:    []string{"1"},
		HTMLBody:    []string{},
		Attachments: []string{},
	}

	// Verify keys work with full struct
	if email.PK() != "ACCOUNT#user-123" {
		t.Errorf("PK() = %q, want %q", email.PK(), "ACCOUNT#user-123")
	}
	if email.SK() != "EMAIL#email-456" {
		t.Errorf("SK() = %q, want %q", email.SK(), "EMAIL#email-456")
	}

	// Verify JSON round-trip
	data, err := json.Marshal(email)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var parsed EmailItem
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if parsed.Subject != email.Subject {
		t.Errorf("Subject mismatch: got %q, want %q", parsed.Subject, email.Subject)
	}
	if len(parsed.MailboxIDs) != len(email.MailboxIDs) {
		t.Errorf("MailboxIDs length mismatch: got %d, want %d", len(parsed.MailboxIDs), len(email.MailboxIDs))
	}
}

func TestMailboxMembershipItem_SortKeyOrdering(t *testing.T) {
	accountID := "user-123"
	mailboxID := "inbox"

	// Create memberships at different times
	memberships := []MailboxMembershipItem{
		{
			AccountID:  accountID,
			MailboxID:  mailboxID,
			ReceivedAt: time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC),
			EmailID:    "email-1",
		},
		{
			AccountID:  accountID,
			MailboxID:  mailboxID,
			ReceivedAt: time.Date(2024, 1, 20, 11, 0, 0, 0, time.UTC),
			EmailID:    "email-2",
		},
		{
			AccountID:  accountID,
			MailboxID:  mailboxID,
			ReceivedAt: time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC),
			EmailID:    "email-3", // Same time as email-1, different ID
		},
	}

	sk1 := memberships[0].SK()
	sk2 := memberships[1].SK()
	sk3 := memberships[2].SK()

	// Earlier time should sort before later time
	if sk1 >= sk2 {
		t.Errorf("Expected sk1 < sk2: %q >= %q", sk1, sk2)
	}

	// Same time, different emailId should produce different sort keys
	if sk1 == sk3 {
		t.Errorf("Expected sk1 != sk3: both are %q", sk1)
	}

	// Both should still have the same prefix for mailbox queries
	expectedPrefix := "MBOX#inbox#EMAIL#"
	for i, sk := range []string{sk1, sk2, sk3} {
		if len(sk) < len(expectedPrefix) || sk[:len(expectedPrefix)] != expectedPrefix {
			t.Errorf("SK %d doesn't have expected prefix: got %q, want prefix %q", i, sk, expectedPrefix)
		}
	}
}
