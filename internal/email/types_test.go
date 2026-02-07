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

func TestEmailItem_LSI1SK(t *testing.T) {
	email := EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		ReceivedAt: time.Date(2024, 1, 20, 10, 30, 45, 0, time.UTC),
	}

	lsi := email.LSI1SK()
	expected := "RCVD#2024-01-20T10:30:45Z#email-456"
	if lsi != expected {
		t.Errorf("LSI1SK() = %q, want %q", lsi, expected)
	}
}

func TestQueryRequest_Defaults(t *testing.T) {
	// Verify zero values work as expected
	req := QueryRequest{}
	if req.Position != 0 {
		t.Errorf("Position = %d, want 0", req.Position)
	}
	if req.Limit != 0 {
		t.Errorf("Limit = %d, want 0", req.Limit)
	}
	if req.Filter != nil {
		t.Errorf("Filter = %v, want nil", req.Filter)
	}
}

func TestQueryFilter_InMailbox(t *testing.T) {
	filter := &QueryFilter{InMailbox: "inbox-123"}
	if filter.InMailbox != "inbox-123" {
		t.Errorf("InMailbox = %q, want %q", filter.InMailbox, "inbox-123")
	}
}

func TestQueryFilter_NeedsVectorSearch(t *testing.T) {
	tests := []struct {
		name   string
		filter QueryFilter
		want   bool
	}{
		{"empty filter", QueryFilter{}, false},
		{"inMailbox only", QueryFilter{InMailbox: "inbox"}, false},
		{"hasKeyword only", QueryFilter{HasKeyword: "$seen"}, false},
		{"from only", QueryFilter{From: "alice"}, false},
		{"text filter", QueryFilter{Text: "hello"}, true},
		{"body filter", QueryFilter{Body: "hello"}, true},
		{"subject filter", QueryFilter{Subject: "meeting"}, true},
		{"text + inMailbox", QueryFilter{Text: "hello", InMailbox: "inbox"}, true},
		{"subject + from", QueryFilter{Subject: "PTO", From: "alice"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.filter.NeedsVectorSearch()
			if got != tt.want {
				t.Errorf("NeedsVectorSearch() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryFilter_HasAddressFilter(t *testing.T) {
	tests := []struct {
		name   string
		filter QueryFilter
		want   bool
	}{
		{"empty filter", QueryFilter{}, false},
		{"inMailbox only", QueryFilter{InMailbox: "inbox"}, false},
		{"text only", QueryFilter{Text: "hello"}, false},
		{"from", QueryFilter{From: "alice"}, true},
		{"to", QueryFilter{To: "bob"}, true},
		{"cc", QueryFilter{CC: "carol"}, true},
		{"bcc", QueryFilter{Bcc: "dave"}, true},
		{"from + inMailbox", QueryFilter{From: "alice", InMailbox: "inbox"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.filter.HasAddressFilter()
			if got != tt.want {
				t.Errorf("HasAddressFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComparator_Defaults(t *testing.T) {
	comp := Comparator{Property: "receivedAt"}
	if comp.IsAscending != false {
		t.Errorf("IsAscending default = %v, want false", comp.IsAscending)
	}
}

func TestQueryResult_Fields(t *testing.T) {
	result := QueryResult{
		IDs:        []string{"email-1", "email-2"},
		Position:   0,
		QueryState: "state-123",
	}
	if len(result.IDs) != 2 {
		t.Errorf("IDs length = %d, want 2", len(result.IDs))
	}
	if result.QueryState != "state-123" {
		t.Errorf("QueryState = %q, want %q", result.QueryState, "state-123")
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

func TestEmailItem_LSI2SK(t *testing.T) {
	tests := []struct {
		name      string
		email     EmailItem
		expected  string
	}{
		{
			name: "single message ID",
			email: EmailItem{
				AccountID: "user-123",
				EmailID:   "email-456",
				MessageID: []string{"<msg-123@example.com>"},
			},
			expected: "MSGID#<msg-123@example.com>",
		},
		{
			name: "multiple message IDs uses first",
			email: EmailItem{
				AccountID: "user-123",
				EmailID:   "email-456",
				MessageID: []string{"<first@example.com>", "<second@example.com>"},
			},
			expected: "MSGID#<first@example.com>",
		},
		{
			name: "empty message ID returns empty string",
			email: EmailItem{
				AccountID: "user-123",
				EmailID:   "email-456",
				MessageID: []string{},
			},
			expected: "",
		},
		{
			name: "nil message ID returns empty string",
			email: EmailItem{
				AccountID: "user-123",
				EmailID:   "email-456",
				MessageID: nil,
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.email.LSI2SK()
			if got != tt.expected {
				t.Errorf("LSI2SK() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestEmailItem_LSI3SK(t *testing.T) {
	tests := []struct {
		name     string
		email    EmailItem
		expected string
	}{
		{
			name: "standard thread key",
			email: EmailItem{
				AccountID:  "user-123",
				EmailID:    "email-456",
				ThreadID:   "thread-789",
				ReceivedAt: time.Date(2024, 1, 20, 10, 30, 45, 0, time.UTC),
			},
			expected: "THREAD#thread-789#RCVD#2024-01-20T10:30:45Z#email-456",
		},
		{
			name: "empty thread ID returns empty string",
			email: EmailItem{
				AccountID:  "user-123",
				EmailID:    "email-456",
				ThreadID:   "",
				ReceivedAt: time.Date(2024, 1, 20, 10, 30, 45, 0, time.UTC),
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.email.LSI3SK()
			if got != tt.expected {
				t.Errorf("LSI3SK() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestEmailItem_LSI3SK_SortOrdering(t *testing.T) {
	threadID := "thread-123"

	// Create emails at different times in the same thread
	email1 := EmailItem{
		EmailID:    "email-1",
		ThreadID:   threadID,
		ReceivedAt: time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC),
	}
	email2 := EmailItem{
		EmailID:    "email-2",
		ThreadID:   threadID,
		ReceivedAt: time.Date(2024, 1, 20, 11, 0, 0, 0, time.UTC),
	}
	email3 := EmailItem{
		EmailID:    "email-3",
		ThreadID:   threadID,
		ReceivedAt: time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC), // Same time as email-1
	}

	lsi1 := email1.LSI3SK()
	lsi2 := email2.LSI3SK()
	lsi3 := email3.LSI3SK()

	// Earlier time should sort before later time
	if lsi1 >= lsi2 {
		t.Errorf("Expected lsi1 < lsi2: %q >= %q", lsi1, lsi2)
	}

	// Same time, different emailId should produce different sort keys
	if lsi1 == lsi3 {
		t.Errorf("Expected lsi1 != lsi3: both are %q", lsi1)
	}

	// All should have the same thread prefix for thread queries
	expectedPrefix := "THREAD#thread-123#RCVD#"
	for i, lsi := range []string{lsi1, lsi2, lsi3} {
		if len(lsi) < len(expectedPrefix) || lsi[:len(expectedPrefix)] != expectedPrefix {
			t.Errorf("LSI3SK %d doesn't have expected prefix: got %q, want prefix %q", i, lsi, expectedPrefix)
		}
	}
}

func TestEmailItem_HeaderSize(t *testing.T) {
	email := EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		BlobID:     "blob-789",
		HeaderSize: 512,
	}

	if email.HeaderSize != 512 {
		t.Errorf("HeaderSize = %d, want 512", email.HeaderSize)
	}

	// Verify JSON round-trip preserves HeaderSize
	data, err := json.Marshal(email)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var parsed EmailItem
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if parsed.HeaderSize != 512 {
		t.Errorf("After round-trip HeaderSize = %d, want 512", parsed.HeaderSize)
	}
}
