package email

import (
	"strings"
	"testing"
	"time"
)

// Simple plain text email
const simpleEmail = `From: Alice <alice@example.com>
To: Bob <bob@example.com>
Subject: Hello World
Date: Sat, 20 Jan 2024 10:00:00 +0000
Message-ID: <msg-123@example.com>

This is a simple plain text email body.
It has multiple lines.
`

// Email with multiple recipients
const multiRecipientEmail = `From: Alice <alice@example.com>
To: Bob <bob@example.com>, Charlie <charlie@example.com>
Cc: Dave <dave@example.com>
Reply-To: Alice Reply <alice-reply@example.com>
Subject: Multiple Recipients
Date: Sat, 20 Jan 2024 10:00:00 +0000
Message-ID: <msg-456@example.com>
In-Reply-To: <msg-000@example.com>
References: <msg-000@example.com> <msg-001@example.com>

Body text here.
`

// Multipart MIME email with HTML
const multipartEmail = `From: Alice <alice@example.com>
To: Bob <bob@example.com>
Subject: Multipart Email
Date: Sat, 20 Jan 2024 10:00:00 +0000
Message-ID: <msg-789@example.com>
MIME-Version: 1.0
Content-Type: multipart/alternative; boundary="boundary1"

--boundary1
Content-Type: text/plain; charset="utf-8"

This is the plain text version.
--boundary1
Content-Type: text/html; charset="utf-8"

<html><body><p>This is the HTML version.</p></body></html>
--boundary1--
`

// Email with attachment
const attachmentEmail = `From: Alice <alice@example.com>
To: Bob <bob@example.com>
Subject: Email with Attachment
Date: Sat, 20 Jan 2024 10:00:00 +0000
Message-ID: <msg-att@example.com>
MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="boundary2"

--boundary2
Content-Type: text/plain; charset="utf-8"

Here is the document you requested.
--boundary2
Content-Type: application/pdf; name="document.pdf"
Content-Disposition: attachment; filename="document.pdf"
Content-Transfer-Encoding: base64

JVBERi0xLjQKJeLjz9MKMyAwIG9iago8PC9UeXBlL1BhZ2UvUGFyZW50IDEgMCBSPj4KZW5kb2Jq
--boundary2--
`

func TestParser_SimpleEmail(t *testing.T) {
	parsed, err := ParseRFC5322([]byte(simpleEmail))
	if err != nil {
		t.Fatalf("ParseRFC5322 failed: %v", err)
	}

	if parsed.Subject != "Hello World" {
		t.Errorf("Subject = %q, want %q", parsed.Subject, "Hello World")
	}

	if len(parsed.From) != 1 {
		t.Fatalf("From length = %d, want 1", len(parsed.From))
	}
	if parsed.From[0].Name != "Alice" {
		t.Errorf("From[0].Name = %q, want %q", parsed.From[0].Name, "Alice")
	}
	if parsed.From[0].Email != "alice@example.com" {
		t.Errorf("From[0].Email = %q, want %q", parsed.From[0].Email, "alice@example.com")
	}

	if len(parsed.To) != 1 {
		t.Fatalf("To length = %d, want 1", len(parsed.To))
	}
	if parsed.To[0].Name != "Bob" {
		t.Errorf("To[0].Name = %q, want %q", parsed.To[0].Name, "Bob")
	}

	if len(parsed.MessageID) != 1 || parsed.MessageID[0] != "<msg-123@example.com>" {
		t.Errorf("MessageID = %v, want [<msg-123@example.com>]", parsed.MessageID)
	}

	expectedDate := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)
	if !parsed.SentAt.Equal(expectedDate) {
		t.Errorf("SentAt = %v, want %v", parsed.SentAt, expectedDate)
	}

	if !strings.Contains(parsed.Preview, "simple plain text email") {
		t.Errorf("Preview should contain body text, got %q", parsed.Preview)
	}
}

func TestParser_MultipleRecipients(t *testing.T) {
	parsed, err := ParseRFC5322([]byte(multiRecipientEmail))
	if err != nil {
		t.Fatalf("ParseRFC5322 failed: %v", err)
	}

	if len(parsed.To) != 2 {
		t.Fatalf("To length = %d, want 2", len(parsed.To))
	}
	if parsed.To[0].Email != "bob@example.com" {
		t.Errorf("To[0].Email = %q, want %q", parsed.To[0].Email, "bob@example.com")
	}
	if parsed.To[1].Email != "charlie@example.com" {
		t.Errorf("To[1].Email = %q, want %q", parsed.To[1].Email, "charlie@example.com")
	}

	if len(parsed.CC) != 1 {
		t.Fatalf("CC length = %d, want 1", len(parsed.CC))
	}
	if parsed.CC[0].Email != "dave@example.com" {
		t.Errorf("CC[0].Email = %q, want %q", parsed.CC[0].Email, "dave@example.com")
	}

	if len(parsed.ReplyTo) != 1 {
		t.Fatalf("ReplyTo length = %d, want 1", len(parsed.ReplyTo))
	}
	if parsed.ReplyTo[0].Email != "alice-reply@example.com" {
		t.Errorf("ReplyTo[0].Email = %q, want %q", parsed.ReplyTo[0].Email, "alice-reply@example.com")
	}

	if len(parsed.InReplyTo) != 1 || parsed.InReplyTo[0] != "<msg-000@example.com>" {
		t.Errorf("InReplyTo = %v, want [<msg-000@example.com>]", parsed.InReplyTo)
	}

	if len(parsed.References) != 2 {
		t.Fatalf("References length = %d, want 2", len(parsed.References))
	}
}

func TestParser_MultipartEmail(t *testing.T) {
	parsed, err := ParseRFC5322([]byte(multipartEmail))
	if err != nil {
		t.Fatalf("ParseRFC5322 failed: %v", err)
	}

	if parsed.BodyStructure.Type != "multipart/alternative" {
		t.Errorf("BodyStructure.Type = %q, want %q", parsed.BodyStructure.Type, "multipart/alternative")
	}

	if len(parsed.BodyStructure.SubParts) != 2 {
		t.Fatalf("SubParts length = %d, want 2", len(parsed.BodyStructure.SubParts))
	}

	// Should have text/plain part
	if parsed.BodyStructure.SubParts[0].Type != "text/plain" {
		t.Errorf("SubParts[0].Type = %q, want %q", parsed.BodyStructure.SubParts[0].Type, "text/plain")
	}

	// Should have text/html part
	if parsed.BodyStructure.SubParts[1].Type != "text/html" {
		t.Errorf("SubParts[1].Type = %q, want %q", parsed.BodyStructure.SubParts[1].Type, "text/html")
	}

	// TextBody should reference the plain text part
	if len(parsed.TextBody) != 1 {
		t.Errorf("TextBody length = %d, want 1", len(parsed.TextBody))
	}

	// HTMLBody should reference the HTML part
	if len(parsed.HTMLBody) != 1 {
		t.Errorf("HTMLBody length = %d, want 1", len(parsed.HTMLBody))
	}

	// No attachments in this email
	if parsed.HasAttachment {
		t.Errorf("HasAttachment = true, want false")
	}
}

func TestParser_AttachmentEmail(t *testing.T) {
	parsed, err := ParseRFC5322([]byte(attachmentEmail))
	if err != nil {
		t.Fatalf("ParseRFC5322 failed: %v", err)
	}

	if !parsed.HasAttachment {
		t.Errorf("HasAttachment = false, want true")
	}

	if len(parsed.Attachments) != 1 {
		t.Fatalf("Attachments length = %d, want 1", len(parsed.Attachments))
	}

	// Find the attachment part
	var attachmentPart *BodyPart
	for i := range parsed.BodyStructure.SubParts {
		if parsed.BodyStructure.SubParts[i].Disposition == "attachment" {
			attachmentPart = &parsed.BodyStructure.SubParts[i]
			break
		}
	}

	if attachmentPart == nil {
		t.Fatal("Could not find attachment part in BodyStructure")
	}

	if attachmentPart.Name != "document.pdf" {
		t.Errorf("Attachment name = %q, want %q", attachmentPart.Name, "document.pdf")
	}
}

func TestParser_PreviewTruncation(t *testing.T) {
	// Create an email with a very long body
	longBody := strings.Repeat("This is a long sentence that will be repeated many times. ", 100)
	longEmail := `From: Alice <alice@example.com>
To: Bob <bob@example.com>
Subject: Long Email
Date: Sat, 20 Jan 2024 10:00:00 +0000
Message-ID: <msg-long@example.com>

` + longBody

	parsed, err := ParseRFC5322([]byte(longEmail))
	if err != nil {
		t.Fatalf("ParseRFC5322 failed: %v", err)
	}

	// Preview should be truncated to ~256 chars
	if len(parsed.Preview) > 300 {
		t.Errorf("Preview length = %d, should be ~256 chars", len(parsed.Preview))
	}

	// Preview should still contain some text
	if len(parsed.Preview) < 100 {
		t.Errorf("Preview length = %d, should be at least 100 chars", len(parsed.Preview))
	}
}

func TestParser_EmptyFields(t *testing.T) {
	minimalEmail := `From: alice@example.com
Subject: Minimal
Date: Sat, 20 Jan 2024 10:00:00 +0000

Body.
`

	parsed, err := ParseRFC5322([]byte(minimalEmail))
	if err != nil {
		t.Fatalf("ParseRFC5322 failed: %v", err)
	}

	// From should still be parsed (without display name)
	if len(parsed.From) != 1 {
		t.Fatalf("From length = %d, want 1", len(parsed.From))
	}
	if parsed.From[0].Email != "alice@example.com" {
		t.Errorf("From[0].Email = %q, want %q", parsed.From[0].Email, "alice@example.com")
	}

	// Optional fields should be empty slices, not nil
	if parsed.To == nil {
		t.Errorf("To should be empty slice, not nil")
	}
	if parsed.CC == nil {
		t.Errorf("CC should be empty slice, not nil")
	}
}

func TestParser_Size(t *testing.T) {
	parsed, err := ParseRFC5322([]byte(simpleEmail))
	if err != nil {
		t.Fatalf("ParseRFC5322 failed: %v", err)
	}

	expectedSize := int64(len(simpleEmail))
	if parsed.Size != expectedSize {
		t.Errorf("Size = %d, want %d", parsed.Size, expectedSize)
	}
}
