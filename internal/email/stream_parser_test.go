package email

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// mockUploader implements BlobUploader for testing.
type mockUploader struct {
	uploads []uploadCall
	blobID  string
	size    int64
	err     error
}

type uploadCall struct {
	accountID    string
	parentBlobID string
	contentType  string
	content      []byte
}

func (m *mockUploader) Upload(ctx context.Context, accountID, parentBlobID, contentType string, body io.Reader) (string, int64, error) {
	if m.err != nil {
		return "", 0, m.err
	}
	content, _ := io.ReadAll(body)
	m.uploads = append(m.uploads, uploadCall{
		accountID:    accountID,
		parentBlobID: parentBlobID,
		contentType:  contentType,
		content:      content,
	})
	return m.blobID, m.size, nil
}

func TestParseRFC5322Stream_SimplePlainText7bit(t *testing.T) {
	email := `From: alice@example.com
To: bob@example.com
Subject: Test
Date: Sat, 20 Jan 2024 10:00:00 +0000
Content-Type: text/plain; charset=utf-8
Content-Transfer-Encoding: 7bit

Hello, this is a test email.
`
	uploader := &mockUploader{blobID: "uploaded-blob", size: 100}

	parsed, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"email-blob-123",
		"account-456",
		uploader,
	)
	if err != nil {
		t.Fatalf("ParseRFC5322Stream error = %v", err)
	}

	// Check basic parsing
	if parsed.Subject != "Test" {
		t.Errorf("Subject = %q, want %q", parsed.Subject, "Test")
	}

	// For 7bit encoding (identity), should use byte-range blobId
	if parsed.BodyStructure.BlobID == "" {
		t.Error("BodyStructure.BlobID should not be empty")
	}
	if !strings.HasPrefix(parsed.BodyStructure.BlobID, "email-blob-123,") {
		t.Errorf("BlobID = %q, should start with email-blob-123,", parsed.BodyStructure.BlobID)
	}

	// Should NOT have uploaded anything (identity encoding)
	if len(uploader.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 (identity encoding should not upload)", len(uploader.uploads))
	}
}

func TestParseRFC5322Stream_Base64Attachment(t *testing.T) {
	// Base64 encoded "Hello World"
	email := `From: alice@example.com
To: bob@example.com
Subject: With Attachment
MIME-Version: 1.0
Content-Type: application/octet-stream; name="test.bin"
Content-Transfer-Encoding: base64
Content-Disposition: attachment; filename="test.bin"

SGVsbG8gV29ybGQ=
`
	uploader := &mockUploader{blobID: "decoded-blob-789", size: 11}

	parsed, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"email-blob-123",
		"account-456",
		uploader,
	)
	if err != nil {
		t.Fatalf("ParseRFC5322Stream error = %v", err)
	}

	// For base64 encoding (non-identity), should upload and use returned blobId
	if parsed.BodyStructure.BlobID != "decoded-blob-789" {
		t.Errorf("BlobID = %q, want %q", parsed.BodyStructure.BlobID, "decoded-blob-789")
	}

	// Should have uploaded the decoded content
	if len(uploader.uploads) != 1 {
		t.Fatalf("uploads = %d, want 1", len(uploader.uploads))
	}
	if string(uploader.uploads[0].content) != "Hello World" {
		t.Errorf("uploaded content = %q, want %q", uploader.uploads[0].content, "Hello World")
	}
	if uploader.uploads[0].parentBlobID != "email-blob-123" {
		t.Errorf("parentBlobID = %q, want %q", uploader.uploads[0].parentBlobID, "email-blob-123")
	}

	// Size should be decoded size
	if parsed.BodyStructure.Size != 11 {
		t.Errorf("Size = %d, want 11", parsed.BodyStructure.Size)
	}
}

func TestParseRFC5322Stream_QuotedPrintable(t *testing.T) {
	// Quoted-printable encoded text
	email := `From: alice@example.com
To: bob@example.com
Subject: QP Test
Content-Type: text/plain; charset=utf-8
Content-Transfer-Encoding: quoted-printable

Hello=20World=21
`
	uploader := &mockUploader{blobID: "qp-blob-456", size: 12}

	parsed, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"email-blob-123",
		"account-456",
		uploader,
	)
	if err != nil {
		t.Fatalf("ParseRFC5322Stream error = %v", err)
	}

	// For quoted-printable (non-identity), should upload decoded
	if parsed.BodyStructure.BlobID != "qp-blob-456" {
		t.Errorf("BlobID = %q, want %q", parsed.BodyStructure.BlobID, "qp-blob-456")
	}

	// Should have uploaded the decoded content
	if len(uploader.uploads) != 1 {
		t.Fatalf("uploads = %d, want 1", len(uploader.uploads))
	}
	// "Hello=20World=21\n" decodes to "Hello World!\n" (includes trailing newline from email)
	if string(uploader.uploads[0].content) != "Hello World!\n" {
		t.Errorf("uploaded content = %q, want %q", uploader.uploads[0].content, "Hello World!\n")
	}
}

func TestParseRFC5322Stream_MultipartAlternative(t *testing.T) {
	email := `From: alice@example.com
To: bob@example.com
Subject: Multipart
MIME-Version: 1.0
Content-Type: multipart/alternative; boundary="boundary1"

--boundary1
Content-Type: text/plain; charset=utf-8
Content-Transfer-Encoding: 7bit

Plain text version.
--boundary1
Content-Type: text/html; charset=utf-8
Content-Transfer-Encoding: 7bit

<html><body>HTML version.</body></html>
--boundary1--
`
	uploader := &sequentialUploader{prefix: "sub-blob"}

	parsed, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"email-blob-123",
		"account-456",
		uploader,
	)
	if err != nil {
		t.Fatalf("ParseRFC5322Stream error = %v", err)
	}

	// Root should be multipart/alternative with no blobId
	if parsed.BodyStructure.Type != "multipart/alternative" {
		t.Errorf("Type = %q, want %q", parsed.BodyStructure.Type, "multipart/alternative")
	}
	if parsed.BodyStructure.BlobID != "" {
		t.Errorf("multipart BlobID = %q, should be empty", parsed.BodyStructure.BlobID)
	}

	// Should have 2 sub-parts
	if len(parsed.BodyStructure.SubParts) != 2 {
		t.Fatalf("SubParts = %d, want 2", len(parsed.BodyStructure.SubParts))
	}

	// First part is text/plain - in streaming mode, all multipart sub-parts are uploaded
	plainPart := parsed.BodyStructure.SubParts[0]
	if plainPart.Type != "text/plain" {
		t.Errorf("SubParts[0].Type = %q, want %q", plainPart.Type, "text/plain")
	}
	if plainPart.BlobID != "sub-blob-1" {
		t.Errorf("SubParts[0].BlobID = %q, want %q", plainPart.BlobID, "sub-blob-1")
	}

	// Second part is text/html - also uploaded
	htmlPart := parsed.BodyStructure.SubParts[1]
	if htmlPart.Type != "text/html" {
		t.Errorf("SubParts[1].Type = %q, want %q", htmlPart.Type, "text/html")
	}
	if htmlPart.BlobID != "sub-blob-2" {
		t.Errorf("SubParts[1].BlobID = %q, want %q", htmlPart.BlobID, "sub-blob-2")
	}

	// All multipart sub-parts are uploaded in streaming mode
	if len(uploader.uploads) != 2 {
		t.Errorf("uploads = %d, want 2", len(uploader.uploads))
	}
}

func TestParseRFC5322Stream_NestedMultipart(t *testing.T) {
	email := `From: alice@example.com
To: bob@example.com
Subject: Nested
MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="outer"

--outer
Content-Type: multipart/alternative; boundary="inner"

--inner
Content-Type: text/plain

Plain text.
--inner
Content-Type: text/html

<p>HTML</p>
--inner--
--outer
Content-Type: application/pdf; name="doc.pdf"
Content-Disposition: attachment; filename="doc.pdf"
Content-Transfer-Encoding: base64

JVBERi0xLjQ=
--outer--
`
	uploader := &sequentialUploader{prefix: "part"}

	parsed, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"email-blob-123",
		"account-456",
		uploader,
	)
	if err != nil {
		t.Fatalf("ParseRFC5322Stream error = %v", err)
	}

	// Root should be multipart/mixed
	if parsed.BodyStructure.Type != "multipart/mixed" {
		t.Errorf("Type = %q, want %q", parsed.BodyStructure.Type, "multipart/mixed")
	}

	// Should have 2 sub-parts (multipart/alternative + PDF)
	if len(parsed.BodyStructure.SubParts) != 2 {
		t.Fatalf("SubParts = %d, want 2", len(parsed.BodyStructure.SubParts))
	}

	// First is nested multipart/alternative
	altPart := parsed.BodyStructure.SubParts[0]
	if altPart.Type != "multipart/alternative" {
		t.Errorf("SubParts[0].Type = %q, want %q", altPart.Type, "multipart/alternative")
	}
	if len(altPart.SubParts) != 2 {
		t.Errorf("SubParts[0].SubParts = %d, want 2", len(altPart.SubParts))
	}

	// Second is PDF attachment (base64)
	pdfPart := parsed.BodyStructure.SubParts[1]
	if pdfPart.Type != "application/pdf" {
		t.Errorf("SubParts[1].Type = %q, want %q", pdfPart.Type, "application/pdf")
	}

	// In streaming mode, ALL leaf parts are uploaded (plain + html + pdf = 3)
	if len(uploader.uploads) != 3 {
		t.Errorf("uploads = %d, want 3", len(uploader.uploads))
	}
}

func TestParseRFC5322Stream_UnknownEncodingTreatedAsIdentity(t *testing.T) {
	// Per RFC 8621, unknown encodings should be treated as identity
	email := `From: alice@example.com
To: bob@example.com
Subject: Unknown Encoding
Content-Type: text/plain
Content-Transfer-Encoding: x-custom-encoding

Some content here.
`
	uploader := &mockUploader{blobID: "should-not-use", size: 100}

	parsed, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"email-blob-123",
		"account-456",
		uploader,
	)
	if err != nil {
		t.Fatalf("ParseRFC5322Stream error = %v", err)
	}

	// Unknown encoding should be treated as identity (byte-range)
	if !strings.HasPrefix(parsed.BodyStructure.BlobID, "email-blob-123,") {
		t.Errorf("BlobID = %q, should be byte-range format for unknown encoding", parsed.BodyStructure.BlobID)
	}

	// Should NOT upload
	if len(uploader.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 for unknown encoding", len(uploader.uploads))
	}
}

func TestParseRFC5322Stream_8bitEncoding(t *testing.T) {
	email := `From: alice@example.com
To: bob@example.com
Subject: 8bit Test
Content-Type: text/plain; charset=utf-8
Content-Transfer-Encoding: 8bit

Héllo Wörld with UTF-8 bytes.
`
	uploader := &mockUploader{}

	parsed, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"email-blob-123",
		"account-456",
		uploader,
	)
	if err != nil {
		t.Fatalf("ParseRFC5322Stream error = %v", err)
	}

	// 8bit is identity encoding
	if !strings.HasPrefix(parsed.BodyStructure.BlobID, "email-blob-123,") {
		t.Errorf("BlobID = %q, should be byte-range format for 8bit", parsed.BodyStructure.BlobID)
	}

	if len(uploader.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 for 8bit encoding", len(uploader.uploads))
	}
}

func TestParseRFC5322Stream_BinaryEncoding(t *testing.T) {
	email := `From: alice@example.com
To: bob@example.com
Subject: Binary Test
Content-Type: application/octet-stream
Content-Transfer-Encoding: binary

` + string([]byte{0x00, 0x01, 0x02, 0x03})

	uploader := &mockUploader{}

	parsed, err := ParseRFC5322Stream(
		context.Background(),
		bytes.NewReader([]byte(email)),
		"email-blob-123",
		"account-456",
		uploader,
	)
	if err != nil {
		t.Fatalf("ParseRFC5322Stream error = %v", err)
	}

	// binary is identity encoding
	if !strings.HasPrefix(parsed.BodyStructure.BlobID, "email-blob-123,") {
		t.Errorf("BlobID = %q, should be byte-range format for binary", parsed.BodyStructure.BlobID)
	}

	if len(uploader.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 for binary encoding", len(uploader.uploads))
	}
}

func TestParseRFC5322Stream_NoEncodingHeader(t *testing.T) {
	// No Content-Transfer-Encoding header - defaults to identity
	email := `From: alice@example.com
To: bob@example.com
Subject: No Encoding
Content-Type: text/plain

Simple message without encoding header.
`
	uploader := &mockUploader{}

	parsed, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"email-blob-123",
		"account-456",
		uploader,
	)
	if err != nil {
		t.Fatalf("ParseRFC5322Stream error = %v", err)
	}

	// No encoding = identity
	if !strings.HasPrefix(parsed.BodyStructure.BlobID, "email-blob-123,") {
		t.Errorf("BlobID = %q, should be byte-range format for no encoding", parsed.BodyStructure.BlobID)
	}

	if len(uploader.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 for no encoding header", len(uploader.uploads))
	}
}

func TestParseRFC5322Stream_FromField_WithCRLF(t *testing.T) {
	// Exactly match the E2E test format (CRLF line endings)
	email := "From: Test Sender <sender@example.com>\r\n" +
		"To: Test Recipient <recipient@example.com>\r\n" +
		"Subject: Test Email for E2E Import Verification\r\n" +
		"Date: Sat, 20 Jan 2024 10:00:00 +0000\r\n" +
		"Content-Type: text/plain; charset=utf-8\r\n" +
		"\r\n" +
		"This is the test email body content.\r\n"

	uploader := &mockUploader{blobID: "test-blob", size: 100}

	parsed, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"email-blob-123",
		"account-456",
		uploader,
	)
	if err != nil {
		t.Fatalf("ParseRFC5322Stream error = %v", err)
	}

	// Verify From field
	if len(parsed.From) != 1 {
		t.Fatalf("From length = %d, want 1; From=%v", len(parsed.From), parsed.From)
	}
	if parsed.From[0].Name != "Test Sender" {
		t.Errorf("From[0].Name = %q, want %q", parsed.From[0].Name, "Test Sender")
	}
	if parsed.From[0].Email != "sender@example.com" {
		t.Errorf("From[0].Email = %q, want %q", parsed.From[0].Email, "sender@example.com")
	}

	// Verify To field for comparison
	if len(parsed.To) != 1 {
		t.Fatalf("To length = %d, want 1; To=%v", len(parsed.To), parsed.To)
	}
	if parsed.To[0].Email != "recipient@example.com" {
		t.Errorf("To[0].Email = %q, want %q", parsed.To[0].Email, "recipient@example.com")
	}
}

func TestIsIdentityEncoding(t *testing.T) {
	tests := []struct {
		encoding string
		want     bool
	}{
		{"7bit", true},
		{"7BIT", true},
		{"8bit", true},
		{"8BIT", true},
		{"binary", true},
		{"BINARY", true},
		{"", true},
		{"base64", false},
		{"BASE64", false},
		{"quoted-printable", false},
		{"QUOTED-PRINTABLE", false},
		{"x-custom", true}, // unknown treated as identity
	}

	for _, tt := range tests {
		t.Run(tt.encoding, func(t *testing.T) {
			got := isIdentityEncoding(tt.encoding)
			if got != tt.want {
				t.Errorf("isIdentityEncoding(%q) = %v, want %v", tt.encoding, got, tt.want)
			}
		})
	}
}

func TestParseRFC5322Stream_SenderHeader(t *testing.T) {
	email := `From: Alice <alice@example.com>
Sender: Secretary <secretary@example.com>
To: Bob <bob@example.com>
Subject: Email with Sender
Date: Sat, 20 Jan 2024 10:00:00 +0000
Content-Type: text/plain

Body text here.
`
	uploader := &mockUploader{}

	parsed, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"email-blob-123",
		"account-456",
		uploader,
	)
	if err != nil {
		t.Fatalf("ParseRFC5322Stream error = %v", err)
	}

	if len(parsed.Sender) != 1 {
		t.Fatalf("Sender length = %d, want 1", len(parsed.Sender))
	}
	if parsed.Sender[0].Name != "Secretary" {
		t.Errorf("Sender[0].Name = %q, want %q", parsed.Sender[0].Name, "Secretary")
	}
	if parsed.Sender[0].Email != "secretary@example.com" {
		t.Errorf("Sender[0].Email = %q, want %q", parsed.Sender[0].Email, "secretary@example.com")
	}
}

func TestParseRFC5322Stream_BccHeader(t *testing.T) {
	email := `From: Alice <alice@example.com>
To: Bob <bob@example.com>
Bcc: Secret <secret@example.com>, Hidden <hidden@example.com>
Subject: Email with Bcc
Date: Sat, 20 Jan 2024 10:00:00 +0000
Content-Type: text/plain

Body text here.
`
	uploader := &mockUploader{}

	parsed, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"email-blob-123",
		"account-456",
		uploader,
	)
	if err != nil {
		t.Fatalf("ParseRFC5322Stream error = %v", err)
	}

	if len(parsed.Bcc) != 2 {
		t.Fatalf("Bcc length = %d, want 2", len(parsed.Bcc))
	}
	if parsed.Bcc[0].Name != "Secret" {
		t.Errorf("Bcc[0].Name = %q, want %q", parsed.Bcc[0].Name, "Secret")
	}
	if parsed.Bcc[0].Email != "secret@example.com" {
		t.Errorf("Bcc[0].Email = %q, want %q", parsed.Bcc[0].Email, "secret@example.com")
	}
	if parsed.Bcc[1].Name != "Hidden" {
		t.Errorf("Bcc[1].Name = %q, want %q", parsed.Bcc[1].Name, "Hidden")
	}
	if parsed.Bcc[1].Email != "hidden@example.com" {
		t.Errorf("Bcc[1].Email = %q, want %q", parsed.Bcc[1].Email, "hidden@example.com")
	}
}

func TestParseRFC5322Stream_EmptySenderAndBcc(t *testing.T) {
	// Simple email without Sender or Bcc headers
	email := `From: Alice <alice@example.com>
To: Bob <bob@example.com>
Subject: Simple
Date: Sat, 20 Jan 2024 10:00:00 +0000
Content-Type: text/plain

Body.
`
	uploader := &mockUploader{}

	parsed, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"email-blob-123",
		"account-456",
		uploader,
	)
	if err != nil {
		t.Fatalf("ParseRFC5322Stream error = %v", err)
	}

	// Sender and Bcc should be empty slices, not nil
	if parsed.Sender == nil {
		t.Error("Sender should be empty slice, not nil")
	}
	if len(parsed.Sender) != 0 {
		t.Errorf("Sender length = %d, want 0", len(parsed.Sender))
	}
	if parsed.Bcc == nil {
		t.Error("Bcc should be empty slice, not nil")
	}
	if len(parsed.Bcc) != 0 {
		t.Errorf("Bcc length = %d, want 0", len(parsed.Bcc))
	}
}

func TestParseRFC5322Stream_HeaderSize(t *testing.T) {
	// Test that HeaderSize correctly reports the byte offset where headers end
	// The headers in this email are exactly 112 bytes (including final CRLF before body)
	email := "From: alice@example.com\r\n" +
		"To: bob@example.com\r\n" +
		"Subject: Test\r\n" +
		"Date: Sat, 20 Jan 2024 10:00:00 +0000\r\n" +
		"\r\n" +
		"Body content here."

	uploader := &mockUploader{}

	parsed, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"email-blob-123",
		"account-456",
		uploader,
	)
	if err != nil {
		t.Fatalf("ParseRFC5322Stream error = %v", err)
	}

	// Calculate expected header size: all header lines + blank line separator
	// "From: alice@example.com\r\n" = 25
	// "To: bob@example.com\r\n" = 21
	// "Subject: Test\r\n" = 15
	// "Date: Sat, 20 Jan 2024 10:00:00 +0000\r\n" = 39
	// "\r\n" = 2
	// Total = 102
	expectedHeaderSize := int64(102)

	if parsed.HeaderSize != expectedHeaderSize {
		t.Errorf("HeaderSize = %d, want %d", parsed.HeaderSize, expectedHeaderSize)
	}
}

func TestParseRFC5322Stream_HeaderSize_LFOnly(t *testing.T) {
	// Test with LF-only line endings (common in some systems)
	email := "From: alice@example.com\n" +
		"To: bob@example.com\n" +
		"Subject: Test\n" +
		"\n" +
		"Body content here."

	uploader := &mockUploader{}

	parsed, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"email-blob-123",
		"account-456",
		uploader,
	)
	if err != nil {
		t.Fatalf("ParseRFC5322Stream error = %v", err)
	}

	// "From: alice@example.com\n" = 24
	// "To: bob@example.com\n" = 20
	// "Subject: Test\n" = 14
	// "\n" = 1
	// Total = 59
	expectedHeaderSize := int64(59)

	if parsed.HeaderSize != expectedHeaderSize {
		t.Errorf("HeaderSize = %d, want %d", parsed.HeaderSize, expectedHeaderSize)
	}
}

func TestReadHeaderBytes_CRLFSeparator(t *testing.T) {
	input := "From: alice@example.com\r\nTo: bob@example.com\r\n\r\nBody content here."
	headerData, bodyReader, err := readHeaderBytes(strings.NewReader(input))
	if err != nil {
		t.Fatalf("readHeaderBytes error = %v", err)
	}

	// Header data should include everything up to and including the separator
	expectedHeader := "From: alice@example.com\r\nTo: bob@example.com\r\n\r\n"
	if string(headerData) != expectedHeader {
		t.Errorf("headerData = %q, want %q", string(headerData), expectedHeader)
	}

	// Body reader should return the remaining bytes
	bodyBytes, err := io.ReadAll(bodyReader)
	if err != nil {
		t.Fatalf("ReadAll(bodyReader) error = %v", err)
	}
	if string(bodyBytes) != "Body content here." {
		t.Errorf("body = %q, want %q", string(bodyBytes), "Body content here.")
	}
}

func TestReadHeaderBytes_LFOnlySeparator(t *testing.T) {
	input := "From: alice@example.com\nTo: bob@example.com\n\nBody content here."
	headerData, bodyReader, err := readHeaderBytes(strings.NewReader(input))
	if err != nil {
		t.Fatalf("readHeaderBytes error = %v", err)
	}

	expectedHeader := "From: alice@example.com\nTo: bob@example.com\n\n"
	if string(headerData) != expectedHeader {
		t.Errorf("headerData = %q, want %q", string(headerData), expectedHeader)
	}

	bodyBytes, err := io.ReadAll(bodyReader)
	if err != nil {
		t.Fatalf("ReadAll(bodyReader) error = %v", err)
	}
	if string(bodyBytes) != "Body content here." {
		t.Errorf("body = %q, want %q", string(bodyBytes), "Body content here.")
	}
}

func TestReadHeaderBytes_LargeHeaders(t *testing.T) {
	// Build headers > 4KB to span multiple read chunks
	var sb strings.Builder
	sb.WriteString("From: alice@example.com\r\n")
	for i := 0; i < 100; i++ {
		sb.WriteString("X-Custom-Header-" + strings.Repeat("a", 40) + ": value\r\n")
	}
	sb.WriteString("\r\n")
	sb.WriteString("Body after large headers.")

	input := sb.String()
	headerData, bodyReader, err := readHeaderBytes(strings.NewReader(input))
	if err != nil {
		t.Fatalf("readHeaderBytes error = %v", err)
	}

	// Verify header data length matches everything up to separator
	expectedHeaderLen := len(input) - len("Body after large headers.")
	if len(headerData) != expectedHeaderLen {
		t.Errorf("headerData length = %d, want %d", len(headerData), expectedHeaderLen)
	}

	bodyBytes, err := io.ReadAll(bodyReader)
	if err != nil {
		t.Fatalf("ReadAll(bodyReader) error = %v", err)
	}
	if string(bodyBytes) != "Body after large headers." {
		t.Errorf("body = %q, want %q", string(bodyBytes), "Body after large headers.")
	}
}

func TestReadHeaderBytes_HeadersOnly(t *testing.T) {
	// Message with headers and separator but no body (EOF after separator)
	input := "From: alice@example.com\r\nSubject: Test\r\n\r\n"
	headerData, bodyReader, err := readHeaderBytes(strings.NewReader(input))
	if err != nil {
		t.Fatalf("readHeaderBytes error = %v", err)
	}

	if string(headerData) != input {
		t.Errorf("headerData = %q, want %q", string(headerData), input)
	}

	bodyBytes, err := io.ReadAll(bodyReader)
	if err != nil {
		t.Fatalf("ReadAll(bodyReader) error = %v", err)
	}
	if len(bodyBytes) != 0 {
		t.Errorf("body = %q, want empty", string(bodyBytes))
	}
}

func TestReadHeaderBytes_BodyReaderReturnsCorrectRemaining(t *testing.T) {
	body := "This is the full body.\r\nWith multiple lines.\r\nAnd more content."
	input := "Subject: Test\r\n\r\n" + body
	_, bodyReader, err := readHeaderBytes(strings.NewReader(input))
	if err != nil {
		t.Fatalf("readHeaderBytes error = %v", err)
	}

	bodyBytes, err := io.ReadAll(bodyReader)
	if err != nil {
		t.Fatalf("ReadAll(bodyReader) error = %v", err)
	}
	if string(bodyBytes) != body {
		t.Errorf("body = %q, want %q", string(bodyBytes), body)
	}
}

func TestReadHeaderBytes_ExactLength(t *testing.T) {
	input := "From: alice@example.com\r\nTo: bob@example.com\r\n\r\nBody."
	headerData, _, err := readHeaderBytes(strings.NewReader(input))
	if err != nil {
		t.Fatalf("readHeaderBytes error = %v", err)
	}

	// "From: alice@example.com\r\n" = 25
	// "To: bob@example.com\r\n" = 21
	// "\r\n" = 2
	// Total = 48
	expectedLen := 48
	if len(headerData) != expectedLen {
		t.Errorf("headerData length = %d, want %d", len(headerData), expectedLen)
	}
}

func TestParseSinglePartStreaming_IdentityTextPlain(t *testing.T) {
	bodyContent := "Hello, this is a test email body."
	headerSize := int64(100) // simulated header size
	fullData := make([]byte, int(headerSize)+len(bodyContent))
	copy(fullData[int(headerSize):], bodyContent)

	cr := NewCountingReader(bytes.NewReader(fullData))
	// Advance past headers
	headerBuf := make([]byte, headerSize)
	_, _ = cr.Read(headerBuf)

	bodyReader := io.LimitReader(cr, int64(len(bodyContent)))
	pc := NewPreviewCapture(256)

	part, err := parseSinglePartStreaming(
		context.Background(),
		bodyReader,
		headerSize,
		cr,
		"text/plain",
		"7bit",
		"email-blob-123",
		"account-456",
		&mockUploader{blobID: "should-not-use", size: 100},
		pc,
	)
	if err != nil {
		t.Fatalf("parseSinglePartStreaming error = %v", err)
	}

	// Identity encoding should produce byte-range blob ID
	expectedBlobID := "email-blob-123,100,133"
	if part.BlobID != expectedBlobID {
		t.Errorf("BlobID = %q, want %q", part.BlobID, expectedBlobID)
	}
	if part.Size != int64(len(bodyContent)) {
		t.Errorf("Size = %d, want %d", part.Size, int64(len(bodyContent)))
	}
	// Preview should be captured for text/plain
	if pc.Preview() != bodyContent {
		t.Errorf("Preview = %q, want %q", pc.Preview(), bodyContent)
	}
}

func TestParseSinglePartStreaming_IdentityNonText(t *testing.T) {
	bodyContent := "binary-data-here"
	headerSize := int64(50)
	fullData := make([]byte, int(headerSize)+len(bodyContent))
	copy(fullData[int(headerSize):], bodyContent)

	cr := NewCountingReader(bytes.NewReader(fullData))
	headerBuf := make([]byte, headerSize)
	_, _ = cr.Read(headerBuf)

	bodyReader := io.LimitReader(cr, int64(len(bodyContent)))
	pc := NewPreviewCapture(256)

	part, err := parseSinglePartStreaming(
		context.Background(),
		bodyReader,
		headerSize,
		cr,
		"application/octet-stream",
		"binary",
		"email-blob-123",
		"account-456",
		&mockUploader{blobID: "should-not-use", size: 100},
		pc,
	)
	if err != nil {
		t.Fatalf("parseSinglePartStreaming error = %v", err)
	}

	// Identity encoding should produce byte-range blob ID
	if !strings.HasPrefix(part.BlobID, "email-blob-123,") {
		t.Errorf("BlobID = %q, should be byte-range format", part.BlobID)
	}
	// No preview capture for non-text
	if pc.Preview() != "" {
		t.Errorf("Preview = %q, want empty for non-text", pc.Preview())
	}
}

func TestParseSinglePartStreaming_Base64Upload(t *testing.T) {
	bodyContent := "SGVsbG8gV29ybGQ=" // base64("Hello World")
	headerSize := int64(50)
	fullData := make([]byte, int(headerSize)+len(bodyContent))
	copy(fullData[int(headerSize):], bodyContent)

	cr := NewCountingReader(bytes.NewReader(fullData))
	headerBuf := make([]byte, headerSize)
	_, _ = cr.Read(headerBuf)

	bodyReader := io.LimitReader(cr, int64(len(bodyContent)))
	pc := NewPreviewCapture(256)
	uploader := &mockUploader{blobID: "decoded-blob-789", size: 11}

	part, err := parseSinglePartStreaming(
		context.Background(),
		bodyReader,
		headerSize,
		cr,
		"application/octet-stream",
		"base64",
		"email-blob-123",
		"account-456",
		uploader,
		pc,
	)
	if err != nil {
		t.Fatalf("parseSinglePartStreaming error = %v", err)
	}

	// Non-identity should upload and return uploaded blob ID
	if part.BlobID != "decoded-blob-789" {
		t.Errorf("BlobID = %q, want %q", part.BlobID, "decoded-blob-789")
	}
	if part.Size != 11 {
		t.Errorf("Size = %d, want 11", part.Size)
	}
	if len(uploader.uploads) != 1 {
		t.Fatalf("uploads = %d, want 1", len(uploader.uploads))
	}
	if string(uploader.uploads[0].content) != "Hello World" {
		t.Errorf("uploaded content = %q, want %q", uploader.uploads[0].content, "Hello World")
	}
}

func TestParseSinglePartStreaming_QPTextPlainWithPreview(t *testing.T) {
	bodyContent := "Hello=20World=21\r\n" // QP encoded "Hello World!\r\n"
	headerSize := int64(50)
	fullData := make([]byte, int(headerSize)+len(bodyContent))
	copy(fullData[int(headerSize):], bodyContent)

	cr := NewCountingReader(bytes.NewReader(fullData))
	headerBuf := make([]byte, headerSize)
	_, _ = cr.Read(headerBuf)

	bodyReader := io.LimitReader(cr, int64(len(bodyContent)))
	pc := NewPreviewCapture(256)
	uploader := &mockUploader{blobID: "qp-blob-456", size: 13}

	part, err := parseSinglePartStreaming(
		context.Background(),
		bodyReader,
		headerSize,
		cr,
		"text/plain",
		"quoted-printable",
		"email-blob-123",
		"account-456",
		uploader,
		pc,
	)
	if err != nil {
		t.Fatalf("parseSinglePartStreaming error = %v", err)
	}

	// Non-identity should upload
	if part.BlobID != "qp-blob-456" {
		t.Errorf("BlobID = %q, want %q", part.BlobID, "qp-blob-456")
	}
	// Preview should capture decoded text/plain content
	if pc.Preview() == "" {
		t.Error("Preview should not be empty for decoded text/plain")
	}
}

// sequentialUploader returns incrementing blob IDs for each upload.
type sequentialUploader struct {
	uploads []uploadCall
	prefix  string
	counter int
}

func (s *sequentialUploader) Upload(ctx context.Context, accountID, parentBlobID, contentType string, body io.Reader) (string, int64, error) {
	content, _ := io.ReadAll(body)
	s.uploads = append(s.uploads, uploadCall{
		accountID:    accountID,
		parentBlobID: parentBlobID,
		contentType:  contentType,
		content:      content,
	})
	s.counter++
	blobID := fmt.Sprintf("%s-%d", s.prefix, s.counter)
	return blobID, int64(len(content)), nil
}

func TestParseMultipartStreaming_TwoPartAlternative(t *testing.T) {
	// Build a multipart/alternative body with text/plain + text/html
	body := "--boundary1\r\n" +
		"Content-Type: text/plain; charset=utf-8\r\n" +
		"Content-Transfer-Encoding: 7bit\r\n" +
		"\r\n" +
		"Plain text version.\r\n" +
		"--boundary1\r\n" +
		"Content-Type: text/html; charset=utf-8\r\n" +
		"Content-Transfer-Encoding: 7bit\r\n" +
		"\r\n" +
		"<html><body>HTML version.</body></html>\r\n" +
		"--boundary1--\r\n"

	uploader := &sequentialUploader{prefix: "part-blob"}
	counter := 0
	pc := NewPreviewCapture(256)

	parts, err := parseMultipartStreaming(
		context.Background(),
		strings.NewReader(body),
		"boundary1",
		"email-blob-123",
		"account-456",
		uploader,
		&counter,
		pc,
		1,
	)
	if err != nil {
		t.Fatalf("parseMultipartStreaming error = %v", err)
	}

	// Should have 2 sub-parts
	if len(parts) != 2 {
		t.Fatalf("parts = %d, want 2", len(parts))
	}

	// Both parts should be uploaded
	if len(uploader.uploads) != 2 {
		t.Fatalf("uploads = %d, want 2", len(uploader.uploads))
	}

	// First part: text/plain
	if parts[0].Type != "text/plain" {
		t.Errorf("parts[0].Type = %q, want %q", parts[0].Type, "text/plain")
	}
	if parts[0].BlobID != "part-blob-1" {
		t.Errorf("parts[0].BlobID = %q, want %q", parts[0].BlobID, "part-blob-1")
	}

	// Second part: text/html
	if parts[1].Type != "text/html" {
		t.Errorf("parts[1].Type = %q, want %q", parts[1].Type, "text/html")
	}
	if parts[1].BlobID != "part-blob-2" {
		t.Errorf("parts[1].BlobID = %q, want %q", parts[1].BlobID, "part-blob-2")
	}

	// Preview should be captured from first text/plain
	if pc.Preview() == "" {
		t.Error("Preview should be captured from text/plain part")
	}
}

func TestParseMultipartStreaming_NestedMultipart(t *testing.T) {
	body := "--outer\r\n" +
		"Content-Type: multipart/alternative; boundary=\"inner\"\r\n" +
		"\r\n" +
		"--inner\r\n" +
		"Content-Type: text/plain\r\n" +
		"\r\n" +
		"Plain text.\r\n" +
		"--inner\r\n" +
		"Content-Type: text/html\r\n" +
		"\r\n" +
		"<p>HTML</p>\r\n" +
		"--inner--\r\n" +
		"--outer\r\n" +
		"Content-Type: application/pdf; name=\"doc.pdf\"\r\n" +
		"Content-Disposition: attachment; filename=\"doc.pdf\"\r\n" +
		"Content-Transfer-Encoding: base64\r\n" +
		"\r\n" +
		"JVBERi0xLjQ=\r\n" +
		"--outer--\r\n"

	uploader := &sequentialUploader{prefix: "part"}
	counter := 0
	pc := NewPreviewCapture(256)

	parts, err := parseMultipartStreaming(
		context.Background(),
		strings.NewReader(body),
		"outer",
		"email-blob-123",
		"account-456",
		uploader,
		&counter,
		pc,
		1,
	)
	if err != nil {
		t.Fatalf("parseMultipartStreaming error = %v", err)
	}

	// Should have 2 top-level sub-parts (multipart/alternative + PDF)
	if len(parts) != 2 {
		t.Fatalf("parts = %d, want 2", len(parts))
	}

	// First is nested multipart/alternative with 2 sub-parts
	if parts[0].Type != "multipart/alternative" {
		t.Errorf("parts[0].Type = %q, want %q", parts[0].Type, "multipart/alternative")
	}
	if len(parts[0].SubParts) != 2 {
		t.Errorf("parts[0].SubParts = %d, want 2", len(parts[0].SubParts))
	}

	// Second is PDF attachment
	if parts[1].Type != "application/pdf" {
		t.Errorf("parts[1].Type = %q, want %q", parts[1].Type, "application/pdf")
	}

	// All 3 leaf parts should be uploaded (plain + html + pdf)
	if len(uploader.uploads) != 3 {
		t.Errorf("uploads = %d, want 3", len(uploader.uploads))
	}

	// Disposition and filename should be extracted
	if parts[1].Disposition != "attachment" {
		t.Errorf("parts[1].Disposition = %q, want %q", parts[1].Disposition, "attachment")
	}
	if parts[1].Name != "doc.pdf" {
		t.Errorf("parts[1].Name = %q, want %q", parts[1].Name, "doc.pdf")
	}
}

func TestParseMultipartStreaming_PreviewFromTextPlain(t *testing.T) {
	body := "--boundary\r\n" +
		"Content-Type: text/plain\r\n" +
		"\r\n" +
		"This is the preview text.\r\n" +
		"--boundary\r\n" +
		"Content-Type: text/html\r\n" +
		"\r\n" +
		"<p>Not preview</p>\r\n" +
		"--boundary--\r\n"

	uploader := &sequentialUploader{prefix: "blob"}
	counter := 0
	pc := NewPreviewCapture(256)

	_, err := parseMultipartStreaming(
		context.Background(),
		strings.NewReader(body),
		"boundary",
		"email-blob-123",
		"account-456",
		uploader,
		&counter,
		pc,
		1,
	)
	if err != nil {
		t.Fatalf("parseMultipartStreaming error = %v", err)
	}

	preview := pc.Preview()
	if !strings.Contains(preview, "This is the preview text") {
		t.Errorf("Preview = %q, want to contain %q", preview, "This is the preview text")
	}
}

// trackingWriter increments a shared counter on each Write call,
// allowing a maxOutstandingReader to track how many bytes have been consumed.
type trackingWriter struct {
	consumed *int64
}

func (tw *trackingWriter) Write(p []byte) (int, error) {
	n := len(p)
	*tw.consumed += int64(n)
	return n, nil
}

// maxOutstandingReader wraps an io.Reader and fails the test if the number of
// bytes produced (returned by Read) but not yet consumed exceeds a threshold.
// This deterministically proves the parser streams data rather than buffering
// the entire body in memory.
type maxOutstandingReader struct {
	inner          io.Reader
	consumed       *int64
	produced       int64
	maxOutstanding int64
	t              *testing.T
}

func (r *maxOutstandingReader) Read(p []byte) (int, error) {
	outstanding := r.produced - *r.consumed
	if outstanding > r.maxOutstanding {
		r.t.Fatalf("maxOutstandingReader: outstanding bytes %d exceeds threshold %d (produced=%d, consumed=%d)",
			outstanding, r.maxOutstanding, r.produced, *r.consumed)
	}
	n, err := r.inner.Read(p)
	r.produced += int64(n)
	return n, err
}

// trackingUploader implements BlobUploader by streaming body content through
// a trackingWriter that updates a shared consumed counter.
type trackingUploader struct {
	consumed *int64
}

func (u *trackingUploader) Upload(_ context.Context, _, _, _ string, body io.Reader) (string, int64, error) {
	tw := &trackingWriter{consumed: u.consumed}
	n, err := io.Copy(tw, body)
	if err != nil {
		return "", 0, err
	}
	return "tracking-blob", n, nil
}

func TestParseRFC5322Stream_NoFullBuffering(t *testing.T) {
	// Build a multipart email with a ~1MB text/plain part using 7bit encoding.
	// Multipart forces all leaf parts through the uploader, letting us track
	// consumption via a trackingUploader. Using 7bit encoding means raw bytes
	// read from the source ≈ bytes consumed by the uploader, so the
	// maxOutstandingReader can detect if the parser buffers the entire body.
	//
	// If the parser were to io.ReadAll the body, produced would reach ~1MB
	// while consumed stays at 0, far exceeding the 128KB threshold.
	largeBody := strings.Repeat("The quick brown fox jumps. ", 40000) // ~1.08MB
	boundary := "TESTBOUNDARY1234"

	email := "From: alice@example.com\r\n" +
		"To: bob@example.com\r\n" +
		"Subject: Streaming Buffering Test\r\n" +
		"MIME-Version: 1.0\r\n" +
		"Content-Type: multipart/mixed; boundary=\"" + boundary + "\"\r\n" +
		"\r\n" +
		"--" + boundary + "\r\n" +
		"Content-Type: text/plain; charset=utf-8\r\n" +
		"Content-Transfer-Encoding: 7bit\r\n" +
		"\r\n" +
		largeBody + "\r\n" +
		"--" + boundary + "--\r\n"

	var consumed int64
	reader := &maxOutstandingReader{
		inner:          strings.NewReader(email),
		consumed:       &consumed,
		produced:       0,
		maxOutstanding: 131072, // 128KB — well below the ~1MB body
		t:              t,
	}
	uploader := &trackingUploader{consumed: &consumed}

	parsed, err := ParseRFC5322Stream(
		context.Background(),
		reader,
		"email-blob-123",
		"account-456",
		uploader,
	)
	if err != nil {
		t.Fatalf("ParseRFC5322Stream error = %v", err)
	}

	if parsed.Subject != "Streaming Buffering Test" {
		t.Errorf("Subject = %q, want %q", parsed.Subject, "Streaming Buffering Test")
	}
	if parsed.Size != int64(len(email)) {
		t.Errorf("Size = %d, want %d", parsed.Size, len(email))
	}
}

func TestParseRFC5322Stream_PreviewFromMultipartTextPlain(t *testing.T) {
	email := `From: alice@example.com
To: bob@example.com
Subject: Multipart Preview
MIME-Version: 1.0
Content-Type: multipart/alternative; boundary="boundary1"

--boundary1
Content-Type: text/plain; charset=utf-8

This is the plain text preview content.
--boundary1
Content-Type: text/html; charset=utf-8

<html><body>HTML content</body></html>
--boundary1--
`
	uploader := &sequentialUploader{prefix: "blob"}

	parsed, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"email-blob-123",
		"account-456",
		uploader,
	)
	if err != nil {
		t.Fatalf("ParseRFC5322Stream error = %v", err)
	}

	// Preview should be generated from the text/plain part in multipart
	if !strings.Contains(parsed.Preview, "This is the plain text preview content") {
		t.Errorf("Preview = %q, want to contain %q", parsed.Preview, "This is the plain text preview content")
	}
}

func TestParseRFC5322Stream_SizeIsAccurate(t *testing.T) {
	tests := []struct {
		name  string
		email string
	}{
		{
			name: "simple_plain_text",
			email: "From: alice@example.com\r\n" +
				"Subject: Test\r\n" +
				"\r\n" +
				"Body content.",
		},
		{
			name: "with_crlf_body",
			email: "From: alice@example.com\r\n" +
				"Subject: Test\r\n" +
				"\r\n" +
				"Line 1\r\nLine 2\r\nLine 3\r\n",
		},
		{
			name: "multipart",
			email: "From: alice@example.com\r\n" +
				"Content-Type: multipart/mixed; boundary=\"b\"\r\n" +
				"\r\n" +
				"--b\r\nContent-Type: text/plain\r\n\r\nHello\r\n--b--\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uploader := &sequentialUploader{prefix: "blob"}
			parsed, err := ParseRFC5322Stream(
				context.Background(),
				strings.NewReader(tt.email),
				"blob-123",
				"acct-1",
				uploader,
			)
			if err != nil {
				t.Fatalf("ParseRFC5322Stream error = %v", err)
			}
			if parsed.Size != int64(len(tt.email)) {
				t.Errorf("Size = %d, want %d", parsed.Size, len(tt.email))
			}
		})
	}
}

func TestParseRFC5322Stream_TooManyParts(t *testing.T) {
	// Build a multipart message with 101 leaf parts (+ 1 root container = 102 total).
	// MaxParts is 100, so this should be rejected.
	boundary := "PARTBOMB"
	var body strings.Builder
	for i := 0; i < 101; i++ {
		body.WriteString("--" + boundary + "\r\n")
		body.WriteString("Content-Type: text/plain\r\n\r\n")
		body.WriteString(fmt.Sprintf("part %d\r\n", i))
	}
	body.WriteString("--" + boundary + "--\r\n")

	email := "From: alice@example.com\r\n" +
		"Content-Type: multipart/mixed; boundary=\"" + boundary + "\"\r\n" +
		"\r\n" +
		body.String()

	uploader := &sequentialUploader{prefix: "blob"}
	_, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"blob-123",
		"acct-1",
		uploader,
	)

	if err == nil {
		t.Fatal("expected error for too many parts, got nil")
	}
	if !errors.Is(err, ErrTooManyParts) {
		t.Errorf("error = %v, want ErrTooManyParts", err)
	}
}

func TestParseRFC5322Stream_NestingTooDeep(t *testing.T) {
	// Build 11 levels of nested multipart. MaxMultipartDepth is 10.
	// Level 1 is the root multipart, levels 2-11 are nested.
	levels := 11
	var sb strings.Builder

	// Build nested multipart from outer to inner
	for i := 1; i <= levels; i++ {
		boundary := fmt.Sprintf("level%d", i)
		sb.WriteString("--" + boundary + "\r\n")
		if i < levels {
			nextBoundary := fmt.Sprintf("level%d", i+1)
			sb.WriteString("Content-Type: multipart/mixed; boundary=\"" + nextBoundary + "\"\r\n\r\n")
		} else {
			// Innermost leaf part
			sb.WriteString("Content-Type: text/plain\r\n\r\n")
			sb.WriteString("deep leaf\r\n")
		}
	}
	// Close all boundaries from inner to outer
	for i := levels; i >= 1; i-- {
		boundary := fmt.Sprintf("level%d", i)
		sb.WriteString("--" + boundary + "--\r\n")
	}

	email := "From: alice@example.com\r\n" +
		"Content-Type: multipart/mixed; boundary=\"level1\"\r\n" +
		"\r\n" +
		sb.String()

	uploader := &sequentialUploader{prefix: "blob"}
	_, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"blob-123",
		"acct-1",
		uploader,
	)

	if err == nil {
		t.Fatal("expected error for nesting too deep, got nil")
	}
	if !errors.Is(err, ErrNestingTooDeep) {
		t.Errorf("error = %v, want ErrNestingTooDeep", err)
	}
}

func TestParseRFC5322Stream_AtPartLimit(t *testing.T) {
	// Build a multipart message with exactly 99 leaf parts.
	// Together with the root container part, total = 100 = MaxParts. Should succeed.
	boundary := "LIMIT"
	var body strings.Builder
	for i := 0; i < 99; i++ {
		body.WriteString("--" + boundary + "\r\n")
		body.WriteString("Content-Type: text/plain\r\n\r\n")
		body.WriteString(fmt.Sprintf("part %d\r\n", i))
	}
	body.WriteString("--" + boundary + "--\r\n")

	email := "From: alice@example.com\r\n" +
		"Content-Type: multipart/mixed; boundary=\"" + boundary + "\"\r\n" +
		"\r\n" +
		body.String()

	uploader := &sequentialUploader{prefix: "blob"}
	_, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"blob-123",
		"acct-1",
		uploader,
	)

	if err != nil {
		t.Fatalf("expected no error at part limit (100 parts), got %v", err)
	}
}

func TestParseRFC5322Stream_AtDepthLimit(t *testing.T) {
	// Build exactly 10 levels of nested multipart. MaxMultipartDepth is 10. Should succeed.
	levels := 10
	var sb strings.Builder

	for i := 1; i <= levels; i++ {
		boundary := fmt.Sprintf("depth%d", i)
		sb.WriteString("--" + boundary + "\r\n")
		if i < levels {
			nextBoundary := fmt.Sprintf("depth%d", i+1)
			sb.WriteString("Content-Type: multipart/mixed; boundary=\"" + nextBoundary + "\"\r\n\r\n")
		} else {
			sb.WriteString("Content-Type: text/plain\r\n\r\n")
			sb.WriteString("deep leaf\r\n")
		}
	}
	for i := levels; i >= 1; i-- {
		boundary := fmt.Sprintf("depth%d", i)
		sb.WriteString("--" + boundary + "--\r\n")
	}

	email := "From: alice@example.com\r\n" +
		"Content-Type: multipart/mixed; boundary=\"depth1\"\r\n" +
		"\r\n" +
		sb.String()

	uploader := &sequentialUploader{prefix: "blob"}
	_, err := ParseRFC5322Stream(
		context.Background(),
		strings.NewReader(email),
		"blob-123",
		"acct-1",
		uploader,
	)

	if err != nil {
		t.Fatalf("expected no error at depth limit (10 levels), got %v", err)
	}
}
