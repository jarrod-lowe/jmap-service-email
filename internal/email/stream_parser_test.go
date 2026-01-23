package email

import (
	"bytes"
	"context"
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
	uploader := &mockUploader{blobID: "sub-blob", size: 50}

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

	// First part is text/plain with 7bit - should have byte-range blobId
	plainPart := parsed.BodyStructure.SubParts[0]
	if plainPart.Type != "text/plain" {
		t.Errorf("SubParts[0].Type = %q, want %q", plainPart.Type, "text/plain")
	}
	if !strings.HasPrefix(plainPart.BlobID, "email-blob-123,") {
		t.Errorf("SubParts[0].BlobID = %q, should be byte-range format", plainPart.BlobID)
	}

	// Second part is text/html with 7bit - should have byte-range blobId
	htmlPart := parsed.BodyStructure.SubParts[1]
	if htmlPart.Type != "text/html" {
		t.Errorf("SubParts[1].Type = %q, want %q", htmlPart.Type, "text/html")
	}
	if !strings.HasPrefix(htmlPart.BlobID, "email-blob-123,") {
		t.Errorf("SubParts[1].BlobID = %q, should be byte-range format", htmlPart.BlobID)
	}

	// No uploads for 7bit parts
	if len(uploader.uploads) != 0 {
		t.Errorf("uploads = %d, want 0", len(uploader.uploads))
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
	uploader := &mockUploader{blobID: "pdf-blob", size: 8}

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
	if pdfPart.BlobID != "pdf-blob" {
		t.Errorf("SubParts[1].BlobID = %q, want %q", pdfPart.BlobID, "pdf-blob")
	}

	// Only the base64 PDF should be uploaded
	if len(uploader.uploads) != 1 {
		t.Errorf("uploads = %d, want 1", len(uploader.uploads))
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
