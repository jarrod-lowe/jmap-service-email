// Package charset provides character set decoding for email body content.
package charset

import (
	"io"
	"strings"
	"testing"
)

func TestDecodeReader_UTF8(t *testing.T) {
	// Valid UTF-8 content with charset="utf-8" should decode correctly
	input := "Hello, 世界! Привет мир!"
	reader := strings.NewReader(input)

	decoded, isEncodingProblem, err := DecodeReader(reader, "utf-8")
	if err != nil {
		t.Fatalf("DecodeReader failed: %v", err)
	}
	if isEncodingProblem {
		t.Error("isEncodingProblem should be false for valid UTF-8")
	}

	result, err := io.ReadAll(decoded)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(result) != input {
		t.Errorf("got %q, want %q", string(result), input)
	}
}

func TestDecodeReader_ISO88591(t *testing.T) {
	// Valid ISO-8859-1 content should decode to UTF-8
	// ISO-8859-1: é = 0xE9, ñ = 0xF1
	input := []byte{0xE9, 0xF1} // "éñ" in ISO-8859-1
	reader := strings.NewReader(string(input))

	decoded, isEncodingProblem, err := DecodeReader(reader, "iso-8859-1")
	if err != nil {
		t.Fatalf("DecodeReader failed: %v", err)
	}
	if isEncodingProblem {
		t.Error("isEncodingProblem should be false for valid ISO-8859-1")
	}

	result, err := io.ReadAll(decoded)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	// UTF-8: é = C3 A9, ñ = C3 B1
	expected := "éñ"
	if string(result) != expected {
		t.Errorf("got %q (%x), want %q (%x)", string(result), result, expected, []byte(expected))
	}
}

func TestDecodeReader_Windows1252(t *testing.T) {
	// Windows-1252 is commonly used - should decode correctly
	// Windows-1252: € = 0x80
	input := []byte{0x80} // "€" in Windows-1252
	reader := strings.NewReader(string(input))

	decoded, isEncodingProblem, err := DecodeReader(reader, "windows-1252")
	if err != nil {
		t.Fatalf("DecodeReader failed: %v", err)
	}
	if isEncodingProblem {
		t.Error("isEncodingProblem should be false for valid Windows-1252")
	}

	result, err := io.ReadAll(decoded)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	expected := "€"
	if string(result) != expected {
		t.Errorf("got %q (%x), want %q (%x)", string(result), result, expected, []byte(expected))
	}
}

func TestDecodeReader_UnknownCharsetFallsBack(t *testing.T) {
	// Unknown charset should fall back gracefully
	// For pure ASCII content, should work with any fallback
	input := "Hello, World!"
	reader := strings.NewReader(input)

	decoded, isEncodingProblem, err := DecodeReader(reader, "unknown-charset-xyz")
	if err != nil {
		t.Fatalf("DecodeReader failed: %v", err)
	}
	if !isEncodingProblem {
		t.Error("isEncodingProblem should be true for unknown charset")
	}

	result, err := io.ReadAll(decoded)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(result) != input {
		t.Errorf("got %q, want %q", string(result), input)
	}
}

func TestDecodeReader_EmptyCharsetDefaultsToUSASCII(t *testing.T) {
	// Empty charset should default to us-ascii
	input := "Hello, World!"
	reader := strings.NewReader(input)

	decoded, isEncodingProblem, err := DecodeReader(reader, "")
	if err != nil {
		t.Fatalf("DecodeReader failed: %v", err)
	}
	if isEncodingProblem {
		t.Error("isEncodingProblem should be false for ASCII content with empty charset")
	}

	result, err := io.ReadAll(decoded)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(result) != input {
		t.Errorf("got %q, want %q", string(result), input)
	}
}

func TestDecodeReader_CaseInsensitive(t *testing.T) {
	// Charset names should be case-insensitive
	input := "Hello"
	testCases := []string{"UTF-8", "utf-8", "Utf-8", "UTF8"}

	for _, charset := range testCases {
		t.Run(charset, func(t *testing.T) {
			reader := strings.NewReader(input)

			decoded, isEncodingProblem, err := DecodeReader(reader, charset)
			if err != nil {
				t.Fatalf("DecodeReader failed for charset %q: %v", charset, err)
			}
			if isEncodingProblem {
				t.Errorf("isEncodingProblem should be false for charset %q", charset)
			}

			result, err := io.ReadAll(decoded)
			if err != nil {
				t.Fatalf("ReadAll failed: %v", err)
			}
			if string(result) != input {
				t.Errorf("got %q, want %q", string(result), input)
			}
		})
	}
}

func TestDecodeReader_InvalidBytesWithFallback(t *testing.T) {
	// When charset is specified but content has invalid bytes for that charset,
	// should fall back and set isEncodingProblem=true
	// Create bytes that are invalid UTF-8 (continuation byte without lead byte)
	input := []byte{0x80, 0x81, 0x82} // Invalid UTF-8 sequence
	reader := strings.NewReader(string(input))

	decoded, isEncodingProblem, err := DecodeReader(reader, "utf-8")
	if err != nil {
		t.Fatalf("DecodeReader failed: %v", err)
	}

	// Should fall back when UTF-8 decoding encounters invalid bytes
	if !isEncodingProblem {
		t.Error("isEncodingProblem should be true for invalid UTF-8 bytes")
	}

	// Should still be readable (via Latin-1 fallback)
	result, err := io.ReadAll(decoded)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	// Latin-1 converts each byte to a UTF-8 character
	// 0x80-0xFF in Latin-1 become multi-byte UTF-8, so output will be longer
	// Just verify it's non-empty and valid UTF-8
	if len(result) == 0 {
		t.Error("result should not be empty")
	}
	// Verify the result contains 3 runes (one per input byte)
	runeCount := 0
	for range string(result) {
		runeCount++
	}
	if runeCount != len(input) {
		t.Errorf("rune count = %d, want %d", runeCount, len(input))
	}
}

func TestDecodeReader_EmptyInput(t *testing.T) {
	// Empty input should return empty output
	reader := strings.NewReader("")

	decoded, isEncodingProblem, err := DecodeReader(reader, "utf-8")
	if err != nil {
		t.Fatalf("DecodeReader failed: %v", err)
	}
	if isEncodingProblem {
		t.Error("isEncodingProblem should be false for empty input")
	}

	result, err := io.ReadAll(decoded)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("got %q, want empty string", string(result))
	}
}

func TestDecodeReader_QuotedPrintableCharset(t *testing.T) {
	// Some charsets have aliases - verify commonly-used ones work
	testCases := []struct {
		charset  string
		input    []byte
		expected string
	}{
		{"latin1", []byte{0xE9}, "é"},     // Latin1 alias for ISO-8859-1
		{"ascii", []byte("Hello"), "Hello"}, // ASCII alias for US-ASCII
	}

	for _, tc := range testCases {
		t.Run(tc.charset, func(t *testing.T) {
			reader := strings.NewReader(string(tc.input))

			decoded, _, err := DecodeReader(reader, tc.charset)
			if err != nil {
				t.Fatalf("DecodeReader failed for charset %q: %v", tc.charset, err)
			}

			result, err := io.ReadAll(decoded)
			if err != nil {
				t.Fatalf("ReadAll failed: %v", err)
			}
			if string(result) != tc.expected {
				t.Errorf("got %q, want %q", string(result), tc.expected)
			}
		})
	}
}
