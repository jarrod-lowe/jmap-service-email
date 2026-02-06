package email

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestPreviewCapture_SmallInput(t *testing.T) {
	pc := NewPreviewCapture(256)
	input := "Hello, this is a short email."
	n, err := pc.Write([]byte(input))
	if err != nil {
		t.Fatalf("Write error = %v", err)
	}
	if n != len(input) {
		t.Errorf("Write returned %d, want %d", n, len(input))
	}
	got := pc.Preview()
	if got != input {
		t.Errorf("Preview() = %q, want %q", got, input)
	}
}

func TestPreviewCapture_TruncatesLargeInput(t *testing.T) {
	pc := NewPreviewCapture(256)
	// Create input longer than 256 bytes
	input := strings.Repeat("word ", 100) // 500 bytes
	_, _ = pc.Write([]byte(input))

	got := pc.Preview()
	runeLen := utf8.RuneCountInString(got)
	if runeLen > 260 { // some tolerance for "…" suffix
		t.Errorf("Preview() rune length = %d, should be <= ~260", runeLen)
	}
	if !strings.HasSuffix(got, "…") {
		t.Errorf("Preview() = %q, should end with '…'", got)
	}
}

func TestPreviewCapture_CleansNewlines(t *testing.T) {
	pc := NewPreviewCapture(256)
	input := "Line one\r\nLine two\nLine three"
	_, _ = pc.Write([]byte(input))

	got := pc.Preview()
	want := "Line one Line two Line three"
	if got != want {
		t.Errorf("Preview() = %q, want %q", got, want)
	}
}

func TestPreviewCapture_CollapsesDoubleSpaces(t *testing.T) {
	pc := NewPreviewCapture(256)
	input := "Hello  World   Test"
	_, _ = pc.Write([]byte(input))

	got := pc.Preview()
	want := "Hello World Test"
	if got != want {
		t.Errorf("Preview() = %q, want %q", got, want)
	}
}

func TestPreviewCapture_TruncatesAtWordBoundary(t *testing.T) {
	pc := NewPreviewCapture(20)
	input := "Hello World this is a longer text"
	_, _ = pc.Write([]byte(input))

	got := pc.Preview()
	// Should truncate at a word boundary near 20 characters (runes) and add "…"
	runeLen := utf8.RuneCountInString(got)
	if runeLen > 25 {
		t.Errorf("Preview() rune length = %d, should be <= ~25", runeLen)
	}
	if !strings.HasSuffix(got, "…") {
		t.Errorf("Preview() = %q, should end with '…'", got)
	}
	// Should not cut in the middle of a word
	withoutSuffix := strings.TrimSuffix(got, "…")
	if strings.Contains(withoutSuffix, "thi") && !strings.Contains(withoutSuffix, "this") {
		t.Errorf("Preview() = %q, cuts in middle of word", got)
	}
}

func TestPreviewCapture_EmptyInput(t *testing.T) {
	pc := NewPreviewCapture(256)
	got := pc.Preview()
	if got != "" {
		t.Errorf("Preview() = %q, want empty string", got)
	}
}

func TestPreviewCapture_MultipleWrites(t *testing.T) {
	pc := NewPreviewCapture(256)
	_, _ = pc.Write([]byte("Hello "))
	_, _ = pc.Write([]byte("World "))
	_, _ = pc.Write([]byte("Test"))

	got := pc.Preview()
	want := "Hello World Test"
	if got != want {
		t.Errorf("Preview() = %q, want %q", got, want)
	}
}

func TestPreviewCapture_WriteReturnsFullLength(t *testing.T) {
	pc := NewPreviewCapture(10) // small buffer
	input := "This is a much longer string that exceeds the buffer"
	n, err := pc.Write([]byte(input))
	if err != nil {
		t.Fatalf("Write error = %v", err)
	}
	// Must return len(input) per io.Writer contract, even when discarding
	if n != len(input) {
		t.Errorf("Write returned %d, want %d", n, len(input))
	}
}

func TestPreviewCapture_FullReturnsTrueAfterMaxBytes(t *testing.T) {
	pc := NewPreviewCapture(10)
	_, _ = pc.Write([]byte("12345678901234567890")) // 20 bytes, well over 10

	if !pc.Full() {
		t.Error("Full() should return true after exceeding max bytes")
	}
}

func TestPreviewCapture_FullReturnsFalseBeforeMaxBytes(t *testing.T) {
	pc := NewPreviewCapture(256)
	_, _ = pc.Write([]byte("short"))

	if pc.Full() {
		t.Error("Full() should return false before reaching max bytes")
	}
}

func TestPreviewCapture_MultiByte(t *testing.T) {
	pc := NewPreviewCapture(256)
	// Each CJK character is 3 bytes in UTF-8, so 256 chars = 768 bytes.
	// With byte-based limiting, we'd only get ~85 characters instead of 256.
	input := strings.Repeat("日", 300) // 300 CJK chars = 900 bytes
	_, _ = pc.Write([]byte(input))

	got := pc.Preview()

	// Must be valid UTF-8
	if !utf8.ValidString(got) {
		t.Errorf("Preview() is not valid UTF-8")
	}

	runeLen := utf8.RuneCountInString(got)

	// Preview must be at most 256 characters (runes) + "…" suffix (1 rune) = 257 runes max
	// The "…" is appended because input was truncated
	withoutSuffix := strings.TrimSuffix(got, "…")
	suffixRuneLen := utf8.RuneCountInString(withoutSuffix)
	if suffixRuneLen > 256 {
		t.Errorf("Preview() content before suffix has %d runes, want <= 256", suffixRuneLen)
	}

	// With byte-based limiting, we'd get far fewer than 256 characters.
	// Ensure we actually get close to 256 characters of content.
	if suffixRuneLen < 200 {
		t.Errorf("Preview() content has only %d runes, want close to 256 (byte-based bug?)", suffixRuneLen)
	}

	if !strings.HasSuffix(got, "…") {
		t.Errorf("Preview() = length %d runes, should end with '…' for truncated input", runeLen)
	}
}
