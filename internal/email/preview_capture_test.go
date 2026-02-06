package email

import (
	"strings"
	"testing"
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
	if len(got) > 260 { // some tolerance for "..." suffix
		t.Errorf("Preview() length = %d, should be <= ~260", len(got))
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
	// Should truncate at a word boundary near 20 chars and add "..."
	if len(got) > 25 {
		t.Errorf("Preview() length = %d, should be <= ~25", len(got))
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
