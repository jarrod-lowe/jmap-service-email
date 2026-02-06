package email

import (
	"strings"
	"unicode/utf8"
)

// PreviewCapture captures text content for email preview generation.
// It limits output to maxChars characters (runes) per RFC 8621 Section 4.1.4.
// It implements io.Writer so it can be used with io.TeeReader or io.MultiWriter.
type PreviewCapture struct {
	maxChars int
	buf      []byte
	full     bool
}

// NewPreviewCapture creates a PreviewCapture that captures up to maxChars characters (runes).
func NewPreviewCapture(maxChars int) *PreviewCapture {
	return &PreviewCapture{maxChars: maxChars}
}

// Write implements io.Writer. It always returns len(p), nil to satisfy the
// io.Writer contract, even when discarding data after the buffer is full.
func (pc *PreviewCapture) Write(p []byte) (int, error) {
	if !pc.full {
		pc.buf = append(pc.buf, p...)
		if utf8.RuneCount(pc.buf) >= pc.maxChars {
			pc.full = true
		}
	}
	return len(p), nil
}

// Full returns true if the capture buffer has reached its maximum size.
func (pc *PreviewCapture) Full() bool {
	return pc.full
}

// Preview returns the captured text, cleaned up for use as an email preview:
// - \r\n and \n replaced with spaces
// - multiple spaces collapsed
// - truncated at word boundary with "…" suffix if over maxChars characters
func (pc *PreviewCapture) Preview() string {
	text := string(pc.buf)

	text = strings.TrimSpace(text)
	text = strings.ReplaceAll(text, "\r\n", " ")
	text = strings.ReplaceAll(text, "\n", " ")

	for strings.Contains(text, "  ") {
		text = strings.ReplaceAll(text, "  ", " ")
	}

	runeLen := utf8.RuneCountInString(text)
	if pc.full || runeLen > pc.maxChars {
		if runeLen > pc.maxChars {
			runes := []rune(text)
			text = string(runes[:pc.maxChars])
		}
		if lastSpace := strings.LastIndex(text, " "); lastSpace > len(text)-50 {
			text = text[:lastSpace]
		}
		text += "…"
	}

	return text
}
