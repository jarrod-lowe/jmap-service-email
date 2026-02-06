package email

import "strings"

// PreviewCapture captures the first ~maxBytes of text content for email preview generation.
// It implements io.Writer so it can be used with io.TeeReader or io.MultiWriter.
type PreviewCapture struct {
	maxBytes int
	buf      []byte
	full     bool
}

// NewPreviewCapture creates a PreviewCapture that captures up to maxBytes of input.
func NewPreviewCapture(maxBytes int) *PreviewCapture {
	return &PreviewCapture{maxBytes: maxBytes}
}

// Write implements io.Writer. It always returns len(p), nil to satisfy the
// io.Writer contract, even when discarding data after the buffer is full.
func (pc *PreviewCapture) Write(p []byte) (int, error) {
	if remaining := pc.maxBytes - len(pc.buf); remaining > 0 {
		toCapture := p
		if len(toCapture) > remaining {
			toCapture = toCapture[:remaining]
			pc.full = true
		}
		pc.buf = append(pc.buf, toCapture...)
	} else {
		pc.full = true
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
// - truncated at word boundary with "…" suffix if over maxBytes
func (pc *PreviewCapture) Preview() string {
	text := string(pc.buf)

	text = strings.TrimSpace(text)
	text = strings.ReplaceAll(text, "\r\n", " ")
	text = strings.ReplaceAll(text, "\n", " ")

	for strings.Contains(text, "  ") {
		text = strings.ReplaceAll(text, "  ", " ")
	}

	if pc.full || len(text) > pc.maxBytes {
		if len(text) > pc.maxBytes {
			text = text[:pc.maxBytes]
		}
		if lastSpace := strings.LastIndex(text, " "); lastSpace > len(text)-50 {
			text = text[:lastSpace]
		}
		text += "…"
	}

	return text
}
