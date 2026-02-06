// Package htmlstrip provides a streaming HTML-to-text converter.
package htmlstrip

import (
	"bytes"
	"io"
	"strings"

	"golang.org/x/net/html"
)

// skipElements are elements whose text content should be discarded.
var skipElements = map[string]bool{
	"script":   true,
	"style":    true,
	"noscript": true,
}

// blockElements are elements that should have whitespace separation.
var blockElements = map[string]bool{
	"p": true, "div": true, "h1": true, "h2": true, "h3": true,
	"h4": true, "h5": true, "h6": true, "li": true, "blockquote": true,
	"pre": true, "table": true, "tr": true, "td": true, "th": true,
	"section": true, "article": true, "header": true, "footer": true,
	"nav": true, "main": true, "aside": true, "figure": true,
	"figcaption": true, "details": true, "summary": true,
}

// reader wraps an HTML stream and emits plain text.
type reader struct {
	tokenizer *html.Tokenizer
	buf       bytes.Buffer
	done      bool
	skipDepth int // depth counter for elements being skipped
	lastSpace bool
	hasOutput bool
}

// NewReader wraps an HTML io.Reader and returns a reader that emits plain text.
// It uses the html.Tokenizer to process input incrementally.
func NewReader(r io.Reader) io.Reader {
	return &reader{
		tokenizer: html.NewTokenizer(r),
	}
}

func (r *reader) Read(p []byte) (int, error) {
	for r.buf.Len() < len(p) && !r.done {
		if !r.next() {
			break
		}
	}
	if r.buf.Len() == 0 && r.done {
		return 0, io.EOF
	}
	return r.buf.Read(p)
}

func (r *reader) next() bool {
	tt := r.tokenizer.Next()
	switch tt {
	case html.ErrorToken:
		r.done = true
		// Trim trailing space
		trimmed := strings.TrimRight(r.buf.String(), " ")
		r.buf.Reset()
		r.buf.WriteString(trimmed)
		return false

	case html.StartTagToken:
		tn, hasAttr := r.tokenizer.TagName()
		tagName := string(tn)

		if skipElements[tagName] {
			r.skipDepth++
			return true
		}

		if tagName == "br" {
			r.writeSpace()
		}

		if blockElements[tagName] {
			r.writeSpace()
		}

		// Extract alt attribute from img tags
		if tagName == "img" && hasAttr {
			r.extractAlt()
		}

		return true

	case html.EndTagToken:
		tn, _ := r.tokenizer.TagName()

		if skipElements[string(tn)] && r.skipDepth > 0 {
			r.skipDepth--
		}

		if blockElements[string(tn)] {
			r.writeSpace()
		}
		return true

	case html.SelfClosingTagToken:
		tn, hasAttr := r.tokenizer.TagName()
		tagName := string(tn)

		if tagName == "br" {
			r.writeSpace()
		}
		if tagName == "img" && hasAttr {
			r.extractAlt()
		}
		return true

	case html.TextToken:
		if r.skipDepth > 0 {
			return true
		}
		text := r.tokenizer.Text()
		r.writeText(text)
		return true
	}
	return true
}

func (r *reader) extractAlt() {
	for {
		key, val, more := r.tokenizer.TagAttr()
		if string(key) == "alt" && len(val) > 0 {
			r.writeText(val)
		}
		if !more {
			break
		}
	}
}

func (r *reader) writeSpace() {
	if r.hasOutput && !r.lastSpace {
		r.buf.WriteByte(' ')
		r.lastSpace = true
	}
}

func (r *reader) writeText(text []byte) {
	for _, b := range text {
		isSpace := b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\f'
		if isSpace {
			r.writeSpace()
		} else {
			r.buf.WriteByte(b)
			r.lastSpace = false
			r.hasOutput = true
		}
	}
}
