// Package snippet provides search term highlighting for JMAP SearchSnippet/get.
package snippet

import (
	"html"
	"sort"
	"strings"
	"unicode/utf8"
)

// maxPreviewBytes is the maximum byte length for preview snippets per RFC 8621.
const maxPreviewBytes = 255

// match represents a byte range [start, end) in the original text.
type match struct {
	start, end int
}

// Highlight performs case-insensitive search for terms in text,
// HTML-escapes the text, and wraps matches in <mark> tags.
// Returns nil if no matches are found.
func Highlight(text string, searchTerms []string) *string {
	if text == "" || len(searchTerms) == 0 {
		return nil
	}

	matches := findMatches(text, searchTerms)
	if len(matches) == 0 {
		return nil
	}

	result := buildHighlighted(text, matches)
	return &result
}

// HighlightPreview is like Highlight but truncates the result to 255 bytes.
// Truncation respects UTF-8 rune boundaries and does not split <mark> tags.
func HighlightPreview(text string, searchTerms []string) *string {
	result := Highlight(text, searchTerms)
	if result == nil {
		return nil
	}

	if len([]byte(*result)) <= maxPreviewBytes {
		return result
	}

	truncated := truncateHighlighted(*result, maxPreviewBytes)
	return &truncated
}

// findMatches finds all case-insensitive matches of searchTerms in text,
// returning merged, non-overlapping byte ranges sorted by position.
func findMatches(text string, searchTerms []string) []match {
	lower := strings.ToLower(text)
	var matches []match

	for _, term := range searchTerms {
		if term == "" {
			continue
		}
		lowerTerm := strings.ToLower(term)
		offset := 0
		for {
			idx := strings.Index(lower[offset:], lowerTerm)
			if idx < 0 {
				break
			}
			start := offset + idx
			end := start + len(lowerTerm)
			matches = append(matches, match{start, end})
			offset = start + 1
		}
	}

	if len(matches) == 0 {
		return nil
	}

	// Sort by start position, then by end (longer match first)
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].start != matches[j].start {
			return matches[i].start < matches[j].start
		}
		return matches[i].end > matches[j].end
	})

	// Merge overlapping/adjacent ranges
	merged := []match{matches[0]}
	for _, m := range matches[1:] {
		last := &merged[len(merged)-1]
		if m.start <= last.end {
			if m.end > last.end {
				last.end = m.end
			}
		} else {
			merged = append(merged, m)
		}
	}

	return merged
}

// buildHighlighted constructs the highlighted string from original text and match ranges.
// Non-matching segments are HTML-escaped; matching segments are HTML-escaped then wrapped in <mark>.
func buildHighlighted(text string, matches []match) string {
	var b strings.Builder
	pos := 0

	for _, m := range matches {
		// Write non-matching segment before this match
		if pos < m.start {
			b.WriteString(html.EscapeString(text[pos:m.start]))
		}
		// Write matching segment wrapped in <mark>
		b.WriteString("<mark>")
		b.WriteString(html.EscapeString(text[m.start:m.end]))
		b.WriteString("</mark>")
		pos = m.end
	}

	// Write any remaining text after last match
	if pos < len(text) {
		b.WriteString(html.EscapeString(text[pos:]))
	}

	return b.String()
}

// truncateHighlighted truncates a highlighted string to maxBytes,
// respecting UTF-8 rune boundaries and not splitting HTML tags.
// Appends "..." to the truncated result.
func truncateHighlighted(s string, maxBytes int) string {
	ellipsis := "..."
	targetBytes := maxBytes - len(ellipsis) // reserve space for "..."

	if targetBytes <= 0 {
		return ellipsis
	}

	// Truncate at rune boundary
	byteCount := 0
	for i := 0; i < len(s); {
		_, size := utf8.DecodeRuneInString(s[i:])
		if byteCount+size > targetBytes {
			break
		}
		byteCount += size
		i += size
	}

	truncated := s[:byteCount]

	// Don't split an HTML tag — if there's an unmatched '<', back up
	lastOpen := strings.LastIndex(truncated, "<")
	if lastOpen >= 0 {
		lastClose := strings.LastIndex(truncated, ">")
		if lastClose < lastOpen {
			// We have an incomplete tag, back up to before it
			truncated = truncated[:lastOpen]
		}
	}

	return truncated + ellipsis
}
