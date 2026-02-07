package snippet

import (
	"testing"
)

func TestHighlight_SingleMatch(t *testing.T) {
	result := Highlight("Hello World", []string{"World"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	want := "Hello <mark>World</mark>"
	if *result != want {
		t.Errorf("got %q, want %q", *result, want)
	}
}

func TestHighlight_NoMatch(t *testing.T) {
	result := Highlight("Hello World", []string{"foo"})
	if result != nil {
		t.Errorf("expected nil, got %q", *result)
	}
}

func TestHighlight_EmptyTerms(t *testing.T) {
	result := Highlight("Hello World", nil)
	if result != nil {
		t.Errorf("expected nil for empty terms, got %q", *result)
	}
}

func TestHighlight_EmptyText(t *testing.T) {
	result := Highlight("", []string{"foo"})
	if result != nil {
		t.Errorf("expected nil for empty text, got %q", *result)
	}
}

func TestHighlight_CaseInsensitive(t *testing.T) {
	result := Highlight("Hello WORLD", []string{"world"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	want := "Hello <mark>WORLD</mark>"
	if *result != want {
		t.Errorf("got %q, want %q", *result, want)
	}
}

func TestHighlight_HTMLEscaping(t *testing.T) {
	result := Highlight("a < b & c > d", []string{"b"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	want := "a &lt; <mark>b</mark> &amp; c &gt; d"
	if *result != want {
		t.Errorf("got %q, want %q", *result, want)
	}
}

func TestHighlight_HTMLEscapingInMatch(t *testing.T) {
	result := Highlight("a<b", []string{"a<b"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	want := "<mark>a&lt;b</mark>"
	if *result != want {
		t.Errorf("got %q, want %q", *result, want)
	}
}

func TestHighlight_MultipleMatches(t *testing.T) {
	result := Highlight("foo bar foo", []string{"foo"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	want := "<mark>foo</mark> bar <mark>foo</mark>"
	if *result != want {
		t.Errorf("got %q, want %q", *result, want)
	}
}

func TestHighlight_MultipleTerms(t *testing.T) {
	result := Highlight("hello beautiful world", []string{"hello", "world"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	want := "<mark>hello</mark> beautiful <mark>world</mark>"
	if *result != want {
		t.Errorf("got %q, want %q", *result, want)
	}
}

func TestHighlight_OverlappingTerms(t *testing.T) {
	// "ab" and "bc" overlap in "abc" — should merge into one mark
	result := Highlight("xabcy", []string{"ab", "bc"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	want := "x<mark>abc</mark>y"
	if *result != want {
		t.Errorf("got %q, want %q", *result, want)
	}
}

func TestHighlight_Unicode(t *testing.T) {
	result := Highlight("Héllo Wörld", []string{"Wörld"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	want := "Héllo <mark>Wörld</mark>"
	if *result != want {
		t.Errorf("got %q, want %q", *result, want)
	}
}

func TestHighlight_AdjacentTerms(t *testing.T) {
	result := Highlight("foobar", []string{"foo", "bar"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	want := "<mark>foobar</mark>"
	if *result != want {
		t.Errorf("got %q, want %q", *result, want)
	}
}

func TestHighlightPreview_ShortText(t *testing.T) {
	result := HighlightPreview("Hello World", []string{"World"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	want := "Hello <mark>World</mark>"
	if *result != want {
		t.Errorf("got %q, want %q", *result, want)
	}
}

func TestHighlightPreview_NoMatch(t *testing.T) {
	result := HighlightPreview("Hello World", []string{"foo"})
	if result != nil {
		t.Errorf("expected nil, got %q", *result)
	}
}

func TestHighlightPreview_TruncatesAt255Bytes(t *testing.T) {
	// Create text that will exceed 255 bytes when highlighted
	// "match" appears at the start, then lots of filler
	text := "match " + string(make([]byte, 300))
	// Replace null bytes with 'x'
	textBytes := []byte(text)
	for i := 6; i < len(textBytes); i++ {
		textBytes[i] = 'x'
	}
	text = string(textBytes)

	result := HighlightPreview(text, []string{"match"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if len([]byte(*result)) > 255 {
		t.Errorf("result is %d bytes, want <= 255", len([]byte(*result)))
	}
	// Should end with ...
	if (*result)[len(*result)-3:] != "..." {
		t.Errorf("expected result to end with ..., got %q", (*result)[len(*result)-10:])
	}
}

func TestHighlightPreview_TruncationPreservesRuneBoundary(t *testing.T) {
	// Use multi-byte unicode characters to test rune boundary handling
	// Each é is 2 bytes in UTF-8
	text := "match " + repeatString("é", 200)

	result := HighlightPreview(text, []string{"match"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	resultBytes := []byte(*result)
	if len(resultBytes) > 255 {
		t.Errorf("result is %d bytes, want <= 255", len(resultBytes))
	}
	// Verify it's valid UTF-8 by checking we can range over it without issues
	for _, r := range *result {
		if r == 0xFFFD {
			t.Error("result contains invalid UTF-8 replacement character")
		}
	}
}

func TestHighlightPreview_TruncationDoesNotSplitMarkTag(t *testing.T) {
	// Put a match near byte 255 so the <mark> tag might get split
	filler := repeatString("x", 240)
	text := filler + "match"

	result := HighlightPreview(text, []string{"match"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	resultBytes := []byte(*result)
	if len(resultBytes) > 255 {
		t.Errorf("result is %d bytes, want <= 255", len(resultBytes))
	}
	// Should not contain incomplete tags
	for i := len(*result) - 1; i >= 0; i-- {
		if (*result)[i] == '<' {
			// Found an opening <, check it's complete
			rest := (*result)[i:]
			if rest != "..." && !startsWith(rest, "<mark>") && !startsWith(rest, "</mark>") {
				t.Errorf("found incomplete tag near end: %q", rest)
			}
			break
		}
	}
}

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func repeatString(s string, n int) string {
	result := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		result = append(result, s...)
	}
	return string(result)
}
