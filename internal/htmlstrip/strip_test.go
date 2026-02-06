package htmlstrip

import (
	"io"
	"strings"
	"testing"
)

func readAll(t *testing.T, r io.Reader) string {
	t.Helper()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	return string(b)
}

func TestNewReader_PlainText(t *testing.T) {
	input := "<p>Hello, world!</p>"
	got := readAll(t, NewReader(strings.NewReader(input)))
	if got != "Hello, world!" {
		t.Errorf("got %q, want %q", got, "Hello, world!")
	}
}

func TestNewReader_NestedTags(t *testing.T) {
	input := "<div><p>Hello <b>bold</b> text</p></div>"
	got := readAll(t, NewReader(strings.NewReader(input)))
	if got != "Hello bold text" {
		t.Errorf("got %q, want %q", got, "Hello bold text")
	}
}

func TestNewReader_SkipScript(t *testing.T) {
	input := "<p>Before</p><script>var x = 1;</script><p>After</p>"
	got := readAll(t, NewReader(strings.NewReader(input)))
	if got != "Before After" {
		t.Errorf("got %q, want %q", got, "Before After")
	}
}

func TestNewReader_SkipStyle(t *testing.T) {
	input := "<p>Before</p><style>.foo { color: red; }</style><p>After</p>"
	got := readAll(t, NewReader(strings.NewReader(input)))
	if got != "Before After" {
		t.Errorf("got %q, want %q", got, "Before After")
	}
}

func TestNewReader_ImgAlt(t *testing.T) {
	input := `<p>See <img alt="a cat" src="cat.jpg"> here</p>`
	got := readAll(t, NewReader(strings.NewReader(input)))
	if got != "See a cat here" {
		t.Errorf("got %q, want %q", got, "See a cat here")
	}
}

func TestNewReader_CollapseWhitespace(t *testing.T) {
	input := "<p>  Hello   world  </p>"
	got := readAll(t, NewReader(strings.NewReader(input)))
	if got != "Hello world" {
		t.Errorf("got %q, want %q", got, "Hello world")
	}
}

func TestNewReader_BlockElements(t *testing.T) {
	input := "<h1>Title</h1><p>Paragraph</p>"
	got := readAll(t, NewReader(strings.NewReader(input)))
	if got != "Title Paragraph" {
		t.Errorf("got %q, want %q", got, "Title Paragraph")
	}
}

func TestNewReader_BrTag(t *testing.T) {
	input := "Line one<br>Line two<br/>Line three"
	got := readAll(t, NewReader(strings.NewReader(input)))
	if got != "Line one Line two Line three" {
		t.Errorf("got %q, want %q", got, "Line one Line two Line three")
	}
}

func TestNewReader_EmptyInput(t *testing.T) {
	got := readAll(t, NewReader(strings.NewReader("")))
	if got != "" {
		t.Errorf("got %q, want %q", got, "")
	}
}

func TestNewReader_NoHTML(t *testing.T) {
	input := "Just plain text"
	got := readAll(t, NewReader(strings.NewReader(input)))
	if got != "Just plain text" {
		t.Errorf("got %q, want %q", got, "Just plain text")
	}
}

func TestNewReader_Entities(t *testing.T) {
	input := "<p>Hello &amp; goodbye &lt;world&gt;</p>"
	got := readAll(t, NewReader(strings.NewReader(input)))
	if got != "Hello & goodbye <world>" {
		t.Errorf("got %q, want %q", got, "Hello & goodbye <world>")
	}
}

func TestNewReader_TitleAttribute(t *testing.T) {
	input := `<a title="Click me" href="#">Link</a>`
	got := readAll(t, NewReader(strings.NewReader(input)))
	// Title attribute should not duplicate link text if text is present
	if got != "Link" {
		t.Errorf("got %q, want %q", got, "Link")
	}
}

func TestNewReader_ImgNoAlt(t *testing.T) {
	input := `<p>See <img src="cat.jpg"> here</p>`
	got := readAll(t, NewReader(strings.NewReader(input)))
	if got != "See here" {
		t.Errorf("got %q, want %q", got, "See here")
	}
}
