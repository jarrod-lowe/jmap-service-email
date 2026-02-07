package email

import (
	"sort"
	"testing"
)

func TestTokenizeAddress_FullAddress(t *testing.T) {
	addr := EmailAddress{
		Name:  "John Smith",
		Email: "john.smith@example.com",
	}

	tokens := TokenizeAddress(addr)
	sort.Strings(tokens)

	expected := []string{
		"example.com",
		"john",
		"john.smith",
		"john.smith@example.com",
		"smith",
	}

	if len(tokens) != len(expected) {
		t.Fatalf("got %d tokens %v, want %d tokens %v", len(tokens), tokens, len(expected), expected)
	}
	for i, tok := range tokens {
		if tok != expected[i] {
			t.Errorf("token[%d] = %q, want %q", i, tok, expected[i])
		}
	}
}

func TestTokenizeAddress_EmailOnly(t *testing.T) {
	addr := EmailAddress{
		Email: "alice@example.com",
	}

	tokens := TokenizeAddress(addr)
	sort.Strings(tokens)

	expected := []string{
		"alice",
		"alice@example.com",
		"example.com",
	}

	if len(tokens) != len(expected) {
		t.Fatalf("got %d tokens %v, want %d tokens %v", len(tokens), tokens, len(expected), expected)
	}
	for i, tok := range tokens {
		if tok != expected[i] {
			t.Errorf("token[%d] = %q, want %q", i, tok, expected[i])
		}
	}
}

func TestTokenizeAddress_UnicodeNormalization(t *testing.T) {
	// \u0041\u0301 is A + combining acute accent, NFC form is \u00C1
	addr := EmailAddress{
		Name:  "Sm\u0069\u0302th", // i + combining circumflex → î in NFC
		Email: "test@example.com",
	}

	tokens := TokenizeAddress(addr)

	// Check that the name word is NFC-normalized
	found := false
	for _, tok := range tokens {
		if tok == "sm\u00eeth" { // î (NFC)
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected NFC-normalized token 'smîth', got tokens: %v", tokens)
	}
}

func TestTokenizeAddress_CaseInsensitive(t *testing.T) {
	addr := EmailAddress{
		Name:  "JOHN",
		Email: "John.Smith@Example.COM",
	}

	tokens := TokenizeAddress(addr)

	for _, tok := range tokens {
		for _, r := range tok {
			if r >= 'A' && r <= 'Z' {
				t.Errorf("token %q contains uppercase character", tok)
				break
			}
		}
	}
}

func TestTokenizeAddress_Deduplication(t *testing.T) {
	// Name "alice" and local part "alice" should not produce duplicates
	addr := EmailAddress{
		Name:  "Alice",
		Email: "alice@example.com",
	}

	tokens := TokenizeAddress(addr)

	seen := make(map[string]int)
	for _, tok := range tokens {
		seen[tok]++
		if seen[tok] > 1 {
			t.Errorf("duplicate token: %q", tok)
		}
	}
}

func TestTokenizeAddress_EmptyEmail(t *testing.T) {
	addr := EmailAddress{
		Name: "Just A Name",
	}

	tokens := TokenizeAddress(addr)

	sort.Strings(tokens)
	expected := []string{"a", "just", "name"}

	if len(tokens) != len(expected) {
		t.Fatalf("got %d tokens %v, want %d tokens %v", len(tokens), tokens, len(expected), expected)
	}
	for i, tok := range tokens {
		if tok != expected[i] {
			t.Errorf("token[%d] = %q, want %q", i, tok, expected[i])
		}
	}
}

func TestTokenizeAddress_Empty(t *testing.T) {
	addr := EmailAddress{}
	tokens := TokenizeAddress(addr)
	if len(tokens) != 0 {
		t.Errorf("expected no tokens for empty address, got %v", tokens)
	}
}

func TestTokenizeAddresses_Multiple(t *testing.T) {
	addrs := []EmailAddress{
		{Email: "alice@example.com"},
		{Email: "bob@example.com"},
	}

	tokens := TokenizeAddresses(addrs)

	// Should contain tokens from both addresses
	has := make(map[string]bool)
	for _, tok := range tokens {
		has[tok] = true
	}

	if !has["alice"] {
		t.Error("missing token 'alice'")
	}
	if !has["bob"] {
		t.Error("missing token 'bob'")
	}
	if !has["example.com"] {
		t.Error("missing token 'example.com'")
	}
}

func TestTokenizeAddresses_Deduplicated(t *testing.T) {
	addrs := []EmailAddress{
		{Email: "alice@example.com"},
		{Email: "bob@example.com"},
	}

	tokens := TokenizeAddresses(addrs)

	seen := make(map[string]int)
	for _, tok := range tokens {
		seen[tok]++
		if seen[tok] > 1 {
			t.Errorf("duplicate token: %q", tok)
		}
	}
}

func TestNormalizeSearchQuery(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"John", "john"},
		{"ALICE@EXAMPLE.COM", "alice@example.com"},
		{"Sm\u0069\u0302th", "sm\u00eeth"}, // NFC normalize
	}

	for _, tt := range tests {
		got := NormalizeSearchQuery(tt.input)
		if got != tt.want {
			t.Errorf("NormalizeSearchQuery(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
