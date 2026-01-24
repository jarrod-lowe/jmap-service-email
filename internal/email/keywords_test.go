package email

import (
	"strings"
	"testing"
)

func TestValidateKeyword_ValidKeywords(t *testing.T) {
	validKeywords := []string{
		"$seen",
		"$flagged",
		"$draft",
		"$answered",
		"$forwarded",
		"custom",
		"a",
		strings.Repeat("a", 255), // max length
	}

	for _, kw := range validKeywords {
		t.Run(kw, func(t *testing.T) {
			if err := ValidateKeyword(kw); err != nil {
				t.Errorf("ValidateKeyword(%q) = %v, want nil", kw, err)
			}
		})
	}
}

func TestValidateKeyword_EmptyKeyword(t *testing.T) {
	err := ValidateKeyword("")
	if err == nil {
		t.Error("ValidateKeyword(\"\") = nil, want error")
	}
}

func TestValidateKeyword_TooLong(t *testing.T) {
	longKeyword := strings.Repeat("a", 256)
	err := ValidateKeyword(longKeyword)
	if err == nil {
		t.Errorf("ValidateKeyword(%d chars) = nil, want error", len(longKeyword))
	}
}

func TestValidateKeyword_ForbiddenCharacters(t *testing.T) {
	forbiddenChars := []string{
		"test(paren",
		"test)paren",
		"test{brace",
		"test]bracket",
		"test%percent",
		"test*star",
		"test\"quote",
		"test\\backslash",
	}

	for _, kw := range forbiddenChars {
		t.Run(kw, func(t *testing.T) {
			err := ValidateKeyword(kw)
			if err == nil {
				t.Errorf("ValidateKeyword(%q) = nil, want error for forbidden char", kw)
			}
		})
	}
}

func TestValidateKeyword_NonASCII(t *testing.T) {
	nonASCII := []string{
		"tëst",        // non-ASCII letter
		"test\x00",    // null byte
		"test\x1f",    // control char below 0x21
		"test\x7f",    // DEL character (0x7f)
		"日本語",         // unicode
		"test\t",      // tab (0x09)
		"test ",       // space (0x20)
		" test",       // leading space
	}

	for _, kw := range nonASCII {
		t.Run(kw, func(t *testing.T) {
			err := ValidateKeyword(kw)
			if err == nil {
				t.Errorf("ValidateKeyword(%q) = nil, want error for non-ASCII/control char", kw)
			}
		})
	}
}

func TestNormalizeKeyword(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"$Seen", "$seen"},
		{"FLAGGED", "flagged"},
		{"Custom", "custom"},
		{"$seen", "$seen"},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result := NormalizeKeyword(tc.input)
			if result != tc.expected {
				t.Errorf("NormalizeKeyword(%q) = %q, want %q", tc.input, result, tc.expected)
			}
		})
	}
}
