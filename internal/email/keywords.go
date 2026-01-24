package email

import (
	"errors"
	"strings"
)

// Keyword validation errors.
var (
	ErrKeywordEmpty         = errors.New("keyword must not be empty")
	ErrKeywordTooLong       = errors.New("keyword must not exceed 255 characters")
	ErrKeywordInvalidChar   = errors.New("keyword contains invalid character")
	ErrKeywordForbiddenChar = errors.New("keyword contains forbidden character")
)

// Forbidden characters in keywords per RFC 8621.
// These are: ( ) { ] % * " \
var forbiddenChars = map[rune]bool{
	'(':  true,
	')':  true,
	'{':  true,
	']':  true,
	'%':  true,
	'*':  true,
	'"':  true,
	'\\': true,
}

// ValidateKeyword validates a keyword per RFC 8621 rules.
// Keywords must be 1-255 characters, ASCII 0x21-0x7E only,
// and must not contain ( ) { ] % * " \.
func ValidateKeyword(keyword string) error {
	if keyword == "" {
		return ErrKeywordEmpty
	}

	if len(keyword) > 255 {
		return ErrKeywordTooLong
	}

	for _, r := range keyword {
		// Must be ASCII 0x21-0x7E (printable ASCII excluding space)
		if r < 0x21 || r > 0x7E {
			return ErrKeywordInvalidChar
		}

		// Check forbidden characters
		if forbiddenChars[r] {
			return ErrKeywordForbiddenChar
		}
	}

	return nil
}

// NormalizeKeyword converts a keyword to lowercase.
func NormalizeKeyword(keyword string) string {
	return strings.ToLower(keyword)
}
