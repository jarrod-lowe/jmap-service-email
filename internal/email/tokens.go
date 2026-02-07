package email

import (
	"strings"

	"golang.org/x/text/unicode/norm"
)

// TokenizeAddress produces search tokens from an EmailAddress.
// Tokens are NFC-normalized, lowercased, and deduplicated.
// For each address it produces:
//   - Each word in the display name (split on whitespace)
//   - The full email address
//   - The local part (before @)
//   - The domain (after @)
func TokenizeAddress(addr EmailAddress) []string {
	seen := make(map[string]bool)
	var tokens []string

	add := func(s string) {
		s = normalizeToken(s)
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		tokens = append(tokens, s)
	}

	// Display name words
	if addr.Name != "" {
		for _, word := range strings.Fields(addr.Name) {
			add(word)
		}
	}

	// Email address parts
	if addr.Email != "" {
		add(addr.Email)
		if at := strings.LastIndex(addr.Email, "@"); at > 0 {
			add(addr.Email[:at])  // local part
			add(addr.Email[at+1:]) // domain
		}
	}

	return tokens
}

// TokenizeAddresses produces deduplicated search tokens from multiple addresses.
func TokenizeAddresses(addrs []EmailAddress) []string {
	seen := make(map[string]bool)
	var tokens []string

	for _, addr := range addrs {
		for _, tok := range TokenizeAddress(addr) {
			if !seen[tok] {
				seen[tok] = true
				tokens = append(tokens, tok)
			}
		}
	}

	return tokens
}

// NormalizeSearchQuery normalizes a search query string the same way tokens
// are normalized: NFC normalization followed by lowercasing.
func NormalizeSearchQuery(s string) string {
	return normalizeToken(s)
}

// normalizeToken applies NFC normalization and lowercasing to a string.
func normalizeToken(s string) string {
	return strings.ToLower(norm.NFC.String(s))
}
