package headers

import (
	"mime"
	"net/mail"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// EmailAddress represents an email address with optional display name.
type EmailAddress struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// EmailAddressGroup represents a group of email addresses with an optional group name.
type EmailAddressGroup struct {
	Name      *string        `json:"name"`
	Addresses []EmailAddress `json:"addresses"`
}

// ParseRaw returns the header value as-is, replacing invalid UTF-8 with U+FFFD.
func ParseRaw(value string) string {
	if utf8.ValidString(value) {
		return value
	}

	// Replace invalid UTF-8 sequences with U+FFFD
	var result strings.Builder
	for i := 0; i < len(value); {
		r, size := utf8.DecodeRuneInString(value[i:])
		if r == utf8.RuneError && size == 1 {
			result.WriteRune('\ufffd')
			i++
		} else {
			result.WriteRune(r)
			i += size
		}
	}
	return result.String()
}

// ParseText decodes RFC 2047 encoded words, unfolds whitespace, and normalizes to NFC.
func ParseText(value string) string {
	if value == "" {
		return ""
	}

	// Decode RFC 2047 encoded words
	dec := new(mime.WordDecoder)
	decoded, err := dec.DecodeHeader(value)
	if err != nil {
		decoded = value
	}

	// Unfold whitespace: replace CRLF/LF followed by space/tab with single space
	decoded = regexp.MustCompile(`\r?\n[ \t]`).ReplaceAllString(decoded, " ")

	// Replace tabs with spaces
	decoded = strings.ReplaceAll(decoded, "\t", " ")

	// Collapse multiple spaces into one
	decoded = regexp.MustCompile(`  +`).ReplaceAllString(decoded, " ")

	// Trim leading/trailing whitespace
	decoded = strings.TrimSpace(decoded)

	// Normalize to NFC
	decoded = norm.NFC.String(decoded)

	return decoded
}

// ParseAddresses parses an address header into a list of EmailAddress.
func ParseAddresses(value string) ([]EmailAddress, error) {
	if value == "" {
		return nil, nil
	}

	addrs, err := mail.ParseAddressList(value)
	if err != nil {
		return nil, err
	}

	result := make([]EmailAddress, len(addrs))
	for i, addr := range addrs {
		result[i] = EmailAddress{
			Name:  addr.Name,
			Email: addr.Address,
		}
	}
	return result, nil
}

// ParseGroupedAddresses parses an address header into a list of EmailAddressGroup.
// Non-group addresses are returned as groups with nil Name.
func ParseGroupedAddresses(value string) ([]EmailAddressGroup, error) {
	if value == "" {
		return nil, nil
	}

	// Try to parse as a standard address list first
	// Go's mail package doesn't support group syntax directly,
	// so we handle simple cases
	addrs, err := mail.ParseAddressList(value)
	if err != nil {
		return nil, err
	}

	// Each individual address becomes a group with nil name
	result := make([]EmailAddressGroup, len(addrs))
	for i, addr := range addrs {
		result[i] = EmailAddressGroup{
			Name: nil,
			Addresses: []EmailAddress{
				{Name: addr.Name, Email: addr.Address},
			},
		}
	}
	return result, nil
}

// ParseMessageIds parses a message ID header into a list of message IDs.
// Angle brackets are stripped from each ID.
func ParseMessageIds(value string) []string {
	if value == "" {
		return nil
	}

	// Split on whitespace
	parts := strings.Fields(value)
	if len(parts) == 0 {
		return nil
	}

	result := make([]string, 0, len(parts))
	for _, part := range parts {
		// Strip angle brackets
		id := strings.TrimPrefix(part, "<")
		id = strings.TrimSuffix(id, ">")
		if id != "" {
			result = append(result, id)
		}
	}
	return result
}

// ParseDate parses a date header into an RFC 3339 string, or nil if invalid.
func ParseDate(value string) *string {
	if value == "" {
		return nil
	}

	t, err := mail.ParseDate(value)
	if err != nil {
		return nil
	}

	// Convert to UTC and format as RFC 3339
	result := t.UTC().Format(time.RFC3339)
	return &result
}

// ParseURLs parses an RFC 2369 URL header into a list of URLs.
// Angle brackets are stripped from each URL.
func ParseURLs(value string) []string {
	if value == "" {
		return nil
	}

	var result []string

	// Find all URLs enclosed in angle brackets
	// RFC 2369 format: <url1>, <url2> (optional comment)
	re := regexp.MustCompile(`<([^>]+)>`)
	matches := re.FindAllStringSubmatch(value, -1)

	for _, match := range matches {
		if len(match) > 1 {
			result = append(result, match[1])
		}
	}

	return result
}

// ApplyForm applies the specified form transformation to a header value.
func ApplyForm(value string, form Form) (any, error) {
	switch form {
	case FormRaw:
		return ParseRaw(value), nil

	case FormText:
		return ParseText(value), nil

	case FormAddresses:
		return ParseAddresses(value)

	case FormGroupedAddresses:
		return ParseGroupedAddresses(value)

	case FormMessageIds:
		return ParseMessageIds(value), nil

	case FormDate:
		return ParseDate(value), nil

	case FormURLs:
		return ParseURLs(value), nil

	default:
		return ParseRaw(value), nil
	}
}
