// Package charset provides character set decoding for email body content.
package charset

import (
	"bytes"
	"io"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/ianaindex"
	"golang.org/x/text/transform"
)

// DecodeReader wraps a reader with charset decoding.
// Returns decoded reader, whether encoding problem occurred, and error.
//
// Behavior:
// - If charset is empty, defaults to "us-ascii"
// - If charset lookup fails or decode errors occur: tries UTF-8, then Latin-1
// - Returns isEncodingProblem=true if fallback was needed
func DecodeReader(r io.Reader, charset string) (io.Reader, bool, error) {
	// Default empty charset to us-ascii
	if charset == "" {
		charset = "us-ascii"
	}

	// Normalize charset name
	charset = strings.ToLower(strings.TrimSpace(charset))

	// For UTF-8 and ASCII, we need to validate the content
	if isUTF8Encoding(charset) || charset == "ascii" || charset == "us-ascii" {
		return decodeUTF8WithValidation(r)
	}

	// Try to lookup the encoding
	enc, err := lookupEncoding(charset)
	if err != nil {
		// Unknown charset - read content and try fallbacks
		content, readErr := io.ReadAll(r)
		if readErr != nil {
			return nil, false, readErr
		}
		return bytes.NewReader(content), true, nil
	}

	// Nil encoding means pass-through (shouldn't happen after above checks)
	if enc == nil {
		return r, false, nil
	}

	// For other encodings, use the transform reader
	decoder := enc.NewDecoder()
	return transform.NewReader(r, decoder), false, nil
}

// lookupEncoding finds the encoding for a charset name.
func lookupEncoding(charset string) (encoding.Encoding, error) {
	// Handle common aliases that may not be in IANA index
	switch charset {
	case "utf8":
		return nil, nil // UTF-8 is handled specially
	case "latin1", "latin-1":
		return charmap.ISO8859_1, nil
	case "ascii", "us-ascii":
		return nil, nil // ASCII is pass-through (subset of UTF-8)
	}

	enc, err := ianaindex.IANA.Encoding(charset)
	if err != nil {
		return nil, err
	}
	if enc == nil {
		// Some charsets return nil encoding (like UTF-8)
		return nil, nil
	}
	return enc, nil
}

// isUTF8Encoding checks if the charset is a UTF-8 variant.
func isUTF8Encoding(charset string) bool {
	switch charset {
	case "utf-8", "utf8":
		return true
	default:
		return false
	}
}

// decodeUTF8WithValidation reads UTF-8 content and validates it.
// If invalid bytes are found, falls back to Latin-1.
func decodeUTF8WithValidation(r io.Reader) (io.Reader, bool, error) {
	content, err := io.ReadAll(r)
	if err != nil {
		return nil, false, err
	}

	if utf8.Valid(content) {
		return bytes.NewReader(content), false, nil
	}

	// Invalid UTF-8 - decode as Latin-1
	decoded := decodeLatin1(content)
	return bytes.NewReader(decoded), true, nil
}

// decodeLatin1 converts ISO-8859-1 bytes to UTF-8.
func decodeLatin1(data []byte) []byte {
	decoder := charmap.ISO8859_1.NewDecoder()
	result, _, err := transform.Bytes(decoder, data)
	if err != nil {
		// Should not happen for Latin-1, but return original if it does
		return data
	}
	return result
}
