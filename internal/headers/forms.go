package headers

import (
	"fmt"
	"strings"
)

// Header name sets for validation (all lowercase for case-insensitive matching).
var (
	// addressHeaders are headers that can use Addresses/GroupedAddresses forms.
	// Per RFC 8621, this includes RFC 5322 address headers and Resent-* variants.
	addressHeaders = map[string]bool{
		"from":          true,
		"sender":        true,
		"reply-to":      true,
		"to":            true,
		"cc":            true,
		"bcc":           true,
		"resent-from":   true,
		"resent-sender": true,
		"resent-to":     true,
		"resent-cc":     true,
		"resent-bcc":    true,
	}

	// messageIdHeaders are headers that can use MessageIds form.
	messageIdHeaders = map[string]bool{
		"message-id":        true,
		"in-reply-to":       true,
		"references":        true,
		"resent-message-id": true,
	}

	// dateHeaders are headers that can use Date form.
	dateHeaders = map[string]bool{
		"date":        true,
		"resent-date": true,
	}

	// urlHeaders are RFC 2369 headers that can use URLs form.
	urlHeaders = map[string]bool{
		"list-help":        true,
		"list-unsubscribe": true,
		"list-subscribe":   true,
		"list-post":        true,
		"list-owner":       true,
		"list-archive":     true,
	}

	// textHeaders are unstructured headers that allow Text form.
	// Non-RFC5322/RFC2369 headers also allow Text form.
	textHeaders = map[string]bool{
		"subject":  true,
		"comments": true,
		"keywords": true,
		"list-id":  true,
	}

	// structuredHeaders is the union of all structured header types.
	// Headers not in this set are considered unstructured.
	structuredHeaders = map[string]bool{}
)

func init() {
	// Build structuredHeaders from all the specific sets
	for h := range addressHeaders {
		structuredHeaders[h] = true
	}
	for h := range messageIdHeaders {
		structuredHeaders[h] = true
	}
	for h := range dateHeaders {
		structuredHeaders[h] = true
	}
	for h := range urlHeaders {
		structuredHeaders[h] = true
	}
}

// ValidateForm checks if the given form is valid for the header name.
// Returns nil if valid, or an error describing why it's invalid.
func ValidateForm(headerName string, form Form) error {
	name := strings.ToLower(headerName)

	switch form {
	case FormRaw:
		// Raw is always allowed
		return nil

	case FormText:
		// Text is allowed for unstructured headers (Subject, Comments, Keywords, List-Id)
		// and non-RFC5322/RFC2369 headers
		if textHeaders[name] || !structuredHeaders[name] {
			return nil
		}
		return fmt.Errorf("header %q cannot use asText form (structured header)", headerName)

	case FormAddresses, FormGroupedAddresses:
		if addressHeaders[name] {
			return nil
		}
		return fmt.Errorf("header %q cannot use address forms (not an address header)", headerName)

	case FormMessageIds:
		if messageIdHeaders[name] {
			return nil
		}
		return fmt.Errorf("header %q cannot use asMessageIds form (not a message-id header)", headerName)

	case FormDate:
		if dateHeaders[name] {
			return nil
		}
		return fmt.Errorf("header %q cannot use asDate form (not a date header)", headerName)

	case FormURLs:
		if urlHeaders[name] {
			return nil
		}
		return fmt.Errorf("header %q cannot use asURLs form (not an RFC 2369 header)", headerName)

	default:
		return fmt.Errorf("unknown form: %v", form)
	}
}

// IsStructuredHeader returns true if the header is a structured header
// defined in RFC 5322 or RFC 2369 (as opposed to unstructured headers).
func IsStructuredHeader(headerName string) bool {
	return structuredHeaders[strings.ToLower(headerName)]
}
