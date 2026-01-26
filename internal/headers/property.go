// Package headers provides parsing and transformation for JMAP header:* properties.
package headers

import (
	"errors"
	"strings"
)

// Form represents the output form for a header value.
type Form int

const (
	FormRaw Form = iota
	FormText
	FormAddresses
	FormGroupedAddresses
	FormMessageIds
	FormDate
	FormURLs
)

// HeaderProperty represents a parsed header:Name:asForm:all property.
type HeaderProperty struct {
	Name string
	Form Form
	All  bool
}

// IsHeaderProperty returns true if the property string is a header:* property.
func IsHeaderProperty(prop string) bool {
	return strings.HasPrefix(prop, "header:")
}

// ParseHeaderProperty parses a header:Name:asForm:all property string.
// Returns an error if the property is not a valid header property.
func ParseHeaderProperty(prop string) (*HeaderProperty, error) {
	if !IsHeaderProperty(prop) {
		return nil, errors.New("not a header property")
	}

	// Remove "header:" prefix
	rest := prop[7:]

	if rest == "" {
		return nil, errors.New("missing header name")
	}

	// Split on colons
	parts := strings.Split(rest, ":")

	if parts[0] == "" {
		return nil, errors.New("missing header name")
	}

	result := &HeaderProperty{
		Name: parts[0],
		Form: FormRaw,
		All:  false,
	}

	// Parse remaining parts
	for i := 1; i < len(parts); i++ {
		part := parts[i]

		if part == "all" {
			result.All = true
			continue
		}

		// Check for form specifier
		form, err := parseForm(part)
		if err != nil {
			return nil, err
		}
		result.Form = form
	}

	return result, nil
}

// parseForm parses a form specifier (asText, asAddresses, etc.)
func parseForm(s string) (Form, error) {
	switch s {
	case "asRaw":
		return FormRaw, nil
	case "asText":
		return FormText, nil
	case "asAddresses":
		return FormAddresses, nil
	case "asGroupedAddresses":
		return FormGroupedAddresses, nil
	case "asMessageIds":
		return FormMessageIds, nil
	case "asDate":
		return FormDate, nil
	case "asURLs":
		return FormURLs, nil
	default:
		return FormRaw, errors.New("invalid header form: " + s)
	}
}
