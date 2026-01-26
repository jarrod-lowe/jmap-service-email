package headers

import "testing"

func TestParseHeaderProperty(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantProp   *HeaderProperty
		wantErr    bool
		wantErrMsg string
	}{
		// Basic cases
		{
			name:  "simple header name",
			input: "header:Subject",
			wantProp: &HeaderProperty{
				Name: "Subject",
				Form: FormRaw,
				All:  false,
			},
		},
		{
			name:  "header with asText form",
			input: "header:Subject:asText",
			wantProp: &HeaderProperty{
				Name: "Subject",
				Form: FormText,
				All:  false,
			},
		},
		{
			name:  "header with asAddresses form",
			input: "header:From:asAddresses",
			wantProp: &HeaderProperty{
				Name: "From",
				Form: FormAddresses,
				All:  false,
			},
		},
		{
			name:  "header with asGroupedAddresses form",
			input: "header:To:asGroupedAddresses",
			wantProp: &HeaderProperty{
				Name: "To",
				Form: FormGroupedAddresses,
				All:  false,
			},
		},
		{
			name:  "header with asMessageIds form",
			input: "header:Message-ID:asMessageIds",
			wantProp: &HeaderProperty{
				Name: "Message-ID",
				Form: FormMessageIds,
				All:  false,
			},
		},
		{
			name:  "header with asDate form",
			input: "header:Date:asDate",
			wantProp: &HeaderProperty{
				Name: "Date",
				Form: FormDate,
				All:  false,
			},
		},
		{
			name:  "header with asURLs form",
			input: "header:List-Unsubscribe:asURLs",
			wantProp: &HeaderProperty{
				Name: "List-Unsubscribe",
				Form: FormURLs,
				All:  false,
			},
		},
		{
			name:  "header with asRaw form (explicit)",
			input: "header:X-Custom:asRaw",
			wantProp: &HeaderProperty{
				Name: "X-Custom",
				Form: FormRaw,
				All:  false,
			},
		},
		// All suffix
		{
			name:  "header with all suffix",
			input: "header:Received:all",
			wantProp: &HeaderProperty{
				Name: "Received",
				Form: FormRaw,
				All:  true,
			},
		},
		{
			name:  "header with form and all suffix",
			input: "header:Subject:asText:all",
			wantProp: &HeaderProperty{
				Name: "Subject",
				Form: FormText,
				All:  true,
			},
		},
		{
			name:  "header with Addresses form and all suffix",
			input: "header:From:asAddresses:all",
			wantProp: &HeaderProperty{
				Name: "From",
				Form: FormAddresses,
				All:  true,
			},
		},
		// Case sensitivity
		{
			name:  "header name is preserved case",
			input: "header:SUBJECT",
			wantProp: &HeaderProperty{
				Name: "SUBJECT", // Name is preserved for matching
				Form: FormRaw,
				All:  false,
			},
		},
		{
			name:  "header name with mixed case",
			input: "header:X-My-Custom-Header",
			wantProp: &HeaderProperty{
				Name: "X-My-Custom-Header",
				Form: FormRaw,
				All:  false,
			},
		},
		// Error cases
		{
			name:       "not a header property",
			input:      "subject",
			wantErr:    true,
			wantErrMsg: "not a header property",
		},
		{
			name:       "missing header name",
			input:      "header:",
			wantErr:    true,
			wantErrMsg: "missing header name",
		},
		{
			name:       "invalid form",
			input:      "header:Subject:asInvalid",
			wantErr:    true,
			wantErrMsg: "invalid header form",
		},
		{
			name:       "empty header name after colon",
			input:      "header::asText",
			wantErr:    true,
			wantErrMsg: "missing header name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prop, err := ParseHeaderProperty(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseHeaderProperty(%q) expected error containing %q, got nil", tt.input, tt.wantErrMsg)
					return
				}
				if tt.wantErrMsg != "" && !contains(err.Error(), tt.wantErrMsg) {
					t.Errorf("ParseHeaderProperty(%q) error = %q, want error containing %q", tt.input, err.Error(), tt.wantErrMsg)
				}
				return
			}

			if err != nil {
				t.Errorf("ParseHeaderProperty(%q) unexpected error: %v", tt.input, err)
				return
			}

			if prop.Name != tt.wantProp.Name {
				t.Errorf("Name = %q, want %q", prop.Name, tt.wantProp.Name)
			}
			if prop.Form != tt.wantProp.Form {
				t.Errorf("Form = %v, want %v", prop.Form, tt.wantProp.Form)
			}
			if prop.All != tt.wantProp.All {
				t.Errorf("All = %v, want %v", prop.All, tt.wantProp.All)
			}
		})
	}
}

func TestIsHeaderProperty(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"header:Subject", true},
		{"header:From:asAddresses", true},
		{"header:Received:all", true},
		{"subject", false},
		{"id", false},
		{"Header:Subject", false}, // Must start with lowercase "header:"
		{"HEADER:Subject", false},
		{"headers:Subject", false}, // Wrong prefix
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := IsHeaderProperty(tt.input)
			if got != tt.want {
				t.Errorf("IsHeaderProperty(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
