package headers

import "testing"

func TestValidateForm(t *testing.T) {
	tests := []struct {
		name       string
		headerName string
		form       Form
		wantValid  bool
	}{
		// Raw form - allowed for any header
		{"Raw allowed for Subject", "Subject", FormRaw, true},
		{"Raw allowed for From", "From", FormRaw, true},
		{"Raw allowed for X-Custom", "X-Custom", FormRaw, true},
		{"Raw allowed for Date", "Date", FormRaw, true},

		// Text form - restricted headers
		{"Text allowed for Subject", "Subject", FormText, true},
		{"Text allowed for Comments", "Comments", FormText, true},
		{"Text allowed for Keywords", "Keywords", FormText, true},
		{"Text allowed for List-Id", "List-Id", FormText, true},
		{"Text allowed for X-Custom (non-RFC5322)", "X-Custom-Header", FormText, true},
		{"Text NOT allowed for From", "From", FormText, false},
		{"Text NOT allowed for Date", "Date", FormText, false},
		{"Text NOT allowed for Message-ID", "Message-ID", FormText, false},

		// Addresses form - restricted to address headers
		{"Addresses allowed for From", "From", FormAddresses, true},
		{"Addresses allowed for Sender", "Sender", FormAddresses, true},
		{"Addresses allowed for Reply-To", "Reply-To", FormAddresses, true},
		{"Addresses allowed for To", "To", FormAddresses, true},
		{"Addresses allowed for Cc", "Cc", FormAddresses, true},
		{"Addresses allowed for Bcc", "Bcc", FormAddresses, true},
		{"Addresses allowed for Resent-From", "Resent-From", FormAddresses, true},
		{"Addresses allowed for Resent-Sender", "Resent-Sender", FormAddresses, true},
		{"Addresses allowed for Resent-To", "Resent-To", FormAddresses, true},
		{"Addresses allowed for Resent-Cc", "Resent-Cc", FormAddresses, true},
		{"Addresses allowed for Resent-Bcc", "Resent-Bcc", FormAddresses, true},
		{"Addresses NOT allowed for Subject", "Subject", FormAddresses, false},
		{"Addresses NOT allowed for Date", "Date", FormAddresses, false},
		{"Addresses NOT allowed for X-Custom", "X-Custom", FormAddresses, false},

		// GroupedAddresses form - same restrictions as Addresses
		{"GroupedAddresses allowed for From", "From", FormGroupedAddresses, true},
		{"GroupedAddresses allowed for To", "To", FormGroupedAddresses, true},
		{"GroupedAddresses allowed for Resent-To", "Resent-To", FormGroupedAddresses, true},
		{"GroupedAddresses NOT allowed for Subject", "Subject", FormGroupedAddresses, false},

		// MessageIds form - restricted to message ID headers
		{"MessageIds allowed for Message-ID", "Message-ID", FormMessageIds, true},
		{"MessageIds allowed for In-Reply-To", "In-Reply-To", FormMessageIds, true},
		{"MessageIds allowed for References", "References", FormMessageIds, true},
		{"MessageIds allowed for Resent-Message-ID", "Resent-Message-ID", FormMessageIds, true},
		{"MessageIds NOT allowed for Subject", "Subject", FormMessageIds, false},
		{"MessageIds NOT allowed for From", "From", FormMessageIds, false},

		// Date form - restricted to date headers
		{"Date form allowed for Date", "Date", FormDate, true},
		{"Date form allowed for Resent-Date", "Resent-Date", FormDate, true},
		{"Date form NOT allowed for From", "From", FormDate, false},
		{"Date form NOT allowed for Subject", "Subject", FormDate, false},

		// URLs form - restricted to List-* headers (RFC 2369)
		{"URLs allowed for List-Help", "List-Help", FormURLs, true},
		{"URLs allowed for List-Unsubscribe", "List-Unsubscribe", FormURLs, true},
		{"URLs allowed for List-Subscribe", "List-Subscribe", FormURLs, true},
		{"URLs allowed for List-Post", "List-Post", FormURLs, true},
		{"URLs allowed for List-Owner", "List-Owner", FormURLs, true},
		{"URLs allowed for List-Archive", "List-Archive", FormURLs, true},
		{"URLs NOT allowed for Subject", "Subject", FormURLs, false},
		{"URLs NOT allowed for From", "From", FormURLs, false},
		{"URLs NOT allowed for List-Id", "List-Id", FormURLs, false}, // List-Id is not RFC 2369

		// Case insensitivity
		{"Case insensitive - from", "from", FormAddresses, true},
		{"Case insensitive - FROM", "FROM", FormAddresses, true},
		{"Case insensitive - message-id", "message-id", FormMessageIds, true},
		{"Case insensitive - DATE", "DATE", FormDate, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateForm(tt.headerName, tt.form)
			gotValid := err == nil

			if gotValid != tt.wantValid {
				if tt.wantValid {
					t.Errorf("ValidateForm(%q, %v) returned error %v, want valid", tt.headerName, tt.form, err)
				} else {
					t.Errorf("ValidateForm(%q, %v) returned valid, want error", tt.headerName, tt.form)
				}
			}
		})
	}
}

func TestIsStructuredHeader(t *testing.T) {
	tests := []struct {
		headerName string
		want       bool
	}{
		// RFC 5322 structured headers
		{"From", true},
		{"Sender", true},
		{"Reply-To", true},
		{"To", true},
		{"Cc", true},
		{"Bcc", true},
		{"Message-ID", true},
		{"In-Reply-To", true},
		{"References", true},
		{"Date", true},
		{"Resent-From", true},
		{"Resent-Date", true},

		// RFC 2369 headers
		{"List-Help", true},
		{"List-Unsubscribe", true},

		// Unstructured headers
		{"Subject", false},
		{"Comments", false},
		{"Keywords", false},
		{"X-Custom", false},

		// Case insensitivity
		{"from", true},
		{"subject", false},
	}

	for _, tt := range tests {
		t.Run(tt.headerName, func(t *testing.T) {
			got := IsStructuredHeader(tt.headerName)
			if got != tt.want {
				t.Errorf("IsStructuredHeader(%q) = %v, want %v", tt.headerName, got, tt.want)
			}
		})
	}
}
