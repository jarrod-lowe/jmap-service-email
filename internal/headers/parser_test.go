package headers

import (
	"testing"
)

func TestParseRaw(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"simple text", "Hello World", "Hello World"},
		{"with newline", "Hello\r\n World", "Hello\r\n World"},
		{"empty", "", ""},
		{"invalid UTF-8 replaced", "Hello \xff World", "Hello \ufffd World"},
		{"valid UTF-8 preserved", "Héllo Wörld", "Héllo Wörld"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseRaw(tt.input)
			if got != tt.want {
				t.Errorf("ParseRaw(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseText(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"simple text", "Hello World", "Hello World"},
		{"RFC 2047 encoded", "=?UTF-8?Q?Hello_World?=", "Hello World"},
		{"RFC 2047 base64", "=?UTF-8?B?SGVsbG8gV29ybGQ=?=", "Hello World"},
		{"folded whitespace", "Hello\r\n World", "Hello World"},
		{"multiple spaces collapsed", "Hello   World", "Hello World"},
		{"tabs normalized", "Hello\tWorld", "Hello World"},
		{"empty", "", ""},
		{"unicode NFC normalized", "café", "café"}, // already NFC
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseText(tt.input)
			if got != tt.want {
				t.Errorf("ParseText(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseAddresses(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantAddrs  []EmailAddress
		wantErr    bool
	}{
		{
			name:  "single address",
			input: "alice@example.com",
			wantAddrs: []EmailAddress{
				{Email: "alice@example.com"},
			},
		},
		{
			name:  "address with name",
			input: "Alice <alice@example.com>",
			wantAddrs: []EmailAddress{
				{Name: "Alice", Email: "alice@example.com"},
			},
		},
		{
			name:  "quoted name",
			input: `"Alice Smith" <alice@example.com>`,
			wantAddrs: []EmailAddress{
				{Name: "Alice Smith", Email: "alice@example.com"},
			},
		},
		{
			name:  "multiple addresses",
			input: "Alice <alice@example.com>, Bob <bob@example.com>",
			wantAddrs: []EmailAddress{
				{Name: "Alice", Email: "alice@example.com"},
				{Name: "Bob", Email: "bob@example.com"},
			},
		},
		{
			name:      "empty input",
			input:     "",
			wantAddrs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseAddresses(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if len(got) != len(tt.wantAddrs) {
				t.Errorf("got %d addresses, want %d", len(got), len(tt.wantAddrs))
				return
			}

			for i, addr := range got {
				if addr.Name != tt.wantAddrs[i].Name {
					t.Errorf("addr[%d].Name = %q, want %q", i, addr.Name, tt.wantAddrs[i].Name)
				}
				if addr.Email != tt.wantAddrs[i].Email {
					t.Errorf("addr[%d].Email = %q, want %q", i, addr.Email, tt.wantAddrs[i].Email)
				}
			}
		})
	}
}

func TestParseGroupedAddresses(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantCount int // number of groups
	}{
		{
			name:      "simple address becomes ungrouped",
			input:     "alice@example.com",
			wantCount: 1,
		},
		{
			name:      "address with name",
			input:     "Alice <alice@example.com>",
			wantCount: 1,
		},
		{
			name:      "multiple addresses become multiple groups",
			input:     "alice@example.com, bob@example.com",
			wantCount: 2, // Each address becomes its own group with nil name
		},
		{
			name:      "empty",
			input:     "",
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseGroupedAddresses(tt.input)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if len(got) != tt.wantCount {
				t.Errorf("got %d groups, want %d", len(got), tt.wantCount)
			}
		})
	}
}

func TestParseMessageIds(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "single message ID",
			input: "<msg-123@example.com>",
			want:  []string{"msg-123@example.com"},
		},
		{
			name:  "multiple message IDs",
			input: "<msg-1@example.com> <msg-2@example.com>",
			want:  []string{"msg-1@example.com", "msg-2@example.com"},
		},
		{
			name:  "with extra whitespace",
			input: "  <msg-1@example.com>   <msg-2@example.com>  ",
			want:  []string{"msg-1@example.com", "msg-2@example.com"},
		},
		{
			name:  "no angle brackets",
			input: "msg-123@example.com",
			want:  []string{"msg-123@example.com"},
		},
		{
			name:  "empty",
			input: "",
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseMessageIds(tt.input)

			if len(got) != len(tt.want) {
				t.Errorf("got %d message IDs, want %d: %v", len(got), len(tt.want), got)
				return
			}

			for i, id := range got {
				if id != tt.want[i] {
					t.Errorf("id[%d] = %q, want %q", i, id, tt.want[i])
				}
			}
		})
	}
}

func TestParseDate(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string // RFC 3339 format, or empty for null
		wantNil bool
	}{
		{
			name:  "standard RFC 2822 date",
			input: "Sat, 20 Jan 2024 10:30:00 +0000",
			want:  "2024-01-20T10:30:00Z",
		},
		{
			name:  "with timezone offset",
			input: "Sat, 20 Jan 2024 10:30:00 -0500",
			want:  "2024-01-20T15:30:00Z", // converted to UTC
		},
		{
			name:    "empty returns nil",
			input:   "",
			wantNil: true,
		},
		{
			name:    "invalid date returns nil",
			input:   "not a date",
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseDate(tt.input)

			if tt.wantNil {
				if got != nil {
					t.Errorf("got %v, want nil", *got)
				}
				return
			}

			if got == nil {
				t.Error("got nil, want non-nil")
				return
			}

			if *got != tt.want {
				t.Errorf("got %q, want %q", *got, tt.want)
			}
		})
	}
}

func TestParseURLs(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "single URL",
			input: "<mailto:unsubscribe@example.com>",
			want:  []string{"mailto:unsubscribe@example.com"},
		},
		{
			name:  "multiple URLs",
			input: "<mailto:unsubscribe@example.com>, <https://example.com/unsubscribe>",
			want:  []string{"mailto:unsubscribe@example.com", "https://example.com/unsubscribe"},
		},
		{
			name:  "empty",
			input: "",
			want:  nil,
		},
		{
			name:  "RFC 2369 format with comment",
			input: "<mailto:list@example.com> (Use this to subscribe)",
			want:  []string{"mailto:list@example.com"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseURLs(tt.input)

			if len(got) != len(tt.want) {
				t.Errorf("got %d URLs, want %d: %v", len(got), len(tt.want), got)
				return
			}

			for i, url := range got {
				if url != tt.want[i] {
					t.Errorf("url[%d] = %q, want %q", i, url, tt.want[i])
				}
			}
		})
	}
}

func TestApplyForm(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		form     Form
		wantType string // "string", "[]string", "[]EmailAddress", "[]EmailAddressGroup", "*string", "nil"
	}{
		{"Raw returns string", "test", FormRaw, "string"},
		{"Text returns string", "test", FormText, "string"},
		{"Addresses returns slice", "alice@example.com", FormAddresses, "[]EmailAddress"},
		{"GroupedAddresses returns slice", "alice@example.com", FormGroupedAddresses, "[]EmailAddressGroup"},
		{"MessageIds returns slice", "<msg@example.com>", FormMessageIds, "[]string"},
		{"Date returns pointer", "Sat, 20 Jan 2024 10:30:00 +0000", FormDate, "*string"},
		{"URLs returns slice", "<mailto:test@example.com>", FormURLs, "[]string"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ApplyForm(tt.value, tt.form)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// Just verify we get the right type
			switch tt.wantType {
			case "string":
				if _, ok := got.(string); !ok {
					t.Errorf("expected string, got %T", got)
				}
			case "[]string":
				if _, ok := got.([]string); !ok {
					t.Errorf("expected []string, got %T", got)
				}
			case "[]EmailAddress":
				if _, ok := got.([]EmailAddress); !ok {
					t.Errorf("expected []EmailAddress, got %T", got)
				}
			case "[]EmailAddressGroup":
				if _, ok := got.([]EmailAddressGroup); !ok {
					t.Errorf("expected []EmailAddressGroup, got %T", got)
				}
			case "*string":
				if _, ok := got.(*string); !ok {
					t.Errorf("expected *string, got %T", got)
				}
			}
		})
	}
}
