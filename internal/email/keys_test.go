package email

import "testing"

func TestAttrHeaderSize(t *testing.T) {
	// AttrHeaderSize should be defined for DynamoDB attribute mapping
	if AttrHeaderSize != "headerSize" {
		t.Errorf("AttrHeaderSize = %q, want %q", AttrHeaderSize, "headerSize")
	}
}
