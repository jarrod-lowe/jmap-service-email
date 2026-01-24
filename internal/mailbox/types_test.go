package mailbox

import (
	"testing"
	"time"
)

func TestMailboxItem_PK(t *testing.T) {
	m := &MailboxItem{
		AccountID: "user-123",
		MailboxID: "inbox",
	}

	got := m.PK()
	want := "ACCOUNT#user-123"

	if got != want {
		t.Errorf("PK() = %q, want %q", got, want)
	}
}

func TestMailboxItem_SK(t *testing.T) {
	m := &MailboxItem{
		AccountID: "user-123",
		MailboxID: "inbox",
	}

	got := m.SK()
	want := "MAILBOX#inbox"

	if got != want {
		t.Errorf("SK() = %q, want %q", got, want)
	}
}

func TestValidRoles(t *testing.T) {
	validRoles := []string{"inbox", "drafts", "sent", "trash", "junk", "archive"}

	for _, role := range validRoles {
		if !ValidRoles[role] {
			t.Errorf("ValidRoles[%q] = false, want true", role)
		}
	}

	// Invalid role should not be in the map
	if ValidRoles["invalid"] {
		t.Error("ValidRoles[\"invalid\"] = true, want false")
	}
}

func TestAllRights(t *testing.T) {
	rights := AllRights()

	if !rights.MayReadItems {
		t.Error("AllRights().MayReadItems = false, want true")
	}
	if !rights.MayAddItems {
		t.Error("AllRights().MayAddItems = false, want true")
	}
	if !rights.MayRemoveItems {
		t.Error("AllRights().MayRemoveItems = false, want true")
	}
	if !rights.MaySetSeen {
		t.Error("AllRights().MaySetSeen = false, want true")
	}
	if !rights.MaySetKeywords {
		t.Error("AllRights().MaySetKeywords = false, want true")
	}
	if !rights.MayCreateChild {
		t.Error("AllRights().MayCreateChild = false, want true")
	}
	if !rights.MayRename {
		t.Error("AllRights().MayRename = false, want true")
	}
	if !rights.MayDelete {
		t.Error("AllRights().MayDelete = false, want true")
	}
	if !rights.MaySubmit {
		t.Error("AllRights().MaySubmit = false, want true")
	}
}

func TestMailboxItem_Fields(t *testing.T) {
	now := time.Now().UTC()
	m := &MailboxItem{
		AccountID:    "user-123",
		MailboxID:    "inbox",
		Name:         "Inbox",
		Role:         "inbox",
		SortOrder:    0,
		TotalEmails:  10,
		UnreadEmails: 3,
		IsSubscribed: true,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	if m.AccountID != "user-123" {
		t.Errorf("AccountID = %q, want %q", m.AccountID, "user-123")
	}
	if m.MailboxID != "inbox" {
		t.Errorf("MailboxID = %q, want %q", m.MailboxID, "inbox")
	}
	if m.Name != "Inbox" {
		t.Errorf("Name = %q, want %q", m.Name, "Inbox")
	}
	if m.Role != "inbox" {
		t.Errorf("Role = %q, want %q", m.Role, "inbox")
	}
	if m.TotalEmails != 10 {
		t.Errorf("TotalEmails = %d, want %d", m.TotalEmails, 10)
	}
	if m.UnreadEmails != 3 {
		t.Errorf("UnreadEmails = %d, want %d", m.UnreadEmails, 3)
	}
	if !m.IsSubscribed {
		t.Error("IsSubscribed = false, want true")
	}
}
