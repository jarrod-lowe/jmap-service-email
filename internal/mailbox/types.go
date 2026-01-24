// Package mailbox provides types and operations for JMAP mailbox storage.
package mailbox

import (
	"fmt"
	"time"
)

// ValidRoles defines the valid mailbox roles per RFC 8621.
var ValidRoles = map[string]bool{
	"inbox":   true,
	"drafts":  true,
	"sent":    true,
	"trash":   true,
	"junk":    true,
	"archive": true,
}

// MailboxRights represents permissions for a mailbox.
type MailboxRights struct {
	MayReadItems   bool
	MayAddItems    bool
	MayRemoveItems bool
	MaySetSeen     bool
	MaySetKeywords bool
	MayCreateChild bool
	MayRename      bool
	MayDelete      bool
	MaySubmit      bool
}

// AllRights returns a MailboxRights with all permissions enabled.
func AllRights() MailboxRights {
	return MailboxRights{
		MayReadItems:   true,
		MayAddItems:    true,
		MayRemoveItems: true,
		MaySetSeen:     true,
		MaySetKeywords: true,
		MayCreateChild: true,
		MayRename:      true,
		MayDelete:      true,
		MaySubmit:      true,
	}
}

// MailboxItem represents a mailbox stored in DynamoDB.
type MailboxItem struct {
	AccountID    string
	MailboxID    string
	Name         string
	Role         string
	SortOrder    int
	TotalEmails  int
	UnreadEmails int
	IsSubscribed bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// PK returns the DynamoDB partition key for this mailbox.
func (m *MailboxItem) PK() string {
	return fmt.Sprintf("ACCOUNT#%s", m.AccountID)
}

// SK returns the DynamoDB sort key for this mailbox.
func (m *MailboxItem) SK() string {
	return fmt.Sprintf("MAILBOX#%s", m.MailboxID)
}
