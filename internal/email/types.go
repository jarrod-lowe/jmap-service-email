// Package email provides types and operations for JMAP email storage.
package email

import (
	"fmt"
	"time"
)

// EmailAddress represents an email address with optional display name.
type EmailAddress struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// BodyPart represents a MIME part in the email body structure.
type BodyPart struct {
	PartID      string     `json:"partId"`
	BlobID      string     `json:"blobId,omitempty"`
	Size        int64      `json:"size"`
	Type        string     `json:"type"`
	Charset     string     `json:"charset,omitempty"`
	Disposition string     `json:"disposition,omitempty"`
	Name        string     `json:"name,omitempty"`
	SubParts    []BodyPart `json:"subParts,omitempty"`
}

// EmailItem represents an email stored in DynamoDB.
type EmailItem struct {
	AccountID     string          `json:"accountId"`
	EmailID       string          `json:"emailId"`
	BlobID        string          `json:"blobId"`
	ThreadID      string          `json:"threadId"`
	MailboxIDs    map[string]bool `json:"mailboxIds"`
	Keywords      map[string]bool `json:"keywords"`
	ReceivedAt    time.Time       `json:"receivedAt"`
	Size          int64           `json:"size"`
	HasAttachment bool            `json:"hasAttachment"`
	Subject       string          `json:"subject"`
	From          []EmailAddress  `json:"from"`
	To            []EmailAddress  `json:"to"`
	CC            []EmailAddress  `json:"cc"`
	ReplyTo       []EmailAddress  `json:"replyTo"`
	SentAt        time.Time       `json:"sentAt"`
	MessageID     []string        `json:"messageId"`
	InReplyTo     []string        `json:"inReplyTo"`
	References    []string        `json:"references"`
	Preview       string          `json:"preview"`
	BodyStructure BodyPart        `json:"bodyStructure"`
	TextBody      []string        `json:"textBody"`
	HTMLBody      []string        `json:"htmlBody"`
	Attachments   []string        `json:"attachments"`
}

// PK returns the DynamoDB partition key for this email.
func (e *EmailItem) PK() string {
	return fmt.Sprintf("ACCOUNT#%s", e.AccountID)
}

// SK returns the DynamoDB sort key for this email.
func (e *EmailItem) SK() string {
	return fmt.Sprintf("EMAIL#%s", e.EmailID)
}

// MailboxMembershipItem represents a mailbox membership record in DynamoDB.
type MailboxMembershipItem struct {
	AccountID  string    `json:"accountId"`
	MailboxID  string    `json:"mailboxId"`
	ReceivedAt time.Time `json:"receivedAt"`
	EmailID    string    `json:"emailId"`
}

// PK returns the DynamoDB partition key for this membership.
func (m *MailboxMembershipItem) PK() string {
	return fmt.Sprintf("ACCOUNT#%s", m.AccountID)
}

// SK returns the DynamoDB sort key for this membership.
func (m *MailboxMembershipItem) SK() string {
	return fmt.Sprintf("MBOX#%s#EMAIL#%s#%s", m.MailboxID, m.ReceivedAt.UTC().Format(time.RFC3339), m.EmailID)
}
