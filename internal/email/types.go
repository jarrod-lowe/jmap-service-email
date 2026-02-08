// Package email provides types and operations for JMAP email storage.
package email

import (
	"fmt"
	"time"

	"github.com/jarrod-lowe/jmap-service-email/internal/dynamo"
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
	HeaderSize    int64           `json:"headerSize"`
	HasAttachment bool            `json:"hasAttachment"`
	Subject       string          `json:"subject"`
	From          []EmailAddress  `json:"from"`
	Sender        []EmailAddress  `json:"sender"`
	To            []EmailAddress  `json:"to"`
	CC            []EmailAddress  `json:"cc"`
	Bcc           []EmailAddress  `json:"bcc"`
	ReplyTo       []EmailAddress  `json:"replyTo"`
	SentAt        time.Time       `json:"sentAt"`
	MessageID     []string        `json:"messageId"`
	InReplyTo     []string        `json:"inReplyTo"`
	References    []string        `json:"references"`
	Preview       string          `json:"preview"`
	Summary       string          `json:"summary,omitempty"`
	BodyStructure BodyPart        `json:"bodyStructure"`
	TextBody      []string        `json:"textBody"`
	HTMLBody      []string        `json:"htmlBody"`
	Attachments   []string        `json:"attachments"`
	Version       int             `json:"version"`
	SearchChunks  int             `json:"searchChunks,omitempty"`
	DeletedAt     *time.Time      `json:"deletedAt,omitempty"`
}

// PK returns the DynamoDB partition key for this email.
func (e *EmailItem) PK() string {
	return dynamo.PrefixAccount + e.AccountID
}

// SK returns the DynamoDB sort key for this email.
func (e *EmailItem) SK() string {
	return PrefixEmail + e.EmailID
}

// LSI1SK returns the DynamoDB LSI sort key for receivedAt sorting.
// Format: RCVD#{receivedAt}#{emailId}
func (e *EmailItem) LSI1SK() string {
	return fmt.Sprintf("%s%s#%s", PrefixRcvd, e.ReceivedAt.UTC().Format(time.RFC3339), e.EmailID)
}

// LSI2SK returns the DynamoDB LSI sort key for Message-ID lookup.
// Format: MSGID#{messageId}
// Returns empty string if no Message-ID is present.
func (e *EmailItem) LSI2SK() string {
	if len(e.MessageID) == 0 {
		return ""
	}
	return PrefixMsgID + e.MessageID[0]
}

// LSI3SK returns the DynamoDB LSI sort key for thread queries.
// Format: THREAD#{threadId}#RCVD#{receivedAt}#{emailId}
// Returns empty string if no ThreadID is present.
func (e *EmailItem) LSI3SK() string {
	if e.ThreadID == "" {
		return ""
	}
	return fmt.Sprintf("%s%s#%s%s#%s", PrefixThread, e.ThreadID, PrefixRcvd, e.ReceivedAt.UTC().Format(time.RFC3339), e.EmailID)
}

// QueryRequest represents an Email/query request parameters.
type QueryRequest struct {
	Filter          *QueryFilter
	Sort            []Comparator
	Position        int
	Anchor          string
	AnchorOffset    int
	Limit           int
	CollapseThreads bool
}

// QueryFilter represents filter conditions for Email/query.
// All specified conditions must match (AND semantics).
type QueryFilter struct {
	// Structural filters (DynamoDB key conditions + filter expressions)
	InMailbox        string
	InMailboxOtherThan []string
	Before           *time.Time
	After            *time.Time
	MinSize          *int64
	MaxSize          *int64
	HasAttachment    *bool
	HasKeyword       string
	NotKeyword       string

	// Address filters (DynamoDB TOK# entries or S3 Vectors metadata)
	From string
	To   string
	CC   string
	Bcc  string

	// Content filters (S3 Vectors semantic search)
	Text    string
	Body    string
	Subject string
	Summary string
}

// NeedsVectorSearch returns true if the filter requires S3 Vectors semantic search.
func (f *QueryFilter) NeedsVectorSearch() bool {
	return f.Text != "" || f.Body != "" || f.Subject != "" || f.Summary != ""
}

// HasAddressFilter returns true if any address filter is set.
func (f *QueryFilter) HasAddressFilter() bool {
	return f.From != "" || f.To != "" || f.CC != "" || f.Bcc != ""
}

// Comparator represents a sort condition for Email/query.
type Comparator struct {
	Property    string
	IsAscending bool
}

// QueryResult represents the result of an Email/query operation.
type QueryResult struct {
	IDs        []string
	Position   int
	QueryState string
	Total      *int
}

// MailboxMembershipItem represents a mailbox membership record in DynamoDB.
type MailboxMembershipItem struct {
	AccountID  string    `json:"accountId"`
	MailboxID  string    `json:"mailboxId"`
	ReceivedAt time.Time `json:"receivedAt"`
	EmailID    string    `json:"emailId"`
	ThreadID   string    `json:"threadId"`
}

// PK returns the DynamoDB partition key for this membership.
func (m *MailboxMembershipItem) PK() string {
	return dynamo.PrefixAccount + m.AccountID
}

// SK returns the DynamoDB sort key for this membership.
func (m *MailboxMembershipItem) SK() string {
	return fmt.Sprintf("%s%s#%s%s#%s", PrefixMbox, m.MailboxID, PrefixEmail, m.ReceivedAt.UTC().Format(time.RFC3339), m.EmailID)
}
