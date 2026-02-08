// Package searchindex provides async search indexing via SQS.
package searchindex

import "time"

// Action represents the type of search index operation.
type Action string

const (
	// ActionIndex indicates an email should be indexed (create/update).
	ActionIndex Action = "index"
	// ActionDelete indicates an email's vectors should be deleted.
	ActionDelete Action = "delete"
)

// Message is the SQS message body for search index requests.
type Message struct {
	AccountID string `json:"accountId"`
	EmailID   string `json:"emailId"`
	Action    Action `json:"action"`
	APIURL    string `json:"apiUrl"`

	// DeleteMetadata contains deletion info (populated only for ActionDelete)
	DeleteMetadata *DeleteMetadata `json:"deleteMetadata,omitempty"`
}

// DeleteMetadata contains the information needed to delete search artifacts
// without fetching the email record (avoids race with email-cleanup).
type DeleteMetadata struct {
	SearchChunks int            `json:"searchChunks"`
	Summary      string         `json:"summary"`
	From         []EmailAddress `json:"from"`
	To           []EmailAddress `json:"to"`
	CC           []EmailAddress `json:"cc"`
	Bcc          []EmailAddress `json:"bcc"`
	ReceivedAt   time.Time      `json:"receivedAt"`
}

// EmailAddress mirrors email.EmailAddress to avoid circular dependencies.
type EmailAddress struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}
