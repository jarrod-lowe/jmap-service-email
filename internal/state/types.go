// Package state provides types and operations for JMAP state tracking.
package state

import (
	"fmt"
	"time"

	"github.com/jarrod-lowe/jmap-service-email/internal/dynamo"
)

// ObjectType represents the type of JMAP object being tracked.
type ObjectType string

const (
	// ObjectTypeEmail represents the Email object type.
	ObjectTypeEmail ObjectType = "Email"
	// ObjectTypeMailbox represents the Mailbox object type.
	ObjectTypeMailbox ObjectType = "Mailbox"
	// ObjectTypeThread represents the Thread object type.
	ObjectTypeThread ObjectType = "Thread"
)

// ChangeType represents the type of change made to an object.
type ChangeType string

const (
	// ChangeTypeCreated indicates a new object was created.
	ChangeTypeCreated ChangeType = "created"
	// ChangeTypeUpdated indicates an existing object was modified.
	ChangeTypeUpdated ChangeType = "updated"
	// ChangeTypeDestroyed indicates an object was deleted.
	ChangeTypeDestroyed ChangeType = "destroyed"
)

// StateItem represents a state counter stored in DynamoDB.
// PK: ACCOUNT#{accountId}
// SK: STATE#{type}
type StateItem struct {
	AccountID    string
	ObjectType   ObjectType
	CurrentState int64
	UpdatedAt    time.Time
}

// PK returns the DynamoDB partition key for this state item.
func (s *StateItem) PK() string {
	return dynamo.PrefixAccount + s.AccountID
}

// SK returns the DynamoDB sort key for this state item.
func (s *StateItem) SK() string {
	return PrefixState + string(s.ObjectType)
}

// ChangeRecord represents a change log entry stored in DynamoDB.
// PK: ACCOUNT#{accountId}
// SK: CHANGE#{type}#{state} (state is zero-padded to 10 digits)
type ChangeRecord struct {
	AccountID  string
	ObjectType ObjectType
	State      int64
	ObjectID   string
	ChangeType ChangeType
	Timestamp  time.Time
	TTL        int64
}

// PK returns the DynamoDB partition key for this change record.
func (c *ChangeRecord) PK() string {
	return dynamo.PrefixAccount + c.AccountID
}

// SK returns the DynamoDB sort key for this change record.
// State is zero-padded to 10 digits to ensure lexicographic ordering.
func (c *ChangeRecord) SK() string {
	return fmt.Sprintf("%s%s#%010d", PrefixChange, c.ObjectType, c.State)
}

// ChangesResult represents the result of a /changes query.
type ChangesResult struct {
	OldState  string
	NewState  string
	HasMore   bool
	Created   []string
	Updated   []string
	Destroyed []string
}

// DefaultRetentionDays is the default TTL for change log entries.
const DefaultRetentionDays = 7

// MaxStateValue is the maximum value for a state counter (10 digits).
const MaxStateValue = 9999999999
