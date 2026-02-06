// Package searchindex provides async search indexing via SQS.
package searchindex

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
}
