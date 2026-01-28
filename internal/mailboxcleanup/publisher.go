// Package mailboxcleanup provides async mailbox email cleanup via SQS.
package mailboxcleanup

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// MailboxCleanupPublisher publishes mailbox cleanup requests to an async queue.
type MailboxCleanupPublisher interface {
	PublishMailboxCleanup(ctx context.Context, accountID, mailboxID string) error
}

// MailboxCleanupMessage is the SQS message body for mailbox cleanup requests.
type MailboxCleanupMessage struct {
	AccountID string `json:"accountId"`
	MailboxID string `json:"mailboxId"`
}

// SQSSender abstracts SQS send operations for dependency inversion.
type SQSSender interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// SQSPublisher publishes mailbox cleanup requests to an SQS queue.
type SQSPublisher struct {
	client   SQSSender
	queueURL string
}

// NewSQSPublisher creates a new SQSPublisher.
func NewSQSPublisher(client SQSSender, queueURL string) *SQSPublisher {
	return &SQSPublisher{
		client:   client,
		queueURL: queueURL,
	}
}

// PublishMailboxCleanup sends a mailbox cleanup message to SQS.
func (p *SQSPublisher) PublishMailboxCleanup(ctx context.Context, accountID, mailboxID string) error {
	msg := MailboxCleanupMessage{
		AccountID: accountID,
		MailboxID: mailboxID,
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	bodyStr := string(body)
	_, err = p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    &p.queueURL,
		MessageBody: &bodyStr,
	})
	return err
}
