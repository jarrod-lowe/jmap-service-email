// Package blobdelete provides async blob deletion via SQS.
package blobdelete

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// BlobDeletePublisher publishes blob deletion requests to an async queue.
type BlobDeletePublisher interface {
	PublishBlobDeletions(ctx context.Context, accountID string, blobIDs []string) error
}

// BlobDeleteMessage is the SQS message body for blob deletion requests.
type BlobDeleteMessage struct {
	AccountID string   `json:"accountId"`
	BlobIDs   []string `json:"blobIds"`
}

// SQSSender abstracts SQS send operations for dependency inversion.
type SQSSender interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// SQSPublisher publishes blob deletion requests to an SQS queue.
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

// PublishBlobDeletions sends a blob deletion message to SQS.
func (p *SQSPublisher) PublishBlobDeletions(ctx context.Context, accountID string, blobIDs []string) error {
	if len(blobIDs) == 0 {
		return nil
	}

	msg := BlobDeleteMessage{
		AccountID: accountID,
		BlobIDs:   blobIDs,
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
