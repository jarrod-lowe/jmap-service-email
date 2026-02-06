package searchindex

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Publisher publishes search index requests to an async queue.
type Publisher interface {
	PublishIndexRequest(ctx context.Context, accountID, emailID string, action Action, apiURL string) error
}

// SQSSender abstracts SQS send operations for dependency inversion.
type SQSSender interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// SQSPublisher publishes search index requests to an SQS queue.
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

// PublishIndexRequest sends a search index message to SQS.
func (p *SQSPublisher) PublishIndexRequest(ctx context.Context, accountID, emailID string, action Action, apiURL string) error {
	msg := Message{
		AccountID: accountID,
		EmailID:   emailID,
		Action:    action,
		APIURL:    apiURL,
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
