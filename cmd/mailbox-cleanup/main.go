// Package main implements the mailbox-cleanup SQS consumer Lambda handler.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	"github.com/jarrod-lowe/jmap-service-email/internal/blobdelete"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailboxcleanup"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda/xrayconfig"
	"go.opentelemetry.io/otel"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

// EmailRepository defines the interface for email operations needed by cleanup.
type EmailRepository interface {
	QueryEmailsByMailbox(ctx context.Context, accountID, mailboxID string) ([]string, error)
	GetEmail(ctx context.Context, accountID, emailID string) (*email.EmailItem, error)
	BuildDeleteEmailItems(emailItem *email.EmailItem) []types.TransactWriteItem
	BuildUpdateEmailMailboxesItems(emailItem *email.EmailItem, newMailboxIDs map[string]bool) (addedMailboxes []string, removedMailboxes []string, items []types.TransactWriteItem)
}

// StateRepository defines the interface for state tracking operations.
type StateRepository interface {
	GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
	BuildStateChangeItems(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem)
	BuildStateChangeItemsMulti(accountID string, objectType state.ObjectType, currentState int64, objectIDs []string, changeType state.ChangeType) (int64, []types.TransactWriteItem)
}

// TransactWriter executes DynamoDB transactions.
type TransactWriter interface {
	TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// handler implements the mailbox-cleanup SQS consumer logic.
type handler struct {
	emailRepo           EmailRepository
	stateRepo           StateRepository
	blobDeletePublisher blobdelete.BlobDeletePublisher
	transactor          TransactWriter
}

// newHandler creates a new handler.
func newHandler(emailRepo EmailRepository, stateRepo StateRepository, blobDeletePublisher blobdelete.BlobDeletePublisher, transactor TransactWriter) *handler {
	return &handler{
		emailRepo:           emailRepo,
		stateRepo:           stateRepo,
		blobDeletePublisher: blobDeletePublisher,
		transactor:          transactor,
	}
}

// handle processes an SQS event containing mailbox cleanup messages.
func (h *handler) handle(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
	tracer := otel.Tracer("jmap-mailbox-cleanup")
	ctx, span := tracer.Start(ctx, "MailboxCleanupHandler")
	defer span.End()

	var failures []events.SQSBatchItemFailure

	for _, record := range event.Records {
		var msg mailboxcleanup.MailboxCleanupMessage
		if err := json.Unmarshal([]byte(record.Body), &msg); err != nil {
			logger.ErrorContext(ctx, "Failed to parse SQS message",
				slog.String("message_id", record.MessageId),
				slog.String("error", err.Error()),
			)
			failures = append(failures, events.SQSBatchItemFailure{
				ItemIdentifier: record.MessageId,
			})
			continue
		}

		if err := h.processMailboxCleanup(ctx, msg.AccountID, msg.MailboxID); err != nil {
			logger.ErrorContext(ctx, "Failed to process mailbox cleanup",
				slog.String("account_id", msg.AccountID),
				slog.String("mailbox_id", msg.MailboxID),
				slog.String("error", err.Error()),
			)
			failures = append(failures, events.SQSBatchItemFailure{
				ItemIdentifier: record.MessageId,
			})
		}
	}

	logger.InfoContext(ctx, "Mailbox cleanup batch completed",
		slog.Int("total", len(event.Records)),
		slog.Int("failures", len(failures)),
	)

	return events.SQSEventResponse{
		BatchItemFailures: failures,
	}, nil
}

// processMailboxCleanup handles cleanup for a single destroyed mailbox.
func (h *handler) processMailboxCleanup(ctx context.Context, accountID, mailboxID string) error {
	// Query all email IDs that were in this mailbox
	emailIDs, err := h.emailRepo.QueryEmailsByMailbox(ctx, accountID, mailboxID)
	if err != nil {
		return err
	}

	if len(emailIDs) == 0 {
		return nil
	}

	// Process each email individually (transactions have a 100-item limit)
	for _, emailID := range emailIDs {
		if err := h.processEmail(ctx, accountID, mailboxID, emailID); err != nil {
			return err
		}
	}

	return nil
}

// processEmail handles cleanup for a single email in the destroyed mailbox.
func (h *handler) processEmail(ctx context.Context, accountID, mailboxID, emailID string) error {
	emailItem, err := h.emailRepo.GetEmail(ctx, accountID, emailID)
	if err != nil {
		if err == email.ErrEmailNotFound {
			// Already deleted, skip
			return nil
		}
		return err
	}

	// Check if this mailbox is still in the email's mailboxIds
	// (might have been removed by a concurrent operation)
	if !emailItem.MailboxIDs[mailboxID] {
		return nil
	}

	isUnread := emailItem.Keywords == nil || !emailItem.Keywords["$seen"]

	if len(emailItem.MailboxIDs) == 1 {
		// Orphaned email — destroy it
		return h.destroyOrphanedEmail(ctx, accountID, mailboxID, emailItem, isUnread)
	}

	// Multi-mailbox email — remove the destroyed mailbox
	return h.removeMailboxFromEmail(ctx, accountID, mailboxID, emailItem, isUnread)
}

// destroyOrphanedEmail deletes an email that was only in the destroyed mailbox.
func (h *handler) destroyOrphanedEmail(ctx context.Context, accountID, mailboxID string, emailItem *email.EmailItem, isUnread bool) error {
	var transactItems []types.TransactWriteItem

	// Email + membership deletes
	transactItems = append(transactItems, h.emailRepo.BuildDeleteEmailItems(emailItem)...)

	// State changes
	if h.stateRepo != nil {
		emailState, err := h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeEmail)
		if err != nil {
			return err
		}
		_, emailStateItems := h.stateRepo.BuildStateChangeItems(accountID, state.ObjectTypeEmail, emailState, emailItem.EmailID, state.ChangeTypeDestroyed)
		transactItems = append(transactItems, emailStateItems...)

		threadState, err := h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeThread)
		if err != nil {
			return err
		}
		_, threadStateItems := h.stateRepo.BuildStateChangeItems(accountID, state.ObjectTypeThread, threadState, emailItem.ThreadID, state.ChangeTypeUpdated)
		transactItems = append(transactItems, threadStateItems...)
	}

	// Execute transaction
	_, err := h.transactor.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		return err
	}

	// Publish blob deletions (best-effort)
	blobIDs := collectBlobIDs(emailItem)
	if h.blobDeletePublisher != nil && len(blobIDs) > 0 {
		if err := h.blobDeletePublisher.PublishBlobDeletions(ctx, accountID, blobIDs); err != nil {
			logger.ErrorContext(ctx, "Failed to publish blob deletions",
				slog.String("account_id", accountID),
				slog.String("email_id", emailItem.EmailID),
				slog.String("error", err.Error()),
			)
		}
	}

	return nil
}

// removeMailboxFromEmail removes the destroyed mailbox from an email that belongs to multiple mailboxes.
func (h *handler) removeMailboxFromEmail(ctx context.Context, accountID, mailboxID string, emailItem *email.EmailItem, isUnread bool) error {
	// Build new mailboxIDs without the destroyed mailbox
	newMailboxIDs := make(map[string]bool)
	for k, v := range emailItem.MailboxIDs {
		if k != mailboxID {
			newMailboxIDs[k] = v
		}
	}

	var transactItems []types.TransactWriteItem

	_, _, emailItems := h.emailRepo.BuildUpdateEmailMailboxesItems(emailItem, newMailboxIDs)
	transactItems = append(transactItems, emailItems...)

	// State changes
	if h.stateRepo != nil {
		emailState, err := h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeEmail)
		if err != nil {
			return err
		}
		_, emailStateItems := h.stateRepo.BuildStateChangeItems(accountID, state.ObjectTypeEmail, emailState, emailItem.EmailID, state.ChangeTypeUpdated)
		transactItems = append(transactItems, emailStateItems...)
	}

	// Execute transaction
	_, err := h.transactor.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	return err
}

// collectBlobIDs returns the root blob ID and any non-composite part blob IDs.
func collectBlobIDs(emailItem *email.EmailItem) []string {
	var ids []string
	ids = append(ids, emailItem.BlobID)
	collectPartBlobIDs(&emailItem.BodyStructure, &ids)
	return ids
}

// collectPartBlobIDs recursively walks the body structure collecting non-composite blob IDs.
func collectPartBlobIDs(part *email.BodyPart, ids *[]string) {
	if part.BlobID != "" && !strings.Contains(part.BlobID, ",") {
		*ids = append(*ids, part.BlobID)
	}
	for i := range part.SubParts {
		collectPartBlobIDs(&part.SubParts[i], ids)
	}
}

func main() {
	ctx := context.Background()

	tp, err := xrayconfig.NewTracerProvider(ctx)
	if err != nil {
		logger.Error("FATAL: Failed to initialize tracer provider", slog.String("error", err.Error()))
		panic(err)
	}
	otel.SetTracerProvider(tp)

	tableName := os.Getenv("EMAIL_TABLE_NAME")
	blobDeleteQueueURL := os.Getenv("BLOB_DELETE_QUEUE_URL")

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		logger.Error("FATAL: Failed to load AWS config", slog.String("error", err.Error()))
		panic(err)
	}

	// Instrument AWS SDK clients with OTel tracing
	otelaws.AppendMiddlewares(&cfg.APIOptions)

	dynamoClient := dynamodb.NewFromConfig(cfg)
	emailRepo := email.NewRepository(dynamoClient, tableName)
	stateRepo := state.NewRepository(dynamoClient, tableName, 7)

	var blobPub blobdelete.BlobDeletePublisher
	if blobDeleteQueueURL != "" {
		sqsClient := sqs.NewFromConfig(cfg)
		blobPub = blobdelete.NewSQSPublisher(sqsClient, blobDeleteQueueURL)
	}

	h := newHandler(emailRepo, stateRepo, blobPub, dynamoClient)
	lambda.Start(otellambda.InstrumentHandler(h.handle, xrayconfig.WithRecommendedOptions(tp)...))
}
