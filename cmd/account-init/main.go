// Package main implements the account-init SQS consumer Lambda handler.
// This Lambda listens to account.created events and provisions special mailboxes.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailbox"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda/xrayconfig"
	"go.opentelemetry.io/otel"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

// EventPayload represents the account event from jmap-service-core.
type EventPayload struct {
	EventType  string         `json:"eventType"`
	OccurredAt string         `json:"occurredAt"`
	AccountID  string         `json:"accountId"`
	Data       map[string]any `json:"data,omitempty"`
}

// SpecialMailbox defines a special mailbox to be provisioned.
type SpecialMailbox struct {
	ID        string
	Name      string
	Role      string
	SortOrder int
}

// SpecialMailboxes defines the 6 special mailboxes to provision.
var SpecialMailboxes = []SpecialMailbox{
	{ID: "inbox", Name: "Inbox", Role: "inbox", SortOrder: 0},
	{ID: "drafts", Name: "Drafts", Role: "drafts", SortOrder: 1},
	{ID: "sent", Name: "Sent", Role: "sent", SortOrder: 2},
	{ID: "trash", Name: "Trash", Role: "trash", SortOrder: 3},
	{ID: "junk", Name: "Junk", Role: "junk", SortOrder: 4},
	{ID: "archive", Name: "Archive", Role: "archive", SortOrder: 5},
}

// MailboxRepository defines the interface for mailbox operations.
type MailboxRepository interface {
	BuildCreateMailboxItem(mbox *mailbox.MailboxItem) types.TransactWriteItem
	GetMailbox(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error)
}

// StateRepository defines the interface for state tracking operations.
type StateRepository interface {
	GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
	BuildStateChangeItemsMulti(accountID string, objectType state.ObjectType, currentState int64, objectIDs []string, changeType state.ChangeType) (int64, []types.TransactWriteItem)
}

// TransactWriter executes DynamoDB transactions.
type TransactWriter interface {
	TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// handler implements the account-init SQS consumer logic.
type handler struct {
	mailboxRepo MailboxRepository
	stateRepo   StateRepository
	transactor  TransactWriter
}

// newHandler creates a new handler.
func newHandler(mailboxRepo MailboxRepository, stateRepo StateRepository, transactor TransactWriter) *handler {
	return &handler{
		mailboxRepo: mailboxRepo,
		stateRepo:   stateRepo,
		transactor:  transactor,
	}
}

// handle processes an SQS event containing account event messages.
func (h *handler) handle(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
	tracer := otel.Tracer("jmap-account-init")
	ctx, span := tracer.Start(ctx, "AccountInitHandler")
	defer span.End()

	var failures []events.SQSBatchItemFailure

	for _, record := range event.Records {
		var payload EventPayload
		if err := json.Unmarshal([]byte(record.Body), &payload); err != nil {
			logger.ErrorContext(ctx, "Failed to parse SQS message",
				slog.String("message_id", record.MessageId),
				slog.String("error", err.Error()),
			)
			failures = append(failures, events.SQSBatchItemFailure{
				ItemIdentifier: record.MessageId,
			})
			continue
		}

		if payload.EventType != "account.created" {
			logger.InfoContext(ctx, "Ignoring non-account.created event",
				slog.String("event_type", payload.EventType),
				slog.String("account_id", payload.AccountID),
			)
			continue
		}

		if err := h.provisionMailboxes(ctx, payload.AccountID); err != nil {
			logger.ErrorContext(ctx, "Failed to provision mailboxes",
				slog.String("account_id", payload.AccountID),
				slog.String("error", err.Error()),
			)
			failures = append(failures, events.SQSBatchItemFailure{
				ItemIdentifier: record.MessageId,
			})
		}
	}

	logger.InfoContext(ctx, "Account init batch completed",
		slog.Int("total", len(event.Records)),
		slog.Int("failures", len(failures)),
	)

	return events.SQSEventResponse{
		BatchItemFailures: failures,
	}, nil
}

// provisionMailboxes creates all 6 special mailboxes for a new account.
func (h *handler) provisionMailboxes(ctx context.Context, accountID string) error {
	now := time.Now().UTC()

	// Check which mailboxes need to be created (idempotency check)
	var mailboxesToCreate []*mailbox.MailboxItem
	var mailboxIDs []string

	for _, special := range SpecialMailboxes {
		_, err := h.mailboxRepo.GetMailbox(ctx, accountID, special.ID)
		if err == nil {
			// Mailbox already exists, skip
			continue
		}
		if err != mailbox.ErrMailboxNotFound {
			// Unexpected error
			return err
		}

		// Mailbox doesn't exist, add to creation list
		mbox := &mailbox.MailboxItem{
			AccountID:    accountID,
			MailboxID:    special.ID,
			Name:         special.Name,
			Role:         special.Role,
			SortOrder:    special.SortOrder,
			TotalEmails:  0,
			UnreadEmails: 0,
			IsSubscribed: true,
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		mailboxesToCreate = append(mailboxesToCreate, mbox)
		mailboxIDs = append(mailboxIDs, special.ID)
	}

	// If all mailboxes exist, nothing to do
	if len(mailboxesToCreate) == 0 {
		logger.InfoContext(ctx, "All mailboxes already exist",
			slog.String("account_id", accountID),
		)
		return nil
	}

	// Build transaction items for mailbox creation
	var transactItems []types.TransactWriteItem
	for _, mbox := range mailboxesToCreate {
		transactItems = append(transactItems, h.mailboxRepo.BuildCreateMailboxItem(mbox))
	}

	// Build state tracking items
	if h.stateRepo != nil {
		currentState, err := h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeMailbox)
		if err != nil {
			return err
		}
		_, stateItems := h.stateRepo.BuildStateChangeItemsMulti(
			accountID,
			state.ObjectTypeMailbox,
			currentState,
			mailboxIDs,
			state.ChangeTypeCreated,
		)
		transactItems = append(transactItems, stateItems...)
	}

	// Execute transaction
	_, err := h.transactor.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		return err
	}

	logger.InfoContext(ctx, "Provisioned special mailboxes",
		slog.String("account_id", accountID),
		slog.Int("count", len(mailboxesToCreate)),
	)

	return nil
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

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		logger.Error("FATAL: Failed to load AWS config", slog.String("error", err.Error()))
		panic(err)
	}

	// Instrument AWS SDK clients with OTel tracing
	otelaws.AppendMiddlewares(&cfg.APIOptions)

	dynamoClient := dynamodb.NewFromConfig(cfg)
	mailboxRepo := mailbox.NewDynamoDBRepository(dynamoClient, tableName)
	stateRepo := state.NewRepository(dynamoClient, tableName, 7)

	h := newHandler(mailboxRepo, stateRepo, dynamoClient)
	lambda.Start(otellambda.InstrumentHandler(h.handle, xrayconfig.WithRecommendedOptions(tp)...))
}
