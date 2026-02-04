// Package main implements the email-cleanup DynamoDB Streams handler.
// It triggers on MODIFY events where deletedAt was added, performing actual
// record deletion (email + membership records) and blob cleanup publishing.
package main

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/jarrod-lowe/jmap-service-libs/logging"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	"github.com/jarrod-lowe/jmap-service-email/internal/blobdelete"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-libs/tracing"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda/xrayconfig"
	"go.opentelemetry.io/otel"
)

var logger = logging.New()

// EmailRepository defines the interface for email operations needed by cleanup.
type EmailRepository interface {
	GetEmail(ctx context.Context, accountID, emailID string) (*email.EmailItem, error)
	BuildDeleteEmailItems(emailItem *email.EmailItem) []types.TransactWriteItem
}

// TransactWriter executes DynamoDB transactions.
type TransactWriter interface {
	TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// handler implements the email-cleanup stream consumer logic.
type handler struct {
	emailRepo           EmailRepository
	blobDeletePublisher blobdelete.BlobDeletePublisher
	transactor          TransactWriter
}

// newHandler creates a new handler.
func newHandler(emailRepo EmailRepository, blobDeletePublisher blobdelete.BlobDeletePublisher, transactor TransactWriter) *handler {
	return &handler{
		emailRepo:           emailRepo,
		blobDeletePublisher: blobDeletePublisher,
		transactor:          transactor,
	}
}

// handle processes a DynamoDB Streams event.
func (h *handler) handle(ctx context.Context, event events.DynamoDBEvent) error {
	tracer := tracing.Tracer("jmap-email-cleanup")
	ctx, span := tracer.Start(ctx, "EmailCleanupHandler")
	defer span.End()

	for _, record := range event.Records {
		if record.EventName != "MODIFY" {
			continue
		}

		// Check if deletedAt was added (old image doesn't have it, new image does)
		oldImage := record.Change.OldImage
		newImage := record.Change.NewImage

		if _, hasOld := oldImage[email.AttrDeletedAt]; hasOld {
			continue // Already had deletedAt, not a new soft-delete
		}
		if _, hasNew := newImage[email.AttrDeletedAt]; !hasNew {
			continue // No deletedAt in new image
		}

		// Extract accountID and emailID from the record
		accountID := getStringAttr(newImage, email.AttrAccountID)
		emailID := getStringAttr(newImage, email.AttrEmailID)

		if accountID == "" || emailID == "" {
			continue
		}

		if err := h.processEmailCleanup(ctx, accountID, emailID); err != nil {
			logger.ErrorContext(ctx, "Failed to process email cleanup",
				slog.String("account_id", accountID),
				slog.String("email_id", emailID),
				slog.String("error", err.Error()),
			)
			return err // Fail the batch to retry
		}
	}

	return nil
}

// processEmailCleanup handles the actual deletion of an email and its related records.
func (h *handler) processEmailCleanup(ctx context.Context, accountID, emailID string) error {
	// Fetch the full email item
	emailItem, err := h.emailRepo.GetEmail(ctx, accountID, emailID)
	if err != nil {
		if err == email.ErrEmailNotFound {
			// Already cleaned up
			return nil
		}
		return err
	}

	// Delete email + membership records
	transactItems := h.emailRepo.BuildDeleteEmailItems(emailItem)
	if len(transactItems) > 0 {
		_, err = h.transactor.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: transactItems,
		})
		if err != nil {
			return err
		}
	}

	// Publish blob deletions (best-effort)
	blobIDs := collectBlobIDs(emailItem)
	if h.blobDeletePublisher != nil && len(blobIDs) > 0 {
		if err := h.blobDeletePublisher.PublishBlobDeletions(ctx, accountID, blobIDs); err != nil {
			logger.ErrorContext(ctx, "Failed to publish blob deletions",
				slog.String("account_id", accountID),
				slog.String("email_id", emailID),
				slog.String("error", err.Error()),
			)
		}
	}

	logger.InfoContext(ctx, "Email cleanup completed",
		slog.String("account_id", accountID),
		slog.String("email_id", emailID),
	)

	return nil
}

// getStringAttr extracts a string attribute from a DynamoDB stream image.
func getStringAttr(image map[string]events.DynamoDBAttributeValue, key string) string {
	if v, ok := image[key]; ok {
		return v.String()
	}
	return ""
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

	tp, err := tracing.Init(ctx)
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

	var blobPub blobdelete.BlobDeletePublisher
	if blobDeleteQueueURL != "" {
		sqsClient := sqs.NewFromConfig(cfg)
		blobPub = blobdelete.NewSQSPublisher(sqsClient, blobDeleteQueueURL)
	}

	h := newHandler(emailRepo, blobPub, dynamoClient)
	lambda.Start(otellambda.InstrumentHandler(h.handle, xrayconfig.WithRecommendedOptions(tp)...))
}
