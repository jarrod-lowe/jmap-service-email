// Package main implements the Email/import Lambda handler.
package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/blob"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailbox"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda/xrayconfig"
	"go.opentelemetry.io/otel"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

// emailItem is an alias for the internal email.EmailItem type.
type emailItem = email.EmailItem

// BlobStreamer defines the interface for streaming blobs.
type BlobStreamer interface {
	Stream(ctx context.Context, accountID, blobID string) (io.ReadCloser, error)
}

// BlobUploader defines the interface for uploading blobs.
type BlobUploader interface {
	Upload(ctx context.Context, accountID, parentBlobID, contentType string, body io.Reader) (blobID string, size int64, err error)
}

// EmailRepository defines the interface for storing emails.
type EmailRepository interface {
	CreateEmail(ctx context.Context, email *emailItem) error
}

// MailboxRepository defines the interface for mailbox operations.
type MailboxRepository interface {
	MailboxExists(ctx context.Context, accountID, mailboxID string) (bool, error)
	IncrementCounts(ctx context.Context, accountID, mailboxID string, incrementUnread bool) error
}

// handler implements the Email/import logic.
type handler struct {
	repo        EmailRepository
	streamer    BlobStreamer
	uploader    BlobUploader
	mailboxRepo MailboxRepository
}

// newHandler creates a new handler.
func newHandler(repo EmailRepository, streamer BlobStreamer, uploader BlobUploader, mailboxRepo MailboxRepository) *handler {
	return &handler{
		repo:        repo,
		streamer:    streamer,
		uploader:    uploader,
		mailboxRepo: mailboxRepo,
	}
}

// handle processes an Email/import request.
func (h *handler) handle(ctx context.Context, request plugincontract.PluginInvocationRequest) (plugincontract.PluginInvocationResponse, error) {
	tracer := otel.Tracer("jmap-email-import")
	ctx, span := tracer.Start(ctx, "EmailImportHandler")
	defer span.End()

	// Check method
	if request.Method != "Email/import" {
		return plugincontract.PluginInvocationResponse{
			MethodResponse: plugincontract.MethodResponse{
				Name: "error",
				Args: map[string]any{
					"type":        "unknownMethod",
					"description": "This handler only supports Email/import",
				},
				ClientID: request.ClientID,
			},
		}, nil
	}

	// Parse request args
	accountID := request.AccountID
	if argAccountID, ok := request.Args["accountId"].(string); ok {
		accountID = argAccountID
	}

	emailsArg, ok := request.Args["emails"].(map[string]any)
	if !ok {
		return plugincontract.PluginInvocationResponse{
			MethodResponse: plugincontract.MethodResponse{
				Name: "error",
				Args: map[string]any{
					"type":        "invalidArguments",
					"description": "emails argument must be an object",
				},
				ClientID: request.ClientID,
			},
		}, nil
	}

	created := make(map[string]any)
	notCreated := make(map[string]any)

	// Process each email
	for clientRef, emailArg := range emailsArg {
		emailMap, ok := emailArg.(map[string]any)
		if !ok {
			notCreated[clientRef] = map[string]any{
				"type":        "invalidArguments",
				"description": "email entry must be an object",
			}
			continue
		}

		result, err := h.importEmail(ctx, accountID, emailMap)
		if err != nil {
			notCreated[clientRef] = err
		} else {
			created[clientRef] = result
		}
	}

	return plugincontract.PluginInvocationResponse{
		MethodResponse: plugincontract.MethodResponse{
			Name: "Email/import",
			Args: map[string]any{
				"accountId":  accountID,
				"created":    created,
				"notCreated": notCreated,
			},
			ClientID: request.ClientID,
		},
	}, nil
}

// importEmail imports a single email and returns the created email info or an error map.
func (h *handler) importEmail(ctx context.Context, accountID string, emailArgs map[string]any) (map[string]any, map[string]any) {
	// Extract required blobId
	blobID, ok := emailArgs["blobId"].(string)
	if !ok || blobID == "" {
		return nil, map[string]any{
			"type":        "invalidArguments",
			"description": "blobId is required",
		}
	}

	// Extract mailboxIds
	mailboxIDs := make(map[string]bool)
	if mailboxArg, ok := emailArgs["mailboxIds"].(map[string]any); ok {
		for k, v := range mailboxArg {
			if b, ok := v.(bool); ok && b {
				mailboxIDs[k] = true
			}
		}
	}

	// Validate all mailboxIds exist
	for mailboxID := range mailboxIDs {
		exists, err := h.mailboxRepo.MailboxExists(ctx, accountID, mailboxID)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to check mailbox existence",
				slog.String("account_id", accountID),
				slog.String("mailbox_id", mailboxID),
				slog.String("error", err.Error()),
			)
			return nil, map[string]any{
				"type":        "serverFail",
				"description": err.Error(),
			}
		}
		if !exists {
			return nil, map[string]any{
				"type":        "invalidMailboxId",
				"description": "Mailbox does not exist: " + mailboxID,
			}
		}
	}

	// Extract keywords
	keywords := make(map[string]bool)
	if keywordArg, ok := emailArgs["keywords"].(map[string]any); ok {
		for k, v := range keywordArg {
			if b, ok := v.(bool); ok && b {
				keywords[k] = true
			}
		}
	}

	// Extract receivedAt (optional)
	var receivedAt time.Time
	if receivedAtStr, ok := emailArgs["receivedAt"].(string); ok {
		if t, err := time.Parse(time.RFC3339, receivedAtStr); err == nil {
			receivedAt = t
		}
	}
	if receivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}

	// Stream blob
	stream, err := h.streamer.Stream(ctx, accountID, blobID)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to stream blob",
			slog.String("account_id", accountID),
			slog.String("blob_id", blobID),
			slog.String("error", err.Error()),
		)
		errorType := "serverFail"
		if errors.Is(err, blob.ErrBlobNotFound) {
			errorType = "blobNotFound"
		}
		return nil, map[string]any{
			"type":        errorType,
			"description": err.Error(),
		}
	}
	defer stream.Close()

	// Parse email with streaming parser
	parsed, err := email.ParseRFC5322Stream(ctx, stream, blobID, accountID, h.uploader)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to parse email",
			slog.String("account_id", accountID),
			slog.String("blob_id", blobID),
			slog.String("error", err.Error()),
		)
		return nil, map[string]any{
			"type":        "invalidEmail",
			"description": err.Error(),
		}
	}

	// Generate IDs
	emailID := uuid.New().String()
	threadID := emailID // Threading deferred

	// Create email item
	emailItem := &email.EmailItem{
		AccountID:     accountID,
		EmailID:       emailID,
		BlobID:        blobID,
		ThreadID:      threadID,
		MailboxIDs:    mailboxIDs,
		Keywords:      keywords,
		ReceivedAt:    receivedAt,
		Size:          parsed.Size,
		HasAttachment: parsed.HasAttachment,
		Subject:       parsed.Subject,
		From:          parsed.From,
		To:            parsed.To,
		CC:            parsed.CC,
		ReplyTo:       parsed.ReplyTo,
		SentAt:        parsed.SentAt,
		MessageID:     parsed.MessageID,
		InReplyTo:     parsed.InReplyTo,
		References:    parsed.References,
		Preview:       parsed.Preview,
		BodyStructure: parsed.BodyStructure,
		TextBody:      parsed.TextBody,
		HTMLBody:      parsed.HTMLBody,
		Attachments:   parsed.Attachments,
	}

	// Store in repository
	if err := h.repo.CreateEmail(ctx, emailItem); err != nil {
		logger.ErrorContext(ctx, "Failed to store email",
			slog.String("account_id", accountID),
			slog.String("email_id", emailID),
			slog.String("error", err.Error()),
		)
		return nil, map[string]any{
			"type":        "serverFail",
			"description": err.Error(),
		}
	}

	// Increment mailbox counts
	isSeen := keywords["$seen"]
	incrementUnread := !isSeen
	for mailboxID := range mailboxIDs {
		if err := h.mailboxRepo.IncrementCounts(ctx, accountID, mailboxID, incrementUnread); err != nil {
			// Log but don't fail - email was already stored successfully
			logger.ErrorContext(ctx, "Failed to increment mailbox counts",
				slog.String("account_id", accountID),
				slog.String("mailbox_id", mailboxID),
				slog.String("error", err.Error()),
			)
		}
	}

	logger.InfoContext(ctx, "Email imported successfully",
		slog.String("account_id", accountID),
		slog.String("email_id", emailID),
		slog.String("blob_id", blobID),
	)

	return map[string]any{
		"id":       emailID,
		"blobId":   blobID,
		"threadId": threadID,
		"size":     parsed.Size,
	}, nil
}

func main() {
	ctx := context.Background()

	tp, err := xrayconfig.NewTracerProvider(ctx)
	if err != nil {
		logger.Error("FATAL: Failed to initialize tracer provider", slog.String("error", err.Error()))
		panic(err)
	}
	otel.SetTracerProvider(tp)

	// Load config from environment
	tableName := os.Getenv("EMAIL_TABLE_NAME")
	coreAPIURL := os.Getenv("CORE_API_URL")

	// Initialize AWS config
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		logger.Error("FATAL: Failed to load AWS config", slog.String("error", err.Error()))
		panic(err)
	}

	// Create DynamoDB client
	dynamoClient := dynamodb.NewFromConfig(cfg)
	repo := email.NewRepository(dynamoClient, tableName)
	mailboxRepo := mailbox.NewDynamoDBRepository(dynamoClient, tableName)

	// Create blob client with SigV4 signing
	transport := blob.NewSigV4Transport(http.DefaultTransport, cfg.Credentials, cfg.Region)
	httpClient := &http.Client{Transport: transport}
	blobClient := blob.NewHTTPBlobClient(coreAPIURL, httpClient)

	// HTTPBlobClient implements both BlobStreamer and BlobUploader
	h := newHandler(repo, blobClient, blobClient, mailboxRepo)
	lambda.Start(otellambda.InstrumentHandler(h.handle, xrayconfig.WithRecommendedOptions(tp)...))
}
