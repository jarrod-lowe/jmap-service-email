// Package main implements the Email/import Lambda handler.
package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/jarrod-lowe/jmap-service-libs/plugincontract"
	"github.com/jarrod-lowe/jmap-service-libs/dbclient"
	"github.com/jarrod-lowe/jmap-service-email/internal/blob"
	"github.com/jarrod-lowe/jmap-service-email/internal/blobdelete"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailbox"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
	"github.com/jarrod-lowe/jmap-service-libs/awsinit"
	"github.com/jarrod-lowe/jmap-service-libs/jmaperror"
	"github.com/jarrod-lowe/jmap-service-libs/logging"
	"github.com/jarrod-lowe/jmap-service-libs/tracing"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var logger = logging.New()

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
	FindByMessageID(ctx context.Context, accountID, messageID string) (*emailItem, error)
	BuildCreateEmailItems(email *emailItem) []types.TransactWriteItem
}

// MailboxRepository defines the interface for mailbox operations.
type MailboxRepository interface {
	MailboxExists(ctx context.Context, accountID, mailboxID string) (bool, error)
	IncrementCounts(ctx context.Context, accountID, mailboxID string, incrementUnread bool) error
	BuildIncrementCountsItems(accountID, mailboxID string, incrementUnread bool) types.TransactWriteItem
}

// StateRepository defines the interface for state tracking operations.
type StateRepository interface {
	IncrementStateAndLogChange(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error)
	GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
	BuildStateChangeItems(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem)
	BuildStateChangeItemsMulti(accountID string, objectType state.ObjectType, currentState int64, objectIDs []string, changeType state.ChangeType) (int64, []types.TransactWriteItem)
}

// BlobDeletePublisher publishes blob deletion requests to an async queue.
type BlobDeletePublisher interface {
	PublishBlobDeletions(ctx context.Context, accountID string, blobIDs []string, apiURL string) error
}

// TransactWriter defines the interface for DynamoDB transactional writes.
type TransactWriter interface {
	TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// handler implements the Email/import logic.
type handler struct {
	repo                EmailRepository
	blobClientFactory   func(baseURL string) (BlobStreamer, BlobUploader)
	mailboxRepo         MailboxRepository
	stateRepo           StateRepository
	blobDeletePublisher BlobDeletePublisher
	transactor          TransactWriter
}

// newHandler creates a new handler.
func newHandler(repo EmailRepository, blobClientFactory func(baseURL string) (BlobStreamer, BlobUploader), mailboxRepo MailboxRepository, stateRepo StateRepository, opts ...any) *handler {
	h := &handler{
		repo:              repo,
		blobClientFactory: blobClientFactory,
		mailboxRepo:       mailboxRepo,
		stateRepo:         stateRepo,
	}
	for _, opt := range opts {
		switch v := opt.(type) {
		case BlobDeletePublisher:
			h.blobDeletePublisher = v
		case TransactWriter:
			h.transactor = v
		}
	}
	return h
}

// handle processes an Email/import request.
func (h *handler) handle(ctx context.Context, request plugincontract.PluginInvocationRequest) (plugincontract.PluginInvocationResponse, error) {
	tracer := tracing.Tracer("jmap-email-import")
	ctx, span := tracer.Start(ctx, "EmailImportHandler")
	defer span.End()

	// Check method
	if request.Method != "Email/import" {
		return plugincontract.PluginInvocationResponse{
			MethodResponse: plugincontract.MethodResponse{
				Name:     "error",
				Args:     jmaperror.UnknownMethod("This handler only supports Email/import").ToMap(),
				ClientID: request.ClientID,
			},
		}, nil
	}

	// Parse request args
	accountID := request.Args.StringOr("accountId", request.AccountID)

	emailsArg, ok := request.Args.Object("emails")
	if !ok {
		return plugincontract.PluginInvocationResponse{
			MethodResponse: plugincontract.MethodResponse{
				Name:     "error",
				Args:     jmaperror.InvalidArguments("emails argument must be an object").ToMap(),
				ClientID: request.ClientID,
			},
		}, nil
	}

	// Create blob client from request APIURL
	var streamer BlobStreamer
	var uploader BlobUploader
	if h.blobClientFactory != nil && request.APIURL != "" {
		streamer, uploader = h.blobClientFactory(request.APIURL)
	}

	created := make(map[string]any)
	notCreated := make(map[string]any)

	// Process each email
	for clientRef, emailArg := range emailsArg {
		emailMap, ok := emailArg.(map[string]any)
		if !ok {
			notCreated[clientRef] = jmaperror.InvalidProperties("email entry must be an object", nil).ToMap()
			continue
		}

		result, err := h.importEmail(ctx, accountID, emailMap, streamer, uploader, request.APIURL)
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
func (h *handler) importEmail(ctx context.Context, accountID string, emailArgs map[string]any, streamer BlobStreamer, uploader BlobUploader, apiURL string) (map[string]any, map[string]any) {
	// Extract required blobId
	blobID, ok := emailArgs["blobId"].(string)
	if !ok || blobID == "" {
		return nil, jmaperror.InvalidProperties("blobId is required", nil).ToMap()
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
			return nil, jmaperror.SetServerFail(err.Error()).ToMap()
		}
		if !exists {
			return nil, jmaperror.InvalidMailboxId("Mailbox does not exist: " + mailboxID).ToMap()
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
	stream, err := streamer.Stream(ctx, accountID, blobID)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to stream blob",
			slog.String("account_id", accountID),
			slog.String("blob_id", blobID),
			slog.String("error", err.Error()),
		)
		if errors.Is(err, blob.ErrBlobNotFound) {
			return nil, jmaperror.BlobNotFound(err.Error()).ToMap()
		}
		return nil, jmaperror.SetServerFail(err.Error()).ToMap()
	}
	defer stream.Close()

	// Parse email with streaming parser
	parsed, err := email.ParseRFC5322Stream(ctx, stream, blobID, accountID, uploader)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to parse email",
			slog.String("account_id", accountID),
			slog.String("blob_id", blobID),
			slog.String("error", err.Error()),
		)
		return nil, jmaperror.InvalidEmail(err.Error()).ToMap()
	}

	// Generate email ID
	emailID := uuid.New().String()

	// Determine thread ID based on In-Reply-To header
	threadID := h.determineThreadID(ctx, accountID, parsed.InReplyTo, emailID)

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
		HeaderSize:    parsed.HeaderSize,
		HasAttachment: parsed.HasAttachment,
		Subject:       parsed.Subject,
		From:          parsed.From,
		Sender:        parsed.Sender,
		To:            parsed.To,
		CC:            parsed.CC,
		Bcc:           parsed.Bcc,
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
		Version:       1,
	}

	// If transactor is available, use atomic transaction
	if h.transactor != nil {
		if err := h.importEmailTransactional(ctx, accountID, emailItem, threadID, keywords); err != nil {
			logger.ErrorContext(ctx, "Failed to store email transactionally",
				slog.String("account_id", accountID),
				slog.String("email_id", emailID),
				slog.String("error", err.Error()),
			)
			// Clean up uploaded part blobs on failure
			h.publishBlobCleanup(ctx, accountID, &parsed.BodyStructure, apiURL)
			return nil, jmaperror.SetServerFail(err.Error()).ToMap()
		}
	} else {
		// Fallback to non-transactional (legacy) path
		// Store in repository
		if err := h.repo.CreateEmail(ctx, emailItem); err != nil {
			logger.ErrorContext(ctx, "Failed to store email",
				slog.String("account_id", accountID),
				slog.String("email_id", emailID),
				slog.String("error", err.Error()),
			)
			// Clean up uploaded part blobs on failure
			h.publishBlobCleanup(ctx, accountID, &parsed.BodyStructure, apiURL)
			return nil, jmaperror.SetServerFail(err.Error()).ToMap()
		}

		// Track state changes for Email (created) and Thread (updated)
		if h.stateRepo != nil {
			if _, err := h.stateRepo.IncrementStateAndLogChange(ctx, accountID, state.ObjectTypeEmail, emailID, state.ChangeTypeCreated); err != nil {
				// Log but don't fail - email was already stored successfully
				logger.ErrorContext(ctx, "Failed to track email state change",
					slog.String("account_id", accountID),
					slog.String("email_id", emailID),
					slog.String("error", err.Error()),
				)
			}

			// Determine if this is a new thread or existing thread
			// New thread: threadID == emailID (no parent found or no In-Reply-To)
			// Existing thread: threadID != emailID (inherited from parent)
			threadChangeType := state.ChangeTypeUpdated
			if threadID == emailID {
				threadChangeType = state.ChangeTypeCreated
			}
			if _, err := h.stateRepo.IncrementStateAndLogChange(ctx, accountID, state.ObjectTypeThread, threadID, threadChangeType); err != nil {
				logger.ErrorContext(ctx, "Failed to track thread state change",
					slog.String("account_id", accountID),
					slog.String("thread_id", threadID),
					slog.String("error", err.Error()),
				)
			}
		}

		// Increment mailbox counts and track state changes
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

			// Track state changes for Mailbox (updated)
			if h.stateRepo != nil {
				if _, err := h.stateRepo.IncrementStateAndLogChange(ctx, accountID, state.ObjectTypeMailbox, mailboxID, state.ChangeTypeUpdated); err != nil {
					logger.ErrorContext(ctx, "Failed to track mailbox state change",
						slog.String("account_id", accountID),
						slog.String("mailbox_id", mailboxID),
						slog.String("error", err.Error()),
					)
				}
			}
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

// importEmailTransactional imports an email using a single atomic transaction.
// All writes (email creation, state updates, mailbox counters) are bundled into one transaction.
func (h *handler) importEmailTransactional(ctx context.Context, accountID string, emailItem *email.EmailItem, threadID string, keywords map[string]bool) error {
	var transactItems []types.TransactWriteItem

	// Email creation items (email + membership records)
	transactItems = append(transactItems, h.repo.BuildCreateEmailItems(emailItem)...)

	// Read current states for Email, Thread, Mailbox types
	emailState, err := h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeEmail)
	if err != nil {
		return err
	}
	threadState, err := h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeThread)
	if err != nil {
		return err
	}
	mailboxState, err := h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeMailbox)
	if err != nil {
		return err
	}

	// Email state change (created)
	_, emailStateItems := h.stateRepo.BuildStateChangeItems(accountID, state.ObjectTypeEmail, emailState, emailItem.EmailID, state.ChangeTypeCreated)
	transactItems = append(transactItems, emailStateItems...)

	// Thread state change (created if new thread, updated if joining existing)
	threadChangeType := state.ChangeTypeUpdated
	if threadID == emailItem.EmailID {
		threadChangeType = state.ChangeTypeCreated
	}
	_, threadStateItems := h.stateRepo.BuildStateChangeItems(accountID, state.ObjectTypeThread, threadState, threadID, threadChangeType)
	transactItems = append(transactItems, threadStateItems...)

	// Mailbox counter increments + state changes
	isSeen := keywords["$seen"]
	incrementUnread := !isSeen
	mailboxIDs := make([]string, 0, len(emailItem.MailboxIDs))
	for mailboxID := range emailItem.MailboxIDs {
		// Counter increment
		transactItems = append(transactItems, h.mailboxRepo.BuildIncrementCountsItems(accountID, mailboxID, incrementUnread))
		mailboxIDs = append(mailboxIDs, mailboxID)
	}
	_, mailboxStateItems := h.stateRepo.BuildStateChangeItemsMulti(accountID, state.ObjectTypeMailbox, mailboxState, mailboxIDs, state.ChangeTypeUpdated)
	transactItems = append(transactItems, mailboxStateItems...)

	// Execute transaction
	_, err = h.transactor.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		return err
	}

	return nil
}

// publishBlobCleanup publishes blob IDs from a body structure for async deletion.
func (h *handler) publishBlobCleanup(ctx context.Context, accountID string, bodyStructure *email.BodyPart, apiURL string) {
	if h.blobDeletePublisher == nil {
		return
	}
	var blobIDs []string
	collectPartBlobIDs(bodyStructure, &blobIDs)
	if len(blobIDs) == 0 {
		return
	}
	if err := h.blobDeletePublisher.PublishBlobDeletions(ctx, accountID, blobIDs, apiURL); err != nil {
		logger.ErrorContext(ctx, "Failed to publish blob cleanup",
			slog.String("account_id", accountID),
			slog.String("error", err.Error()),
		)
	}
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

// determineThreadID determines the thread ID for an email.
// If the email has an In-Reply-To header and the parent email is found,
// the parent's thread ID is used. Otherwise, a new UUID is generated.
func (h *handler) determineThreadID(ctx context.Context, accountID string, inReplyTo []string, fallbackID string) string {
	// If no In-Reply-To header, this starts a new thread
	if len(inReplyTo) == 0 {
		return fallbackID
	}

	// Look up the parent email by its Message-ID
	parentMessageID := inReplyTo[0]
	parent, err := h.repo.FindByMessageID(ctx, accountID, parentMessageID)
	if err != nil {
		// Log error but don't fail - fall back to new thread
		logger.WarnContext(ctx, "Failed to look up parent thread",
			slog.String("account_id", accountID),
			slog.String("in_reply_to", parentMessageID),
			slog.String("error", err.Error()),
		)
		return fallbackID
	}

	if parent != nil && parent.ThreadID != "" {
		return parent.ThreadID
	}

	// Parent not found, start a new thread
	return fallbackID
}

func main() {
	ctx := context.Background()

	result, err := awsinit.Init(ctx)
	if err != nil {
		logger.Error("FATAL: Failed to initialize", slog.String("error", err.Error()))
		panic(err)
	}

	// Load config from environment
	tableName := os.Getenv("EMAIL_TABLE_NAME")

	// Create DynamoDB client
	dynamoClient := dbclient.NewClient(result.Config)

	// Warm the DynamoDB connection during init
	// This establishes TCP+TLS connection before first real request
	warmCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	_, _ = dynamoClient.GetItem(warmCtx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "WARMUP"},
			"sk": &types.AttributeValueMemberS{Value: "WARMUP"},
		},
	})
	cancel()

	repo := email.NewRepository(dynamoClient, tableName)
	mailboxRepo := mailbox.NewDynamoDBRepository(dynamoClient, tableName)
	stateRepo := state.NewRepository(dynamoClient, tableName, 7)

	// Create HTTP clients: signed for IAM-authenticated API calls, plain for presigned URL PUTs
	baseTransport := otelhttp.NewTransport(http.DefaultTransport)
	sigv4Transport := blob.NewSigV4Transport(baseTransport, result.Config.Credentials, result.Config.Region)
	signedHTTPClient := &http.Client{Transport: sigv4Transport}
	plainHTTPClient := &http.Client{Transport: baseTransport}

	factory := func(baseURL string) (BlobStreamer, BlobUploader) {
		streamer := blob.NewHTTPBlobClient(baseURL, signedHTTPClient)
		uploader := blob.NewPresignedUploadClient(baseURL, signedHTTPClient, plainHTTPClient)
		return streamer, uploader
	}

	// Set up blob delete publisher
	blobDeleteQueueURL := os.Getenv("BLOB_DELETE_QUEUE_URL")
	var blobPub BlobDeletePublisher
	if blobDeleteQueueURL != "" {
		sqsClient := sqs.NewFromConfig(result.Config)
		blobPub = blobdelete.NewSQSPublisher(sqsClient, blobDeleteQueueURL)
	}

	// Pass dynamoClient as TransactWriter for atomic operations
	h := newHandler(repo, factory, mailboxRepo, stateRepo, dynamoClient, blobPub)
	result.Start(h.handle)
}
