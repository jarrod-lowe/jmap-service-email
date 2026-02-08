// Package main implements the email-index SQS consumer Lambda handler.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors"
	"github.com/jarrod-lowe/jmap-service-email/internal/blob"
	"github.com/jarrod-lowe/jmap-service-email/internal/embeddings"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/htmlstrip"
	"github.com/jarrod-lowe/jmap-service-email/internal/searchindex"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
	"github.com/jarrod-lowe/jmap-service-email/internal/summary"
	"github.com/jarrod-lowe/jmap-service-email/internal/vectorstore"
	"github.com/jarrod-lowe/jmap-service-libs/awsinit"
	"github.com/jarrod-lowe/jmap-service-libs/dbclient"
	"github.com/jarrod-lowe/jmap-service-libs/logging"
	"github.com/jarrod-lowe/jmap-service-libs/tracing"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var logger = logging.New()

const (
	// chunkSize is the target size in characters for each text chunk (~7500 tokens).
	chunkSize = 30000
	// chunkOverlap is the overlap between chunks in characters.
	chunkOverlap = 800
)

// EmailReader reads email records from DynamoDB.
type EmailReader interface {
	GetEmail(ctx context.Context, accountID, emailID string) (*email.EmailItem, error)
}

// EmailUpdater updates email records in DynamoDB.
type EmailUpdater interface {
	UpdateSearchChunks(ctx context.Context, accountID, emailID string, searchChunks int) error
	UpdateSummary(ctx context.Context, accountID, emailID, summary string, overwritePreview bool) error
}

// Summarizer generates a short summary of an email.
type Summarizer interface {
	Summarize(ctx context.Context, subject, from, bodyText string) (string, error)
}

// StateChanger records state changes for Email/changes tracking.
type StateChanger interface {
	IncrementStateAndLogChange(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error)
}

// BlobStreamer streams blob data.
type BlobStreamer interface {
	Stream(ctx context.Context, accountID, blobID string) (io.ReadCloser, error)
}

// EmbeddingClient generates vector embeddings.
type EmbeddingClient interface {
	GenerateEmbedding(ctx context.Context, text string) ([]float32, error)
}

// VectorStore manages vector storage.
type VectorStore interface {
	EnsureIndex(ctx context.Context, accountID string) error
	PutVector(ctx context.Context, accountID string, vector vectorstore.Vector) error
	DeleteVectors(ctx context.Context, accountID string, keys []string) error
}

// TokenWriter writes and deletes address search tokens.
type TokenWriter interface {
	WriteTokens(ctx context.Context, emailItem *email.EmailItem) error
	DeleteTokens(ctx context.Context, emailItem *email.EmailItem) error
}

// handler implements the email-index SQS consumer logic.
type handler struct {
	emailReader       EmailReader
	emailUpdater      EmailUpdater
	blobClientFactory func(baseURL string) BlobStreamer
	embedder          EmbeddingClient
	vectorStore       VectorStore
	tokenWriter       TokenWriter
	summarizer        Summarizer
	stateChanger      StateChanger
	overwritePreview  bool
}

// newHandler creates a new handler.
func newHandler(reader EmailReader, updater EmailUpdater, blobClientFactory func(baseURL string) BlobStreamer, embedder EmbeddingClient, store VectorStore, tokenWriter TokenWriter) *handler {
	return &handler{
		emailReader:       reader,
		emailUpdater:      updater,
		blobClientFactory: blobClientFactory,
		embedder:          embedder,
		vectorStore:       store,
		tokenWriter:       tokenWriter,
	}
}

// handle processes an SQS event containing search index messages.
func (h *handler) handle(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
	tracer := tracing.Tracer("jmap-email-index")
	ctx, span := tracer.Start(ctx, "EmailIndexHandler")
	defer span.End()

	var failures []events.SQSBatchItemFailure

	for _, record := range event.Records {
		var msg searchindex.Message
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

		var err error
		switch msg.Action {
		case searchindex.ActionIndex:
			err = h.indexEmail(ctx, msg)
		case searchindex.ActionDelete:
			err = h.deleteEmail(ctx, msg)
		default:
			logger.ErrorContext(ctx, "Unknown action",
				slog.String("action", string(msg.Action)),
				slog.String("email_id", msg.EmailID),
			)
			failures = append(failures, events.SQSBatchItemFailure{
				ItemIdentifier: record.MessageId,
			})
			continue
		}

		if err != nil {
			logger.ErrorContext(ctx, "Failed to process message",
				slog.String("action", string(msg.Action)),
				slog.String("email_id", msg.EmailID),
				slog.String("error", err.Error()),
			)
			failures = append(failures, events.SQSBatchItemFailure{
				ItemIdentifier: record.MessageId,
			})
		}
	}

	logger.InfoContext(ctx, "Email index batch completed",
		slog.Int("total", len(event.Records)),
		slog.Int("failures", len(failures)),
	)

	return events.SQSEventResponse{
		BatchItemFailures: failures,
	}, nil
}

// maxSummaryInput is the maximum body text chars captured for the summarizer.
const maxSummaryInput = 4000

// captureWriter captures the first maxBytes of data written through it.
type captureWriter struct {
	buf      []byte
	maxBytes int
}

func newCaptureWriter(maxBytes int) *captureWriter {
	return &captureWriter{maxBytes: maxBytes}
}

func (cw *captureWriter) Write(p []byte) (int, error) {
	remaining := cw.maxBytes - len(cw.buf)
	if remaining > 0 {
		n := len(p)
		if n > remaining {
			n = remaining
		}
		cw.buf = append(cw.buf, p[:n]...)
	}
	return len(p), nil
}

func (cw *captureWriter) String() string {
	return string(cw.buf)
}

// indexEmail indexes a single email: fetches text parts, chunks, embeds, stores vectors,
// writes address tokens, generates summary, and creates subject/summary vectors.
func (h *handler) indexEmail(ctx context.Context, msg searchindex.Message) error {
	// Read email from DynamoDB
	emailItem, err := h.emailReader.GetEmail(ctx, msg.AccountID, msg.EmailID)
	if err != nil {
		if errors.Is(err, email.ErrEmailNotFound) {
			// Email was deleted before we could index it — not an error
			logger.InfoContext(ctx, "Email not found for indexing, skipping",
				slog.String("email_id", msg.EmailID),
			)
			return nil
		}
		return fmt.Errorf("get email: %w", err)
	}

	// Write address tokens to DynamoDB
	if h.tokenWriter != nil {
		if err := h.tokenWriter.WriteTokens(ctx, emailItem); err != nil {
			return fmt.Errorf("write tokens: %w", err)
		}
	}

	// Determine which parts to index: prefer textBody, fall back to htmlBody
	partIDs := emailItem.TextBody
	isHTML := false
	if len(partIDs) == 0 {
		partIDs = emailItem.HTMLBody
		isHTML = true
	}
	if len(partIDs) == 0 {
		// No text content to index, but tokens were still written
		logger.InfoContext(ctx, "No text parts to index",
			slog.String("email_id", msg.EmailID),
		)
		return nil
	}

	// Ensure vector index exists
	if err := h.vectorStore.EnsureIndex(ctx, msg.AccountID); err != nil {
		return fmt.Errorf("ensure index: %w", err)
	}

	// Build header prefix
	headerPrefix := buildHeaderPrefix(emailItem)

	// Build metadata for vectors (body type)
	metadata := buildVectorMetadata(emailItem, "body")

	// Process parts and generate body vectors, capturing text for summarization
	chunkIndex := 0
	var streamer BlobStreamer
	if h.blobClientFactory != nil {
		streamer = h.blobClientFactory(msg.APIURL)
	}

	capture := newCaptureWriter(maxSummaryInput)

	for _, partID := range partIDs {
		if streamer == nil {
			return fmt.Errorf("no blob client available")
		}

		part := email.FindBodyPart(emailItem.BodyStructure, partID)
		if part == nil || part.BlobID == "" {
			logger.WarnContext(ctx, "Skipping part with no blob",
				slog.String("email_id", msg.EmailID),
				slog.String("part_id", partID),
			)
			continue
		}

		stream, err := streamer.Stream(ctx, msg.AccountID, part.BlobID)
		if err != nil {
			return fmt.Errorf("stream blob %s: %w", part.BlobID, err)
		}

		var textReader io.Reader = stream
		if isHTML {
			textReader = htmlstrip.NewReader(stream)
		}

		// Tee the stream to capture text for summarization
		textReader = io.TeeReader(textReader, capture)

		processed, err := h.processPartStream(ctx, msg.AccountID, msg.EmailID, headerPrefix, metadata, textReader, chunkIndex)
		stream.Close()
		if err != nil {
			return fmt.Errorf("process part %s: %w", part.BlobID, err)
		}
		chunkIndex += processed
	}

	// Generate AI summary (best-effort)
	h.generateSummary(ctx, msg.AccountID, msg.EmailID, emailItem, capture.String())

	// Create subject vector
	if emailItem.Subject != "" {
		if err := h.indexSubjectVector(ctx, msg.AccountID, emailItem); err != nil {
			return fmt.Errorf("index subject: %w", err)
		}
	}

	// Create summary vector
	if emailItem.Summary != "" {
		if err := h.indexSummaryVector(ctx, msg.AccountID, emailItem); err != nil {
			return fmt.Errorf("index summary: %w", err)
		}
	}

	// Update email record with chunk count
	if err := h.emailUpdater.UpdateSearchChunks(ctx, msg.AccountID, msg.EmailID, chunkIndex); err != nil {
		return fmt.Errorf("update search chunks: %w", err)
	}

	logger.InfoContext(ctx, "Email indexed successfully",
		slog.String("email_id", msg.EmailID),
		slog.Int("chunks", chunkIndex),
		slog.String("summary", emailItem.Summary),
	)
	return nil
}

// generateSummary calls the summarizer and persists the result. Failures are logged and ignored.
func (h *handler) generateSummary(ctx context.Context, accountID, emailID string, emailItem *email.EmailItem, bodyText string) {
	if h.summarizer == nil || bodyText == "" {
		return
	}

	fromAddr := ""
	if len(emailItem.From) > 0 {
		fromAddr = emailItem.From[0].Email
	}

	summaryText, err := h.summarizer.Summarize(ctx, emailItem.Subject, fromAddr, bodyText)
	if err != nil {
		logger.WarnContext(ctx, "Failed to generate summary, continuing without it",
			slog.String("email_id", emailID),
			slog.String("error", err.Error()),
		)
		return
	}

	if summaryText == "" {
		return
	}

	// Store summary on the emailItem so it's available for vector metadata
	emailItem.Summary = summaryText

	// Persist summary to DynamoDB
	if err := h.emailUpdater.UpdateSummary(ctx, accountID, emailID, summaryText, h.overwritePreview); err != nil {
		logger.WarnContext(ctx, "Failed to persist summary",
			slog.String("email_id", emailID),
			slog.String("error", err.Error()),
		)
		return
	}

	// Log state change so clients see the update via Email/changes
	if h.stateChanger != nil {
		if _, err := h.stateChanger.IncrementStateAndLogChange(ctx, accountID, state.ObjectTypeEmail, emailID, state.ChangeTypeUpdated); err != nil {
			logger.WarnContext(ctx, "Failed to log summary state change",
				slog.String("email_id", emailID),
				slog.String("error", err.Error()),
			)
		}
	}
}

// indexSubjectVector creates a separate subject vector for subject-specific search.
func (h *handler) indexSubjectVector(ctx context.Context, accountID string, emailItem *email.EmailItem) error {
	vector, err := h.embedder.GenerateEmbedding(ctx, emailItem.Subject)
	if err != nil {
		return fmt.Errorf("generate subject embedding: %w", err)
	}

	metadata := buildVectorMetadata(emailItem, "subject")
	metadata["chunkIndex"] = 0

	key := emailItem.EmailID + "#subject"
	return h.vectorStore.PutVector(ctx, accountID, vectorstore.Vector{
		Key:      key,
		Data:     vector,
		Metadata: metadata,
	})
}

// indexSummaryVector creates a separate summary vector for summary-specific search.
func (h *handler) indexSummaryVector(ctx context.Context, accountID string, emailItem *email.EmailItem) error {
	vector, err := h.embedder.GenerateEmbedding(ctx, emailItem.Summary)
	if err != nil {
		return fmt.Errorf("generate summary embedding: %w", err)
	}

	metadata := buildVectorMetadata(emailItem, "summary")
	metadata["chunkIndex"] = 0

	key := emailItem.EmailID + "#summary"
	return h.vectorStore.PutVector(ctx, accountID, vectorstore.Vector{
		Key:      key,
		Data:     vector,
		Metadata: metadata,
	})
}

// processPartStream reads text from a stream, chunks it, generates embeddings, and stores vectors.
// Returns the number of chunks processed.
func (h *handler) processPartStream(ctx context.Context, accountID, emailID, headerPrefix string, metadata map[string]any, r io.Reader, startChunkIndex int) (int, error) {
	chunkIndex := startChunkIndex
	buf := make([]byte, chunkSize)
	overlap := ""

	for {
		// Start with overlap from previous chunk
		text := overlap

		// Read up to chunkSize characters
		for len(text) < chunkSize {
			n, err := r.Read(buf[:min(len(buf), chunkSize-len(text))])
			if n > 0 {
				text += string(buf[:n])
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				return 0, fmt.Errorf("read stream: %w", err)
			}
		}

		if len(strings.TrimSpace(text)) == 0 {
			break
		}

		// Build chunk with header prefix
		chunk := headerPrefix + text

		// Generate embedding
		vector, err := h.embedder.GenerateEmbedding(ctx, chunk)
		if err != nil {
			return 0, fmt.Errorf("generate embedding: %w", err)
		}

		// Build per-chunk metadata
		chunkMeta := make(map[string]any, len(metadata)+1)
		for k, v := range metadata {
			chunkMeta[k] = v
		}
		chunkMeta["chunkIndex"] = chunkIndex

		// Store vector
		key := fmt.Sprintf("%s#%d", emailID, chunkIndex)
		if err := h.vectorStore.PutVector(ctx, accountID, vectorstore.Vector{
			Key:      key,
			Data:     vector,
			Metadata: chunkMeta,
		}); err != nil {
			return 0, fmt.Errorf("put vector: %w", err)
		}

		chunkIndex++

		// Save overlap for next chunk
		if len(text) > chunkOverlap {
			overlap = text[len(text)-chunkOverlap:]
		} else {
			// Text was shorter than overlap; we're done
			break
		}

		// If text was shorter than chunkSize, we've read everything
		if len(text) < chunkSize {
			break
		}
	}

	return chunkIndex - startChunkIndex, nil
}

// deleteEmail removes all vectors and tokens for an email.
func (h *handler) deleteEmail(ctx context.Context, msg searchindex.Message) error {
	emailItem, err := h.emailReader.GetEmail(ctx, msg.AccountID, msg.EmailID)
	if err != nil {
		if errors.Is(err, email.ErrEmailNotFound) {
			logger.InfoContext(ctx, "Email not found for deletion, skipping",
				slog.String("email_id", msg.EmailID),
			)
			return nil
		}
		return fmt.Errorf("get email: %w", err)
	}

	// Delete address tokens
	if h.tokenWriter != nil {
		if err := h.tokenWriter.DeleteTokens(ctx, emailItem); err != nil {
			return fmt.Errorf("delete tokens: %w", err)
		}
	}

	// Delete vectors (body chunks + subject vector + summary vector)
	if emailItem.SearchChunks > 0 || emailItem.Summary != "" {
		keys := make([]string, 0, emailItem.SearchChunks+2)
		for i := 0; i < emailItem.SearchChunks; i++ {
			keys = append(keys, fmt.Sprintf("%s#%d", msg.EmailID, i))
		}
		// Also delete subject and summary vectors
		keys = append(keys, msg.EmailID+"#subject")
		keys = append(keys, msg.EmailID+"#summary")

		if err := h.vectorStore.DeleteVectors(ctx, msg.AccountID, keys); err != nil {
			return fmt.Errorf("delete vectors: %w", err)
		}
	}

	logger.InfoContext(ctx, "Email search data deleted",
		slog.String("email_id", msg.EmailID),
		slog.Int("chunks", emailItem.SearchChunks),
	)
	return nil
}

// buildHeaderPrefix constructs the header prefix that's prepended to every chunk.
func buildHeaderPrefix(e *email.EmailItem) string {
	var sb strings.Builder
	if e.Subject != "" {
		sb.WriteString("Subject: ")
		sb.WriteString(e.Subject)
		sb.WriteByte('\n')
	}
	if len(e.From) > 0 {
		sb.WriteString("From: ")
		sb.WriteString(formatAddresses(e.From))
		sb.WriteByte('\n')
	}
	if len(e.To) > 0 {
		sb.WriteString("To: ")
		sb.WriteString(formatAddresses(e.To))
		sb.WriteByte('\n')
	}
	if len(e.CC) > 0 {
		sb.WriteString("Cc: ")
		sb.WriteString(formatAddresses(e.CC))
		sb.WriteByte('\n')
	}
	if len(e.Bcc) > 0 {
		sb.WriteString("Bcc: ")
		sb.WriteString(formatAddresses(e.Bcc))
		sb.WriteByte('\n')
	}
	if e.Summary != "" {
		sb.WriteString("Summary: ")
		sb.WriteString(e.Summary)
		sb.WriteByte('\n')
	}
	if sb.Len() > 0 {
		sb.WriteByte('\n')
	}
	return sb.String()
}

// formatAddresses formats email addresses as a comma-separated string.
func formatAddresses(addrs []email.EmailAddress) string {
	parts := make([]string, len(addrs))
	for i, a := range addrs {
		if a.Name != "" {
			parts[i] = a.Name + " <" + a.Email + ">"
		} else {
			parts[i] = a.Email
		}
	}
	return strings.Join(parts, ", ")
}

// buildVectorMetadata constructs the metadata stored on each vector.
// vectorType is "body", "subject", or "summary".
func buildVectorMetadata(e *email.EmailItem, vectorType string) map[string]any {
	meta := map[string]any{
		"type":          vectorType,
		"emailId":       e.EmailID,
		"hasAttachment": e.HasAttachment,
		"size":          e.Size,
	}
	if e.Subject != "" {
		meta["subject"] = e.Subject
	}
	if e.Summary != "" {
		meta["summary"] = e.Summary
	}
	if len(e.From) > 0 {
		meta["from"] = e.From[0].Email
	}
	if len(e.To) > 0 {
		meta["to"] = formatAddresses(e.To)
	}
	if !e.ReceivedAt.IsZero() {
		meta["receivedAt"] = e.ReceivedAt.UTC().Format(time.RFC3339)
	}
	if len(e.MailboxIDs) > 0 {
		mailboxIDs := make([]string, 0, len(e.MailboxIDs))
		for k := range e.MailboxIDs {
			mailboxIDs = append(mailboxIDs, k)
		}
		meta["mailboxIds"] = mailboxIDs
	}
	if len(e.Keywords) > 0 {
		keywords := make([]string, 0, len(e.Keywords))
		for k := range e.Keywords {
			keywords = append(keywords, k)
		}
		meta["keywords"] = keywords
	}

	// Address token lists for S3 Vectors metadata filtering
	if tokens := email.TokenizeAddresses(e.From); len(tokens) > 0 {
		meta["fromTokens"] = tokens
	}
	if tokens := email.TokenizeAddresses(e.To); len(tokens) > 0 {
		meta["toTokens"] = tokens
	}
	if tokens := email.TokenizeAddresses(e.CC); len(tokens) > 0 {
		meta["ccTokens"] = tokens
	}
	if tokens := email.TokenizeAddresses(e.Bcc); len(tokens) > 0 {
		meta["bccTokens"] = tokens
	}

	return meta
}

func main() {
	ctx := context.Background()

	result, err := awsinit.Init(ctx)
	if err != nil {
		logger.Error("FATAL: Failed to initialize", slog.String("error", err.Error()))
		panic(err)
	}

	tableName := os.Getenv("EMAIL_TABLE_NAME")
	vectorBucketName := os.Getenv("VECTOR_BUCKET_NAME")

	dynamoClient := dbclient.NewClient(result.Config)

	// Warm DynamoDB connection
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

	// Blob client for streaming parts
	baseTransport := otelhttp.NewTransport(http.DefaultTransport)
	sigv4Transport := blob.NewSigV4Transport(baseTransport, result.Config.Credentials, result.Config.Region)
	signedHTTPClient := &http.Client{Transport: sigv4Transport}

	blobClientFactory := func(baseURL string) BlobStreamer {
		return blob.NewHTTPBlobClient(baseURL, signedHTTPClient)
	}

	// Bedrock embeddings client
	bedrockClient := bedrockruntime.NewFromConfig(result.Config)
	embedder := embeddings.NewBedrockClient(bedrockClient)

	// S3 Vectors client
	var resourceTags map[string]string
	if tagsJSON := os.Getenv("RESOURCE_TAGS"); tagsJSON != "" {
		if err := json.Unmarshal([]byte(tagsJSON), &resourceTags); err != nil {
			logger.Error("FATAL: Failed to parse RESOURCE_TAGS", slog.String("error", err.Error()))
			panic(err)
		}
	}
	s3vClient := s3vectors.NewFromConfig(result.Config)
	store := vectorstore.NewS3VectorsClient(s3vClient, vectorBucketName, resourceTags)

	tokenRepo := email.NewTokenRepository(dynamoClient, tableName)

	h := newHandler(repo, repo, blobClientFactory, embedder, store, tokenRepo)

	// Summarizer (optional — controlled by SUMMARY_MODEL_ID env var)
	if modelID := os.Getenv("SUMMARY_MODEL_ID"); modelID != "" {
		maxLength := summary.DefaultMaxLength
		if maxLenStr := os.Getenv("SUMMARY_MAX_LENGTH"); maxLenStr != "" {
			if parsed, err := strconv.Atoi(maxLenStr); err == nil && parsed > 0 {
				maxLength = parsed
			}
		}
		h.summarizer = summary.NewBedrockSummarizer(bedrockClient, summary.Config{
			ModelID:   modelID,
			MaxLength: maxLength,
		})
		h.overwritePreview = os.Getenv("SUMMARY_OVERWRITES_PREVIEW") == "true"

		stateRepo := state.NewRepository(dynamoClient, tableName, 7)
		h.stateChanger = stateRepo
	}

	result.Start(h.handle)
}
