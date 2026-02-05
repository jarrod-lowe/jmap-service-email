// Package main implements the blob-delete SQS consumer Lambda handler.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/jarrod-lowe/jmap-service-email/internal/blob"
	"github.com/jarrod-lowe/jmap-service-email/internal/blobdelete"
	"github.com/jarrod-lowe/jmap-service-libs/awsinit"
	"github.com/jarrod-lowe/jmap-service-libs/logging"
	"github.com/jarrod-lowe/jmap-service-libs/tracing"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var logger = logging.New()

// BlobDeleter abstracts blob deletion for dependency inversion.
type BlobDeleter interface {
	Delete(ctx context.Context, accountID, blobID string) error
}

// handler implements the blob-delete SQS consumer logic.
type handler struct {
	blobDeleterFactory func(baseURL string) BlobDeleter
}

// newHandler creates a new handler.
func newHandler(blobDeleterFactory func(baseURL string) BlobDeleter) *handler {
	return &handler{blobDeleterFactory: blobDeleterFactory}
}

// handle processes an SQS event containing blob deletion messages.
func (h *handler) handle(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
	tracer := tracing.Tracer("jmap-blob-delete")
	ctx, span := tracer.Start(ctx, "BlobDeleteHandler")
	defer span.End()

	var failures []events.SQSBatchItemFailure

	for _, record := range event.Records {
		var msg blobdelete.BlobDeleteMessage
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

		blobDeleter := h.blobDeleterFactory(msg.APIURL)

		failed := false
		for _, blobID := range msg.BlobIDs {
			if err := blobDeleter.Delete(ctx, msg.AccountID, blobID); err != nil {
				logger.ErrorContext(ctx, "Failed to delete blob",
					slog.String("account_id", msg.AccountID),
					slog.String("blob_id", blobID),
					slog.String("error", err.Error()),
				)
				failed = true
			}
		}

		if failed {
			failures = append(failures, events.SQSBatchItemFailure{
				ItemIdentifier: record.MessageId,
			})
		}
	}

	logger.InfoContext(ctx, "Blob delete batch completed",
		slog.Int("total", len(event.Records)),
		slog.Int("failures", len(failures)),
	)

	return events.SQSEventResponse{
		BatchItemFailures: failures,
	}, nil
}

func main() {
	ctx := context.Background()

	result, err := awsinit.Init(ctx)
	if err != nil {
		logger.Error("FATAL: Failed to initialize", slog.String("error", err.Error()))
		panic(err)
	}

	// Create blob client with OTel instrumentation and SigV4 signing
	baseTransport := otelhttp.NewTransport(http.DefaultTransport)
	transport := blob.NewSigV4Transport(baseTransport, result.Config.Credentials, result.Config.Region)
	httpClient := &http.Client{Transport: transport}

	factory := func(baseURL string) BlobDeleter {
		return blob.NewHTTPBlobClient(baseURL, httpClient)
	}

	h := newHandler(factory)
	result.Start(h.handle)
}
