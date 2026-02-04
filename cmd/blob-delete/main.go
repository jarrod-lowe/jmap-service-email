// Package main implements the blob-delete SQS consumer Lambda handler.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"

	"github.com/jarrod-lowe/jmap-service-email/internal/blob"
	"github.com/jarrod-lowe/jmap-service-email/internal/blobdelete"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda/xrayconfig"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/contrib/propagators/aws/xray"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

// BlobDeleter abstracts blob deletion for dependency inversion.
type BlobDeleter interface {
	Delete(ctx context.Context, accountID, blobID string) error
}

// handler implements the blob-delete SQS consumer logic.
type handler struct {
	blobDeleter BlobDeleter
}

// newHandler creates a new handler.
func newHandler(blobDeleter BlobDeleter) *handler {
	return &handler{blobDeleter: blobDeleter}
}

// handle processes an SQS event containing blob deletion messages.
func (h *handler) handle(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
	tracer := otel.Tracer("jmap-blob-delete")
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

		failed := false
		for _, blobID := range msg.BlobIDs {
			if err := h.blobDeleter.Delete(ctx, msg.AccountID, blobID); err != nil {
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

	tp, err := xrayconfig.NewTracerProvider(ctx)
	if err != nil {
		logger.Error("FATAL: Failed to initialize tracer provider", slog.String("error", err.Error()))
		panic(err)
	}
	otel.SetTracerProvider(tp)

	// Set X-Ray propagator as global propagator for HTTP client trace context injection
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		xray.Propagator{},
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	coreAPIURL := os.Getenv("CORE_API_URL")

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		logger.Error("FATAL: Failed to load AWS config", slog.String("error", err.Error()))
		panic(err)
	}

	// Instrument AWS SDK clients with OTel tracing
	otelaws.AppendMiddlewares(&cfg.APIOptions)

	// Create blob client with OTel instrumentation and SigV4 signing
	baseTransport := otelhttp.NewTransport(http.DefaultTransport)
	transport := blob.NewSigV4Transport(baseTransport, cfg.Credentials, cfg.Region)
	httpClient := &http.Client{Transport: transport}
	blobClient := blob.NewHTTPBlobClient(coreAPIURL, httpClient)

	h := newHandler(blobClient)
	lambda.Start(otellambda.InstrumentHandler(h.handle, xrayconfig.WithRecommendedOptions(tp)...))
}
