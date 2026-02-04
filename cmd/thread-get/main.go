// Package main implements the Thread/get Lambda handler.
package main

import (
	"context"
	"log/slog"
	"os"
	"strconv"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
	"github.com/jarrod-lowe/jmap-service-libs/tracing"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda/xrayconfig"
	"go.opentelemetry.io/otel"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

// EmailRepository defines the interface for retrieving emails.
type EmailRepository interface {
	FindByThreadID(ctx context.Context, accountID, threadID string) ([]*email.EmailItem, error)
}

// StateRepository defines the interface for state operations.
type StateRepository interface {
	GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
}

// handler implements the Thread/get logic.
type handler struct {
	repo      EmailRepository
	stateRepo StateRepository
}

// newHandler creates a new handler (for backward compatibility in tests).
func newHandler(repo EmailRepository) *handler {
	return &handler{
		repo: repo,
	}
}

// newHandlerWithState creates a new handler with state repository.
func newHandlerWithState(repo EmailRepository, stateRepo StateRepository) *handler {
	return &handler{
		repo:      repo,
		stateRepo: stateRepo,
	}
}

// handle processes a Thread/get request.
func (h *handler) handle(ctx context.Context, request plugincontract.PluginInvocationRequest) (plugincontract.PluginInvocationResponse, error) {
	tracer := tracing.Tracer("jmap-thread-get")
	ctx, span := tracer.Start(ctx, "ThreadGetHandler")
	defer span.End()

	// Check method
	if request.Method != "Thread/get" {
		return plugincontract.PluginInvocationResponse{
			MethodResponse: plugincontract.MethodResponse{
				Name: "error",
				Args: map[string]any{
					"type":        "unknownMethod",
					"description": "This handler only supports Thread/get",
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

	// Get current Thread state
	var currentState int64
	if h.stateRepo != nil {
		var err error
		currentState, err = h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeThread)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to get current state",
				slog.String("account_id", accountID),
				slog.String("error", err.Error()),
			)
			return errorResponse(request.ClientID, "serverFail", err.Error()), nil
		}
	}

	// Extract and validate ids
	idsArg, ok := request.Args["ids"]
	if !ok {
		return errorResponse(request.ClientID, "invalidArguments", "ids argument is required"), nil
	}

	idsSlice, ok := idsArg.([]any)
	if !ok {
		return errorResponse(request.ClientID, "invalidArguments", "ids argument must be an array"), nil
	}

	// Convert IDs to strings
	var ids []string
	for _, id := range idsSlice {
		idStr, ok := id.(string)
		if !ok {
			return errorResponse(request.ClientID, "invalidArguments", "ids must contain strings"), nil
		}
		ids = append(ids, idStr)
	}

	// Fetch threads
	var list []any
	var notFound []any

	for _, threadID := range ids {
		emails, err := h.repo.FindByThreadID(ctx, accountID, threadID)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to query thread",
				slog.String("account_id", accountID),
				slog.String("thread_id", threadID),
				slog.String("error", err.Error()),
			)
			return errorResponse(request.ClientID, "serverFail", err.Error()), nil
		}

		if len(emails) == 0 {
			notFound = append(notFound, threadID)
			continue
		}

		// Build thread object with emailIds in order (already sorted by receivedAt from repo)
		// Exclude soft-deleted emails
		emailIds := make([]string, 0, len(emails))
		for _, e := range emails {
			if e.DeletedAt == nil {
				emailIds = append(emailIds, e.EmailID)
			}
		}

		// If all emails in thread are soft-deleted, treat thread as not found
		if len(emailIds) == 0 {
			notFound = append(notFound, threadID)
			continue
		}

		thread := map[string]any{
			"id":       threadID,
			"emailIds": emailIds,
		}
		list = append(list, thread)
	}

	// Ensure slices are not nil (JMAP spec requires empty arrays, not null)
	if list == nil {
		list = []any{}
	}
	if notFound == nil {
		notFound = []any{}
	}

	logger.InfoContext(ctx, "Thread/get completed",
		slog.String("account_id", accountID),
		slog.Int("list_count", len(list)),
		slog.Int("not_found_count", len(notFound)),
	)

	return plugincontract.PluginInvocationResponse{
		MethodResponse: plugincontract.MethodResponse{
			Name: "Thread/get",
			Args: map[string]any{
				"accountId": accountID,
				"state":     strconv.FormatInt(currentState, 10),
				"list":      list,
				"notFound":  notFound,
			},
			ClientID: request.ClientID,
		},
	}, nil
}

// errorResponse creates an error response.
func errorResponse(clientID, errorType, description string) plugincontract.PluginInvocationResponse {
	return plugincontract.PluginInvocationResponse{
		MethodResponse: plugincontract.MethodResponse{
			Name: "error",
			Args: map[string]any{
				"type":        errorType,
				"description": description,
			},
			ClientID: clientID,
		},
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

	// Load config from environment
	tableName := os.Getenv("EMAIL_TABLE_NAME")

	// Initialize AWS config
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		logger.Error("FATAL: Failed to load AWS config", slog.String("error", err.Error()))
		panic(err)
	}

	// Instrument AWS SDK clients with OTel tracing
	otelaws.AppendMiddlewares(&cfg.APIOptions)

	// Create DynamoDB client
	dynamoClient := dynamodb.NewFromConfig(cfg)
	repo := email.NewRepository(dynamoClient, tableName)
	stateRepo := state.NewRepository(dynamoClient, tableName, 7)

	h := newHandlerWithState(repo, stateRepo)
	lambda.Start(otellambda.InstrumentHandler(h.handle, xrayconfig.WithRecommendedOptions(tp)...))
}
