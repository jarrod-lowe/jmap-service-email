// Package main implements the Thread/get Lambda handler.
package main

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
	"github.com/jarrod-lowe/jmap-service-libs/awsinit"
	"github.com/jarrod-lowe/jmap-service-libs/logging"
	"github.com/jarrod-lowe/jmap-service-libs/tracing"
	"golang.org/x/sync/errgroup"
)

var logger = logging.New()

// defaultMaxConcurrentThreadQueries is the fallback if THREAD_QUERY_CONCURRENCY env var is not set.
const defaultMaxConcurrentThreadQueries = 5

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
	repo                 EmailRepository
	stateRepo            StateRepository
	maxConcurrentQueries int
}

// newHandler creates a new handler (for backward compatibility in tests).
func newHandler(repo EmailRepository) *handler {
	return &handler{
		repo:                 repo,
		maxConcurrentQueries: defaultMaxConcurrentThreadQueries,
	}
}

// newHandlerWithState creates a new handler with state repository and configurable concurrency.
func newHandlerWithState(repo EmailRepository, stateRepo StateRepository, maxConcurrentQueries int) *handler {
	return &handler{
		repo:                 repo,
		stateRepo:            stateRepo,
		maxConcurrentQueries: maxConcurrentQueries,
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

	// Fetch threads concurrently
	type threadResult struct {
		threadID string
		emails   []*email.EmailItem
		err      error
	}

	results := make([]threadResult, len(ids))
	eg := new(errgroup.Group)
	eg.SetLimit(h.maxConcurrentQueries)

	for i, threadID := range ids {
		i, threadID := i, threadID // capture for closure
		eg.Go(func() error {
			emails, err := h.repo.FindByThreadID(ctx, accountID, threadID)
			results[i] = threadResult{threadID: threadID, emails: emails, err: err}
			return nil // Don't fail fast - collect all results
		})
	}

	_ = eg.Wait() // Always succeeds since goroutines return nil

	// Process results in order
	var list []any
	var notFound []any

	for _, r := range results {
		if r.err != nil {
			logger.ErrorContext(ctx, "Failed to query thread",
				slog.String("account_id", accountID),
				slog.String("thread_id", r.threadID),
				slog.String("error", r.err.Error()),
			)
			return errorResponse(request.ClientID, "serverFail", r.err.Error()), nil
		}

		if len(r.emails) == 0 {
			notFound = append(notFound, r.threadID)
			continue
		}

		// Build thread object with emailIds in order (already sorted by receivedAt from repo)
		// Exclude soft-deleted emails
		emailIds := make([]string, 0, len(r.emails))
		for _, e := range r.emails {
			if e.DeletedAt == nil {
				emailIds = append(emailIds, e.EmailID)
			}
		}

		// If all emails in thread are soft-deleted, treat thread as not found
		if len(emailIds) == 0 {
			notFound = append(notFound, r.threadID)
			continue
		}

		thread := map[string]any{
			"id":       r.threadID,
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

	result, err := awsinit.Init(ctx)
	if err != nil {
		logger.Error("FATAL: Failed to initialize", slog.String("error", err.Error()))
		panic(err)
	}

	// Load config from environment
	tableName := os.Getenv("EMAIL_TABLE_NAME")

	// Parse max concurrent thread queries (default 5)
	maxConcurrentQueries := defaultMaxConcurrentThreadQueries
	if v := os.Getenv("THREAD_QUERY_CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxConcurrentQueries = n
		}
	}

	// Create DynamoDB client
	dynamoClient := dynamodb.NewFromConfig(result.Config)

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
	stateRepo := state.NewRepository(dynamoClient, tableName, 7)

	h := newHandlerWithState(repo, stateRepo, maxConcurrentQueries)
	result.Start(h.handle)
}
