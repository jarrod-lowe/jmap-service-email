// Package main implements the Thread/changes Lambda handler.
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
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
	"github.com/jarrod-lowe/jmap-service-libs/awsinit"
	"github.com/jarrod-lowe/jmap-service-libs/logging"
	"github.com/jarrod-lowe/jmap-service-libs/tracing"
)

var logger = logging.New()

// StateRepository defines the interface for state operations.
type StateRepository interface {
	GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
	QueryChanges(ctx context.Context, accountID string, objectType state.ObjectType, sinceState int64, maxChanges int) ([]state.ChangeRecord, error)
	GetOldestAvailableState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
}

// handler implements the Thread/changes logic.
type handler struct {
	stateRepo StateRepository
}

// newHandler creates a new handler.
func newHandler(stateRepo StateRepository) *handler {
	return &handler{
		stateRepo: stateRepo,
	}
}

// handle processes a Thread/changes request.
func (h *handler) handle(ctx context.Context, request plugincontract.PluginInvocationRequest) (plugincontract.PluginInvocationResponse, error) {
	tracer := tracing.Tracer("jmap-thread-changes")
	ctx, span := tracer.Start(ctx, "ThreadChangesHandler")
	defer span.End()

	// Check method
	if request.Method != "Thread/changes" {
		return errorResponse(request.ClientID, "unknownMethod", "This handler only supports Thread/changes"), nil
	}

	// Parse request args
	accountID := request.AccountID
	if argAccountID, ok := request.Args["accountId"].(string); ok {
		accountID = argAccountID
	}

	// Extract sinceState (required)
	sinceStateArg, ok := request.Args["sinceState"]
	if !ok {
		return errorResponse(request.ClientID, "invalidArguments", "sinceState argument is required"), nil
	}

	sinceStateStr, ok := sinceStateArg.(string)
	if !ok {
		return errorResponse(request.ClientID, "invalidArguments", "sinceState must be a string"), nil
	}

	sinceState, err := strconv.ParseInt(sinceStateStr, 10, 64)
	if err != nil {
		return errorResponse(request.ClientID, "cannotCalculateChanges", "sinceState does not represent a valid state"), nil
	}

	// Extract maxChanges (optional)
	maxChanges := 0 // 0 means no limit
	if maxChangesArg, ok := request.Args["maxChanges"]; ok {
		switch v := maxChangesArg.(type) {
		case float64:
			maxChanges = int(v)
		case int:
			maxChanges = v
		}
	}

	// Get current state
	currentState, err := h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeThread)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get current state",
			slog.String("account_id", accountID),
			slog.String("error", err.Error()),
		)
		return errorResponse(request.ClientID, "serverFail", err.Error()), nil
	}

	// Check for gap (cannotCalculateChanges)
	oldestAvailable, err := h.stateRepo.GetOldestAvailableState(ctx, accountID, state.ObjectTypeThread)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get oldest available state",
			slog.String("account_id", accountID),
			slog.String("error", err.Error()),
		)
		return errorResponse(request.ClientID, "serverFail", err.Error()), nil
	}

	// If oldestAvailable > sinceState + 1, we have a gap
	if oldestAvailable > 0 && sinceState < oldestAvailable-1 {
		return errorResponse(request.ClientID, "cannotCalculateChanges",
			"State is too old, change log entries have expired"), nil
	}

	// If sinceState > currentState, it's a future state we don't know about
	if sinceState > currentState {
		return errorResponse(request.ClientID, "cannotCalculateChanges",
			"sinceState is newer than current state"), nil
	}

	// Query changes since sinceState
	changes, err := h.stateRepo.QueryChanges(ctx, accountID, state.ObjectTypeThread, sinceState, maxChanges)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to query changes",
			slog.String("account_id", accountID),
			slog.String("error", err.Error()),
		)
		return errorResponse(request.ClientID, "serverFail", err.Error()), nil
	}

	// Process changes to determine created/updated/destroyed
	created, updated, destroyed := processThreadChanges(changes)

	// Determine hasMoreChanges
	hasMore := false
	newState := currentState
	if len(changes) > 0 {
		lastChangeState := changes[len(changes)-1].State
		if lastChangeState < currentState {
			hasMore = true
			newState = lastChangeState
		}
	}

	logger.InfoContext(ctx, "Thread/changes completed",
		slog.String("account_id", accountID),
		slog.Int64("since_state", sinceState),
		slog.Int64("new_state", newState),
		slog.Int("created_count", len(created)),
		slog.Int("updated_count", len(updated)),
		slog.Int("destroyed_count", len(destroyed)),
	)

	return plugincontract.PluginInvocationResponse{
		MethodResponse: plugincontract.MethodResponse{
			Name: "Thread/changes",
			Args: map[string]any{
				"accountId":      accountID,
				"oldState":       sinceStateStr,
				"newState":       strconv.FormatInt(newState, 10),
				"hasMoreChanges": hasMore,
				"created":        created,
				"updated":        updated,
				"destroyed":      destroyed,
			},
			ClientID: request.ClientID,
		},
	}, nil
}

// processThreadChanges groups change records by object ID and determines final state.
// Rules:
// - If latest change is destroyed → destroyed list
// - Else if earliest change is created → created list
// - Else → updated list
// - If created then destroyed in window → omit entirely
func processThreadChanges(changes []state.ChangeRecord) (created, updated, destroyed []string) {
	// Group changes by objectId
	type changeInfo struct {
		earliest state.ChangeType
		latest   state.ChangeType
	}
	byObject := make(map[string]*changeInfo)

	for _, change := range changes {
		info, exists := byObject[change.ObjectID]
		if !exists {
			info = &changeInfo{earliest: change.ChangeType, latest: change.ChangeType}
			byObject[change.ObjectID] = info
		} else {
			info.latest = change.ChangeType
		}
	}

	// Build result lists
	created = []string{}
	updated = []string{}
	destroyed = []string{}

	for objectID, info := range byObject {
		// If created then destroyed, omit entirely
		if info.earliest == state.ChangeTypeCreated && info.latest == state.ChangeTypeDestroyed {
			continue
		}

		if info.latest == state.ChangeTypeDestroyed {
			destroyed = append(destroyed, objectID)
		} else if info.earliest == state.ChangeTypeCreated {
			created = append(created, objectID)
		} else {
			updated = append(updated, objectID)
		}
	}

	return created, updated, destroyed
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

	tableName := os.Getenv("EMAIL_TABLE_NAME")
	retentionDays := 7 // Default

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

	stateRepo := state.NewRepository(dynamoClient, tableName, retentionDays)

	h := newHandler(stateRepo)
	result.Start(h.handle)
}
