// Package main implements the Mailbox/get Lambda handler.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailbox"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda/xrayconfig"
	"go.opentelemetry.io/otel"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

// MailboxRepository defines the interface for retrieving mailboxes.
type MailboxRepository interface {
	GetMailbox(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error)
	GetAllMailboxes(ctx context.Context, accountID string) ([]*mailbox.MailboxItem, error)
}

// StateRepository defines the interface for state operations.
type StateRepository interface {
	GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
}

// handler implements the Mailbox/get logic.
type handler struct {
	repo      MailboxRepository
	stateRepo StateRepository
}

// newHandler creates a new handler.
func newHandler(repo MailboxRepository, stateRepo StateRepository) *handler {
	return &handler{
		repo:      repo,
		stateRepo: stateRepo,
	}
}

// handle processes a Mailbox/get request.
func (h *handler) handle(ctx context.Context, request plugincontract.PluginInvocationRequest) (plugincontract.PluginInvocationResponse, error) {
	tracer := otel.Tracer("jmap-mailbox-get")
	ctx, span := tracer.Start(ctx, "MailboxGetHandler")
	defer span.End()

	// Check method
	if request.Method != "Mailbox/get" {
		return plugincontract.PluginInvocationResponse{
			MethodResponse: plugincontract.MethodResponse{
				Name: "error",
				Args: map[string]any{
					"type":        "unknownMethod",
					"description": "This handler only supports Mailbox/get",
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

	// Extract and validate properties (optional)
	var properties []string
	if propsArg, ok := request.Args["properties"]; ok && propsArg != nil {
		propsSlice, ok := propsArg.([]any)
		if !ok {
			return errorResponse(request.ClientID, "invalidArguments", "properties argument must be an array"), nil
		}
		for _, p := range propsSlice {
			prop, ok := p.(string)
			if !ok {
				return errorResponse(request.ClientID, "invalidArguments", "properties must contain strings"), nil
			}
			properties = append(properties, prop)
		}
	}

	// Check if ids is nil (get all) or a list
	idsArg := request.Args["ids"]
	var mailboxes []*mailbox.MailboxItem
	var notFound []any

	if idsArg == nil {
		// Get all mailboxes
		all, err := h.repo.GetAllMailboxes(ctx, accountID)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to get all mailboxes",
				slog.String("account_id", accountID),
				slog.String("error", err.Error()),
			)
			return errorResponse(request.ClientID, "serverFail", err.Error()), nil
		}
		mailboxes = all
	} else {
		// Get specific mailboxes by ID
		idsSlice, ok := idsArg.([]any)
		if !ok {
			return errorResponse(request.ClientID, "invalidArguments", "ids argument must be an array or null"), nil
		}

		for _, id := range idsSlice {
			idStr, ok := id.(string)
			if !ok {
				return errorResponse(request.ClientID, "invalidArguments", "ids must contain strings"), nil
			}

			mbox, err := h.repo.GetMailbox(ctx, accountID, idStr)
			if err != nil {
				if errors.Is(err, mailbox.ErrMailboxNotFound) {
					notFound = append(notFound, idStr)
					continue
				}
				// Repository error
				logger.ErrorContext(ctx, "Failed to get mailbox",
					slog.String("account_id", accountID),
					slog.String("mailbox_id", idStr),
					slog.String("error", err.Error()),
				)
				return errorResponse(request.ClientID, "serverFail", err.Error()), nil
			}
			mailboxes = append(mailboxes, mbox)
		}
	}

	// Transform mailboxes to response format
	var list []any
	for _, mbox := range mailboxes {
		list = append(list, transformMailbox(mbox, properties))
	}

	// Ensure slices are not nil
	if list == nil {
		list = []any{}
	}
	if notFound == nil {
		notFound = []any{}
	}

	// Get current state
	stateStr := "0"
	if h.stateRepo != nil {
		currentState, err := h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeMailbox)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to get current state",
				slog.String("account_id", accountID),
				slog.String("error", err.Error()),
			)
			return errorResponse(request.ClientID, "serverFail", err.Error()), nil
		}
		stateStr = strconv.FormatInt(currentState, 10)
	}

	logger.InfoContext(ctx, "Mailbox/get completed",
		slog.String("account_id", accountID),
		slog.Int("list_count", len(list)),
		slog.Int("not_found_count", len(notFound)),
	)

	return plugincontract.PluginInvocationResponse{
		MethodResponse: plugincontract.MethodResponse{
			Name: "Mailbox/get",
			Args: map[string]any{
				"accountId": accountID,
				"state":     stateStr,
				"list":      list,
				"notFound":  notFound,
			},
			ClientID: request.ClientID,
		},
	}, nil
}

// transformMailbox converts a MailboxItem to the JMAP response format.
func transformMailbox(m *mailbox.MailboxItem, properties []string) map[string]any {
	// Build full mailbox map with computed fields
	full := map[string]any{
		"id":            m.MailboxID,
		"name":          m.Name,
		"parentId":      nil, // Always null (flat hierarchy)
		"sortOrder":     m.SortOrder,
		"totalEmails":   m.TotalEmails,
		"unreadEmails":  m.UnreadEmails,
		"totalThreads":  m.TotalEmails,  // Stubbed: equals totalEmails
		"unreadThreads": m.UnreadEmails, // Stubbed: equals unreadEmails
		"myRights":      transformRights(mailbox.AllRights()),
		"isSubscribed":  m.IsSubscribed,
	}

	// Only include role if non-empty (omit rather than null to avoid client sorting issues)
	if m.Role != "" {
		full["role"] = m.Role
	}

	// If no properties specified, return all
	if len(properties) == 0 {
		return full
	}

	// Filter to requested properties
	filtered := make(map[string]any)
	for _, prop := range properties {
		if val, ok := full[prop]; ok {
			filtered[prop] = val
		}
	}

	// RFC 8620 Section 5.1: id is always returned regardless of properties list
	filtered["id"] = full["id"]

	return filtered
}

// transformRights converts MailboxRights to the JMAP response format.
func transformRights(r mailbox.MailboxRights) map[string]any {
	return map[string]any{
		"mayReadItems":   r.MayReadItems,
		"mayAddItems":    r.MayAddItems,
		"mayRemoveItems": r.MayRemoveItems,
		"maySetSeen":     r.MaySetSeen,
		"maySetKeywords": r.MaySetKeywords,
		"mayCreateChild": r.MayCreateChild,
		"mayRename":      r.MayRename,
		"mayDelete":      r.MayDelete,
		"maySubmit":      r.MaySubmit,
	}
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

	tp, err := xrayconfig.NewTracerProvider(ctx)
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

	// Create DynamoDB client
	dynamoClient := dynamodb.NewFromConfig(cfg)
	repo := mailbox.NewDynamoDBRepository(dynamoClient, tableName)
	stateRepo := state.NewRepository(dynamoClient, tableName, 7)

	h := newHandler(repo, stateRepo)
	lambda.Start(otellambda.InstrumentHandler(h.handle, xrayconfig.WithRecommendedOptions(tp)...))
}
