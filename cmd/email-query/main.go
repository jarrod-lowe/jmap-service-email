// Package main implements the Email/query Lambda handler.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"github.com/jarrod-lowe/jmap-service-libs/dbclient"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailbox"
	"github.com/jarrod-lowe/jmap-service-libs/awsinit"
	"github.com/jarrod-lowe/jmap-service-libs/jmaperror"
	"github.com/jarrod-lowe/jmap-service-libs/logging"
	"github.com/jarrod-lowe/jmap-service-libs/tracing"
)

var logger = logging.New()

// EmailRepository defines the interface for email operations.
type EmailRepository interface {
	QueryEmails(ctx context.Context, accountID string, req *email.QueryRequest) (*email.QueryResult, error)
	GetEmail(ctx context.Context, accountID, emailID string) (*email.EmailItem, error)
}

// MailboxChecker defines the interface for checking mailbox existence and retrieval.
type MailboxChecker interface {
	MailboxExists(ctx context.Context, accountID, mailboxID string) (bool, error)
	GetMailbox(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error)
}

// handler implements the Email/query logic.
type handler struct {
	emailRepo   EmailRepository
	mailboxRepo MailboxChecker
}

// newHandler creates a new handler.
func newHandler(emailRepo EmailRepository, mailboxRepo MailboxChecker) *handler {
	return &handler{
		emailRepo:   emailRepo,
		mailboxRepo: mailboxRepo,
	}
}

// Constants for query limits.
const (
	defaultLimit = 25
	maxLimit     = 100
)

// handle processes an Email/query request.
func (h *handler) handle(ctx context.Context, request plugincontract.PluginInvocationRequest) (plugincontract.PluginInvocationResponse, error) {
	tracer := tracing.Tracer("jmap-email-query")
	ctx, span := tracer.Start(ctx, "EmailQueryHandler")
	defer span.End()

	// Check method
	if request.Method != "Email/query" {
		return errorResponse(request.ClientID, jmaperror.UnknownMethod("This handler only supports Email/query")), nil
	}

	// Parse request args
	accountID := request.AccountID
	if argAccountID, ok := request.Args["accountId"].(string); ok {
		accountID = argAccountID
	}

	// Parse filter
	var queryFilter *email.QueryFilter
	if filterArg, ok := request.Args["filter"].(map[string]any); ok {
		// Check for unsupported filter properties
		for key := range filterArg {
			if key != "inMailbox" {
				return errorResponse(request.ClientID, jmaperror.UnsupportedFilter("Only inMailbox filter is supported")), nil
			}
		}

		if inMailbox, ok := filterArg["inMailbox"].(string); ok {
			// Validate mailbox exists
			exists, err := h.mailboxRepo.MailboxExists(ctx, accountID, inMailbox)
			if err != nil {
				logger.ErrorContext(ctx, "Failed to check mailbox existence",
					slog.String("account_id", accountID),
					slog.String("mailbox_id", inMailbox),
					slog.String("error", err.Error()),
				)
				return errorResponse(request.ClientID, jmaperror.ServerFail(err.Error(), err)), nil
			}
			if !exists {
				return errorResponse(request.ClientID, jmaperror.InvalidArguments("Mailbox not found: "+inMailbox)), nil
			}
			queryFilter = &email.QueryFilter{InMailbox: inMailbox}
		}
	}

	// Parse sort
	var comparators []email.Comparator
	if sortArg, ok := request.Args["sort"].([]any); ok {
		for _, s := range sortArg {
			sortMap, ok := s.(map[string]any)
			if !ok {
				continue
			}
			property, _ := sortMap["property"].(string)
			if property != "" && property != "receivedAt" {
				return errorResponse(request.ClientID, jmaperror.UnsupportedSort("Only receivedAt sort is supported")), nil
			}
			isAscending, _ := sortMap["isAscending"].(bool)
			comparators = append(comparators, email.Comparator{
				Property:    property,
				IsAscending: isAscending,
			})
		}
	}

	// Parse position
	position := 0
	if posArg, ok := request.Args["position"].(float64); ok {
		position = int(posArg)
	}

	// Parse anchor
	anchor := ""
	if anchorArg, ok := request.Args["anchor"].(string); ok {
		anchor = anchorArg
		// Validate anchor exists
		_, err := h.emailRepo.GetEmail(ctx, accountID, anchor)
		if err != nil {
			if err == email.ErrEmailNotFound {
				return errorResponse(request.ClientID, jmaperror.AnchorNotFound("Anchor email not found: "+anchor)), nil
			}
			logger.ErrorContext(ctx, "Failed to check anchor existence",
				slog.String("account_id", accountID),
				slog.String("anchor", anchor),
				slog.String("error", err.Error()),
			)
			return errorResponse(request.ClientID, jmaperror.ServerFail(err.Error(), err)), nil
		}
	}

	// Parse anchorOffset
	anchorOffset := 0
	if offsetArg, ok := request.Args["anchorOffset"].(float64); ok {
		anchorOffset = int(offsetArg)
	}

	// Parse limit
	limit := defaultLimit
	if limitArg, ok := request.Args["limit"].(float64); ok {
		limit = int(limitArg)
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	if limit < 0 {
		limit = 0
	}

	// Parse collapseThreads
	collapseThreads := false
	if ct, ok := request.Args["collapseThreads"].(bool); ok {
		collapseThreads = ct
	}

	// Build query request
	queryReq := &email.QueryRequest{
		Filter:          queryFilter,
		Sort:            comparators,
		Position:        position,
		Anchor:          anchor,
		AnchorOffset:    anchorOffset,
		Limit:           limit,
		CollapseThreads: collapseThreads,
	}

	// Execute query
	result, err := h.emailRepo.QueryEmails(ctx, accountID, queryReq)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to query emails",
			slog.String("account_id", accountID),
			slog.String("error", err.Error()),
		)
		return errorResponse(request.ClientID, jmaperror.ServerFail(err.Error(), err)), nil
	}

	logger.InfoContext(ctx, "Email/query completed",
		slog.String("account_id", accountID),
		slog.Int("result_count", len(result.IDs)),
		slog.Int("position", result.Position),
	)

	// Build response
	response := map[string]any{
		"accountId":           accountID,
		"queryState":          result.QueryState,
		"canCalculateChanges": false,
		"position":            result.Position,
		"ids":                 result.IDs,
		"collapseThreads":     collapseThreads,
	}

	// Add total when inMailbox filter is used (use mailbox counters)
	if queryFilter != nil && queryFilter.InMailbox != "" {
		mbox, err := h.mailboxRepo.GetMailbox(ctx, accountID, queryFilter.InMailbox)
		if err == nil {
			response["total"] = mbox.TotalEmails
		}
		// If GetMailbox fails, we just don't include total (it's optional)
	}

	return plugincontract.PluginInvocationResponse{
		MethodResponse: plugincontract.MethodResponse{
			Name:     "Email/query",
			Args:     response,
			ClientID: request.ClientID,
		},
	}, nil
}

// errorResponse creates an error response from a jmaperror.MethodError.
func errorResponse(clientID string, err *jmaperror.MethodError) plugincontract.PluginInvocationResponse {
	return plugincontract.PluginInvocationResponse{
		MethodResponse: plugincontract.MethodResponse{
			Name:     "error",
			Args:     err.ToMap(),
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

	emailRepo := email.NewRepository(dynamoClient, tableName)
	mailboxRepo := mailbox.NewDynamoDBRepository(dynamoClient, tableName)

	h := newHandler(emailRepo, mailboxRepo)
	result.Start(h.handle)
}
