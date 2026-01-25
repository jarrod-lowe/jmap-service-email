// Package main implements the Email/get Lambda handler.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda/xrayconfig"
	"go.opentelemetry.io/otel"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

// EmailRepository defines the interface for retrieving emails.
type EmailRepository interface {
	GetEmail(ctx context.Context, accountID, emailID string) (*email.EmailItem, error)
}

// StateRepository defines the interface for state operations.
type StateRepository interface {
	GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
}

// handler implements the Email/get logic.
type handler struct {
	repo      EmailRepository
	stateRepo StateRepository
}

// newHandler creates a new handler.
func newHandler(repo EmailRepository, stateRepo StateRepository) *handler {
	return &handler{
		repo:      repo,
		stateRepo: stateRepo,
	}
}

// handle processes an Email/get request.
func (h *handler) handle(ctx context.Context, request plugincontract.PluginInvocationRequest) (plugincontract.PluginInvocationResponse, error) {
	tracer := otel.Tracer("jmap-email-get")
	ctx, span := tracer.Start(ctx, "EmailGetHandler")
	defer span.End()

	// Check method
	if request.Method != "Email/get" {
		return plugincontract.PluginInvocationResponse{
			MethodResponse: plugincontract.MethodResponse{
				Name: "error",
				Args: map[string]any{
					"type":        "unknownMethod",
					"description": "This handler only supports Email/get",
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

	// Extract and validate ids
	idsArg, ok := request.Args["ids"]
	if !ok {
		return errorResponse(request.ClientID, "invalidArguments", "ids argument is required"), nil
	}

	idsSlice, ok := idsArg.([]any)
	if !ok {
		return errorResponse(request.ClientID, "invalidArguments", "ids argument must be an array"), nil
	}

	// Extract and validate properties (optional)
	var properties []string
	if propsArg, ok := request.Args["properties"]; ok {
		propsSlice, ok := propsArg.([]any)
		if !ok {
			return errorResponse(request.ClientID, "invalidArguments", "properties argument must be an array"), nil
		}
		for _, p := range propsSlice {
			prop, ok := p.(string)
			if !ok {
				return errorResponse(request.ClientID, "invalidArguments", "properties must contain strings"), nil
			}
			// Reject header:* properties
			if strings.HasPrefix(prop, "header:") {
				return errorResponse(request.ClientID, "invalidArguments", "header:* properties are not supported"), nil
			}
			properties = append(properties, prop)
		}
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

	// Fetch emails
	var list []any
	var notFound []any

	for _, emailID := range ids {
		emailItem, err := h.repo.GetEmail(ctx, accountID, emailID)
		if err != nil {
			if errors.Is(err, email.ErrEmailNotFound) {
				notFound = append(notFound, emailID)
				continue
			}
			// Repository error
			logger.ErrorContext(ctx, "Failed to get email",
				slog.String("account_id", accountID),
				slog.String("email_id", emailID),
				slog.String("error", err.Error()),
			)
			return errorResponse(request.ClientID, "serverFail", err.Error()), nil
		}

		// Transform email to response format
		emailMap := transformEmail(emailItem, properties)
		list = append(list, emailMap)
	}

	// Ensure slices are not nil (JMAP spec requires empty arrays, not null)
	if list == nil {
		list = []any{}
	}
	if notFound == nil {
		notFound = []any{}
	}

	// Get current state
	stateStr := "0"
	if h.stateRepo != nil {
		currentState, err := h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeEmail)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to get current state",
				slog.String("account_id", accountID),
				slog.String("error", err.Error()),
			)
			return errorResponse(request.ClientID, "serverFail", err.Error()), nil
		}
		stateStr = strconv.FormatInt(currentState, 10)
	}

	logger.InfoContext(ctx, "Email/get completed",
		slog.String("account_id", accountID),
		slog.Int("list_count", len(list)),
		slog.Int("not_found_count", len(notFound)),
	)

	return plugincontract.PluginInvocationResponse{
		MethodResponse: plugincontract.MethodResponse{
			Name: "Email/get",
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

// transformEmail converts an EmailItem to the JMAP response format.
// If properties is non-empty, only those properties are included.
func transformEmail(e *email.EmailItem, properties []string) map[string]any {
	// Build full email map
	full := map[string]any{
		"id":            e.EmailID,
		"blobId":        e.BlobID,
		"threadId":      e.ThreadID,
		"mailboxIds":    e.MailboxIDs,
		"keywords":      e.Keywords,
		"size":          e.Size,
		"receivedAt":    formatTime(e.ReceivedAt),
		"messageId":     e.MessageID,
		"inReplyTo":     e.InReplyTo,
		"references":    e.References,
		"from":          transformAddresses(e.From),
		"sender":        transformAddressesNullable(e.Sender),
		"to":            transformAddresses(e.To),
		"cc":            transformAddresses(e.CC),
		"bcc":           transformAddressesNullable(e.Bcc),
		"replyTo":       transformAddresses(e.ReplyTo),
		"subject":       e.Subject,
		"sentAt":        formatTime(e.SentAt),
		"hasAttachment": e.HasAttachment,
		"preview":       e.Preview,
		"bodyStructure": transformBodyPart(e.BodyStructure),
		"textBody":      transformBodyPartRefs(e.TextBody),
		"htmlBody":      transformBodyPartRefs(e.HTMLBody),
		"attachments":   transformBodyPartRefs(e.Attachments),
		"bodyValues":    map[string]any{}, // Always empty for v1
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

	return filtered
}

// transformAddresses converts EmailAddress slice to JMAP format.
func transformAddresses(addrs []email.EmailAddress) []map[string]any {
	if addrs == nil {
		return nil
	}
	result := make([]map[string]any, len(addrs))
	for i, addr := range addrs {
		result[i] = map[string]any{
			"name":  addr.Name,
			"email": addr.Email,
		}
	}
	return result
}

// transformAddressesNullable returns nil (JSON null) for empty address lists.
// Per RFC 8621, some address fields should be null when empty rather than an empty array.
func transformAddressesNullable(addrs []email.EmailAddress) any {
	if len(addrs) == 0 {
		return nil
	}
	return transformAddresses(addrs)
}

// transformBodyPart converts a BodyPart to JMAP format.
func transformBodyPart(bp email.BodyPart) map[string]any {
	result := map[string]any{
		"partId": bp.PartID,
		"type":   bp.Type,
		"size":   bp.Size,
	}
	if bp.BlobID != "" {
		result["blobId"] = bp.BlobID
	}
	if bp.Charset != "" {
		result["charset"] = bp.Charset
	}
	if bp.Disposition != "" {
		result["disposition"] = bp.Disposition
	}
	if bp.Name != "" {
		result["name"] = bp.Name
	}
	if len(bp.SubParts) > 0 {
		subParts := make([]map[string]any, len(bp.SubParts))
		for i, sub := range bp.SubParts {
			subParts[i] = transformBodyPart(sub)
		}
		result["subParts"] = subParts
	}
	return result
}

// transformBodyPartRefs converts string slice to body part reference format.
func transformBodyPartRefs(refs []string) []map[string]any {
	if refs == nil {
		return nil
	}
	result := make([]map[string]any, len(refs))
	for i, ref := range refs {
		result[i] = map[string]any{"partId": ref}
	}
	return result
}

// formatTime formats a time.Time to RFC3339 string.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
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
	repo := email.NewRepository(dynamoClient, tableName)
	stateRepo := state.NewRepository(dynamoClient, tableName, 7)

	h := newHandler(repo, stateRepo)
	lambda.Start(otellambda.InstrumentHandler(h.handle, xrayconfig.WithRecommendedOptions(tp)...))
}
