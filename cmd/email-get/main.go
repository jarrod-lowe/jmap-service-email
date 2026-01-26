// Package main implements the Email/get Lambda handler.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/textproto"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/blob"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/headers"
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

// BlobStreamer defines the interface for streaming blob content.
type BlobStreamer interface {
	Stream(ctx context.Context, accountID, blobID string) (io.ReadCloser, error)
}

// handler implements the Email/get logic.
type handler struct {
	repo         EmailRepository
	stateRepo    StateRepository
	blobStreamer BlobStreamer
}

// newHandler creates a new handler.
func newHandler(repo EmailRepository, stateRepo StateRepository, blobStreamer BlobStreamer) *handler {
	return &handler{
		repo:         repo,
		stateRepo:    stateRepo,
		blobStreamer: blobStreamer,
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
	var headerProps []*headers.HeaderProperty
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
			// Parse and validate header:* properties
			if headers.IsHeaderProperty(prop) {
				headerProp, err := headers.ParseHeaderProperty(prop)
				if err != nil {
					return errorResponse(request.ClientID, "invalidArguments", fmt.Sprintf("invalid header property %q: %v", prop, err)), nil
				}
				// Validate form is allowed for this header
				if err := headers.ValidateForm(headerProp.Name, headerProp.Form); err != nil {
					return errorResponse(request.ClientID, "invalidArguments", err.Error()), nil
				}
				headerProps = append(headerProps, headerProp)
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

		// Fetch raw headers if header:* properties requested
		var rawHeaders textproto.MIMEHeader
		if len(headerProps) > 0 && h.blobStreamer != nil && emailItem.HeaderSize > 0 {
			// Build range blob ID: {blobId},0,{headerSize}
			rangeBlobID := fmt.Sprintf("%s,0,%d", emailItem.BlobID, emailItem.HeaderSize)
			headerReader, err := h.blobStreamer.Stream(ctx, accountID, rangeBlobID)
			if err != nil {
				logger.WarnContext(ctx, "Failed to fetch headers",
					slog.String("email_id", emailID),
					slog.String("error", err.Error()),
				)
			} else {
				rawHeaders, _ = textproto.NewReader(bufio.NewReader(headerReader)).ReadMIMEHeader()
				headerReader.Close()
			}
		}

		// Transform email to response format
		emailMap := transformEmail(emailItem, properties, headerProps, rawHeaders)
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

// ensureMap returns an empty map if the input is nil.
// This ensures JSON serialization produces {} instead of null.
func ensureMap(m map[string]bool) map[string]bool {
	if m == nil {
		return map[string]bool{}
	}
	return m
}

// transformEmail converts an EmailItem to the JMAP response format.
// If properties is non-empty, only those properties are included.
func transformEmail(e *email.EmailItem, properties []string, headerProps []*headers.HeaderProperty, rawHeaders textproto.MIMEHeader) map[string]any {
	// Build full email map
	full := map[string]any{
		"id":            e.EmailID,
		"blobId":        e.BlobID,
		"threadId":      e.ThreadID,
		"mailboxIds":    e.MailboxIDs,
		"keywords":      ensureMap(e.Keywords),
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

	// Add header:* properties if requested
	for _, hp := range headerProps {
		propName := buildHeaderPropertyName(hp)
		full[propName] = getHeaderValue(rawHeaders, hp)
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

	// RFC 8621 Section 4.1: id is always returned regardless of properties list
	filtered["id"] = full["id"]

	return filtered
}

// buildHeaderPropertyName constructs the property name for a header property.
func buildHeaderPropertyName(hp *headers.HeaderProperty) string {
	name := "header:" + hp.Name
	switch hp.Form {
	case headers.FormText:
		name += ":asText"
	case headers.FormAddresses:
		name += ":asAddresses"
	case headers.FormGroupedAddresses:
		name += ":asGroupedAddresses"
	case headers.FormMessageIds:
		name += ":asMessageIds"
	case headers.FormDate:
		name += ":asDate"
	case headers.FormURLs:
		name += ":asURLs"
	// FormRaw is default, no suffix needed
	}
	if hp.All {
		name += ":all"
	}
	return name
}

// getHeaderValue retrieves and transforms a header value based on the header property.
func getHeaderValue(rawHeaders textproto.MIMEHeader, hp *headers.HeaderProperty) any {
	if rawHeaders == nil {
		return nil
	}

	// Get header values (case-insensitive via textproto)
	values := rawHeaders.Values(hp.Name)
	if len(values) == 0 {
		// RFC 8621 Section 4.1.3: "If no header fields exist in the message
		// with the requested name, the value is null if fetching a single
		// instance or an empty array if requesting :all."
		if hp.All {
			return []any{}
		}
		return nil
	}

	// If :all suffix, return array of all values
	if hp.All {
		results := make([]any, len(values))
		for i, v := range values {
			result, _ := headers.ApplyForm(v, hp.Form)
			results[i] = result
		}
		return results
	}

	// Otherwise, return last value (per RFC 8621)
	lastValue := values[len(values)-1]
	result, _ := headers.ApplyForm(lastValue, hp.Form)
	return result
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
	coreAPIURL := os.Getenv("CORE_API_URL")

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

	// Create blob client with SigV4 signing for header:* properties
	transport := blob.NewSigV4Transport(http.DefaultTransport, cfg.Credentials, cfg.Region)
	httpClient := &http.Client{Transport: transport}
	blobClient := blob.NewHTTPBlobClient(coreAPIURL, httpClient)

	h := newHandler(repo, stateRepo, blobClient)
	lambda.Start(otellambda.InstrumentHandler(h.handle, xrayconfig.WithRecommendedOptions(tp)...))
}
