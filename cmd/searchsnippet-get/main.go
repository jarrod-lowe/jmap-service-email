// Package main implements the SearchSnippet/get Lambda handler.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/filter"
	"github.com/jarrod-lowe/jmap-service-email/internal/snippet"
	"github.com/jarrod-lowe/jmap-service-libs/awsinit"
	"github.com/jarrod-lowe/jmap-service-libs/dbclient"
	"github.com/jarrod-lowe/jmap-service-libs/jmaperror"
	"github.com/jarrod-lowe/jmap-service-libs/logging"
	"github.com/jarrod-lowe/jmap-service-libs/plugincontract"
	"github.com/jarrod-lowe/jmap-service-libs/tracing"
)

const maxEmailIds = 100

var logger = logging.New()

// EmailRepository defines the interface for retrieving emails.
type EmailRepository interface {
	GetEmail(ctx context.Context, accountID, emailID string) (*email.EmailItem, error)
}

// handler implements the SearchSnippet/get logic.
type handler struct {
	repo EmailRepository
}

// newHandler creates a new handler.
func newHandler(repo EmailRepository) *handler {
	return &handler{repo: repo}
}

// searchTerms holds the extracted search terms from the filter, split by target field.
type searchTerms struct {
	subject []string
	preview []string
}

// supportedFilterKeys defines all filter properties this handler accepts.
var supportedFilterKeys = map[string]bool{
	"inMailbox":          true,
	"inMailboxOtherThan": true,
	"before":             true,
	"after":              true,
	"minSize":            true,
	"maxSize":            true,
	"hasAttachment":      true,
	"hasKeyword":         true,
	"notKeyword":         true,
	"from":               true,
	"to":                 true,
	"cc":                 true,
	"bcc":                true,
	"text":               true,
	"body":               true,
	"subject":            true,
}

// unsupportedFilterKeys are filter properties that we explicitly reject.
var unsupportedFilterKeys = map[string]string{
	"header":                  "header filter is not supported",
	"allInThreadHaveKeyword":  "thread keyword filters are not supported",
	"someInThreadHaveKeyword": "thread keyword filters are not supported",
	"noneInThreadHaveKeyword": "thread keyword filters are not supported",
}

// handle processes a SearchSnippet/get request.
func (h *handler) handle(ctx context.Context, request plugincontract.PluginInvocationRequest) (plugincontract.PluginInvocationResponse, error) {
	tracer := tracing.Tracer("jmap-searchsnippet-get")
	ctx, span := tracer.Start(ctx, "SearchSnippetGetHandler")
	defer span.End()

	if request.Method != "SearchSnippet/get" {
		return errorResponse(request.ClientID, jmaperror.UnknownMethod("This handler only supports SearchSnippet/get")), nil
	}

	accountID := request.Args.StringOr("accountId", request.AccountID)

	// Extract emailIds (required)
	if !request.Args.Has("emailIds") {
		return errorResponse(request.ClientID, jmaperror.InvalidArguments("emailIds argument is required")), nil
	}
	emailIds, ok := request.Args.StringSlice("emailIds")
	if !ok {
		return errorResponse(request.ClientID, jmaperror.InvalidArguments("emailIds must be an array of strings")), nil
	}

	if len(emailIds) > maxEmailIds {
		return errorResponse(request.ClientID, &jmaperror.MethodError{
			ErrType:     "requestTooLarge",
			Description: "Too many emailIds; maximum is 100",
		}), nil
	}

	// Extract and validate filter
	terms, err := extractSearchTerms(request.Args)
	if err != nil {
		return errorResponse(request.ClientID, err.(*jmaperror.MethodError)), nil
	}

	// Fetch emails and build snippets
	var list []any
	var notFound []string

	for _, emailID := range emailIds {
		emailItem, err := h.repo.GetEmail(ctx, accountID, emailID)
		if err != nil {
			if errors.Is(err, email.ErrEmailNotFound) {
				notFound = append(notFound, emailID)
				continue
			}
			logger.ErrorContext(ctx, "Failed to get email",
				slog.String("account_id", accountID),
				slog.String("email_id", emailID),
				slog.String("error", err.Error()),
			)
			return errorResponse(request.ClientID, jmaperror.ServerFail(err.Error(), err)), nil
		}

		if emailItem.DeletedAt != nil {
			notFound = append(notFound, emailID)
			continue
		}

		snippetMap := map[string]any{
			"emailId": emailID,
			"subject": snippet.Highlight(emailItem.Subject, terms.subject),
			"preview": snippet.HighlightPreview(emailItem.Preview, terms.preview),
		}
		list = append(list, snippetMap)
	}

	if list == nil {
		list = []any{}
	}

	var notFoundResult any
	if len(notFound) > 0 {
		notFoundResult = notFound
	}

	logger.InfoContext(ctx, "SearchSnippet/get completed",
		slog.String("account_id", accountID),
		slog.Int("list_count", len(list)),
	)

	return plugincontract.PluginInvocationResponse{
		MethodResponse: plugincontract.MethodResponse{
			Name: "SearchSnippet/get",
			Args: map[string]any{
				"accountId": accountID,
				"list":      list,
				"notFound":  notFoundResult,
			},
			ClientID: request.ClientID,
		},
	}, nil
}

// extractSearchTerms parses the filter from request args and returns search terms.
// Returns a *jmaperror.MethodError if the filter contains unsupported keys.
func extractSearchTerms(args plugincontract.Args) (*searchTerms, error) {
	terms := &searchTerms{}

	filterArg, ok := args.Object("filter")
	if !ok {
		return terms, nil
	}

	if filter.IsFilterOperator(filterArg) {
		flattened, err := filter.FlattenFilter(filterArg)
		if err != nil {
			return nil, err
		}
		filterArg = flattened
	}

	// Validate filter keys
	for key := range filterArg {
		if !supportedFilterKeys[key] {
			if msg, unsupported := unsupportedFilterKeys[key]; unsupported {
				return nil, jmaperror.UnsupportedFilter(msg)
			}
			return nil, jmaperror.UnsupportedFilter("Unknown filter property: " + key)
		}
	}

	// Extract text search terms
	if text, ok := filterArg.String("text"); ok && text != "" {
		terms.subject = append(terms.subject, text)
		terms.preview = append(terms.preview, text)
	}
	if subj, ok := filterArg.String("subject"); ok && subj != "" {
		terms.subject = append(terms.subject, subj)
	}
	if body, ok := filterArg.String("body"); ok && body != "" {
		terms.preview = append(terms.preview, body)
	}

	return terms, nil
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
	h := newHandler(repo)
	result.Start(h.handle)
}
