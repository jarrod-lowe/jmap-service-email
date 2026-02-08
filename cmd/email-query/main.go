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
	"github.com/jarrod-lowe/jmap-service-libs/plugincontract"
	"github.com/jarrod-lowe/jmap-service-libs/dbclient"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/filter"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailbox"
	"github.com/jarrod-lowe/jmap-service-email/internal/search"
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

// VectorSearcher performs vector-based semantic search.
type VectorSearcher interface {
	Search(ctx context.Context, accountID string, filter *email.QueryFilter, position, limit int) (*search.SearchResult, error)
}

// TokenQuerier queries address token entries.
type TokenQuerier interface {
	QueryTokens(ctx context.Context, accountID string, field email.TokenField, tokenPrefix string, limit int32, scanForward bool) ([]email.TokenQueryResult, error)
}

// EmailFilter filters email IDs by applying structural filters.
type EmailFilter interface {
	FilterEmailIDs(ctx context.Context, accountID string, emailIDs []string, filter *email.QueryFilter) ([]string, error)
}

// handler implements the Email/query logic.
type handler struct {
	emailRepo      EmailRepository
	mailboxRepo    MailboxChecker
	vectorSearcher VectorSearcher
	tokenQuerier   TokenQuerier
	emailFilter    EmailFilter
}

// newHandler creates a new handler.
func newHandler(emailRepo EmailRepository, mailboxRepo MailboxChecker, vectorSearcher VectorSearcher, tokenQuerier TokenQuerier, emailFilter EmailFilter) *handler {
	return &handler{
		emailRepo:      emailRepo,
		mailboxRepo:    mailboxRepo,
		vectorSearcher: vectorSearcher,
		tokenQuerier:   tokenQuerier,
		emailFilter:    emailFilter,
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
	accountID := request.Args.StringOr("accountId", request.AccountID)

	// Parse filter
	var queryFilter *email.QueryFilter
	if filterArg, ok := request.Args.Object("filter"); ok {
		queryFilter = &email.QueryFilter{}
		if err := h.parseFilter(ctx, accountID, filterArg, queryFilter); err != nil {
			return errorResponse(request.ClientID, err.(*jmaperror.MethodError)), nil
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
			sortArgs := plugincontract.Args(sortMap)
			property := sortArgs.StringOr("property", "")
			if property != "" && property != "receivedAt" {
				return errorResponse(request.ClientID, jmaperror.UnsupportedSort("Only receivedAt sort is supported")), nil
			}
			comparators = append(comparators, email.Comparator{
				Property:    property,
				IsAscending: sortArgs.BoolOr("isAscending", false),
			})
		}
	}

	// Parse position
	position := int(request.Args.IntOr("position", 0))

	// Parse anchor
	anchor, hasAnchor := request.Args.String("anchor")
	if hasAnchor {
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
	anchorOffset := int(request.Args.IntOr("anchorOffset", 0))

	// Parse limit
	limit := int(request.Args.IntOr("limit", int64(defaultLimit)))
	if limit > maxLimit {
		limit = maxLimit
	}
	if limit < 0 {
		limit = 0
	}

	// Parse collapseThreads
	collapseThreads := request.Args.BoolOr("collapseThreads", false)

	// Execute query via appropriate path
	var result *email.QueryResult
	var queryErr error

	if queryFilter != nil && queryFilter.NeedsVectorSearch() && h.vectorSearcher != nil {
		// Vector search path: text/body/subject filters
		searchResult, err := h.vectorSearcher.Search(ctx, accountID, queryFilter, position, limit)
		if err != nil {
			queryErr = err
		} else {
			result = &email.QueryResult{
				IDs:      searchResult.IDs,
				Position: searchResult.Position,
			}
		}
	} else if queryFilter != nil && queryFilter.HasAddressFilter() && !queryFilter.NeedsVectorSearch() && h.tokenQuerier != nil {
		// Address filter path: from/to/cc/bcc via TOK# entries + structural join
		ids, err := h.queryByAddress(ctx, accountID, queryFilter, position, limit)
		if err != nil {
			queryErr = err
		} else {
			result = &email.QueryResult{
				IDs:      ids,
				Position: position,
			}
		}
	} else {
		// DynamoDB path: structural filters only (or no filter)
		queryReq := &email.QueryRequest{
			Filter:          queryFilter,
			Sort:            comparators,
			Position:        position,
			Anchor:          anchor,
			AnchorOffset:    anchorOffset,
			Limit:           limit,
			CollapseThreads: collapseThreads,
		}
		result, queryErr = h.emailRepo.QueryEmails(ctx, accountID, queryReq)
	}

	if queryErr != nil {
		logger.ErrorContext(ctx, "Failed to query emails",
			slog.String("account_id", accountID),
			slog.String("error", queryErr.Error()),
		)
		return errorResponse(request.ClientID, jmaperror.ServerFail(queryErr.Error(), queryErr)), nil
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

// supportedFilterKeys defines all filter properties this handler supports.
var supportedFilterKeys = map[string]bool{
	"inMailbox":        true,
	"inMailboxOtherThan": true,
	"before":           true,
	"after":            true,
	"minSize":          true,
	"maxSize":          true,
	"hasAttachment":    true,
	"hasKeyword":       true,
	"notKeyword":       true,
	"from":             true,
	"to":               true,
	"cc":               true,
	"bcc":              true,
	"text":             true,
	"body":             true,
	"subject":          true,
	"summary":          true,
}

// unsupportedFilterKeys are filter properties that we explicitly reject.
var unsupportedFilterKeys = map[string]string{
	"header":                    "header filter is not supported",
	"allInThreadHaveKeyword":    "thread keyword filters are not supported",
	"someInThreadHaveKeyword":   "thread keyword filters are not supported",
	"noneInThreadHaveKeyword":   "thread keyword filters are not supported",
}

// parseFilter populates a QueryFilter from the filter args.
// Returns a *jmaperror.MethodError on validation failure.
func (h *handler) parseFilter(ctx context.Context, accountID string, filterArg plugincontract.Args, f *email.QueryFilter) error {
	if filter.IsFilterOperator(filterArg) {
		flattened, err := filter.FlattenFilter(filterArg)
		if err != nil {
			return err
		}
		filterArg = flattened
	}

	for key := range filterArg {
		if !supportedFilterKeys[key] {
			if msg, unsupported := unsupportedFilterKeys[key]; unsupported {
				return jmaperror.UnsupportedFilter(msg)
			}
			return jmaperror.UnsupportedFilter("Unknown filter property: " + key)
		}
	}

	// Structural filters
	if inMailbox, ok := filterArg.String("inMailbox"); ok {
		exists, err := h.mailboxRepo.MailboxExists(ctx, accountID, inMailbox)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to check mailbox existence",
				slog.String("account_id", accountID),
				slog.String("mailbox_id", inMailbox),
				slog.String("error", err.Error()),
			)
			return jmaperror.ServerFail(err.Error(), err)
		}
		if !exists {
			return jmaperror.InvalidArguments("Mailbox not found: " + inMailbox)
		}
		f.InMailbox = inMailbox
	}
	if ids, ok := filterArg.StringSlice("inMailboxOtherThan"); ok {
		f.InMailboxOtherThan = ids
	}
	if before, ok := filterArg.String("before"); ok {
		t, err := time.Parse(time.RFC3339, before)
		if err != nil {
			return jmaperror.InvalidArguments("Invalid before date: " + before)
		}
		f.Before = &t
	}
	if after, ok := filterArg.String("after"); ok {
		t, err := time.Parse(time.RFC3339, after)
		if err != nil {
			return jmaperror.InvalidArguments("Invalid after date: " + after)
		}
		f.After = &t
	}
	if minSize, ok := filterArg.Int("minSize"); ok {
		s := int64(minSize)
		f.MinSize = &s
	}
	if maxSize, ok := filterArg.Int("maxSize"); ok {
		s := int64(maxSize)
		f.MaxSize = &s
	}
	if hasAttachment, ok := filterArg.Bool("hasAttachment"); ok {
		f.HasAttachment = &hasAttachment
	}
	if hasKeyword, ok := filterArg.String("hasKeyword"); ok {
		f.HasKeyword = hasKeyword
	}
	if notKeyword, ok := filterArg.String("notKeyword"); ok {
		f.NotKeyword = notKeyword
	}

	// Address filters
	if from, ok := filterArg.String("from"); ok {
		f.From = from
	}
	if to, ok := filterArg.String("to"); ok {
		f.To = to
	}
	if cc, ok := filterArg.String("cc"); ok {
		f.CC = cc
	}
	if bcc, ok := filterArg.String("bcc"); ok {
		f.Bcc = bcc
	}

	// Content filters
	if text, ok := filterArg.String("text"); ok {
		f.Text = text
	}
	if body, ok := filterArg.String("body"); ok {
		f.Body = body
	}
	if subject, ok := filterArg.String("subject"); ok {
		f.Subject = subject
	}
	if summaryVal, ok := filterArg.String("summary"); ok {
		f.Summary = summaryVal
	}

	return nil
}

// queryByAddress handles the address filter path.
// It queries TOK# entries for address matches, then optionally filters by structural conditions.
func (h *handler) queryByAddress(ctx context.Context, accountID string, filter *email.QueryFilter, position, limit int) ([]string, error) {
	field, searchValue := extractAddressFilter(filter)
	if searchValue == "" {
		return []string{}, nil
	}

	normalized := email.NormalizeSearchQuery(searchValue)

	// Over-fetch to have headroom after structural filtering + pagination
	tokenLimit := int32((position + limit) * 4)
	if tokenLimit < 100 {
		tokenLimit = 100
	}

	results, err := h.tokenQuerier.QueryTokens(ctx, accountID, field, normalized, tokenLimit, false)
	if err != nil {
		return nil, err
	}

	emailIDs := make([]string, 0, len(results))
	for _, r := range results {
		emailIDs = append(emailIDs, r.EmailID)
	}

	// Apply structural filters if available
	if h.emailFilter != nil {
		emailIDs, err = h.emailFilter.FilterEmailIDs(ctx, accountID, emailIDs, filter)
		if err != nil {
			return nil, err
		}
	}

	// Apply pagination
	startIdx := position
	if startIdx > len(emailIDs) {
		startIdx = len(emailIDs)
	}
	endIdx := startIdx + limit
	if endIdx > len(emailIDs) {
		endIdx = len(emailIDs)
	}

	return emailIDs[startIdx:endIdx], nil
}

// extractAddressFilter returns the first address field and its value from the filter.
func extractAddressFilter(filter *email.QueryFilter) (email.TokenField, string) {
	if filter.From != "" {
		return email.TokenFieldFrom, filter.From
	}
	if filter.To != "" {
		return email.TokenFieldTo, filter.To
	}
	if filter.CC != "" {
		return email.TokenFieldCC, filter.CC
	}
	if filter.Bcc != "" {
		return email.TokenFieldBcc, filter.Bcc
	}
	return "", ""
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
	tokenRepo := email.NewTokenRepository(dynamoClient, tableName)

	// vectorSearcher is nil until S3 Vectors + Bedrock infrastructure is deployed
	h := newHandler(emailRepo, mailboxRepo, nil, tokenRepo, emailRepo)
	result.Start(h.handle)
}
