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
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jarrod-lowe/jmap-service-libs/plugincontract"
	"github.com/jarrod-lowe/jmap-service-libs/dbclient"
	"github.com/jarrod-lowe/jmap-service-email/internal/blob"
	"github.com/jarrod-lowe/jmap-service-email/internal/charset"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/headers"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
	"github.com/jarrod-lowe/jmap-service-libs/awsinit"
	"github.com/jarrod-lowe/jmap-service-libs/jmaperror"
	"github.com/jarrod-lowe/jmap-service-libs/logging"
	"github.com/jarrod-lowe/jmap-service-libs/tracing"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// defaultMaxBodyValueBytes is the fallback if MAX_BODY_VALUE_BYTES env var is not set.
const defaultMaxBodyValueBytes = 256 * 1024

var logger = logging.New()

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
	repo                    EmailRepository
	stateRepo               StateRepository
	blobStreamerFactory     func(baseURL string) BlobStreamer
	serverMaxBodyValueBytes int
}

// newHandler creates a new handler.
func newHandler(repo EmailRepository, stateRepo StateRepository, blobStreamerFactory func(baseURL string) BlobStreamer, serverMaxBodyValueBytes int) *handler {
	return &handler{
		repo:                    repo,
		stateRepo:               stateRepo,
		blobStreamerFactory:     blobStreamerFactory,
		serverMaxBodyValueBytes: serverMaxBodyValueBytes,
	}
}

// handle processes an Email/get request.
func (h *handler) handle(ctx context.Context, request plugincontract.PluginInvocationRequest) (plugincontract.PluginInvocationResponse, error) {
	tracer := tracing.Tracer("jmap-email-get")
	ctx, span := tracer.Start(ctx, "EmailGetHandler")
	defer span.End()

	// Check method
	if request.Method != "Email/get" {
		return errorResponse(request.ClientID, jmaperror.UnknownMethod("This handler only supports Email/get")), nil
	}

	// Parse request args
	accountID := request.Args.StringOr("accountId", request.AccountID)

	// Extract and validate ids
	if !request.Args.Has("ids") {
		return errorResponse(request.ClientID, jmaperror.InvalidArguments("ids argument is required")), nil
	}

	ids, ok := request.Args.StringSlice("ids")
	if !ok {
		return errorResponse(request.ClientID, jmaperror.InvalidArguments("ids must be an array of strings")), nil
	}

	// Extract and validate properties (optional)
	var properties []string
	var headerProps []*headers.HeaderProperty
	if request.Args.Has("properties") {
		props, ok := request.Args.StringSlice("properties")
		if !ok {
			return errorResponse(request.ClientID, jmaperror.InvalidArguments("properties must be an array of strings")), nil
		}
		for _, prop := range props {
			// Parse and validate header:* properties
			if headers.IsHeaderProperty(prop) {
				headerProp, err := headers.ParseHeaderProperty(prop)
				if err != nil {
					return errorResponse(request.ClientID, jmaperror.InvalidArguments(fmt.Sprintf("invalid header property %q: %v", prop, err))), nil
				}
				// Validate form is allowed for this header
				if err := headers.ValidateForm(headerProp.Name, headerProp.Form); err != nil {
					return errorResponse(request.ClientID, jmaperror.InvalidArguments(err.Error())), nil
				}
				headerProps = append(headerProps, headerProp)
			}
			properties = append(properties, prop)
		}
	}

	// Parse bodyProperties (optional - defaults handled in resolveBodyPartRefs)
	var bodyProperties []string
	if request.Args.Has("bodyProperties") {
		bp, ok := request.Args.StringSlice("bodyProperties")
		if !ok {
			return errorResponse(request.ClientID, jmaperror.InvalidArguments("bodyProperties must be an array of strings")), nil
		}
		bodyProperties = bp
	}

	// Parse fetch body values flags
	fetchTextBodyValues := request.Args.BoolOr("fetchTextBodyValues", false)
	fetchHTMLBodyValues := request.Args.BoolOr("fetchHTMLBodyValues", false)
	fetchAllBodyValues := request.Args.BoolOr("fetchAllBodyValues", false)

	// Parse maxBodyValueBytes (default to server max if not specified or invalid)
	maxBodyValueBytes := int(request.Args.IntOr("maxBodyValueBytes", int64(h.serverMaxBodyValueBytes)))
	if maxBodyValueBytes <= 0 || maxBodyValueBytes > h.serverMaxBodyValueBytes {
		maxBodyValueBytes = h.serverMaxBodyValueBytes
	}

	// Create blob streamer from request APIURL
	var blobStreamer BlobStreamer
	if h.blobStreamerFactory != nil && request.APIURL != "" {
		blobStreamer = h.blobStreamerFactory(request.APIURL)
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
			return errorResponse(request.ClientID, jmaperror.ServerFail(err.Error(), err)), nil
		}

		// Treat soft-deleted emails as not found
		if emailItem.DeletedAt != nil {
			notFound = append(notFound, emailID)
			continue
		}

		// Fetch raw headers if header:* properties requested
		var rawHeaders textproto.MIMEHeader
		if len(headerProps) > 0 && blobStreamer != nil && emailItem.HeaderSize > 0 {
			// Build range blob ID: {blobId},0,{headerSize}
			rangeBlobID := fmt.Sprintf("%s,0,%d", emailItem.BlobID, emailItem.HeaderSize)
			headerReader, err := blobStreamer.Stream(ctx, accountID, rangeBlobID)
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
		emailMap := h.transformEmail(ctx, accountID, emailItem, properties, bodyProperties, headerProps, rawHeaders, fetchTextBodyValues, fetchHTMLBodyValues, fetchAllBodyValues, maxBodyValueBytes, blobStreamer)
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
			return errorResponse(request.ClientID, jmaperror.ServerFail(err.Error(), err)), nil
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

// collectBodyPartIDs collects part IDs based on fetch flags.
func collectBodyPartIDs(e *email.EmailItem, fetchText, fetchHTML, fetchAll bool) []string {
	seen := make(map[string]bool)
	var partIDs []string

	if fetchText {
		for _, partID := range e.TextBody {
			if !seen[partID] {
				seen[partID] = true
				partIDs = append(partIDs, partID)
			}
		}
	}

	if fetchHTML {
		// Use htmlBody if available, otherwise fall back to textBody
		htmlParts := e.HTMLBody
		if len(htmlParts) == 0 {
			htmlParts = e.TextBody
		}
		for _, partID := range htmlParts {
			if !seen[partID] {
				seen[partID] = true
				partIDs = append(partIDs, partID)
			}
		}
	}

	if fetchAll {
		// Add all text/* parts from bodyStructure
		collectAllTextPartIDs(e.BodyStructure, seen, &partIDs)
	}

	return partIDs
}

// collectAllTextPartIDs recursively collects part IDs for all text/* parts.
func collectAllTextPartIDs(bp email.BodyPart, seen map[string]bool, partIDs *[]string) {
	if strings.HasPrefix(bp.Type, "text/") {
		if !seen[bp.PartID] {
			seen[bp.PartID] = true
			*partIDs = append(*partIDs, bp.PartID)
		}
	}
	for _, sub := range bp.SubParts {
		collectAllTextPartIDs(sub, seen, partIDs)
	}
}

// fetchBodyValue fetches and decodes the content of a body part.
func fetchBodyValue(ctx context.Context, blobStreamer BlobStreamer, accountID string, part *email.BodyPart, maxBytes int) (value string, isTruncated bool, isEncodingProblem bool) {
	if blobStreamer == nil || part == nil || part.BlobID == "" {
		return "", false, true
	}

	reader, err := blobStreamer.Stream(ctx, accountID, part.BlobID)
	if err != nil {
		logger.WarnContext(ctx, "Failed to stream body part",
			slog.String("blob_id", part.BlobID),
			slog.String("error", err.Error()),
		)
		return "", false, true
	}
	defer reader.Close()

	// Decode charset
	decodedReader, encodingProblem, err := charset.DecodeReader(reader, part.Charset)
	if err != nil {
		logger.WarnContext(ctx, "Failed to decode charset",
			slog.String("blob_id", part.BlobID),
			slog.String("charset", part.Charset),
			slog.String("error", err.Error()),
		)
		return "", false, true
	}

	// Read up to maxBytes + 1 to detect truncation
	buf := make([]byte, maxBytes+1)
	n, err := io.ReadFull(decodedReader, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		logger.WarnContext(ctx, "Failed to read body part",
			slog.String("blob_id", part.BlobID),
			slog.String("error", err.Error()),
		)
		return "", false, true
	}

	// Check if truncated
	if n > maxBytes {
		return string(buf[:maxBytes]), true, encodingProblem
	}

	return string(buf[:n]), false, encodingProblem
}

// buildBodyValues creates bodyValues entries by fetching actual content.
func buildBodyValues(ctx context.Context, blobStreamer BlobStreamer, accountID string, e *email.EmailItem, fetchText, fetchHTML, fetchAll bool, maxBytes int) map[string]any {
	result := map[string]any{}

	partIDs := collectBodyPartIDs(e, fetchText, fetchHTML, fetchAll)

	for _, partID := range partIDs {
		part := email.FindBodyPart(e.BodyStructure, partID)
		value, isTruncated, isEncodingProblem := fetchBodyValue(ctx, blobStreamer, accountID, part, maxBytes)
		result[partID] = map[string]any{
			"value":             value,
			"isTruncated":       isTruncated,
			"isEncodingProblem": isEncodingProblem,
		}
	}

	return result
}

// transformEmail converts an EmailItem to the JMAP response format.
// If properties is non-empty, only those properties are included.
func (h *handler) transformEmail(ctx context.Context, accountID string, e *email.EmailItem, properties []string, bodyProperties []string, headerProps []*headers.HeaderProperty, rawHeaders textproto.MIMEHeader, fetchText, fetchHTML, fetchAll bool, maxBodyValueBytes int, blobStreamer BlobStreamer) map[string]any {
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
		"textBody":      resolveBodyPartRefs(e.TextBody, e.BodyStructure, bodyProperties),
		"htmlBody":      resolveBodyPartRefs(e.HTMLBody, e.BodyStructure, bodyProperties),
		"attachments":   resolveBodyPartRefs(e.Attachments, e.BodyStructure, bodyProperties),
		"bodyValues":    buildBodyValues(ctx, blobStreamer, accountID, e, fetchText, fetchHTML, fetchAll, maxBodyValueBytes),
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

// defaultBodyProperties lists the default properties returned for body part
// references per RFC 8621 Section 4.1.4 (all EmailBodyPart properties except subParts).
var defaultBodyProperties = []string{
	"partId", "blobId", "size", "name", "type",
	"charset", "disposition", "cid", "language", "location",
}

// resolveBodyPartRefs resolves part ID references to full EmailBodyPart objects
// by looking them up in the bodyStructure tree. If bodyProperties is nil, all
// default leaf-part properties are included. Falls back to {"partId": ref} if
// a part is not found in the tree.
func resolveBodyPartRefs(refs []string, bodyStructure email.BodyPart, bodyProperties []string) []map[string]any {
	if refs == nil {
		return nil
	}

	if bodyProperties == nil {
		bodyProperties = defaultBodyProperties
	}

	// Build a set for fast lookup
	propSet := make(map[string]bool, len(bodyProperties))
	for _, p := range bodyProperties {
		propSet[p] = true
	}

	result := make([]map[string]any, len(refs))
	for i, ref := range refs {
		part := email.FindBodyPart(bodyStructure, ref)
		if part == nil {
			result[i] = map[string]any{"partId": ref}
			continue
		}

		// Build full map of all leaf-part properties (no subParts)
		full := map[string]any{
			"partId": part.PartID,
			"type":   part.Type,
			"size":   part.Size,
		}
		if part.BlobID != "" {
			full["blobId"] = part.BlobID
		}
		if part.Charset != "" {
			full["charset"] = part.Charset
		}
		if part.Disposition != "" {
			full["disposition"] = part.Disposition
		}
		if part.Name != "" {
			full["name"] = part.Name
		}

		// Filter to requested bodyProperties
		filtered := make(map[string]any, len(bodyProperties))
		for _, key := range bodyProperties {
			if val, ok := full[key]; ok {
				filtered[key] = val
			}
		}
		result[i] = filtered
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

	// Load config from environment
	tableName := os.Getenv("EMAIL_TABLE_NAME")

	// Parse max body value bytes from environment (default if not set or invalid)
	serverMaxBodyValueBytes := defaultMaxBodyValueBytes
	if maxBytesStr := os.Getenv("MAX_BODY_VALUE_BYTES"); maxBytesStr != "" {
		if parsed, err := strconv.Atoi(maxBytesStr); err == nil && parsed > 0 {
			serverMaxBodyValueBytes = parsed
		}
	}

	// Create DynamoDB client
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

	repo := email.NewRepository(dynamoClient, tableName)
	stateRepo := state.NewRepository(dynamoClient, tableName, 7)

	// Create blob client with OTel instrumentation and SigV4 signing for header:* properties
	baseTransport := otelhttp.NewTransport(http.DefaultTransport)
	transport := blob.NewSigV4Transport(baseTransport, result.Config.Credentials, result.Config.Region)
	httpClient := &http.Client{Transport: transport}

	factory := func(baseURL string) BlobStreamer {
		return blob.NewHTTPBlobClient(baseURL, httpClient)
	}

	h := newHandler(repo, stateRepo, factory, serverMaxBodyValueBytes)
	result.Start(h.handle)
}
