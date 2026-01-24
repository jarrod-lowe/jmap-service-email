// Package main implements the Email/set Lambda handler.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailbox"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda/xrayconfig"
	"go.opentelemetry.io/otel"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

// EmailRepository defines the interface for email operations.
type EmailRepository interface {
	UpdateEmailMailboxes(ctx context.Context, accountID, emailID string, newMailboxIDs map[string]bool) (oldMailboxIDs map[string]bool, email *email.EmailItem, err error)
	GetEmail(ctx context.Context, accountID, emailID string) (*email.EmailItem, error)
}

// MailboxRepository defines the interface for mailbox operations.
type MailboxRepository interface {
	MailboxExists(ctx context.Context, accountID, mailboxID string) (bool, error)
	IncrementCounts(ctx context.Context, accountID, mailboxID string, incrementUnread bool) error
	DecrementCounts(ctx context.Context, accountID, mailboxID string, decrementUnread bool) error
}

// StateRepository defines the interface for state tracking operations.
type StateRepository interface {
	GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
	IncrementStateAndLogChange(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error)
}

// handler implements the Email/set logic.
type handler struct {
	emailRepo   EmailRepository
	mailboxRepo MailboxRepository
	stateRepo   StateRepository
}

// newHandler creates a new handler.
func newHandler(emailRepo EmailRepository, mailboxRepo MailboxRepository, stateRepo StateRepository) *handler {
	return &handler{
		emailRepo:   emailRepo,
		mailboxRepo: mailboxRepo,
		stateRepo:   stateRepo,
	}
}

// handle processes an Email/set request.
func (h *handler) handle(ctx context.Context, request plugincontract.PluginInvocationRequest) (plugincontract.PluginInvocationResponse, error) {
	tracer := otel.Tracer("jmap-email-set")
	ctx, span := tracer.Start(ctx, "EmailSetHandler")
	defer span.End()

	// Check method
	if request.Method != "Email/set" {
		return plugincontract.PluginInvocationResponse{
			MethodResponse: plugincontract.MethodResponse{
				Name: "error",
				Args: map[string]any{
					"type":        "unknownMethod",
					"description": "This handler only supports Email/set",
				},
				ClientID: request.ClientID,
			},
		}, nil
	}

	accountID := request.AccountID
	if argAccountID, ok := request.Args["accountId"].(string); ok {
		accountID = argAccountID
	}

	// Get old state
	oldState := int64(0)
	if h.stateRepo != nil {
		var err error
		oldState, err = h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeEmail)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to get current state",
				slog.String("account_id", accountID),
				slog.String("error", err.Error()),
			)
			return errorResponse(request.ClientID, "serverFail", err.Error()), nil
		}
	}

	// Check ifInState (handles both string and numeric values)
	if ifInStateRaw, ok := request.Args["ifInState"]; ok {
		var expectedState int64
		var parseErr error

		switch v := ifInStateRaw.(type) {
		case string:
			expectedState, parseErr = strconv.ParseInt(v, 10, 64)
		case float64:
			expectedState = int64(v)
		default:
			parseErr = errors.New("invalid ifInState type")
		}

		// If ifInState is provided but invalid, or doesn't match current state, return stateMismatch
		if parseErr != nil || expectedState != oldState {
			return plugincontract.PluginInvocationResponse{
				MethodResponse: plugincontract.MethodResponse{
					Name: "error",
					Args: map[string]any{
						"type":        "stateMismatch",
						"description": "State mismatch",
					},
					ClientID: request.ClientID,
				},
			}, nil
		}
	}

	created := make(map[string]any)
	notCreated := make(map[string]any)
	updated := make(map[string]any)
	notUpdated := make(map[string]any)
	destroyed := []any{}
	notDestroyed := make(map[string]any)
	newState := oldState
	affectedMailboxes := make(map[string]bool)

	// Handle create (not supported)
	if createArg, ok := request.Args["create"].(map[string]any); ok {
		for clientID := range createArg {
			notCreated[clientID] = setError("forbidden", "Email/set create is not supported. Use Email/import instead.")
		}
	}

	// Handle update
	if updateArg, ok := request.Args["update"].(map[string]any); ok {
		for emailID, updateData := range updateArg {
			data, ok := updateData.(map[string]any)
			if !ok {
				notUpdated[emailID] = setError("invalidArguments", "update data must be an object")
				continue
			}

			err := h.updateEmail(ctx, accountID, emailID, data, affectedMailboxes)
			if err != nil {
				notUpdated[emailID] = err
			} else {
				updated[emailID] = nil
				// Track state change for email
				if h.stateRepo != nil {
					if s, err := h.stateRepo.IncrementStateAndLogChange(ctx, accountID, state.ObjectTypeEmail, emailID, state.ChangeTypeUpdated); err != nil {
						logger.ErrorContext(ctx, "Failed to track email state change",
							slog.String("account_id", accountID),
							slog.String("email_id", emailID),
							slog.String("error", err.Error()),
						)
					} else {
						newState = s
					}
				}
			}
		}
	}

	// Track state changes for affected mailboxes
	if h.stateRepo != nil {
		for mailboxID := range affectedMailboxes {
			if s, err := h.stateRepo.IncrementStateAndLogChange(ctx, accountID, state.ObjectTypeMailbox, mailboxID, state.ChangeTypeUpdated); err != nil {
				logger.ErrorContext(ctx, "Failed to track mailbox state change",
					slog.String("account_id", accountID),
					slog.String("mailbox_id", mailboxID),
					slog.String("error", err.Error()),
				)
			} else {
				_ = s // Mailbox state tracked separately
			}
		}
	}

	// Handle destroy (not supported)
	if destroyArg, ok := request.Args["destroy"].([]any); ok {
		for _, id := range destroyArg {
			emailID, ok := id.(string)
			if !ok {
				continue
			}
			notDestroyed[emailID] = setError("forbidden", "Email/set destroy is not supported")
		}
	}

	logger.InfoContext(ctx, "Email/set completed",
		slog.String("account_id", accountID),
		slog.Int("created_count", len(created)),
		slog.Int("updated_count", len(updated)),
		slog.Int("destroyed_count", len(destroyed)),
	)

	return plugincontract.PluginInvocationResponse{
		MethodResponse: plugincontract.MethodResponse{
			Name: "Email/set",
			Args: map[string]any{
				"accountId":    accountID,
				"oldState":     strconv.FormatInt(oldState, 10),
				"newState":     strconv.FormatInt(newState, 10),
				"created":      created,
				"updated":      updated,
				"destroyed":    destroyed,
				"notCreated":   notCreated,
				"notUpdated":   notUpdated,
				"notDestroyed": notDestroyed,
			},
			ClientID: request.ClientID,
		},
	}, nil
}

// updateEmail processes an email update.
func (h *handler) updateEmail(ctx context.Context, accountID, emailID string, data map[string]any, affectedMailboxes map[string]bool) map[string]any {
	// Check for unsupported properties
	for key := range data {
		if key == "keywords" || strings.HasPrefix(key, "keywords/") {
			return setError("invalidProperties", "keywords updates are not yet supported")
		}
		if key != "mailboxIds" && !strings.HasPrefix(key, "mailboxIds/") {
			return setError("invalidProperties", "only mailboxIds updates are supported")
		}
	}

	// Parse mailboxIds update (full replacement vs patch)
	newMailboxIDs, parseErr := h.parseMailboxIDsUpdate(ctx, accountID, emailID, data)
	if parseErr != nil {
		return parseErr
	}

	// Validate at least one mailbox
	if len(newMailboxIDs) == 0 {
		return setError("invalidProperties", "email must belong to at least one mailbox")
	}

	// Validate all mailboxes exist
	for mailboxID := range newMailboxIDs {
		exists, checkErr := h.mailboxRepo.MailboxExists(ctx, accountID, mailboxID)
		if checkErr != nil {
			logger.ErrorContext(ctx, "Failed to check mailbox exists",
				slog.String("account_id", accountID),
				slog.String("mailbox_id", mailboxID),
				slog.String("error", checkErr.Error()),
			)
			return setError("serverFail", checkErr.Error())
		}
		if !exists {
			return setError("invalidProperties", "mailbox does not exist: "+mailboxID)
		}
	}

	// Update email mailboxIds
	oldMailboxIDs, emailItem, updateErr := h.emailRepo.UpdateEmailMailboxes(ctx, accountID, emailID, newMailboxIDs)
	if updateErr != nil {
		if errors.Is(updateErr, email.ErrEmailNotFound) {
			return setError("notFound", "email not found")
		}
		logger.ErrorContext(ctx, "Failed to update email mailboxes",
			slog.String("account_id", accountID),
			slog.String("email_id", emailID),
			slog.String("error", updateErr.Error()),
		)
		return setError("serverFail", updateErr.Error())
	}

	// Determine if email is unread (doesn't have $seen keyword)
	isUnread := emailItem.Keywords == nil || !emailItem.Keywords["$seen"]

	// Update mailbox counters
	// Added mailboxes: increment count
	for mailboxID := range newMailboxIDs {
		if !oldMailboxIDs[mailboxID] {
			if err := h.mailboxRepo.IncrementCounts(ctx, accountID, mailboxID, isUnread); err != nil {
				logger.ErrorContext(ctx, "Failed to increment mailbox counts",
					slog.String("account_id", accountID),
					slog.String("mailbox_id", mailboxID),
					slog.String("error", err.Error()),
				)
			}
			affectedMailboxes[mailboxID] = true
		}
	}

	// Removed mailboxes: decrement count
	for mailboxID := range oldMailboxIDs {
		if !newMailboxIDs[mailboxID] {
			if err := h.mailboxRepo.DecrementCounts(ctx, accountID, mailboxID, isUnread); err != nil {
				logger.ErrorContext(ctx, "Failed to decrement mailbox counts",
					slog.String("account_id", accountID),
					slog.String("mailbox_id", mailboxID),
					slog.String("error", err.Error()),
				)
			}
			affectedMailboxes[mailboxID] = true
		}
	}

	return nil
}

// parseMailboxIDsUpdate parses the mailboxIds update data.
// Supports both full replacement and JMAP patch syntax.
func (h *handler) parseMailboxIDsUpdate(ctx context.Context, accountID, emailID string, data map[string]any) (map[string]bool, map[string]any) {
	// Check for full replacement
	if mailboxIDs, ok := data["mailboxIds"].(map[string]any); ok {
		result := make(map[string]bool)
		for k, v := range mailboxIDs {
			if b, ok := v.(bool); ok && b {
				result[k] = true
			}
		}
		return result, nil
	}

	// Check for patch syntax (mailboxIds/{id})
	hasPatch := false
	for key := range data {
		if strings.HasPrefix(key, "mailboxIds/") {
			hasPatch = true
			break
		}
	}

	if !hasPatch {
		// No mailboxIds update at all
		return nil, setError("invalidProperties", "no mailboxIds update found")
	}

	// Get current mailboxIds for the email
	emailItem, err := h.emailRepo.GetEmail(ctx, accountID, emailID)
	if err != nil {
		if errors.Is(err, email.ErrEmailNotFound) {
			return nil, setError("notFound", "email not found")
		}
		return nil, setError("serverFail", err.Error())
	}

	// Start with current mailboxIds
	result := make(map[string]bool)
	for k, v := range emailItem.MailboxIDs {
		result[k] = v
	}

	// Apply patches
	for key, value := range data {
		if !strings.HasPrefix(key, "mailboxIds/") {
			continue
		}
		mailboxID := strings.TrimPrefix(key, "mailboxIds/")

		if value == nil {
			// Remove from mailbox
			delete(result, mailboxID)
		} else if b, ok := value.(bool); ok && b {
			// Add to mailbox
			result[mailboxID] = true
		}
	}

	return result, nil
}

// setError creates a JMAP SetError response.
func setError(errorType, description string) map[string]any {
	return map[string]any{
		"type":        errorType,
		"description": description,
	}
}

// errorResponse creates a method-level error response.
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

	tableName := os.Getenv("EMAIL_TABLE_NAME")

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		logger.Error("FATAL: Failed to load AWS config", slog.String("error", err.Error()))
		panic(err)
	}

	dynamoClient := dynamodb.NewFromConfig(cfg)
	emailRepo := email.NewRepository(dynamoClient, tableName)
	mailboxRepo := mailbox.NewDynamoDBRepository(dynamoClient, tableName)
	stateRepo := state.NewRepository(dynamoClient, tableName, 7)

	h := newHandler(emailRepo, mailboxRepo, stateRepo)
	lambda.Start(otellambda.InstrumentHandler(h.handle, xrayconfig.WithRecommendedOptions(tp)...))
}
