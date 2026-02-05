// Package main implements the Email/set Lambda handler.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jarrod-lowe/jmap-service-libs/plugincontract"
	"github.com/jarrod-lowe/jmap-service-libs/dbclient"
	"github.com/jarrod-lowe/jmap-service-email/internal/blobdelete"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailbox"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
	"github.com/jarrod-lowe/jmap-service-libs/awsinit"
	"github.com/jarrod-lowe/jmap-service-libs/jmaperror"
	"github.com/jarrod-lowe/jmap-service-libs/logging"
	"github.com/jarrod-lowe/jmap-service-libs/tracing"
)

var logger = logging.New()

// EmailRepository defines the interface for email operations.
type EmailRepository interface {
	UpdateEmailMailboxes(ctx context.Context, accountID, emailID string, newMailboxIDs map[string]bool) (oldMailboxIDs map[string]bool, email *email.EmailItem, err error)
	GetEmail(ctx context.Context, accountID, emailID string) (*email.EmailItem, error)
	UpdateEmailKeywords(ctx context.Context, accountID, emailID string, newKeywords map[string]bool, expectedVersion int) (*email.EmailItem, error)
	BuildDeleteEmailItems(emailItem *email.EmailItem) []types.TransactWriteItem
	BuildSoftDeleteEmailItem(emailItem *email.EmailItem, deletedAt time.Time) types.TransactWriteItem
	BuildUpdateEmailMailboxesItems(emailItem *email.EmailItem, newMailboxIDs map[string]bool) (addedMailboxes []string, removedMailboxes []string, items []types.TransactWriteItem)
}

// MailboxRepository defines the interface for mailbox operations.
type MailboxRepository interface {
	MailboxExists(ctx context.Context, accountID, mailboxID string) (bool, error)
	IncrementCounts(ctx context.Context, accountID, mailboxID string, incrementUnread bool) error
	DecrementCounts(ctx context.Context, accountID, mailboxID string, decrementUnread bool) error
	BuildDecrementCountsItems(accountID, mailboxID string, decrementUnread bool) types.TransactWriteItem
	BuildIncrementCountsItems(accountID, mailboxID string, incrementUnread bool) types.TransactWriteItem
}

// StateRepository defines the interface for state tracking operations.
type StateRepository interface {
	GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
	IncrementStateAndLogChange(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error)
	BuildStateChangeItems(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem)
	BuildStateChangeItemsMulti(accountID string, objectType state.ObjectType, currentState int64, objectIDs []string, changeType state.ChangeType) (int64, []types.TransactWriteItem)
}

// TransactWriter executes DynamoDB transactions.
type TransactWriter interface {
	TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// handler implements the Email/set logic.
type handler struct {
	emailRepo            EmailRepository
	mailboxRepo          MailboxRepository
	stateRepo            StateRepository
	blobDeletePublisher  blobdelete.BlobDeletePublisher
	transactor           TransactWriter
}

// newHandler creates a new handler.
func newHandler(emailRepo EmailRepository, mailboxRepo MailboxRepository, stateRepo StateRepository, blobDeletePublisher blobdelete.BlobDeletePublisher, transactor TransactWriter) *handler {
	return &handler{
		emailRepo:           emailRepo,
		mailboxRepo:         mailboxRepo,
		stateRepo:           stateRepo,
		blobDeletePublisher: blobDeletePublisher,
		transactor:          transactor,
	}
}

// handle processes an Email/set request.
func (h *handler) handle(ctx context.Context, request plugincontract.PluginInvocationRequest) (plugincontract.PluginInvocationResponse, error) {
	tracer := tracing.Tracer("jmap-email-set")
	ctx, span := tracer.Start(ctx, "EmailSetHandler")
	defer span.End()

	// Check method
	if request.Method != "Email/set" {
		return errorResponse(request.ClientID, jmaperror.UnknownMethod("This handler only supports Email/set")), nil
	}

	accountID := request.Args.StringOr("accountId", request.AccountID)

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
			return errorResponse(request.ClientID, jmaperror.ServerFail(err.Error(), err)), nil
		}
	}

	// Check ifInState (handles both string and numeric values)
	if request.Args.Has("ifInState") {
		var expectedState int64
		var parseErr error

		if strVal, ok := request.Args.String("ifInState"); ok {
			expectedState, parseErr = strconv.ParseInt(strVal, 10, 64)
		} else if intVal, ok := request.Args.Int("ifInState"); ok {
			expectedState = intVal
		} else {
			parseErr = errors.New("invalid ifInState type")
		}

		// If ifInState is provided but invalid, or doesn't match current state, return stateMismatch
		if parseErr != nil || expectedState != oldState {
			return errorResponse(request.ClientID, jmaperror.StateMismatch("State mismatch")), nil
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
	if createArg, ok := request.Args.Object("create"); ok {
		for clientID := range createArg {
			notCreated[clientID] = jmaperror.SetForbidden("Email/set create is not supported. Use Email/import instead.").ToMap()
		}
	}

	// Handle update
	if updateArg, ok := request.Args.Object("update"); ok {
		for emailID, updateData := range updateArg {
			data, ok := updateData.(map[string]any)
			if !ok {
				notUpdated[emailID] = jmaperror.InvalidProperties("update data must be an object", nil).ToMap()
				continue
			}

			newEmailState, err := h.updateEmail(ctx, accountID, emailID, data, affectedMailboxes)
			if err != nil {
				notUpdated[emailID] = err
			} else {
				updated[emailID] = nil
				// If updateEmail did transactional state update (mailbox update), use the returned state
				if newEmailState > 0 {
					if newEmailState > newState {
						newState = newEmailState
					}
				} else {
					// Otherwise track state change separately (keywords-only update)
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

	// Handle destroy
	if destroyIDs, ok := request.Args.StringSlice("destroy"); ok {
		for _, emailID := range destroyIDs {
			destroyNewState, destroyErr := h.destroyEmail(ctx, accountID, emailID, affectedMailboxes)
			if destroyErr != nil {
				notDestroyed[emailID] = destroyErr
			} else {
				destroyed = append(destroyed, emailID)
				if destroyNewState > newState {
					newState = destroyNewState
				}
			}
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

// maxKeywordRetries is the number of times to retry on version conflict.
const maxKeywordRetries = 3

// updateEmail processes an email update.
// Returns the new email state (if transactional update was performed), or 0 if state tracking should be done separately.
func (h *handler) updateEmail(ctx context.Context, accountID, emailID string, data map[string]any, affectedMailboxes map[string]bool) (int64, map[string]any) {
	// Check for unsupported properties
	hasMailboxUpdate := false
	hasKeywordUpdate := false
	for key := range data {
		if key == "mailboxIds" || strings.HasPrefix(key, "mailboxIds/") {
			hasMailboxUpdate = true
		} else if key == "keywords" || strings.HasPrefix(key, "keywords/") {
			hasKeywordUpdate = true
		} else {
			return 0, jmaperror.InvalidProperties("unsupported property: "+key, nil).ToMap()
		}
	}

	// Handle keywords update (non-transactional for now)
	if hasKeywordUpdate {
		if err := h.updateKeywords(ctx, accountID, emailID, data, affectedMailboxes); err != nil {
			return 0, err
		}
	}

	// Handle mailboxIds update
	if !hasMailboxUpdate {
		// If we had keywords and succeeded, we're done (state tracking happens in handle())
		if hasKeywordUpdate {
			return 0, nil
		}
		return 0, jmaperror.InvalidProperties("no mailboxIds or keywords update found", nil).ToMap()
	}

	// Parse mailboxIds update (full replacement vs patch)
	newMailboxIDs, fetchedEmail, parseErr := h.parseMailboxIDsUpdate(ctx, accountID, emailID, data)
	if parseErr != nil {
		return 0, parseErr
	}

	// Validate at least one mailbox
	if len(newMailboxIDs) == 0 {
		return 0, jmaperror.InvalidProperties("email must belong to at least one mailbox", nil).ToMap()
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
			return 0, jmaperror.SetServerFail(checkErr.Error()).ToMap()
		}
		if !exists {
			return 0, jmaperror.InvalidProperties("mailbox does not exist: "+mailboxID, nil).ToMap()
		}
	}

	// Transactional path: bundle email update + counter updates + state changes
	if h.transactor != nil {
		return h.updateEmailMailboxesTransactional(ctx, accountID, emailID, newMailboxIDs, fetchedEmail, affectedMailboxes)
	}

	// Non-transactional fallback
	return h.updateEmailMailboxesLegacy(ctx, accountID, emailID, newMailboxIDs, affectedMailboxes)
}

// updateEmailMailboxesTransactional bundles email mailbox update + counter updates + state changes into one transaction.
// It does NOT populate affectedMailboxes since state changes are handled within the transaction.
func (h *handler) updateEmailMailboxesTransactional(ctx context.Context, accountID, emailID string, newMailboxIDs map[string]bool, fetchedEmail *email.EmailItem, _ map[string]bool) (int64, map[string]any) {
	// Get the email if not already fetched (full replacement syntax)
	emailItem := fetchedEmail
	if emailItem == nil {
		var err error
		emailItem, err = h.emailRepo.GetEmail(ctx, accountID, emailID)
		if err != nil {
			if errors.Is(err, email.ErrEmailNotFound) {
				return 0, jmaperror.NotFound("email not found").ToMap()
			}
			return 0, jmaperror.SetServerFail(err.Error()).ToMap()
		}
	}
	if emailItem.DeletedAt != nil {
		return 0, jmaperror.NotFound("email not found").ToMap()
	}

	// Get current states
	var emailState, mailboxState int64
	if h.stateRepo != nil {
		var err error
		emailState, err = h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeEmail)
		if err != nil {
			return 0, jmaperror.SetServerFail(err.Error()).ToMap()
		}
		mailboxState, err = h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeMailbox)
		if err != nil {
			return 0, jmaperror.SetServerFail(err.Error()).ToMap()
		}
	}

	// Build transaction items
	var transactItems []types.TransactWriteItem

	addedMailboxes, removedMailboxes, emailItems := h.emailRepo.BuildUpdateEmailMailboxesItems(emailItem, newMailboxIDs)
	transactItems = append(transactItems, emailItems...)

	isUnread := emailItem.Keywords == nil || !emailItem.Keywords["$seen"]
	for _, mailboxID := range addedMailboxes {
		transactItems = append(transactItems, h.mailboxRepo.BuildIncrementCountsItems(accountID, mailboxID, isUnread))
	}
	for _, mailboxID := range removedMailboxes {
		transactItems = append(transactItems, h.mailboxRepo.BuildDecrementCountsItems(accountID, mailboxID, isUnread))
	}

	var newEmailState int64
	if h.stateRepo != nil {
		var emailStateItems []types.TransactWriteItem
		newEmailState, emailStateItems = h.stateRepo.BuildStateChangeItems(accountID, state.ObjectTypeEmail, emailState, emailID, state.ChangeTypeUpdated)
		transactItems = append(transactItems, emailStateItems...)

		allAffected := append(addedMailboxes, removedMailboxes...)
		if len(allAffected) > 0 {
			_, mailboxStateItems := h.stateRepo.BuildStateChangeItemsMulti(accountID, state.ObjectTypeMailbox, mailboxState, allAffected, state.ChangeTypeUpdated)
			transactItems = append(transactItems, mailboxStateItems...)
		}
	}

	_, txErr := h.transactor.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if txErr != nil {
		logger.ErrorContext(ctx, "Failed to update email mailboxes transaction",
			slog.String("account_id", accountID),
			slog.String("email_id", emailID),
			slog.String("error", txErr.Error()),
		)
		return 0, jmaperror.SetServerFail(txErr.Error()).ToMap()
	}

	return newEmailState, nil
}

// updateEmailMailboxesLegacy performs the non-transactional mailbox update (fallback when no transactor).
func (h *handler) updateEmailMailboxesLegacy(ctx context.Context, accountID, emailID string, newMailboxIDs map[string]bool, affectedMailboxes map[string]bool) (int64, map[string]any) {
	oldMailboxIDs, emailItem, updateErr := h.emailRepo.UpdateEmailMailboxes(ctx, accountID, emailID, newMailboxIDs)
	if updateErr != nil {
		if errors.Is(updateErr, email.ErrEmailNotFound) {
			return 0, jmaperror.NotFound("email not found").ToMap()
		}
		logger.ErrorContext(ctx, "Failed to update email mailboxes",
			slog.String("account_id", accountID),
			slog.String("email_id", emailID),
			slog.String("error", updateErr.Error()),
		)
		return 0, jmaperror.SetServerFail(updateErr.Error()).ToMap()
	}

	isUnread := emailItem.Keywords == nil || !emailItem.Keywords["$seen"]
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

	return 0, nil
}

// parseMailboxIDsUpdate parses the mailboxIds update data.
// Supports both full replacement and JMAP patch syntax.
// Returns the new mailboxIDs, the fetched email (if patch syntax was used), and any error.
func (h *handler) parseMailboxIDsUpdate(ctx context.Context, accountID, emailID string, data map[string]any) (map[string]bool, *email.EmailItem, map[string]any) {
	// Check for full replacement
	if mailboxIDs, ok := data["mailboxIds"].(map[string]any); ok {
		result := make(map[string]bool)
		for k, v := range mailboxIDs {
			if b, ok := v.(bool); ok && b {
				result[k] = true
			}
		}
		return result, nil, nil
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
		return nil, nil, jmaperror.InvalidProperties("no mailboxIds update found", nil).ToMap()
	}

	// Get current mailboxIds for the email
	emailItem, err := h.emailRepo.GetEmail(ctx, accountID, emailID)
	if err != nil {
		if errors.Is(err, email.ErrEmailNotFound) {
			return nil, nil, jmaperror.NotFound("email not found").ToMap()
		}
		return nil, nil, jmaperror.SetServerFail(err.Error()).ToMap()
	}
	if emailItem.DeletedAt != nil {
		return nil, nil, jmaperror.NotFound("email not found").ToMap()
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
		// Validate that the mailboxID doesn't contain nested paths (RFC 8620 Section 5.3)
		if strings.Contains(mailboxID, "/") {
			return nil, nil, jmaperror.InvalidPatch("invalid patch path: "+key).ToMap()
		}

		if value == nil {
			// Remove from mailbox
			delete(result, mailboxID)
		} else if b, ok := value.(bool); ok && b {
			// Add to mailbox
			result[mailboxID] = true
		}
	}

	return result, emailItem, nil
}

// updateKeywords updates an email's keywords with retry logic for version conflicts.
func (h *handler) updateKeywords(ctx context.Context, accountID, emailID string, data map[string]any, affectedMailboxes map[string]bool) map[string]any {
	for attempt := 0; attempt < maxKeywordRetries; attempt++ {
		// Get current email state
		emailItem, err := h.emailRepo.GetEmail(ctx, accountID, emailID)
		if err != nil {
			if errors.Is(err, email.ErrEmailNotFound) {
				return jmaperror.NotFound("email not found").ToMap()
			}
			return jmaperror.SetServerFail(err.Error()).ToMap()
		}
		if emailItem.DeletedAt != nil {
			return jmaperror.NotFound("email not found").ToMap()
		}

		// Parse keyword updates
		newKeywords, parseErr := h.parseKeywordUpdates(data, emailItem.Keywords)
		if parseErr != nil {
			return parseErr
		}

		// Validate keywords
		for keyword := range newKeywords {
			if err := email.ValidateKeyword(keyword); err != nil {
				return jmaperror.InvalidProperties("invalid keyword: "+keyword+": "+err.Error(), nil).ToMap()
			}
		}

		// Update keywords
		_, updateErr := h.emailRepo.UpdateEmailKeywords(ctx, accountID, emailID, newKeywords, emailItem.Version)
		if updateErr != nil {
			if errors.Is(updateErr, email.ErrVersionConflict) {
				// Retry with fresh read
				logger.InfoContext(ctx, "Keywords version conflict, retrying",
					slog.String("account_id", accountID),
					slog.String("email_id", emailID),
					slog.Int("attempt", attempt+1),
				)
				continue
			}
			if errors.Is(updateErr, email.ErrEmailNotFound) {
				return jmaperror.NotFound("email not found").ToMap()
			}
			logger.ErrorContext(ctx, "Failed to update email keywords",
				slog.String("account_id", accountID),
				slog.String("email_id", emailID),
				slog.String("error", updateErr.Error()),
			)
			return jmaperror.SetServerFail(updateErr.Error()).ToMap()
		}

		// Success - mark mailboxes as affected since unread counts may have changed
		for mailboxID := range emailItem.MailboxIDs {
			affectedMailboxes[mailboxID] = true
		}
		return nil
	}

	// All retries exhausted
	logger.ErrorContext(ctx, "Keywords update failed after max retries",
		slog.String("account_id", accountID),
		slog.String("email_id", emailID),
	)
	return jmaperror.SetServerFail("concurrent update conflict, please retry").ToMap()
}

// parseKeywordUpdates parses the keywords update data.
// Supports both full replacement and JMAP patch syntax.
func (h *handler) parseKeywordUpdates(data map[string]any, currentKeywords map[string]bool) (map[string]bool, map[string]any) {
	// Check for full replacement
	if keywords, ok := data["keywords"].(map[string]any); ok {
		result := make(map[string]bool)
		for k, v := range keywords {
			if b, ok := v.(bool); ok && b {
				result[email.NormalizeKeyword(k)] = true
			}
		}
		return result, nil
	}

	// Check for patch syntax (keywords/{keyword})
	hasPatch := false
	for key := range data {
		if strings.HasPrefix(key, "keywords/") {
			hasPatch = true
			break
		}
	}

	if !hasPatch {
		return nil, nil
	}

	// Start with current keywords
	result := make(map[string]bool)
	for k, v := range currentKeywords {
		result[k] = v
	}

	// Apply patches
	for key, value := range data {
		if !strings.HasPrefix(key, "keywords/") {
			continue
		}
		keyword := strings.TrimPrefix(key, "keywords/")
		// Validate that the keyword doesn't contain nested paths (RFC 8620 Section 5.3)
		if strings.Contains(keyword, "/") {
			return nil, jmaperror.InvalidPatch("invalid patch path: "+key).ToMap()
		}
		keyword = email.NormalizeKeyword(keyword)

		if value == nil {
			// Remove keyword
			delete(result, keyword)
		} else if b, ok := value.(bool); ok && b {
			// Add keyword
			result[keyword] = true
		}
	}

	return result, nil
}

// maxDestroyRetries is the number of times to retry destroy on transaction conflict.
const maxDestroyRetries = 3

// destroyEmail soft-deletes an email by setting deletedAt, updating state, and decrementing mailbox counters.
// A DynamoDB Streams handler performs actual record deletion and blob cleanup.
// Returns the new email state, or a JMAP error map.
func (h *handler) destroyEmail(ctx context.Context, accountID, emailID string, affectedMailboxes map[string]bool) (int64, map[string]any) {
	for attempt := 0; attempt < maxDestroyRetries; attempt++ {
		// 1. Fetch email
		emailItem, err := h.emailRepo.GetEmail(ctx, accountID, emailID)
		if err != nil {
			if errors.Is(err, email.ErrEmailNotFound) {
				return 0, jmaperror.NotFound("email not found").ToMap()
			}
			return 0, jmaperror.SetServerFail(err.Error()).ToMap()
		}

		// Treat already soft-deleted emails as not found
		if emailItem.DeletedAt != nil {
			return 0, jmaperror.NotFound("email not found").ToMap()
		}

		// 2. Read current states
		var emailState, threadState, mailboxState int64

		if h.stateRepo != nil {
			emailState, err = h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeEmail)
			if err != nil {
				return 0, jmaperror.SetServerFail(err.Error()).ToMap()
			}
			threadState, err = h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeThread)
			if err != nil {
				return 0, jmaperror.SetServerFail(err.Error()).ToMap()
			}
			mailboxState, err = h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeMailbox)
			if err != nil {
				return 0, jmaperror.SetServerFail(err.Error()).ToMap()
			}
		}

		// 3. Build transaction items
		var transactItems []types.TransactWriteItem

		// Soft-delete email (set deletedAt, increment version with condition)
		transactItems = append(transactItems, h.emailRepo.BuildSoftDeleteEmailItem(emailItem, time.Now()))

		// Mailbox counter decrements
		isUnread := emailItem.Keywords == nil || !emailItem.Keywords["$seen"]
		for mailboxID := range emailItem.MailboxIDs {
			transactItems = append(transactItems, h.mailboxRepo.BuildDecrementCountsItems(accountID, mailboxID, isUnread))
			affectedMailboxes[mailboxID] = true
		}

		// State changes
		var newEmailState int64
		if h.stateRepo != nil {
			var stateItems []types.TransactWriteItem

			// Email destroyed
			newEmailState, stateItems = h.stateRepo.BuildStateChangeItems(accountID, state.ObjectTypeEmail, emailState, emailID, state.ChangeTypeDestroyed)
			transactItems = append(transactItems, stateItems...)

			// Mailbox state: one state increment per mailbox, sequential states
			mailboxIDs := make([]string, 0, len(emailItem.MailboxIDs))
			for mailboxID := range emailItem.MailboxIDs {
				mailboxIDs = append(mailboxIDs, mailboxID)
			}
			_, mailboxStateItems := h.stateRepo.BuildStateChangeItemsMulti(accountID, state.ObjectTypeMailbox, mailboxState, mailboxIDs, state.ChangeTypeUpdated)
			transactItems = append(transactItems, mailboxStateItems...)

			// Thread updated
			_, stateItems = h.stateRepo.BuildStateChangeItems(accountID, state.ObjectTypeThread, threadState, emailItem.ThreadID, state.ChangeTypeUpdated)
			transactItems = append(transactItems, stateItems...)
		}

		// 4. Execute transaction
		_, err = h.transactor.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: transactItems,
		})
		if err != nil {
			// Check for condition check failure → retry
			var txCanceled *types.TransactionCanceledException
			if errors.As(err, &txCanceled) {
				logger.InfoContext(ctx, "Destroy transaction conflict, retrying",
					slog.String("account_id", accountID),
					slog.String("email_id", emailID),
					slog.Int("attempt", attempt+1),
				)
				continue
			}
			logger.ErrorContext(ctx, "Destroy transaction failed",
				slog.String("account_id", accountID),
				slog.String("email_id", emailID),
				slog.String("error", err.Error()),
			)
			return 0, jmaperror.SetServerFail(err.Error()).ToMap()
		}

		return newEmailState, nil
	}

	// All retries exhausted
	return 0, jmaperror.SetServerFail("concurrent update conflict, please retry").ToMap()
}

// errorResponse creates a method-level error response from a jmaperror.MethodError.
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
	blobDeleteQueueURL := os.Getenv("BLOB_DELETE_QUEUE_URL")

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
	stateRepo := state.NewRepository(dynamoClient, tableName, 7)

	var blobPub blobdelete.BlobDeletePublisher
	if blobDeleteQueueURL != "" {
		sqsClient := sqs.NewFromConfig(result.Config)
		blobPub = blobdelete.NewSQSPublisher(sqsClient, blobDeleteQueueURL)
	}

	h := newHandler(emailRepo, mailboxRepo, stateRepo, blobPub, dynamoClient)
	result.Start(h.handle)
}
