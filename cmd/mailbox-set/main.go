// Package main implements the Mailbox/set Lambda handler.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	"github.com/google/uuid"
	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailbox"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
	"github.com/jarrod-lowe/jmap-service-libs/tracing"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda/xrayconfig"
	"go.opentelemetry.io/otel"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

// MailboxRepository defines the interface for mailbox operations.
type MailboxRepository interface {
	GetMailbox(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error)
	GetAllMailboxes(ctx context.Context, accountID string) ([]*mailbox.MailboxItem, error)
	CreateMailbox(ctx context.Context, mailbox *mailbox.MailboxItem) error
	UpdateMailbox(ctx context.Context, mailbox *mailbox.MailboxItem) error
	DeleteMailbox(ctx context.Context, accountID, mailboxID string) error
	BuildCreateMailboxItem(mbox *mailbox.MailboxItem) types.TransactWriteItem
	BuildUpdateMailboxItem(mbox *mailbox.MailboxItem) types.TransactWriteItem
	BuildDeleteMailboxItem(accountID, mailboxID string) types.TransactWriteItem
}

// StateRepository defines the interface for state tracking operations.
type StateRepository interface {
	GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
	IncrementStateAndLogChange(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error)
	BuildStateChangeItems(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem)
}

// TransactWriter executes DynamoDB transactions.
type TransactWriter interface {
	TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// EmailRepository defines the interface for email operations needed by mailbox destroy.
type EmailRepository interface {
	QueryEmailsByMailbox(ctx context.Context, accountID, mailboxID string) ([]string, error)
	GetEmail(ctx context.Context, accountID, emailID string) (*email.EmailItem, error)
	BuildSoftDeleteEmailItem(emailItem *email.EmailItem, deletedAt time.Time) types.TransactWriteItem
	BuildUpdateEmailMailboxesItems(emailItem *email.EmailItem, newMailboxIDs map[string]bool) (addedMailboxes []string, removedMailboxes []string, items []types.TransactWriteItem)
}

// handler implements the Mailbox/set logic.
type handler struct {
	repo      MailboxRepository
	stateRepo StateRepository
	transactor TransactWriter
	emailRepo  EmailRepository
}

// newHandler creates a new handler.
func newHandler(repo MailboxRepository, stateRepo StateRepository, transactor ...TransactWriter) *handler {
	h := &handler{
		repo:      repo,
		stateRepo: stateRepo,
	}
	if len(transactor) > 0 {
		h.transactor = transactor[0]
	}
	return h
}

// withEmailRepo sets the email repository for mailbox destroy cleanup.
func (h *handler) withEmailRepo(emailRepo EmailRepository) *handler {
	h.emailRepo = emailRepo
	return h
}

// handle processes a Mailbox/set request.
func (h *handler) handle(ctx context.Context, request plugincontract.PluginInvocationRequest) (plugincontract.PluginInvocationResponse, error) {
	tracer := tracing.Tracer("jmap-mailbox-set")
	ctx, span := tracer.Start(ctx, "MailboxSetHandler")
	defer span.End()

	// Check method
	if request.Method != "Mailbox/set" {
		return plugincontract.PluginInvocationResponse{
			MethodResponse: plugincontract.MethodResponse{
				Name: "error",
				Args: map[string]any{
					"type":        "unknownMethod",
					"description": "This handler only supports Mailbox/set",
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
		oldState, err = h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeMailbox)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to get current state",
				slog.String("account_id", accountID),
				slog.String("error", err.Error()),
			)
			return errorResponse(request.ClientID, "serverFail", err.Error()), nil
		}
	}

	created := make(map[string]any)
	notCreated := make(map[string]any)
	updated := make(map[string]any)
	notUpdated := make(map[string]any)
	destroyed := []any{}
	notDestroyed := make(map[string]any)
	newState := oldState

	// Handle create
	if createArg, ok := request.Args["create"].(map[string]any); ok {
		for clientID, createData := range createArg {
			data, ok := createData.(map[string]any)
			if !ok {
				notCreated[clientID] = setError("invalidArguments", "create data must be an object")
				continue
			}

			result, err := h.createMailbox(ctx, accountID, data, newState)
			if err != nil {
				notCreated[clientID] = err
			} else {
				created[clientID] = result
				// Update newState after successful creation
				newState = result["newState"].(int64)
			}
		}
	}

	// Handle update
	if updateArg, ok := request.Args["update"].(map[string]any); ok {
		for mailboxID, updateData := range updateArg {
			data, ok := updateData.(map[string]any)
			if !ok {
				notUpdated[mailboxID] = setError("invalidArguments", "update data must be an object")
				continue
			}

			result, err := h.updateMailbox(ctx, accountID, mailboxID, data, newState)
			if err != nil {
				notUpdated[mailboxID] = err
			} else {
				updated[mailboxID] = nil
				// Update newState after successful update
				if ns, ok := result["newState"].(int64); ok {
					newState = ns
				}
			}
		}
	}

	// Handle destroy
	if destroyArg, ok := request.Args["destroy"].([]any); ok {
		onDestroyRemoveEmails := false
		if v, ok := request.Args["onDestroyRemoveEmails"].(bool); ok {
			onDestroyRemoveEmails = v
		}

		for _, id := range destroyArg {
			mailboxID, ok := id.(string)
			if !ok {
				continue
			}

			result, err := h.destroyMailbox(ctx, accountID, mailboxID, onDestroyRemoveEmails, newState)
			if err != nil {
				notDestroyed[mailboxID] = err
			} else {
				destroyed = append(destroyed, mailboxID)
				// Update newState after successful deletion
				if ns, ok := result["newState"].(int64); ok {
					newState = ns
				}
			}
		}
	}

	logger.InfoContext(ctx, "Mailbox/set completed",
		slog.String("account_id", accountID),
		slog.Int("created_count", len(created)),
		slog.Int("updated_count", len(updated)),
		slog.Int("destroyed_count", len(destroyed)),
	)

	return plugincontract.PluginInvocationResponse{
		MethodResponse: plugincontract.MethodResponse{
			Name: "Mailbox/set",
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

// createMailbox creates a new mailbox.
func (h *handler) createMailbox(ctx context.Context, accountID string, data map[string]any, currentState int64) (map[string]any, map[string]any) {
	name, _ := data["name"].(string)
	role, _ := data["role"].(string)
	sortOrder := 0
	if v, ok := data["sortOrder"].(float64); ok {
		sortOrder = int(v)
	}
	isSubscribed := true
	if v, ok := data["isSubscribed"].(bool); ok {
		isSubscribed = v
	}

	// Reject non-null parentId — only flat mailboxes supported
	if parentId, hasParentId := data["parentId"]; hasParentId && parentId != nil {
		return nil, setError("invalidProperties", "Hierarchical mailboxes are not supported")
	}

	// Validate role if provided
	if role != "" && !mailbox.ValidRoles[role] {
		return nil, setError("invalidProperties", "Invalid role: "+role)
	}

	// Check for duplicate roles if transactor is available
	if h.transactor != nil && role != "" {
		mailboxes, err := h.repo.GetAllMailboxes(ctx, accountID)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to get all mailboxes for role check",
				slog.String("account_id", accountID),
				slog.String("error", err.Error()),
			)
			return nil, setError("serverFail", err.Error())
		}
		for _, m := range mailboxes {
			if m.Role == role {
				return nil, setError("invalidProperties", "A mailbox with this role already exists")
			}
		}
	}

	// Determine mailbox ID
	var mailboxID string
	if role != "" {
		mailboxID = role
	} else {
		mailboxID = uuid.New().String()
	}

	now := time.Now().UTC()
	mbox := &mailbox.MailboxItem{
		AccountID:    accountID,
		MailboxID:    mailboxID,
		Name:         name,
		Role:         role,
		SortOrder:    sortOrder,
		TotalEmails:  0,
		UnreadEmails: 0,
		IsSubscribed: isSubscribed,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	// If transactor is available, use transaction
	if h.transactor != nil && h.stateRepo != nil {
		// Build mailbox creation item
		mailboxItem := h.repo.BuildCreateMailboxItem(mbox)

		// Build state change items
		newState, stateItems := h.stateRepo.BuildStateChangeItems(accountID, state.ObjectTypeMailbox, currentState, mailboxID, state.ChangeTypeCreated)

		// Combine all items
		transactItems := []types.TransactWriteItem{mailboxItem}
		transactItems = append(transactItems, stateItems...)

		// Execute transaction
		_, err := h.transactor.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: transactItems,
		})
		if err != nil {
			logger.ErrorContext(ctx, "Failed to create mailbox transactionally",
				slog.String("account_id", accountID),
				slog.String("mailbox_id", mailboxID),
				slog.String("error", err.Error()),
			)
			return nil, setError("serverFail", err.Error())
		}

		return map[string]any{
			"id":       mailboxID,
			"newState": newState,
		}, nil
	}

	// Fallback to non-transactional path
	err := h.repo.CreateMailbox(ctx, mbox)
	if err != nil {
		if errors.Is(err, mailbox.ErrRoleAlreadyExists) {
			return nil, setError("invalidProperties", "A mailbox with this role already exists")
		}
		logger.ErrorContext(ctx, "Failed to create mailbox",
			slog.String("account_id", accountID),
			slog.String("error", err.Error()),
		)
		return nil, setError("serverFail", err.Error())
	}

	// Track state change separately (old behavior)
	if h.stateRepo != nil {
		if s, err := h.stateRepo.IncrementStateAndLogChange(ctx, accountID, state.ObjectTypeMailbox, mailboxID, state.ChangeTypeCreated); err != nil {
			logger.ErrorContext(ctx, "Failed to track mailbox state change",
				slog.String("account_id", accountID),
				slog.String("mailbox_id", mailboxID),
				slog.String("error", err.Error()),
			)
		} else {
			return map[string]any{
				"id":       mailboxID,
				"newState": s,
			}, nil
		}
	}

	return map[string]any{
		"id":       mailboxID,
		"newState": currentState,
	}, nil
}

// updateMailbox updates an existing mailbox.
func (h *handler) updateMailbox(ctx context.Context, accountID, mailboxID string, data map[string]any, currentState int64) (map[string]any, map[string]any) {
	// Check for parentId - we don't support hierarchy
	if _, hasParentId := data["parentId"]; hasParentId {
		if data["parentId"] != nil {
			return nil, setError("invalidProperties", "Hierarchical mailboxes are not supported")
		}
	}

	// Get existing mailbox
	mbox, err := h.repo.GetMailbox(ctx, accountID, mailboxID)
	if err != nil {
		if errors.Is(err, mailbox.ErrMailboxNotFound) {
			return nil, setError("notFound", "Mailbox not found")
		}
		logger.ErrorContext(ctx, "Failed to get mailbox",
			slog.String("account_id", accountID),
			slog.String("mailbox_id", mailboxID),
			slog.String("error", err.Error()),
		)
		return nil, setError("serverFail", err.Error())
	}

	// Apply updates
	if name, ok := data["name"].(string); ok {
		mbox.Name = name
	}
	if role, ok := data["role"].(string); ok {
		if role != "" && !mailbox.ValidRoles[role] {
			return nil, setError("invalidProperties", "Invalid role: "+role)
		}
		mbox.Role = role
	}
	if sortOrder, ok := data["sortOrder"].(float64); ok {
		mbox.SortOrder = int(sortOrder)
	}
	if isSubscribed, ok := data["isSubscribed"].(bool); ok {
		mbox.IsSubscribed = isSubscribed
	}

	mbox.UpdatedAt = time.Now().UTC()

	// If transactor is available, use transaction
	if h.transactor != nil && h.stateRepo != nil {
		// Build mailbox update item
		mailboxItem := h.repo.BuildUpdateMailboxItem(mbox)

		// Build state change items
		newState, stateItems := h.stateRepo.BuildStateChangeItems(accountID, state.ObjectTypeMailbox, currentState, mailboxID, state.ChangeTypeUpdated)

		// Combine all items
		transactItems := []types.TransactWriteItem{mailboxItem}
		transactItems = append(transactItems, stateItems...)

		// Execute transaction
		_, err = h.transactor.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: transactItems,
		})
		if err != nil {
			logger.ErrorContext(ctx, "Failed to update mailbox transactionally",
				slog.String("account_id", accountID),
				slog.String("mailbox_id", mailboxID),
				slog.String("error", err.Error()),
			)
			return nil, setError("serverFail", err.Error())
		}

		return map[string]any{
			"newState": newState,
		}, nil
	}

	// Fallback to non-transactional path
	err = h.repo.UpdateMailbox(ctx, mbox)
	if err != nil {
		if errors.Is(err, mailbox.ErrRoleAlreadyExists) {
			return nil, setError("invalidProperties", "A mailbox with this role already exists")
		}
		logger.ErrorContext(ctx, "Failed to update mailbox",
			slog.String("account_id", accountID),
			slog.String("mailbox_id", mailboxID),
			slog.String("error", err.Error()),
		)
		return nil, setError("serverFail", err.Error())
	}

	// Track state change separately (old behavior)
	if h.stateRepo != nil {
		if s, err := h.stateRepo.IncrementStateAndLogChange(ctx, accountID, state.ObjectTypeMailbox, mailboxID, state.ChangeTypeUpdated); err != nil {
			logger.ErrorContext(ctx, "Failed to track mailbox state change",
				slog.String("account_id", accountID),
				slog.String("mailbox_id", mailboxID),
				slog.String("error", err.Error()),
			)
		} else {
			return map[string]any{
				"newState": s,
			}, nil
		}
	}

	return map[string]any{
		"newState": currentState,
	}, nil
}

// destroyMailbox deletes a mailbox.
func (h *handler) destroyMailbox(ctx context.Context, accountID, mailboxID string, onDestroyRemoveEmails bool, currentState int64) (map[string]any, map[string]any) {
	// Get mailbox to check if it has emails
	mbox, err := h.repo.GetMailbox(ctx, accountID, mailboxID)
	if err != nil {
		if errors.Is(err, mailbox.ErrMailboxNotFound) {
			return nil, setError("notFound", "Mailbox not found")
		}
		logger.ErrorContext(ctx, "Failed to get mailbox",
			slog.String("account_id", accountID),
			slog.String("mailbox_id", mailboxID),
			slog.String("error", err.Error()),
		)
		return nil, setError("serverFail", err.Error())
	}

	// Check if mailbox has emails
	if mbox.TotalEmails > 0 && !onDestroyRemoveEmails {
		return nil, setError("mailboxHasEmail", "Mailbox is not empty")
	}

	// If transactor is available, use transaction
	if h.transactor != nil && h.stateRepo != nil {
		// Build mailbox deletion item
		mailboxItem := h.repo.BuildDeleteMailboxItem(accountID, mailboxID)

		// Build state change items
		newState, stateItems := h.stateRepo.BuildStateChangeItems(accountID, state.ObjectTypeMailbox, currentState, mailboxID, state.ChangeTypeDestroyed)

		// Combine all items
		transactItems := []types.TransactWriteItem{mailboxItem}
		transactItems = append(transactItems, stateItems...)

		// Execute transaction
		_, err = h.transactor.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: transactItems,
		})
		if err != nil {
			logger.ErrorContext(ctx, "Failed to destroy mailbox transactionally",
				slog.String("account_id", accountID),
				slog.String("mailbox_id", mailboxID),
				slog.String("error", err.Error()),
			)
			return nil, setError("serverFail", err.Error())
		}

		// If onDestroyRemoveEmails and mailbox had emails, synchronously clean up
		if onDestroyRemoveEmails && mbox.TotalEmails > 0 && h.emailRepo != nil {
			if cleanupErr := h.cleanupMailboxEmails(ctx, accountID, mailboxID); cleanupErr != nil {
				logger.ErrorContext(ctx, "Failed to clean up mailbox emails",
					slog.String("account_id", accountID),
					slog.String("mailbox_id", mailboxID),
					slog.String("error", cleanupErr.Error()),
				)
				// Mailbox is already deleted; email cleanup failure is non-fatal
				// Emails will be cleaned up by eventual consistency
			}
		}

		return map[string]any{
			"newState": newState,
		}, nil
	}

	// Fallback to non-transactional path
	err = h.repo.DeleteMailbox(ctx, accountID, mailboxID)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to delete mailbox",
			slog.String("account_id", accountID),
			slog.String("mailbox_id", mailboxID),
			slog.String("error", err.Error()),
		)
		return nil, setError("serverFail", err.Error())
	}

	// Track state change separately (old behavior)
	if h.stateRepo != nil {
		if s, err := h.stateRepo.IncrementStateAndLogChange(ctx, accountID, state.ObjectTypeMailbox, mailboxID, state.ChangeTypeDestroyed); err != nil {
			logger.ErrorContext(ctx, "Failed to track mailbox state change",
				slog.String("account_id", accountID),
				slog.String("mailbox_id", mailboxID),
				slog.String("error", err.Error()),
			)
		} else {
			return map[string]any{
				"newState": s,
			}, nil
		}
	}

	return map[string]any{
		"newState": currentState,
	}, nil
}

// cleanupMailboxEmails handles email cleanup when a mailbox is destroyed with onDestroyRemoveEmails=true.
// For orphaned emails (only in destroyed mailbox), sets deletedAt.
// For multi-mailbox emails, removes the destroyed mailbox from mailboxIds.
func (h *handler) cleanupMailboxEmails(ctx context.Context, accountID, mailboxID string) error {
	emailIDs, err := h.emailRepo.QueryEmailsByMailbox(ctx, accountID, mailboxID)
	if err != nil {
		return err
	}

	now := time.Now()
	for _, emailID := range emailIDs {
		emailItem, err := h.emailRepo.GetEmail(ctx, accountID, emailID)
		if err != nil {
			if errors.Is(err, email.ErrEmailNotFound) {
				continue
			}
			return err
		}

		// Skip if already soft-deleted or mailbox already removed
		if emailItem.DeletedAt != nil || !emailItem.MailboxIDs[mailboxID] {
			continue
		}

		if len(emailItem.MailboxIDs) == 1 {
			// Orphaned email — soft-delete
			var transactItems []types.TransactWriteItem
			transactItems = append(transactItems, h.emailRepo.BuildSoftDeleteEmailItem(emailItem, now))

			if h.stateRepo != nil {
				emailState, err := h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeEmail)
				if err != nil {
					return err
				}
				_, stateItems := h.stateRepo.BuildStateChangeItems(accountID, state.ObjectTypeEmail, emailState, emailItem.EmailID, state.ChangeTypeDestroyed)
				transactItems = append(transactItems, stateItems...)
			}

			_, err = h.transactor.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
				TransactItems: transactItems,
			})
			if err != nil {
				logger.ErrorContext(ctx, "Failed to soft-delete orphaned email",
					slog.String("account_id", accountID),
					slog.String("email_id", emailID),
					slog.String("error", err.Error()),
				)
				return err
			}
		} else {
			// Multi-mailbox email — remove destroyed mailbox
			newMailboxIDs := make(map[string]bool)
			for k, v := range emailItem.MailboxIDs {
				if k != mailboxID {
					newMailboxIDs[k] = v
				}
			}

			var transactItems []types.TransactWriteItem
			_, _, emailItems := h.emailRepo.BuildUpdateEmailMailboxesItems(emailItem, newMailboxIDs)
			transactItems = append(transactItems, emailItems...)

			if h.stateRepo != nil {
				emailState, err := h.stateRepo.GetCurrentState(ctx, accountID, state.ObjectTypeEmail)
				if err != nil {
					return err
				}
				_, stateItems := h.stateRepo.BuildStateChangeItems(accountID, state.ObjectTypeEmail, emailState, emailItem.EmailID, state.ChangeTypeUpdated)
				transactItems = append(transactItems, stateItems...)
			}

			_, err = h.transactor.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
				TransactItems: transactItems,
			})
			if err != nil {
				logger.ErrorContext(ctx, "Failed to update multi-mailbox email",
					slog.String("account_id", accountID),
					slog.String("email_id", emailID),
					slog.String("error", err.Error()),
				)
				return err
			}
		}
	}

	return nil
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

	tp, err := tracing.Init(ctx)
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

	// Instrument AWS SDK clients with OTel tracing
	otelaws.AppendMiddlewares(&cfg.APIOptions)

	dynamoClient := dynamodb.NewFromConfig(cfg)

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

	repo := mailbox.NewDynamoDBRepository(dynamoClient, tableName)
	stateRepo := state.NewRepository(dynamoClient, tableName, 7)
	emailRepo := email.NewRepository(dynamoClient, tableName)

	h := newHandler(repo, stateRepo, dynamoClient)
	h.emailRepo = emailRepo

	lambda.Start(otellambda.InstrumentHandler(h.handle, xrayconfig.WithRecommendedOptions(tp)...))
}
