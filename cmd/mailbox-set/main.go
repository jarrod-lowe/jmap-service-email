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
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
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

// MailboxRepository defines the interface for mailbox operations.
type MailboxRepository interface {
	GetMailbox(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error)
	GetAllMailboxes(ctx context.Context, accountID string) ([]*mailbox.MailboxItem, error)
	CreateMailbox(ctx context.Context, mailbox *mailbox.MailboxItem) error
	UpdateMailbox(ctx context.Context, mailbox *mailbox.MailboxItem) error
	DeleteMailbox(ctx context.Context, accountID, mailboxID string) error
}

// StateRepository defines the interface for state tracking operations.
type StateRepository interface {
	GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
	IncrementStateAndLogChange(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error)
}

// handler implements the Mailbox/set logic.
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

// handle processes a Mailbox/set request.
func (h *handler) handle(ctx context.Context, request plugincontract.PluginInvocationRequest) (plugincontract.PluginInvocationResponse, error) {
	tracer := otel.Tracer("jmap-mailbox-set")
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

			result, err := h.createMailbox(ctx, accountID, data)
			if err != nil {
				notCreated[clientID] = err
			} else {
				created[clientID] = result
				// Track state change
				if h.stateRepo != nil {
					mailboxID := result["id"].(string)
					if s, err := h.stateRepo.IncrementStateAndLogChange(ctx, accountID, state.ObjectTypeMailbox, mailboxID, state.ChangeTypeCreated); err != nil {
						logger.ErrorContext(ctx, "Failed to track mailbox state change",
							slog.String("account_id", accountID),
							slog.String("mailbox_id", mailboxID),
							slog.String("error", err.Error()),
						)
					} else {
						newState = s
					}
				}
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

			err := h.updateMailbox(ctx, accountID, mailboxID, data)
			if err != nil {
				notUpdated[mailboxID] = err
			} else {
				updated[mailboxID] = nil
				// Track state change
				if h.stateRepo != nil {
					if s, err := h.stateRepo.IncrementStateAndLogChange(ctx, accountID, state.ObjectTypeMailbox, mailboxID, state.ChangeTypeUpdated); err != nil {
						logger.ErrorContext(ctx, "Failed to track mailbox state change",
							slog.String("account_id", accountID),
							slog.String("mailbox_id", mailboxID),
							slog.String("error", err.Error()),
						)
					} else {
						newState = s
					}
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

			err := h.destroyMailbox(ctx, accountID, mailboxID, onDestroyRemoveEmails)
			if err != nil {
				notDestroyed[mailboxID] = err
			} else {
				destroyed = append(destroyed, mailboxID)
				// Track state change
				if h.stateRepo != nil {
					if s, err := h.stateRepo.IncrementStateAndLogChange(ctx, accountID, state.ObjectTypeMailbox, mailboxID, state.ChangeTypeDestroyed); err != nil {
						logger.ErrorContext(ctx, "Failed to track mailbox state change",
							slog.String("account_id", accountID),
							slog.String("mailbox_id", mailboxID),
							slog.String("error", err.Error()),
						)
					} else {
						newState = s
					}
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
func (h *handler) createMailbox(ctx context.Context, accountID string, data map[string]any) (map[string]any, map[string]any) {
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

	// Validate role if provided
	if role != "" && !mailbox.ValidRoles[role] {
		return nil, setError("invalidProperties", "Invalid role: "+role)
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

	return map[string]any{
		"id": mailboxID,
	}, nil
}

// updateMailbox updates an existing mailbox.
func (h *handler) updateMailbox(ctx context.Context, accountID, mailboxID string, data map[string]any) map[string]any {
	// Check for parentId - we don't support hierarchy
	if _, hasParentId := data["parentId"]; hasParentId {
		if data["parentId"] != nil {
			return setError("invalidProperties", "Hierarchical mailboxes are not supported")
		}
	}

	// Get existing mailbox
	mbox, err := h.repo.GetMailbox(ctx, accountID, mailboxID)
	if err != nil {
		if errors.Is(err, mailbox.ErrMailboxNotFound) {
			return setError("notFound", "Mailbox not found")
		}
		logger.ErrorContext(ctx, "Failed to get mailbox",
			slog.String("account_id", accountID),
			slog.String("mailbox_id", mailboxID),
			slog.String("error", err.Error()),
		)
		return setError("serverFail", err.Error())
	}

	// Apply updates
	if name, ok := data["name"].(string); ok {
		mbox.Name = name
	}
	if role, ok := data["role"].(string); ok {
		if role != "" && !mailbox.ValidRoles[role] {
			return setError("invalidProperties", "Invalid role: "+role)
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

	err = h.repo.UpdateMailbox(ctx, mbox)
	if err != nil {
		if errors.Is(err, mailbox.ErrRoleAlreadyExists) {
			return setError("invalidProperties", "A mailbox with this role already exists")
		}
		logger.ErrorContext(ctx, "Failed to update mailbox",
			slog.String("account_id", accountID),
			slog.String("mailbox_id", mailboxID),
			slog.String("error", err.Error()),
		)
		return setError("serverFail", err.Error())
	}

	return nil
}

// destroyMailbox deletes a mailbox.
func (h *handler) destroyMailbox(ctx context.Context, accountID, mailboxID string, onDestroyRemoveEmails bool) map[string]any {
	// Get mailbox to check if it has emails
	mbox, err := h.repo.GetMailbox(ctx, accountID, mailboxID)
	if err != nil {
		if errors.Is(err, mailbox.ErrMailboxNotFound) {
			return setError("notFound", "Mailbox not found")
		}
		logger.ErrorContext(ctx, "Failed to get mailbox",
			slog.String("account_id", accountID),
			slog.String("mailbox_id", mailboxID),
			slog.String("error", err.Error()),
		)
		return setError("serverFail", err.Error())
	}

	// Check if mailbox has emails
	if mbox.TotalEmails > 0 && !onDestroyRemoveEmails {
		return setError("mailboxHasEmail", "Mailbox is not empty")
	}

	// TODO: If onDestroyRemoveEmails is true, we should remove emails from mailbox
	// and destroy orphaned emails. For now, just delete the mailbox.

	err = h.repo.DeleteMailbox(ctx, accountID, mailboxID)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to delete mailbox",
			slog.String("account_id", accountID),
			slog.String("mailbox_id", mailboxID),
			slog.String("error", err.Error()),
		)
		return setError("serverFail", err.Error())
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
	repo := mailbox.NewDynamoDBRepository(dynamoClient, tableName)
	stateRepo := state.NewRepository(dynamoClient, tableName, 7)

	h := newHandler(repo, stateRepo)
	lambda.Start(otellambda.InstrumentHandler(h.handle, xrayconfig.WithRecommendedOptions(tp)...))
}
