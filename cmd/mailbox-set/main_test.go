package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jarrod-lowe/jmap-service-libs/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/email"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailbox"
	"github.com/jarrod-lowe/jmap-service-email/internal/state"
)

// mockEmailRepository implements EmailRepository for testing.
type mockEmailRepository struct {
	queryEmailsByMailboxFunc           func(ctx context.Context, accountID, mailboxID string) ([]string, error)
	getEmailFunc                       func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error)
	buildSoftDeleteEmailItemFunc       func(emailItem *email.EmailItem, deletedAt time.Time, apiURL string) types.TransactWriteItem
	buildUpdateEmailMailboxesItemsFunc func(emailItem *email.EmailItem, newMailboxIDs map[string]bool) ([]string, []string, []types.TransactWriteItem)
}

func (m *mockEmailRepository) QueryEmailsByMailbox(ctx context.Context, accountID, mailboxID string) ([]string, error) {
	if m.queryEmailsByMailboxFunc != nil {
		return m.queryEmailsByMailboxFunc(ctx, accountID, mailboxID)
	}
	return nil, nil
}

func (m *mockEmailRepository) GetEmail(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
	if m.getEmailFunc != nil {
		return m.getEmailFunc(ctx, accountID, emailID)
	}
	return nil, email.ErrEmailNotFound
}

func (m *mockEmailRepository) BuildSoftDeleteEmailItem(emailItem *email.EmailItem, deletedAt time.Time, apiURL string) types.TransactWriteItem {
	if m.buildSoftDeleteEmailItemFunc != nil {
		return m.buildSoftDeleteEmailItemFunc(emailItem, deletedAt, apiURL)
	}
	return types.TransactWriteItem{Update: &types.Update{}}
}

func (m *mockEmailRepository) BuildUpdateEmailMailboxesItems(emailItem *email.EmailItem, newMailboxIDs map[string]bool) ([]string, []string, []types.TransactWriteItem) {
	if m.buildUpdateEmailMailboxesItemsFunc != nil {
		return m.buildUpdateEmailMailboxesItemsFunc(emailItem, newMailboxIDs)
	}
	return nil, nil, []types.TransactWriteItem{}
}

type mockMailboxRepository struct {
	getMailboxFunc             func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error)
	getAllMailboxesFunc        func(ctx context.Context, accountID string) ([]*mailbox.MailboxItem, error)
	createMailboxFunc          func(ctx context.Context, mailbox *mailbox.MailboxItem) error
	updateMailboxFunc          func(ctx context.Context, mailbox *mailbox.MailboxItem) error
	deleteMailboxFunc          func(ctx context.Context, accountID, mailboxID string) error
	buildCreateMailboxItemFunc func(mbox *mailbox.MailboxItem) types.TransactWriteItem
	buildUpdateMailboxItemFunc func(mbox *mailbox.MailboxItem) types.TransactWriteItem
	buildDeleteMailboxItemFunc func(accountID, mailboxID string) types.TransactWriteItem
}

func (m *mockMailboxRepository) GetMailbox(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
	if m.getMailboxFunc != nil {
		return m.getMailboxFunc(ctx, accountID, mailboxID)
	}
	return nil, mailbox.ErrMailboxNotFound
}

func (m *mockMailboxRepository) GetAllMailboxes(ctx context.Context, accountID string) ([]*mailbox.MailboxItem, error) {
	if m.getAllMailboxesFunc != nil {
		return m.getAllMailboxesFunc(ctx, accountID)
	}
	return []*mailbox.MailboxItem{}, nil
}

func (m *mockMailboxRepository) CreateMailbox(ctx context.Context, mbox *mailbox.MailboxItem) error {
	if m.createMailboxFunc != nil {
		return m.createMailboxFunc(ctx, mbox)
	}
	return nil
}

func (m *mockMailboxRepository) UpdateMailbox(ctx context.Context, mbox *mailbox.MailboxItem) error {
	if m.updateMailboxFunc != nil {
		return m.updateMailboxFunc(ctx, mbox)
	}
	return nil
}

func (m *mockMailboxRepository) DeleteMailbox(ctx context.Context, accountID, mailboxID string) error {
	if m.deleteMailboxFunc != nil {
		return m.deleteMailboxFunc(ctx, accountID, mailboxID)
	}
	return nil
}

func (m *mockMailboxRepository) BuildCreateMailboxItem(mbox *mailbox.MailboxItem) types.TransactWriteItem {
	if m.buildCreateMailboxItemFunc != nil {
		return m.buildCreateMailboxItemFunc(mbox)
	}
	return types.TransactWriteItem{}
}

func (m *mockMailboxRepository) BuildUpdateMailboxItem(mbox *mailbox.MailboxItem) types.TransactWriteItem {
	if m.buildUpdateMailboxItemFunc != nil {
		return m.buildUpdateMailboxItemFunc(mbox)
	}
	return types.TransactWriteItem{}
}

func (m *mockMailboxRepository) BuildDeleteMailboxItem(accountID, mailboxID string) types.TransactWriteItem {
	if m.buildDeleteMailboxItemFunc != nil {
		return m.buildDeleteMailboxItemFunc(accountID, mailboxID)
	}
	return types.TransactWriteItem{}
}

// mockStateRepository implements the StateRepository interface for testing.
type mockStateRepository struct {
	getCurrentStateFunc            func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error)
	incrementStateAndLogChangeFunc func(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error)
	buildStateChangeItemsFunc      func(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem)
}

func (m *mockStateRepository) GetCurrentState(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
	if m.getCurrentStateFunc != nil {
		return m.getCurrentStateFunc(ctx, accountID, objectType)
	}
	return 0, nil
}

func (m *mockStateRepository) IncrementStateAndLogChange(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error) {
	if m.incrementStateAndLogChangeFunc != nil {
		return m.incrementStateAndLogChangeFunc(ctx, accountID, objectType, objectID, changeType)
	}
	return 0, nil
}

func (m *mockStateRepository) BuildStateChangeItems(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem) {
	if m.buildStateChangeItemsFunc != nil {
		return m.buildStateChangeItemsFunc(accountID, objectType, currentState, objectID, changeType)
	}
	return currentState + 1, []types.TransactWriteItem{}
}

// mockTransactWriter implements the TransactWriter interface for testing.
type mockTransactWriter struct {
	transactWriteItemsFunc func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

func (m *mockTransactWriter) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if m.transactWriteItemsFunc != nil {
		return m.transactWriteItemsFunc(ctx, input, opts...)
	}
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

// Test: Create mailbox with role sets ID to role value
func TestHandler_CreateMailboxWithRole(t *testing.T) {
	var createdMailbox *mailbox.MailboxItem
	mock := &mockMailboxRepository{
		createMailboxFunc: func(ctx context.Context, mbox *mailbox.MailboxItem) error {
			createdMailbox = mbox
			return nil
		},
	}

	h := newHandler(mock, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"create": map[string]any{
				"c0": map[string]any{
					"name": "Inbox",
					"role": "inbox",
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if resp.MethodResponse.Name != "Mailbox/set" {
		t.Errorf("Name = %q, want %q", resp.MethodResponse.Name, "Mailbox/set")
	}

	created, ok := resp.MethodResponse.Args["created"].(map[string]any)
	if !ok {
		t.Fatalf("created not a map: %T", resp.MethodResponse.Args["created"])
	}
	c0, ok := created["c0"].(map[string]any)
	if !ok {
		t.Fatalf("created[c0] not a map: %T", created["c0"])
	}
	if c0["id"] != "inbox" {
		t.Errorf("id = %v, want %q", c0["id"], "inbox")
	}
	if createdMailbox == nil {
		t.Fatal("CreateMailbox was not called")
	}
	if createdMailbox.MailboxID != "inbox" {
		t.Errorf("MailboxID = %q, want %q", createdMailbox.MailboxID, "inbox")
	}
}

// Test: Create mailbox without role generates UUID
func TestHandler_CreateMailboxWithoutRole(t *testing.T) {
	var createdMailbox *mailbox.MailboxItem
	mock := &mockMailboxRepository{
		createMailboxFunc: func(ctx context.Context, mbox *mailbox.MailboxItem) error {
			createdMailbox = mbox
			return nil
		},
	}

	h := newHandler(mock, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"create": map[string]any{
				"c0": map[string]any{
					"name": "Custom Folder",
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	created, ok := resp.MethodResponse.Args["created"].(map[string]any)
	if !ok {
		t.Fatalf("created not a map: %T", resp.MethodResponse.Args["created"])
	}
	c0, ok := created["c0"].(map[string]any)
	if !ok {
		t.Fatalf("created[c0] not a map: %T", created["c0"])
	}

	id, ok := c0["id"].(string)
	if !ok || id == "" {
		t.Errorf("id should be a non-empty string, got %v", c0["id"])
	}
	// UUID format validation: should not equal any role name
	if mailbox.ValidRoles[id] {
		t.Errorf("id %q should be a UUID, not a role name", id)
	}
	if createdMailbox == nil {
		t.Fatal("CreateMailbox was not called")
	}
}

// Test: Create mailbox with invalid role returns error
func TestHandler_CreateMailboxInvalidRole(t *testing.T) {
	mock := &mockMailboxRepository{}

	h := newHandler(mock, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"create": map[string]any{
				"c0": map[string]any{
					"name": "Test",
					"role": "invalid-role",
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notCreated, ok := resp.MethodResponse.Args["notCreated"].(map[string]any)
	if !ok {
		t.Fatalf("notCreated not a map: %T", resp.MethodResponse.Args["notCreated"])
	}
	c0, ok := notCreated["c0"].(map[string]any)
	if !ok {
		t.Fatalf("notCreated[c0] not a map: %T", notCreated["c0"])
	}
	if c0["type"] != "invalidProperties" {
		t.Errorf("type = %v, want %q", c0["type"], "invalidProperties")
	}
}

// Test: Create mailbox with parentId rejected (flat hierarchy only)
func TestHandler_CreateMailboxParentIdRejected(t *testing.T) {
	mock := &mockMailboxRepository{}

	h := newHandler(mock, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"create": map[string]any{
				"c0": map[string]any{
					"name":     "Child",
					"parentId": "some-parent",
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notCreated, ok := resp.MethodResponse.Args["notCreated"].(map[string]any)
	if !ok {
		t.Fatalf("notCreated not a map: %T", resp.MethodResponse.Args["notCreated"])
	}
	c0, ok := notCreated["c0"].(map[string]any)
	if !ok {
		t.Fatalf("notCreated[c0] not a map: %T", notCreated["c0"])
	}
	if c0["type"] != "invalidProperties" {
		t.Errorf("type = %v, want %q", c0["type"], "invalidProperties")
	}
}

// Test: Create mailbox with duplicate role returns error
func TestHandler_CreateMailboxDuplicateRole(t *testing.T) {
	mock := &mockMailboxRepository{
		createMailboxFunc: func(ctx context.Context, mbox *mailbox.MailboxItem) error {
			return mailbox.ErrRoleAlreadyExists
		},
	}

	h := newHandler(mock, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"create": map[string]any{
				"c0": map[string]any{
					"name": "Inbox",
					"role": "inbox",
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notCreated, ok := resp.MethodResponse.Args["notCreated"].(map[string]any)
	if !ok {
		t.Fatalf("notCreated not a map: %T", resp.MethodResponse.Args["notCreated"])
	}
	c0, ok := notCreated["c0"].(map[string]any)
	if !ok {
		t.Fatalf("notCreated[c0] not a map: %T", notCreated["c0"])
	}
	if c0["type"] != "invalidProperties" {
		t.Errorf("type = %v, want %q", c0["type"], "invalidProperties")
	}
}

// Test: Update mailbox successfully
func TestHandler_UpdateMailbox(t *testing.T) {
	var updatedMailbox *mailbox.MailboxItem
	mock := &mockMailboxRepository{
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return &mailbox.MailboxItem{
				AccountID:    accountID,
				MailboxID:    mailboxID,
				Name:         "Old Name",
				TotalEmails:  5,
				UnreadEmails: 2,
				IsSubscribed: true,
			}, nil
		},
		updateMailboxFunc: func(ctx context.Context, mbox *mailbox.MailboxItem) error {
			updatedMailbox = mbox
			return nil
		},
	}

	h := newHandler(mock, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"update": map[string]any{
				"inbox": map[string]any{
					"name":      "New Name",
					"sortOrder": float64(10),
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	updated, ok := resp.MethodResponse.Args["updated"].(map[string]any)
	if !ok {
		t.Fatalf("updated not a map: %T", resp.MethodResponse.Args["updated"])
	}
	if _, ok := updated["inbox"]; !ok {
		t.Error("inbox should be in updated")
	}
	if updatedMailbox == nil {
		t.Fatal("UpdateMailbox was not called")
	}
	if updatedMailbox.Name != "New Name" {
		t.Errorf("Name = %q, want %q", updatedMailbox.Name, "New Name")
	}
	if updatedMailbox.SortOrder != 10 {
		t.Errorf("SortOrder = %d, want 10", updatedMailbox.SortOrder)
	}
}

// Test: Update mailbox with parentId returns error
func TestHandler_UpdateMailboxParentIdRejected(t *testing.T) {
	mock := &mockMailboxRepository{
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return &mailbox.MailboxItem{
				AccountID: accountID,
				MailboxID: mailboxID,
				Name:      "Test",
			}, nil
		},
	}

	h := newHandler(mock, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"update": map[string]any{
				"inbox": map[string]any{
					"parentId": "some-parent",
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notUpdated, ok := resp.MethodResponse.Args["notUpdated"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated not a map: %T", resp.MethodResponse.Args["notUpdated"])
	}
	inbox, ok := notUpdated["inbox"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated[inbox] not a map: %T", notUpdated["inbox"])
	}
	if inbox["type"] != "invalidProperties" {
		t.Errorf("type = %v, want %q", inbox["type"], "invalidProperties")
	}
}

// Test: Update non-existent mailbox returns notFound
func TestHandler_UpdateMailboxNotFound(t *testing.T) {
	mock := &mockMailboxRepository{
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return nil, mailbox.ErrMailboxNotFound
		},
	}

	h := newHandler(mock, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"update": map[string]any{
				"nonexistent": map[string]any{
					"name": "New Name",
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notUpdated, ok := resp.MethodResponse.Args["notUpdated"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated not a map: %T", resp.MethodResponse.Args["notUpdated"])
	}
	item, ok := notUpdated["nonexistent"].(map[string]any)
	if !ok {
		t.Fatalf("notUpdated[nonexistent] not a map: %T", notUpdated["nonexistent"])
	}
	if item["type"] != "notFound" {
		t.Errorf("type = %v, want %q", item["type"], "notFound")
	}
}

// Test: Destroy empty mailbox succeeds
func TestHandler_DestroyMailbox(t *testing.T) {
	deleteCalled := false
	mock := &mockMailboxRepository{
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return &mailbox.MailboxItem{
				AccountID:   accountID,
				MailboxID:   mailboxID,
				Name:        "Test",
				TotalEmails: 0, // Empty mailbox
			}, nil
		},
		deleteMailboxFunc: func(ctx context.Context, accountID, mailboxID string) error {
			deleteCalled = true
			return nil
		},
	}

	h := newHandler(mock, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"destroy": []any{"test-mailbox"},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	destroyed, ok := resp.MethodResponse.Args["destroyed"].([]any)
	if !ok {
		t.Fatalf("destroyed not a slice: %T", resp.MethodResponse.Args["destroyed"])
	}
	if len(destroyed) != 1 || destroyed[0] != "test-mailbox" {
		t.Errorf("destroyed = %v, want [test-mailbox]", destroyed)
	}
	if !deleteCalled {
		t.Error("DeleteMailbox was not called")
	}
}

// Test: Destroy non-empty mailbox without onDestroyRemoveEmails returns mailboxHasEmail
func TestHandler_DestroyNonEmptyMailbox(t *testing.T) {
	mock := &mockMailboxRepository{
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return &mailbox.MailboxItem{
				AccountID:   accountID,
				MailboxID:   mailboxID,
				Name:        "Test",
				TotalEmails: 5, // Non-empty
			}, nil
		},
	}

	h := newHandler(mock, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"destroy": []any{"test-mailbox"},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notDestroyed, ok := resp.MethodResponse.Args["notDestroyed"].(map[string]any)
	if !ok {
		t.Fatalf("notDestroyed not a map: %T", resp.MethodResponse.Args["notDestroyed"])
	}
	item, ok := notDestroyed["test-mailbox"].(map[string]any)
	if !ok {
		t.Fatalf("notDestroyed[test-mailbox] not a map: %T", notDestroyed["test-mailbox"])
	}
	if item["type"] != "mailboxHasEmail" {
		t.Errorf("type = %v, want %q", item["type"], "mailboxHasEmail")
	}
}

// Test: Destroy non-existent mailbox returns notFound
func TestHandler_DestroyMailboxNotFound(t *testing.T) {
	mock := &mockMailboxRepository{
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return nil, mailbox.ErrMailboxNotFound
		},
	}

	h := newHandler(mock, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"destroy": []any{"nonexistent"},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notDestroyed, ok := resp.MethodResponse.Args["notDestroyed"].(map[string]any)
	if !ok {
		t.Fatalf("notDestroyed not a map: %T", resp.MethodResponse.Args["notDestroyed"])
	}
	item, ok := notDestroyed["nonexistent"].(map[string]any)
	if !ok {
		t.Fatalf("notDestroyed[nonexistent] not a map: %T", notDestroyed["nonexistent"])
	}
	if item["type"] != "notFound" {
		t.Errorf("type = %v, want %q", item["type"], "notFound")
	}
}

// Test: Invalid method returns error
func TestHandler_InvalidMethod(t *testing.T) {
	mock := &mockMailboxRepository{}

	h := newHandler(mock, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/set", // Wrong method
		Args:      map[string]any{},
		ClientID:  "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if resp.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", resp.MethodResponse.Name, "error")
	}
	if resp.MethodResponse.Args["type"] != "unknownMethod" {
		t.Errorf("type = %v, want %q", resp.MethodResponse.Args["type"], "unknownMethod")
	}
}

// Test: Repository error on create returns serverFail
func TestHandler_CreateRepositoryError(t *testing.T) {
	mock := &mockMailboxRepository{
		createMailboxFunc: func(ctx context.Context, mbox *mailbox.MailboxItem) error {
			return errors.New("database error")
		},
	}

	h := newHandler(mock, nil)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"create": map[string]any{
				"c0": map[string]any{
					"name": "Test",
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notCreated, ok := resp.MethodResponse.Args["notCreated"].(map[string]any)
	if !ok {
		t.Fatalf("notCreated not a map: %T", resp.MethodResponse.Args["notCreated"])
	}
	c0, ok := notCreated["c0"].(map[string]any)
	if !ok {
		t.Fatalf("notCreated[c0] not a map: %T", notCreated["c0"])
	}
	if c0["type"] != "serverFail" {
		t.Errorf("type = %v, want %q", c0["type"], "serverFail")
	}
}

// Test: State tracking on create, update, destroy
func TestHandler_StateTracking(t *testing.T) {
	stateChanges := []struct {
		objectID   string
		changeType state.ChangeType
	}{}

	mockRepo := &mockMailboxRepository{
		createMailboxFunc: func(ctx context.Context, mbox *mailbox.MailboxItem) error {
			return nil
		},
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return &mailbox.MailboxItem{
				AccountID:   accountID,
				MailboxID:   mailboxID,
				Name:        "Test",
				TotalEmails: 0,
			}, nil
		},
		updateMailboxFunc: func(ctx context.Context, mbox *mailbox.MailboxItem) error {
			return nil
		},
		deleteMailboxFunc: func(ctx context.Context, accountID, mailboxID string) error {
			return nil
		},
	}

	currentState := int64(10)
	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			if objectType != state.ObjectTypeMailbox {
				t.Errorf("objectType = %q, want %q", objectType, state.ObjectTypeMailbox)
			}
			return currentState, nil
		},
		incrementStateAndLogChangeFunc: func(ctx context.Context, accountID string, objectType state.ObjectType, objectID string, changeType state.ChangeType) (int64, error) {
			stateChanges = append(stateChanges, struct {
				objectID   string
				changeType state.ChangeType
			}{objectID, changeType})
			currentState++
			return currentState, nil
		},
	}

	h := newHandler(mockRepo, mockStateRepo)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"create": map[string]any{
				"c0": map[string]any{
					"name": "New Folder",
				},
			},
			"update": map[string]any{
				"existing": map[string]any{
					"name": "Updated Name",
				},
			},
			"destroy": []any{"to-delete"},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if resp.MethodResponse.Name != "Mailbox/set" {
		t.Fatalf("Name = %q, want %q", resp.MethodResponse.Name, "Mailbox/set")
	}

	// Check oldState and newState
	oldState, ok := resp.MethodResponse.Args["oldState"].(string)
	if !ok || oldState != "10" {
		t.Errorf("oldState = %v, want %q", resp.MethodResponse.Args["oldState"], "10")
	}
	newState, ok := resp.MethodResponse.Args["newState"].(string)
	if !ok || newState != "13" {
		t.Errorf("newState = %v, want %q (expected 3 state changes)", resp.MethodResponse.Args["newState"], "13")
	}

	// Verify state changes were recorded
	if len(stateChanges) != 3 {
		t.Fatalf("expected 3 state changes, got %d", len(stateChanges))
	}

	// Find each change type
	var hasCreate, hasUpdate, hasDestroy bool
	for _, change := range stateChanges {
		switch change.changeType {
		case state.ChangeTypeCreated:
			hasCreate = true
		case state.ChangeTypeUpdated:
			hasUpdate = true
			if change.objectID != "existing" {
				t.Errorf("update change objectID = %q, want %q", change.objectID, "existing")
			}
		case state.ChangeTypeDestroyed:
			hasDestroy = true
			if change.objectID != "to-delete" {
				t.Errorf("destroy change objectID = %q, want %q", change.objectID, "to-delete")
			}
		}
	}

	if !hasCreate {
		t.Error("expected create state change")
	}
	if !hasUpdate {
		t.Error("expected update state change")
	}
	if !hasDestroy {
		t.Error("expected destroy state change")
	}
}

// Test: State repository error on GetCurrentState returns serverFail
func TestHandler_StateGetError(t *testing.T) {
	mockRepo := &mockMailboxRepository{}
	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 0, errors.New("state lookup failed")
		},
	}

	h := newHandler(mockRepo, mockStateRepo)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args:      map[string]any{},
		ClientID:  "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if resp.MethodResponse.Name != "error" {
		t.Fatalf("Name = %q, want %q", resp.MethodResponse.Name, "error")
	}

	errorType, ok := resp.MethodResponse.Args["type"].(string)
	if !ok || errorType != "serverFail" {
		t.Errorf("error type = %v, want %q", resp.MethodResponse.Args["type"], "serverFail")
	}
}

// Test: Create mailbox with transaction combines mailbox creation and state change atomically
func TestHandler_CreateMailboxTransaction(t *testing.T) {
	var transactInput *dynamodb.TransactWriteItemsInput

	mockRepo := &mockMailboxRepository{
		getAllMailboxesFunc: func(ctx context.Context, accountID string) ([]*mailbox.MailboxItem, error) {
			return []*mailbox.MailboxItem{}, nil
		},
		buildCreateMailboxItemFunc: func(mbox *mailbox.MailboxItem) types.TransactWriteItem {
			return types.TransactWriteItem{
				Put: &types.Put{
					TableName: aws.String("test-table"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "ACCOUNT#" + mbox.AccountID},
						"sk": &types.AttributeValueMemberS{Value: "MAILBOX#" + mbox.MailboxID},
					},
				},
			}
		},
	}

	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 5, nil
		},
		buildStateChangeItemsFunc: func(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem) {
			newState := currentState + 1
			return newState, []types.TransactWriteItem{
				{
					Update: &types.Update{
						TableName: aws.String("test-table"),
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "state-key"},
						},
					},
				},
			}
		},
	}

	mockTransactor := &mockTransactWriter{
		transactWriteItemsFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			transactInput = input
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	h := newHandler(mockRepo, mockStateRepo, mockTransactor)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"create": map[string]any{
				"c0": map[string]any{
					"name": "Test Folder",
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	// Verify mailbox was created
	created, ok := resp.MethodResponse.Args["created"].(map[string]any)
	if !ok {
		t.Fatalf("created not a map: %T", resp.MethodResponse.Args["created"])
	}
	if _, ok := created["c0"]; !ok {
		t.Fatal("c0 should be in created")
	}

	// Verify transaction was executed with both mailbox creation and state change
	if transactInput == nil {
		t.Fatal("TransactWriteItems was not called")
	}
	if len(transactInput.TransactItems) != 2 {
		t.Fatalf("expected 2 transaction items (mailbox + state), got %d", len(transactInput.TransactItems))
	}

	// Verify state was updated correctly
	newState, ok := resp.MethodResponse.Args["newState"].(string)
	if !ok || newState != "6" {
		t.Errorf("newState = %v, want %q", resp.MethodResponse.Args["newState"], "6")
	}
}

// Test: Update mailbox with transaction combines mailbox update and state change atomically
func TestHandler_UpdateMailboxTransaction(t *testing.T) {
	var transactInput *dynamodb.TransactWriteItemsInput

	mockRepo := &mockMailboxRepository{
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return &mailbox.MailboxItem{
				AccountID:    accountID,
				MailboxID:    mailboxID,
				Name:         "Old Name",
				TotalEmails:  5,
				UnreadEmails: 2,
				IsSubscribed: true,
			}, nil
		},
		buildUpdateMailboxItemFunc: func(mbox *mailbox.MailboxItem) types.TransactWriteItem {
			return types.TransactWriteItem{
				Update: &types.Update{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "ACCOUNT#" + mbox.AccountID},
						"sk": &types.AttributeValueMemberS{Value: "MAILBOX#" + mbox.MailboxID},
					},
				},
			}
		},
	}

	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 10, nil
		},
		buildStateChangeItemsFunc: func(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem) {
			newState := currentState + 1
			return newState, []types.TransactWriteItem{
				{
					Update: &types.Update{
						TableName: aws.String("test-table"),
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "state-key"},
						},
					},
				},
			}
		},
	}

	mockTransactor := &mockTransactWriter{
		transactWriteItemsFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			transactInput = input
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	h := newHandler(mockRepo, mockStateRepo, mockTransactor)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"update": map[string]any{
				"inbox": map[string]any{
					"name": "New Name",
				},
			},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	// Verify mailbox was updated
	updated, ok := resp.MethodResponse.Args["updated"].(map[string]any)
	if !ok {
		t.Fatalf("updated not a map: %T", resp.MethodResponse.Args["updated"])
	}
	if _, ok := updated["inbox"]; !ok {
		t.Fatal("inbox should be in updated")
	}

	// Verify transaction was executed with both mailbox update and state change
	if transactInput == nil {
		t.Fatal("TransactWriteItems was not called")
	}
	if len(transactInput.TransactItems) != 2 {
		t.Fatalf("expected 2 transaction items (mailbox + state), got %d", len(transactInput.TransactItems))
	}

	// Verify state was updated correctly
	newState, ok := resp.MethodResponse.Args["newState"].(string)
	if !ok || newState != "11" {
		t.Errorf("newState = %v, want %q", resp.MethodResponse.Args["newState"], "11")
	}
}

// Test: Destroy mailbox with transaction combines mailbox deletion and state change atomically
func TestHandler_DestroyMailboxTransaction(t *testing.T) {
	var transactInput *dynamodb.TransactWriteItemsInput

	mockRepo := &mockMailboxRepository{
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return &mailbox.MailboxItem{
				AccountID:   accountID,
				MailboxID:   mailboxID,
				Name:        "Test",
				TotalEmails: 0, // Empty
			}, nil
		},
		buildDeleteMailboxItemFunc: func(accountID, mailboxID string) types.TransactWriteItem {
			return types.TransactWriteItem{
				Delete: &types.Delete{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "ACCOUNT#" + accountID},
						"sk": &types.AttributeValueMemberS{Value: "MAILBOX#" + mailboxID},
					},
				},
			}
		},
	}

	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 20, nil
		},
		buildStateChangeItemsFunc: func(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem) {
			newState := currentState + 1
			return newState, []types.TransactWriteItem{
				{
					Update: &types.Update{
						TableName: aws.String("test-table"),
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "state-key"},
						},
					},
				},
			}
		},
	}

	mockTransactor := &mockTransactWriter{
		transactWriteItemsFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			transactInput = input
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	h := newHandler(mockRepo, mockStateRepo, mockTransactor)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"destroy": []any{"test-mailbox"},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	// Verify mailbox was destroyed
	destroyed, ok := resp.MethodResponse.Args["destroyed"].([]any)
	if !ok {
		t.Fatalf("destroyed not a slice: %T", resp.MethodResponse.Args["destroyed"])
	}
	if len(destroyed) != 1 || destroyed[0] != "test-mailbox" {
		t.Errorf("destroyed = %v, want [test-mailbox]", destroyed)
	}

	// Verify transaction was executed with both mailbox deletion and state change
	if transactInput == nil {
		t.Fatal("TransactWriteItems was not called")
	}
	if len(transactInput.TransactItems) != 2 {
		t.Fatalf("expected 2 transaction items (mailbox + state), got %d", len(transactInput.TransactItems))
	}

	// Verify state was updated correctly
	newState, ok := resp.MethodResponse.Args["newState"].(string)
	if !ok || newState != "21" {
		t.Errorf("newState = %v, want %q", resp.MethodResponse.Args["newState"], "21")
	}
}

// Test: Destroy non-empty mailbox with onDestroyRemoveEmails publishes cleanup message
func TestHandler_DestroyMailboxWithOnDestroyRemoveEmails(t *testing.T) {
	mockRepo := &mockMailboxRepository{
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return &mailbox.MailboxItem{
				AccountID:   accountID,
				MailboxID:   mailboxID,
				Name:        "Test",
				TotalEmails: 5, // Non-empty
			}, nil
		},
		buildDeleteMailboxItemFunc: func(accountID, mailboxID string) types.TransactWriteItem {
			return types.TransactWriteItem{
				Delete: &types.Delete{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "ACCOUNT#" + accountID},
						"sk": &types.AttributeValueMemberS{Value: "MAILBOX#" + mailboxID},
					},
				},
			}
		},
	}

	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 10, nil
		},
		buildStateChangeItemsFunc: func(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem) {
			return currentState + 1, []types.TransactWriteItem{}
		},
	}

	mockTransactor := &mockTransactWriter{}

	var softDeletedEmails []string
	mockEmailRepo := &mockEmailRepository{
		queryEmailsByMailboxFunc: func(ctx context.Context, accountID, mailboxID string) ([]string, error) {
			return []string{"email-1"}, nil
		},
		getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return &email.EmailItem{
				AccountID:  accountID,
				EmailID:    emailID,
				MailboxIDs: map[string]bool{"test-mailbox": true},
				Version:    1,
			}, nil
		},
		buildSoftDeleteEmailItemFunc: func(emailItem *email.EmailItem, deletedAt time.Time, apiURL string) types.TransactWriteItem {
			softDeletedEmails = append(softDeletedEmails, emailItem.EmailID)
			return types.TransactWriteItem{Update: &types.Update{}}
		},
	}

	h := newHandler(mockRepo, mockStateRepo, mockTransactor)
	h.emailRepo = mockEmailRepo

	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"destroy":                []any{"test-mailbox"},
			"onDestroyRemoveEmails": true,
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	// Verify mailbox was destroyed
	destroyed, ok := resp.MethodResponse.Args["destroyed"].([]any)
	if !ok {
		t.Fatalf("destroyed not a slice: %T", resp.MethodResponse.Args["destroyed"])
	}
	if len(destroyed) != 1 || destroyed[0] != "test-mailbox" {
		t.Errorf("destroyed = %v, want [test-mailbox]", destroyed)
	}

	// Verify email was soft-deleted
	if len(softDeletedEmails) != 1 || softDeletedEmails[0] != "email-1" {
		t.Errorf("soft-deleted emails = %v, want [email-1]", softDeletedEmails)
	}
}

// Test: Destroy empty mailbox with onDestroyRemoveEmails does NOT publish cleanup
func TestHandler_DestroyEmptyMailboxWithOnDestroyRemoveEmails(t *testing.T) {
	mockRepo := &mockMailboxRepository{
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return &mailbox.MailboxItem{
				AccountID:   accountID,
				MailboxID:   mailboxID,
				Name:        "Test",
				TotalEmails: 0, // Empty
			}, nil
		},
		buildDeleteMailboxItemFunc: func(accountID, mailboxID string) types.TransactWriteItem {
			return types.TransactWriteItem{
				Delete: &types.Delete{
					TableName: aws.String("test-table"),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "ACCOUNT#" + accountID},
						"sk": &types.AttributeValueMemberS{Value: "MAILBOX#" + mailboxID},
					},
				},
			}
		},
	}

	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 10, nil
		},
		buildStateChangeItemsFunc: func(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem) {
			return currentState + 1, []types.TransactWriteItem{}
		},
	}

	mockTransactor := &mockTransactWriter{}

	queryCalled := false
	mockEmailRepo := &mockEmailRepository{
		queryEmailsByMailboxFunc: func(ctx context.Context, accountID, mailboxID string) ([]string, error) {
			queryCalled = true
			return nil, nil
		},
	}

	h := newHandler(mockRepo, mockStateRepo, mockTransactor)
	h.emailRepo = mockEmailRepo

	_, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		Args: map[string]any{
			"destroy":                []any{"test-mailbox"},
			"onDestroyRemoveEmails": true,
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	// Verify NO email cleanup was attempted (empty mailbox)
	if queryCalled {
		t.Error("expected no email query for empty mailbox")
	}
}

// Test: request.APIURL flows through to BuildSoftDeleteEmailItem
func TestHandler_DestroyMailboxAPIURLPassedToSoftDelete(t *testing.T) {
	mockRepo := &mockMailboxRepository{
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return &mailbox.MailboxItem{
				AccountID:   accountID,
				MailboxID:   mailboxID,
				Name:        "Test",
				TotalEmails: 1,
			}, nil
		},
		buildDeleteMailboxItemFunc: func(accountID, mailboxID string) types.TransactWriteItem {
			return types.TransactWriteItem{Delete: &types.Delete{}}
		},
	}

	mockStateRepo := &mockStateRepository{
		getCurrentStateFunc: func(ctx context.Context, accountID string, objectType state.ObjectType) (int64, error) {
			return 10, nil
		},
		buildStateChangeItemsFunc: func(accountID string, objectType state.ObjectType, currentState int64, objectID string, changeType state.ChangeType) (int64, []types.TransactWriteItem) {
			return currentState + 1, []types.TransactWriteItem{}
		},
	}

	mockTransactor := &mockTransactWriter{}

	var capturedAPIURL string
	mockEmailRepo := &mockEmailRepository{
		queryEmailsByMailboxFunc: func(ctx context.Context, accountID, mailboxID string) ([]string, error) {
			return []string{"email-1"}, nil
		},
		getEmailFunc: func(ctx context.Context, accountID, emailID string) (*email.EmailItem, error) {
			return &email.EmailItem{
				AccountID:  accountID,
				EmailID:    emailID,
				MailboxIDs: map[string]bool{"test-mailbox": true},
				Version:    1,
			}, nil
		},
		buildSoftDeleteEmailItemFunc: func(emailItem *email.EmailItem, deletedAt time.Time, apiURL string) types.TransactWriteItem {
			capturedAPIURL = apiURL
			return types.TransactWriteItem{Update: &types.Update{}}
		},
	}

	h := newHandler(mockRepo, mockStateRepo, mockTransactor)
	h.emailRepo = mockEmailRepo

	_, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/set",
		APIURL:    "https://api.example.com/stage",
		Args: map[string]any{
			"destroy":                []any{"test-mailbox"},
			"onDestroyRemoveEmails": true,
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	if capturedAPIURL != "https://api.example.com/stage" {
		t.Errorf("apiURL = %q, want %q", capturedAPIURL, "https://api.example.com/stage")
	}
}
