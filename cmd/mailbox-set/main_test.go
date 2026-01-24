package main

import (
	"context"
	"errors"
	"testing"

	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailbox"
)

type mockMailboxRepository struct {
	getMailboxFunc      func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error)
	getAllMailboxesFunc func(ctx context.Context, accountID string) ([]*mailbox.MailboxItem, error)
	createMailboxFunc   func(ctx context.Context, mailbox *mailbox.MailboxItem) error
	updateMailboxFunc   func(ctx context.Context, mailbox *mailbox.MailboxItem) error
	deleteMailboxFunc   func(ctx context.Context, accountID, mailboxID string) error
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

// Test: Create mailbox with role sets ID to role value
func TestHandler_CreateMailboxWithRole(t *testing.T) {
	var createdMailbox *mailbox.MailboxItem
	mock := &mockMailboxRepository{
		createMailboxFunc: func(ctx context.Context, mbox *mailbox.MailboxItem) error {
			createdMailbox = mbox
			return nil
		},
	}

	h := newHandler(mock)
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

	h := newHandler(mock)
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

	h := newHandler(mock)
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

// Test: Create mailbox with duplicate role returns error
func TestHandler_CreateMailboxDuplicateRole(t *testing.T) {
	mock := &mockMailboxRepository{
		createMailboxFunc: func(ctx context.Context, mbox *mailbox.MailboxItem) error {
			return mailbox.ErrRoleAlreadyExists
		},
	}

	h := newHandler(mock)
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

	h := newHandler(mock)
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

	h := newHandler(mock)
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

	h := newHandler(mock)
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

	h := newHandler(mock)
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

	h := newHandler(mock)
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

	h := newHandler(mock)
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

	h := newHandler(mock)
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

	h := newHandler(mock)
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
