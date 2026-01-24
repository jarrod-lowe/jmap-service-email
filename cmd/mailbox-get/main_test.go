package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailbox"
)

type mockMailboxRepository struct {
	getMailboxFunc     func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error)
	getAllMailboxesFunc func(ctx context.Context, accountID string) ([]*mailbox.MailboxItem, error)
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

func TestHandler_SingleMailboxFound(t *testing.T) {
	now := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	mock := &mockMailboxRepository{
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return &mailbox.MailboxItem{
				AccountID:    accountID,
				MailboxID:    mailboxID,
				Name:         "Inbox",
				Role:         "inbox",
				SortOrder:    0,
				TotalEmails:  10,
				UnreadEmails: 3,
				IsSubscribed: true,
				CreatedAt:    now,
				UpdatedAt:    now,
			}, nil
		},
	}

	h := newHandler(mock)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/get",
		Args: map[string]any{
			"ids": []any{"inbox"},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if resp.MethodResponse.Name != "Mailbox/get" {
		t.Errorf("Name = %q, want %q", resp.MethodResponse.Name, "Mailbox/get")
	}

	list, ok := resp.MethodResponse.Args["list"].([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("list = %v, want 1 item", resp.MethodResponse.Args["list"])
	}

	mbox, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("list[0] is not a map")
	}
	if mbox["id"] != "inbox" {
		t.Errorf("id = %v, want %q", mbox["id"], "inbox")
	}
	if mbox["name"] != "Inbox" {
		t.Errorf("name = %v, want %q", mbox["name"], "Inbox")
	}
	if mbox["role"] != "inbox" {
		t.Errorf("role = %v, want %q", mbox["role"], "inbox")
	}
	// Computed fields
	if mbox["parentId"] != nil {
		t.Errorf("parentId = %v, want nil", mbox["parentId"])
	}
	if mbox["totalEmails"] != 10 {
		t.Errorf("totalEmails = %v, want 10", mbox["totalEmails"])
	}
	if mbox["totalThreads"] != 10 { // Stubbed: equals totalEmails
		t.Errorf("totalThreads = %v, want 10", mbox["totalThreads"])
	}
	if mbox["unreadEmails"] != 3 {
		t.Errorf("unreadEmails = %v, want 3", mbox["unreadEmails"])
	}
	if mbox["unreadThreads"] != 3 { // Stubbed: equals unreadEmails
		t.Errorf("unreadThreads = %v, want 3", mbox["unreadThreads"])
	}
}

func TestHandler_MailboxNotFound(t *testing.T) {
	mock := &mockMailboxRepository{
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return nil, mailbox.ErrMailboxNotFound
		},
	}

	h := newHandler(mock)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/get",
		Args: map[string]any{
			"ids": []any{"nonexistent"},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	notFound, ok := resp.MethodResponse.Args["notFound"].([]any)
	if !ok || len(notFound) != 1 {
		t.Fatalf("notFound = %v, want 1 item", resp.MethodResponse.Args["notFound"])
	}
	if notFound[0] != "nonexistent" {
		t.Errorf("notFound[0] = %v, want %q", notFound[0], "nonexistent")
	}
}

func TestHandler_GetAllMailboxes(t *testing.T) {
	now := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	mock := &mockMailboxRepository{
		getAllMailboxesFunc: func(ctx context.Context, accountID string) ([]*mailbox.MailboxItem, error) {
			return []*mailbox.MailboxItem{
				{
					AccountID:    accountID,
					MailboxID:    "inbox",
					Name:         "Inbox",
					Role:         "inbox",
					SortOrder:    0,
					TotalEmails:  10,
					UnreadEmails: 3,
					IsSubscribed: true,
					CreatedAt:    now,
					UpdatedAt:    now,
				},
				{
					AccountID:    accountID,
					MailboxID:    "sent",
					Name:         "Sent",
					Role:         "sent",
					SortOrder:    1,
					TotalEmails:  5,
					UnreadEmails: 0,
					IsSubscribed: true,
					CreatedAt:    now,
					UpdatedAt:    now,
				},
			}, nil
		},
	}

	h := newHandler(mock)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/get",
		Args: map[string]any{
			"ids": nil, // nil means get all
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	list, ok := resp.MethodResponse.Args["list"].([]any)
	if !ok || len(list) != 2 {
		t.Fatalf("list = %v, want 2 items", resp.MethodResponse.Args["list"])
	}
}

func TestHandler_PropertyFiltering(t *testing.T) {
	now := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	mock := &mockMailboxRepository{
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return &mailbox.MailboxItem{
				AccountID:    accountID,
				MailboxID:    mailboxID,
				Name:         "Inbox",
				Role:         "inbox",
				SortOrder:    0,
				TotalEmails:  10,
				UnreadEmails: 3,
				IsSubscribed: true,
				CreatedAt:    now,
				UpdatedAt:    now,
			}, nil
		},
	}

	h := newHandler(mock)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/get",
		Args: map[string]any{
			"ids":        []any{"inbox"},
			"properties": []any{"id", "name"},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	list, ok := resp.MethodResponse.Args["list"].([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("list = %v, want 1 item", resp.MethodResponse.Args["list"])
	}

	mbox, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("list[0] is not a map")
	}

	// Should have requested properties
	if _, ok := mbox["id"]; !ok {
		t.Error("id should be present")
	}
	if _, ok := mbox["name"]; !ok {
		t.Error("name should be present")
	}

	// Should NOT have other properties
	if _, ok := mbox["role"]; ok {
		t.Error("role should NOT be present")
	}
	if _, ok := mbox["totalEmails"]; ok {
		t.Error("totalEmails should NOT be present")
	}
}

func TestHandler_MyRightsComputed(t *testing.T) {
	now := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	mock := &mockMailboxRepository{
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return &mailbox.MailboxItem{
				AccountID:    accountID,
				MailboxID:    mailboxID,
				Name:         "Inbox",
				TotalEmails:  0,
				UnreadEmails: 0,
				IsSubscribed: true,
				CreatedAt:    now,
				UpdatedAt:    now,
			}, nil
		},
	}

	h := newHandler(mock)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/get",
		Args: map[string]any{
			"ids": []any{"inbox"},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}

	list, ok := resp.MethodResponse.Args["list"].([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("list = %v, want 1 item", resp.MethodResponse.Args["list"])
	}
	mbox, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("list[0] is not a map")
	}

	rights, ok := mbox["myRights"].(map[string]any)
	if !ok {
		t.Fatalf("myRights is not a map: %T", mbox["myRights"])
	}

	// All rights should be true
	expectedRights := []string{
		"mayReadItems", "mayAddItems", "mayRemoveItems",
		"maySetSeen", "maySetKeywords", "mayCreateChild",
		"mayRename", "mayDelete", "maySubmit",
	}
	for _, right := range expectedRights {
		if rights[right] != true {
			t.Errorf("rights[%q] = %v, want true", right, rights[right])
		}
	}
}

func TestHandler_InvalidMethod(t *testing.T) {
	mock := &mockMailboxRepository{}

	h := newHandler(mock)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Email/get", // Wrong method
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

func TestHandler_RepositoryError(t *testing.T) {
	mock := &mockMailboxRepository{
		getMailboxFunc: func(ctx context.Context, accountID, mailboxID string) (*mailbox.MailboxItem, error) {
			return nil, errors.New("database error")
		},
	}

	h := newHandler(mock)
	resp, err := h.handle(context.Background(), plugincontract.PluginInvocationRequest{
		AccountID: "user-123",
		Method:    "Mailbox/get",
		Args: map[string]any{
			"ids": []any{"inbox"},
		},
		ClientID: "c0",
	})

	if err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if resp.MethodResponse.Name != "error" {
		t.Errorf("Name = %q, want %q", resp.MethodResponse.Name, "error")
	}
	if resp.MethodResponse.Args["type"] != "serverFail" {
		t.Errorf("type = %v, want %q", resp.MethodResponse.Args["type"], "serverFail")
	}
}
