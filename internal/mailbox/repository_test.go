package mailbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// mockDynamoDBClient is a test double for DynamoDB operations.
type mockDynamoDBClient struct {
	getItemFunc            func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	queryFunc              func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	putItemFunc            func(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	updateItemFunc         func(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	deleteItemFunc         func(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	transactWriteItemsFunc func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

func (m *mockDynamoDBClient) GetItem(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if m.getItemFunc != nil {
		return m.getItemFunc(ctx, input, opts...)
	}
	return &dynamodb.GetItemOutput{}, nil
}

func (m *mockDynamoDBClient) Query(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if m.queryFunc != nil {
		return m.queryFunc(ctx, input, opts...)
	}
	return &dynamodb.QueryOutput{}, nil
}

func (m *mockDynamoDBClient) PutItem(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if m.putItemFunc != nil {
		return m.putItemFunc(ctx, input, opts...)
	}
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockDynamoDBClient) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if m.updateItemFunc != nil {
		return m.updateItemFunc(ctx, input, opts...)
	}
	return &dynamodb.UpdateItemOutput{}, nil
}

func (m *mockDynamoDBClient) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if m.deleteItemFunc != nil {
		return m.deleteItemFunc(ctx, input, opts...)
	}
	return &dynamodb.DeleteItemOutput{}, nil
}

func (m *mockDynamoDBClient) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if m.transactWriteItemsFunc != nil {
		return m.transactWriteItemsFunc(ctx, input, opts...)
	}
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func TestDynamoDBRepository_GetMailbox(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	mock := &mockDynamoDBClient{
		getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			// Verify the keys
			if pk, ok := input.Key["pk"].(*types.AttributeValueMemberS); !ok || pk.Value != "ACCOUNT#user-123" {
				t.Errorf("unexpected pk: %v", input.Key["pk"])
			}
			if sk, ok := input.Key["sk"].(*types.AttributeValueMemberS); !ok || sk.Value != "MAILBOX#inbox" {
				t.Errorf("unexpected sk: %v", input.Key["sk"])
			}

			return &dynamodb.GetItemOutput{
				Item: map[string]types.AttributeValue{
					"mailboxId":    &types.AttributeValueMemberS{Value: "inbox"},
					"accountId":    &types.AttributeValueMemberS{Value: "user-123"},
					"name":         &types.AttributeValueMemberS{Value: "Inbox"},
					"role":         &types.AttributeValueMemberS{Value: "inbox"},
					"sortOrder":    &types.AttributeValueMemberN{Value: "0"},
					"totalEmails":  &types.AttributeValueMemberN{Value: "10"},
					"unreadEmails": &types.AttributeValueMemberN{Value: "3"},
					"isSubscribed": &types.AttributeValueMemberBOOL{Value: true},
					"createdAt":    &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
					"updatedAt":    &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
				},
			}, nil
		},
	}

	repo := NewDynamoDBRepository(mock, "test-table")
	mailbox, err := repo.GetMailbox(ctx, "user-123", "inbox")

	if err != nil {
		t.Fatalf("GetMailbox() error = %v", err)
	}
	if mailbox.MailboxID != "inbox" {
		t.Errorf("MailboxID = %q, want %q", mailbox.MailboxID, "inbox")
	}
	if mailbox.Name != "Inbox" {
		t.Errorf("Name = %q, want %q", mailbox.Name, "Inbox")
	}
	if mailbox.TotalEmails != 10 {
		t.Errorf("TotalEmails = %d, want %d", mailbox.TotalEmails, 10)
	}
	if mailbox.UnreadEmails != 3 {
		t.Errorf("UnreadEmails = %d, want %d", mailbox.UnreadEmails, 3)
	}
}

func TestDynamoDBRepository_GetMailbox_NotFound(t *testing.T) {
	ctx := context.Background()

	mock := &mockDynamoDBClient{
		getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}

	repo := NewDynamoDBRepository(mock, "test-table")
	_, err := repo.GetMailbox(ctx, "user-123", "nonexistent")

	if !errors.Is(err, ErrMailboxNotFound) {
		t.Errorf("GetMailbox() error = %v, want %v", err, ErrMailboxNotFound)
	}
}

func TestDynamoDBRepository_GetAllMailboxes(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	mock := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]types.AttributeValue{
					{
						"mailboxId":    &types.AttributeValueMemberS{Value: "inbox"},
						"accountId":    &types.AttributeValueMemberS{Value: "user-123"},
						"name":         &types.AttributeValueMemberS{Value: "Inbox"},
						"role":         &types.AttributeValueMemberS{Value: "inbox"},
						"sortOrder":    &types.AttributeValueMemberN{Value: "0"},
						"totalEmails":  &types.AttributeValueMemberN{Value: "10"},
						"unreadEmails": &types.AttributeValueMemberN{Value: "3"},
						"isSubscribed": &types.AttributeValueMemberBOOL{Value: true},
						"createdAt":    &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
						"updatedAt":    &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
					},
					{
						"mailboxId":    &types.AttributeValueMemberS{Value: "sent"},
						"accountId":    &types.AttributeValueMemberS{Value: "user-123"},
						"name":         &types.AttributeValueMemberS{Value: "Sent"},
						"role":         &types.AttributeValueMemberS{Value: "sent"},
						"sortOrder":    &types.AttributeValueMemberN{Value: "1"},
						"totalEmails":  &types.AttributeValueMemberN{Value: "5"},
						"unreadEmails": &types.AttributeValueMemberN{Value: "0"},
						"isSubscribed": &types.AttributeValueMemberBOOL{Value: true},
						"createdAt":    &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
						"updatedAt":    &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
					},
				},
			}, nil
		},
	}

	repo := NewDynamoDBRepository(mock, "test-table")
	mailboxes, err := repo.GetAllMailboxes(ctx, "user-123")

	if err != nil {
		t.Fatalf("GetAllMailboxes() error = %v", err)
	}
	if len(mailboxes) != 2 {
		t.Fatalf("GetAllMailboxes() returned %d mailboxes, want 2", len(mailboxes))
	}
	if mailboxes[0].MailboxID != "inbox" {
		t.Errorf("mailboxes[0].MailboxID = %q, want %q", mailboxes[0].MailboxID, "inbox")
	}
	if mailboxes[1].MailboxID != "sent" {
		t.Errorf("mailboxes[1].MailboxID = %q, want %q", mailboxes[1].MailboxID, "sent")
	}
}

func TestDynamoDBRepository_CreateMailbox(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	var capturedItem map[string]types.AttributeValue
	mock := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			// No existing mailboxes
			return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{}}, nil
		},
		putItemFunc: func(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			capturedItem = input.Item
			return &dynamodb.PutItemOutput{}, nil
		},
	}

	repo := NewDynamoDBRepository(mock, "test-table")
	mailbox := &MailboxItem{
		AccountID:    "user-123",
		MailboxID:    "inbox",
		Name:         "Inbox",
		Role:         "inbox",
		SortOrder:    0,
		TotalEmails:  0,
		UnreadEmails: 0,
		IsSubscribed: true,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	err := repo.CreateMailbox(ctx, mailbox)

	if err != nil {
		t.Fatalf("CreateMailbox() error = %v", err)
	}
	if capturedItem == nil {
		t.Fatal("PutItem was not called")
	}
	if pk, ok := capturedItem["pk"].(*types.AttributeValueMemberS); !ok || pk.Value != "ACCOUNT#user-123" {
		t.Errorf("pk = %v, want ACCOUNT#user-123", capturedItem["pk"])
	}
}

func TestDynamoDBRepository_CreateMailbox_DuplicateRole(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	mock := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			// Return existing mailbox with inbox role
			return &dynamodb.QueryOutput{
				Items: []map[string]types.AttributeValue{
					{
						"mailboxId":    &types.AttributeValueMemberS{Value: "existing-inbox"},
						"accountId":    &types.AttributeValueMemberS{Value: "user-123"},
						"name":         &types.AttributeValueMemberS{Value: "Inbox"},
						"role":         &types.AttributeValueMemberS{Value: "inbox"},
						"sortOrder":    &types.AttributeValueMemberN{Value: "0"},
						"totalEmails":  &types.AttributeValueMemberN{Value: "0"},
						"unreadEmails": &types.AttributeValueMemberN{Value: "0"},
						"isSubscribed": &types.AttributeValueMemberBOOL{Value: true},
						"createdAt":    &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
						"updatedAt":    &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
					},
				},
			}, nil
		},
	}

	repo := NewDynamoDBRepository(mock, "test-table")
	mailbox := &MailboxItem{
		AccountID:    "user-123",
		MailboxID:    "new-inbox",
		Name:         "Another Inbox",
		Role:         "inbox", // duplicate role
		SortOrder:    0,
		TotalEmails:  0,
		UnreadEmails: 0,
		IsSubscribed: true,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	err := repo.CreateMailbox(ctx, mailbox)

	if !errors.Is(err, ErrRoleAlreadyExists) {
		t.Errorf("CreateMailbox() error = %v, want %v", err, ErrRoleAlreadyExists)
	}
}

func TestDynamoDBRepository_MailboxExists(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		item     map[string]types.AttributeValue
		expected bool
	}{
		{
			name:     "mailbox exists",
			item:     map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"}},
			expected: true,
		},
		{
			name:     "mailbox does not exist",
			item:     nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockDynamoDBClient{
				getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
					return &dynamodb.GetItemOutput{Item: tt.item}, nil
				},
			}

			repo := NewDynamoDBRepository(mock, "test-table")
			exists, err := repo.MailboxExists(ctx, "user-123", "inbox")

			if err != nil {
				t.Fatalf("MailboxExists() error = %v", err)
			}
			if exists != tt.expected {
				t.Errorf("MailboxExists() = %v, want %v", exists, tt.expected)
			}
		})
	}
}

func TestDynamoDBRepository_IncrementCounts(t *testing.T) {
	ctx := context.Background()

	var capturedUpdateExpr string
	mock := &mockDynamoDBClient{
		updateItemFunc: func(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
			capturedUpdateExpr = *input.UpdateExpression
			return &dynamodb.UpdateItemOutput{}, nil
		},
	}

	repo := NewDynamoDBRepository(mock, "test-table")

	t.Run("increment total only", func(t *testing.T) {
		err := repo.IncrementCounts(ctx, "user-123", "inbox", false)
		if err != nil {
			t.Fatalf("IncrementCounts() error = %v", err)
		}
		if capturedUpdateExpr == "" {
			t.Error("UpdateItem was not called")
		}
	})

	t.Run("increment total and unread", func(t *testing.T) {
		err := repo.IncrementCounts(ctx, "user-123", "inbox", true)
		if err != nil {
			t.Fatalf("IncrementCounts() error = %v", err)
		}
	})
}

func TestDynamoDBRepository_DeleteMailbox(t *testing.T) {
	ctx := context.Background()

	mock := &mockDynamoDBClient{
		deleteItemFunc: func(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
			return &dynamodb.DeleteItemOutput{}, nil
		},
	}

	repo := NewDynamoDBRepository(mock, "test-table")
	err := repo.DeleteMailbox(ctx, "user-123", "inbox")

	if err != nil {
		t.Errorf("DeleteMailbox() error = %v", err)
	}
}

func TestDynamoDBRepository_DeleteMailbox_NotFound(t *testing.T) {
	ctx := context.Background()

	mock := &mockDynamoDBClient{
		deleteItemFunc: func(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
			return nil, &types.ConditionalCheckFailedException{}
		},
	}

	repo := NewDynamoDBRepository(mock, "test-table")
	err := repo.DeleteMailbox(ctx, "user-123", "nonexistent")

	if !errors.Is(err, ErrMailboxNotFound) {
		t.Errorf("DeleteMailbox() error = %v, want %v", err, ErrMailboxNotFound)
	}
}

func TestDynamoDBRepository_UpdateMailbox(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	var capturedUpdateExpr string
	mock := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			// No existing mailboxes with same role
			return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{}}, nil
		},
		updateItemFunc: func(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
			capturedUpdateExpr = *input.UpdateExpression
			return &dynamodb.UpdateItemOutput{}, nil
		},
	}

	repo := NewDynamoDBRepository(mock, "test-table")
	mailbox := &MailboxItem{
		AccountID:    "user-123",
		MailboxID:    "inbox",
		Name:         "Updated Inbox",
		Role:         "inbox",
		SortOrder:    1,
		IsSubscribed: false,
		UpdatedAt:    now,
	}

	err := repo.UpdateMailbox(ctx, mailbox)

	if err != nil {
		t.Fatalf("UpdateMailbox() error = %v", err)
	}
	if capturedUpdateExpr == "" {
		t.Error("UpdateItem was not called")
	}
}

func TestDynamoDBRepository_UpdateMailbox_NotFound(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	mock := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{}}, nil
		},
		updateItemFunc: func(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
			return nil, &types.ConditionalCheckFailedException{}
		},
	}

	repo := NewDynamoDBRepository(mock, "test-table")
	mailbox := &MailboxItem{
		AccountID: "user-123",
		MailboxID: "nonexistent",
		Name:      "Test",
		UpdatedAt: now,
	}

	err := repo.UpdateMailbox(ctx, mailbox)

	if !errors.Is(err, ErrMailboxNotFound) {
		t.Errorf("UpdateMailbox() error = %v, want %v", err, ErrMailboxNotFound)
	}
}

func TestDynamoDBRepository_DecrementCounts(t *testing.T) {
	ctx := context.Background()

	var capturedUpdateExpr string
	mock := &mockDynamoDBClient{
		updateItemFunc: func(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
			capturedUpdateExpr = *input.UpdateExpression
			return &dynamodb.UpdateItemOutput{}, nil
		},
	}

	repo := NewDynamoDBRepository(mock, "test-table")

	t.Run("decrement total only", func(t *testing.T) {
		err := repo.DecrementCounts(ctx, "user-123", "inbox", false)
		if err != nil {
			t.Fatalf("DecrementCounts() error = %v", err)
		}
		if capturedUpdateExpr == "" {
			t.Error("UpdateItem was not called")
		}
	})

	t.Run("decrement total and unread", func(t *testing.T) {
		err := repo.DecrementCounts(ctx, "user-123", "inbox", true)
		if err != nil {
			t.Fatalf("DecrementCounts() error = %v", err)
		}
	})
}

func TestDynamoDBRepository_BuildDecrementCountsItems_TotalOnly(t *testing.T) {
	repo := NewDynamoDBRepository(&mockDynamoDBClient{}, "test-table")

	item := repo.BuildDecrementCountsItems("user-123", "inbox-id", false)

	if item.Update == nil {
		t.Fatal("Expected Update item")
	}
	if *item.Update.TableName != "test-table" {
		t.Errorf("TableName = %q, want %q", *item.Update.TableName, "test-table")
	}

	pk := item.Update.Key["pk"].(*types.AttributeValueMemberS).Value
	sk := item.Update.Key["sk"].(*types.AttributeValueMemberS).Value
	if pk != "ACCOUNT#user-123" {
		t.Errorf("pk = %q, want %q", pk, "ACCOUNT#user-123")
	}
	if sk != "MAILBOX#inbox-id" {
		t.Errorf("sk = %q, want %q", sk, "MAILBOX#inbox-id")
	}

	// Should NOT contain unreadEmails in update expression
	expr := *item.Update.UpdateExpression
	if !contains(expr, "totalEmails") {
		t.Errorf("expression %q should contain totalEmails", expr)
	}
	if contains(expr, "unreadEmails") {
		t.Errorf("expression %q should NOT contain unreadEmails when decrementUnread=false", expr)
	}
}

func TestDynamoDBRepository_BuildDecrementCountsItems_WithUnread(t *testing.T) {
	repo := NewDynamoDBRepository(&mockDynamoDBClient{}, "test-table")

	item := repo.BuildDecrementCountsItems("user-123", "inbox-id", true)

	if item.Update == nil {
		t.Fatal("Expected Update item")
	}

	expr := *item.Update.UpdateExpression
	if !contains(expr, "totalEmails") {
		t.Errorf("expression %q should contain totalEmails", expr)
	}
	if !contains(expr, "unreadEmails") {
		t.Errorf("expression %q should contain unreadEmails when decrementUnread=true", expr)
	}
}

func TestDynamoDBRepository_BuildIncrementCountsItems_TotalOnly(t *testing.T) {
	repo := NewDynamoDBRepository(&mockDynamoDBClient{}, "test-table")

	item := repo.BuildIncrementCountsItems("user-123", "inbox-id", false)

	if item.Update == nil {
		t.Fatal("Expected Update item")
	}
	if *item.Update.TableName != "test-table" {
		t.Errorf("TableName = %q, want %q", *item.Update.TableName, "test-table")
	}

	pk := item.Update.Key["pk"].(*types.AttributeValueMemberS).Value
	sk := item.Update.Key["sk"].(*types.AttributeValueMemberS).Value
	if pk != "ACCOUNT#user-123" {
		t.Errorf("pk = %q, want %q", pk, "ACCOUNT#user-123")
	}
	if sk != "MAILBOX#inbox-id" {
		t.Errorf("sk = %q, want %q", sk, "MAILBOX#inbox-id")
	}

	// Should NOT contain unreadEmails in update expression
	expr := *item.Update.UpdateExpression
	if !contains(expr, "totalEmails") {
		t.Errorf("expression %q should contain totalEmails", expr)
	}
	if contains(expr, "unreadEmails") {
		t.Errorf("expression %q should NOT contain unreadEmails when incrementUnread=false", expr)
	}
	// Should contain + :one (not - :one)
	if !contains(expr, "+ :one") {
		t.Errorf("expression %q should contain '+ :one' for increment", expr)
	}
	if contains(expr, "- :one") {
		t.Errorf("expression %q should NOT contain '- :one' for increment", expr)
	}
}

func TestDynamoDBRepository_BuildIncrementCountsItems_WithUnread(t *testing.T) {
	repo := NewDynamoDBRepository(&mockDynamoDBClient{}, "test-table")

	item := repo.BuildIncrementCountsItems("user-123", "inbox-id", true)

	if item.Update == nil {
		t.Fatal("Expected Update item")
	}

	expr := *item.Update.UpdateExpression
	if !contains(expr, "totalEmails") {
		t.Errorf("expression %q should contain totalEmails", expr)
	}
	if !contains(expr, "unreadEmails") {
		t.Errorf("expression %q should contain unreadEmails when incrementUnread=true", expr)
	}
	// Should contain + :one (not - :one)
	if !contains(expr, "+ :one") {
		t.Errorf("expression %q should contain '+ :one' for increment", expr)
	}
	if contains(expr, "- :one") {
		t.Errorf("expression %q should NOT contain '- :one' for increment", expr)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestDynamoDBRepository_BuildCreateMailboxItem(t *testing.T) {
	repo := NewDynamoDBRepository(nil, "test-table")
	now := time.Now().UTC()

	mailbox := &MailboxItem{
		AccountID:    "user-123",
		MailboxID:    "inbox-id",
		Name:         "Inbox",
		Role:         "inbox",
		SortOrder:    0,
		TotalEmails:  0,
		UnreadEmails: 0,
		IsSubscribed: true,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	item := repo.BuildCreateMailboxItem(mailbox)

	// Verify it's a Put operation
	if item.Put == nil {
		t.Fatal("Expected Put item")
	}

	// Verify table name
	if *item.Put.TableName != "test-table" {
		t.Errorf("TableName = %q, want %q", *item.Put.TableName, "test-table")
	}

	// Verify keys are in the item
	pk := item.Put.Item["pk"].(*types.AttributeValueMemberS).Value
	sk := item.Put.Item["sk"].(*types.AttributeValueMemberS).Value
	if pk != "ACCOUNT#user-123" {
		t.Errorf("pk = %q, want %q", pk, "ACCOUNT#user-123")
	}
	if sk != "MAILBOX#inbox-id" {
		t.Errorf("sk = %q, want %q", sk, "MAILBOX#inbox-id")
	}

	// Verify condition expression (attribute_not_exists)
	if item.Put.ConditionExpression == nil {
		t.Fatal("Expected ConditionExpression")
	}
	if *item.Put.ConditionExpression != "attribute_not_exists(pk)" {
		t.Errorf("ConditionExpression = %q, want %q", *item.Put.ConditionExpression, "attribute_not_exists(pk)")
	}

	// Verify mailbox data is marshaled
	if name, ok := item.Put.Item["name"].(*types.AttributeValueMemberS); !ok || name.Value != "Inbox" {
		t.Errorf("name attribute incorrect")
	}
	if role, ok := item.Put.Item["role"].(*types.AttributeValueMemberS); !ok || role.Value != "inbox" {
		t.Errorf("role attribute incorrect")
	}
}

func TestDynamoDBRepository_BuildUpdateMailboxItem(t *testing.T) {
	repo := NewDynamoDBRepository(nil, "test-table")
	now := time.Now().UTC()

	mailbox := &MailboxItem{
		AccountID:    "user-123",
		MailboxID:    "inbox-id",
		Name:         "Updated Inbox",
		Role:         "inbox",
		SortOrder:    5,
		IsSubscribed: false,
		UpdatedAt:    now,
	}

	item := repo.BuildUpdateMailboxItem(mailbox)

	// Verify it's an Update operation
	if item.Update == nil {
		t.Fatal("Expected Update item")
	}

	// Verify table name
	if *item.Update.TableName != "test-table" {
		t.Errorf("TableName = %q, want %q", *item.Update.TableName, "test-table")
	}

	// Verify keys
	pk := item.Update.Key["pk"].(*types.AttributeValueMemberS).Value
	sk := item.Update.Key["sk"].(*types.AttributeValueMemberS).Value
	if pk != "ACCOUNT#user-123" {
		t.Errorf("pk = %q, want %q", pk, "ACCOUNT#user-123")
	}
	if sk != "MAILBOX#inbox-id" {
		t.Errorf("sk = %q, want %q", sk, "MAILBOX#inbox-id")
	}

	// Verify update expression exists
	if item.Update.UpdateExpression == nil {
		t.Fatal("Expected UpdateExpression")
	}

	// Verify condition expression (attribute_exists)
	if item.Update.ConditionExpression == nil {
		t.Fatal("Expected ConditionExpression")
	}
	if *item.Update.ConditionExpression != "attribute_exists(pk)" {
		t.Errorf("ConditionExpression = %q, want %q", *item.Update.ConditionExpression, "attribute_exists(pk)")
	}

	// Verify expression attribute names and values exist
	if item.Update.ExpressionAttributeNames == nil {
		t.Fatal("Expected ExpressionAttributeNames")
	}
	if item.Update.ExpressionAttributeValues == nil {
		t.Fatal("Expected ExpressionAttributeValues")
	}

	// Verify name is in the values
	if nameVal, ok := item.Update.ExpressionAttributeValues[":name"].(*types.AttributeValueMemberS); !ok || nameVal.Value != "Updated Inbox" {
		t.Errorf("name value incorrect")
	}
}

func TestDynamoDBRepository_BuildUpdateMailboxItem_NoRole(t *testing.T) {
	repo := NewDynamoDBRepository(nil, "test-table")
	now := time.Now().UTC()

	mailbox := &MailboxItem{
		AccountID:    "user-123",
		MailboxID:    "custom-id",
		Name:         "Custom Folder",
		Role:         "", // No role
		SortOrder:    10,
		IsSubscribed: true,
		UpdatedAt:    now,
	}

	item := repo.BuildUpdateMailboxItem(mailbox)

	// Verify it's an Update operation
	if item.Update == nil {
		t.Fatal("Expected Update item")
	}

	// Verify update expression contains REMOVE for role
	expr := *item.Update.UpdateExpression
	if !contains(expr, "REMOVE") {
		t.Errorf("UpdateExpression should contain REMOVE when role is empty: %s", expr)
	}
}

func TestDynamoDBRepository_BuildDeleteMailboxItem(t *testing.T) {
	repo := NewDynamoDBRepository(nil, "test-table")

	item := repo.BuildDeleteMailboxItem("user-123", "inbox-id")

	// Verify it's a Delete operation
	if item.Delete == nil {
		t.Fatal("Expected Delete item")
	}

	// Verify table name
	if *item.Delete.TableName != "test-table" {
		t.Errorf("TableName = %q, want %q", *item.Delete.TableName, "test-table")
	}

	// Verify keys
	pk := item.Delete.Key["pk"].(*types.AttributeValueMemberS).Value
	sk := item.Delete.Key["sk"].(*types.AttributeValueMemberS).Value
	if pk != "ACCOUNT#user-123" {
		t.Errorf("pk = %q, want %q", pk, "ACCOUNT#user-123")
	}
	if sk != "MAILBOX#inbox-id" {
		t.Errorf("sk = %q, want %q", sk, "MAILBOX#inbox-id")
	}

	// Verify condition expression (attribute_exists)
	if item.Delete.ConditionExpression == nil {
		t.Fatal("Expected ConditionExpression")
	}
	if *item.Delete.ConditionExpression != "attribute_exists(pk)" {
		t.Errorf("ConditionExpression = %q, want %q", *item.Delete.ConditionExpression, "attribute_exists(pk)")
	}
}
