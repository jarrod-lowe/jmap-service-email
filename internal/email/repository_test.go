package email

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// mockDynamoDBClient implements the DynamoDBClient interface for testing.
type mockDynamoDBClient struct {
	transactWriteFunc func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	getItemFunc       func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	queryFunc         func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

func (m *mockDynamoDBClient) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if m.transactWriteFunc != nil {
		return m.transactWriteFunc(ctx, input, opts...)
	}
	return &dynamodb.TransactWriteItemsOutput{}, nil
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

func TestRepository_CreateEmail(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	email := &EmailItem{
		AccountID: "user-123",
		EmailID:   "email-456",
		BlobID:    "blob-789",
		ThreadID:  "email-456",
		MailboxIDs: map[string]bool{
			"inbox-id":    true,
			"projects-id": true,
		},
		Keywords: map[string]bool{
			"$seen": true,
		},
		ReceivedAt:    receivedAt,
		Size:          2048,
		HasAttachment: false,
		Subject:       "Test Subject",
		From: []EmailAddress{
			{Name: "Alice", Email: "alice@example.com"},
		},
		To: []EmailAddress{
			{Name: "Bob", Email: "bob@example.com"},
		},
	}

	var capturedInput *dynamodb.TransactWriteItemsInput
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedInput = input
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	err := repo.CreateEmail(context.Background(), email)
	if err != nil {
		t.Fatalf("CreateEmail failed: %v", err)
	}

	// Should have 1 email item + 2 mailbox membership items = 3 items
	if capturedInput == nil {
		t.Fatal("TransactWriteItems was not called")
	}
	if len(capturedInput.TransactItems) != 3 {
		t.Errorf("TransactItems count = %d, want 3", len(capturedInput.TransactItems))
	}

	// Verify email item
	emailPut := capturedInput.TransactItems[0].Put
	if emailPut == nil {
		t.Fatal("First item should be a Put for email")
	}
	if *emailPut.TableName != "test-table" {
		t.Errorf("TableName = %q, want %q", *emailPut.TableName, "test-table")
	}

	// Verify PK/SK in email item
	pk, ok := emailPut.Item["pk"]
	if !ok {
		t.Fatal("Email item missing pk")
	}
	pkVal := pk.(*types.AttributeValueMemberS).Value
	if pkVal != "ACCOUNT#user-123" {
		t.Errorf("pk = %q, want %q", pkVal, "ACCOUNT#user-123")
	}

	sk, ok := emailPut.Item["sk"]
	if !ok {
		t.Fatal("Email item missing sk")
	}
	skVal := sk.(*types.AttributeValueMemberS).Value
	if skVal != "EMAIL#email-456" {
		t.Errorf("sk = %q, want %q", skVal, "EMAIL#email-456")
	}
}

func TestRepository_CreateEmail_TransactionError(t *testing.T) {
	email := &EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		MailboxIDs: map[string]bool{"inbox": true},
		ReceivedAt: time.Now(),
	}

	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			return nil, errors.New("transaction failed")
		},
	}

	repo := NewRepository(mockClient, "test-table")
	err := repo.CreateEmail(context.Background(), email)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if !errors.Is(err, ErrTransactionFailed) {
		t.Errorf("Expected ErrTransactionFailed, got %v", err)
	}
}

func TestRepository_GetEmail(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	mockClient := &mockDynamoDBClient{
		getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			// Verify the query is correct
			pk := input.Key["pk"].(*types.AttributeValueMemberS).Value
			sk := input.Key["sk"].(*types.AttributeValueMemberS).Value
			if pk != "ACCOUNT#user-123" {
				t.Errorf("pk = %q, want %q", pk, "ACCOUNT#user-123")
			}
			if sk != "EMAIL#email-456" {
				t.Errorf("sk = %q, want %q", sk, "EMAIL#email-456")
			}

			return &dynamodb.GetItemOutput{
				Item: map[string]types.AttributeValue{
					"pk":         &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
					"sk":         &types.AttributeValueMemberS{Value: "EMAIL#email-456"},
					"emailId":    &types.AttributeValueMemberS{Value: "email-456"},
					"accountId":  &types.AttributeValueMemberS{Value: "user-123"},
					"blobId":     &types.AttributeValueMemberS{Value: "blob-789"},
					"threadId":   &types.AttributeValueMemberS{Value: "email-456"},
					"subject":    &types.AttributeValueMemberS{Value: "Test Subject"},
					"receivedAt": &types.AttributeValueMemberS{Value: receivedAt.Format(time.RFC3339)},
					"size":       &types.AttributeValueMemberN{Value: "2048"},
					"mailboxIds": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
						"inbox-id": &types.AttributeValueMemberBOOL{Value: true},
					}},
					"keywords": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
						"$seen": &types.AttributeValueMemberBOOL{Value: true},
					}},
				},
			}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	email, err := repo.GetEmail(context.Background(), "user-123", "email-456")
	if err != nil {
		t.Fatalf("GetEmail failed: %v", err)
	}

	if email.EmailID != "email-456" {
		t.Errorf("EmailID = %q, want %q", email.EmailID, "email-456")
	}
	if email.Subject != "Test Subject" {
		t.Errorf("Subject = %q, want %q", email.Subject, "Test Subject")
	}
	if email.Size != 2048 {
		t.Errorf("Size = %d, want %d", email.Size, 2048)
	}
}

func TestRepository_GetEmail_NotFound(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: nil, // No item found
			}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	_, err := repo.GetEmail(context.Background(), "user-123", "nonexistent")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if !errors.Is(err, ErrEmailNotFound) {
		t.Errorf("Expected ErrEmailNotFound, got %v", err)
	}
}

func TestMarshalEmailItem_IncludesTextBody(t *testing.T) {
	email := &EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		ReceivedAt: time.Now(),
		TextBody:   []string{"1", "2"},
	}

	var capturedItem map[string]types.AttributeValue
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedItem = input.TransactItems[0].Put.Item
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	repo := NewRepository(mockClient, "test-table")
	_ = repo.CreateEmail(context.Background(), email)

	if _, ok := capturedItem["textBody"]; !ok {
		t.Error("marshalEmailItem missing textBody field")
	}
}

func TestMarshalEmailItem_IncludesHTMLBody(t *testing.T) {
	email := &EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		ReceivedAt: time.Now(),
		HTMLBody:   []string{"3", "4"},
	}

	var capturedItem map[string]types.AttributeValue
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedItem = input.TransactItems[0].Put.Item
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	repo := NewRepository(mockClient, "test-table")
	_ = repo.CreateEmail(context.Background(), email)

	if _, ok := capturedItem["htmlBody"]; !ok {
		t.Error("marshalEmailItem missing htmlBody field")
	}
}

func TestMarshalEmailItem_IncludesAttachments(t *testing.T) {
	email := &EmailItem{
		AccountID:   "user-123",
		EmailID:     "email-456",
		ReceivedAt:  time.Now(),
		Attachments: []string{"5", "6"},
	}

	var capturedItem map[string]types.AttributeValue
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedItem = input.TransactItems[0].Put.Item
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	repo := NewRepository(mockClient, "test-table")
	_ = repo.CreateEmail(context.Background(), email)

	if _, ok := capturedItem["attachments"]; !ok {
		t.Error("marshalEmailItem missing attachments field")
	}
}

func TestMarshalEmailItem_IncludesLSI1SK(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 30, 45, 0, time.UTC)
	email := &EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		ReceivedAt: receivedAt,
	}

	var capturedItem map[string]types.AttributeValue
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedItem = input.TransactItems[0].Put.Item
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	repo := NewRepository(mockClient, "test-table")
	_ = repo.CreateEmail(context.Background(), email)

	lsi1sk, ok := capturedItem["lsi1sk"]
	if !ok {
		t.Fatal("marshalEmailItem missing lsi1sk field")
	}
	lsiVal := lsi1sk.(*types.AttributeValueMemberS).Value
	expectedLSI := "RCVD#2024-01-20T10:30:45Z#email-456"
	if lsiVal != expectedLSI {
		t.Errorf("lsi1sk = %q, want %q", lsiVal, expectedLSI)
	}
}

func TestMarshalEmailItem_IncludesBodyStructure(t *testing.T) {
	email := &EmailItem{
		AccountID:     "user-123",
		EmailID:       "email-456",
		ReceivedAt:    time.Now(),
		BodyStructure: BodyPart{PartID: "1", Type: "text/plain"},
	}

	var capturedItem map[string]types.AttributeValue
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedItem = input.TransactItems[0].Put.Item
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	repo := NewRepository(mockClient, "test-table")
	_ = repo.CreateEmail(context.Background(), email)

	if _, ok := capturedItem["bodyStructure"]; !ok {
		t.Error("marshalEmailItem missing bodyStructure field")
	}
}

func TestMarshalUnmarshal_FromField_RoundTrip(t *testing.T) {
	original := &EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		ReceivedAt: time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC),
		From: []EmailAddress{
			{Name: "Test Sender", Email: "sender@example.com"},
		},
		To: []EmailAddress{
			{Name: "Test Recipient", Email: "recipient@example.com"},
		},
	}

	var capturedItem map[string]types.AttributeValue
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedItem = input.TransactItems[0].Put.Item
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	repo := NewRepository(mockClient, "test-table")
	_ = repo.CreateEmail(context.Background(), original)

	// Verify "from" was marshaled
	if _, ok := capturedItem["from"]; !ok {
		t.Fatal("From field not marshaled to DynamoDB")
	}

	// Now unmarshal and verify
	mockClient.getItemFunc = func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
		return &dynamodb.GetItemOutput{Item: capturedItem}, nil
	}

	retrieved, err := repo.GetEmail(context.Background(), "user-123", "email-456")
	if err != nil {
		t.Fatalf("GetEmail failed: %v", err)
	}

	// Verify From
	if len(retrieved.From) != 1 {
		t.Fatalf("From length = %d, want 1", len(retrieved.From))
	}
	if retrieved.From[0].Name != "Test Sender" {
		t.Errorf("From[0].Name = %q, want %q", retrieved.From[0].Name, "Test Sender")
	}
	if retrieved.From[0].Email != "sender@example.com" {
		t.Errorf("From[0].Email = %q, want %q", retrieved.From[0].Email, "sender@example.com")
	}

	// Verify To for comparison
	if len(retrieved.To) != 1 {
		t.Fatalf("To length = %d, want 1", len(retrieved.To))
	}
}

func TestMarshalUnmarshal_RoundTrip(t *testing.T) {
	original := &EmailItem{
		AccountID:     "user-123",
		EmailID:       "email-456",
		BlobID:        "blob-789",
		ThreadID:      "thread-123",
		ReceivedAt:    time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC),
		Size:          1024,
		TextBody:      []string{"1", "2"},
		HTMLBody:      []string{"3"},
		Attachments:   []string{"4", "5"},
		BodyStructure: BodyPart{PartID: "1", Type: "multipart/mixed", Size: 1024},
	}

	var capturedItem map[string]types.AttributeValue
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedItem = input.TransactItems[0].Put.Item
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	repo := NewRepository(mockClient, "test-table")
	_ = repo.CreateEmail(context.Background(), original)

	// Now unmarshal and verify
	mockClient.getItemFunc = func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
		return &dynamodb.GetItemOutput{Item: capturedItem}, nil
	}

	retrieved, err := repo.GetEmail(context.Background(), "user-123", "email-456")
	if err != nil {
		t.Fatalf("GetEmail failed: %v", err)
	}

	// Verify TextBody
	if len(retrieved.TextBody) != 2 || retrieved.TextBody[0] != "1" || retrieved.TextBody[1] != "2" {
		t.Errorf("TextBody = %v, want [1 2]", retrieved.TextBody)
	}

	// Verify HTMLBody
	if len(retrieved.HTMLBody) != 1 || retrieved.HTMLBody[0] != "3" {
		t.Errorf("HTMLBody = %v, want [3]", retrieved.HTMLBody)
	}

	// Verify Attachments
	if len(retrieved.Attachments) != 2 || retrieved.Attachments[0] != "4" || retrieved.Attachments[1] != "5" {
		t.Errorf("Attachments = %v, want [4 5]", retrieved.Attachments)
	}

	// Verify BodyStructure
	if retrieved.BodyStructure.PartID != "1" || retrieved.BodyStructure.Type != "multipart/mixed" {
		t.Errorf("BodyStructure = %+v, want PartID=1, Type=multipart/mixed", retrieved.BodyStructure)
	}
}

func TestRepository_QueryEmails_InMailbox(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			// Verify the query is correct
			if *input.TableName != "test-table" {
				t.Errorf("TableName = %q, want %q", *input.TableName, "test-table")
			}

			// Should query with SK begins_with MBOX#{mailboxId}#EMAIL#
			keyCondExpr := *input.KeyConditionExpression
			if keyCondExpr != "pk = :pk AND begins_with(sk, :skPrefix)" {
				t.Errorf("KeyConditionExpression = %q", keyCondExpr)
			}

			// Return mock results
			return &dynamodb.QueryOutput{
				Items: []map[string]types.AttributeValue{
					{
						"pk":      &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
						"sk":      &types.AttributeValueMemberS{Value: "MBOX#inbox-id#EMAIL#2024-01-20T10:00:00Z#email-1"},
						"emailId": &types.AttributeValueMemberS{Value: "email-1"},
					},
					{
						"pk":      &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
						"sk":      &types.AttributeValueMemberS{Value: "MBOX#inbox-id#EMAIL#2024-01-20T11:00:00Z#email-2"},
						"emailId": &types.AttributeValueMemberS{Value: "email-2"},
					},
				},
			}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	result, err := repo.QueryEmails(context.Background(), "user-123", &QueryRequest{
		Filter: &QueryFilter{InMailbox: "inbox-id"},
		Limit:  25,
	})
	if err != nil {
		t.Fatalf("QueryEmails failed: %v", err)
	}

	if len(result.IDs) != 2 {
		t.Fatalf("IDs length = %d, want 2", len(result.IDs))
	}
	if result.IDs[0] != "email-1" {
		t.Errorf("IDs[0] = %q, want %q", result.IDs[0], "email-1")
	}
	if result.IDs[1] != "email-2" {
		t.Errorf("IDs[1] = %q, want %q", result.IDs[1], "email-2")
	}
}

func TestRepository_QueryEmails_NoFilter_UsesLSI(t *testing.T) {
	var capturedInput *dynamodb.QueryInput
	mockClient := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			capturedInput = input
			// Mock returns emailId attribute because LSI uses INCLUDE projection
			// with non_key_attributes = ["emailId"] in dynamodb.tf
			return &dynamodb.QueryOutput{
				Items: []map[string]types.AttributeValue{
					{
						"pk":      &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
						"lsi1sk":  &types.AttributeValueMemberS{Value: "RCVD#2024-01-20T11:00:00Z#email-2"},
						"emailId": &types.AttributeValueMemberS{Value: "email-2"},
					},
					{
						"pk":      &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
						"lsi1sk":  &types.AttributeValueMemberS{Value: "RCVD#2024-01-20T10:00:00Z#email-1"},
						"emailId": &types.AttributeValueMemberS{Value: "email-1"},
					},
				},
			}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	result, err := repo.QueryEmails(context.Background(), "user-123", &QueryRequest{
		Filter: nil, // No filter
		Limit:  25,
	})
	if err != nil {
		t.Fatalf("QueryEmails failed: %v", err)
	}

	// Verify LSI was used
	if capturedInput.IndexName == nil || *capturedInput.IndexName != "lsi1" {
		t.Errorf("Expected IndexName = lsi1, got %v", capturedInput.IndexName)
	}

	if len(result.IDs) != 2 {
		t.Fatalf("IDs length = %d, want 2", len(result.IDs))
	}
}

func TestRepository_QueryEmails_PositionPagination(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			// Simulate 5 items being returned
			return &dynamodb.QueryOutput{
				Items: []map[string]types.AttributeValue{
					{"emailId": &types.AttributeValueMemberS{Value: "email-1"}},
					{"emailId": &types.AttributeValueMemberS{Value: "email-2"}},
					{"emailId": &types.AttributeValueMemberS{Value: "email-3"}},
					{"emailId": &types.AttributeValueMemberS{Value: "email-4"}},
					{"emailId": &types.AttributeValueMemberS{Value: "email-5"}},
				},
			}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	result, err := repo.QueryEmails(context.Background(), "user-123", &QueryRequest{
		Filter:   &QueryFilter{InMailbox: "inbox"},
		Position: 2, // Skip first 2
		Limit:    2, // Take 2
	})
	if err != nil {
		t.Fatalf("QueryEmails failed: %v", err)
	}

	if len(result.IDs) != 2 {
		t.Fatalf("IDs length = %d, want 2", len(result.IDs))
	}
	if result.IDs[0] != "email-3" {
		t.Errorf("IDs[0] = %q, want %q", result.IDs[0], "email-3")
	}
	if result.IDs[1] != "email-4" {
		t.Errorf("IDs[1] = %q, want %q", result.IDs[1], "email-4")
	}
	if result.Position != 2 {
		t.Errorf("Position = %d, want 2", result.Position)
	}
}

func TestRepository_QueryEmails_SortAscending(t *testing.T) {
	var capturedInput *dynamodb.QueryInput
	mockClient := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			capturedInput = input
			return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{}}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	_, _ = repo.QueryEmails(context.Background(), "user-123", &QueryRequest{
		Filter: &QueryFilter{InMailbox: "inbox"},
		Sort:   []Comparator{{Property: "receivedAt", IsAscending: true}},
	})

	if capturedInput.ScanIndexForward == nil || !*capturedInput.ScanIndexForward {
		t.Error("Expected ScanIndexForward = true for ascending sort")
	}
}

func TestRepository_QueryEmails_SortDescending(t *testing.T) {
	var capturedInput *dynamodb.QueryInput
	mockClient := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			capturedInput = input
			return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{}}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	_, _ = repo.QueryEmails(context.Background(), "user-123", &QueryRequest{
		Filter: &QueryFilter{InMailbox: "inbox"},
		Sort:   []Comparator{{Property: "receivedAt", IsAscending: false}},
	})

	if capturedInput.ScanIndexForward == nil || *capturedInput.ScanIndexForward {
		t.Error("Expected ScanIndexForward = false for descending sort")
	}
}

func TestRepository_QueryEmails_DefaultLimit(t *testing.T) {
	var capturedInput *dynamodb.QueryInput
	mockClient := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			capturedInput = input
			return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{}}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	_, _ = repo.QueryEmails(context.Background(), "user-123", &QueryRequest{
		Filter: &QueryFilter{InMailbox: "inbox"},
		// Limit not set, should default to 25
	})

	if capturedInput.Limit == nil || *capturedInput.Limit != 25 {
		t.Errorf("Expected Limit = 25, got %v", capturedInput.Limit)
	}
}

func TestRepository_FindByMessageID(t *testing.T) {
	var capturedInput *dynamodb.QueryInput
	mockClient := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			capturedInput = input
			return &dynamodb.QueryOutput{
				Items: []map[string]types.AttributeValue{
					{
						"pk":       &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
						"lsi2sk":   &types.AttributeValueMemberS{Value: "MSGID#<msg-123@example.com>"},
						"emailId":  &types.AttributeValueMemberS{Value: "email-456"},
						"threadId": &types.AttributeValueMemberS{Value: "thread-789"},
					},
				},
			}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	result, err := repo.FindByMessageID(context.Background(), "user-123", "<msg-123@example.com>")
	if err != nil {
		t.Fatalf("FindByMessageID failed: %v", err)
	}

	// Verify LSI2 was used
	if capturedInput.IndexName == nil || *capturedInput.IndexName != "lsi2" {
		t.Errorf("Expected IndexName = lsi2, got %v", capturedInput.IndexName)
	}

	// Verify key condition
	if *capturedInput.KeyConditionExpression != "pk = :pk AND lsi2sk = :lsi2sk" {
		t.Errorf("KeyConditionExpression = %q", *capturedInput.KeyConditionExpression)
	}

	// Verify result
	if result == nil {
		t.Fatal("Expected result, got nil")
	}
	if result.EmailID != "email-456" {
		t.Errorf("EmailID = %q, want %q", result.EmailID, "email-456")
	}
	if result.ThreadID != "thread-789" {
		t.Errorf("ThreadID = %q, want %q", result.ThreadID, "thread-789")
	}
}

func TestRepository_FindByMessageID_NotFound(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]types.AttributeValue{}, // No results
			}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	result, err := repo.FindByMessageID(context.Background(), "user-123", "<nonexistent@example.com>")
	if err != nil {
		t.Fatalf("FindByMessageID failed: %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil result for not found, got %+v", result)
	}
}

func TestRepository_FindByMessageID_QueryError(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return nil, errors.New("dynamodb error")
		},
	}

	repo := NewRepository(mockClient, "test-table")
	_, err := repo.FindByMessageID(context.Background(), "user-123", "<msg@example.com>")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
}

func TestRepository_FindByThreadID(t *testing.T) {
	var capturedInput *dynamodb.QueryInput
	mockClient := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			capturedInput = input
			return &dynamodb.QueryOutput{
				Items: []map[string]types.AttributeValue{
					{
						"pk":         &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
						"lsi3sk":     &types.AttributeValueMemberS{Value: "THREAD#thread-789#RCVD#2024-01-20T10:00:00Z#email-1"},
						"emailId":    &types.AttributeValueMemberS{Value: "email-1"},
						"threadId":   &types.AttributeValueMemberS{Value: "thread-789"},
						"receivedAt": &types.AttributeValueMemberS{Value: "2024-01-20T10:00:00Z"},
					},
					{
						"pk":         &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
						"lsi3sk":     &types.AttributeValueMemberS{Value: "THREAD#thread-789#RCVD#2024-01-20T11:00:00Z#email-2"},
						"emailId":    &types.AttributeValueMemberS{Value: "email-2"},
						"threadId":   &types.AttributeValueMemberS{Value: "thread-789"},
						"receivedAt": &types.AttributeValueMemberS{Value: "2024-01-20T11:00:00Z"},
					},
				},
			}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	results, err := repo.FindByThreadID(context.Background(), "user-123", "thread-789")
	if err != nil {
		t.Fatalf("FindByThreadID failed: %v", err)
	}

	// Verify LSI3 was used
	if capturedInput.IndexName == nil || *capturedInput.IndexName != "lsi3" {
		t.Errorf("Expected IndexName = lsi3, got %v", capturedInput.IndexName)
	}

	// Verify key condition uses begins_with for thread prefix
	if *capturedInput.KeyConditionExpression != "pk = :pk AND begins_with(lsi3sk, :threadPrefix)" {
		t.Errorf("KeyConditionExpression = %q", *capturedInput.KeyConditionExpression)
	}

	// Verify ascending sort order (oldest first)
	if capturedInput.ScanIndexForward == nil || !*capturedInput.ScanIndexForward {
		t.Error("Expected ScanIndexForward = true for ascending receivedAt sort")
	}

	// Verify results
	if len(results) != 2 {
		t.Fatalf("Results length = %d, want 2", len(results))
	}
	if results[0].EmailID != "email-1" {
		t.Errorf("results[0].EmailID = %q, want %q", results[0].EmailID, "email-1")
	}
	if results[1].EmailID != "email-2" {
		t.Errorf("results[1].EmailID = %q, want %q", results[1].EmailID, "email-2")
	}
}

func TestRepository_FindByThreadID_Empty(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]types.AttributeValue{}, // No results
			}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	results, err := repo.FindByThreadID(context.Background(), "user-123", "nonexistent-thread")
	if err != nil {
		t.Fatalf("FindByThreadID failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected empty results, got %d", len(results))
	}
}

func TestRepository_FindByThreadID_QueryError(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return nil, errors.New("dynamodb error")
		},
	}

	repo := NewRepository(mockClient, "test-table")
	_, err := repo.FindByThreadID(context.Background(), "user-123", "thread-123")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
}

func TestRepository_UpdateEmailMailboxes_AddMailbox(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	// Email currently in inbox, moving to inbox + archive
	existingEmail := map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
		"sk":         &types.AttributeValueMemberS{Value: "EMAIL#email-456"},
		"emailId":    &types.AttributeValueMemberS{Value: "email-456"},
		"accountId":  &types.AttributeValueMemberS{Value: "user-123"},
		"blobId":     &types.AttributeValueMemberS{Value: "blob-789"},
		"threadId":   &types.AttributeValueMemberS{Value: "email-456"},
		"subject":    &types.AttributeValueMemberS{Value: "Test Subject"},
		"receivedAt": &types.AttributeValueMemberS{Value: receivedAt.Format(time.RFC3339)},
		"size":       &types.AttributeValueMemberN{Value: "2048"},
		"mailboxIds": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"inbox-id": &types.AttributeValueMemberBOOL{Value: true},
		}},
	}

	var capturedTransaction *dynamodb.TransactWriteItemsInput
	mockClient := &mockDynamoDBClient{
		getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: existingEmail}, nil
		},
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedTransaction = input
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	newMailboxIDs := map[string]bool{"inbox-id": true, "archive-id": true}
	oldMailboxIDs, email, err := repo.UpdateEmailMailboxes(context.Background(), "user-123", "email-456", newMailboxIDs)

	if err != nil {
		t.Fatalf("UpdateEmailMailboxes failed: %v", err)
	}

	// Should return old mailboxIds
	if len(oldMailboxIDs) != 1 || !oldMailboxIDs["inbox-id"] {
		t.Errorf("oldMailboxIDs = %v, want map[inbox-id:true]", oldMailboxIDs)
	}

	// Should return email
	if email == nil || email.EmailID != "email-456" {
		t.Errorf("email = %+v, want email-456", email)
	}

	// Transaction should have: 1 email update + 1 new membership (archive-id added)
	if capturedTransaction == nil {
		t.Fatal("TransactWriteItems was not called")
	}
	if len(capturedTransaction.TransactItems) != 2 {
		t.Errorf("TransactItems count = %d, want 2 (email update + new membership)", len(capturedTransaction.TransactItems))
	}
}

func TestRepository_UpdateEmailMailboxes_RemoveMailbox(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	// Email currently in inbox + archive, removing from archive
	existingEmail := map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
		"sk":         &types.AttributeValueMemberS{Value: "EMAIL#email-456"},
		"emailId":    &types.AttributeValueMemberS{Value: "email-456"},
		"accountId":  &types.AttributeValueMemberS{Value: "user-123"},
		"blobId":     &types.AttributeValueMemberS{Value: "blob-789"},
		"threadId":   &types.AttributeValueMemberS{Value: "email-456"},
		"subject":    &types.AttributeValueMemberS{Value: "Test Subject"},
		"receivedAt": &types.AttributeValueMemberS{Value: receivedAt.Format(time.RFC3339)},
		"size":       &types.AttributeValueMemberN{Value: "2048"},
		"mailboxIds": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"inbox-id":   &types.AttributeValueMemberBOOL{Value: true},
			"archive-id": &types.AttributeValueMemberBOOL{Value: true},
		}},
	}

	var capturedTransaction *dynamodb.TransactWriteItemsInput
	mockClient := &mockDynamoDBClient{
		getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: existingEmail}, nil
		},
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedTransaction = input
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	newMailboxIDs := map[string]bool{"inbox-id": true} // archive-id removed
	oldMailboxIDs, email, err := repo.UpdateEmailMailboxes(context.Background(), "user-123", "email-456", newMailboxIDs)

	if err != nil {
		t.Fatalf("UpdateEmailMailboxes failed: %v", err)
	}

	// Should return old mailboxIds with both
	if len(oldMailboxIDs) != 2 {
		t.Errorf("oldMailboxIDs = %v, want map with inbox-id and archive-id", oldMailboxIDs)
	}

	// Should return email
	if email == nil || email.EmailID != "email-456" {
		t.Errorf("email = %+v, want email-456", email)
	}

	// Transaction should have: 1 email update + 1 delete membership (archive-id removed)
	if capturedTransaction == nil {
		t.Fatal("TransactWriteItems was not called")
	}
	if len(capturedTransaction.TransactItems) != 2 {
		t.Errorf("TransactItems count = %d, want 2 (email update + delete membership)", len(capturedTransaction.TransactItems))
	}

	// Verify delete operation exists
	hasDelete := false
	for _, item := range capturedTransaction.TransactItems {
		if item.Delete != nil {
			hasDelete = true
			break
		}
	}
	if !hasDelete {
		t.Error("Expected Delete operation in transaction for removed mailbox")
	}
}

func TestRepository_UpdateEmailMailboxes_NotFound(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	_, _, err := repo.UpdateEmailMailboxes(context.Background(), "user-123", "nonexistent", map[string]bool{"inbox-id": true})

	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if !errors.Is(err, ErrEmailNotFound) {
		t.Errorf("Expected ErrEmailNotFound, got %v", err)
	}
}

func TestMarshalEmailItem_IncludesVersion(t *testing.T) {
	email := &EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		ReceivedAt: time.Now(),
		Version:    1,
	}

	var capturedItem map[string]types.AttributeValue
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedItem = input.TransactItems[0].Put.Item
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	repo := NewRepository(mockClient, "test-table")
	_ = repo.CreateEmail(context.Background(), email)

	versionAttr, ok := capturedItem[AttrVersion]
	if !ok {
		t.Fatal("marshalEmailItem missing version field")
	}
	versionVal := versionAttr.(*types.AttributeValueMemberN).Value
	if versionVal != "1" {
		t.Errorf("version = %q, want %q", versionVal, "1")
	}
}

func TestUnmarshalEmailItem_IncludesVersion(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	mockClient := &mockDynamoDBClient{
		getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]types.AttributeValue{
					"pk":         &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
					"sk":         &types.AttributeValueMemberS{Value: "EMAIL#email-456"},
					"emailId":    &types.AttributeValueMemberS{Value: "email-456"},
					"accountId":  &types.AttributeValueMemberS{Value: "user-123"},
					"receivedAt": &types.AttributeValueMemberS{Value: receivedAt.Format(time.RFC3339)},
					AttrVersion:  &types.AttributeValueMemberN{Value: "5"},
				},
			}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	email, err := repo.GetEmail(context.Background(), "user-123", "email-456")
	if err != nil {
		t.Fatalf("GetEmail failed: %v", err)
	}

	if email.Version != 5 {
		t.Errorf("Version = %d, want %d", email.Version, 5)
	}
}

func TestRepository_UpdateEmailKeywords_Success(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	// Email with $seen keyword and version 1
	existingEmail := map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
		"sk":         &types.AttributeValueMemberS{Value: "EMAIL#email-456"},
		"emailId":    &types.AttributeValueMemberS{Value: "email-456"},
		"accountId":  &types.AttributeValueMemberS{Value: "user-123"},
		"receivedAt": &types.AttributeValueMemberS{Value: receivedAt.Format(time.RFC3339)},
		"mailboxIds": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"inbox-id": &types.AttributeValueMemberBOOL{Value: true},
		}},
		"keywords": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"$seen": &types.AttributeValueMemberBOOL{Value: true},
		}},
		AttrVersion: &types.AttributeValueMemberN{Value: "1"},
	}

	var capturedTransaction *dynamodb.TransactWriteItemsInput
	mockClient := &mockDynamoDBClient{
		getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: existingEmail}, nil
		},
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedTransaction = input
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	newKeywords := map[string]bool{"$seen": true, "$flagged": true}
	email, err := repo.UpdateEmailKeywords(context.Background(), "user-123", "email-456", newKeywords, 1)

	if err != nil {
		t.Fatalf("UpdateEmailKeywords failed: %v", err)
	}

	// Should return updated email
	if email == nil || email.EmailID != "email-456" {
		t.Errorf("email = %+v, want email-456", email)
	}

	// Email should have new keywords
	if !email.Keywords["$seen"] || !email.Keywords["$flagged"] {
		t.Errorf("email.Keywords = %v, want $seen and $flagged", email.Keywords)
	}

	// Version should be incremented
	if email.Version != 2 {
		t.Errorf("email.Version = %d, want 2", email.Version)
	}

	// Transaction should have been called
	if capturedTransaction == nil {
		t.Fatal("TransactWriteItems was not called")
	}
}

func TestRepository_UpdateEmailKeywords_VersionConflict(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	existingEmail := map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
		"sk":         &types.AttributeValueMemberS{Value: "EMAIL#email-456"},
		"emailId":    &types.AttributeValueMemberS{Value: "email-456"},
		"accountId":  &types.AttributeValueMemberS{Value: "user-123"},
		"receivedAt": &types.AttributeValueMemberS{Value: receivedAt.Format(time.RFC3339)},
		"mailboxIds": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"inbox-id": &types.AttributeValueMemberBOOL{Value: true},
		}},
		"keywords": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"$seen": &types.AttributeValueMemberBOOL{Value: true},
		}},
		AttrVersion: &types.AttributeValueMemberN{Value: "2"}, // Version is 2, but we'll send 1
	}

	mockClient := &mockDynamoDBClient{
		getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: existingEmail}, nil
		},
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			// Simulate version conflict - ConditionalCheckFailedException
			return nil, &types.TransactionCanceledException{
				CancellationReasons: []types.CancellationReason{
					{Code: aws.String("ConditionalCheckFailed")},
				},
			}
		},
	}

	repo := NewRepository(mockClient, "test-table")
	newKeywords := map[string]bool{"$flagged": true}
	_, err := repo.UpdateEmailKeywords(context.Background(), "user-123", "email-456", newKeywords, 1) // Expected version 1 but actual is 2

	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if !errors.Is(err, ErrVersionConflict) {
		t.Errorf("Expected ErrVersionConflict, got %v", err)
	}
}

func TestRepository_UpdateEmailKeywords_SeenChangeUpdatesMailboxCounters(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	// Email in inbox, not seen (unread)
	existingEmail := map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
		"sk":         &types.AttributeValueMemberS{Value: "EMAIL#email-456"},
		"emailId":    &types.AttributeValueMemberS{Value: "email-456"},
		"accountId":  &types.AttributeValueMemberS{Value: "user-123"},
		"receivedAt": &types.AttributeValueMemberS{Value: receivedAt.Format(time.RFC3339)},
		"mailboxIds": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"inbox-id":   &types.AttributeValueMemberBOOL{Value: true},
			"archive-id": &types.AttributeValueMemberBOOL{Value: true},
		}},
		"keywords":  &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{}}, // No $seen
		AttrVersion: &types.AttributeValueMemberN{Value: "1"},
	}

	var capturedTransaction *dynamodb.TransactWriteItemsInput
	mockClient := &mockDynamoDBClient{
		getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: existingEmail}, nil
		},
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedTransaction = input
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	// Mark as seen (was unread, now read)
	newKeywords := map[string]bool{"$seen": true}
	_, err := repo.UpdateEmailKeywords(context.Background(), "user-123", "email-456", newKeywords, 1)

	if err != nil {
		t.Fatalf("UpdateEmailKeywords failed: %v", err)
	}

	// Transaction should have: 1 email update + 2 mailbox counter decrements
	if capturedTransaction == nil {
		t.Fatal("TransactWriteItems was not called")
	}
	// 1 Put for email + 2 Updates for mailbox counters
	if len(capturedTransaction.TransactItems) != 3 {
		t.Errorf("TransactItems count = %d, want 3 (email update + 2 mailbox counter updates)", len(capturedTransaction.TransactItems))
	}

	// Check for Update operations (counter updates)
	updateCount := 0
	for _, item := range capturedTransaction.TransactItems {
		if item.Update != nil {
			updateCount++
		}
	}
	if updateCount != 2 {
		t.Errorf("Update operations = %d, want 2 (one per mailbox)", updateCount)
	}
}

func TestRepository_UpdateEmailKeywords_NotFound(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	_, err := repo.UpdateEmailKeywords(context.Background(), "user-123", "nonexistent", map[string]bool{"$seen": true}, 1)

	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if !errors.Is(err, ErrEmailNotFound) {
		t.Errorf("Expected ErrEmailNotFound, got %v", err)
	}
}

func TestRepository_UpdateEmailMailboxes_TransactionError(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	existingEmail := map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
		"sk":         &types.AttributeValueMemberS{Value: "EMAIL#email-456"},
		"emailId":    &types.AttributeValueMemberS{Value: "email-456"},
		"accountId":  &types.AttributeValueMemberS{Value: "user-123"},
		"receivedAt": &types.AttributeValueMemberS{Value: receivedAt.Format(time.RFC3339)},
		"mailboxIds": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"inbox-id": &types.AttributeValueMemberBOOL{Value: true},
		}},
	}

	mockClient := &mockDynamoDBClient{
		getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: existingEmail}, nil
		},
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			return nil, errors.New("transaction failed")
		},
	}

	repo := NewRepository(mockClient, "test-table")
	_, _, err := repo.UpdateEmailMailboxes(context.Background(), "user-123", "email-456", map[string]bool{"archive-id": true})

	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if !errors.Is(err, ErrTransactionFailed) {
		t.Errorf("Expected ErrTransactionFailed, got %v", err)
	}
}

func TestMarshalEmailItem_IncludesSender(t *testing.T) {
	email := &EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		ReceivedAt: time.Now(),
		Sender: []EmailAddress{
			{Name: "Secretary", Email: "secretary@example.com"},
		},
	}

	var capturedItem map[string]types.AttributeValue
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedItem = input.TransactItems[0].Put.Item
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	repo := NewRepository(mockClient, "test-table")
	_ = repo.CreateEmail(context.Background(), email)

	if _, ok := capturedItem[AttrSender]; !ok {
		t.Error("marshalEmailItem missing sender field")
	}
}

func TestMarshalEmailItem_IncludesBcc(t *testing.T) {
	email := &EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		ReceivedAt: time.Now(),
		Bcc: []EmailAddress{
			{Name: "Secret", Email: "secret@example.com"},
		},
	}

	var capturedItem map[string]types.AttributeValue
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedItem = input.TransactItems[0].Put.Item
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	repo := NewRepository(mockClient, "test-table")
	_ = repo.CreateEmail(context.Background(), email)

	if _, ok := capturedItem[AttrBcc]; !ok {
		t.Error("marshalEmailItem missing bcc field")
	}
}

func TestMarshalUnmarshal_SenderAndBcc_RoundTrip(t *testing.T) {
	original := &EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		ReceivedAt: time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC),
		Sender: []EmailAddress{
			{Name: "Secretary", Email: "secretary@example.com"},
		},
		Bcc: []EmailAddress{
			{Name: "Secret", Email: "secret@example.com"},
			{Name: "Hidden", Email: "hidden@example.com"},
		},
	}

	var capturedItem map[string]types.AttributeValue
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedItem = input.TransactItems[0].Put.Item
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	repo := NewRepository(mockClient, "test-table")
	_ = repo.CreateEmail(context.Background(), original)

	// Verify "sender" was marshaled
	if _, ok := capturedItem[AttrSender]; !ok {
		t.Fatal("Sender field not marshaled to DynamoDB")
	}

	// Verify "bcc" was marshaled
	if _, ok := capturedItem[AttrBcc]; !ok {
		t.Fatal("Bcc field not marshaled to DynamoDB")
	}

	// Now unmarshal and verify
	mockClient.getItemFunc = func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
		return &dynamodb.GetItemOutput{Item: capturedItem}, nil
	}

	retrieved, err := repo.GetEmail(context.Background(), "user-123", "email-456")
	if err != nil {
		t.Fatalf("GetEmail failed: %v", err)
	}

	// Verify Sender
	if len(retrieved.Sender) != 1 {
		t.Fatalf("Sender length = %d, want 1", len(retrieved.Sender))
	}
	if retrieved.Sender[0].Name != "Secretary" {
		t.Errorf("Sender[0].Name = %q, want %q", retrieved.Sender[0].Name, "Secretary")
	}
	if retrieved.Sender[0].Email != "secretary@example.com" {
		t.Errorf("Sender[0].Email = %q, want %q", retrieved.Sender[0].Email, "secretary@example.com")
	}

	// Verify Bcc
	if len(retrieved.Bcc) != 2 {
		t.Fatalf("Bcc length = %d, want 2", len(retrieved.Bcc))
	}
	if retrieved.Bcc[0].Name != "Secret" {
		t.Errorf("Bcc[0].Name = %q, want %q", retrieved.Bcc[0].Name, "Secret")
	}
	if retrieved.Bcc[0].Email != "secret@example.com" {
		t.Errorf("Bcc[0].Email = %q, want %q", retrieved.Bcc[0].Email, "secret@example.com")
	}
	if retrieved.Bcc[1].Name != "Hidden" {
		t.Errorf("Bcc[1].Name = %q, want %q", retrieved.Bcc[1].Name, "Hidden")
	}
	if retrieved.Bcc[1].Email != "hidden@example.com" {
		t.Errorf("Bcc[1].Email = %q, want %q", retrieved.Bcc[1].Email, "hidden@example.com")
	}
}

func TestMarshalEmailItem_IncludesHeaderSize(t *testing.T) {
	email := &EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		ReceivedAt: time.Now(),
		HeaderSize: 512, // Non-zero HeaderSize should be marshaled
	}

	var capturedItem map[string]types.AttributeValue
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedItem = input.TransactItems[0].Put.Item
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	repo := NewRepository(mockClient, "test-table")
	_ = repo.CreateEmail(context.Background(), email)

	headerSizeAttr, ok := capturedItem[AttrHeaderSize]
	if !ok {
		t.Fatal("marshalEmailItem missing headerSize field when HeaderSize > 0")
	}
	headerSizeVal := headerSizeAttr.(*types.AttributeValueMemberN).Value
	if headerSizeVal != "512" {
		t.Errorf("headerSize = %q, want %q", headerSizeVal, "512")
	}
}

func TestMarshalEmailItem_OmitsHeaderSizeWhenZero(t *testing.T) {
	email := &EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		ReceivedAt: time.Now(),
		HeaderSize: 0, // Zero HeaderSize should NOT be marshaled
	}

	var capturedItem map[string]types.AttributeValue
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedItem = input.TransactItems[0].Put.Item
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	repo := NewRepository(mockClient, "test-table")
	_ = repo.CreateEmail(context.Background(), email)

	if _, ok := capturedItem[AttrHeaderSize]; ok {
		t.Error("marshalEmailItem should NOT include headerSize field when HeaderSize == 0")
	}
}

func TestUnmarshalEmailItem_IncludesHeaderSize(t *testing.T) {
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	mockClient := &mockDynamoDBClient{
		getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]types.AttributeValue{
					"pk":            &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
					"sk":            &types.AttributeValueMemberS{Value: "EMAIL#email-456"},
					"emailId":       &types.AttributeValueMemberS{Value: "email-456"},
					"accountId":     &types.AttributeValueMemberS{Value: "user-123"},
					"receivedAt":    &types.AttributeValueMemberS{Value: receivedAt.Format(time.RFC3339)},
					AttrHeaderSize:  &types.AttributeValueMemberN{Value: "1024"},
				},
			}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table")
	email, err := repo.GetEmail(context.Background(), "user-123", "email-456")
	if err != nil {
		t.Fatalf("GetEmail failed: %v", err)
	}

	if email.HeaderSize != 1024 {
		t.Errorf("HeaderSize = %d, want %d", email.HeaderSize, 1024)
	}
}

func TestMarshalUnmarshal_HeaderSize_RoundTrip(t *testing.T) {
	original := &EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		ReceivedAt: time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC),
		HeaderSize: 2048,
	}

	var capturedItem map[string]types.AttributeValue
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedItem = input.TransactItems[0].Put.Item
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	repo := NewRepository(mockClient, "test-table")
	_ = repo.CreateEmail(context.Background(), original)

	// Verify headerSize was marshaled
	if _, ok := capturedItem[AttrHeaderSize]; !ok {
		t.Fatal("HeaderSize field not marshaled to DynamoDB")
	}

	// Now unmarshal and verify round-trip
	mockClient.getItemFunc = func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
		return &dynamodb.GetItemOutput{Item: capturedItem}, nil
	}

	retrieved, err := repo.GetEmail(context.Background(), "user-123", "email-456")
	if err != nil {
		t.Fatalf("GetEmail failed: %v", err)
	}

	if retrieved.HeaderSize != 2048 {
		t.Errorf("HeaderSize = %d, want %d", retrieved.HeaderSize, 2048)
	}
}

func TestRepository_BuildDeleteEmailItems_SingleMailbox(t *testing.T) {
	repo := NewRepository(&mockDynamoDBClient{}, "test-table")
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	emailItem := &EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		ReceivedAt: receivedAt,
		MailboxIDs: map[string]bool{"inbox-id": true},
		Version:    3,
	}

	items := repo.BuildDeleteEmailItems(emailItem)

	// 1 email delete + 1 membership delete = 2
	if len(items) != 2 {
		t.Fatalf("items count = %d, want 2", len(items))
	}

	// First item: delete email with condition check
	emailDelete := items[0].Delete
	if emailDelete == nil {
		t.Fatal("First item should be a Delete")
	}
	if *emailDelete.TableName != "test-table" {
		t.Errorf("TableName = %q, want %q", *emailDelete.TableName, "test-table")
	}
	pk := emailDelete.Key["pk"].(*types.AttributeValueMemberS).Value
	sk := emailDelete.Key["sk"].(*types.AttributeValueMemberS).Value
	if pk != "ACCOUNT#user-123" {
		t.Errorf("pk = %q, want %q", pk, "ACCOUNT#user-123")
	}
	if sk != "EMAIL#email-456" {
		t.Errorf("sk = %q, want %q", sk, "EMAIL#email-456")
	}
	// Should have condition expression for version check
	if emailDelete.ConditionExpression == nil {
		t.Fatal("Expected condition expression on email delete")
	}

	// Second item: delete membership
	memberDelete := items[1].Delete
	if memberDelete == nil {
		t.Fatal("Second item should be a Delete")
	}
}

func TestRepository_BuildDeleteEmailItems_MultipleMailboxes(t *testing.T) {
	repo := NewRepository(&mockDynamoDBClient{}, "test-table")
	receivedAt := time.Date(2024, 1, 20, 10, 0, 0, 0, time.UTC)

	emailItem := &EmailItem{
		AccountID:  "user-123",
		EmailID:    "email-456",
		ReceivedAt: receivedAt,
		MailboxIDs: map[string]bool{"inbox-id": true, "archive-id": true, "label-id": true},
		Version:    1,
	}

	items := repo.BuildDeleteEmailItems(emailItem)

	// 1 email delete + 3 membership deletes = 4
	if len(items) != 4 {
		t.Fatalf("items count = %d, want 4", len(items))
	}
}
