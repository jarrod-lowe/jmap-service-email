package state

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// mockDynamoDBClient implements the dbclient.DynamoDBClient interface for testing.
type mockDynamoDBClient struct {
	getItemFunc       func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	queryFunc         func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	putItemFunc       func(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	updateItemFunc    func(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	deleteItemFunc    func(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	transactWriteFunc func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
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

func (m *mockDynamoDBClient) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if m.updateItemFunc != nil {
		return m.updateItemFunc(ctx, input, opts...)
	}
	return &dynamodb.UpdateItemOutput{}, nil
}

func (m *mockDynamoDBClient) PutItem(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if m.putItemFunc != nil {
		return m.putItemFunc(ctx, input, opts...)
	}
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockDynamoDBClient) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if m.deleteItemFunc != nil {
		return m.deleteItemFunc(ctx, input, opts...)
	}
	return &dynamodb.DeleteItemOutput{}, nil
}

func (m *mockDynamoDBClient) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if m.transactWriteFunc != nil {
		return m.transactWriteFunc(ctx, input, opts...)
	}
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func TestRepository_GetCurrentState(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			// Verify the query is correct
			pk := input.Key["pk"].(*types.AttributeValueMemberS).Value
			sk := input.Key["sk"].(*types.AttributeValueMemberS).Value
			if pk != "ACCOUNT#user-123" {
				t.Errorf("pk = %q, want %q", pk, "ACCOUNT#user-123")
			}
			if sk != "STATE#Email" {
				t.Errorf("sk = %q, want %q", sk, "STATE#Email")
			}

			return &dynamodb.GetItemOutput{
				Item: map[string]types.AttributeValue{
					"pk":           &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
					"sk":           &types.AttributeValueMemberS{Value: "STATE#Email"},
					"currentState": &types.AttributeValueMemberN{Value: "42"},
					"updatedAt":    &types.AttributeValueMemberS{Value: "2024-01-20T10:00:00Z"},
				},
			}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table", 7)
	state, err := repo.GetCurrentState(context.Background(), "user-123", ObjectTypeEmail)
	if err != nil {
		t.Fatalf("GetCurrentState failed: %v", err)
	}

	if state != 42 {
		t.Errorf("state = %d, want 42", state)
	}
}

func TestRepository_GetCurrentState_NotFound(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table", 7)
	state, err := repo.GetCurrentState(context.Background(), "user-123", ObjectTypeEmail)
	if err != nil {
		t.Fatalf("GetCurrentState failed: %v", err)
	}

	// If no state exists, return 0
	if state != 0 {
		t.Errorf("state = %d, want 0", state)
	}
}

func TestRepository_GetCurrentState_DynamoDBError(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		getItemFunc: func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return nil, errors.New("dynamodb error")
		},
	}

	repo := NewRepository(mockClient, "test-table", 7)
	_, err := repo.GetCurrentState(context.Background(), "user-123", ObjectTypeEmail)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
}

func TestRepository_IncrementStateAndLogChange(t *testing.T) {
	var capturedInput *dynamodb.TransactWriteItemsInput
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedInput = input
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table", 7)
	newState, err := repo.IncrementStateAndLogChange(context.Background(), "user-123", ObjectTypeEmail, "email-456", ChangeTypeCreated)
	if err != nil {
		t.Fatalf("IncrementStateAndLogChange failed: %v", err)
	}

	// Should return the new state (1 for first increment from 0)
	if newState != 1 {
		t.Errorf("newState = %d, want 1", newState)
	}

	if capturedInput == nil {
		t.Fatal("TransactWriteItems was not called")
	}

	// Should have 2 items: state update and change log entry
	if len(capturedInput.TransactItems) != 2 {
		t.Errorf("TransactItems count = %d, want 2", len(capturedInput.TransactItems))
	}

	// First item should be the state counter update
	stateUpdate := capturedInput.TransactItems[0].Update
	if stateUpdate == nil {
		t.Fatal("First item should be an Update for state counter")
	}
	if *stateUpdate.TableName != "test-table" {
		t.Errorf("TableName = %q, want %q", *stateUpdate.TableName, "test-table")
	}

	// Verify PK/SK in state update
	pk := stateUpdate.Key["pk"].(*types.AttributeValueMemberS).Value
	sk := stateUpdate.Key["sk"].(*types.AttributeValueMemberS).Value
	if pk != "ACCOUNT#user-123" {
		t.Errorf("pk = %q, want %q", pk, "ACCOUNT#user-123")
	}
	if sk != "STATE#Email" {
		t.Errorf("sk = %q, want %q", sk, "STATE#Email")
	}

	// Second item should be the change log entry
	changePut := capturedInput.TransactItems[1].Put
	if changePut == nil {
		t.Fatal("Second item should be a Put for change log")
	}

	// Verify change log entry has correct attributes
	changeSK := changePut.Item["sk"].(*types.AttributeValueMemberS).Value
	if changeSK != "CHANGE#Email#0000000001" {
		t.Errorf("change sk = %q, want CHANGE#Email#0000000001", changeSK)
	}

	objectID := changePut.Item["objectId"].(*types.AttributeValueMemberS).Value
	if objectID != "email-456" {
		t.Errorf("objectId = %q, want %q", objectID, "email-456")
	}

	changeType := changePut.Item["changeType"].(*types.AttributeValueMemberS).Value
	if changeType != "created" {
		t.Errorf("changeType = %q, want %q", changeType, "created")
	}

	// Verify TTL is set
	if _, ok := changePut.Item["ttl"]; !ok {
		t.Error("change log entry missing ttl field")
	}
}

func TestRepository_IncrementStateAndLogChange_TransactionError(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			return nil, errors.New("transaction failed")
		},
	}

	repo := NewRepository(mockClient, "test-table", 7)
	_, err := repo.IncrementStateAndLogChange(context.Background(), "user-123", ObjectTypeEmail, "email-456", ChangeTypeCreated)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if !errors.Is(err, ErrTransactionFailed) {
		t.Errorf("Expected ErrTransactionFailed, got %v", err)
	}
}

func TestRepository_QueryChanges(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			// Verify query parameters
			if *input.TableName != "test-table" {
				t.Errorf("TableName = %q, want %q", *input.TableName, "test-table")
			}

			return &dynamodb.QueryOutput{
				Items: []map[string]types.AttributeValue{
					{
						"pk":         &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
						"sk":         &types.AttributeValueMemberS{Value: "CHANGE#Email#0000000011"},
						"objectId":   &types.AttributeValueMemberS{Value: "email-1"},
						"changeType": &types.AttributeValueMemberS{Value: "created"},
						"timestamp":  &types.AttributeValueMemberS{Value: "2024-01-20T10:00:00Z"},
						"state":      &types.AttributeValueMemberN{Value: "11"},
					},
					{
						"pk":         &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
						"sk":         &types.AttributeValueMemberS{Value: "CHANGE#Email#0000000012"},
						"objectId":   &types.AttributeValueMemberS{Value: "email-2"},
						"changeType": &types.AttributeValueMemberS{Value: "updated"},
						"timestamp":  &types.AttributeValueMemberS{Value: "2024-01-20T10:01:00Z"},
						"state":      &types.AttributeValueMemberN{Value: "12"},
					},
				},
			}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table", 7)
	changes, err := repo.QueryChanges(context.Background(), "user-123", ObjectTypeEmail, 10, 50)
	if err != nil {
		t.Fatalf("QueryChanges failed: %v", err)
	}

	if len(changes) != 2 {
		t.Fatalf("changes count = %d, want 2", len(changes))
	}

	if changes[0].ObjectID != "email-1" {
		t.Errorf("changes[0].ObjectID = %q, want %q", changes[0].ObjectID, "email-1")
	}
	if changes[0].ChangeType != ChangeTypeCreated {
		t.Errorf("changes[0].ChangeType = %q, want %q", changes[0].ChangeType, ChangeTypeCreated)
	}
	if changes[0].State != 11 {
		t.Errorf("changes[0].State = %d, want 11", changes[0].State)
	}

	if changes[1].ObjectID != "email-2" {
		t.Errorf("changes[1].ObjectID = %q, want %q", changes[1].ObjectID, "email-2")
	}
	if changes[1].ChangeType != ChangeTypeUpdated {
		t.Errorf("changes[1].ChangeType = %q, want %q", changes[1].ChangeType, ChangeTypeUpdated)
	}
}

func TestRepository_QueryChanges_Empty(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{}}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table", 7)
	changes, err := repo.QueryChanges(context.Background(), "user-123", ObjectTypeEmail, 10, 50)
	if err != nil {
		t.Fatalf("QueryChanges failed: %v", err)
	}

	if len(changes) != 0 {
		t.Errorf("expected empty changes, got %d", len(changes))
	}
}

func TestRepository_QueryChanges_Error(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return nil, errors.New("dynamodb error")
		},
	}

	repo := NewRepository(mockClient, "test-table", 7)
	_, err := repo.QueryChanges(context.Background(), "user-123", ObjectTypeEmail, 10, 50)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
}

func TestRepository_GetOldestAvailableState(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			// Verify ascending sort order to get oldest first
			if input.ScanIndexForward == nil || !*input.ScanIndexForward {
				t.Error("Expected ScanIndexForward = true for ascending sort")
			}

			return &dynamodb.QueryOutput{
				Items: []map[string]types.AttributeValue{
					{
						"pk":    &types.AttributeValueMemberS{Value: "ACCOUNT#user-123"},
						"sk":    &types.AttributeValueMemberS{Value: "CHANGE#Email#0000000005"},
						"state": &types.AttributeValueMemberN{Value: "5"},
					},
				},
			}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table", 7)
	oldestState, err := repo.GetOldestAvailableState(context.Background(), "user-123", ObjectTypeEmail)
	if err != nil {
		t.Fatalf("GetOldestAvailableState failed: %v", err)
	}

	if oldestState != 5 {
		t.Errorf("oldestState = %d, want 5", oldestState)
	}
}

func TestRepository_GetOldestAvailableState_NoChanges(t *testing.T) {
	mockClient := &mockDynamoDBClient{
		queryFunc: func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{}}, nil
		},
	}

	repo := NewRepository(mockClient, "test-table", 7)
	oldestState, err := repo.GetOldestAvailableState(context.Background(), "user-123", ObjectTypeEmail)
	if err != nil {
		t.Fatalf("GetOldestAvailableState failed: %v", err)
	}

	// If no changes exist, return 0 (we can calculate from beginning)
	if oldestState != 0 {
		t.Errorf("oldestState = %d, want 0", oldestState)
	}
}

func TestStateItem_PK_SK(t *testing.T) {
	item := &StateItem{
		AccountID:  "user-123",
		ObjectType: ObjectTypeEmail,
	}

	if pk := item.PK(); pk != "ACCOUNT#user-123" {
		t.Errorf("PK() = %q, want %q", pk, "ACCOUNT#user-123")
	}
	if sk := item.SK(); sk != "STATE#Email" {
		t.Errorf("SK() = %q, want %q", sk, "STATE#Email")
	}
}

func TestChangeRecord_PK_SK(t *testing.T) {
	record := &ChangeRecord{
		AccountID:  "user-123",
		ObjectType: ObjectTypeEmail,
		State:      42,
	}

	if pk := record.PK(); pk != "ACCOUNT#user-123" {
		t.Errorf("PK() = %q, want %q", pk, "ACCOUNT#user-123")
	}
	if sk := record.SK(); sk != "CHANGE#Email#0000000042" {
		t.Errorf("SK() = %q, want %q", sk, "CHANGE#Email#0000000042")
	}
}

func TestChangeRecord_SK_ZeroPadding(t *testing.T) {
	testCases := []struct {
		state    int64
		expected string
	}{
		{1, "CHANGE#Email#0000000001"},
		{12, "CHANGE#Email#0000000012"},
		{123, "CHANGE#Email#0000000123"},
		{1234567890, "CHANGE#Email#1234567890"},
	}

	for _, tc := range testCases {
		record := &ChangeRecord{
			AccountID:  "user-123",
			ObjectType: ObjectTypeEmail,
			State:      tc.state,
		}
		if sk := record.SK(); sk != tc.expected {
			t.Errorf("State %d: SK() = %q, want %q", tc.state, sk, tc.expected)
		}
	}
}

func TestRepository_IncrementStateAndLogChange_TTL(t *testing.T) {
	var capturedInput *dynamodb.TransactWriteItemsInput
	mockClient := &mockDynamoDBClient{
		transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			capturedInput = input
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	retentionDays := 14
	repo := NewRepository(mockClient, "test-table", retentionDays)
	_, err := repo.IncrementStateAndLogChange(context.Background(), "user-123", ObjectTypeEmail, "email-456", ChangeTypeCreated)
	if err != nil {
		t.Fatalf("IncrementStateAndLogChange failed: %v", err)
	}

	// Verify TransactWriteItems was called
	if capturedInput == nil {
		t.Fatal("TransactWriteItems was not called")
	}
	if len(capturedInput.TransactItems) < 2 {
		t.Fatalf("TransactItems count = %d, want at least 2", len(capturedInput.TransactItems))
	}
	changePut := capturedInput.TransactItems[1].Put
	if changePut == nil {
		t.Fatal("Second item should be a Put for change log")
	}

	// Verify TTL is approximately correct (within 1 hour tolerance for test timing)
	ttlAttrVal, ok := changePut.Item["ttl"]
	if !ok {
		t.Fatal("change log entry missing ttl field")
	}
	ttlAttr := ttlAttrVal.(*types.AttributeValueMemberN).Value

	// Parse TTL
	var ttl int64
	if _, err := time.Parse("2006-01-02", ttlAttr); err != nil {
		// It's a Unix timestamp
		if n, parseErr := parseInt64(ttlAttr); parseErr == nil {
			ttl = n
		}
	}

	expectedTTL := time.Now().Add(time.Duration(retentionDays) * 24 * time.Hour).Unix()
	// Allow 1 hour tolerance
	tolerance := int64(3600)
	if ttl < expectedTTL-tolerance || ttl > expectedTTL+tolerance {
		t.Errorf("TTL = %d, expected approximately %d", ttl, expectedTTL)
	}
}

func TestRepository_BuildStateChangeItems_ReturnsCorrectItems(t *testing.T) {
	repo := NewRepository(&mockDynamoDBClient{}, "test-table", 7)

	newState, items := repo.BuildStateChangeItems("user-123", ObjectTypeEmail, 5, "email-456", ChangeTypeDestroyed)

	if newState != 6 {
		t.Errorf("newState = %d, want 6", newState)
	}

	if len(items) != 2 {
		t.Fatalf("items count = %d, want 2", len(items))
	}

	// First item: state counter update
	stateUpdate := items[0].Update
	if stateUpdate == nil {
		t.Fatal("First item should be an Update")
	}
	if *stateUpdate.TableName != "test-table" {
		t.Errorf("TableName = %q, want %q", *stateUpdate.TableName, "test-table")
	}
	pk := stateUpdate.Key["pk"].(*types.AttributeValueMemberS).Value
	sk := stateUpdate.Key["sk"].(*types.AttributeValueMemberS).Value
	if pk != "ACCOUNT#user-123" {
		t.Errorf("pk = %q, want %q", pk, "ACCOUNT#user-123")
	}
	if sk != "STATE#Email" {
		t.Errorf("sk = %q, want %q", sk, "STATE#Email")
	}

	// Second item: change log put
	changePut := items[1].Put
	if changePut == nil {
		t.Fatal("Second item should be a Put")
	}
	changeSK := changePut.Item["sk"].(*types.AttributeValueMemberS).Value
	if changeSK != "CHANGE#Email#0000000006" {
		t.Errorf("change sk = %q, want CHANGE#Email#0000000006", changeSK)
	}
	objectID := changePut.Item["objectId"].(*types.AttributeValueMemberS).Value
	if objectID != "email-456" {
		t.Errorf("objectId = %q, want %q", objectID, "email-456")
	}
	changeType := changePut.Item["changeType"].(*types.AttributeValueMemberS).Value
	if changeType != "destroyed" {
		t.Errorf("changeType = %q, want %q", changeType, "destroyed")
	}
}

func TestRepository_BuildStateChangeItems_FromZero(t *testing.T) {
	repo := NewRepository(&mockDynamoDBClient{}, "test-table", 7)

	newState, items := repo.BuildStateChangeItems("user-123", ObjectTypeMailbox, 0, "mbox-1", ChangeTypeUpdated)

	if newState != 1 {
		t.Errorf("newState = %d, want 1", newState)
	}
	if len(items) != 2 {
		t.Fatalf("items count = %d, want 2", len(items))
	}
}

func TestRepository_BuildStateChangeItemsMulti_MultipleObjects(t *testing.T) {
	repo := NewRepository(&mockDynamoDBClient{}, "test-table", 7)

	objectIDs := []string{"mbox-1", "mbox-2", "mbox-3"}
	newState, items := repo.BuildStateChangeItemsMulti("user-123", ObjectTypeMailbox, 5, objectIDs, ChangeTypeUpdated)

	// newState should be currentState + len(objectIDs) = 5 + 3 = 8
	if newState != 8 {
		t.Errorf("newState = %d, want 8", newState)
	}

	// Should have 1 state update + 3 change log puts = 4 items
	if len(items) != 4 {
		t.Fatalf("items count = %d, want 4", len(items))
	}

	// First item: state counter update
	stateUpdate := items[0].Update
	if stateUpdate == nil {
		t.Fatal("First item should be an Update")
	}
	pk := stateUpdate.Key["pk"].(*types.AttributeValueMemberS).Value
	sk := stateUpdate.Key["sk"].(*types.AttributeValueMemberS).Value
	if pk != "ACCOUNT#user-123" {
		t.Errorf("pk = %q, want %q", pk, "ACCOUNT#user-123")
	}
	if sk != "STATE#Mailbox" {
		t.Errorf("sk = %q, want %q", sk, "STATE#Mailbox")
	}

	// Verify the increment expression uses :n (the count) not :one
	updateExpr := *stateUpdate.UpdateExpression
	if updateExpr == "" {
		t.Fatal("UpdateExpression is empty")
	}
	incrementVal := stateUpdate.ExpressionAttributeValues[":n"].(*types.AttributeValueMemberN).Value
	if incrementVal != "3" {
		t.Errorf("increment value = %q, want %q", incrementVal, "3")
	}

	// Change log entries should have sequential states: 6, 7, 8
	expectedSKs := []string{
		"CHANGE#Mailbox#0000000006",
		"CHANGE#Mailbox#0000000007",
		"CHANGE#Mailbox#0000000008",
	}
	expectedObjectIDs := []string{"mbox-1", "mbox-2", "mbox-3"}

	for i := 0; i < 3; i++ {
		put := items[i+1].Put
		if put == nil {
			t.Fatalf("items[%d] should be a Put", i+1)
		}
		changeSK := put.Item["sk"].(*types.AttributeValueMemberS).Value
		if changeSK != expectedSKs[i] {
			t.Errorf("items[%d] sk = %q, want %q", i+1, changeSK, expectedSKs[i])
		}
		objectID := put.Item["objectId"].(*types.AttributeValueMemberS).Value
		if objectID != expectedObjectIDs[i] {
			t.Errorf("items[%d] objectId = %q, want %q", i+1, objectID, expectedObjectIDs[i])
		}
		changeType := put.Item["changeType"].(*types.AttributeValueMemberS).Value
		if changeType != "updated" {
			t.Errorf("items[%d] changeType = %q, want %q", i+1, changeType, "updated")
		}
	}
}

func TestRepository_BuildStateChangeItemsMulti_SingleObject(t *testing.T) {
	repo := NewRepository(&mockDynamoDBClient{}, "test-table", 7)

	newState, items := repo.BuildStateChangeItemsMulti("user-123", ObjectTypeMailbox, 5, []string{"mbox-1"}, ChangeTypeUpdated)

	if newState != 6 {
		t.Errorf("newState = %d, want 6", newState)
	}
	// 1 state update + 1 change log = 2 items
	if len(items) != 2 {
		t.Fatalf("items count = %d, want 2", len(items))
	}
}

func TestRepository_BuildStateChangeItemsMulti_EmptySlice(t *testing.T) {
	repo := NewRepository(&mockDynamoDBClient{}, "test-table", 7)

	newState, items := repo.BuildStateChangeItemsMulti("user-123", ObjectTypeMailbox, 5, []string{}, ChangeTypeUpdated)

	// No objects means no state change
	if newState != 5 {
		t.Errorf("newState = %d, want 5", newState)
	}
	if len(items) != 0 {
		t.Fatalf("items count = %d, want 0", len(items))
	}
}

func TestChangeLogPuts_HaveConditionExpression(t *testing.T) {
	t.Run("IncrementStateAndLogChange", func(t *testing.T) {
		var capturedInput *dynamodb.TransactWriteItemsInput
		mockClient := &mockDynamoDBClient{
			transactWriteFunc: func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
				capturedInput = input
				return &dynamodb.TransactWriteItemsOutput{}, nil
			},
		}

		repo := NewRepository(mockClient, "test-table", 7)
		_, err := repo.IncrementStateAndLogChange(context.Background(), "user-123", ObjectTypeEmail, "email-1", ChangeTypeCreated)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		changePut := capturedInput.TransactItems[1].Put
		if changePut.ConditionExpression == nil || *changePut.ConditionExpression != "attribute_not_exists(pk)" {
			t.Errorf("change log Put ConditionExpression = %v, want %q", changePut.ConditionExpression, "attribute_not_exists(pk)")
		}
	})

	t.Run("BuildStateChangeItems", func(t *testing.T) {
		repo := NewRepository(&mockDynamoDBClient{}, "test-table", 7)
		_, items := repo.BuildStateChangeItems("user-123", ObjectTypeEmail, 5, "email-1", ChangeTypeCreated)

		changePut := items[1].Put
		if changePut.ConditionExpression == nil || *changePut.ConditionExpression != "attribute_not_exists(pk)" {
			t.Errorf("change log Put ConditionExpression = %v, want %q", changePut.ConditionExpression, "attribute_not_exists(pk)")
		}
	})

	t.Run("BuildStateChangeItemsMulti", func(t *testing.T) {
		repo := NewRepository(&mockDynamoDBClient{}, "test-table", 7)
		_, items := repo.BuildStateChangeItemsMulti("user-123", ObjectTypeEmail, 5, []string{"e1", "e2"}, ChangeTypeCreated)

		for i := 1; i < len(items); i++ {
			put := items[i].Put
			if put.ConditionExpression == nil || *put.ConditionExpression != "attribute_not_exists(pk)" {
				t.Errorf("items[%d] Put ConditionExpression = %v, want %q", i, put.ConditionExpression, "attribute_not_exists(pk)")
			}
		}
	})

	t.Run("BuildChangeLogItem", func(t *testing.T) {
		repo := NewRepository(&mockDynamoDBClient{}, "test-table", 7)
		item := repo.BuildChangeLogItem("user-123", ObjectTypeEmail, 5, "email-1", ChangeTypeCreated)

		put := item.Put
		if put.ConditionExpression == nil || *put.ConditionExpression != "attribute_not_exists(pk)" {
			t.Errorf("BuildChangeLogItem Put ConditionExpression = %v, want %q", put.ConditionExpression, "attribute_not_exists(pk)")
		}
	})
}

// parseInt64 parses a string as int64
func parseInt64(s string) (int64, error) {
	var n int64
	_, err := parseIntHelper(s, &n)
	return n, err
}

func parseIntHelper(s string, n *int64) (int, error) {
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, errors.New("invalid number")
		}
		*n = *n*10 + int64(c-'0')
	}
	return 0, nil
}
