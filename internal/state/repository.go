package state

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Error types for repository operations.
var (
	ErrTransactionFailed = errors.New("transaction failed")
)

// DynamoDBClient defines the interface for DynamoDB operations.
type DynamoDBClient interface {
	GetItem(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	Query(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// Repository handles state tracking operations.
type Repository struct {
	client        DynamoDBClient
	tableName     string
	retentionDays int
}

// NewRepository creates a new Repository.
func NewRepository(client DynamoDBClient, tableName string, retentionDays int) *Repository {
	if retentionDays <= 0 {
		retentionDays = DefaultRetentionDays
	}
	return &Repository{
		client:        client,
		tableName:     tableName,
		retentionDays: retentionDays,
	}
}

// GetCurrentState retrieves the current state counter for an account and object type.
// Returns 0 if no state exists yet.
func (r *Repository) GetCurrentState(ctx context.Context, accountID string, objectType ObjectType) (int64, error) {
	stateItem := &StateItem{AccountID: accountID, ObjectType: objectType}

	output, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: stateItem.PK()},
			"sk": &types.AttributeValueMemberS{Value: stateItem.SK()},
		},
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get current state: %w", err)
	}

	if output.Item == nil {
		return 0, nil
	}

	if v, ok := output.Item["currentState"].(*types.AttributeValueMemberN); ok {
		state, err := strconv.ParseInt(v.Value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse state: %w", err)
		}
		return state, nil
	}

	return 0, nil
}

// IncrementStateAndLogChange atomically increments the state counter and writes a change log entry.
// Returns the new state value.
func (r *Repository) IncrementStateAndLogChange(ctx context.Context, accountID string, objectType ObjectType, objectID string, changeType ChangeType) (int64, error) {
	stateItem := &StateItem{AccountID: accountID, ObjectType: objectType}
	now := time.Now().UTC()
	ttl := now.Add(time.Duration(r.retentionDays) * 24 * time.Hour).Unix()

	// We need to get current state first to know the new state for the change log SK
	currentState, err := r.GetCurrentState(ctx, accountID, objectType)
	if err != nil {
		return 0, err
	}
	newState := currentState + 1

	changeRecord := &ChangeRecord{
		AccountID:  accountID,
		ObjectType: objectType,
		State:      newState,
		ObjectID:   objectID,
		ChangeType: changeType,
		Timestamp:  now,
		TTL:        ttl,
	}

	transactItems := []types.TransactWriteItem{
		// Update state counter
		{
			Update: &types.Update{
				TableName: aws.String(r.tableName),
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: stateItem.PK()},
					"sk": &types.AttributeValueMemberS{Value: stateItem.SK()},
				},
				UpdateExpression: aws.String("SET currentState = if_not_exists(currentState, :zero) + :one, updatedAt = :now"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":zero": &types.AttributeValueMemberN{Value: "0"},
					":one":  &types.AttributeValueMemberN{Value: "1"},
					":now":  &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
				},
			},
		},
		// Write change log entry
		{
			Put: &types.Put{
				TableName: aws.String(r.tableName),
				Item: map[string]types.AttributeValue{
					"pk":         &types.AttributeValueMemberS{Value: changeRecord.PK()},
					"sk":         &types.AttributeValueMemberS{Value: changeRecord.SK()},
					"objectId":   &types.AttributeValueMemberS{Value: objectID},
					"changeType": &types.AttributeValueMemberS{Value: string(changeType)},
					"timestamp":  &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
					"state":      &types.AttributeValueMemberN{Value: strconv.FormatInt(newState, 10)},
					"ttl":        &types.AttributeValueMemberN{Value: strconv.FormatInt(ttl, 10)},
				},
			},
		},
	}

	_, err = r.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrTransactionFailed, err)
	}

	return newState, nil
}

// QueryChanges retrieves change log entries since a given state.
func (r *Repository) QueryChanges(ctx context.Context, accountID string, objectType ObjectType, sinceState int64, maxChanges int) ([]ChangeRecord, error) {
	pk := fmt.Sprintf("ACCOUNT#%s", accountID)
	// Start from sinceState + 1 (we want changes AFTER sinceState)
	skStart := fmt.Sprintf("CHANGE#%s#%010d", objectType, sinceState+1)
	// End at max possible state for this type
	skEnd := fmt.Sprintf("CHANGE#%s#%010d", objectType, MaxStateValue)

	queryInput := &dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		KeyConditionExpression: aws.String("pk = :pk AND sk BETWEEN :skStart AND :skEnd"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":      &types.AttributeValueMemberS{Value: pk},
			":skStart": &types.AttributeValueMemberS{Value: skStart},
			":skEnd":   &types.AttributeValueMemberS{Value: skEnd},
		},
		ScanIndexForward: aws.Bool(true), // Ascending order
	}

	if maxChanges > 0 {
		queryInput.Limit = aws.Int32(int32(maxChanges))
	}

	output, err := r.client.Query(ctx, queryInput)
	if err != nil {
		return nil, fmt.Errorf("failed to query changes: %w", err)
	}

	changes := make([]ChangeRecord, 0, len(output.Items))
	for _, item := range output.Items {
		record := ChangeRecord{
			AccountID:  accountID,
			ObjectType: objectType,
		}

		if v, ok := item["objectId"].(*types.AttributeValueMemberS); ok {
			record.ObjectID = v.Value
		}
		if v, ok := item["changeType"].(*types.AttributeValueMemberS); ok {
			record.ChangeType = ChangeType(v.Value)
		}
		if v, ok := item["timestamp"].(*types.AttributeValueMemberS); ok {
			if t, err := time.Parse(time.RFC3339, v.Value); err == nil {
				record.Timestamp = t
			}
		}
		if v, ok := item["state"].(*types.AttributeValueMemberN); ok {
			if n, err := strconv.ParseInt(v.Value, 10, 64); err == nil {
				record.State = n
			}
		}

		changes = append(changes, record)
	}

	return changes, nil
}

// GetOldestAvailableState returns the oldest state still in the change log.
// Returns 0 if no changes exist (we can calculate from the beginning).
func (r *Repository) GetOldestAvailableState(ctx context.Context, accountID string, objectType ObjectType) (int64, error) {
	pk := fmt.Sprintf("ACCOUNT#%s", accountID)
	skPrefix := fmt.Sprintf("CHANGE#%s#", objectType)

	output, err := r.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		KeyConditionExpression: aws.String("pk = :pk AND begins_with(sk, :skPrefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":       &types.AttributeValueMemberS{Value: pk},
			":skPrefix": &types.AttributeValueMemberS{Value: skPrefix},
		},
		ScanIndexForward: aws.Bool(true), // Ascending order to get oldest first
		Limit:            aws.Int32(1),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get oldest available state: %w", err)
	}

	if len(output.Items) == 0 {
		return 0, nil
	}

	if v, ok := output.Items[0]["state"].(*types.AttributeValueMemberN); ok {
		state, err := strconv.ParseInt(v.Value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse state: %w", err)
		}
		return state, nil
	}

	return 0, nil
}
