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
	"github.com/jarrod-lowe/jmap-service-libs/dbclient"

	"github.com/jarrod-lowe/jmap-service-email/internal/dynamo"
)

// Error types for repository operations.
var (
	ErrTransactionFailed = errors.New("transaction failed")
)

// Repository handles state tracking operations.
type Repository struct {
	client        dbclient.DynamoDBClient
	tableName     string
	retentionDays int
}

// NewRepository creates a new Repository.
func NewRepository(client dbclient.DynamoDBClient, tableName string, retentionDays int) *Repository {
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
			dynamo.AttrPK: &types.AttributeValueMemberS{Value: stateItem.PK()},
			dynamo.AttrSK: &types.AttributeValueMemberS{Value: stateItem.SK()},
		},
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get current state: %w", err)
	}

	if output.Item == nil {
		return 0, nil
	}

	if v, ok := output.Item[AttrCurrentState].(*types.AttributeValueMemberN); ok {
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
					dynamo.AttrPK: &types.AttributeValueMemberS{Value: stateItem.PK()},
					dynamo.AttrSK: &types.AttributeValueMemberS{Value: stateItem.SK()},
				},
				UpdateExpression: aws.String("SET " + AttrCurrentState + " = if_not_exists(" + AttrCurrentState + ", :zero) + :one, " + AttrUpdatedAt + " = :now"),
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
				TableName:           aws.String(r.tableName),
				ConditionExpression: aws.String("attribute_not_exists(pk)"),
				Item: map[string]types.AttributeValue{
					dynamo.AttrPK:  &types.AttributeValueMemberS{Value: changeRecord.PK()},
					dynamo.AttrSK:  &types.AttributeValueMemberS{Value: changeRecord.SK()},
					AttrObjectID:   &types.AttributeValueMemberS{Value: objectID},
					AttrChangeType: &types.AttributeValueMemberS{Value: string(changeType)},
					AttrTimestamp:  &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
					AttrState:      &types.AttributeValueMemberN{Value: strconv.FormatInt(newState, 10)},
					AttrTTL:        &types.AttributeValueMemberN{Value: strconv.FormatInt(ttl, 10)},
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

// BuildStateChangeItems returns the transaction items needed to increment state
// and log a change, without executing the transaction. The caller is responsible
// for including these items in their own transaction.
func (r *Repository) BuildStateChangeItems(accountID string, objectType ObjectType, currentState int64, objectID string, changeType ChangeType) (int64, []types.TransactWriteItem) {
	newState := currentState + 1
	now := time.Now().UTC()
	ttl := now.Add(time.Duration(r.retentionDays) * 24 * time.Hour).Unix()

	stateItem := &StateItem{AccountID: accountID, ObjectType: objectType}
	changeRecord := &ChangeRecord{
		AccountID:  accountID,
		ObjectType: objectType,
		State:      newState,
		ObjectID:   objectID,
		ChangeType: changeType,
		Timestamp:  now,
		TTL:        ttl,
	}

	items := []types.TransactWriteItem{
		{
			Update: &types.Update{
				TableName: aws.String(r.tableName),
				Key: map[string]types.AttributeValue{
					dynamo.AttrPK: &types.AttributeValueMemberS{Value: stateItem.PK()},
					dynamo.AttrSK: &types.AttributeValueMemberS{Value: stateItem.SK()},
				},
				UpdateExpression: aws.String("SET " + AttrCurrentState + " = if_not_exists(" + AttrCurrentState + ", :zero) + :one, " + AttrUpdatedAt + " = :now"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":zero": &types.AttributeValueMemberN{Value: "0"},
					":one":  &types.AttributeValueMemberN{Value: "1"},
					":now":  &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
				},
			},
		},
		{
			Put: &types.Put{
				TableName:           aws.String(r.tableName),
				ConditionExpression: aws.String("attribute_not_exists(pk)"),
				Item: map[string]types.AttributeValue{
					dynamo.AttrPK:  &types.AttributeValueMemberS{Value: changeRecord.PK()},
					dynamo.AttrSK:  &types.AttributeValueMemberS{Value: changeRecord.SK()},
					AttrObjectID:   &types.AttributeValueMemberS{Value: objectID},
					AttrChangeType: &types.AttributeValueMemberS{Value: string(changeType)},
					AttrTimestamp:  &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
					AttrState:      &types.AttributeValueMemberN{Value: strconv.FormatInt(newState, 10)},
					AttrTTL:        &types.AttributeValueMemberN{Value: strconv.FormatInt(ttl, 10)},
				},
			},
		},
	}

	return newState, items
}

// BuildStateChangeItemsMulti returns the transaction items needed to increment state
// by len(objectIDs) and log a change for each object with sequential state numbers.
// This avoids duplicate SK values when multiple objects of the same type are changed
// in a single transaction.
func (r *Repository) BuildStateChangeItemsMulti(accountID string, objectType ObjectType, currentState int64, objectIDs []string, changeType ChangeType) (int64, []types.TransactWriteItem) {
	n := int64(len(objectIDs))
	if n == 0 {
		return currentState, nil
	}

	now := time.Now().UTC()
	ttl := now.Add(time.Duration(r.retentionDays) * 24 * time.Hour).Unix()
	newState := currentState + n

	stateItem := &StateItem{AccountID: accountID, ObjectType: objectType}

	items := make([]types.TransactWriteItem, 0, n+1)

	// State counter update — increment by n
	items = append(items, types.TransactWriteItem{
		Update: &types.Update{
			TableName: aws.String(r.tableName),
			Key: map[string]types.AttributeValue{
				dynamo.AttrPK: &types.AttributeValueMemberS{Value: stateItem.PK()},
				dynamo.AttrSK: &types.AttributeValueMemberS{Value: stateItem.SK()},
			},
			UpdateExpression: aws.String("SET " + AttrCurrentState + " = if_not_exists(" + AttrCurrentState + ", :zero) + :n, " + AttrUpdatedAt + " = :now"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":zero": &types.AttributeValueMemberN{Value: "0"},
				":n":    &types.AttributeValueMemberN{Value: strconv.FormatInt(n, 10)},
				":now":  &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
			},
		},
	})

	// One change log entry per object, with sequential states
	for i, objectID := range objectIDs {
		seqState := currentState + int64(i) + 1
		changeRecord := &ChangeRecord{
			AccountID:  accountID,
			ObjectType: objectType,
			State:      seqState,
			ObjectID:   objectID,
			ChangeType: changeType,
			Timestamp:  now,
			TTL:        ttl,
		}
		items = append(items, types.TransactWriteItem{
			Put: &types.Put{
				TableName:           aws.String(r.tableName),
				ConditionExpression: aws.String("attribute_not_exists(pk)"),
				Item: map[string]types.AttributeValue{
					dynamo.AttrPK:  &types.AttributeValueMemberS{Value: changeRecord.PK()},
					dynamo.AttrSK:  &types.AttributeValueMemberS{Value: changeRecord.SK()},
					AttrObjectID:   &types.AttributeValueMemberS{Value: objectID},
					AttrChangeType: &types.AttributeValueMemberS{Value: string(changeType)},
					AttrTimestamp:  &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
					AttrState:      &types.AttributeValueMemberN{Value: strconv.FormatInt(seqState, 10)},
					AttrTTL:        &types.AttributeValueMemberN{Value: strconv.FormatInt(ttl, 10)},
				},
			},
		})
	}

	return newState, items
}

// BuildChangeLogItem returns a single Put transaction item that logs a change
// for the given object without incrementing the state counter. This is used when
// multiple objects of the same type are affected in a single transaction and only
// one state increment is needed (via BuildStateChangeItems), but each object
// still needs its own change log entry.
func (r *Repository) BuildChangeLogItem(accountID string, objectType ObjectType, newState int64, objectID string, changeType ChangeType) types.TransactWriteItem {
	now := time.Now().UTC()
	ttl := now.Add(time.Duration(r.retentionDays) * 24 * time.Hour).Unix()

	changeRecord := &ChangeRecord{
		AccountID:  accountID,
		ObjectType: objectType,
		State:      newState,
		ObjectID:   objectID,
		ChangeType: changeType,
		Timestamp:  now,
		TTL:        ttl,
	}

	return types.TransactWriteItem{
		Put: &types.Put{
			TableName:           aws.String(r.tableName),
			ConditionExpression: aws.String("attribute_not_exists(pk)"),
			Item: map[string]types.AttributeValue{
				dynamo.AttrPK:  &types.AttributeValueMemberS{Value: changeRecord.PK()},
				dynamo.AttrSK:  &types.AttributeValueMemberS{Value: changeRecord.SK()},
				AttrObjectID:   &types.AttributeValueMemberS{Value: objectID},
				AttrChangeType: &types.AttributeValueMemberS{Value: string(changeType)},
				AttrTimestamp:  &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
				AttrState:      &types.AttributeValueMemberN{Value: strconv.FormatInt(newState, 10)},
				AttrTTL:        &types.AttributeValueMemberN{Value: strconv.FormatInt(ttl, 10)},
			},
		},
	}
}

// QueryChanges retrieves change log entries since a given state.
func (r *Repository) QueryChanges(ctx context.Context, accountID string, objectType ObjectType, sinceState int64, maxChanges int) ([]ChangeRecord, error) {
	pk := dynamo.PrefixAccount + accountID
	// Start from sinceState + 1 (we want changes AFTER sinceState)
	skStart := fmt.Sprintf("%s%s#%010d", PrefixChange, objectType, sinceState+1)
	// End at max possible state for this type
	skEnd := fmt.Sprintf("%s%s#%010d", PrefixChange, objectType, MaxStateValue)

	queryInput := &dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		KeyConditionExpression: aws.String(dynamo.AttrPK + " = :pk AND " + dynamo.AttrSK + " BETWEEN :skStart AND :skEnd"),
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

		if v, ok := item[AttrObjectID].(*types.AttributeValueMemberS); ok {
			record.ObjectID = v.Value
		}
		if v, ok := item[AttrChangeType].(*types.AttributeValueMemberS); ok {
			record.ChangeType = ChangeType(v.Value)
		}
		if v, ok := item[AttrTimestamp].(*types.AttributeValueMemberS); ok {
			if t, err := time.Parse(time.RFC3339, v.Value); err == nil {
				record.Timestamp = t
			}
		}
		if v, ok := item[AttrState].(*types.AttributeValueMemberN); ok {
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
	pk := dynamo.PrefixAccount + accountID
	skPrefix := PrefixChange + string(objectType) + "#"

	output, err := r.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		KeyConditionExpression: aws.String(dynamo.AttrPK + " = :pk AND begins_with(" + dynamo.AttrSK + ", :skPrefix)"),
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

	if v, ok := output.Items[0][AttrState].(*types.AttributeValueMemberN); ok {
		state, err := strconv.ParseInt(v.Value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse state: %w", err)
		}
		return state, nil
	}

	return 0, nil
}
