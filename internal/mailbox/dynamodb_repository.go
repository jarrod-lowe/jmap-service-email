package mailbox

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jarrod-lowe/jmap-service-libs/dbclient"

	"github.com/jarrod-lowe/jmap-service-email/internal/dynamo"
)

// DynamoDBRepository implements Repository using DynamoDB.
type DynamoDBRepository struct {
	client    dbclient.DynamoDBClient
	tableName string
}

// NewDynamoDBRepository creates a new DynamoDBRepository.
func NewDynamoDBRepository(client dbclient.DynamoDBClient, tableName string) *DynamoDBRepository {
	return &DynamoDBRepository{
		client:    client,
		tableName: tableName,
	}
}

// GetMailbox retrieves a single mailbox by ID.
func (r *DynamoDBRepository) GetMailbox(ctx context.Context, accountID, mailboxID string) (*MailboxItem, error) {
	mailbox := &MailboxItem{AccountID: accountID, MailboxID: mailboxID}

	output, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			dynamo.AttrPK: &types.AttributeValueMemberS{Value: mailbox.PK()},
			dynamo.AttrSK: &types.AttributeValueMemberS{Value: mailbox.SK()},
		},
	})
	if err != nil {
		return nil, err
	}

	if output.Item == nil {
		return nil, ErrMailboxNotFound
	}

	return unmarshalMailboxItem(output.Item), nil
}

// GetAllMailboxes retrieves all mailboxes for an account.
func (r *DynamoDBRepository) GetAllMailboxes(ctx context.Context, accountID string) ([]*MailboxItem, error) {
	output, err := r.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		KeyConditionExpression: aws.String(dynamo.AttrPK + " = :pk AND begins_with(" + dynamo.AttrSK + ", :prefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     &types.AttributeValueMemberS{Value: dynamo.PrefixAccount + accountID},
			":prefix": &types.AttributeValueMemberS{Value: PrefixMailbox},
		},
	})
	if err != nil {
		return nil, err
	}

	mailboxes := make([]*MailboxItem, len(output.Items))
	for i, item := range output.Items {
		mailboxes[i] = unmarshalMailboxItem(item)
	}
	return mailboxes, nil
}

// CreateMailbox creates a new mailbox.
func (r *DynamoDBRepository) CreateMailbox(ctx context.Context, mailbox *MailboxItem) error {
	// Check for duplicate role
	if mailbox.Role != "" {
		existing, err := r.GetAllMailboxes(ctx, mailbox.AccountID)
		if err != nil {
			return err
		}
		for _, m := range existing {
			if m.Role == mailbox.Role {
				return ErrRoleAlreadyExists
			}
		}
	}

	item := marshalMailboxItem(mailbox)
	_, err := r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(r.tableName),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(" + dynamo.AttrPK + ")"),
	})
	return err
}

// UpdateMailbox updates an existing mailbox.
func (r *DynamoDBRepository) UpdateMailbox(ctx context.Context, mailbox *MailboxItem) error {
	// Check for duplicate role if role is set
	if mailbox.Role != "" {
		existing, err := r.GetAllMailboxes(ctx, mailbox.AccountID)
		if err != nil {
			return err
		}
		for _, m := range existing {
			if m.Role == mailbox.Role && m.MailboxID != mailbox.MailboxID {
				return ErrRoleAlreadyExists
			}
		}
	}

	updateExpr := "SET #name = :name, " + AttrSortOrder + " = :sortOrder, " + AttrIsSubscribed + " = :isSubscribed, " + AttrUpdatedAt + " = :updatedAt"
	exprAttrNames := map[string]string{
		"#name": AttrName,
	}
	exprAttrValues := map[string]types.AttributeValue{
		":name":         &types.AttributeValueMemberS{Value: mailbox.Name},
		":sortOrder":    &types.AttributeValueMemberN{Value: strconv.Itoa(mailbox.SortOrder)},
		":isSubscribed": &types.AttributeValueMemberBOOL{Value: mailbox.IsSubscribed},
		":updatedAt":    &types.AttributeValueMemberS{Value: mailbox.UpdatedAt.UTC().Format(time.RFC3339)},
	}

	if mailbox.Role != "" {
		updateExpr += ", #role = :role"
		exprAttrNames["#role"] = AttrRole
		exprAttrValues[":role"] = &types.AttributeValueMemberS{Value: mailbox.Role}
	} else {
		updateExpr += " REMOVE #role"
		exprAttrNames["#role"] = AttrRole
	}

	_, err := r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			dynamo.AttrPK: &types.AttributeValueMemberS{Value: mailbox.PK()},
			dynamo.AttrSK: &types.AttributeValueMemberS{Value: mailbox.SK()},
		},
		UpdateExpression:          aws.String(updateExpr),
		ExpressionAttributeNames:  exprAttrNames,
		ExpressionAttributeValues: exprAttrValues,
		ConditionExpression:       aws.String("attribute_exists(" + dynamo.AttrPK + ")"),
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return ErrMailboxNotFound
		}
		return err
	}

	return nil
}

// DeleteMailbox deletes a mailbox by ID.
func (r *DynamoDBRepository) DeleteMailbox(ctx context.Context, accountID, mailboxID string) error {
	mailbox := &MailboxItem{AccountID: accountID, MailboxID: mailboxID}

	_, err := r.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			dynamo.AttrPK: &types.AttributeValueMemberS{Value: mailbox.PK()},
			dynamo.AttrSK: &types.AttributeValueMemberS{Value: mailbox.SK()},
		},
		ConditionExpression: aws.String("attribute_exists(" + dynamo.AttrPK + ")"),
	})
	if err != nil {
		// Check for ConditionalCheckFailedException
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return ErrMailboxNotFound
		}
		return err
	}
	return nil
}

// IncrementCounts increments totalEmails and optionally unreadEmails.
func (r *DynamoDBRepository) IncrementCounts(ctx context.Context, accountID, mailboxID string, incrementUnread bool) error {
	mailbox := &MailboxItem{AccountID: accountID, MailboxID: mailboxID}

	updateExpr := "SET " + AttrTotalEmails + " = " + AttrTotalEmails + " + :one, " + AttrUpdatedAt + " = :updatedAt"
	exprAttrValues := map[string]types.AttributeValue{
		":one":       &types.AttributeValueMemberN{Value: "1"},
		":updatedAt": &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
	}

	if incrementUnread {
		updateExpr = "SET " + AttrTotalEmails + " = " + AttrTotalEmails + " + :one, " + AttrUnreadEmails + " = " + AttrUnreadEmails + " + :one, " + AttrUpdatedAt + " = :updatedAt"
	}

	_, err := r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			dynamo.AttrPK: &types.AttributeValueMemberS{Value: mailbox.PK()},
			dynamo.AttrSK: &types.AttributeValueMemberS{Value: mailbox.SK()},
		},
		UpdateExpression:          aws.String(updateExpr),
		ExpressionAttributeValues: exprAttrValues,
		ConditionExpression:       aws.String("attribute_exists(" + dynamo.AttrPK + ")"),
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return ErrMailboxNotFound
		}
		return err
	}
	return nil
}

// DecrementCounts decrements totalEmails and optionally unreadEmails.
func (r *DynamoDBRepository) DecrementCounts(ctx context.Context, accountID, mailboxID string, decrementUnread bool) error {
	mailbox := &MailboxItem{AccountID: accountID, MailboxID: mailboxID}

	updateExpr := "SET " + AttrTotalEmails + " = " + AttrTotalEmails + " - :one, " + AttrUpdatedAt + " = :updatedAt"
	exprAttrValues := map[string]types.AttributeValue{
		":one":       &types.AttributeValueMemberN{Value: "1"},
		":updatedAt": &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
	}

	if decrementUnread {
		updateExpr = "SET " + AttrTotalEmails + " = " + AttrTotalEmails + " - :one, " + AttrUnreadEmails + " = " + AttrUnreadEmails + " - :one, " + AttrUpdatedAt + " = :updatedAt"
	}

	_, err := r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			dynamo.AttrPK: &types.AttributeValueMemberS{Value: mailbox.PK()},
			dynamo.AttrSK: &types.AttributeValueMemberS{Value: mailbox.SK()},
		},
		UpdateExpression:          aws.String(updateExpr),
		ExpressionAttributeValues: exprAttrValues,
		ConditionExpression:       aws.String("attribute_exists(" + dynamo.AttrPK + ")"),
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return ErrMailboxNotFound
		}
		return err
	}
	return nil
}

// BuildDecrementCountsItems returns a transaction item that decrements totalEmails
// and optionally unreadEmails. The caller includes this in their own transaction.
func (r *DynamoDBRepository) BuildDecrementCountsItems(accountID, mailboxID string, decrementUnread bool) types.TransactWriteItem {
	mbox := &MailboxItem{AccountID: accountID, MailboxID: mailboxID}

	updateExpr := "SET " + AttrTotalEmails + " = " + AttrTotalEmails + " - :one, " + AttrUpdatedAt + " = :updatedAt"
	exprAttrValues := map[string]types.AttributeValue{
		":one":       &types.AttributeValueMemberN{Value: "1"},
		":updatedAt": &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
	}

	if decrementUnread {
		updateExpr = "SET " + AttrTotalEmails + " = " + AttrTotalEmails + " - :one, " + AttrUnreadEmails + " = " + AttrUnreadEmails + " - :one, " + AttrUpdatedAt + " = :updatedAt"
	}

	return types.TransactWriteItem{
		Update: &types.Update{
			TableName: aws.String(r.tableName),
			Key: map[string]types.AttributeValue{
				dynamo.AttrPK: &types.AttributeValueMemberS{Value: mbox.PK()},
				dynamo.AttrSK: &types.AttributeValueMemberS{Value: mbox.SK()},
			},
			UpdateExpression:          aws.String(updateExpr),
			ExpressionAttributeValues: exprAttrValues,
		},
	}
}

// BuildIncrementCountsItems returns a transaction item that increments totalEmails
// and optionally unreadEmails. The caller includes this in their own transaction.
func (r *DynamoDBRepository) BuildIncrementCountsItems(accountID, mailboxID string, incrementUnread bool) types.TransactWriteItem {
	mbox := &MailboxItem{AccountID: accountID, MailboxID: mailboxID}

	updateExpr := "SET " + AttrTotalEmails + " = " + AttrTotalEmails + " + :one, " + AttrUpdatedAt + " = :updatedAt"
	exprAttrValues := map[string]types.AttributeValue{
		":one":       &types.AttributeValueMemberN{Value: "1"},
		":updatedAt": &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
	}

	if incrementUnread {
		updateExpr = "SET " + AttrTotalEmails + " = " + AttrTotalEmails + " + :one, " + AttrUnreadEmails + " = " + AttrUnreadEmails + " + :one, " + AttrUpdatedAt + " = :updatedAt"
	}

	return types.TransactWriteItem{
		Update: &types.Update{
			TableName: aws.String(r.tableName),
			Key: map[string]types.AttributeValue{
				dynamo.AttrPK: &types.AttributeValueMemberS{Value: mbox.PK()},
				dynamo.AttrSK: &types.AttributeValueMemberS{Value: mbox.SK()},
			},
			UpdateExpression:          aws.String(updateExpr),
			ExpressionAttributeValues: exprAttrValues,
		},
	}
}

// BuildCreateMailboxItem returns a transaction item that creates a new mailbox.
// The caller includes this in their own transaction.
// Note: This does NOT include the role uniqueness check - the caller must perform
// that check before building the transaction.
func (r *DynamoDBRepository) BuildCreateMailboxItem(mbox *MailboxItem) types.TransactWriteItem {
	item := marshalMailboxItem(mbox)
	return types.TransactWriteItem{
		Put: &types.Put{
			TableName:           aws.String(r.tableName),
			Item:                item,
			ConditionExpression: aws.String("attribute_not_exists(" + dynamo.AttrPK + ")"),
		},
	}
}

// BuildUpdateMailboxItem returns a transaction item that updates an existing mailbox.
// The caller includes this in their own transaction.
// Note: This does NOT include the role uniqueness check - the caller must perform
// that check before building the transaction.
func (r *DynamoDBRepository) BuildUpdateMailboxItem(mbox *MailboxItem) types.TransactWriteItem {
	updateExpr := "SET #name = :name, " + AttrSortOrder + " = :sortOrder, " + AttrIsSubscribed + " = :isSubscribed, " + AttrUpdatedAt + " = :updatedAt"
	exprAttrNames := map[string]string{
		"#name": AttrName,
	}
	exprAttrValues := map[string]types.AttributeValue{
		":name":         &types.AttributeValueMemberS{Value: mbox.Name},
		":sortOrder":    &types.AttributeValueMemberN{Value: strconv.Itoa(mbox.SortOrder)},
		":isSubscribed": &types.AttributeValueMemberBOOL{Value: mbox.IsSubscribed},
		":updatedAt":    &types.AttributeValueMemberS{Value: mbox.UpdatedAt.UTC().Format(time.RFC3339)},
	}

	if mbox.Role != "" {
		updateExpr += ", #role = :role"
		exprAttrNames["#role"] = AttrRole
		exprAttrValues[":role"] = &types.AttributeValueMemberS{Value: mbox.Role}
	} else {
		updateExpr += " REMOVE #role"
		exprAttrNames["#role"] = AttrRole
	}

	return types.TransactWriteItem{
		Update: &types.Update{
			TableName: aws.String(r.tableName),
			Key: map[string]types.AttributeValue{
				dynamo.AttrPK: &types.AttributeValueMemberS{Value: mbox.PK()},
				dynamo.AttrSK: &types.AttributeValueMemberS{Value: mbox.SK()},
			},
			UpdateExpression:          aws.String(updateExpr),
			ExpressionAttributeNames:  exprAttrNames,
			ExpressionAttributeValues: exprAttrValues,
			ConditionExpression:       aws.String("attribute_exists(" + dynamo.AttrPK + ")"),
		},
	}
}

// BuildDeleteMailboxItem returns a transaction item that deletes a mailbox.
// The caller includes this in their own transaction.
func (r *DynamoDBRepository) BuildDeleteMailboxItem(accountID, mailboxID string) types.TransactWriteItem {
	mbox := &MailboxItem{AccountID: accountID, MailboxID: mailboxID}

	return types.TransactWriteItem{
		Delete: &types.Delete{
			TableName: aws.String(r.tableName),
			Key: map[string]types.AttributeValue{
				dynamo.AttrPK: &types.AttributeValueMemberS{Value: mbox.PK()},
				dynamo.AttrSK: &types.AttributeValueMemberS{Value: mbox.SK()},
			},
			ConditionExpression: aws.String("attribute_exists(" + dynamo.AttrPK + ")"),
		},
	}
}

// MailboxExists checks if a mailbox exists.
func (r *DynamoDBRepository) MailboxExists(ctx context.Context, accountID, mailboxID string) (bool, error) {
	mailbox := &MailboxItem{AccountID: accountID, MailboxID: mailboxID}

	output, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			dynamo.AttrPK: &types.AttributeValueMemberS{Value: mailbox.PK()},
			dynamo.AttrSK: &types.AttributeValueMemberS{Value: mailbox.SK()},
		},
		ProjectionExpression: aws.String(dynamo.AttrPK),
	})
	if err != nil {
		return false, err
	}

	return output.Item != nil, nil
}

// marshalMailboxItem converts a MailboxItem to DynamoDB attribute values.
func marshalMailboxItem(mailbox *MailboxItem) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		dynamo.AttrPK:    &types.AttributeValueMemberS{Value: mailbox.PK()},
		dynamo.AttrSK:    &types.AttributeValueMemberS{Value: mailbox.SK()},
		AttrMailboxID:    &types.AttributeValueMemberS{Value: mailbox.MailboxID},
		AttrAccountID:    &types.AttributeValueMemberS{Value: mailbox.AccountID},
		AttrName:         &types.AttributeValueMemberS{Value: mailbox.Name},
		AttrSortOrder:    &types.AttributeValueMemberN{Value: strconv.Itoa(mailbox.SortOrder)},
		AttrTotalEmails:  &types.AttributeValueMemberN{Value: strconv.Itoa(mailbox.TotalEmails)},
		AttrUnreadEmails: &types.AttributeValueMemberN{Value: strconv.Itoa(mailbox.UnreadEmails)},
		AttrIsSubscribed: &types.AttributeValueMemberBOOL{Value: mailbox.IsSubscribed},
		AttrCreatedAt:    &types.AttributeValueMemberS{Value: mailbox.CreatedAt.UTC().Format(time.RFC3339)},
		AttrUpdatedAt:    &types.AttributeValueMemberS{Value: mailbox.UpdatedAt.UTC().Format(time.RFC3339)},
	}

	if mailbox.Role != "" {
		item[AttrRole] = &types.AttributeValueMemberS{Value: mailbox.Role}
	}

	return item
}

// unmarshalMailboxItem converts DynamoDB attribute values to a MailboxItem.
func unmarshalMailboxItem(item map[string]types.AttributeValue) *MailboxItem {
	mailbox := &MailboxItem{}

	if v, ok := item[AttrMailboxID].(*types.AttributeValueMemberS); ok {
		mailbox.MailboxID = v.Value
	}
	if v, ok := item[AttrAccountID].(*types.AttributeValueMemberS); ok {
		mailbox.AccountID = v.Value
	}
	if v, ok := item[AttrName].(*types.AttributeValueMemberS); ok {
		mailbox.Name = v.Value
	}
	if v, ok := item[AttrRole].(*types.AttributeValueMemberS); ok {
		mailbox.Role = v.Value
	}
	if v, ok := item[AttrSortOrder].(*types.AttributeValueMemberN); ok {
		if n, err := strconv.Atoi(v.Value); err == nil {
			mailbox.SortOrder = n
		}
	}
	if v, ok := item[AttrTotalEmails].(*types.AttributeValueMemberN); ok {
		if n, err := strconv.Atoi(v.Value); err == nil {
			mailbox.TotalEmails = n
		}
	}
	if v, ok := item[AttrUnreadEmails].(*types.AttributeValueMemberN); ok {
		if n, err := strconv.Atoi(v.Value); err == nil {
			mailbox.UnreadEmails = n
		}
	}
	if v, ok := item[AttrIsSubscribed].(*types.AttributeValueMemberBOOL); ok {
		mailbox.IsSubscribed = v.Value
	}
	if v, ok := item[AttrCreatedAt].(*types.AttributeValueMemberS); ok {
		if t, err := time.Parse(time.RFC3339, v.Value); err == nil {
			mailbox.CreatedAt = t
		}
	}
	if v, ok := item[AttrUpdatedAt].(*types.AttributeValueMemberS); ok {
		if t, err := time.Parse(time.RFC3339, v.Value); err == nil {
			mailbox.UpdatedAt = t
		}
	}

	return mailbox
}
