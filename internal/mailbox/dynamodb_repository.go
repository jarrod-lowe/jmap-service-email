package mailbox

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DynamoDBClient defines the interface for DynamoDB operations.
type DynamoDBClient interface {
	GetItem(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	Query(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	PutItem(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// DynamoDBRepository implements Repository using DynamoDB.
type DynamoDBRepository struct {
	client    DynamoDBClient
	tableName string
}

// NewDynamoDBRepository creates a new DynamoDBRepository.
func NewDynamoDBRepository(client DynamoDBClient, tableName string) *DynamoDBRepository {
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
			"pk": &types.AttributeValueMemberS{Value: mailbox.PK()},
			"sk": &types.AttributeValueMemberS{Value: mailbox.SK()},
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
		KeyConditionExpression: aws.String("pk = :pk AND begins_with(sk, :prefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     &types.AttributeValueMemberS{Value: "ACCOUNT#" + accountID},
			":prefix": &types.AttributeValueMemberS{Value: "MAILBOX#"},
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
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
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

	updateExpr := "SET #name = :name, sortOrder = :sortOrder, isSubscribed = :isSubscribed, updatedAt = :updatedAt"
	exprAttrNames := map[string]string{
		"#name": "name",
	}
	exprAttrValues := map[string]types.AttributeValue{
		":name":         &types.AttributeValueMemberS{Value: mailbox.Name},
		":sortOrder":    &types.AttributeValueMemberN{Value: strconv.Itoa(mailbox.SortOrder)},
		":isSubscribed": &types.AttributeValueMemberBOOL{Value: mailbox.IsSubscribed},
		":updatedAt":    &types.AttributeValueMemberS{Value: mailbox.UpdatedAt.UTC().Format(time.RFC3339)},
	}

	if mailbox.Role != "" {
		updateExpr += ", #role = :role"
		exprAttrNames["#role"] = "role"
		exprAttrValues[":role"] = &types.AttributeValueMemberS{Value: mailbox.Role}
	} else {
		updateExpr += " REMOVE #role"
		exprAttrNames["#role"] = "role"
	}

	_, err := r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: mailbox.PK()},
			"sk": &types.AttributeValueMemberS{Value: mailbox.SK()},
		},
		UpdateExpression:          aws.String(updateExpr),
		ExpressionAttributeNames:  exprAttrNames,
		ExpressionAttributeValues: exprAttrValues,
		ConditionExpression:       aws.String("attribute_exists(pk)"),
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
			"pk": &types.AttributeValueMemberS{Value: mailbox.PK()},
			"sk": &types.AttributeValueMemberS{Value: mailbox.SK()},
		},
		ConditionExpression: aws.String("attribute_exists(pk)"),
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

	updateExpr := "SET totalEmails = totalEmails + :one, updatedAt = :updatedAt"
	exprAttrValues := map[string]types.AttributeValue{
		":one":       &types.AttributeValueMemberN{Value: "1"},
		":updatedAt": &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
	}

	if incrementUnread {
		updateExpr = "SET totalEmails = totalEmails + :one, unreadEmails = unreadEmails + :one, updatedAt = :updatedAt"
	}

	_, err := r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: mailbox.PK()},
			"sk": &types.AttributeValueMemberS{Value: mailbox.SK()},
		},
		UpdateExpression:          aws.String(updateExpr),
		ExpressionAttributeValues: exprAttrValues,
		ConditionExpression:       aws.String("attribute_exists(pk)"),
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

	updateExpr := "SET totalEmails = totalEmails - :one, updatedAt = :updatedAt"
	exprAttrValues := map[string]types.AttributeValue{
		":one":       &types.AttributeValueMemberN{Value: "1"},
		":updatedAt": &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
	}

	if decrementUnread {
		updateExpr = "SET totalEmails = totalEmails - :one, unreadEmails = unreadEmails - :one, updatedAt = :updatedAt"
	}

	_, err := r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: mailbox.PK()},
			"sk": &types.AttributeValueMemberS{Value: mailbox.SK()},
		},
		UpdateExpression:          aws.String(updateExpr),
		ExpressionAttributeValues: exprAttrValues,
		ConditionExpression:       aws.String("attribute_exists(pk)"),
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

// MailboxExists checks if a mailbox exists.
func (r *DynamoDBRepository) MailboxExists(ctx context.Context, accountID, mailboxID string) (bool, error) {
	mailbox := &MailboxItem{AccountID: accountID, MailboxID: mailboxID}

	output, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: mailbox.PK()},
			"sk": &types.AttributeValueMemberS{Value: mailbox.SK()},
		},
		ProjectionExpression: aws.String("pk"),
	})
	if err != nil {
		return false, err
	}

	return output.Item != nil, nil
}

// marshalMailboxItem converts a MailboxItem to DynamoDB attribute values.
func marshalMailboxItem(mailbox *MailboxItem) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		"pk":           &types.AttributeValueMemberS{Value: mailbox.PK()},
		"sk":           &types.AttributeValueMemberS{Value: mailbox.SK()},
		"mailboxId":    &types.AttributeValueMemberS{Value: mailbox.MailboxID},
		"accountId":    &types.AttributeValueMemberS{Value: mailbox.AccountID},
		"name":         &types.AttributeValueMemberS{Value: mailbox.Name},
		"sortOrder":    &types.AttributeValueMemberN{Value: strconv.Itoa(mailbox.SortOrder)},
		"totalEmails":  &types.AttributeValueMemberN{Value: strconv.Itoa(mailbox.TotalEmails)},
		"unreadEmails": &types.AttributeValueMemberN{Value: strconv.Itoa(mailbox.UnreadEmails)},
		"isSubscribed": &types.AttributeValueMemberBOOL{Value: mailbox.IsSubscribed},
		"createdAt":    &types.AttributeValueMemberS{Value: mailbox.CreatedAt.UTC().Format(time.RFC3339)},
		"updatedAt":    &types.AttributeValueMemberS{Value: mailbox.UpdatedAt.UTC().Format(time.RFC3339)},
	}

	if mailbox.Role != "" {
		item["role"] = &types.AttributeValueMemberS{Value: mailbox.Role}
	}

	return item
}

// unmarshalMailboxItem converts DynamoDB attribute values to a MailboxItem.
func unmarshalMailboxItem(item map[string]types.AttributeValue) *MailboxItem {
	mailbox := &MailboxItem{}

	if v, ok := item["mailboxId"].(*types.AttributeValueMemberS); ok {
		mailbox.MailboxID = v.Value
	}
	if v, ok := item["accountId"].(*types.AttributeValueMemberS); ok {
		mailbox.AccountID = v.Value
	}
	if v, ok := item["name"].(*types.AttributeValueMemberS); ok {
		mailbox.Name = v.Value
	}
	if v, ok := item["role"].(*types.AttributeValueMemberS); ok {
		mailbox.Role = v.Value
	}
	if v, ok := item["sortOrder"].(*types.AttributeValueMemberN); ok {
		if n, err := strconv.Atoi(v.Value); err == nil {
			mailbox.SortOrder = n
		}
	}
	if v, ok := item["totalEmails"].(*types.AttributeValueMemberN); ok {
		if n, err := strconv.Atoi(v.Value); err == nil {
			mailbox.TotalEmails = n
		}
	}
	if v, ok := item["unreadEmails"].(*types.AttributeValueMemberN); ok {
		if n, err := strconv.Atoi(v.Value); err == nil {
			mailbox.UnreadEmails = n
		}
	}
	if v, ok := item["isSubscribed"].(*types.AttributeValueMemberBOOL); ok {
		mailbox.IsSubscribed = v.Value
	}
	if v, ok := item["createdAt"].(*types.AttributeValueMemberS); ok {
		if t, err := time.Parse(time.RFC3339, v.Value); err == nil {
			mailbox.CreatedAt = t
		}
	}
	if v, ok := item["updatedAt"].(*types.AttributeValueMemberS); ok {
		if t, err := time.Parse(time.RFC3339, v.Value); err == nil {
			mailbox.UpdatedAt = t
		}
	}

	return mailbox
}
