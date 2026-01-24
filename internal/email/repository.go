package email

import (
	"context"
	"encoding/json"
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
	ErrEmailNotFound     = errors.New("email not found")
)

// DynamoDBClient defines the interface for DynamoDB operations.
type DynamoDBClient interface {
	TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	GetItem(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	Query(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

// Repository handles email storage operations.
type Repository struct {
	client    DynamoDBClient
	tableName string
}

// NewRepository creates a new Repository.
func NewRepository(client DynamoDBClient, tableName string) *Repository {
	return &Repository{
		client:    client,
		tableName: tableName,
	}
}

// CreateEmail stores a new email and its mailbox memberships in a transaction.
func (r *Repository) CreateEmail(ctx context.Context, email *EmailItem) error {
	// Build the transaction items
	transactItems := make([]types.TransactWriteItem, 0, 1+len(email.MailboxIDs))

	// Email item
	emailItem := r.marshalEmailItem(email)
	transactItems = append(transactItems, types.TransactWriteItem{
		Put: &types.Put{
			TableName: aws.String(r.tableName),
			Item:      emailItem,
		},
	})

	// Mailbox membership items
	for mailboxID := range email.MailboxIDs {
		membership := &MailboxMembershipItem{
			AccountID:  email.AccountID,
			MailboxID:  mailboxID,
			ReceivedAt: email.ReceivedAt,
			EmailID:    email.EmailID,
		}
		membershipItem := r.marshalMembershipItem(membership)
		transactItems = append(transactItems, types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String(r.tableName),
				Item:      membershipItem,
			},
		})
	}

	// Execute transaction
	_, err := r.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrTransactionFailed, err)
	}

	return nil
}

// QueryEmails queries emails based on the provided request.
// Returns email IDs matching the filter, sorted as requested.
func (r *Repository) QueryEmails(ctx context.Context, accountID string, req *QueryRequest) (*QueryResult, error) {
	pk := fmt.Sprintf("ACCOUNT#%s", accountID)

	// Determine query parameters based on filter
	var queryInput *dynamodb.QueryInput
	if req.Filter != nil && req.Filter.InMailbox != "" {
		// Query mailbox membership items
		skPrefix := fmt.Sprintf("MBOX#%s#EMAIL#", req.Filter.InMailbox)
		queryInput = &dynamodb.QueryInput{
			TableName:              aws.String(r.tableName),
			KeyConditionExpression: aws.String("pk = :pk AND begins_with(sk, :skPrefix)"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":       &types.AttributeValueMemberS{Value: pk},
				":skPrefix": &types.AttributeValueMemberS{Value: skPrefix},
			},
		}
	} else {
		// Query all emails using LSI
		queryInput = &dynamodb.QueryInput{
			TableName:              aws.String(r.tableName),
			IndexName:              aws.String("lsi1"),
			KeyConditionExpression: aws.String("pk = :pk AND begins_with(lsi1sk, :lsiPrefix)"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":        &types.AttributeValueMemberS{Value: pk},
				":lsiPrefix": &types.AttributeValueMemberS{Value: "RCVD#"},
			},
		}
	}

	// Set sort order (default descending for receivedAt)
	scanForward := false
	if len(req.Sort) > 0 && req.Sort[0].IsAscending {
		scanForward = true
	}
	queryInput.ScanIndexForward = aws.Bool(scanForward)

	// Set limit (fetch position + limit to support pagination)
	limit := req.Limit
	if limit <= 0 {
		limit = 25
	}
	queryInput.Limit = aws.Int32(int32(req.Position + limit))

	// Execute query
	output, err := r.client.Query(ctx, queryInput)
	if err != nil {
		return nil, fmt.Errorf("failed to query emails: %w", err)
	}

	// Extract email IDs from results
	allIDs := make([]string, 0, len(output.Items))
	for _, item := range output.Items {
		if emailID, ok := item["emailId"].(*types.AttributeValueMemberS); ok {
			allIDs = append(allIDs, emailID.Value)
		}
	}

	// Apply position-based pagination
	startIdx := req.Position
	if startIdx > len(allIDs) {
		startIdx = len(allIDs)
	}
	endIdx := startIdx + limit
	if endIdx > len(allIDs) {
		endIdx = len(allIDs)
	}

	return &QueryResult{
		IDs:        allIDs[startIdx:endIdx],
		Position:   req.Position,
		QueryState: "", // TODO: implement state tracking
	}, nil
}

// FindByMessageID finds an email by its Message-ID header.
// Returns nil if no email is found with the given Message-ID.
func (r *Repository) FindByMessageID(ctx context.Context, accountID, messageID string) (*EmailItem, error) {
	pk := fmt.Sprintf("ACCOUNT#%s", accountID)
	lsi2sk := fmt.Sprintf("MSGID#%s", messageID)

	output, err := r.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		IndexName:              aws.String("lsi2"),
		KeyConditionExpression: aws.String("pk = :pk AND lsi2sk = :lsi2sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     &types.AttributeValueMemberS{Value: pk},
			":lsi2sk": &types.AttributeValueMemberS{Value: lsi2sk},
		},
		Limit: aws.Int32(1),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query by message ID: %w", err)
	}

	if len(output.Items) == 0 {
		return nil, nil
	}

	// Extract emailId and threadId from the LSI projection
	item := output.Items[0]
	email := &EmailItem{}
	if v, ok := item["emailId"].(*types.AttributeValueMemberS); ok {
		email.EmailID = v.Value
	}
	if v, ok := item["threadId"].(*types.AttributeValueMemberS); ok {
		email.ThreadID = v.Value
	}

	return email, nil
}

// FindByThreadID finds all emails in a thread, sorted by receivedAt ascending.
func (r *Repository) FindByThreadID(ctx context.Context, accountID, threadID string) ([]*EmailItem, error) {
	pk := fmt.Sprintf("ACCOUNT#%s", accountID)
	threadPrefix := fmt.Sprintf("THREAD#%s#RCVD#", threadID)

	output, err := r.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		IndexName:              aws.String("lsi3"),
		KeyConditionExpression: aws.String("pk = :pk AND begins_with(lsi3sk, :threadPrefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":           &types.AttributeValueMemberS{Value: pk},
			":threadPrefix": &types.AttributeValueMemberS{Value: threadPrefix},
		},
		ScanIndexForward: aws.Bool(true), // Ascending order by receivedAt
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query by thread ID: %w", err)
	}

	emails := make([]*EmailItem, 0, len(output.Items))
	for _, item := range output.Items {
		email := &EmailItem{}
		if v, ok := item["emailId"].(*types.AttributeValueMemberS); ok {
			email.EmailID = v.Value
		}
		if v, ok := item["threadId"].(*types.AttributeValueMemberS); ok {
			email.ThreadID = v.Value
		}
		if v, ok := item["receivedAt"].(*types.AttributeValueMemberS); ok {
			if t, err := time.Parse(time.RFC3339, v.Value); err == nil {
				email.ReceivedAt = t
			}
		}
		emails = append(emails, email)
	}

	return emails, nil
}

// GetEmail retrieves an email by account ID and email ID.
func (r *Repository) GetEmail(ctx context.Context, accountID, emailID string) (*EmailItem, error) {
	email := &EmailItem{AccountID: accountID, EmailID: emailID}

	output, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: email.PK()},
			"sk": &types.AttributeValueMemberS{Value: email.SK()},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get email: %w", err)
	}

	if output.Item == nil {
		return nil, ErrEmailNotFound
	}

	return r.unmarshalEmailItem(output.Item)
}

// marshalEmailItem converts an EmailItem to DynamoDB attribute values.
func (r *Repository) marshalEmailItem(email *EmailItem) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		"pk":            &types.AttributeValueMemberS{Value: email.PK()},
		"sk":            &types.AttributeValueMemberS{Value: email.SK()},
		"lsi1sk":        &types.AttributeValueMemberS{Value: email.LSI1SK()},
		"emailId":       &types.AttributeValueMemberS{Value: email.EmailID},
		"accountId":     &types.AttributeValueMemberS{Value: email.AccountID},
		"blobId":        &types.AttributeValueMemberS{Value: email.BlobID},
		"threadId":      &types.AttributeValueMemberS{Value: email.ThreadID},
		"subject":       &types.AttributeValueMemberS{Value: email.Subject},
		"receivedAt":    &types.AttributeValueMemberS{Value: email.ReceivedAt.UTC().Format(time.RFC3339)},
		"size":          &types.AttributeValueMemberN{Value: strconv.FormatInt(email.Size, 10)},
		"hasAttachment": &types.AttributeValueMemberBOOL{Value: email.HasAttachment},
		"preview":       &types.AttributeValueMemberS{Value: email.Preview},
	}

	// LSI2 key for Message-ID lookup (only if Message-ID is present)
	if lsi2sk := email.LSI2SK(); lsi2sk != "" {
		item["lsi2sk"] = &types.AttributeValueMemberS{Value: lsi2sk}
	}

	// LSI3 key for thread queries (only if ThreadID is present)
	if lsi3sk := email.LSI3SK(); lsi3sk != "" {
		item["lsi3sk"] = &types.AttributeValueMemberS{Value: lsi3sk}
	}

	// MailboxIDs
	if len(email.MailboxIDs) > 0 {
		mailboxMap := make(map[string]types.AttributeValue)
		for k, v := range email.MailboxIDs {
			mailboxMap[k] = &types.AttributeValueMemberBOOL{Value: v}
		}
		item["mailboxIds"] = &types.AttributeValueMemberM{Value: mailboxMap}
	}

	// Keywords
	if len(email.Keywords) > 0 {
		keywordMap := make(map[string]types.AttributeValue)
		for k, v := range email.Keywords {
			keywordMap[k] = &types.AttributeValueMemberBOOL{Value: v}
		}
		item["keywords"] = &types.AttributeValueMemberM{Value: keywordMap}
	}

	// From addresses
	if len(email.From) > 0 {
		item["from"] = marshalAddressList(email.From)
	}

	// To addresses
	if len(email.To) > 0 {
		item["to"] = marshalAddressList(email.To)
	}

	// CC addresses
	if len(email.CC) > 0 {
		item["cc"] = marshalAddressList(email.CC)
	}

	// ReplyTo addresses
	if len(email.ReplyTo) > 0 {
		item["replyTo"] = marshalAddressList(email.ReplyTo)
	}

	// SentAt
	if !email.SentAt.IsZero() {
		item["sentAt"] = &types.AttributeValueMemberS{Value: email.SentAt.UTC().Format(time.RFC3339)}
	}

	// MessageID
	if len(email.MessageID) > 0 {
		item["messageId"] = marshalStringList(email.MessageID)
	}

	// InReplyTo
	if len(email.InReplyTo) > 0 {
		item["inReplyTo"] = marshalStringList(email.InReplyTo)
	}

	// References
	if len(email.References) > 0 {
		item["references"] = marshalStringList(email.References)
	}

	// TextBody
	if len(email.TextBody) > 0 {
		item["textBody"] = marshalStringList(email.TextBody)
	}

	// HTMLBody
	if len(email.HTMLBody) > 0 {
		item["htmlBody"] = marshalStringList(email.HTMLBody)
	}

	// Attachments
	if len(email.Attachments) > 0 {
		item["attachments"] = marshalStringList(email.Attachments)
	}

	// BodyStructure - serialize as JSON string for simplicity
	if email.BodyStructure.PartID != "" {
		bodyStructureJSON, _ := json.Marshal(email.BodyStructure)
		item["bodyStructure"] = &types.AttributeValueMemberS{Value: string(bodyStructureJSON)}
	}

	return item
}

// marshalMembershipItem converts a MailboxMembershipItem to DynamoDB attribute values.
func (r *Repository) marshalMembershipItem(membership *MailboxMembershipItem) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk":      &types.AttributeValueMemberS{Value: membership.PK()},
		"sk":      &types.AttributeValueMemberS{Value: membership.SK()},
		"emailId": &types.AttributeValueMemberS{Value: membership.EmailID},
	}
}

// unmarshalEmailItem converts DynamoDB attribute values to an EmailItem.
func (r *Repository) unmarshalEmailItem(item map[string]types.AttributeValue) (*EmailItem, error) {
	email := &EmailItem{}

	if v, ok := item["emailId"].(*types.AttributeValueMemberS); ok {
		email.EmailID = v.Value
	}
	if v, ok := item["accountId"].(*types.AttributeValueMemberS); ok {
		email.AccountID = v.Value
	}
	if v, ok := item["blobId"].(*types.AttributeValueMemberS); ok {
		email.BlobID = v.Value
	}
	if v, ok := item["threadId"].(*types.AttributeValueMemberS); ok {
		email.ThreadID = v.Value
	}
	if v, ok := item["subject"].(*types.AttributeValueMemberS); ok {
		email.Subject = v.Value
	}
	if v, ok := item["receivedAt"].(*types.AttributeValueMemberS); ok {
		if t, err := time.Parse(time.RFC3339, v.Value); err == nil {
			email.ReceivedAt = t
		}
	}
	if v, ok := item["size"].(*types.AttributeValueMemberN); ok {
		if n, err := strconv.ParseInt(v.Value, 10, 64); err == nil {
			email.Size = n
		}
	}
	if v, ok := item["hasAttachment"].(*types.AttributeValueMemberBOOL); ok {
		email.HasAttachment = v.Value
	}
	if v, ok := item["preview"].(*types.AttributeValueMemberS); ok {
		email.Preview = v.Value
	}

	// MailboxIDs
	if v, ok := item["mailboxIds"].(*types.AttributeValueMemberM); ok {
		email.MailboxIDs = make(map[string]bool)
		for k, val := range v.Value {
			if b, ok := val.(*types.AttributeValueMemberBOOL); ok {
				email.MailboxIDs[k] = b.Value
			}
		}
	}

	// Keywords
	if v, ok := item["keywords"].(*types.AttributeValueMemberM); ok {
		email.Keywords = make(map[string]bool)
		for k, val := range v.Value {
			if b, ok := val.(*types.AttributeValueMemberBOOL); ok {
				email.Keywords[k] = b.Value
			}
		}
	}

	// From addresses
	if v, ok := item["from"].(*types.AttributeValueMemberL); ok {
		email.From = unmarshalAddressList(v.Value)
	}

	// To addresses
	if v, ok := item["to"].(*types.AttributeValueMemberL); ok {
		email.To = unmarshalAddressList(v.Value)
	}

	// CC addresses
	if v, ok := item["cc"].(*types.AttributeValueMemberL); ok {
		email.CC = unmarshalAddressList(v.Value)
	}

	// ReplyTo addresses
	if v, ok := item["replyTo"].(*types.AttributeValueMemberL); ok {
		email.ReplyTo = unmarshalAddressList(v.Value)
	}

	// SentAt
	if v, ok := item["sentAt"].(*types.AttributeValueMemberS); ok {
		if t, err := time.Parse(time.RFC3339, v.Value); err == nil {
			email.SentAt = t
		}
	}

	// MessageID
	if v, ok := item["messageId"].(*types.AttributeValueMemberL); ok {
		email.MessageID = unmarshalStringList(v.Value)
	}

	// InReplyTo
	if v, ok := item["inReplyTo"].(*types.AttributeValueMemberL); ok {
		email.InReplyTo = unmarshalStringList(v.Value)
	}

	// References
	if v, ok := item["references"].(*types.AttributeValueMemberL); ok {
		email.References = unmarshalStringList(v.Value)
	}

	// TextBody
	if v, ok := item["textBody"].(*types.AttributeValueMemberL); ok {
		email.TextBody = unmarshalStringList(v.Value)
	}

	// HTMLBody
	if v, ok := item["htmlBody"].(*types.AttributeValueMemberL); ok {
		email.HTMLBody = unmarshalStringList(v.Value)
	}

	// Attachments
	if v, ok := item["attachments"].(*types.AttributeValueMemberL); ok {
		email.Attachments = unmarshalStringList(v.Value)
	}

	// BodyStructure - deserialize from JSON string
	if v, ok := item["bodyStructure"].(*types.AttributeValueMemberS); ok {
		_ = json.Unmarshal([]byte(v.Value), &email.BodyStructure)
	}

	return email, nil
}

// marshalAddressList converts a slice of EmailAddress to DynamoDB list attribute.
func marshalAddressList(addrs []EmailAddress) types.AttributeValue {
	list := make([]types.AttributeValue, len(addrs))
	for i, addr := range addrs {
		list[i] = &types.AttributeValueMemberM{
			Value: map[string]types.AttributeValue{
				"name":  &types.AttributeValueMemberS{Value: addr.Name},
				"email": &types.AttributeValueMemberS{Value: addr.Email},
			},
		}
	}
	return &types.AttributeValueMemberL{Value: list}
}

// unmarshalAddressList converts a DynamoDB list attribute to a slice of EmailAddress.
func unmarshalAddressList(list []types.AttributeValue) []EmailAddress {
	addrs := make([]EmailAddress, 0, len(list))
	for _, item := range list {
		if m, ok := item.(*types.AttributeValueMemberM); ok {
			addr := EmailAddress{}
			if v, ok := m.Value["name"].(*types.AttributeValueMemberS); ok {
				addr.Name = v.Value
			}
			if v, ok := m.Value["email"].(*types.AttributeValueMemberS); ok {
				addr.Email = v.Value
			}
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

// marshalStringList converts a slice of strings to DynamoDB list attribute.
func marshalStringList(strs []string) types.AttributeValue {
	list := make([]types.AttributeValue, len(strs))
	for i, s := range strs {
		list[i] = &types.AttributeValueMemberS{Value: s}
	}
	return &types.AttributeValueMemberL{Value: list}
}

// unmarshalStringList converts a DynamoDB list attribute to a slice of strings.
func unmarshalStringList(list []types.AttributeValue) []string {
	strs := make([]string, 0, len(list))
	for _, item := range list {
		if s, ok := item.(*types.AttributeValueMemberS); ok {
			strs = append(strs, s.Value)
		}
	}
	return strs
}

// UpdateEmailMailboxes updates an email's mailbox memberships in a transaction.
// Returns the old mailboxIds (for counter calculations) and the email item.
func (r *Repository) UpdateEmailMailboxes(ctx context.Context, accountID, emailID string,
	newMailboxIDs map[string]bool) (oldMailboxIDs map[string]bool, email *EmailItem, err error) {
	// Fetch existing email
	email, err = r.GetEmail(ctx, accountID, emailID)
	if err != nil {
		return nil, nil, err
	}

	oldMailboxIDs = make(map[string]bool)
	for k, v := range email.MailboxIDs {
		oldMailboxIDs[k] = v
	}

	// Calculate added and removed mailboxes
	addedMailboxes := make([]string, 0)
	for mailboxID := range newMailboxIDs {
		if !oldMailboxIDs[mailboxID] {
			addedMailboxes = append(addedMailboxes, mailboxID)
		}
	}

	removedMailboxes := make([]string, 0)
	for mailboxID := range oldMailboxIDs {
		if !newMailboxIDs[mailboxID] {
			removedMailboxes = append(removedMailboxes, mailboxID)
		}
	}

	// Build transaction items
	transactItems := make([]types.TransactWriteItem, 0, 1+len(addedMailboxes)+len(removedMailboxes))

	// Update email's mailboxIds
	email.MailboxIDs = newMailboxIDs
	emailItem := r.marshalEmailItem(email)
	transactItems = append(transactItems, types.TransactWriteItem{
		Put: &types.Put{
			TableName:           aws.String(r.tableName),
			Item:                emailItem,
			ConditionExpression: aws.String("attribute_exists(pk)"),
		},
	})

	// Add new membership items
	for _, mailboxID := range addedMailboxes {
		membership := &MailboxMembershipItem{
			AccountID:  accountID,
			MailboxID:  mailboxID,
			ReceivedAt: email.ReceivedAt,
			EmailID:    emailID,
		}
		membershipItem := r.marshalMembershipItem(membership)
		transactItems = append(transactItems, types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String(r.tableName),
				Item:      membershipItem,
			},
		})
	}

	// Delete removed membership items
	for _, mailboxID := range removedMailboxes {
		membership := &MailboxMembershipItem{
			AccountID:  accountID,
			MailboxID:  mailboxID,
			ReceivedAt: email.ReceivedAt,
			EmailID:    emailID,
		}
		transactItems = append(transactItems, types.TransactWriteItem{
			Delete: &types.Delete{
				TableName: aws.String(r.tableName),
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: membership.PK()},
					"sk": &types.AttributeValueMemberS{Value: membership.SK()},
				},
			},
		})
	}

	// Execute transaction
	_, err = r.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrTransactionFailed, err)
	}

	return oldMailboxIDs, email, nil
}
