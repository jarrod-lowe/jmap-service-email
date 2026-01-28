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

	"github.com/jarrod-lowe/jmap-service-email/internal/dynamo"
	"github.com/jarrod-lowe/jmap-service-email/internal/mailbox"
)

// Error types for repository operations.
var (
	ErrTransactionFailed = errors.New("transaction failed")
	ErrEmailNotFound     = errors.New("email not found")
	ErrVersionConflict   = errors.New("version conflict")
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
	transactItems := r.BuildCreateEmailItems(email)

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
	pk := dynamo.PrefixAccount + accountID

	// Determine query parameters based on filter
	var queryInput *dynamodb.QueryInput
	if req.Filter != nil && req.Filter.InMailbox != "" {
		// Query mailbox membership items
		skPrefix := PrefixMbox + req.Filter.InMailbox + "#" + PrefixEmail
		queryInput = &dynamodb.QueryInput{
			TableName:              aws.String(r.tableName),
			KeyConditionExpression: aws.String(dynamo.AttrPK + " = :pk AND begins_with(" + dynamo.AttrSK + ", :skPrefix)"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":       &types.AttributeValueMemberS{Value: pk},
				":skPrefix": &types.AttributeValueMemberS{Value: skPrefix},
			},
		}
	} else {
		// Query all emails using LSI
		queryInput = &dynamodb.QueryInput{
			TableName:              aws.String(r.tableName),
			IndexName:              aws.String(dynamo.IndexLSI1),
			KeyConditionExpression: aws.String(dynamo.AttrPK + " = :pk AND begins_with(" + dynamo.AttrLSI1SK + ", :lsiPrefix)"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":        &types.AttributeValueMemberS{Value: pk},
				":lsiPrefix": &types.AttributeValueMemberS{Value: PrefixRcvd},
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
		if emailID, ok := item[AttrEmailID].(*types.AttributeValueMemberS); ok {
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
	pk := dynamo.PrefixAccount + accountID
	lsi2sk := PrefixMsgID + messageID

	output, err := r.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		IndexName:              aws.String(dynamo.IndexLSI2),
		KeyConditionExpression: aws.String(dynamo.AttrPK + " = :pk AND " + dynamo.AttrLSI2SK + " = :lsi2sk"),
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
	if v, ok := item[AttrEmailID].(*types.AttributeValueMemberS); ok {
		email.EmailID = v.Value
	}
	if v, ok := item[AttrThreadID].(*types.AttributeValueMemberS); ok {
		email.ThreadID = v.Value
	}

	return email, nil
}

// FindByThreadID finds all emails in a thread, sorted by receivedAt ascending.
func (r *Repository) FindByThreadID(ctx context.Context, accountID, threadID string) ([]*EmailItem, error) {
	pk := dynamo.PrefixAccount + accountID
	threadPrefix := PrefixThread + threadID + "#" + PrefixRcvd

	output, err := r.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		IndexName:              aws.String(dynamo.IndexLSI3),
		KeyConditionExpression: aws.String(dynamo.AttrPK + " = :pk AND begins_with(" + dynamo.AttrLSI3SK + ", :threadPrefix)"),
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
		if v, ok := item[AttrEmailID].(*types.AttributeValueMemberS); ok {
			email.EmailID = v.Value
		}
		if v, ok := item[AttrThreadID].(*types.AttributeValueMemberS); ok {
			email.ThreadID = v.Value
		}
		if v, ok := item[AttrReceivedAt].(*types.AttributeValueMemberS); ok {
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
			dynamo.AttrPK: &types.AttributeValueMemberS{Value: email.PK()},
			dynamo.AttrSK: &types.AttributeValueMemberS{Value: email.SK()},
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
		dynamo.AttrPK:     &types.AttributeValueMemberS{Value: email.PK()},
		dynamo.AttrSK:     &types.AttributeValueMemberS{Value: email.SK()},
		dynamo.AttrLSI1SK: &types.AttributeValueMemberS{Value: email.LSI1SK()},
		AttrEmailID:       &types.AttributeValueMemberS{Value: email.EmailID},
		AttrAccountID:     &types.AttributeValueMemberS{Value: email.AccountID},
		AttrBlobID:        &types.AttributeValueMemberS{Value: email.BlobID},
		AttrThreadID:      &types.AttributeValueMemberS{Value: email.ThreadID},
		AttrSubject:       &types.AttributeValueMemberS{Value: email.Subject},
		AttrReceivedAt:    &types.AttributeValueMemberS{Value: email.ReceivedAt.UTC().Format(time.RFC3339)},
		AttrSize:          &types.AttributeValueMemberN{Value: strconv.FormatInt(email.Size, 10)},
		AttrHasAttachment: &types.AttributeValueMemberBOOL{Value: email.HasAttachment},
		AttrPreview:       &types.AttributeValueMemberS{Value: email.Preview},
	}

	// LSI2 key for Message-ID lookup (only if Message-ID is present)
	if lsi2sk := email.LSI2SK(); lsi2sk != "" {
		item[dynamo.AttrLSI2SK] = &types.AttributeValueMemberS{Value: lsi2sk}
	}

	// LSI3 key for thread queries (only if ThreadID is present)
	if lsi3sk := email.LSI3SK(); lsi3sk != "" {
		item[dynamo.AttrLSI3SK] = &types.AttributeValueMemberS{Value: lsi3sk}
	}

	// MailboxIDs
	if len(email.MailboxIDs) > 0 {
		mailboxMap := make(map[string]types.AttributeValue)
		for k, v := range email.MailboxIDs {
			mailboxMap[k] = &types.AttributeValueMemberBOOL{Value: v}
		}
		item[AttrMailboxIDs] = &types.AttributeValueMemberM{Value: mailboxMap}
	}

	// Keywords
	if len(email.Keywords) > 0 {
		keywordMap := make(map[string]types.AttributeValue)
		for k, v := range email.Keywords {
			keywordMap[k] = &types.AttributeValueMemberBOOL{Value: v}
		}
		item[AttrKeywords] = &types.AttributeValueMemberM{Value: keywordMap}
	}

	// From addresses
	if len(email.From) > 0 {
		item[AttrFrom] = marshalAddressList(email.From)
	}

	// Sender addresses
	if len(email.Sender) > 0 {
		item[AttrSender] = marshalAddressList(email.Sender)
	}

	// To addresses
	if len(email.To) > 0 {
		item[AttrTo] = marshalAddressList(email.To)
	}

	// CC addresses
	if len(email.CC) > 0 {
		item[AttrCC] = marshalAddressList(email.CC)
	}

	// Bcc addresses
	if len(email.Bcc) > 0 {
		item[AttrBcc] = marshalAddressList(email.Bcc)
	}

	// ReplyTo addresses
	if len(email.ReplyTo) > 0 {
		item[AttrReplyTo] = marshalAddressList(email.ReplyTo)
	}

	// SentAt
	if !email.SentAt.IsZero() {
		item[AttrSentAt] = &types.AttributeValueMemberS{Value: email.SentAt.UTC().Format(time.RFC3339)}
	}

	// MessageID
	if len(email.MessageID) > 0 {
		item[AttrMessageID] = marshalStringList(email.MessageID)
	}

	// InReplyTo
	if len(email.InReplyTo) > 0 {
		item[AttrInReplyTo] = marshalStringList(email.InReplyTo)
	}

	// References
	if len(email.References) > 0 {
		item[AttrReferences] = marshalStringList(email.References)
	}

	// TextBody
	if len(email.TextBody) > 0 {
		item[AttrTextBody] = marshalStringList(email.TextBody)
	}

	// HTMLBody
	if len(email.HTMLBody) > 0 {
		item[AttrHTMLBody] = marshalStringList(email.HTMLBody)
	}

	// Attachments
	if len(email.Attachments) > 0 {
		item[AttrAttachments] = marshalStringList(email.Attachments)
	}

	// BodyStructure - serialize as JSON string for simplicity
	if email.BodyStructure.PartID != "" {
		bodyStructureJSON, _ := json.Marshal(email.BodyStructure)
		item[AttrBodyStructure] = &types.AttributeValueMemberS{Value: string(bodyStructureJSON)}
	}

	// HeaderSize (for header:* property retrieval)
	if email.HeaderSize > 0 {
		item[AttrHeaderSize] = &types.AttributeValueMemberN{Value: strconv.FormatInt(email.HeaderSize, 10)}
	}

	// Version
	item[AttrVersion] = &types.AttributeValueMemberN{Value: strconv.Itoa(email.Version)}

	return item
}

// marshalMembershipItem converts a MailboxMembershipItem to DynamoDB attribute values.
func (r *Repository) marshalMembershipItem(membership *MailboxMembershipItem) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		dynamo.AttrPK: &types.AttributeValueMemberS{Value: membership.PK()},
		dynamo.AttrSK: &types.AttributeValueMemberS{Value: membership.SK()},
		AttrEmailID:   &types.AttributeValueMemberS{Value: membership.EmailID},
	}
}

// unmarshalEmailItem converts DynamoDB attribute values to an EmailItem.
func (r *Repository) unmarshalEmailItem(item map[string]types.AttributeValue) (*EmailItem, error) {
	email := &EmailItem{}

	if v, ok := item[AttrEmailID].(*types.AttributeValueMemberS); ok {
		email.EmailID = v.Value
	}
	if v, ok := item[AttrAccountID].(*types.AttributeValueMemberS); ok {
		email.AccountID = v.Value
	}
	if v, ok := item[AttrBlobID].(*types.AttributeValueMemberS); ok {
		email.BlobID = v.Value
	}
	if v, ok := item[AttrThreadID].(*types.AttributeValueMemberS); ok {
		email.ThreadID = v.Value
	}
	if v, ok := item[AttrSubject].(*types.AttributeValueMemberS); ok {
		email.Subject = v.Value
	}
	if v, ok := item[AttrReceivedAt].(*types.AttributeValueMemberS); ok {
		if t, err := time.Parse(time.RFC3339, v.Value); err == nil {
			email.ReceivedAt = t
		}
	}
	if v, ok := item[AttrSize].(*types.AttributeValueMemberN); ok {
		if n, err := strconv.ParseInt(v.Value, 10, 64); err == nil {
			email.Size = n
		}
	}
	if v, ok := item[AttrHeaderSize].(*types.AttributeValueMemberN); ok {
		if n, err := strconv.ParseInt(v.Value, 10, 64); err == nil {
			email.HeaderSize = n
		}
	}
	if v, ok := item[AttrHasAttachment].(*types.AttributeValueMemberBOOL); ok {
		email.HasAttachment = v.Value
	}
	if v, ok := item[AttrPreview].(*types.AttributeValueMemberS); ok {
		email.Preview = v.Value
	}

	// MailboxIDs
	if v, ok := item[AttrMailboxIDs].(*types.AttributeValueMemberM); ok {
		email.MailboxIDs = make(map[string]bool)
		for k, val := range v.Value {
			if b, ok := val.(*types.AttributeValueMemberBOOL); ok {
				email.MailboxIDs[k] = b.Value
			}
		}
	}

	// Keywords
	if v, ok := item[AttrKeywords].(*types.AttributeValueMemberM); ok {
		email.Keywords = make(map[string]bool)
		for k, val := range v.Value {
			if b, ok := val.(*types.AttributeValueMemberBOOL); ok {
				email.Keywords[k] = b.Value
			}
		}
	}

	// From addresses
	if v, ok := item[AttrFrom].(*types.AttributeValueMemberL); ok {
		email.From = unmarshalAddressList(v.Value)
	}

	// Sender addresses
	if v, ok := item[AttrSender].(*types.AttributeValueMemberL); ok {
		email.Sender = unmarshalAddressList(v.Value)
	}

	// To addresses
	if v, ok := item[AttrTo].(*types.AttributeValueMemberL); ok {
		email.To = unmarshalAddressList(v.Value)
	}

	// CC addresses
	if v, ok := item[AttrCC].(*types.AttributeValueMemberL); ok {
		email.CC = unmarshalAddressList(v.Value)
	}

	// Bcc addresses
	if v, ok := item[AttrBcc].(*types.AttributeValueMemberL); ok {
		email.Bcc = unmarshalAddressList(v.Value)
	}

	// ReplyTo addresses
	if v, ok := item[AttrReplyTo].(*types.AttributeValueMemberL); ok {
		email.ReplyTo = unmarshalAddressList(v.Value)
	}

	// SentAt
	if v, ok := item[AttrSentAt].(*types.AttributeValueMemberS); ok {
		if t, err := time.Parse(time.RFC3339, v.Value); err == nil {
			email.SentAt = t
		}
	}

	// MessageID
	if v, ok := item[AttrMessageID].(*types.AttributeValueMemberL); ok {
		email.MessageID = unmarshalStringList(v.Value)
	}

	// InReplyTo
	if v, ok := item[AttrInReplyTo].(*types.AttributeValueMemberL); ok {
		email.InReplyTo = unmarshalStringList(v.Value)
	}

	// References
	if v, ok := item[AttrReferences].(*types.AttributeValueMemberL); ok {
		email.References = unmarshalStringList(v.Value)
	}

	// TextBody
	if v, ok := item[AttrTextBody].(*types.AttributeValueMemberL); ok {
		email.TextBody = unmarshalStringList(v.Value)
	}

	// HTMLBody
	if v, ok := item[AttrHTMLBody].(*types.AttributeValueMemberL); ok {
		email.HTMLBody = unmarshalStringList(v.Value)
	}

	// Attachments
	if v, ok := item[AttrAttachments].(*types.AttributeValueMemberL); ok {
		email.Attachments = unmarshalStringList(v.Value)
	}

	// BodyStructure - deserialize from JSON string
	if v, ok := item[AttrBodyStructure].(*types.AttributeValueMemberS); ok {
		_ = json.Unmarshal([]byte(v.Value), &email.BodyStructure)
	}

	// Version
	if v, ok := item[AttrVersion].(*types.AttributeValueMemberN); ok {
		if n, err := strconv.Atoi(v.Value); err == nil {
			email.Version = n
		}
	}

	return email, nil
}

// marshalAddressList converts a slice of EmailAddress to DynamoDB list attribute.
func marshalAddressList(addrs []EmailAddress) types.AttributeValue {
	list := make([]types.AttributeValue, len(addrs))
	for i, addr := range addrs {
		list[i] = &types.AttributeValueMemberM{
			Value: map[string]types.AttributeValue{
				AttrName:  &types.AttributeValueMemberS{Value: addr.Name},
				AttrEmail: &types.AttributeValueMemberS{Value: addr.Email},
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
			if v, ok := m.Value[AttrName].(*types.AttributeValueMemberS); ok {
				addr.Name = v.Value
			}
			if v, ok := m.Value[AttrEmail].(*types.AttributeValueMemberS); ok {
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

// BuildCreateEmailItems returns transaction items to create an email and all its
// mailbox membership records. The caller includes these in their own transaction.
func (r *Repository) BuildCreateEmailItems(email *EmailItem) []types.TransactWriteItem {
	items := make([]types.TransactWriteItem, 0, 1+len(email.MailboxIDs))

	// Email item
	emailItem := r.marshalEmailItem(email)
	items = append(items, types.TransactWriteItem{
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
		items = append(items, types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String(r.tableName),
				Item:      membershipItem,
			},
		})
	}

	return items
}

// BuildDeleteEmailItems returns transaction items to delete an email and all its
// mailbox membership records. The caller includes these in their own transaction.
func (r *Repository) BuildDeleteEmailItems(emailItem *EmailItem) []types.TransactWriteItem {
	items := make([]types.TransactWriteItem, 0, 1+len(emailItem.MailboxIDs))

	// Delete email item with version condition
	items = append(items, types.TransactWriteItem{
		Delete: &types.Delete{
			TableName: aws.String(r.tableName),
			Key: map[string]types.AttributeValue{
				dynamo.AttrPK: &types.AttributeValueMemberS{Value: emailItem.PK()},
				dynamo.AttrSK: &types.AttributeValueMemberS{Value: emailItem.SK()},
			},
			ConditionExpression: aws.String("attribute_exists(" + dynamo.AttrPK + ") AND " + AttrVersion + " = :expectedVersion"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":expectedVersion": &types.AttributeValueMemberN{Value: strconv.Itoa(emailItem.Version)},
			},
		},
	})

	// Delete mailbox membership items
	for mailboxID := range emailItem.MailboxIDs {
		membership := &MailboxMembershipItem{
			AccountID:  emailItem.AccountID,
			MailboxID:  mailboxID,
			ReceivedAt: emailItem.ReceivedAt,
			EmailID:    emailItem.EmailID,
		}
		items = append(items, types.TransactWriteItem{
			Delete: &types.Delete{
				TableName: aws.String(r.tableName),
				Key: map[string]types.AttributeValue{
					dynamo.AttrPK: &types.AttributeValueMemberS{Value: membership.PK()},
					dynamo.AttrSK: &types.AttributeValueMemberS{Value: membership.SK()},
				},
			},
		})
	}

	return items
}

// BuildUpdateEmailMailboxesItems returns transaction items to update an email's mailbox
// memberships, plus the old mailbox IDs for counter calculations.
// The caller fetches the email beforehand and passes it in.
func (r *Repository) BuildUpdateEmailMailboxesItems(emailItem *EmailItem, newMailboxIDs map[string]bool) (addedMailboxes []string, removedMailboxes []string, items []types.TransactWriteItem) {
	oldMailboxIDs := emailItem.MailboxIDs

	// Calculate added and removed
	for mailboxID := range newMailboxIDs {
		if !oldMailboxIDs[mailboxID] {
			addedMailboxes = append(addedMailboxes, mailboxID)
		}
	}
	for mailboxID := range oldMailboxIDs {
		if !newMailboxIDs[mailboxID] {
			removedMailboxes = append(removedMailboxes, mailboxID)
		}
	}

	// Build items (same logic as UpdateEmailMailboxes lines 666-714)
	items = make([]types.TransactWriteItem, 0, 1+len(addedMailboxes)+len(removedMailboxes))

	// Update email's mailboxIds (PUT with condition)
	updatedEmail := *emailItem // copy
	updatedEmail.MailboxIDs = newMailboxIDs
	emailAttr := r.marshalEmailItem(&updatedEmail)
	items = append(items, types.TransactWriteItem{
		Put: &types.Put{
			TableName:           aws.String(r.tableName),
			Item:                emailAttr,
			ConditionExpression: aws.String("attribute_exists(" + dynamo.AttrPK + ")"),
		},
	})

	// Add new membership items
	for _, mailboxID := range addedMailboxes {
		membership := &MailboxMembershipItem{
			AccountID:  emailItem.AccountID,
			MailboxID:  mailboxID,
			ReceivedAt: emailItem.ReceivedAt,
			EmailID:    emailItem.EmailID,
		}
		items = append(items, types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String(r.tableName),
				Item:      r.marshalMembershipItem(membership),
			},
		})
	}

	// Delete removed membership items
	for _, mailboxID := range removedMailboxes {
		membership := &MailboxMembershipItem{
			AccountID:  emailItem.AccountID,
			MailboxID:  mailboxID,
			ReceivedAt: emailItem.ReceivedAt,
			EmailID:    emailItem.EmailID,
		}
		items = append(items, types.TransactWriteItem{
			Delete: &types.Delete{
				TableName: aws.String(r.tableName),
				Key: map[string]types.AttributeValue{
					dynamo.AttrPK: &types.AttributeValueMemberS{Value: membership.PK()},
					dynamo.AttrSK: &types.AttributeValueMemberS{Value: membership.SK()},
				},
			},
		})
	}

	return
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
			ConditionExpression: aws.String("attribute_exists(" + dynamo.AttrPK + ")"),
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
					dynamo.AttrPK: &types.AttributeValueMemberS{Value: membership.PK()},
					dynamo.AttrSK: &types.AttributeValueMemberS{Value: membership.SK()},
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

// UpdateEmailKeywords updates an email's keywords with optimistic locking.
// Returns ErrVersionConflict if expectedVersion doesn't match.
// Updates mailbox unreadEmails counters when $seen changes.
func (r *Repository) UpdateEmailKeywords(ctx context.Context, accountID, emailID string,
	newKeywords map[string]bool, expectedVersion int) (*EmailItem, error) {
	// Fetch existing email
	email, err := r.GetEmail(ctx, accountID, emailID)
	if err != nil {
		return nil, err
	}

	// Check if $seen changed
	oldSeen := email.Keywords["$seen"]
	newSeen := newKeywords["$seen"]
	seenChanged := oldSeen != newSeen

	// Update email with new keywords and increment version
	email.Keywords = newKeywords
	email.Version = expectedVersion + 1

	// Build transaction items
	transactItems := make([]types.TransactWriteItem, 0, 1+len(email.MailboxIDs))

	// Email update with version condition
	emailItem := r.marshalEmailItem(email)
	transactItems = append(transactItems, types.TransactWriteItem{
		Put: &types.Put{
			TableName:           aws.String(r.tableName),
			Item:                emailItem,
			ConditionExpression: aws.String(AttrVersion + " = :expectedVersion"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":expectedVersion": &types.AttributeValueMemberN{Value: strconv.Itoa(expectedVersion)},
			},
		},
	})

	// If $seen changed, update mailbox unread counters
	if seenChanged {
		// delta is -1 if marking as seen (decrement unread), +1 if marking as unseen (increment unread)
		delta := 1
		if newSeen {
			delta = -1
		}

		for mailboxID := range email.MailboxIDs {
			transactItems = append(transactItems, types.TransactWriteItem{
				Update: &types.Update{
					TableName: aws.String(r.tableName),
					Key: map[string]types.AttributeValue{
						dynamo.AttrPK: &types.AttributeValueMemberS{Value: dynamo.PrefixAccount + accountID},
						dynamo.AttrSK: &types.AttributeValueMemberS{Value: mailbox.PrefixMailbox + mailboxID},
					},
					UpdateExpression: aws.String("ADD " + mailbox.AttrUnreadEmails + " :delta"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":delta": &types.AttributeValueMemberN{Value: strconv.Itoa(delta)},
					},
				},
			})
		}
	}

	// Execute transaction
	_, err = r.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		// Check for version conflict
		var txCanceled *types.TransactionCanceledException
		if errors.As(err, &txCanceled) {
			for _, reason := range txCanceled.CancellationReasons {
				if reason.Code != nil && *reason.Code == "ConditionalCheckFailed" {
					return nil, ErrVersionConflict
				}
			}
		}
		return nil, fmt.Errorf("%w: %v", ErrTransactionFailed, err)
	}

	return email, nil
}
