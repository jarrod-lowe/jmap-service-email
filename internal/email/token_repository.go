package email

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/jarrod-lowe/jmap-service-email/internal/dynamo"
	"github.com/jarrod-lowe/jmap-service-libs/dbclient"
)

// Token key prefix for address search tokens.
const PrefixTok = "TOK#"

// TokenField identifies which address field a token belongs to.
type TokenField string

const (
	TokenFieldFrom TokenField = "FROM"
	TokenFieldTo   TokenField = "TO"
	TokenFieldCC   TokenField = "CC"
	TokenFieldBcc  TokenField = "BCC"
)

// TokenEntry represents a single address search token in DynamoDB.
type TokenEntry struct {
	AccountID  string
	Field      TokenField
	Token      string
	ReceivedAt time.Time
	EmailID    string
}

// SK returns the DynamoDB sort key for a token entry.
// Format: TOK#FROM#john#RCVD#2024-01-20T10:30:45Z#email-456
func (t *TokenEntry) SK() string {
	return fmt.Sprintf("%s%s#%s#%s%s#%s",
		PrefixTok, t.Field, t.Token, PrefixRcvd,
		t.ReceivedAt.UTC().Format(time.RFC3339), t.EmailID)
}

// TokenRepository handles address token operations.
type TokenRepository struct {
	client    dbclient.DynamoDBClient
	tableName string
}

// NewTokenRepository creates a new TokenRepository.
func NewTokenRepository(client dbclient.DynamoDBClient, tableName string) *TokenRepository {
	return &TokenRepository{
		client:    client,
		tableName: tableName,
	}
}

// WriteTokens writes address token entries for an email.
// Generates tokens from the email's address fields and writes them as DynamoDB items.
func (r *TokenRepository) WriteTokens(ctx context.Context, emailItem *EmailItem) error {
	entries := buildTokenEntries(emailItem)
	for _, entry := range entries {
		_, err := r.client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(r.tableName),
			Item: map[string]types.AttributeValue{
				dynamo.AttrPK: &types.AttributeValueMemberS{Value: dynamo.PrefixAccount + entry.AccountID},
				dynamo.AttrSK: &types.AttributeValueMemberS{Value: entry.SK()},
				AttrEmailID:   &types.AttributeValueMemberS{Value: entry.EmailID},
			},
		})
		if err != nil {
			return fmt.Errorf("put token %s: %w", entry.SK(), err)
		}
	}
	return nil
}

// DeleteTokens deletes all address token entries for an email.
// Re-tokenizes from the email's address fields to determine which tokens to delete.
func (r *TokenRepository) DeleteTokens(ctx context.Context, emailItem *EmailItem) error {
	entries := buildTokenEntries(emailItem)
	for _, entry := range entries {
		_, err := r.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(r.tableName),
			Key: map[string]types.AttributeValue{
				dynamo.AttrPK: &types.AttributeValueMemberS{Value: dynamo.PrefixAccount + entry.AccountID},
				dynamo.AttrSK: &types.AttributeValueMemberS{Value: entry.SK()},
			},
		})
		if err != nil {
			return fmt.Errorf("delete token %s: %w", entry.SK(), err)
		}
	}
	return nil
}

// QueryTokens queries token entries matching a field + prefix combination.
// Returns matching (emailID, receivedAt) pairs, sorted by receivedAt descending.
func (r *TokenRepository) QueryTokens(ctx context.Context, accountID string, field TokenField, tokenPrefix string, limit int32, scanForward bool) ([]TokenQueryResult, error) {
	pk := dynamo.PrefixAccount + accountID
	skPrefix := fmt.Sprintf("%s%s#%s", PrefixTok, field, tokenPrefix)

	output, err := r.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		KeyConditionExpression: aws.String(dynamo.AttrPK + " = :pk AND begins_with(" + dynamo.AttrSK + ", :skPrefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":       &types.AttributeValueMemberS{Value: pk},
			":skPrefix": &types.AttributeValueMemberS{Value: skPrefix},
		},
		ScanIndexForward: aws.Bool(scanForward),
		Limit:            aws.Int32(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("query tokens: %w", err)
	}

	results := make([]TokenQueryResult, 0, len(output.Items))
	for _, item := range output.Items {
		var emailID string
		if v, ok := item[AttrEmailID].(*types.AttributeValueMemberS); ok {
			emailID = v.Value
		}
		results = append(results, TokenQueryResult{EmailID: emailID})
	}

	return results, nil
}

// TokenQueryResult represents a result from a token query.
type TokenQueryResult struct {
	EmailID string
}

// buildTokenEntries creates token entries for all address fields of an email.
func buildTokenEntries(e *EmailItem) []TokenEntry {
	var entries []TokenEntry

	addField := func(field TokenField, addrs []EmailAddress) {
		tokens := TokenizeAddresses(addrs)
		for _, tok := range tokens {
			entries = append(entries, TokenEntry{
				AccountID:  e.AccountID,
				Field:      field,
				Token:      tok,
				ReceivedAt: e.ReceivedAt,
				EmailID:    e.EmailID,
			})
		}
	}

	addField(TokenFieldFrom, e.From)
	addField(TokenFieldTo, e.To)
	addField(TokenFieldCC, e.CC)
	addField(TokenFieldBcc, e.Bcc)

	return entries
}
