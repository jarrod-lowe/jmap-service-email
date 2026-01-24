package mailbox

// Key prefix for DynamoDB sort key.
const (
	PrefixMailbox = "MAILBOX#"
)

// Attribute names for DynamoDB items.
const (
	AttrMailboxID    = "mailboxId"
	AttrAccountID    = "accountId"
	AttrName         = "name"
	AttrRole         = "role"
	AttrSortOrder    = "sortOrder"
	AttrTotalEmails  = "totalEmails"
	AttrUnreadEmails = "unreadEmails"
	AttrIsSubscribed = "isSubscribed"
	AttrCreatedAt    = "createdAt"
	AttrUpdatedAt    = "updatedAt"
)
