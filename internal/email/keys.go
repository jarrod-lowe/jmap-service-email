package email

// Key prefixes for DynamoDB sort keys.
const (
	PrefixEmail  = "EMAIL#"
	PrefixMbox   = "MBOX#"
	PrefixMsgID  = "MSGID#"
	PrefixRcvd   = "RCVD#"
	PrefixThread = "THREAD#"
)

// Attribute names for DynamoDB items.
const (
	AttrEmailID       = "emailId"
	AttrAccountID     = "accountId"
	AttrBlobID        = "blobId"
	AttrThreadID      = "threadId"
	AttrMailboxIDs    = "mailboxIds"
	AttrKeywords      = "keywords"
	AttrReceivedAt    = "receivedAt"
	AttrSize          = "size"
	AttrHasAttachment = "hasAttachment"
	AttrSubject       = "subject"
	AttrFrom          = "from"
	AttrTo            = "to"
	AttrCC            = "cc"
	AttrReplyTo       = "replyTo"
	AttrSentAt        = "sentAt"
	AttrMessageID     = "messageId"
	AttrInReplyTo     = "inReplyTo"
	AttrReferences    = "references"
	AttrPreview       = "preview"
	AttrBodyStructure = "bodyStructure"
	AttrTextBody      = "textBody"
	AttrHTMLBody      = "htmlBody"
	AttrAttachments   = "attachments"
	AttrName          = "name"
	AttrEmail         = "email"
	AttrVersion       = "version"
)
