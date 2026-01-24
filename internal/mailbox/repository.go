package mailbox

import (
	"context"
	"errors"
)

// Error types for repository operations.
var (
	ErrMailboxNotFound   = errors.New("mailbox not found")
	ErrRoleAlreadyExists = errors.New("mailbox with this role already exists")
	ErrTransactionFailed = errors.New("transaction failed")
)

// Repository defines the interface for mailbox storage operations.
type Repository interface {
	GetMailbox(ctx context.Context, accountID, mailboxID string) (*MailboxItem, error)
	GetAllMailboxes(ctx context.Context, accountID string) ([]*MailboxItem, error)
	CreateMailbox(ctx context.Context, mailbox *MailboxItem) error
	UpdateMailbox(ctx context.Context, mailbox *MailboxItem) error
	DeleteMailbox(ctx context.Context, accountID, mailboxID string) error
	IncrementCounts(ctx context.Context, accountID, mailboxID string, incrementUnread bool) error
	DecrementCounts(ctx context.Context, accountID, mailboxID string, decrementUnread bool) error
	MailboxExists(ctx context.Context, accountID, mailboxID string) (bool, error)
}
