package state

// Key prefixes for DynamoDB sort keys.
const (
	PrefixState  = "STATE#"
	PrefixChange = "CHANGE#"
)

// Attribute names for DynamoDB items.
const (
	AttrCurrentState = "currentState"
	AttrUpdatedAt    = "updatedAt"
	AttrObjectID     = "objectId"
	AttrChangeType   = "changeType"
	AttrTimestamp    = "timestamp"
	AttrState        = "state"
	AttrTTL          = "ttl"
)
