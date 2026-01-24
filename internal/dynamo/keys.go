// Package dynamo provides shared DynamoDB constants and utilities.
package dynamo

const (
	// Primary key attributes.
	AttrPK = "pk"
	AttrSK = "sk"

	// Key prefixes.
	PrefixAccount = "ACCOUNT#"

	// LSI sort key attributes.
	AttrLSI1SK = "lsi1sk"
	AttrLSI2SK = "lsi2sk"
	AttrLSI3SK = "lsi3sk"

	// Index names.
	IndexLSI1 = "lsi1"
	IndexLSI2 = "lsi2"
	IndexLSI3 = "lsi3"
)
