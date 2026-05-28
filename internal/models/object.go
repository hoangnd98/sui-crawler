package models

import "time"

// SuiTransactionObject represents an object's input/output state change within a transaction.
type SuiTransactionObject struct {
	ObjectID          string
	Version           uint64
	TransactionDigest string
	InputVersion      uint64
	InputOwner        *string
	InputDigest       *string
	OutputVersion     uint64
	OutputOwner       *string
	OutputDigest      *string
	IsCreated         uint8
	IsDeleted         uint8
	Timestamp         time.Time
}
