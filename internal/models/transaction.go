package models

import "time"

// SuiTransaction represents a crawled SUI transaction stored in ClickHouse cloud.
type SuiTransaction struct {
	Digest                   string
	CheckpointSequenceNumber uint64
	Timestamp                time.Time
	Sender                   *string // Nullable
	Status                   uint8   // 0=unknown, 1=success, 2=failure
	KindTypename             string
	CommandsJSON             string
	EventsJSON               string
	BalanceChangesJSON       string
	GasFee                   int64
}
