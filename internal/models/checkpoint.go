package models

import "time"

// SuiCheckpoint represents a crawled SUI checkpoint stored in ClickHouse cloud.
type SuiCheckpoint struct {
	SequenceNumber           uint64
	Digest                   string
	PreviousCheckpointDigest string
	NetworkTotalTransactions uint32
	Timestamp                time.Time
}
