package models

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// DegradedTransaction records a transaction that the Sui archive gRPC endpoint
// cannot fully hydrate because rendering its transaction body fails
// deterministically (e.g. `code=13 "unable to parse type"` for a malformed
// on-chain type). The crawler still ingests a partial row (effects, events,
// balance changes) but the transaction body fields are unavailable. These
// records let operators reconcile or reprocess the affected digests later.
type DegradedTransaction struct {
	ID                       bson.ObjectID `bson:"_id,omitempty" json:"id"`
	JobID                    bson.ObjectID `bson:"job_id" json:"jobId"`
	Digest                   string        `bson:"digest" json:"digest"`
	CheckpointSequenceNumber int64         `bson:"checkpoint_sequence_number" json:"checkpointSequenceNumber"`
	Reason                   string        `bson:"reason" json:"reason"`
	CreatedAt                time.Time     `bson:"created_at" json:"created_at"`
}
