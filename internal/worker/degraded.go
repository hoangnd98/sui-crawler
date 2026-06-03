package worker

import (
	"context"
	"log"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"

	"sui-crawler/internal/models"
)

// DegradedTransactionRecorder persists transactions the archive could not fully
// hydrate. The job repository satisfies this; nil disables recording.
type DegradedTransactionRecorder interface {
	Record(ctx context.Context, dt *models.DegradedTransaction) error
}

// degradedRegistry collects digests the SuiClient hydrated with a reduced read
// mask (keyed by digest -> archive reason) so the worker can associate them
// with their checkpoint and persist them. It is safe for concurrent use.
type degradedRegistry struct {
	mu      sync.Mutex
	reasons map[string]string
}

func newDegradedRegistry() *degradedRegistry {
	return &degradedRegistry{reasons: make(map[string]string)}
}

func (r *degradedRegistry) record(digest, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reasons[digest] = reason
}

func (r *degradedRegistry) take(digest string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	reason, ok := r.reasons[digest]
	if ok {
		delete(r.reasons, digest)
	}
	return reason, ok
}

// persistDegradedTransactions writes any degraded digests found among the given
// checkpoint items to MongoDB, associating each with its checkpoint. Recording
// failures are logged but never block ingestion.
func (w *Worker) persistDegradedTransactions(
	ctx context.Context,
	jobID bson.ObjectID,
	items []checkpointDataTask,
	registry *degradedRegistry,
) {
	if w.cfg.DegradedRecorder == nil || registry == nil {
		return
	}
	for _, item := range items {
		for _, digest := range item.data.TransactionDigests {
			reason, ok := registry.take(digest)
			if !ok {
				continue
			}
			dt := &models.DegradedTransaction{
				JobID:                    jobID,
				Digest:                   digest,
				CheckpointSequenceNumber: item.seq,
				Reason:                   reason,
			}
			if err := w.cfg.DegradedRecorder.Record(ctx, dt); err != nil {
				log.Printf("[%s] WARN failed to record degraded transaction digest=%s seq=%d: %v",
					w.id, digest, item.seq, err)
				continue
			}
			log.Printf("[%s] Recorded degraded transaction digest=%s seq=%d",
				w.id, digest, item.seq)
		}
	}
}
