package worker

import (
	"context"
	"errors"
	"sync"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"sui-crawler/internal/client"
	"sui-crawler/internal/models"
)

type fakeDegradedRecorder struct {
	mu      sync.Mutex
	records []*models.DegradedTransaction
	err     error
}

func (f *fakeDegradedRecorder) Record(ctx context.Context, dt *models.DegradedTransaction) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.records = append(f.records, dt)
	return nil
}

func checkpointItem(seq int64, digests ...string) checkpointDataTask {
	return checkpointDataTask{
		seq:  seq,
		data: &client.CheckpointData{TransactionDigests: digests},
	}
}

func TestPersistDegradedTransactionsAssociatesCheckpoint(t *testing.T) {
	registry := newDegradedRegistry()
	registry.record("digestA", "unable to parse type \"LPCoin<0x5c45\"")
	registry.record("digestC", "unable to parse type \"Other\"")

	recorder := &fakeDegradedRecorder{}
	w := &Worker{id: "test", cfg: WorkerConfig{DegradedRecorder: recorder}}

	jobID := bson.NewObjectID()
	items := []checkpointDataTask{
		checkpointItem(10, "digestA", "digestB"),
		checkpointItem(11, "digestC"),
	}

	w.persistDegradedTransactions(context.Background(), jobID, items, registry)

	if got := len(recorder.records); got != 2 {
		t.Fatalf("recorded %d degraded transactions, want 2", got)
	}

	bySeq := make(map[string]int64)
	for _, r := range recorder.records {
		bySeq[r.Digest] = r.CheckpointSequenceNumber
		if r.JobID != jobID {
			t.Fatalf("record %s job id = %v, want %v", r.Digest, r.JobID, jobID)
		}
		if r.Reason == "" {
			t.Fatalf("record %s missing reason", r.Digest)
		}
	}
	if bySeq["digestA"] != 10 {
		t.Fatalf("digestA checkpoint = %d, want 10", bySeq["digestA"])
	}
	if bySeq["digestC"] != 11 {
		t.Fatalf("digestC checkpoint = %d, want 11", bySeq["digestC"])
	}

	// Healthy digestB must not be recorded, and taken digests must be cleared.
	if _, ok := registry.take("digestA"); ok {
		t.Fatal("digestA should have been consumed from the registry")
	}
}

func TestPersistDegradedTransactionsNilRecorderIsNoop(t *testing.T) {
	registry := newDegradedRegistry()
	registry.record("digestA", "reason")
	w := &Worker{id: "test", cfg: WorkerConfig{DegradedRecorder: nil}}

	// Must not panic when no recorder is configured.
	w.persistDegradedTransactions(context.Background(), bson.NewObjectID(),
		[]checkpointDataTask{checkpointItem(10, "digestA")}, registry)
}

func TestPersistDegradedTransactionsRecordErrorDoesNotPanic(t *testing.T) {
	registry := newDegradedRegistry()
	registry.record("digestA", "reason")
	recorder := &fakeDegradedRecorder{err: errors.New("mongo down")}
	w := &Worker{id: "test", cfg: WorkerConfig{DegradedRecorder: recorder}}

	// Recording failures are logged, not propagated; ingestion continues.
	w.persistDegradedTransactions(context.Background(), bson.NewObjectID(),
		[]checkpointDataTask{checkpointItem(10, "digestA")}, registry)
}
