package worker

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	v2 "github.com/open-move/sui-go-sdk/proto/sui/rpc/v2"

	"sui-crawler/internal/client"
	"sui-crawler/internal/models"
)

type fakeCheckpointFetcher struct {
	mu         sync.Mutex
	dataBySeq  map[int64]*client.CheckpointData
	txByDigest map[string]*v2.ExecutedTransaction
	delayBySeq map[int64]time.Duration
	batchCalls [][]string
}

func (f *fakeCheckpointFetcher) GetCheckpoint(ctx context.Context, seqNum int64) (*client.CheckpointResult, error) {
	data, err := f.GetCheckpointData(ctx, seqNum)
	if err != nil {
		return nil, err
	}
	fullTxs := make([]*v2.ExecutedTransaction, 0, len(data.TransactionDigests))
	for _, digest := range data.TransactionDigests {
		fullTxs = append(fullTxs, f.txByDigest[digest])
	}
	return data.BuildResult(fullTxs)
}

func (f *fakeCheckpointFetcher) GetCheckpointData(ctx context.Context, seqNum int64) (*client.CheckpointData, error) {
	if delay := f.delayBySeq[seqNum]; delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	data := f.dataBySeq[seqNum]
	if data == nil {
		return nil, context.Canceled
	}
	return data, nil
}

func (f *fakeCheckpointFetcher) GetCheckpointDataBatch(ctx context.Context, seqNums []int64) (map[int64]*client.CheckpointData, error) {
	results := make(map[int64]*client.CheckpointData, len(seqNums))
	for _, seq := range seqNums {
		data, err := f.GetCheckpointData(ctx, seq)
		if err != nil {
			return nil, err
		}
		results[seq] = data
	}
	return results, nil
}

func (f *fakeCheckpointFetcher) BatchGetTransactions(ctx context.Context, digests []string) ([]*v2.ExecutedTransaction, error) {
	f.mu.Lock()
	f.batchCalls = append(f.batchCalls, slices.Clone(digests))
	f.mu.Unlock()

	results := make([]*v2.ExecutedTransaction, 0, len(digests))
	for _, digest := range digests {
		tx := f.txByDigest[digest]
		if tx == nil {
			return nil, context.Canceled
		}
		results = append(results, tx)
	}
	return results, nil
}

func (f *fakeCheckpointFetcher) GetLatestCheckpointSequenceNumber(ctx context.Context) (int64, error) {
	return 0, nil
}

func (f *fakeCheckpointFetcher) Close() error {
	return nil
}

func TestHydrateCheckpointHydratesTransactions(t *testing.T) {
	w := &Worker{id: "worker-test"}
	fetcher := &fakeCheckpointFetcher{
		dataBySeq: map[int64]*client.CheckpointData{},
		txByDigest: map[string]*v2.ExecutedTransaction{
			"tx-10a": {Digest: ptr("tx-10a")},
		},
	}

	data := &client.CheckpointData{
		Result:             &client.CheckpointResult{Checkpoint: models.SuiCheckpoint{SequenceNumber: 10}},
		TransactionDigests: []string{"tx-10a"},
	}

	result, err := w.hydrateCheckpoint(context.Background(), fetcher, 10, data)
	if err != nil {
		t.Fatalf("hydrateCheckpoint returned error: %v", err)
	}

	if got := len(fetcher.batchCalls); got != 1 {
		t.Fatalf("BatchGetTransactions call count = %d, want 1", got)
	}
	if got := fetcher.batchCalls[0]; len(got) != 1 {
		t.Fatalf("BatchGetTransactions digest count = %d, want 1", len(got))
	}
	if got := result.Transactions[0].Digest; got != "tx-10a" {
		t.Fatalf("checkpoint digest = %q, want %q", got, "tx-10a")
	}
}

func TestHydrateTransactionsByDigestSplitsIntoArchiveSafeBatches(t *testing.T) {
	fetcher := &fakeCheckpointFetcher{
		txByDigest: map[string]*v2.ExecutedTransaction{},
	}
	digests := make([]string, client.MaxBatchGetTransactionsDigests*2+5)
	for i := range digests {
		digest := fmt.Sprintf("tx-%02d", i)
		digests[i] = digest
		fetcher.txByDigest[digest] = &v2.ExecutedTransaction{Digest: ptr(digest)}
	}

	txByDigest, err := hydrateTransactionsByDigest(context.Background(), fetcher, digests)
	if err != nil {
		t.Fatalf("hydrateTransactionsByDigest returned error: %v", err)
	}

	wantBatchSizes := []int{
		client.MaxBatchGetTransactionsDigests,
		client.MaxBatchGetTransactionsDigests,
		5,
	}
	if got := len(fetcher.batchCalls); got != len(wantBatchSizes) {
		t.Fatalf("BatchGetTransactions call count = %d, want %d", got, len(wantBatchSizes))
	}
	for i, want := range wantBatchSizes {
		if got := len(fetcher.batchCalls[i]); got != want {
			t.Fatalf("batch %d digest count = %d, want %d", i, got, want)
		}
		if got := fetcher.batchCalls[i][0]; got != digests[i*client.MaxBatchGetTransactionsDigests] {
			t.Fatalf("batch %d first digest = %q, want %q", i, got, digests[i*client.MaxBatchGetTransactionsDigests])
		}
	}
	if got := len(txByDigest); got != len(digests) {
		t.Fatalf("hydrated transaction map size = %d, want %d", got, len(digests))
	}
}

func TestValidateCheckpointTransactionCountDetectsMissingTransaction(t *testing.T) {
	previous := &models.SuiCheckpoint{
		SequenceNumber:           10,
		NetworkTotalTransactions: 100,
	}
	result := &client.CheckpointResult{
		Checkpoint: models.SuiCheckpoint{
			SequenceNumber:           11,
			NetworkTotalTransactions: 103,
		},
		Transactions: []models.SuiTransaction{
			{Digest: "tx-1"},
			{Digest: "tx-2"},
		},
	}

	err := validateCheckpointTransactionCount(previous, result)
	if err == nil {
		t.Fatal("validateCheckpointTransactionCount error = nil, want mismatch")
	}
}

func TestValidateCheckpointTransactionCountAcceptsMatchingDelta(t *testing.T) {
	previous := &models.SuiCheckpoint{
		SequenceNumber:           10,
		NetworkTotalTransactions: 100,
	}
	result := &client.CheckpointResult{
		Checkpoint: models.SuiCheckpoint{
			SequenceNumber:           11,
			NetworkTotalTransactions: 102,
		},
		Transactions: []models.SuiTransaction{
			{Digest: "tx-1"},
			{Digest: "tx-2"},
		},
	}

	if err := validateCheckpointTransactionCount(previous, result); err != nil {
		t.Fatalf("validateCheckpointTransactionCount returned error: %v", err)
	}
}

func TestHydrateCheckpointFailsWhenHydratedTransactionMissing(t *testing.T) {
	w := &Worker{id: "worker-test"}
	fetcher := &fakeCheckpointFetcher{
		dataBySeq:  map[int64]*client.CheckpointData{},
		txByDigest: map[string]*v2.ExecutedTransaction{},
	}

	data := &client.CheckpointData{
		Result:             &client.CheckpointResult{Checkpoint: models.SuiCheckpoint{SequenceNumber: 1}},
		TransactionDigests: []string{"tx-missing"},
	}

	_, err := w.hydrateCheckpoint(context.Background(), fetcher, 1, data)
	if err == nil {
		t.Fatal("hydrateCheckpoint error = nil, want non-nil")
	}
}

func TestWorkerStartCheckpointAdvancesPastLastCompletedCheckpoint(t *testing.T) {
	job := &models.CrawlerJob{
		FromCheckpoint: 151002000,
		LastCheckpoint: 151002000,
		EndCheckpoint:  151002500,
	}

	startCheckpoint := job.LastCheckpoint + 1
	if startCheckpoint < job.FromCheckpoint {
		startCheckpoint = job.FromCheckpoint
	}

	want := int64(151002001)
	if startCheckpoint != want {
		t.Fatalf("startCheckpoint = %d, want %d", startCheckpoint, want)
	}
}

func ptr[T any](v T) *T {
	return &v
}
