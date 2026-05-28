package client

import (
	"context"
	"errors"

	v2 "github.com/open-move/sui-go-sdk/proto/sui/rpc/v2"
)

// CheckpointData holds checkpoint metadata plus either fully-hydrated transactions
// or the ordered list of digests required for a follow-up batch hydration call.
type CheckpointData struct {
	Result             *CheckpointResult
	TransactionDigests []string
}

// BuildResult constructs a full checkpoint result from hydrated transactions.
// For inline checkpoints, Result is already complete.
func (d *CheckpointData) BuildResult(fullTxs []*v2.ExecutedTransaction) (*CheckpointResult, error) {
	if d == nil || d.Result == nil {
		return nil, errors.New("checkpoint data is nil")
	}
	if len(d.TransactionDigests) == 0 {
		return d.Result, nil
	}
	return d.Result.withHydratedTransactions(fullTxs, d.TransactionDigests)
}

// CheckpointFetcher fetches checkpoint data and hydrates transactions from Sui.
type CheckpointFetcher interface {
	GetCheckpoint(ctx context.Context, seqNum int64) (*CheckpointResult, error)
	GetCheckpointData(ctx context.Context, seqNum int64) (*CheckpointData, error)
	GetCheckpointDataBatch(ctx context.Context, seqNums []int64) (map[int64]*CheckpointData, error)
	BatchGetTransactions(ctx context.Context, digests []string) ([]*v2.ExecutedTransaction, error)
	GetLatestCheckpointSequenceNumber(ctx context.Context) (int64, error)
	Close() error
}
