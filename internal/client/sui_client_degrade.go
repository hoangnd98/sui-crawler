package client

import (
	"context"
	"fmt"
	"log"
	"strings"

	v2 "github.com/open-move/sui-go-sdk/proto/sui/rpc/v2"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// degradedReadMaskPaths is the reduced read mask used when the archive cannot
// render a transaction body. It omits the `transaction` field (the source of
// the deterministic type-parse failure) while keeping everything else.
var degradedReadMaskPaths = []string{"digest", "effects", "events", "balance_changes"}

// DegradedTransactionSink is notified when a transaction is hydrated with the
// reduced read mask because its body could not be rendered. The reason carries
// the original archive error message for auditing.
type DegradedTransactionSink func(digest string, reason string)

// SetDegradedTransactionSink registers a callback invoked for each transaction
// that falls back to reduced-mask hydration. The sink must be safe for
// concurrent use; the worker hydrates batches across multiple goroutines.
func (c *SuiClient) SetDegradedTransactionSink(sink DegradedTransactionSink) {
	c.degradedSink = sink
	if fallback := c.gatewayTimeoutFallback; fallback != nil {
		fallback.degradedSink = sink
	}
}

// isUnparseableTypeError reports whether err is the deterministic, server-side
// archive failure where the transaction body contains a Move type the gRPC
// adapter cannot parse (`code=13 "unable to parse type"`). This is not
// transient and not size-related, so it must not be retried or split forever.
func isUnparseableTypeError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "unable to parse type")
}

// degradeUnparseableTransaction handles a single digest whose body the archive
// cannot render. It re-fetches the transaction with a reduced read mask so the
// pipeline still gets effects, events, and balance changes, records the digest
// as degraded, and never returns an error for this class (keeping the job
// alive). The resulting ExecutedTransaction has a nil body, which the mapper
// already tolerates.
func (c *SuiClient) degradeUnparseableTransaction(
	ctx context.Context,
	digest string,
	idx int,
	results []*v2.ExecutedTransaction,
	origErr error,
) error {
	reason := strings.TrimSpace(origErr.Error())

	tx, err := c.fetchTransactionReducedMask(ctx, digest)
	if err != nil {
		// Even the reduced mask failed; fall back to a digest-only row so the
		// checkpoint transaction count is preserved and the job continues.
		log.Printf(
			"WARN degraded transaction reduced-mask fetch failed; emitting digest-only row digest=%s err=%v orig=%s",
			digest, err, reason,
		)
		d := digest
		tx = &v2.ExecutedTransaction{Digest: &d}
		reason = fmt.Sprintf("%s; reduced-mask fetch failed: %v", reason, err)
	} else {
		log.Printf(
			"WARN degraded transaction hydrated with reduced read mask (no transaction body) digest=%s reason=%s",
			digest, reason,
		)
	}

	results[idx] = tx
	if c.degradedSink != nil {
		c.degradedSink(digest, reason)
	}
	return nil
}

// fetchTransactionReducedMask fetches a single transaction omitting the
// `transaction` body field. It mirrors GetTransaction's transport handling but
// tolerates a nil transaction body in the response.
func (c *SuiClient) fetchTransactionReducedMask(ctx context.Context, digest string) (*v2.ExecutedTransaction, error) {
	var result *v2.ExecutedTransaction
	err := c.callWithRetryAndFallback(ctx, "GetTransactionDegraded", digest, func(active *SuiClient) error {
		if err := active.acquireGRPCSlot(ctx); err != nil {
			return err
		}
		defer active.releaseGRPCSlot()
		if err := active.rateWait(ctx); err != nil {
			return err
		}
		req := &v2.GetTransactionRequest{
			Digest:   &digest,
			ReadMask: &fieldmaskpb.FieldMask{Paths: degradedReadMaskPaths},
		}
		rpcCtx, cancel := context.WithTimeout(ctx, active.rpcTimeout)
		defer cancel()

		resp, err := active.grpcClient.LedgerClient().GetTransaction(rpcCtx, req)
		if err != nil && rpcCtx.Err() != nil {
			return fmt.Errorf("rpc timeout after %s: %w", active.rpcTimeout, err)
		}
		if err != nil {
			return err
		}
		if resp == nil || resp.Transaction == nil {
			return fmt.Errorf("degraded transaction %s not found", digest)
		}
		result = resp.Transaction
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
