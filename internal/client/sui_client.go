package client

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/url"
	"strings"
	"time"

	grpcsdk "github.com/open-move/sui-go-sdk/grpc"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	v2 "github.com/open-move/sui-go-sdk/proto/sui/rpc/v2"

	"sui-crawler/internal/models"
)

const (
	maxRetries                  = 5
	initialBackoffMs            = 1000
	maxBackoffMs                = 60000
	defaultRPCTimeout           = 5 * time.Minute
	maxGRPCMessageSize          = 64 * 1024 * 1024
	publicBatchTxParallelism    = 10
	nonPublicBatchTxParallelism = 1
	checkpointBatchSize         = 10

	// MaxBatchGetTransactionsDigests caps one archive BatchGetTransactions RPC.
	// The archive endpoint can reject larger batches because the underlying
	// Bigtable ReadRows request exceeds 512 KiB even when the gRPC payload is small.
	MaxBatchGetTransactionsDigests = 5
)

type SuiClient struct {
	grpcClient             *grpcsdk.Client
	grpcEndpoint           string
	rateLimiter            *rate.Limiter
	rpcTimeout             time.Duration
	grpcSemaphore          chan struct{}
	batchTxSemaphore       chan struct{}
	rpcHeaders             map[string]string
	jsonRPCURL             string
	gatewayTimeoutFallback *SuiClient
	degradedSink           DegradedTransactionSink
}

// NewSuiClient creates a new SUI gRPC client. limiter may be nil (no rate limiting).
func NewSuiClient(ctx context.Context, endpoint string, limiter *rate.Limiter) (*SuiClient, error) {
	return NewSuiClientWithHeaders(ctx, endpoint, limiter, nil)
}

// NewSuiClientWithHeaders creates a new SUI gRPC client with optional per-RPC headers.
func NewSuiClientWithHeaders(ctx context.Context, endpoint string, limiter *rate.Limiter, headers map[string]string) (*SuiClient, error) {
	if endpoint == "" {
		endpoint = "https://archive.mainnet.sui.io"
	}
	endpoint = normalizeGRPCEndpoint(endpoint)

	opts := []grpcsdk.Option{
		grpcsdk.WithDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxGRPCMessageSize),
			grpc.MaxCallSendMsgSize(maxGRPCMessageSize),
		)),
	}
	if len(headers) > 0 {
		opts = append(opts, grpcsdk.WithDialOption(grpc.WithUnaryInterceptor(unaryMetadataInterceptor(headers))))
	}

	client, err := grpcsdk.NewClient(ctx, endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create sui grpc client: %w", err)
	}

	return &SuiClient{
		grpcClient:       client,
		grpcEndpoint:     client.Endpoint(),
		rateLimiter:      limiter,
		rpcTimeout:       defaultRPCTimeout,
		grpcSemaphore:    make(chan struct{}, 10),
		batchTxSemaphore: make(chan struct{}, batchTxParallelism(headers)),
		rpcHeaders:       cloneHeaders(headers),
		jsonRPCURL:       "",
	}, nil
}

// SetRPCTimeout overrides the timeout applied to individual gRPC calls.
func (c *SuiClient) SetRPCTimeout(timeout time.Duration) {
	if timeout > 0 {
		c.rpcTimeout = timeout
	}
}

// SetGatewayTimeoutFallback configures a secondary client used when the primary
// gRPC endpoint returns an upstream 504 Gateway Timeout.
func (c *SuiClient) SetGatewayTimeoutFallback(fallback *SuiClient) {
	if fallback == nil || fallback == c {
		return
	}
	c.gatewayTimeoutFallback = fallback
}

func unaryMetadataInterceptor(headers map[string]string) grpc.UnaryClientInterceptor {
	md := metadata.New(headers)
	return func(
		ctx context.Context,
		method string,
		req any,
		reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		return invoker(metadata.NewOutgoingContext(ctx, md), method, req, reply, cc, opts...)
	}
}

func cloneHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(headers))
	for k, v := range headers {
		cloned[k] = v
	}
	return cloned
}

func normalizeGRPCEndpoint(endpoint string) string {
	trimmed := strings.TrimSpace(endpoint)
	if !strings.Contains(trimmed, "://") {
		return trimmed
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return trimmed
	}
	scheme := strings.ToLower(parsed.Scheme)
	if (scheme != "https" && scheme != "grpcs") || parsed.Port() != "443" {
		return trimmed
	}

	host := parsed.Hostname()
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	parsed.Host = host
	return parsed.String()
}

func batchTxParallelism(headers map[string]string) int {
	if len(headers) > 0 && strings.TrimSpace(headers["x-token"]) != "" {
		return nonPublicBatchTxParallelism
	}
	return publicBatchTxParallelism
}

func (c *SuiClient) fallbackClient() *SuiClient {
	if c.gatewayTimeoutFallback == nil || c.gatewayTimeoutFallback == c {
		return nil
	}
	return c.gatewayTimeoutFallback
}

func (c *SuiClient) rpcLabel(method, target string) string {
	label := method
	if target != "" {
		label = fmt.Sprintf("%s(%s)", method, target)
	}
	if c.grpcEndpoint != "" {
		label = fmt.Sprintf("%s endpoint=%s", label, c.grpcEndpoint)
	}
	if c.rpcTimeout > 0 {
		label = fmt.Sprintf("%s timeout=%s", label, c.rpcTimeout)
	}
	return label
}

func (c *SuiClient) gatewayTimeoutFallbackLabel(method, target string) string {
	label := method
	if target != "" {
		label = fmt.Sprintf("%s(%s)", method, target)
	}
	return label
}

// SetJSONRPCURL overrides the JSON-RPC endpoint used for event queries.
func (c *SuiClient) SetJSONRPCURL(url string) {
	if url != "" {
		c.jsonRPCURL = url
	}
}

func (c *SuiClient) rateWait(ctx context.Context) error {
	if c.rateLimiter == nil {
		return nil
	}
	return c.rateLimiter.Wait(ctx)
}

func (c *SuiClient) acquireBatchTxSlot(ctx context.Context) error {
	if c.batchTxSemaphore == nil {
		return nil
	}
	select {
	case c.batchTxSemaphore <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *SuiClient) releaseBatchTxSlot() {
	if c.batchTxSemaphore == nil {
		return
	}
	select {
	case <-c.batchTxSemaphore:
	default:
	}
}

func (c *SuiClient) acquireGRPCSlot(ctx context.Context) error {
	if c.grpcSemaphore == nil {
		return nil
	}
	select {
	case c.grpcSemaphore <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *SuiClient) releaseGRPCSlot() {
	if c.grpcSemaphore == nil {
		return
	}
	select {
	case <-c.grpcSemaphore:
	default:
	}
}

// Close closes the underlying gRPC connection.
func (c *SuiClient) Close() error {
	return c.grpcClient.Close()
}

func (c *SuiClient) callWithGatewayTimeoutFallback(
	ctx context.Context,
	method string,
	target string,
	call func(client *SuiClient) error,
) error {
	err := call(c)
	if err == nil || !isGatewayTimeoutError(err) {
		return err
	}

	fallback := c.fallbackClient()
	if fallback == nil {
		return err
	}

	fmt.Printf(
		"[%s] Gateway timeout on endpoint=%s; retrying via endpoint=%s: %v\n",
		c.gatewayTimeoutFallbackLabel(method, target),
		c.grpcEndpoint,
		fallback.grpcEndpoint,
		err,
	)

	if ctx.Err() != nil {
		return err
	}
	return call(fallback)
}

func (c *SuiClient) callWithRetryAndFallback(
	ctx context.Context,
	method string,
	target string,
	call func(client *SuiClient) error,
) error {
	err := withRetryContext(ctx, c.rpcLabel(method, target), func() error {
		return c.callWithGatewayTimeoutFallback(ctx, method, target, call)
	})
	if err == nil {
		return nil
	}
	if isAdaptiveBatchSplitError(err) || isUnparseableTypeError(err) {
		return err
	}

	fallback := c.fallbackClient()
	if fallback == nil || ctx.Err() != nil {
		return err
	}

	fmt.Printf(
		"[%s] Primary endpoint=%s exhausted retries; falling back to endpoint=%s: %v\n",
		c.gatewayTimeoutFallbackLabel(method, target),
		c.grpcEndpoint,
		fallback.grpcEndpoint,
		err,
	)

	return withRetryContext(ctx, fallback.rpcLabel(method, target), func() error {
		return call(fallback)
	})
}

// GetLatestCheckpointSequenceNumber fetches the latest checkpoint sequence number.
func (c *SuiClient) GetLatestCheckpointSequenceNumber(ctx context.Context) (int64, error) {
	var seq int64
	err := c.callWithRetryAndFallback(ctx, "GetLatestCheckpoint", "", func(active *SuiClient) error {
		req := &v2.GetCheckpointRequest{}
		if err := active.acquireGRPCSlot(ctx); err != nil {
			return err
		}
		defer active.releaseGRPCSlot()
		rpcCtx, cancel := context.WithTimeout(ctx, active.rpcTimeout)
		defer cancel()

		resp, err := active.grpcClient.LedgerClient().GetCheckpoint(rpcCtx, req)
		if err != nil {
			return err
		}
		if resp.Checkpoint == nil || resp.Checkpoint.SequenceNumber == nil {
			return fmt.Errorf("no sequence number in response")
		}
		seq = int64(*resp.Checkpoint.SequenceNumber)
		return nil
	})
	return seq, err
}

// CheckpointResult holds the parsed checkpoint data and its transactions and object changes.
type CheckpointResult struct {
	Checkpoint         models.SuiCheckpoint
	Transactions       []models.SuiTransaction
	TransactionObjects []models.SuiTransactionObject
	EventCount         int
}

func (r *CheckpointResult) withHydratedTransactions(fullTxs []*v2.ExecutedTransaction, digests []string) (*CheckpointResult, error) {
	if r == nil {
		return nil, fmt.Errorf("checkpoint result is nil")
	}
	if len(digests) > 0 && len(fullTxs) != len(digests) {
		return nil, fmt.Errorf("hydrated transaction count mismatch: got %d want %d", len(fullTxs), len(digests))
	}

	allTxs := make([]models.SuiTransaction, 0, len(fullTxs))
	allTxObjs := make([]models.SuiTransactionObject, 0, len(fullTxs))
	eventCount := 0
	ts := r.Checkpoint.Timestamp
	seqNum := int64(r.Checkpoint.SequenceNumber)

	for i, fullTx := range fullTxs {
		if fullTx == nil {
			return nil, fmt.Errorf("hydrated transaction %d is nil", i)
		}
		if len(digests) > 0 && fullTx.GetDigest() != digests[i] {
			return nil, fmt.Errorf("hydrated transaction digest mismatch at %d: got %s want %s", i, fullTx.GetDigest(), digests[i])
		}

		parsedTx, txObjs, err := mapTransaction(fullTx, seqNum, ts)
		if err != nil {
			return nil, fmt.Errorf("map transaction %s: %w", fullTx.GetDigest(), err)
		}
		allTxs = append(allTxs, parsedTx)
		allTxObjs = append(allTxObjs, txObjs...)
		if fullTx.Events != nil {
			eventCount += len(fullTx.Events.Events)
		}
	}

	result := *r
	result.Transactions = allTxs
	result.TransactionObjects = allTxObjs
	result.EventCount = eventCount
	return &result, nil
}

// GetCheckpoint fetches a single checkpoint by sequence number and returns the mapped data.
func (c *SuiClient) GetCheckpoint(ctx context.Context, seqNum int64) (*CheckpointResult, error) {
	cpData, err := c.GetCheckpointData(ctx, seqNum)
	if err != nil {
		return nil, err
	}
	if len(cpData.TransactionDigests) == 0 {
		return cpData.Result, nil
	}

	fullTxs, err := c.BatchGetTransactions(ctx, cpData.TransactionDigests)
	if err != nil {
		return nil, err
	}
	return cpData.BuildResult(fullTxs)
}

// GetTransaction fetches one fully-hydrated transaction by digest.
func (c *SuiClient) GetTransaction(ctx context.Context, digest string) (*v2.ExecutedTransaction, error) {
	if strings.TrimSpace(digest) == "" {
		return nil, fmt.Errorf("transaction digest is empty")
	}

	var result *v2.ExecutedTransaction
	err := c.callWithRetryAndFallback(ctx, "GetTransaction", digest, func(active *SuiClient) error {
		if err := active.acquireGRPCSlot(ctx); err != nil {
			return err
		}
		defer active.releaseGRPCSlot()
		startedAt := time.Now()
		log.Printf(
			"get transaction start endpoint=%s token=%q digest=%s",
			active.grpcEndpoint,
			active.rpcHeaders["x-token"],
			digest,
		)
		req := &v2.GetTransactionRequest{
			Digest: &digest,
			ReadMask: &fieldmaskpb.FieldMask{
				Paths: []string{"digest", "transaction", "effects", "events", "balance_changes"},
			},
		}
		if err := active.rateWait(ctx); err != nil {
			return err
		}
		rpcCtx, cancel := context.WithTimeout(ctx, active.rpcTimeout)
		defer cancel()

		resp, err := active.grpcClient.LedgerClient().GetTransaction(rpcCtx, req)
		if err != nil && rpcCtx.Err() != nil {
			log.Printf(
				"get transaction failed endpoint=%s token=%q digest=%s elapsed=%s err=%v",
				active.grpcEndpoint,
				active.rpcHeaders["x-token"],
				digest,
				time.Since(startedAt).Round(time.Millisecond),
				err,
			)
			return fmt.Errorf("rpc timeout after %s: %w", active.rpcTimeout, err)
		}
		if err != nil {
			log.Printf(
				"get transaction failed endpoint=%s token=%q digest=%s elapsed=%s err=%v",
				active.grpcEndpoint,
				active.rpcHeaders["x-token"],
				digest,
				time.Since(startedAt).Round(time.Millisecond),
				err,
			)
			return err
		}
		if resp == nil || resp.Transaction == nil {
			return fmt.Errorf("transaction %s not found", digest)
		}
		log.Printf(
			"get transaction complete endpoint=%s token=%q digest=%s elapsed=%s",
			active.grpcEndpoint,
			active.rpcHeaders["x-token"],
			digest,
			time.Since(startedAt).Round(time.Millisecond),
		)
		result = resp.Transaction
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetCheckpointData fetches checkpoint metadata and either returns a fully
// populated result (when transactions are inline) or the ordered digest list
// needed for a follow-up BatchGetTransactions call.
func (c *SuiClient) GetCheckpointData(ctx context.Context, seqNum int64) (*CheckpointData, error) {
	var result *CheckpointData

	err := c.callWithRetryAndFallback(ctx, "GetCheckpoint", fmt.Sprintf("%d", seqNum), func(active *SuiClient) error {
		req := &v2.GetCheckpointRequest{
			CheckpointId: &v2.GetCheckpointRequest_SequenceNumber{
				SequenceNumber: uint64(seqNum),
			},
			ReadMask: &fieldmaskpb.FieldMask{
				Paths: []string{"sequence_number", "digest", "summary", "signature", "contents", "transactions"},
			},
		}
		if err := active.rateWait(ctx); err != nil {
			return err
		}
		if err := active.acquireGRPCSlot(ctx); err != nil {
			return err
		}
		defer active.releaseGRPCSlot()
		rpcCtx, cancel := context.WithTimeout(ctx, active.rpcTimeout)
		defer cancel()

		resp, err := active.grpcClient.LedgerClient().GetCheckpoint(rpcCtx, req)
		if err != nil {
			return err
		}

		if resp.Checkpoint == nil {
			return fmt.Errorf("checkpoint not found")
		}

		mapped := mapCheckpoint(resp.Checkpoint)
		if len(resp.Checkpoint.Transactions) > 0 {
			built, err := mapped.withHydratedTransactions(resp.Checkpoint.Transactions, nil)
			if err != nil {
				return err
			}
			result = &CheckpointData{Result: built}
			return nil
		}

		digests := checkpointTransactionDigests(resp.Checkpoint.Contents)
		result = &CheckpointData{
			Result:             mapped,
			TransactionDigests: digests,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetCheckpointDataBatch fetches up to 10 checkpoints through gRPC only.
func (c *SuiClient) GetCheckpointDataBatch(ctx context.Context, seqNums []int64) (map[int64]*CheckpointData, error) {
	if len(seqNums) == 0 {
		return nil, nil
	}

	if len(seqNums) > checkpointBatchSize {
		return nil, fmt.Errorf("checkpoint batch too large: got %d want <= %d", len(seqNums), checkpointBatchSize)
	}

	return c.getCheckpointDataBatchViaGRPC(ctx, seqNums)
}

// BatchGetTransactions hydrates multiple transaction digests in order.
func (c *SuiClient) BatchGetTransactions(ctx context.Context, digests []string) ([]*v2.ExecutedTransaction, error) {
	if len(digests) == 0 {
		return nil, nil
	}

	results := make([]*v2.ExecutedTransaction, len(digests))

	for _, batch := range batchGetTransactionDigestChunks(digests) {
		if err := c.batchGetTransactionsChunk(ctx, batch.digests, batch.start, len(digests), results); err != nil {
			return results, err
		}
	}

	return results, nil
}

func (c *SuiClient) batchGetTransactionsChunk(
	ctx context.Context,
	chunk []string,
	chunkStart int,
	total int,
	results []*v2.ExecutedTransaction,
) error {
	err := c.callWithRetryAndFallback(ctx, "BatchGetTransactions", fmt.Sprintf("%d digests", len(chunk)), func(active *SuiClient) error {
		if err := active.acquireBatchTxSlot(ctx); err != nil {
			return err
		}
		defer active.releaseBatchTxSlot()
		if err := active.acquireGRPCSlot(ctx); err != nil {
			return err
		}
		defer active.releaseGRPCSlot()
		startedAt := time.Now()
		log.Printf(
			"batch get transactions start endpoint=%s token=%q digests=%d offset=%d total=%d",
			active.grpcEndpoint,
			active.rpcHeaders["x-token"],
			len(chunk),
			chunkStart,
			total,
		)

		req := &v2.BatchGetTransactionsRequest{
			Digests: chunk,
			ReadMask: &fieldmaskpb.FieldMask{
				Paths: []string{"digest", "transaction", "effects", "events", "balance_changes"},
			},
		}
		if err := active.rateWait(ctx); err != nil {
			return err
		}
		rpcCtx, cancel := context.WithTimeout(ctx, active.rpcTimeout)
		defer cancel()
		resp, err := active.grpcClient.LedgerClient().BatchGetTransactions(rpcCtx, req)
		if err != nil && rpcCtx.Err() != nil {
			log.Printf(
				"batch get transactions failed endpoint=%s token=%q digests=%d offset=%d total=%d elapsed=%s err=%v",
				active.grpcEndpoint,
				active.rpcHeaders["x-token"],
				len(chunk),
				chunkStart,
				total,
				time.Since(startedAt).Round(time.Millisecond),
				err,
			)
			return fmt.Errorf("rpc timeout after %s: %w", active.rpcTimeout, err)
		}
		if err != nil {
			log.Printf(
				"batch get transactions failed endpoint=%s token=%q digests=%d offset=%d total=%d elapsed=%s err=%v",
				active.grpcEndpoint,
				active.rpcHeaders["x-token"],
				len(chunk),
				chunkStart,
				total,
				time.Since(startedAt).Round(time.Millisecond),
				err,
			)
			return err
		}
		log.Printf(
			"batch get transactions complete endpoint=%s token=%q digests=%d offset=%d total=%d elapsed=%s",
			active.grpcEndpoint,
			active.rpcHeaders["x-token"],
			len(chunk),
			chunkStart,
			total,
			time.Since(startedAt).Round(time.Millisecond),
		)
		if resp == nil {
			return fmt.Errorf("batch transaction response is nil")
		}
		if len(resp.Transactions) != len(chunk) {
			return fmt.Errorf("batch transaction response count mismatch: got %d want %d", len(resp.Transactions), len(chunk))
		}

		for i, result := range resp.Transactions {
			if result == nil {
				return fmt.Errorf("batch transaction result %d is nil", chunkStart+i)
			}
			if tx := result.GetTransaction(); tx != nil {
				results[chunkStart+i] = tx
				continue
			}
			if rpcErr := result.GetError(); rpcErr != nil {
				return newBatchTransactionError(chunk[i], rpcErr.Code, rpcErr.Message)
			}
			return fmt.Errorf("batch transaction %s returned no transaction payload", chunk[i])
		}
		return nil
	})
	if err == nil {
		return nil
	}

	// Deterministic malformed-type item errors name the exact offending digest,
	// so isolate it directly instead of repeatedly halving the chunk.
	if isUnparseableTypeError(err) {
		return c.isolateUnparseableTransaction(ctx, chunk, chunkStart, total, results, err)
	}

	// Oversized-request errors carry no per-digest detail; halve the chunk.
	if isAdaptiveBatchSplitError(err) && len(chunk) > 1 {
		mid := len(chunk) / 2
		log.Printf(
			"batch get transactions splitting oversized request digests=%d offset=%d total=%d err=%v",
			len(chunk),
			chunkStart,
			total,
			err,
		)
		if err := c.batchGetTransactionsChunk(ctx, chunk[:mid], chunkStart, total, results); err != nil {
			return err
		}
		return c.batchGetTransactionsChunk(ctx, chunk[mid:], chunkStart+mid, total, results)
	}

	return err
}

type digestChunk struct {
	start   int
	digests []string
}

func batchGetTransactionDigestChunks(digests []string) []digestChunk {
	if len(digests) == 0 {
		return nil
	}
	chunks := make([]digestChunk, 0, (len(digests)+MaxBatchGetTransactionsDigests-1)/MaxBatchGetTransactionsDigests)
	for start := 0; start < len(digests); start += MaxBatchGetTransactionsDigests {
		end := start + MaxBatchGetTransactionsDigests
		if end > len(digests) {
			end = len(digests)
		}
		chunks = append(chunks, digestChunk{
			start:   start,
			digests: digests[start:end],
		})
	}
	return chunks
}

func checkpointTransactionDigests(contents *v2.CheckpointContents) []string {
	if contents == nil || len(contents.Transactions) == 0 {
		return nil
	}
	digests := make([]string, 0, len(contents.Transactions))
	for _, txInfo := range contents.Transactions {
		if txInfo == nil || txInfo.Transaction == nil {
			continue
		}
		digests = append(digests, *txInfo.Transaction)
	}
	return digests
}

func (c *SuiClient) getCheckpointDataBatchViaGRPC(ctx context.Context, seqNums []int64) (map[int64]*CheckpointData, error) {
	results := make(map[int64]*CheckpointData, len(seqNums))
	for _, seq := range seqNums {
		cp, err := c.GetCheckpointData(ctx, seq)
		if err != nil {
			return nil, err
		}
		results[seq] = cp
	}
	return results, nil
}

func mapCheckpoint(cp *v2.Checkpoint) *CheckpointResult {
	seqNum := uint64(0)
	if cp.SequenceNumber != nil {
		seqNum = *cp.SequenceNumber
	}

	digest := ""
	if cp.Digest != nil {
		digest = *cp.Digest
	}

	var ts time.Time
	prevDigest := ""
	netTotalTx := uint32(0)

	if cp.Summary != nil {
		if cp.Summary.Timestamp != nil {
			ts = cp.Summary.Timestamp.AsTime()
		}
		if cp.Summary.PreviousDigest != nil {
			prevDigest = *cp.Summary.PreviousDigest
		}
		if cp.Summary.TotalNetworkTransactions != nil {
			v := *cp.Summary.TotalNetworkTransactions
			if v <= math.MaxUint32 {
				netTotalTx = uint32(v)
			} else {
				netTotalTx = math.MaxUint32
			}
		}
	}

	return &CheckpointResult{
		Checkpoint: models.SuiCheckpoint{
			SequenceNumber:           seqNum,
			Digest:                   digest,
			PreviousCheckpointDigest: prevDigest,
			NetworkTotalTransactions: netTotalTx,
			Timestamp:                ts,
		},
	}
}

func mapTransaction(tx *v2.ExecutedTransaction, checkpointSeqNum int64, ts time.Time) (models.SuiTransaction, []models.SuiTransactionObject, error) {
	txDigest := ""
	if tx.Digest != nil {
		txDigest = *tx.Digest
	}

	var sender *string
	status := uint8(0)
	kindTypename := "Unknown"
	commandsJSON := "[]"
	inputsJSON := "[]"
	eventsJSON := "[]"
	balanceChangesJSON := "[]"
	gasFee := int64(0)

	if tx.Transaction != nil {
		if s := tx.Transaction.GetSender(); s != "" && s != zeroSuiAddress {
			sender = &s
		}
		kindTypename = transactionKindTypename(tx.Transaction)
		commandsJSON = transactionCommandsJSON(tx.Transaction)
		inputsJSON = transactionInputsJSON(tx.Transaction)
	}

	if tx.Effects != nil {
		if tx.Effects.Status != nil && tx.Effects.Status.Success != nil {
			if *tx.Effects.Status.Success {
				status = 1
			} else {
				status = 2
			}
		}

		if tx.Effects.GasUsed != nil {
			gu := tx.Effects.GasUsed
			var compCost, storageCost, storageRebate int64
			if gu.ComputationCost != nil {
				compCost = int64(*gu.ComputationCost)
			}
			if gu.StorageCost != nil {
				storageCost = int64(*gu.StorageCost)
			}
			if gu.StorageRebate != nil {
				storageRebate = int64(*gu.StorageRebate)
			}
			gasFee = compCost + storageCost - storageRebate
			if gasFee < 0 {
				gasFee = 0
			}
		}
	}

	balanceChangesJSON = transactionBalanceChangesJSON(tx.BalanceChanges)
	eventsJSON = transactionEventsJSON(tx.Events)

	parsedTx := models.SuiTransaction{
		Digest:                   txDigest,
		CheckpointSequenceNumber: uint64(checkpointSeqNum),
		Timestamp:                ts,
		Sender:                   sender,
		Status:                   status,
		KindTypename:             kindTypename,
		CommandsJSON:             commandsJSON,
		InputsJSON:               inputsJSON,
		EventsJSON:               eventsJSON,
		BalanceChangesJSON:       balanceChangesJSON,
		GasFee:                   gasFee,
	}

	var changedObjects []*v2.ChangedObject
	if tx.Effects != nil {
		changedObjects = tx.Effects.ChangedObjects
	}
	txObjs := transactionObjectsFromChanges(txDigest, ts, changedObjects)

	return parsedTx, txObjs, nil
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func withRetry(label string, fn func() error) error {
	return withRetryContext(context.Background(), label, fn)
}

func withRetryContext(ctx context.Context, label string, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%s canceled: %w", label, err)
		}
		if attempt > 0 {
			delay := backoffDelay(attempt)
			fmt.Printf("[%s] Retry %d/%d after %v, last err: %v\n", label, attempt, maxRetries, delay, lastErr)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return fmt.Errorf("%s canceled: %w", label, ctx.Err())
			case <-timer.C:
			}
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if isAdaptiveBatchSplitError(lastErr) || isUnparseableTypeError(lastErr) {
			return lastErr
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%s canceled: %w", label, err)
		}
		if attempt == 0 {
			fmt.Printf("[%s] First attempt failed: %v\n", label, lastErr)
		}
	}
	fmt.Printf("[%s] All %d retries exhausted, last err: %v\n", label, maxRetries, lastErr)
	return fmt.Errorf("%s failed after %d retries: %w", label, maxRetries, lastErr)
}

func backoffDelay(attempt int) time.Duration {
	baseMs := math.Min(float64(initialBackoffMs)*math.Pow(2, float64(attempt-1)), float64(maxBackoffMs))
	jitterMs := rand.Float64() * 1000
	return time.Duration(baseMs+jitterMs) * time.Millisecond
}

func isGatewayTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "504") && strings.Contains(message, "Gateway Timeout")
}

func isArchiveReadRowsRequestTooLargeError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "Received ReadRowsRequest message too large") &&
		strings.Contains(message, "maximum allowed: 524288")
}

func isGRPCMessageTooLargeError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "grpc: received message larger than max")
}

func isAdaptiveBatchSplitError(err error) bool {
	return isArchiveReadRowsRequestTooLargeError(err) || isGRPCMessageTooLargeError(err)
}
