package worker

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	v2 "github.com/open-move/sui-go-sdk/proto/sui/rpc/v2"
	"go.mongodb.org/mongo-driver/v2/bson"

	"sui-crawler/internal/client"
	"sui-crawler/internal/models"
	"sui-crawler/internal/storage"
)

const (
	// processingChunkSize controls how many checkpoints a job advances before
	// persisting progress. Larger chunks reduce MongoDB chatter while still
	// bounding replay after failures.
	processingChunkSize = 500
	// transactionHydrationBatchSize caps one BatchGetTransactions RPC.
	transactionHydrationBatchSize = 200
	// checkpointFetchBatchSize groups multiple checkpoints into a single GraphQL request.
	checkpointFetchBatchSize = 10
	// checkpointWriteBatchSize controls how many completed checkpoints are buffered
	// before flushing to ClickHouse.
	checkpointWriteBatchSize = 20
	// checkpointWriteFlushInterval bounds write latency when throughput is low.
	checkpointWriteFlushInterval   = 500 * time.Millisecond
	maxCheckpointFetchParallelism  = 8
	maxTransactionHydrationWorkers = 10
)

// Worker is the single crawler execution unit that receives jobs and processes checkpoint batches.
type Worker struct {
	id       string
	assignCh chan models.JobAssignment
	reportCh chan models.JobReport
	cfg      WorkerConfig
}

// NewWorker creates a new worker.
func NewWorker(
	id string,
	assignCh chan models.JobAssignment,
	reportCh chan models.JobReport,
	cfg WorkerConfig,
) *Worker {
	return &Worker{
		id:       id,
		assignCh: assignCh,
		reportCh: reportCh,
		cfg:      cfg,
	}
}

// Run starts the worker loop. It waits for assignments and processes them until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	log.Printf("[%s] Started", w.id)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] Stopped", w.id)
			return
		case assignment := <-w.assignCh:
			w.processJob(ctx, assignment)
		}
	}
}

func (w *Worker) processJob(ctx context.Context, assignment models.JobAssignment) {
	job := assignment.Job
	startCheckpoint := job.LastCheckpoint + 1
	if startCheckpoint < job.FromCheckpoint {
		startCheckpoint = job.FromCheckpoint
	}

	log.Printf("[%s] Processing job %s range [%d -> %d]",
		w.id, job.ID.Hex(), startCheckpoint, job.EndCheckpoint)

	startTime := time.Now()

	suiClient, err := client.NewSuiClient(ctx, w.cfg.SuiRPCURL, w.cfg.RateLimiter)
	if err != nil {
		w.sendReport(models.JobReport{
			JobID: job.ID,
			Type:  models.ReportJobFailed,
			Error: fmt.Errorf("create sui client: %w", err),
		})
		return
	}
	defer suiClient.Close()
	suiClient.SetRPCTimeout(w.cfg.RPCTimeout)
	suiClient.SetGraphQLURL(w.cfg.SuiGraphQLURL)
	suiClient.SetGraphQLRateLimiter(w.cfg.GraphQLLimiter)

	chStorage, err := storage.NewClickHouseStorage(
		w.cfg.CHAddr, w.cfg.CHDatabase, w.cfg.CHUsername, w.cfg.CHPassword,
	)
	if err != nil {
		w.sendReport(models.JobReport{
			JobID: job.ID,
			Type:  models.ReportJobFailed,
			Error: fmt.Errorf("connect to clickhouse: %w", err),
		})
		return
	}
	defer chStorage.Close()

	err = w.processBatchesSequential(ctx, suiClient, chStorage, job, startCheckpoint, job.EndCheckpoint)
	if err != nil {
		w.sendReport(models.JobReport{
			JobID: job.ID,
			Type:  models.ReportJobFailed,
			Error: err,
		})
		return
	}

	elapsed := time.Since(startTime)
	log.Printf("[%s] Job %s range [%d -> %d] completed in %.2fs",
		w.id, job.ID.Hex(), startCheckpoint, job.EndCheckpoint, elapsed.Seconds())

	w.sendReport(models.JobReport{
		JobID: job.ID,
		Type:  models.ReportJobDone,
	})
}

func (w *Worker) processBatchesSequential(
	ctx context.Context,
	suiClient client.CheckpointFetcher,
	chStorage *storage.ClickHouseStorage,
	job *models.CrawlerJob,
	startPos int64,
	endPos int64,
) error {
	chunkSize := int64(processingChunkSize)
	fetchConcurrency := w.checkpointFetchConcurrency()
	hydrationConcurrency := w.transactionHydrationConcurrency(fetchConcurrency)

	for batchStart := startPos; batchStart <= endPos; batchStart += chunkSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batchEnd := batchStart + chunkSize - 1
		if batchEnd > endPos {
			batchEnd = endPos
		}

		log.Printf("[%s] Job %s — chunk [%d -> %d] fetch_parallelism=%d tx_parallelism=%d",
			w.id, job.ID.Hex(), batchStart, batchEnd, fetchConcurrency, hydrationConcurrency)

		if err := w.processCheckpointRange(ctx, suiClient, chStorage, job.ID, batchStart, batchEnd, fetchConcurrency, hydrationConcurrency); err != nil {
			return fmt.Errorf("chunk [%d-%d]: %w", batchStart, batchEnd, err)
		}
	}

	return nil
}

func (w *Worker) processCheckpointRange(
	ctx context.Context,
	fetcher client.CheckpointFetcher,
	chStorage *storage.ClickHouseStorage,
	jobID bson.ObjectID,
	from, to int64,
	checkpointConcurrency int,
	hydrationConcurrency int,
) error {
	pipelineCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	batchCh := make(chan checkpointBatchTask, checkpointConcurrency)
	checkpointDataCh := make(chan checkpointDataTask, checkpointConcurrency*checkpointFetchBatchSize)
	hydrationBatchCh := make(chan hydrationBatchTask, hydrationConcurrency)
	checkpointResultCh := make(chan checkpointResultTask, hydrationConcurrency*checkpointFetchBatchSize)
	errCh := make(chan error, 1)

	fail := func(err error) {
		if err == nil {
			return
		}
		select {
		case errCh <- err:
		default:
		}
		cancel()
	}

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- w.writeCheckpointStream(pipelineCtx, chStorage, jobID, from, checkpointResultCh)
	}()

	var fetchWG sync.WaitGroup
	for i := 0; i < checkpointConcurrency; i++ {
		fetchWG.Add(1)
		go func() {
			defer fetchWG.Done()
			for task := range batchCh {
				results, err := fetcher.GetCheckpointDataBatch(pipelineCtx, task.seqNums)
				if err != nil {
					fail(fmt.Errorf("fetch checkpoint batch [%d-%d]: %w", task.seqNums[0], task.seqNums[len(task.seqNums)-1], err))
					return
				}
				for _, seq := range task.seqNums {
					data := results[seq]
					if data == nil {
						fail(fmt.Errorf("checkpoint %d missing from batch fetch", seq))
						return
					}
					select {
					case checkpointDataCh <- checkpointDataTask{seq: seq, data: data}:
					case <-pipelineCtx.Done():
						return
					}
				}
			}
		}()
	}

	go func() {
		defer close(batchCh)
		for start := from; start <= to; start += checkpointFetchBatchSize {
			end := start + checkpointFetchBatchSize - 1
			if end > to {
				end = to
			}
			seqNums := make([]int64, 0, end-start+1)
			for seq := start; seq <= end; seq++ {
				seqNums = append(seqNums, seq)
			}
			select {
			case batchCh <- checkpointBatchTask{seqNums: seqNums}:
			case <-pipelineCtx.Done():
				return
			}
		}
	}()

	go func() {
		fetchWG.Wait()
		close(checkpointDataCh)
	}()

	go func() {
		defer close(hydrationBatchCh)
		var batch []checkpointDataTask
		batchDigests := 0
		flush := func() bool {
			if len(batch) == 0 {
				return true
			}
			out := make([]checkpointDataTask, len(batch))
			copy(out, batch)
			select {
			case hydrationBatchCh <- hydrationBatchTask{items: out}:
				batch = nil
				batchDigests = 0
				return true
			case <-pipelineCtx.Done():
				return false
			}
		}

		for task := range checkpointDataCh {
			taskDigests := len(task.data.TransactionDigests)
			if len(batch) > 0 && batchDigests+taskDigests > transactionHydrationBatchSize {
				if !flush() {
					return
				}
			}
			batch = append(batch, task)
			batchDigests += taskDigests
			if batchDigests >= transactionHydrationBatchSize {
				if !flush() {
					return
				}
			}
		}
		flush()
	}()

	var hydrateWG sync.WaitGroup
	for i := 0; i < hydrationConcurrency; i++ {
		hydrateWG.Add(1)
		go func() {
			defer hydrateWG.Done()
			for batch := range hydrationBatchCh {
				results, err := w.hydrateCheckpointBatch(pipelineCtx, fetcher, batch.items)
				if err != nil {
					fail(err)
					return
				}
				for _, result := range results {
					select {
					case checkpointResultCh <- result:
					case <-pipelineCtx.Done():
						return
					}
				}
			}
		}()
	}

	go func() {
		hydrateWG.Wait()
		close(checkpointResultCh)
	}()

	select {
	case err := <-writerDone:
		if err != nil {
			return err
		}
	case err := <-errCh:
		<-writerDone
		return err
	}

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

type checkpointBatchTask struct {
	seqNums []int64
}

type checkpointDataTask struct {
	seq  int64
	data *client.CheckpointData
}

type checkpointResultTask struct {
	seq    int64
	result *client.CheckpointResult
}

type hydrationBatchTask struct {
	items []checkpointDataTask
}

func hydrateTransactionsByDigest(
	ctx context.Context,
	fetcher client.CheckpointFetcher,
	digests []string,
) (map[string]*v2.ExecutedTransaction, error) {
	if len(digests) == 0 {
		return nil, nil
	}

	txByDigest := make(map[string]*v2.ExecutedTransaction, len(digests))
	for start := 0; start < len(digests); start += transactionHydrationBatchSize {
		end := start + transactionHydrationBatchSize
		if end > len(digests) {
			end = len(digests)
		}
		fullTxs, err := fetcher.BatchGetTransactions(ctx, digests[start:end])
		if err != nil {
			return nil, fmt.Errorf("hydrate transactions [%d:%d]: %w", start, end, err)
		}
		for _, tx := range fullTxs {
			if tx == nil {
				return nil, fmt.Errorf("hydrated transaction is nil")
			}
			txByDigest[tx.GetDigest()] = tx
		}
	}
	return txByDigest, nil
}

func orderedTransactionsForDigests(txByDigest map[string]*v2.ExecutedTransaction, digests []string) ([]*v2.ExecutedTransaction, error) {
	if len(digests) == 0 {
		return nil, nil
	}
	fullTxs := make([]*v2.ExecutedTransaction, 0, len(digests))
	for _, digest := range digests {
		tx, ok := txByDigest[digest]
		if !ok {
			return nil, fmt.Errorf("missing hydrated transaction %s", digest)
		}
		fullTxs = append(fullTxs, tx)
	}
	return fullTxs, nil
}

func (w *Worker) writeToClickHouse(
	ctx context.Context,
	chStorage *storage.ClickHouseStorage,
	checkpoints []models.SuiCheckpoint,
	transactions []models.SuiTransaction,
	transactionObjects []models.SuiTransactionObject,
) error {
	if err := chStorage.InsertCheckpoints(ctx, checkpoints); err != nil {
		return fmt.Errorf("insert checkpoints: %w", err)
	}
	if err := chStorage.InsertTransactions(ctx, transactions); err != nil {
		return fmt.Errorf("insert transactions: %w", err)
	}
	if err := chStorage.InsertTransactionObjects(ctx, transactionObjects); err != nil {
		return fmt.Errorf("insert transaction objects: %w", err)
	}
	log.Printf("[%s] ClickHouse insert: checkpoints=%d, transactions=%d, transaction_objects=%d",
		w.id, len(checkpoints), len(transactions), len(transactionObjects))
	return nil
}

func (w *Worker) sendReport(report models.JobReport) {
	w.reportCh <- report
}

func (w *Worker) hydrateCheckpoint(
	ctx context.Context,
	fetcher client.CheckpointFetcher,
	seq int64,
	data *client.CheckpointData,
) (*client.CheckpointResult, error) {
	txByDigest, err := hydrateTransactionsByDigest(ctx, fetcher, data.TransactionDigests)
	if err != nil {
		return nil, fmt.Errorf("checkpoint %d: %w", seq, err)
	}

	fullTxs, err := orderedTransactionsForDigests(txByDigest, data.TransactionDigests)
	if err != nil {
		return nil, fmt.Errorf("checkpoint %d: %w", seq, err)
	}

	result, err := data.BuildResult(fullTxs)
	if err != nil {
		return nil, fmt.Errorf("checkpoint %d: %w", seq, err)
	}

	if len(result.Transactions) > 0 {
		log.Printf("[%s] Checkpoint %d ready: tx=%d tx_objects=%d",
			w.id, seq, len(result.Transactions), len(result.TransactionObjects))
	}

	return result, nil
}

func (w *Worker) hydrateCheckpointBatch(
	ctx context.Context,
	fetcher client.CheckpointFetcher,
	items []checkpointDataTask,
) ([]checkpointResultTask, error) {
	allDigests := make([]string, 0)
	for _, item := range items {
		allDigests = append(allDigests, item.data.TransactionDigests...)
	}

	txByDigest, err := hydrateTransactionsByDigest(ctx, fetcher, allDigests)
	if err != nil {
		return nil, err
	}

	results := make([]checkpointResultTask, 0, len(items))
	for _, item := range items {
		fullTxs, err := orderedTransactionsForDigests(txByDigest, item.data.TransactionDigests)
		if err != nil {
			return nil, fmt.Errorf("checkpoint %d: %w", item.seq, err)
		}

		result, err := item.data.BuildResult(fullTxs)
		if err != nil {
			return nil, fmt.Errorf("checkpoint %d: %w", item.seq, err)
		}

		if len(result.Transactions) > 0 {
			log.Printf("[%s] Checkpoint %d ready: tx=%d tx_objects=%d",
				w.id, item.seq, len(result.Transactions), len(result.TransactionObjects))
		}

		results = append(results, checkpointResultTask{seq: item.seq, result: result})
	}

	return results, nil
}

func (w *Worker) writeCheckpointStream(
	ctx context.Context,
	chStorage *storage.ClickHouseStorage,
	jobID bson.ObjectID,
	startSeq int64,
	results <-chan checkpointResultTask,
) error {
	ticker := time.NewTicker(checkpointWriteFlushInterval)
	defer ticker.Stop()

	pending := make(map[int64]*client.CheckpointResult)
	flushUntil := startSeq - 1
	reportedUntil := startSeq - 1

	var checkpoints []models.SuiCheckpoint
	var transactions []models.SuiTransaction
	var transactionObjects []models.SuiTransactionObject

	flush := func() error {
		if len(checkpoints) == 0 && len(transactions) == 0 && len(transactionObjects) == 0 {
			return nil
		}
		if err := w.writeToClickHouse(ctx, chStorage, checkpoints, transactions, transactionObjects); err != nil {
			return err
		}
		checkpoints = nil
		transactions = nil
		transactionObjects = nil
		if flushUntil > reportedUntil {
			reportedUntil = flushUntil
			w.sendReport(models.JobReport{
				JobID:    jobID,
				Type:     models.ReportBatchComplete,
				Position: reportedUntil,
			})
		}
		return nil
	}

	queueContiguous := func() {
		for {
			nextSeq := flushUntil + 1
			result := pending[nextSeq]
			if result == nil {
				return
			}
			delete(pending, nextSeq)
			checkpoints = append(checkpoints, result.Checkpoint)
			transactions = append(transactions, result.Transactions...)
			transactionObjects = append(transactionObjects, result.TransactionObjects...)
			flushUntil = nextSeq
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task, ok := <-results:
			if !ok {
				queueContiguous()
				return flush()
			}
			pending[task.seq] = task.result
			queueContiguous()
			if len(checkpoints) >= checkpointWriteBatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

func (w *Worker) checkpointFetchConcurrency() int {
	rps := w.cfg.SuiGraphQLRPS
	if rps <= 0 {
		return 1
	}

	concurrency := int(math.Ceil(rps))
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > maxCheckpointFetchParallelism {
		concurrency = maxCheckpointFetchParallelism
	}
	return concurrency
}

func (w *Worker) transactionHydrationConcurrency(fetchConcurrency int) int {
	concurrency := int(math.Ceil(w.cfg.SuiRateRPS))
	if concurrency == 0 {
		concurrency = fetchConcurrency * 2
	}
	if concurrency < 2 {
		concurrency = 2
	}
	if concurrency > maxTransactionHydrationWorkers {
		concurrency = maxTransactionHydrationWorkers
	}
	return concurrency
}
