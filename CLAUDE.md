## What This Project Does

A high-performance Go service that crawls historical SUI blockchain data via the official Sui APIs, maps it into ClickHouse models, and stores it for analytical queries. Jobs are managed through a REST API backed by MongoDB.

## Commands

```bash
# Install dependencies
go mod download

# Build
go build -o sui-crawler ./cmd/crawler/

# Run
./sui-crawler

# Run all tests
go test ./...

# Run a single package's tests
go test ./internal/processor/...
```

## Architecture

The system follows a **Job Orchestrator Pattern** with three concurrent actors communicating via channels:

```
REST API (Gin) ──creates jobs──▶ MongoDB
                                    ▲
Orchestrator ──polls──▶ MongoDB     │ progress updates
     │                              │
     └──dispatches via channel──▶ Single Worker ──fetches──▶ Sui GraphQL + archive gRPC
                                        │
                                        └──writes──▶ ClickHouse
```

### Key packages

| Package | Responsibility |
|---------|---------------|
| `internal/orchestrator` | Single control loop: polls MongoDB for `pending` jobs, claims one job, dispatches it to the worker, and updates `last_checkpoint` as persisted batches are reported |
| `internal/worker` | Single crawler worker that runs a streaming pipeline: GraphQL checkpoint batch fetch, shared gRPC transaction hydration, ordered ClickHouse flush, and progress reporting |
| `internal/client` | Sui client wrapper using `github.com/open-move/sui-go-sdk`; uses Sui GraphQL `checkpoints(filter: ...)` for checkpoint/digest discovery and archive gRPC for `BatchGetTransactions` and fallback reads |
| `internal/storage` | ClickHouse (`clickhouse-go/v2`) batch inserts for checkpoints, transactions, and transaction objects |
| `internal/repository` | MongoDB CRUD for `CrawlerJob` — claims, progress updates, stalled-job reset on startup |
| `internal/api` | Gin router exposing `POST /api/v1/jobs`, `GET /api/v1/jobs`, `GET /api/v1/jobs/:id`, `POST /api/v1/jobs/:id/retry` |
| `internal/models` | Shared types: `CrawlerJob`, `JobAssignment`, `JobReport`, `SuiCheckpoint`, `SuiTransaction`, `SuiTransactionObject` |

### Runtime shape

- There is one orchestrator and one crawler worker.
- Jobs no longer carry `batchSize`, `progress`, `worker_id`, or `concurrency`.
- New jobs are created with `lastCheckpoint = fromCheckpoint`.
- The worker starts at `fromCheckpoint` for fresh jobs and resumes from `lastCheckpoint + 1` for retried/interrupted jobs.

### Worker pipeline

1. Receives `JobAssignment` with the persisted job range.
2. Splits work into internal processing chunks of `500` checkpoints.
3. Fetches checkpoint metadata from Sui GraphQL in batches of `10` checkpoints using `checkpoints(filter: ...)`.
4. Reads up to `50` transaction digests per checkpoint from GraphQL and falls back to archive gRPC `GetCheckpoint` when a checkpoint is incomplete or missing from the GraphQL batch response.
5. Aggregates digests across multiple checkpoints and hydrates them through archive gRPC `BatchGetTransactions` in shared batches of up to `5` digests, with adaptive split if the archive rejects a request as too large or the aggregate response exceeds gRPC receive limits.
6. Buffers completed checkpoints until they are contiguous, then flushes ordered rows to ClickHouse.
7. Reports `ReportBatchComplete` only after data is persisted, so MongoDB `last_checkpoint` reflects durable progress.

### Sui gRPC transaction mapping note

- In `github.com/open-move/sui-go-sdk v0.0.1`, transaction balance changes come from top-level `ExecutedTransaction.balance_changes`, not `TransactionEffects`.
- Any gRPC transaction hydration read mask used for ClickHouse transaction mapping must include `balance_changes`, or `balance_changes_json` will be empty even when GraphQL shows balance changes.

### Rate limits

- GraphQL checkpoint fetch is budgeted separately via `SUI_GRAPHQL_RATE_LIMIT_RPS` and defaults to `3.0`.
- Archive gRPC is budgeted via `SUI_RATE_LIMIT_RPS` and defaults to `10.0`.
- All gRPC request types share a global in-flight semaphore capped at `10` concurrent requests to avoid `429 Too Many Requests`.
- `BatchGetTransactions` also uses an internal batch semaphore to avoid overdriving the archive endpoint with transaction hydration bursts.

### Channel wiring

- `assignCh chan JobAssignment` — the orchestrator dispatches one claimed job at a time to the worker
- `reportCh chan JobReport` — the worker pushes progress/done/failed reports back to the orchestrator

## Configuration (`.env`)

| Variable | Default | Purpose |
|----------|---------|---------|
| `MONGO_URI` | `mongodb://localhost:27019` | MongoDB connection |
| `MONGO_DB` | `sui_crawler` | Database name |
| `API_PORT` | `8080` | REST API + Swagger port |
| `SUI_RPC_URL` | `https://archive.mainnet.sui.io` | Archive Sui gRPC endpoint used for backfill |
| `SUI_GRAPHQL_URL` | `https://graphql.mainnet.sui.io/graphql` | Sui GraphQL endpoint used for checkpoint discovery |
| `SUI_RATE_LIMIT_RPS` | `10.0` | Archive gRPC request budget |
| `SUI_GRAPHQL_RATE_LIMIT_RPS` | `3.0` | GraphQL request budget |
| `CLICK_HOUSE_ADDR` | `localhost:9000` | ClickHouse native TCP |
| `CLICK_HOUSE_DB` | `default` | ClickHouse database |
| `CLICK_HOUSE_USR` | `default` | ClickHouse username |
| `CLICK_HOUSE_PWD` | _(empty)_ | ClickHouse password |

## ClickHouse Schema

Initialize tables before first run:

```bash
clickhouse-client < schema/sui_checkpoints.sql
clickhouse-client < schema/sui_transactions.sql
clickhouse-client < schema/sui_events.sql
```

## Agent Documentation

Project decisions, architecture docs, and task logs live in `.agent/`. Read `.agent/README.md` before planning any non-trivial change.
