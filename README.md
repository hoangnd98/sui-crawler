# SUI Historical Data Crawler

A single-worker crawler that fetches historical Sui checkpoint data from Sui APIs, maps it into ClickHouse models, and stores it for analytical queries.

## Architecture

The crawler uses a **Job Orchestrator Pattern** to manage data ingestion tasks efficiently and reliably:

1.  **MongoDB**: Acts as the central job queue and state store.
2.  **REST API (Gin)**: Provides endpoints to create, list, and monitor crawling jobs.
3.  **Orchestrator**: A single control loop that polls MongoDB for pending jobs, dispatches them to the crawler worker, and tracks progress.
4.  **Crawler Worker**: A single worker that fetches checkpoint batches from Sui GraphQL, falls back to archive gRPC when GraphQL data is incomplete, transforms the results, and writes them to ClickHouse.
5.  **ClickHouse**: The analytical store for checkpoints, transactions, and transaction objects.

## Prerequisites

-   **Go**: 1.25 or later
-   **MongoDB**: v6.0+ (Local or Atlas) for job orchestration and state tracking
-   **ClickHouse**: For analytical data storage
-   **Sui RPC/gRPC endpoint**: Access to a Sui endpoint (defaults to `https://archive.mainnet.sui.io`)

## Setup & Configuration

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd sui-crawler
    ```

2.  **Environment Variables:**
    Copy `.env.sample` to `.env` and adjust the settings:
    ```bash
    cp .env.sample .env
    ```

    **Key Configuration Options:**
    *   `MONGO_URI`: MongoDB connection string (e.g., `mongodb://localhost:27019`)
    *   `MONGO_DB`: MongoDB database name (e.g., `sui_crawler`)
    *   `API_PORT`: Port for the REST API and Swagger UI (default `8080`)
*   `SUI_RPC_URL`: SUI gRPC endpoint
*   `SUI_GRAPHQL_URL`: Sui GraphQL endpoint used for batched checkpoint discovery
*   `SUI_GRAPHQL_RATE_LIMIT_RPS`: GraphQL request budget
*   `CLICK_HOUSE_*`: ClickHouse connection details

## Fetch Strategy

- The worker batches GraphQL checkpoint discovery in windows of `10` checkpoints using the `checkpoints(filter: ...)` query shape.
- Each GraphQL checkpoint node reads up to `50` transaction digests directly from `transactions(first: 50)`.
- If GraphQL omits a checkpoint or returns `hasNextPage=true` for that checkpoint, the crawler falls back to archive gRPC `GetCheckpoint` for that checkpoint only.
- Transaction hydration still runs through archive gRPC `BatchGetTransactions` after digest discovery completes.
- In `github.com/open-move/sui-go-sdk v0.0.1`, transaction balance changes are returned on top-level `ExecutedTransaction.balance_changes`, so gRPC transaction read masks must explicitly request `balance_changes`.

3.  **Database Initialization (ClickHouse):**
    You must create the necessary tables in ClickHouse. Execute the SQL scripts found in the `schema/` directory against your ClickHouse instance:
    *   `schema/sui_checkpoints.sql`
    *   `schema/sui_transactions.sql`
    *   `schema/sui_events.sql`

## Running the Crawler

1.  **Install Dependencies:**
    ```bash
    go mod download
    ```

2.  **Build / Rebuild the Service:**
    ```bash
    go build -o sui-crawler ./cmd/crawler/
    ```

3.  **Start the Service:**
    ```bash
    ./sui-crawler
    ```
    The service will start the MongoDB connection, launch the crawler worker, start the orchestrator, and serve the REST API.

4.  **Open Swagger UI:**
    Visit `http://localhost:8080/swagger/` to create and inspect crawler jobs from the browser.

## API Usage & Job Management

You interact with the crawler by submitting jobs via the REST API.

### Creating a Job
To start crawling a range of checkpoints, submit a `POST` request to `/api/v1/jobs`:

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "fromCheckpoint": 0,
    "endCheckpoint": 1000
  }'
```
*   `fromCheckpoint`: Starting checkpoint sequence number.
*   `endCheckpoint`: Ending checkpoint sequence number (inclusive).
*   `lastCheckpoint`: Set internally to `fromCheckpoint` when the job is created.

### Retrying a Job
If a job fails or you want to re-run a completed job from its last position, submit a `POST` request to `/api/v1/jobs/:id/retry`:

```bash
curl -X POST http://localhost:8080/api/v1/jobs/682a1b2c3d4e5f6a7b8c9d0e/retry
```
This will:
- Reset the status to `pending`.
- Clear any previous error messages.
- Allow the orchestrator to re-assign the job to the crawler worker.
- The worker will automatically resume from `lastCheckpoint + 1`.
- Internal chunking and RPC parallelism are handled automatically by the crawler based on built-in rate-limit-aware settings.

### Job Lifecycle and Resiliency
Jobs transition through the following states: `pending` → `progressing` → `completed`.
-   **Crash Recovery**: On startup, the Orchestrator automatically resets any jobs that were stuck in the `progressing` state back to `pending`.
-   **Resumability**: As the crawler worker completes internal processing chunks, `lastCheckpoint` is updated in MongoDB. If a job is interrupted and resumed, the worker will automatically start from `lastCheckpoint + 1`.
