package storage

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"sui-crawler/internal/models"
)

const (
	insertBatchSize  = 5000
	maxRetries       = 10
	initialBackoffMs = 1000
	maxBackoffMs     = 60000
)

// ClickHouseStorage provides batch insert operations for SUI data into ClickHouse cloud.
type ClickHouseStorage struct {
	conn driver.Conn
}

// NewClickHouseStorage creates a new ClickHouse connection.
func NewClickHouseStorage(addr, database, username, password string) (*ClickHouseStorage, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 10 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse connection: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}

	return &ClickHouseStorage{conn: conn}, nil
}

// Close closes the ClickHouse connection.
func (s *ClickHouseStorage) Close() error {
	return s.conn.Close()
}

// InsertCheckpoints inserts checkpoint rows in batches.
func (s *ClickHouseStorage) InsertCheckpoints(ctx context.Context, rows []models.SuiCheckpoint) error {
	if len(rows) == 0 {
		return nil
	}
	for i := 0; i < len(rows); i += insertBatchSize {
		end := i + insertBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[i:end]
		if err := withRetry(fmt.Sprintf("insertCheckpoints [%d:%d]", i, end), func() error {
			return s.insertCheckpointBatch(ctx, chunk)
		}); err != nil {
			return err
		}
	}
	return nil
}

// InsertTransactions inserts transaction rows in batches.
func (s *ClickHouseStorage) InsertTransactions(ctx context.Context, rows []models.SuiTransaction) error {
	if len(rows) == 0 {
		return nil
	}
	for i := 0; i < len(rows); i += insertBatchSize {
		end := i + insertBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[i:end]
		if err := withRetry(fmt.Sprintf("insertTransactions [%d:%d]", i, end), func() error {
			return s.insertTransactionBatch(ctx, chunk)
		}); err != nil {
			return err
		}
	}
	return nil
}

// InsertTransactionObjects inserts transaction object rows in batches.
func (s *ClickHouseStorage) InsertTransactionObjects(ctx context.Context, rows []models.SuiTransactionObject) error {
	if len(rows) == 0 {
		return nil
	}
	for i := 0; i < len(rows); i += insertBatchSize {
		end := i + insertBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[i:end]
		if err := withRetry(fmt.Sprintf("insertTransactionObjects [%d:%d]", i, end), func() error {
			return s.insertTransactionObjectBatch(ctx, chunk)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *ClickHouseStorage) insertCheckpointBatch(ctx context.Context, rows []models.SuiCheckpoint) error {
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO sui_checkpoints (
		sequence_number, digest, previous_checkpoint_digest,
		network_total_transactions, timestamp
	)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, r := range rows {
		if err := batch.Append(
			r.SequenceNumber, r.Digest, r.PreviousCheckpointDigest,
			r.NetworkTotalTransactions, r.Timestamp,
		); err != nil {
			return fmt.Errorf("append checkpoint row: %w", err)
		}
	}

	return batch.Send()
}

func (s *ClickHouseStorage) insertTransactionBatch(ctx context.Context, rows []models.SuiTransaction) error {
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO sui_transactions (
		digest, checkpoint_sequence_number, timestamp,
		sender, status, kind_typename,
		commands_json, events_json, balance_changes_json, gas_fee
	)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, r := range rows {
		if err := batch.Append(
			r.Digest, r.CheckpointSequenceNumber, r.Timestamp,
			r.Sender, r.Status, r.KindTypename,
			r.CommandsJSON, r.EventsJSON, r.BalanceChangesJSON, r.GasFee,
		); err != nil {
			return fmt.Errorf("append transaction row: %w", err)
		}
	}

	return batch.Send()
}

func (s *ClickHouseStorage) insertTransactionObjectBatch(ctx context.Context, rows []models.SuiTransactionObject) error {
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO sui_transaction_objects (
		object_id, version, transaction_digest,
		input_version, input_owner, input_digest,
		output_version, output_owner, output_digest,
		is_created, is_deleted, timestamp
	)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, r := range rows {
		if err := batch.Append(
			r.ObjectID, r.Version, r.TransactionDigest,
			r.InputVersion, r.InputOwner, r.InputDigest,
			r.OutputVersion, r.OutputOwner, r.OutputDigest,
			r.IsCreated, r.IsDeleted, r.Timestamp,
		); err != nil {
			return fmt.Errorf("append transaction object row: %w", err)
		}
	}

	return batch.Send()
}

func withRetry(label string, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			delay := backoffDelay(attempt)
			fmt.Printf("[ClickHouse] %s failed: %v. Retrying in %.1fs...\n", label, lastErr, delay.Seconds())
			time.Sleep(delay)
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}
	}
	return fmt.Errorf("%s failed after %d retries: %w", label, maxRetries, lastErr)
}

func backoffDelay(attempt int) time.Duration {
	baseMs := math.Min(float64(initialBackoffMs)*math.Pow(2, float64(attempt-1)), float64(maxBackoffMs))
	jitterMs := rand.Float64() * 1000
	return time.Duration(baseMs+jitterMs) * time.Millisecond
}
