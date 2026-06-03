package repository

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"sui-crawler/internal/models"
)

const degradedCollectionName = "degraded_transactions"

// DegradedTransactionRepository persists transactions the Sui archive cannot
// fully hydrate, so data gaps stay visible and reconcilable.
type DegradedTransactionRepository struct {
	collection *mongo.Collection
}

// NewDegradedTransactionRepository creates the repository and ensures the
// unique digest index exists.
func NewDegradedTransactionRepository(db *mongo.Database) *DegradedTransactionRepository {
	repo := &DegradedTransactionRepository{
		collection: db.Collection(degradedCollectionName),
	}
	repo.ensureIndexes()
	return repo
}

// Record upserts a degraded transaction keyed by digest. Re-crawling the same
// range (job retry/resume) updates the existing record instead of duplicating.
func (r *DegradedTransactionRepository) Record(ctx context.Context, dt *models.DegradedTransaction) error {
	filter := bson.D{{Key: "digest", Value: dt.Digest}}
	update := bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "job_id", Value: dt.JobID},
			{Key: "checkpoint_sequence_number", Value: dt.CheckpointSequenceNumber},
			{Key: "reason", Value: dt.Reason},
		}},
		{Key: "$setOnInsert", Value: bson.D{
			{Key: "created_at", Value: time.Now()},
		}},
	}
	opts := options.UpdateOne().SetUpsert(true)
	if _, err := r.collection.UpdateOne(ctx, filter, update, opts); err != nil {
		return fmt.Errorf("upsert degraded transaction %s: %w", dt.Digest, err)
	}
	return nil
}

func (r *DegradedTransactionRepository) ensureIndexes() {
	indexModel := mongo.IndexModel{
		Keys:    bson.D{{Key: "digest", Value: 1}},
		Options: options.Index().SetUnique(true),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := r.collection.Indexes().CreateOne(ctx, indexModel); err != nil {
		log.Printf("[DegradedTransactionRepository] Warning: failed to create index: %v", err)
	}
}
