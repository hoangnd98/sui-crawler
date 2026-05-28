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

const (
	collectionName = "crawler_jobs"
)

// JobRepository provides CRUD and lifecycle operations for CrawlerJob documents.
type JobRepository struct {
	collection *mongo.Collection
}

// NewJobRepository creates a new JobRepository and ensures required indexes exist.
func NewJobRepository(db *mongo.Database) *JobRepository {
	repo := &JobRepository{
		collection: db.Collection(collectionName),
	}
	repo.ensureIndexes()
	return repo
}

// CreateJob inserts a new job with pending status.
func (r *JobRepository) CreateJob(ctx context.Context, job *models.CrawlerJob) error {
	now := time.Now()
	job.Status = models.JobStatusPending
	job.CreatedAt = now
	job.UpdatedAt = now

	result, err := r.collection.InsertOne(ctx, job)
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}

	if oid, ok := result.InsertedID.(bson.ObjectID); ok {
		job.ID = oid
	}
	return nil
}

// FindPendingJobs returns all jobs in pending status, ordered by created_at ASC (oldest first).
func (r *JobRepository) FindPendingJobs(ctx context.Context) ([]*models.CrawlerJob, error) {
	filter := bson.D{{Key: "status", Value: models.JobStatusPending}}
	opts := options.Find().SetSort(bson.D{{Key: "created_at", Value: 1}})

	cursor, err := r.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("find pending jobs: %w", err)
	}
	defer cursor.Close(ctx)

	var jobs []*models.CrawlerJob
	if err := cursor.All(ctx, &jobs); err != nil {
		return nil, fmt.Errorf("decode pending jobs: %w", err)
	}
	return jobs, nil
}

// ClaimJob atomically transitions a pending job to progressing.
func (r *JobRepository) ClaimJob(ctx context.Context, jobID bson.ObjectID) error {
	filter := bson.D{
		{Key: "_id", Value: jobID},
		{Key: "status", Value: models.JobStatusPending},
	}
	update := bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "status", Value: models.JobStatusProgressing},
			{Key: "updated_at", Value: time.Now()},
		}},
		{Key: "$unset", Value: bson.D{
			{Key: "error", Value: ""},
		}},
	}

	result, err := r.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("claim job %s: %w", jobID.Hex(), err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("job %s is no longer pending", jobID.Hex())
	}
	return nil
}

// UpdateProgress updates the last_checkpoint of a progressing job.
func (r *JobRepository) UpdateProgress(ctx context.Context, jobID bson.ObjectID, checkpoint int64) error {
	filter := bson.D{
		{Key: "_id", Value: jobID},
		{Key: "status", Value: models.JobStatusProgressing},
	}
	update := bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "last_checkpoint", Value: checkpoint},
			{Key: "updated_at", Value: time.Now()},
		}},
	}

	_, err := r.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update progress for job %s: %w", jobID.Hex(), err)
	}
	return nil
}

// CompleteJob marks a progressing job as completed.
func (r *JobRepository) CompleteJob(ctx context.Context, jobID bson.ObjectID) error {
	now := time.Now()
	filter := bson.D{
		{Key: "_id", Value: jobID},
		{Key: "status", Value: models.JobStatusProgressing},
	}
	update := bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "status", Value: models.JobStatusCompleted},
			{Key: "updated_at", Value: now},
			{Key: "completed_at", Value: now},
		}},
		{Key: "$unset", Value: bson.D{
			{Key: "error", Value: ""},
		}},
	}

	_, err := r.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("complete job %s: %w", jobID.Hex(), err)
	}
	return nil
}

// RequeueJob resets a progressing job to pending with an error message.
func (r *JobRepository) RequeueJob(ctx context.Context, jobID bson.ObjectID, errMsg string) error {
	filter := bson.D{
		{Key: "_id", Value: jobID},
		{Key: "status", Value: models.JobStatusProgressing},
	}
	update := bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "status", Value: models.JobStatusPending},
			{Key: "error", Value: errMsg},
			{Key: "updated_at", Value: time.Now()},
		}},
		{Key: "$unset", Value: bson.D{
			{Key: "completed_at", Value: ""},
		}},
	}

	_, err := r.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("requeue job %s: %w", jobID.Hex(), err)
	}
	return nil
}

// ResetStalledJobs transitions any progressing jobs back to pending (for crash recovery on startup).
func (r *JobRepository) ResetStalledJobs(ctx context.Context) error {
	filter := bson.D{{Key: "status", Value: models.JobStatusProgressing}}
	update := bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "status", Value: models.JobStatusPending},
			{Key: "error", Value: "job reset after interrupted progress"},
			{Key: "updated_at", Value: time.Now()},
		}},
	}

	result, err := r.collection.UpdateMany(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("reset stalled jobs: %w", err)
	}
	if result.ModifiedCount > 0 {
		log.Printf("[Orchestrator] Reset %d stalled jobs to pending", result.ModifiedCount)
	}
	return nil
}

// GetJob fetches a single job by ID.
func (r *JobRepository) GetJob(ctx context.Context, jobID bson.ObjectID) (*models.CrawlerJob, error) {
	var job models.CrawlerJob
	err := r.collection.FindOne(ctx, bson.D{{Key: "_id", Value: jobID}}).Decode(&job)
	if err != nil {
		return nil, fmt.Errorf("get job %s: %w", jobID.Hex(), err)
	}
	return &job, nil
}

// RetryJob transitions a job back to pending to be picked up again, regardless of its current status.
func (r *JobRepository) RetryJob(ctx context.Context, jobID bson.ObjectID) error {
	filter := bson.D{
		{Key: "_id", Value: jobID},
	}
	update := bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "status", Value: models.JobStatusPending},
			{Key: "updated_at", Value: time.Now()},
		}},
		{Key: "$unset", Value: bson.D{
			{Key: "error", Value: ""},
			{Key: "completed_at", Value: ""},
		}},
	}

	result, err := r.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("retry job %s: %w", jobID.Hex(), err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("job %s not found", jobID.Hex())
	}

	log.Printf("[JobRepository] Job %s has been reset to pending for retry", jobID.Hex())
	return nil
}

// ListJobs returns jobs with an optional status filter. If status is empty, all jobs are returned.
func (r *JobRepository) ListJobs(ctx context.Context, status string) ([]*models.CrawlerJob, error) {
	filter := bson.D{}
	if status != "" {
		filter = bson.D{{Key: "status", Value: status}}
	}

	opts := options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}})
	cursor, err := r.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer cursor.Close(ctx)

	var jobs []*models.CrawlerJob
	if err := cursor.All(ctx, &jobs); err != nil {
		return nil, fmt.Errorf("decode jobs: %w", err)
	}
	return jobs, nil
}

func (r *JobRepository) ensureIndexes() {
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "status", Value: 1},
			{Key: "created_at", Value: 1},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := r.collection.Indexes().CreateOne(ctx, indexModel); err != nil {
		log.Printf("[JobRepository] Warning: failed to create index: %v", err)
	}
}
