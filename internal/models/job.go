package models

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// JobStatus represents the lifecycle state of a crawler job.
type JobStatus string

const (
	JobStatusPending     JobStatus = "pending"
	JobStatusProgressing JobStatus = "progressing"
	JobStatusCompleted   JobStatus = "completed"
)

// CrawlerJob represents a crawling job stored in MongoDB.
type CrawlerJob struct {
	ID             bson.ObjectID `bson:"_id,omitempty" json:"id"`
	FromCheckpoint int64         `bson:"from_checkpoint" json:"fromCheckpoint"`
	LastCheckpoint int64         `bson:"last_checkpoint" json:"lastCheckpoint"`
	EndCheckpoint  int64         `bson:"end_checkpoint" json:"endCheckpoint"`
	Status         JobStatus     `bson:"status" json:"status"`
	Error          string        `bson:"error,omitempty" json:"error,omitempty"`
	CreatedAt      time.Time     `bson:"created_at" json:"created_at"`
	UpdatedAt      time.Time     `bson:"updated_at" json:"updated_at"`
	CompletedAt    *time.Time    `bson:"completed_at,omitempty" json:"completed_at,omitempty"`
}

// ReportType identifies the kind of report a worker sends to the orchestrator.
type ReportType int

const (
	// ReportBatchComplete indicates a batch of checkpoints was processed successfully.
	ReportBatchComplete ReportType = iota
	// ReportJobDone indicates the entire job finished successfully.
	ReportJobDone
	// ReportJobFailed indicates the job encountered an unrecoverable error.
	ReportJobFailed
)

// JobAssignment is the message the Orchestrator sends to a Worker.
type JobAssignment struct {
	Job *CrawlerJob
}

// JobReport is the message a Worker sends back to the Orchestrator.
type JobReport struct {
	JobID    bson.ObjectID
	Type     ReportType
	Position int64 // latest completed checkpoint (for ReportBatchComplete)
	Error    error // non-nil only for ReportJobFailed
}
