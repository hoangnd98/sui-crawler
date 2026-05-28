package orchestrator

import (
	"context"
	"log"
	"time"

	"sui-crawler/internal/models"
	"sui-crawler/internal/repository"
)

const (
	pollInterval = 5 * time.Second
)

// Orchestrator polls MongoDB for pending jobs and dispatches them to the crawler worker.
type Orchestrator struct {
	repo       *repository.JobRepository
	reportCh   chan models.JobReport
	workerCh   chan models.JobAssignment
	workerID   string
	workerBusy bool
}

// NewOrchestrator creates a new Orchestrator.
func NewOrchestrator(
	repo *repository.JobRepository,
	reportCh chan models.JobReport,
	workerCh chan models.JobAssignment,
	workerID string,
) *Orchestrator {
	return &Orchestrator{
		repo:     repo,
		reportCh: reportCh,
		workerCh: workerCh,
		workerID: workerID,
	}
}

// Run starts the orchestrator's main loop. It blocks until ctx is cancelled.
func (o *Orchestrator) Run(ctx context.Context) error {
	if err := o.repo.ResetStalledJobs(ctx); err != nil {
		log.Printf("[Orchestrator] Warning: failed to reset stalled jobs: %v", err)
	}

	log.Println("[Orchestrator] Started")

	for {
		if o.workerBusy {
			select {
			case <-ctx.Done():
				log.Println("[Orchestrator] Shutting down")
				return ctx.Err()
			case report := <-o.reportCh:
				o.handleReport(ctx, report)
			}
			continue
		}

		select {
		case <-ctx.Done():
			log.Println("[Orchestrator] Shutting down")
			return ctx.Err()
		case report := <-o.reportCh:
			o.handleReport(ctx, report)
		default:
			dispatched := o.dispatchPendingJob(ctx)
			if !dispatched {
				o.waitOrCancel(ctx, pollInterval)
			}
		}
	}
}

func (o *Orchestrator) dispatchPendingJob(ctx context.Context) bool {
	jobs, err := o.repo.FindPendingJobs(ctx)
	if err != nil {
		log.Printf("[Orchestrator] Error finding pending jobs: %v", err)
		return false
	}

	for _, job := range jobs {
		startCheckpoint := resolveStartCheckpoint(job)
		if startCheckpoint > job.EndCheckpoint {
			if err := o.repo.ClaimJob(ctx, job.ID); err != nil {
				log.Printf("[Orchestrator] Failed to claim completed job %s: %v", job.ID.Hex(), err)
				continue
			}
			if err := o.repo.CompleteJob(ctx, job.ID); err != nil {
				log.Printf("[Orchestrator] Failed to complete job %s: %v", job.ID.Hex(), err)
			}
			return true
		}

		if err := o.repo.ClaimJob(ctx, job.ID); err != nil {
			log.Printf("[Orchestrator] Failed to claim job %s: %v", job.ID.Hex(), err)
			continue
		}

		log.Printf("[Orchestrator] Dispatching job %s [%d -> %d] to %s",
			job.ID.Hex(), startCheckpoint, job.EndCheckpoint, o.workerID)

		o.workerBusy = true
		o.workerCh <- models.JobAssignment{Job: job}
		return true
	}

	return false
}

func (o *Orchestrator) handleReport(ctx context.Context, report models.JobReport) {
	switch report.Type {
	case models.ReportBatchComplete:
		if err := o.repo.UpdateProgress(ctx, report.JobID, report.Position); err != nil {
			log.Printf("[Orchestrator] Failed to update progress for job %s: %v", report.JobID.Hex(), err)
		}
	case models.ReportJobDone:
		o.workerBusy = false
		if err := o.repo.CompleteJob(ctx, report.JobID); err != nil {
			log.Printf("[Orchestrator] Failed to complete job %s: %v", report.JobID.Hex(), err)
		}
		log.Printf("[Orchestrator] Job %s completed", report.JobID.Hex())
	case models.ReportJobFailed:
		o.workerBusy = false
		errMsg := "unknown error"
		if report.Error != nil {
			errMsg = report.Error.Error()
		}
		if err := o.repo.RequeueJob(ctx, report.JobID, errMsg); err != nil {
			log.Printf("[Orchestrator] Failed to requeue job %s: %v", report.JobID.Hex(), err)
		}
		log.Printf("[Orchestrator] Job %s requeued after error: %s", report.JobID.Hex(), errMsg)
	}
}

func resolveStartCheckpoint(job *models.CrawlerJob) int64 {
	startCheckpoint := job.LastCheckpoint + 1
	if startCheckpoint < job.FromCheckpoint {
		startCheckpoint = job.FromCheckpoint
	}
	return startCheckpoint
}

func (o *Orchestrator) waitOrCancel(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
