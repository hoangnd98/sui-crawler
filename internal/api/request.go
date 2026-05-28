package api

import "sui-crawler/internal/models"

// CreateJobRequest is the request body for creating a new crawler job.
type CreateJobRequest struct {
	FromCheckpoint *int64 `json:"fromCheckpoint" binding:"required,min=0" example:"0"`
	EndCheckpoint  *int64 `json:"endCheckpoint" binding:"required,min=0" example:"1000"`
}

// Validate performs business-level validation beyond struct tags.
func (r *CreateJobRequest) Validate() string {
	if r.FromCheckpoint != nil && r.EndCheckpoint != nil && *r.EndCheckpoint < *r.FromCheckpoint {
		return "endCheckpoint must be >= fromCheckpoint"
	}
	return ""
}

// JobResponse is the API response representing a single crawler job.
type JobResponse struct {
	ID             string  `json:"id" example:"682a1b2c3d4e5f6a7b8c9d0e"`
	FromCheckpoint int64   `json:"fromCheckpoint" example:"0"`
	LastCheckpoint int64   `json:"lastCheckpoint" example:"499"`
	EndCheckpoint  int64   `json:"endCheckpoint" example:"1000"`
	Status         string  `json:"status" example:"progressing"`
	Error          string  `json:"error,omitempty" example:""`
	CreatedAt      string  `json:"created_at" example:"2026-04-24T15:00:00Z"`
	UpdatedAt      string  `json:"updated_at" example:"2026-04-24T15:00:00Z"`
	CompletedAt    *string `json:"completed_at,omitempty" example:"2026-04-24T16:00:00Z"`
}

// ErrorResponse is the standard error response format.
type ErrorResponse struct {
	Error string `json:"error" example:"invalid request body"`
}

// ToJobResponse converts a CrawlerJob model to a JobResponse DTO.
func ToJobResponse(job *models.CrawlerJob) *JobResponse {
	resp := &JobResponse{
		ID:             job.ID.Hex(),
		FromCheckpoint: job.FromCheckpoint,
		LastCheckpoint: job.LastCheckpoint,
		EndCheckpoint:  job.EndCheckpoint,
		Status:         string(job.Status),
		Error:          job.Error,
		CreatedAt:      job.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		UpdatedAt:      job.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}
	if job.CompletedAt != nil {
		t := job.CompletedAt.UTC().Format("2006-01-02T15:04:05Z")
		resp.CompletedAt = &t
	}
	return resp
}

// ToJobResponses converts a slice of CrawlerJob models to JobResponse DTOs.
func ToJobResponses(jobs []*models.CrawlerJob) []*JobResponse {
	responses := make([]*JobResponse, 0, len(jobs))
	for _, job := range jobs {
		responses = append(responses, ToJobResponse(job))
	}
	return responses
}
