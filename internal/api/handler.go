package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/v2/bson"

	"sui-crawler/internal/client"
	"sui-crawler/internal/models"
	"sui-crawler/internal/repository"
)

// Handler holds the dependencies for job API handlers.
type Handler struct {
	repo      *repository.JobRepository
	suiClient *client.SuiClient
}

// NewHandler creates a new API handler.
func NewHandler(repo *repository.JobRepository, suiClient *client.SuiClient) *Handler {
	return &Handler{repo: repo, suiClient: suiClient}
}

// CreateJob godoc
// @Summary      Create a new crawler job
// @Description  Create a new SUI checkpoint crawling job with explicit checkpoint boundaries
// @Tags         jobs
// @Accept       json
// @Produce      json
// @Param        body  body      CreateJobRequest  true  "Job configuration"
// @Success      201   {object}  JobResponse
// @Failure      400   {object}  ErrorResponse
// @Failure      500   {object}  ErrorResponse
// @Router       /api/v1/jobs [post]
func (h *Handler) CreateJob(c *gin.Context) {
	var req CreateJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	if msg := req.Validate(); msg != "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: msg})
		return
	}

	job := &models.CrawlerJob{
		FromCheckpoint: *req.FromCheckpoint,
		LastCheckpoint: *req.FromCheckpoint,
		EndCheckpoint:  *req.EndCheckpoint,
	}

	if err := h.repo.CreateJob(c.Request.Context(), job); err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to create job"})
		return
	}

	c.JSON(http.StatusCreated, ToJobResponse(job))
}

// ListJobs godoc
// @Summary      List crawler jobs
// @Description  List all crawler jobs with optional status filter
// @Tags         jobs
// @Produce      json
// @Param        status  query     string  false  "Filter by status (pending, progressing, completed)"
// @Success      200     {array}   JobResponse
// @Failure      500     {object}  ErrorResponse
// @Router       /api/v1/jobs [get]
func (h *Handler) ListJobs(c *gin.Context) {
	status := c.Query("status")

	jobs, err := h.repo.ListJobs(c.Request.Context(), status)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to list jobs"})
		return
	}

	c.JSON(http.StatusOK, ToJobResponses(jobs))
}

// GetJob godoc
// @Summary      Get a crawler job by ID
// @Description  Retrieve detailed information about a specific crawler job
// @Tags         jobs
// @Produce      json
// @Param        id   path      string  true  "Job ID"
// @Success      200  {object}  JobResponse
// @Failure      400  {object}  ErrorResponse
// @Failure      404  {object}  ErrorResponse
// @Failure      500  {object}  ErrorResponse
// @Router       /api/v1/jobs/{id} [get]
func (h *Handler) GetJob(c *gin.Context) {
	idStr := c.Param("id")

	objectID, err := bson.ObjectIDFromHex(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid job ID format"})
		return
	}

	job, err := h.repo.GetJob(c.Request.Context(), objectID)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: "job not found"})
		return
	}

	c.JSON(http.StatusOK, ToJobResponse(job))
}

// RetryJob godoc
// @Summary      Retry a crawler job
// @Description  Transition a job back to pending status to resume or restart it, regardless of its current state
// @Tags         jobs
// @Produce      json
// @Param        id   path      string  true  "Job ID"
// @Success      200  {object}  JobResponse
// @Failure      400  {object}  ErrorResponse
// @Failure      404  {object}  ErrorResponse
// @Failure      500  {object}  ErrorResponse
// @Router       /api/v1/jobs/{id}/retry [post]
func (h *Handler) RetryJob(c *gin.Context) {
	idStr := c.Param("id")

	objectID, err := bson.ObjectIDFromHex(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid job ID format"})
		return
	}

	if err := h.repo.RetryJob(c.Request.Context(), objectID); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	// Fetch updated job to return
	job, err := h.repo.GetJob(c.Request.Context(), objectID)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: "job not found after retry"})
		return
	}

	c.JSON(http.StatusOK, ToJobResponse(job))
}
