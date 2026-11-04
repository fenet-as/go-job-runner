package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/fenet-as/go-job-runner/internal/store"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// HealthzHandler checks server liveness and database ping.
func (s *Server) HealthzHandler(c *gin.Context) {
	ctx, cancel := c.Request.Context(), func() {}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(c.Request.Context(), 2*time.Second)
	}
	defer cancel()

	if err := s.pool.Ping(ctx); err != nil {
		s.logger.Error("healthz ping failed", "error", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "unhealthy",
			"error":  "database unavailable",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":   "healthy",
		"database": "connected",
	})
}

// CreateJobHandler validates input and inserts a new job with status 'queued'.
func (s *Server) CreateJobHandler(c *gin.Context) {
	var req CreateJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: fmt.Sprintf("invalid request payload: %v", err)})
		return
	}

	if !IsValidJobType(req.Type) {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: fmt.Sprintf("unknown job type: '%s'. Supported types: echo, sleep, fail_randomly", req.Type),
		})
		return
	}

	maxAttempts := int32(5)
	if req.MaxAttempts != nil && *req.MaxAttempts > 0 {
		maxAttempts = *req.MaxAttempts
	}

	runAt := pgtype.Timestamptz{
		Time:  time.Now(),
		Valid: true,
	}
	if req.RunAt != nil {
		runAt.Time = *req.RunAt
	}

	job, err := s.queries.CreateJob(c.Request.Context(), store.CreateJobParams{
		Type:        req.Type,
		Payload:     req.Payload,
		MaxAttempts: maxAttempts,
		RunAt:       runAt,
	})
	if err != nil {
		s.logger.Error("failed to create job", "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to persist job"})
		return
	}

	c.JSON(http.StatusAccepted, ToJobResponse(job))
}

// GetJobByIDHandler retrieves a single job by its numeric ID.
func (s *Server) GetJobByIDHandler(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid job ID"})
		return
	}

	job, err := s.queries.GetJobByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, ErrorResponse{Error: fmt.Sprintf("job with ID %d not found", id)})
			return
		}
		s.logger.Error("failed to get job", "id", id, "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to fetch job"})
		return
	}

	c.JSON(http.StatusOK, ToJobResponse(job))
}

// ListJobsHandler returns a paginated list of jobs, optionally filtered by status and type.
func (s *Server) ListJobsHandler(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "20")
	offsetStr := c.DefaultQuery("offset", "0")

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	offset, err := strconv.Atoi(offsetStr)
	if err != nil || offset < 0 {
		offset = 0
	}

	params := store.ListJobsParams{
		Limit:  int32(limit),
		Offset: int32(offset),
	}

	if statusQuery := c.Query("status"); statusQuery != "" {
		params.Status = store.NullJobStatus{
			JobStatus: store.JobStatus(statusQuery),
			Valid:     true,
		}
	}

	if typeQuery := c.Query("type"); typeQuery != "" {
		params.Type = pgtype.Text{
			String: typeQuery,
			Valid:  true,
		}
	}

	jobs, err := s.queries.ListJobs(c.Request.Context(), params)
	if err != nil {
		s.logger.Error("failed to list jobs", "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to list jobs"})
		return
	}

	responses := make([]JobResponse, len(jobs))
	for i, j := range jobs {
		responses[i] = ToJobResponse(j)
	}

	c.JSON(http.StatusOK, gin.H{
		"jobs":   responses,
		"limit":  limit,
		"offset": offset,
		"count":  len(responses),
	})
}

// CancelJobHandler cancels a job if it is currently in 'queued' status.
func (s *Server) CancelJobHandler(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid job ID"})
		return
	}

	job, err := s.queries.CancelJob(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Check if the job actually exists with another status
			existing, getErr := s.queries.GetJobByID(c.Request.Context(), id)
			if getErr != nil {
				c.JSON(http.StatusNotFound, ErrorResponse{Error: fmt.Sprintf("job with ID %d not found", id)})
				return
			}
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Error: fmt.Sprintf("cannot cancel job with status '%s' (only 'queued' jobs can be cancelled)", existing.Status),
			})
			return
		}
		s.logger.Error("failed to cancel job", "id", id, "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to cancel job"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "job cancelled successfully",
		"job":     ToJobResponse(job),
	})
}
