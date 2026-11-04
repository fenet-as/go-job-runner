package api

import (
	"encoding/json"
	"time"

	"github.com/fenet-as/go-job-runner/internal/store"
)

// SupportedJobTypes defines the recognized job handlers in the system.
var SupportedJobTypes = map[string]bool{
	"echo":          true,
	"sleep":         true,
	"fail_randomly": true,
}

// IsValidJobType checks if the requested job type is registered.
func IsValidJobType(jobType string) bool {
	return SupportedJobTypes[jobType]
}

// CreateJobRequest represents the payload accepted by POST /jobs.
type CreateJobRequest struct {
	Type        string          `json:"type" binding:"required"`
	Payload     json.RawMessage `json:"payload" binding:"required"`
	MaxAttempts *int32          `json:"max_attempts"`
	RunAt       *time.Time      `json:"run_at"`
}

// JobResponse is the client-facing representation of a Job.
type JobResponse struct {
	ID          int64           `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      string          `json:"status"`
	Attempts    int32           `json:"attempts"`
	MaxAttempts int32           `json:"max_attempts"`
	RunAt       time.Time       `json:"run_at"`
	LockedAt    *time.Time      `json:"locked_at,omitempty"`
	LockedBy    *string         `json:"locked_by,omitempty"`
	LastError   *string         `json:"last_error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// ErrorResponse represents a standardized JSON error message.
type ErrorResponse struct {
	Error string `json:"error"`
}

// ToJobResponse maps a store.Job to our API JobResponse.
func ToJobResponse(job store.Job) JobResponse {
	resp := JobResponse{
		ID:          job.ID,
		Type:        job.Type,
		Payload:     json.RawMessage(job.Payload),
		Status:      string(job.Status),
		Attempts:    job.Attempts,
		MaxAttempts: job.MaxAttempts,
		RunAt:       job.RunAt.Time,
		CreatedAt:   job.CreatedAt.Time,
		UpdatedAt:   job.UpdatedAt.Time,
	}

	if job.LockedAt.Valid {
		t := job.LockedAt.Time
		resp.LockedAt = &t
	}
	if job.LockedBy.Valid {
		s := job.LockedBy.String
		resp.LockedBy = &s
	}
	if job.LastError.Valid {
		s := job.LastError.String
		resp.LastError = &s
	}

	return resp
}
