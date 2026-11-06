package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/fenet-as/go-job-runner/internal/api"
	"github.com/fenet-as/go-job-runner/internal/store"
	"github.com/fenet-as/go-job-runner/internal/telemetry"
	"github.com/jackc/pgx/v5/pgtype"
)

func getTestDatabaseURL() string {
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}
	return "postgres://postgres@localhost:5432/jobrunner?sslmode=disable"
}

func setupTestServer(t *testing.T) (*api.Server, *store.Queries, func()) {
	t.Helper()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	pool, err := store.NewPool(ctx, store.PoolConfig{
		DatabaseURL: getTestDatabaseURL(),
		MaxConns:    5,
	})
	if err != nil {
		t.Fatalf("failed to connect to test db: %v", err)
	}

	server := api.NewServer(pool, logger)
	queries := store.New(pool)
	cleanup := func() {
		pool.Close()
	}

	return server, queries, cleanup
}

func TestHealthz(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	req, _ := http.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d. Body: %s", rec.Code, rec.Body.String())
	}

	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse response JSON: %v", err)
	}
	if body["status"] != "healthy" {
		t.Errorf("expected status 'healthy', got %s", body["status"])
	}
}

func TestCreateJob_Success(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	payload := []byte(`{"type": "echo", "payload": {"msg": "ping"}, "max_attempts": 3}`)
	req, _ := http.NewRequest(http.MethodPost, "/jobs", bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d. Body: %s", rec.Code, rec.Body.String())
	}

	var job api.JobResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &job); err != nil {
		t.Fatalf("failed to parse JobResponse: %v", err)
	}

	if job.ID <= 0 {
		t.Errorf("expected positive ID, got %d", job.ID)
	}
	if job.Type != "echo" {
		t.Errorf("expected type echo, got %s", job.Type)
	}
	if job.Status != "queued" {
		t.Errorf("expected status queued, got %s", job.Status)
	}
	if job.MaxAttempts != 3 {
		t.Errorf("expected max_attempts 3, got %d", job.MaxAttempts)
	}
}

func TestCreateJob_UnknownType(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	payload := []byte(`{"type": "unregistered_job_type", "payload": {}}`)
	req, _ := http.NewRequest(http.MethodPost, "/jobs", bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d. Body: %s", rec.Code, rec.Body.String())
	}
}

func TestGetJobByID_SuccessAndNotFound(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	// 1. Create a job first
	createPayload := []byte(`{"type": "sleep", "payload": {"seconds": 2}}`)
	createReq, _ := http.NewRequest(http.MethodPost, "/jobs", bytes.NewBuffer(createPayload))
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	server.Router().ServeHTTP(createRec, createReq)

	var created api.JobResponse
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)

	// 2. Fetch created job
	getReq, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/jobs/%d", created.ID), nil)
	getRec := httptest.NewRecorder()
	server.Router().ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", getRec.Code)
	}

	var fetched api.JobResponse
	_ = json.Unmarshal(getRec.Body.Bytes(), &fetched)
	if fetched.ID != created.ID {
		t.Errorf("expected ID %d, got %d", created.ID, fetched.ID)
	}

	// 3. Fetch non-existent job
	notFoundReq, _ := http.NewRequest(http.MethodGet, "/jobs/99999999", nil)
	notFoundRec := httptest.NewRecorder()
	server.Router().ServeHTTP(notFoundRec, notFoundReq)

	if notFoundRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found, got %d", notFoundRec.Code)
	}
}

func TestListJobs(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	req, _ := http.NewRequest(http.MethodGet, "/jobs?limit=5&offset=0", nil)
	rec := httptest.NewRecorder()
	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to decode list response: %v", err)
	}
	if _, ok := result["jobs"]; !ok {
		t.Errorf("expected jobs key in response")
	}
}

func TestCancelJob_SuccessAndAlreadyDeleted(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	// 1. Create a job
	payload := []byte(`{"type": "echo", "payload": {"foo": "bar"}}`)
	createReq, _ := http.NewRequest(http.MethodPost, "/jobs", bytes.NewBuffer(payload))
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	server.Router().ServeHTTP(createRec, createReq)

	var created api.JobResponse
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)

	// 2. Cancel it
	deleteReq, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("/jobs/%d", created.ID), nil)
	deleteRec := httptest.NewRecorder()
	server.Router().ServeHTTP(deleteRec, deleteReq)

	if deleteRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on cancel, got %d. Body: %s", deleteRec.Code, deleteRec.Body.String())
	}

	// 3. Cancelling again should return 404
	deleteReq2, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("/jobs/%d", created.ID), nil)
	deleteRec2 := httptest.NewRecorder()
	server.Router().ServeHTTP(deleteRec2, deleteReq2)

	if deleteRec2.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found on second cancel, got %d", deleteRec2.Code)
	}
}

func TestRequeueDeadJob(t *testing.T) {
	server, queries, cleanup := setupTestServer(t)
	defer cleanup()
	ctx := context.Background()

	// 1. Create and claim a job, then mark it 'dead'
	job, err := queries.CreateJob(ctx, store.CreateJobParams{
		Type:        "echo",
		Payload:     []byte(`{"test": "dlq"}`),
		MaxAttempts: 1,
		RunAt: pgtype.Timestamptz{
			Time:  time.Now().Add(-5 * time.Second),
			Valid: true,
		},
	})
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	claimed, err := queries.ClaimJob(ctx, "dlq-test")
	if err != nil {
		t.Fatalf("failed to claim job: %v", err)
	}

	deadJob, err := queries.MarkJobDead(ctx, store.MarkJobDeadParams{
		ID:        claimed.ID,
		LastError: "fatal error for dlq test",
	})
	if err != nil {
		t.Fatalf("failed to mark job dead: %v", err)
	}
	if deadJob.Status != store.JobStatusDead {
		t.Fatalf("expected status dead, got %s", deadJob.Status)
	}

	// 2. POST /jobs/:id/retry
	retryReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("/jobs/%d/retry", deadJob.ID), nil)
	retryRec := httptest.NewRecorder()
	server.Router().ServeHTTP(retryRec, retryReq)

	if retryRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on requeue, got %d. Body: %s", retryRec.Code, retryRec.Body.String())
	}

	var resp struct {
		Message string          `json:"message"`
		Job     api.JobResponse `json:"job"`
	}
	if err := json.Unmarshal(retryRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode retry response: %v", err)
	}

	if resp.Job.Status != "queued" {
		t.Errorf("expected status 'queued', got '%s'", resp.Job.Status)
	}
	if resp.Job.Attempts != 0 {
		t.Errorf("expected attempts 0, got %d", resp.Job.Attempts)
	}
	if resp.Job.LastError != nil {
		t.Errorf("expected last_error to be nil after requeue")
	}

	// 3. Attempting to requeue a job that is already 'queued' should return 400
	retryReq2, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("/jobs/%d/retry", deadJob.ID), nil)
	retryRec2 := httptest.NewRecorder()
	server.Router().ServeHTTP(retryRec2, retryReq2)

	if retryRec2.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request when requeueing non-dead job, got %d", retryRec2.Code)
	}

	// 4. Attempting to requeue non-existent job should return 404
	retryReq3, _ := http.NewRequest(http.MethodPost, "/jobs/999999999/retry", nil)
	retryRec3 := httptest.NewRecorder()
	server.Router().ServeHTTP(retryRec3, retryReq3)

	if retryRec3.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found for missing job, got %d", retryRec3.Code)
	}
	_ = job
}

func TestDashboard_ServesHTML(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	for _, path := range []string{"/", "/dashboard"} {
		req, _ := http.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		server.Router().ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("path %s returned code %d, expected 200", path, rec.Code)
		}
		if contentType := rec.Header().Get("Content-Type"); contentType != "text/html; charset=utf-8" {
			t.Errorf("expected text/html; charset=utf-8, got %s", contentType)
		}
		if !bytes.Contains(rec.Body.Bytes(), []byte("Go Job Runner")) {
			t.Errorf("expected HTML body to contain 'Go Job Runner'")
		}
	}
}

func TestDashboard_StatsEndpoint(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	tracker := telemetry.NewTracker(3, 6)
	server.SetTracker(tracker)

	req, _ := http.NewRequest(http.MethodGet, "/api/dashboard/stats", nil)
	rec := httptest.NewRecorder()
	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d. Body: %s", rec.Code, rec.Body.String())
	}

	var stats telemetry.DashboardStats
	if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil {
		t.Fatalf("failed to decode dashboard stats JSON: %v", err)
	}

	if len(stats.Workers) != 3 {
		t.Errorf("expected 3 workers, got %d", len(stats.Workers))
	}
	if stats.Buffer.Capacity != 6 {
		t.Errorf("expected buffer capacity 6, got %d", stats.Buffer.Capacity)
	}
}
