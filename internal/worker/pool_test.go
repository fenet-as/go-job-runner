package worker_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/fenet-as/go-job-runner/internal/store"
	"github.com/fenet-as/go-job-runner/internal/worker"
	"github.com/jackc/pgx/v5/pgtype"
)

func getTestDatabaseURL() string {
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}
	return "postgres://postgres@localhost:5432/jobrunner?sslmode=disable"
}

func setupTestStore(t *testing.T) (*store.Queries, func()) {
	t.Helper()
	ctx := context.Background()
	pool, err := store.NewPool(ctx, store.PoolConfig{
		DatabaseURL: getTestDatabaseURL(),
		MaxConns:    10,
	})
	if err != nil {
		t.Fatalf("failed to connect to test db: %v", err)
	}
	return store.New(pool), func() { pool.Close() }
}

func TestWorkerPool_ExecutesJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries, cleanup := setupTestStore(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	registry := worker.NewRegistry()
	worker.RegisterDefaultHandlers(registry)

	jobChan := make(chan store.Job, 10)
	pool := worker.NewPool(worker.PoolConfig{
		WorkerCount: 3,
		JobTimeout:  5 * time.Second,
	}, registry, queries, logger, jobChan)

	pool.Start(ctx)

	const count = 5
	jobIDs := make([]int64, count)

	// Create and claim jobs manually to feed the channel
	for i := 0; i < count; i++ {
		_, err := queries.CreateJob(ctx, store.CreateJobParams{
			Type:        "echo",
			Payload:     []byte(fmt.Sprintf(`{"msg": "echo-%d"}`, i)),
			MaxAttempts: 3,
			RunAt: pgtype.Timestamptz{
				Time:  time.Now(),
				Valid: true,
			},
		})
		if err != nil {
			t.Fatalf("failed to create job: %v", err)
		}

		claimed, err := queries.ClaimJob(ctx, "test-claimer")
		if err != nil {
			t.Fatalf("failed to claim job: %v", err)
		}
		jobIDs[i] = claimed.ID
		jobChan <- claimed
	}

	// Wait for workers to process all jobs
	deadline := time.Now().Add(5 * time.Second)
	for _, id := range jobIDs {
		for {
			if time.Now().After(deadline) {
				t.Fatalf("timeout waiting for job %d to succeed", id)
			}
			j, err := queries.GetJobByID(ctx, id)
			if err != nil {
				t.Fatalf("failed to get job %d: %v", id, err)
			}
			if j.Status == store.JobStatusSucceeded {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func TestWorkerPool_PanicRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries, cleanup := setupTestStore(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	registry := worker.NewRegistry()

	// Register a handler that panics
	registry.Register("panic_job", func(ctx context.Context, payload []byte) error {
		panic("deliberate worker panic")
	})

	jobChan := make(chan store.Job, 1)
	pool := worker.NewPool(worker.PoolConfig{
		WorkerCount: 1,
		JobTimeout:  5 * time.Second,
	}, registry, queries, logger, jobChan)

	pool.Start(ctx)

	// Create and claim the panicking job
	_, err := queries.CreateJob(ctx, store.CreateJobParams{
		Type:        "panic_job",
		Payload:     []byte(`{}`),
		MaxAttempts: 3,
		RunAt: pgtype.Timestamptz{
			Time:  time.Now(),
			Valid: true,
		},
	})
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	claimed, err := queries.ClaimJob(ctx, "test-claimer")
	if err != nil {
		t.Fatalf("failed to claim job: %v", err)
	}

	jobChan <- claimed

	// Wait and verify status transitions to 'failed' with panic error message
	deadline := time.Now().Add(3 * time.Second)
	var finalJob store.Job
	for {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for panic job to be marked failed")
		}
		j, err := queries.GetJobByID(ctx, claimed.ID)
		if err != nil {
			t.Fatalf("failed to get job: %v", err)
		}
		if j.Status == store.JobStatusFailed {
			finalJob = j
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !finalJob.LastError.Valid || finalJob.LastError.String == "" {
		t.Errorf("expected last_error to be recorded after panic")
	}
}
