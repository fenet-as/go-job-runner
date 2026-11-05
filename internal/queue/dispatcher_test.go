package queue_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/fenet-as/go-job-runner/internal/queue"
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

func TestDispatcher_PollsAndDispatches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries, cleanup := setupTestStore(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Insert 3 jobs with past run_at
	const count = 3
	for i := 0; i < count; i++ {
		_, err := queries.CreateJob(ctx, store.CreateJobParams{
			Type:        "echo",
			Payload:     []byte(fmt.Sprintf(`{"i": %d}`, i)),
			MaxAttempts: 3,
			RunAt: pgtype.Timestamptz{
				Time:  time.Now().Add(-5 * time.Second),
				Valid: true,
			},
		})
		if err != nil {
			t.Fatalf("failed to insert test job: %v", err)
		}
	}

	jobChan := make(chan store.Job, count)
	dispatcher := queue.NewDispatcher(queue.DispatcherConfig{
		PollInterval: 25 * time.Millisecond,
		DispatcherID: "test-dispatcher",
	}, queries, logger, jobChan)

	go dispatcher.Start(ctx)

	// Collect the dispatched jobs
	received := 0
	timeout := time.After(3 * time.Second)

	for received < count {
		select {
		case job := <-jobChan:
			if job.Status != store.JobStatusRunning {
				t.Errorf("expected job to be in running status when dispatched, got %s", job.Status)
			}
			received++
		case <-timeout:
			t.Fatalf("timeout: received only %d of %d jobs from channel", received, count)
		}
	}
}

func TestEndToEnd_DispatcherAndWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries, cleanup := setupTestStore(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	registry := worker.NewRegistry()
	worker.RegisterDefaultHandlers(registry)

	const totalJobs = 20
	const numWorkers = 5

	jobChan := make(chan store.Job, numWorkers*2)

	// 1. Insert 20 jobs with past timestamps so they are immediately due
	jobIDs := make([]int64, totalJobs)
	for i := 0; i < totalJobs; i++ {
		jobType := "echo"
		payload := []byte(fmt.Sprintf(`{"msg": "job-%d"}`, i))
		if i%2 == 0 {
			jobType = "sleep"
			payload = []byte(`{"seconds": 0.02}`)
		}

		created, err := queries.CreateJob(ctx, store.CreateJobParams{
			Type:        jobType,
			Payload:     payload,
			MaxAttempts: 3,
			RunAt: pgtype.Timestamptz{
				Time:  time.Now().Add(-10 * time.Second),
				Valid: true,
			},
		})
		if err != nil {
			t.Fatalf("failed to insert job %d: %v", i, err)
		}
		jobIDs[i] = created.ID
	}

	// 2. Start worker pool
	workerPool := worker.NewPool(worker.PoolConfig{
		WorkerCount: numWorkers,
		JobTimeout:  5 * time.Second,
	}, registry, queries, logger, jobChan)
	workerPool.Start(ctx)

	// 3. Start dispatcher
	dispatcher := queue.NewDispatcher(queue.DispatcherConfig{
		PollInterval: 25 * time.Millisecond,
		DispatcherID: "e2e-dispatcher",
	}, queries, logger, jobChan)
	go dispatcher.Start(ctx)

	// 4. Wait until all 20 jobs transition to 'succeeded' in database
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
			time.Sleep(20 * time.Millisecond)
		}
	}
}
