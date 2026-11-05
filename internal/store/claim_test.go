package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fenet-as/go-job-runner/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestStore_ClaimJob_Concurrent(t *testing.T) {
	ctx := context.Background()
	dbURL := getTestDatabaseURL()

	pool, err := store.NewPool(ctx, store.PoolConfig{
		DatabaseURL: dbURL,
		MaxConns:    15,
	})
	if err != nil {
		t.Fatalf("failed to connect to test db: %v", err)
	}
	defer pool.Close()

	queries := store.New(pool)

	const totalJobs = 100
	const numWorkers = 10

	insertedIDs := make(map[int64]bool, totalJobs)
	now := time.Now()

	// 1. Enqueue 100 ready jobs with past timestamps
	for i := 0; i < totalJobs; i++ {
		job, err := queries.CreateJob(ctx, store.CreateJobParams{
			Type:        "echo",
			Payload:     []byte(fmt.Sprintf(`{"index": %d}`, i)),
			MaxAttempts: 5,
			RunAt: pgtype.Timestamptz{
				Time:  now.Add(-time.Duration(totalJobs-i+10) * time.Second),
				Valid: true,
			},
		})
		if err != nil {
			t.Fatalf("failed to insert job %d: %v", i, err)
		}
		insertedIDs[job.ID] = true
	}

	// 2. Concurrently claim jobs across 10 goroutines
	var wg sync.WaitGroup
	var claimedMap sync.Map // map[int64]string (jobID -> workerID)
	var duplicateClaims int64
	var totalClaimed int64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		workerID := fmt.Sprintf("worker-%02d", w)

		go func(wID string) {
			defer wg.Done()

			emptyRetries := 0
			for {
				job, err := queries.ClaimJob(ctx, wID)
				if err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						emptyRetries++
						if emptyRetries >= 5 {
							// Queue is genuinely empty
							return
						}
						time.Sleep(15 * time.Millisecond)
						continue
					}
					t.Errorf("worker %s unexpected error: %v", wID, err)
					return
				}
				emptyRetries = 0

				// Check if this job was already claimed by another worker
				if prevWorker, loaded := claimedMap.LoadOrStore(job.ID, wID); loaded {
					atomic.AddInt64(&duplicateClaims, 1)
					t.Errorf("RACE CONDITION: Job %d claimed by %s was already claimed by %v!", job.ID, wID, prevWorker)
				} else {
					atomic.AddInt64(&totalClaimed, 1)
				}
			}
		}(workerID)
	}

	wg.Wait()

	// 3. Verify assertions
	if duplicateClaims != 0 {
		t.Fatalf("FAILED: Detected %d duplicate claims! FOR UPDATE SKIP LOCKED failed.", duplicateClaims)
	}

	if totalClaimed < totalJobs {
		t.Fatalf("expected at least %d claimed jobs, got %d", totalJobs, totalClaimed)
	}

	// Verify that all 100 inserted jobs were indeed claimed
	for id := range insertedIDs {
		if _, ok := claimedMap.Load(id); !ok {
			t.Errorf("Job %d was inserted but never claimed", id)
		}

		// Verify database state for this job
		job, err := queries.GetJobByID(ctx, id)
		if err != nil {
			t.Errorf("failed to fetch job %d: %v", id, err)
			continue
		}
		if job.Status != store.JobStatusRunning {
			t.Errorf("expected job %d status 'running', got '%s'", id, job.Status)
		}
		if job.Attempts != 1 {
			t.Errorf("expected job %d attempts 1, got %d", id, job.Attempts)
		}
		if !job.LockedAt.Valid {
			t.Errorf("expected job %d locked_at to be valid", id)
		}
		if !job.LockedBy.Valid || job.LockedBy.String == "" {
			t.Errorf("expected job %d locked_by to be set", id)
		}
	}
}

func TestStore_ClaimJob_IgnoresFutureJobs(t *testing.T) {
	ctx := context.Background()
	dbURL := getTestDatabaseURL()

	pool, err := store.NewPool(ctx, store.PoolConfig{
		DatabaseURL: dbURL,
		MaxConns:    5,
	})
	if err != nil {
		t.Fatalf("failed to connect to test db: %v", err)
	}
	defer pool.Close()

	queries := store.New(pool)

	futureTime := time.Now().Add(1 * time.Hour)
	futureJob, err := queries.CreateJob(ctx, store.CreateJobParams{
		Type:        "echo",
		Payload:     []byte(`{"msg": "future"}`),
		MaxAttempts: 3,
		RunAt: pgtype.Timestamptz{
			Time:  futureTime,
			Valid: true,
		},
	})
	if err != nil {
		t.Fatalf("failed to insert future job: %v", err)
	}

	defer queries.CancelJob(ctx, futureJob.ID)

	claimed, err := queries.ClaimJob(ctx, "future-tester")
	if err == nil {
		if claimed.ID == futureJob.ID {
			t.Fatalf("ERROR: claimed a future job that had run_at in future: %v", claimed.RunAt.Time)
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("unexpected error claiming job: %v", err)
	}
}

func TestStore_CompleteJob(t *testing.T) {
	ctx := context.Background()
	dbURL := getTestDatabaseURL()

	pool, err := store.NewPool(ctx, store.PoolConfig{
		DatabaseURL: dbURL,
		MaxConns:    5,
	})
	if err != nil {
		t.Fatalf("failed to connect to test db: %v", err)
	}
	defer pool.Close()

	queries := store.New(pool)

	job, err := queries.CreateJob(ctx, store.CreateJobParams{
		Type:        "echo",
		Payload:     []byte(`{"msg": "complete_me"}`),
		MaxAttempts: 3,
		RunAt: pgtype.Timestamptz{
			Time:  time.Now().Add(-1 * time.Minute),
			Valid: true,
		},
	})
	if err != nil {
		t.Fatalf("failed to insert job: %v", err)
	}

	claimed, err := queries.ClaimJob(ctx, "worker-finisher")
	if err != nil {
		t.Fatalf("failed to claim job: %v", err)
	}

	completed, err := queries.CompleteJob(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("failed to complete job: %v", err)
	}

	if completed.Status != store.JobStatusSucceeded {
		t.Errorf("expected status succeeded, got %s", completed.Status)
	}
	if completed.LockedAt.Valid {
		t.Errorf("expected locked_at to be cleared (null)")
	}
	if completed.LockedBy.Valid {
		t.Errorf("expected locked_by to be cleared (null)")
	}
	_ = job
}
