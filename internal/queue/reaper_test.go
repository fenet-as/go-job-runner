package queue_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/fenet-as/go-job-runner/internal/queue"
	"github.com/fenet-as/go-job-runner/internal/store"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestReaper_RescuesAbandonedJobs(t *testing.T) {
	ctx := context.Background()
	pool, err := store.NewPool(ctx, store.PoolConfig{
		DatabaseURL: getTestDatabaseURL(),
		MaxConns:    5,
	})
	if err != nil {
		t.Fatalf("failed to connect to test db: %v", err)
	}
	defer pool.Close()

	// Clean up any leftover jobs from prior tests
	_, _ = pool.Exec(ctx, "DELETE FROM jobs")

	queries := store.New(pool)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Create and claim 3 jobs to simulate a worker that subsequently crashed
	const abandonedCount = 3
	abandonedIDs := make([]int64, abandonedCount)

	for i := 0; i < abandonedCount; i++ {
		_, err := queries.CreateJob(ctx, store.CreateJobParams{
			Type:        "echo",
			Payload:     []byte(`{"status": "simulated_crash"}`),
			MaxAttempts: 3,
			RunAt: pgtype.Timestamptz{
				Time:  time.Now().Add(-1 * time.Hour),
				Valid: true,
			},
		})
		if err != nil {
			t.Fatalf("failed to create job: %v", err)
		}

		claimed, err := queries.ClaimJob(ctx, "crashed-worker-pid-99")
		if err != nil {
			t.Fatalf("failed to claim job: %v", err)
		}
		abandonedIDs[i] = claimed.ID
	}

	// Wait 250ms so the 3 abandoned jobs exceed a 200ms threshold
	time.Sleep(250 * time.Millisecond)

	// 2. Create and claim 1 active healthy job RIGHT NOW (fresh locked_at)
	_, err = queries.CreateJob(ctx, store.CreateJobParams{
		Type:        "echo",
		Payload:     []byte(`{"status": "healthy_worker"}`),
		MaxAttempts: 3,
		RunAt: pgtype.Timestamptz{
			Time:  time.Now().Add(-1 * time.Minute),
			Valid: true,
		},
	})
	if err != nil {
		t.Fatalf("failed to create healthy job: %v", err)
	}

	claimedHealthy, err := queries.ClaimJob(ctx, "active-healthy-worker")
	if err != nil {
		t.Fatalf("failed to claim healthy job: %v", err)
	}

	// 3. Configure Reaper with 200ms timeout threshold
	reaper := queue.NewReaper(queue.ReaperConfig{
		Interval:         50 * time.Millisecond,
		TimeoutThreshold: 200 * time.Millisecond,
		BatchSize:        10,
	}, queries, logger)

	// Execute ReapOnce
	reaped, err := reaper.ReapOnce(ctx)
	if err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}

	if len(reaped) != abandonedCount {
		t.Fatalf("expected exactly %d reaped jobs, got %d", abandonedCount, len(reaped))
	}

	// Verify abandoned jobs are now 'queued' with locks cleared and error recorded
	for _, id := range abandonedIDs {
		j, err := queries.GetJobByID(ctx, id)
		if err != nil {
			t.Fatalf("failed to get reaped job %d: %v", id, err)
		}
		if j.Status != store.JobStatusQueued {
			t.Errorf("job %d: expected status 'queued', got '%s'", id, j.Status)
		}
		if j.LockedAt.Valid {
			t.Errorf("job %d: expected locked_at to be NULL", id)
		}
		if j.LockedBy.Valid {
			t.Errorf("job %d: expected locked_by to be NULL", id)
		}
		if !j.LastError.Valid || j.LastError.String == "" {
			t.Errorf("job %d: expected last_error explaining reap", id)
		}
	}

	// Verify the healthy job was NOT reaped
	healthyCheck, err := queries.GetJobByID(ctx, claimedHealthy.ID)
	if err != nil {
		t.Fatalf("failed to get healthy job: %v", err)
	}
	if healthyCheck.Status != store.JobStatusRunning {
		t.Errorf("expected healthy job to remain 'running', got '%s'", healthyCheck.Status)
	}
	if !healthyCheck.LockedAt.Valid {
		t.Errorf("expected healthy job to keep its locked_at timestamp")
	}
}
