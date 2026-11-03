package store_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/fenet-as/go-job-runner/internal/store"
	"github.com/jackc/pgx/v5/pgtype"
)

func getTestDatabaseURL() string {
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}
	return "postgres://postgres@localhost:5432/jobrunner?sslmode=disable"
}

func TestStore_CreateAndGetJob(t *testing.T) {
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

	// 1. Insert a new job
	jobType := "echo"
	payload := []byte(`{"message": "hello world"}`)
	maxAttempts := int32(3)
	runAt := pgtype.Timestamptz{
		Time:  time.Now(),
		Valid: true,
	}

	created, err := queries.CreateJob(ctx, store.CreateJobParams{
		Type:        jobType,
		Payload:     payload,
		MaxAttempts: maxAttempts,
		RunAt:       runAt,
	})
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	if created.ID <= 0 {
		t.Errorf("expected positive ID, got %d", created.ID)
	}
	if created.Type != jobType {
		t.Errorf("expected type %s, got %s", jobType, created.Type)
	}
	if created.Status != store.JobStatusQueued {
		t.Errorf("expected status queued, got %s", created.Status)
	}
	if created.Attempts != 0 {
		t.Errorf("expected 0 attempts, got %d", created.Attempts)
	}
	if created.MaxAttempts != maxAttempts {
		t.Errorf("expected max attempts %d, got %d", maxAttempts, created.MaxAttempts)
	}

	// 2. Fetch the job back by ID
	fetched, err := queries.GetJobByID(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetJobByID failed: %v", err)
	}

	if fetched.ID != created.ID {
		t.Errorf("expected ID %d, got %d", created.ID, fetched.ID)
	}
	if string(fetched.Payload) != string(payload) {
		t.Errorf("expected payload %s, got %s", string(payload), string(fetched.Payload))
	}
}
