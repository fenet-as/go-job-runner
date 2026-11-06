package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/fenet-as/go-job-runner/internal/store"
	"github.com/fenet-as/go-job-runner/internal/telemetry"
	"github.com/jackc/pgx/v5/pgtype"
)

// PoolConfig holds parameters for the worker pool and retry backoff.
type PoolConfig struct {
	WorkerCount int
	JobTimeout  time.Duration
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

// Pool manages a fixed number of worker goroutines consuming jobs from a channel.
type Pool struct {
	cfg      PoolConfig
	registry *Registry
	queries  *store.Queries
	logger   *slog.Logger
	jobChan  <-chan store.Job
	tracker  *telemetry.Tracker
	wg       sync.WaitGroup
}

// NewPool creates a new worker pool instance.
func NewPool(cfg PoolConfig, registry *Registry, queries *store.Queries, logger *slog.Logger, jobChan <-chan store.Job) *Pool {
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 5
	}
	if cfg.JobTimeout <= 0 {
		cfg.JobTimeout = 30 * time.Second
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 1 * time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 1 * time.Hour
	}

	return &Pool{
		cfg:      cfg,
		registry: registry,
		queries:  queries,
		logger:   logger,
		jobChan:  jobChan,
	}
}

// SetTracker registers an optional telemetry tracker for real-time dashboard visualization.
func (p *Pool) SetTracker(tracker *telemetry.Tracker) {
	p.tracker = tracker
}

// Start launches the worker goroutines.
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.cfg.WorkerCount; i++ {
		p.wg.Add(1)
		workerID := fmt.Sprintf("worker-%02d", i)

		go func(wID string) {
			defer p.wg.Done()
			p.logger.Debug("worker started", slog.String("worker_id", wID))

			for {
				select {
				case <-ctx.Done():
					p.logger.Debug("worker context canceled, stopping", slog.String("worker_id", wID))
					return
				case job, ok := <-p.jobChan:
					if !ok {
						p.logger.Debug("job channel closed, stopping worker", slog.String("worker_id", wID))
						return
					}
					p.processJob(ctx, wID, job)
				}
			}
		}(workerID)
	}
}

// Wait waits for all workers to finish their in-flight jobs.
func (p *Pool) Wait() {
	p.wg.Wait()
}

func (p *Pool) processJob(parentCtx context.Context, workerID string, job store.Job) {
	start := time.Now()
	if p.tracker != nil {
		p.tracker.SetWorkerBusy(workerID, job.ID, job.Type, int(job.Attempts), int(job.MaxAttempts))
	}

	p.logger.Info("executing job",
		slog.String("worker_id", workerID),
		slog.Int64("job_id", job.ID),
		slog.String("type", job.Type),
		slog.Int("attempt", int(job.Attempts)),
		slog.Int("max_attempts", int(job.MaxAttempts)),
	)

	// Recover from panics inside job handlers to prevent crashing the worker goroutine
	defer func() {
		if p.tracker != nil {
			p.tracker.SetWorkerIdle(workerID)
		}
		if r := recover(); r != nil {
			elapsed := time.Since(start)
			p.logger.Error("panic recovered during job execution",
				slog.String("worker_id", workerID),
				slog.Int64("job_id", job.ID),
				slog.Any("panic", r),
			)
			p.handleFailure(job, workerID, fmt.Errorf("panic: %v", r), elapsed)
		}
	}()

	// Enforce per-job execution timeout
	jobCtx, cancel := context.WithTimeout(parentCtx, p.cfg.JobTimeout)
	defer cancel()

	handler, ok := p.registry.Get(job.Type)
	if !ok {
		err := ErrUnknownJobType{JobType: job.Type}
		p.handleFailure(job, workerID, err, time.Since(start))
		return
	}

	err := handler(jobCtx, job.Payload)
	elapsed := time.Since(start)

	if err != nil {
		p.handleFailure(job, workerID, err, elapsed)
		return
	}

	p.handleSuccess(job.ID, workerID, elapsed)
}

func (p *Pool) handleSuccess(jobID int64, workerID string, elapsed time.Duration) {
	updateCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := p.queries.CompleteJob(updateCtx, jobID); err != nil {
		p.logger.Error("failed to mark job as succeeded in database",
			slog.Int64("job_id", jobID),
			slog.String("worker_id", workerID),
			slog.String("error", err.Error()),
		)
		return
	}

	p.logger.Info("job completed successfully",
		slog.Int64("job_id", jobID),
		slog.String("worker_id", workerID),
		slog.Duration("duration", elapsed),
	)

	if p.tracker != nil {
		p.tracker.RecordSuccess(jobID, workerID, elapsed)
	}
}

func (p *Pool) handleFailure(job store.Job, workerID string, jobErr error, elapsed time.Duration) {
	updateCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// If attempts have not reached max_attempts, reschedule for retry with exponential backoff
	if job.Attempts < job.MaxAttempts {
		backoff := CalculateBackoff(int(job.Attempts), p.cfg.BaseBackoff, p.cfg.MaxBackoff)
		nextRunAt := time.Now().Add(backoff)

		_, err := p.queries.RetryJob(updateCtx, store.RetryJobParams{
			ID:        job.ID,
			RunAt:     pgtype.Timestamptz{Time: nextRunAt, Valid: true},
			LastError: jobErr.Error(),
		})
		if err != nil {
			p.logger.Error("failed to reschedule job for retry",
				slog.Int64("job_id", job.ID),
				slog.String("worker_id", workerID),
				slog.String("error", err.Error()),
			)
			return
		}

		p.logger.Warn("job failed, scheduled for retry",
			slog.Int64("job_id", job.ID),
			slog.String("worker_id", workerID),
			slog.Int("attempt", int(job.Attempts)),
			slog.Int("max_attempts", int(job.MaxAttempts)),
			slog.Duration("backoff", backoff),
			slog.Time("next_run_at", nextRunAt),
			slog.String("error", jobErr.Error()),
			slog.Duration("duration", elapsed),
		)

		if p.tracker != nil {
			p.tracker.RecordRetry(job.ID, workerID, int(job.Attempts), int(job.MaxAttempts), nextRunAt, jobErr.Error())
		}
		return
	}

	// Attempts exhausted: transition to 'dead' (Dead-Letter Queue)
	_, err := p.queries.MarkJobDead(updateCtx, store.MarkJobDeadParams{
		ID:        job.ID,
		LastError: jobErr.Error(),
	})
	if err != nil {
		p.logger.Error("failed to mark job as dead in database",
			slog.Int64("job_id", job.ID),
			slog.String("worker_id", workerID),
			slog.String("error", err.Error()),
		)
		return
	}

	p.logger.Error("job exhausted max attempts, moved to dead-letter queue",
		slog.Int64("job_id", job.ID),
		slog.String("worker_id", workerID),
		slog.Int("attempts", int(job.Attempts)),
		slog.Int("max_attempts", int(job.MaxAttempts)),
		slog.String("error", jobErr.Error()),
		slog.Duration("duration", elapsed),
	)

	if p.tracker != nil {
		p.tracker.RecordDead(job.ID, workerID, int(job.Attempts), int(job.MaxAttempts), jobErr.Error())
	}
}
