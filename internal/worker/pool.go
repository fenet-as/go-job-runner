package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/fenet-as/go-job-runner/internal/store"
)

// PoolConfig holds parameters for the worker pool.
type PoolConfig struct {
	WorkerCount int
	JobTimeout  time.Duration
}

// Pool manages a fixed number of worker goroutines consuming jobs from a channel.
type Pool struct {
	cfg      PoolConfig
	registry *Registry
	queries  *store.Queries
	logger   *slog.Logger
	jobChan  <-chan store.Job
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

	return &Pool{
		cfg:      cfg,
		registry: registry,
		queries:  queries,
		logger:   logger,
		jobChan:  jobChan,
	}
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
	p.logger.Info("executing job",
		slog.String("worker_id", workerID),
		slog.Int64("job_id", job.ID),
		slog.String("type", job.Type),
	)

	// Recover from panics inside job handlers to prevent crashing the worker goroutine
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("panic recovered during job execution",
				slog.String("worker_id", workerID),
				slog.Int64("job_id", job.ID),
				slog.Any("panic", r),
			)

			// Record failure in DB using a fresh context
			updateCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, _ = p.queries.FailJob(updateCtx, store.FailJobParams{
				ID:        job.ID,
				LastError: fmt.Sprintf("panic: %v", r),
			})
		}
	}()

	// Enforce per-job execution timeout
	jobCtx, cancel := context.WithTimeout(parentCtx, p.cfg.JobTimeout)
	defer cancel()

	handler, ok := p.registry.Get(job.Type)
	if !ok {
		err := ErrUnknownJobType{JobType: job.Type}
		p.handleFailure(job.ID, workerID, err, time.Since(start))
		return
	}

	err := handler(jobCtx, job.Payload)
	elapsed := time.Since(start)

	if err != nil {
		p.handleFailure(job.ID, workerID, err, elapsed)
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
}

func (p *Pool) handleFailure(jobID int64, workerID string, jobErr error, elapsed time.Duration) {
	p.logger.Warn("job execution failed",
		slog.Int64("job_id", jobID),
		slog.String("worker_id", workerID),
		slog.String("error", jobErr.Error()),
		slog.Duration("duration", elapsed),
	)

	updateCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := p.queries.FailJob(updateCtx, store.FailJobParams{
		ID:        jobID,
		LastError: jobErr.Error(),
	}); err != nil {
		p.logger.Error("failed to record job failure in database",
			slog.Int64("job_id", jobID),
			slog.String("worker_id", workerID),
			slog.String("error", err.Error()),
		)
	}
}
