package queue

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/fenet-as/go-job-runner/internal/store"
	"github.com/jackc/pgx/v5"
)

// DispatcherConfig holds configuration for the job dispatcher.
type DispatcherConfig struct {
	PollInterval time.Duration
	DispatcherID string
}

// Dispatcher polls PostgreSQL for ready jobs and feeds them to worker goroutines via a channel.
type Dispatcher struct {
	cfg     DispatcherConfig
	queries *store.Queries
	logger  *slog.Logger
	jobChan chan<- store.Job
}

// NewDispatcher creates a new Dispatcher instance.
func NewDispatcher(cfg DispatcherConfig, queries *store.Queries, logger *slog.Logger, jobChan chan<- store.Job) *Dispatcher {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 1 * time.Second
	}
	if cfg.DispatcherID == "" {
		cfg.DispatcherID = "dispatcher-01"
	}

	return &Dispatcher{
		cfg:     cfg,
		queries: queries,
		logger:  logger,
		jobChan: jobChan,
	}
}

// Start runs the polling loop until the given context is canceled.
func (d *Dispatcher) Start(ctx context.Context) {
	d.logger.Info("dispatcher started",
		slog.String("dispatcher_id", d.cfg.DispatcherID),
		slog.Duration("poll_interval", d.cfg.PollInterval),
	)

	for {
		select {
		case <-ctx.Done():
			d.logger.Info("dispatcher stopping on context cancellation")
			return
		default:
		}

		// Attempt to claim next ready job
		job, err := d.queries.ClaimJob(ctx, d.cfg.DispatcherID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// No ready jobs; sleep for poll interval with context responsiveness
				select {
				case <-time.After(d.cfg.PollInterval):
					continue
				case <-ctx.Done():
					return
				}
			}

			// In case the context was canceled during query execution
			if errors.Is(err, context.Canceled) {
				return
			}

			d.logger.Error("dispatcher error claiming job", slog.String("error", err.Error()))
			select {
			case <-time.After(1 * time.Second):
				continue
			case <-ctx.Done():
				return
			}
		}

		// Push claimed job into the channel.
		// If the channel is full, this send blocks until a worker reads from it (backpressure).
		select {
		case d.jobChan <- job:
			d.logger.Debug("dispatched job to channel", slog.Int64("job_id", job.ID), slog.String("type", job.Type))
		case <-ctx.Done():
			d.logger.Warn("dispatcher context canceled while holding claimed job", slog.Int64("job_id", job.ID))
			return
		}
	}
}
