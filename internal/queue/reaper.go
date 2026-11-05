package queue

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/fenet-as/go-job-runner/internal/store"
	"github.com/jackc/pgx/v5/pgtype"
)

// ReaperConfig configures the crash recovery reaper.
type ReaperConfig struct {
	Interval         time.Duration
	TimeoutThreshold time.Duration
	BatchSize        int32
}

// Reaper sweeps PostgreSQL periodically to rescue jobs abandoned by crashed or killed workers.
type Reaper struct {
	cfg     ReaperConfig
	queries *store.Queries
	logger  *slog.Logger
}

// NewReaper creates a new Reaper instance.
func NewReaper(cfg ReaperConfig, queries *store.Queries, logger *slog.Logger) *Reaper {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.TimeoutThreshold <= 0 {
		cfg.TimeoutThreshold = 5 * time.Minute
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}

	return &Reaper{
		cfg:     cfg,
		queries: queries,
		logger:  logger,
	}
}

// Start begins the periodic reaper loop until the context is canceled.
func (r *Reaper) Start(ctx context.Context) {
	r.logger.Info("reaper started",
		slog.Duration("interval", r.cfg.Interval),
		slog.Duration("timeout_threshold", r.cfg.TimeoutThreshold),
		slog.Int("batch_size", int(r.cfg.BatchSize)),
	)

	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("reaper stopping on context cancellation")
			return
		case <-ticker.C:
			r.ReapOnce(ctx)
		}
	}
}

// ReapOnce executes a single reap iteration. Useful for testing.
func (r *Reaper) ReapOnce(ctx context.Context) ([]store.Job, error) {
	threshold := time.Now().Add(-r.cfg.TimeoutThreshold)

	reaped, err := r.queries.RequeueStuckJobs(ctx, store.RequeueStuckJobsParams{
		Threshold: pgtype.Timestamptz{
			Time:  threshold,
			Valid: true,
		},
		BatchSize: r.cfg.BatchSize,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, err
		}
		r.logger.Error("reaper failed to requeue stuck jobs", slog.String("error", err.Error()))
		return nil, err
	}

	if len(reaped) > 0 {
		r.logger.Warn("reaper rescued orphaned jobs from crashed workers",
			slog.Int("count", len(reaped)),
		)
	}

	return reaped, nil
}
