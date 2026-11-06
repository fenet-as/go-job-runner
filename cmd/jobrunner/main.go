package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fenet-as/go-job-runner/internal/api"
	"github.com/fenet-as/go-job-runner/internal/config"
	"github.com/fenet-as/go-job-runner/internal/queue"
	"github.com/fenet-as/go-job-runner/internal/store"
	"github.com/fenet-as/go-job-runner/internal/telemetry"
	"github.com/fenet-as/go-job-runner/internal/worker"
)

func main() {
	cfg := config.Load()

	// Initialize structured logger
	var logLevel slog.Level
	switch cfg.LogLevel {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	logger.Info("starting jobrunner service",
		slog.String("port", cfg.Port),
		slog.Int("workers", cfg.WorkerCount),
		slog.Duration("poll_interval", cfg.PollInterval),
	)

	// 1. Root context tied to OS termination signals (SIGINT, SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 2. Connect to PostgreSQL
	pool, err := store.NewPool(ctx, store.PoolConfig{
		DatabaseURL: cfg.DatabaseURL,
		MaxConns:    int32(cfg.WorkerCount + 10),
	})
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	logger.Info("connected to PostgreSQL successfully")

	queries := store.New(pool)

	// 3. Initialize Handler Registry
	registry := worker.NewRegistry()
	worker.RegisterDefaultHandlers(registry)

	// 4. Buffered Channel (Backpressure Buffer)
	bufferSize := cfg.WorkerCount * 2
	jobChan := make(chan store.Job, bufferSize)

	// 5. Initialize Telemetry Tracker for Real-Time Visual Dashboard
	tracker := telemetry.NewTracker(cfg.WorkerCount, bufferSize)

	// 6. Initialize and Start Worker Pool
	workerPool := worker.NewPool(worker.PoolConfig{
		WorkerCount: cfg.WorkerCount,
		JobTimeout:  30 * time.Second,
	}, registry, queries, logger, jobChan)
	workerPool.SetTracker(tracker)
	workerPool.Start(ctx)

	// 7. Initialize and Start Dispatcher Goroutine
	dispatcherCtx, cancelDispatcher := context.WithCancel(ctx)
	dispatcher := queue.NewDispatcher(queue.DispatcherConfig{
		PollInterval: cfg.PollInterval,
		DispatcherID: "dispatcher-main",
	}, queries, logger, jobChan)
	dispatcher.SetTracker(tracker)
	go dispatcher.Start(dispatcherCtx)

	// 8. Initialize and Start Crash Reaper
	reaperCtx, cancelReaper := context.WithCancel(ctx)
	reaper := queue.NewReaper(queue.ReaperConfig{
		Interval:         30 * time.Second,
		TimeoutThreshold: 5 * time.Minute,
		BatchSize:        50,
	}, queries, logger)
	go reaper.Start(reaperCtx)

	// 9. Initialize API server
	server := api.NewServer(pool, logger)
	server.SetTracker(tracker)

	addr := fmt.Sprintf(":%s", cfg.Port)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: server.Router(),
	}

	go func() {
		logger.Info("HTTP server listening", slog.String("addr", addr))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server failed", "error", err)
			stop() // Signal application shutdown
		}
	}()

	// 9. Wait for OS Termination Signal
	<-ctx.Done()
	logger.Info("shutdown signal received, initiating graceful shutdown...")

	// Step A: Stop accepting new HTTP requests with a 10s deadline
	shutdownCtx, cancelHTTP := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelHTTP()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server forced to shutdown", "error", err)
	} else {
		logger.Info("HTTP server stopped accepting new requests")
	}

	// Step B: Stop dispatcher from claiming new jobs
	cancelDispatcher()
	logger.Info("dispatcher stopped")

	// Step C: Stop reaper
	cancelReaper()
	logger.Info("reaper stopped")

	// Step D: Close job channel so worker loops exit when drained
	close(jobChan)
	logger.Info("job channel closed, waiting for in-flight workers to finish...")

	// Step E: Wait for workers to finish in-flight jobs with an overall deadline
	drainDone := make(chan struct{})
	go func() {
		workerPool.Wait()
		close(drainDone)
	}()

	select {
	case <-drainDone:
		logger.Info("all workers finished in-flight jobs cleanly")
	case <-time.After(30 * time.Second):
		logger.Warn("worker drain deadline exceeded, forcing termination")
	}

	logger.Info("service shutdown complete")
}
