package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/fenet-as/go-job-runner/internal/api"
	"github.com/fenet-as/go-job-runner/internal/config"
	"github.com/fenet-as/go-job-runner/internal/queue"
	"github.com/fenet-as/go-job-runner/internal/store"
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

	// Connect to PostgreSQL
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	// 1. Initialize Handler Registry
	registry := worker.NewRegistry()
	worker.RegisterDefaultHandlers(registry)

	// 2. Buffered Channel (Backpressure Buffer)
	bufferSize := cfg.WorkerCount * 2
	jobChan := make(chan store.Job, bufferSize)

	// 3. Initialize and Start Worker Pool
	workerPool := worker.NewPool(worker.PoolConfig{
		WorkerCount: cfg.WorkerCount,
		JobTimeout:  30 * time.Second,
	}, registry, queries, logger, jobChan)
	workerPool.Start(ctx)

	// 4. Initialize and Start Dispatcher Goroutine
	dispatcher := queue.NewDispatcher(queue.DispatcherConfig{
		PollInterval: cfg.PollInterval,
		DispatcherID: "dispatcher-main",
	}, queries, logger, jobChan)
	go dispatcher.Start(ctx)

	// 5. Initialize API server
	server := api.NewServer(pool, logger)

	addr := fmt.Sprintf(":%s", cfg.Port)
	logger.Info("HTTP server listening", slog.String("addr", addr))

	httpServer := &http.Server{
		Addr:    addr,
		Handler: server.Router(),
	}

	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("HTTP server failed", "error", err)
		os.Exit(1)
	}
}
