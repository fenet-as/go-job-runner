package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/fenet-as/go-job-runner/internal/api"
	"github.com/fenet-as/go-job-runner/internal/config"
	"github.com/fenet-as/go-job-runner/internal/store"
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
	ctx := context.Background()
	pool, err := store.NewPool(ctx, store.PoolConfig{
		DatabaseURL: cfg.DatabaseURL,
		MaxConns:    int32(cfg.WorkerCount + 5),
	})
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	logger.Info("connected to PostgreSQL successfully")

	// Initialize API server
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
