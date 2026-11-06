package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/fenet-as/go-job-runner/internal/telemetry"
	"github.com/gin-gonic/gin"
)

// DashboardPageHandler serves the single-page HTML architecture visualizer.
func (s *Server) DashboardPageHandler(c *gin.Context) {
	c.Data(http.StatusOK, "text/html; charset=utf-8", dashboardHTML)
}

// DashboardStatsHandler returns an aggregated JSON snapshot of backend telemetry and database counts.
func (s *Server) DashboardStatsHandler(c *gin.Context) {
	var stats telemetry.DashboardStats
	if s.tracker != nil {
		stats = s.tracker.Snapshot()
	} else {
		stats = telemetry.DashboardStats{
			Timestamp: time.Now(),
		}
	}

	// Query PostgreSQL for real-time status counts
	queryCtx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	rows, err := s.pool.Query(queryCtx, "SELECT status, COUNT(*) FROM jobs GROUP BY status")
	if err == nil {
		defer rows.Close()
		var dbStats telemetry.DBStats
		for rows.Next() {
			var status string
			var count int64
			if err := rows.Scan(&status, &count); err == nil {
				switch status {
				case "queued":
					dbStats.Queued = count
				case "running":
					dbStats.Running = count
				case "succeeded":
					dbStats.Succeeded = count
				case "failed":
					dbStats.Failed = count
				case "dead":
					dbStats.Dead = count
				}
				dbStats.Total += count
			}
		}
		stats.DB = dbStats
	}

	c.JSON(http.StatusOK, stats)
}

// DashboardEventsHandler streams real-time telemetry events via Server-Sent Events (SSE).
func (s *Server) DashboardEventsHandler(c *gin.Context) {
	if s.tracker == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "telemetry tracker is not initialized"})
		return
	}

	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("Transfer-Encoding", "chunked")

	eventsChan, unsubscribe := s.tracker.Subscribe()
	defer unsubscribe()

	// Send initial snapshot as the first SSE message
	initialStats := s.tracker.Snapshot()
	if initialBytes, err := json.Marshal(initialStats); err == nil {
		c.SSEvent("initial_state", string(initialBytes))
		c.Writer.Flush()
	}

	notify := c.Request.Context().Done()

	c.Stream(func(w io.Writer) bool {
		select {
		case <-notify:
			return false
		case ev, ok := <-eventsChan:
			if !ok {
				return false
			}
			evBytes, err := json.Marshal(ev)
			if err != nil {
				return true
			}
			c.SSEvent("dashboard_event", string(evBytes))
			return true
		}
	})
}
