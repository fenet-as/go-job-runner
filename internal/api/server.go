package api

import (
	_ "embed"
	"log/slog"

	"github.com/fenet-as/go-job-runner/internal/store"
	"github.com/fenet-as/go-job-runner/internal/telemetry"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed dashboard.html
var dashboardHTML []byte

// Server coordinates HTTP routing, handlers, and persistence.
type Server struct {
	router  *gin.Engine
	queries *store.Queries
	pool    *pgxpool.Pool
	logger  *slog.Logger
	tracker *telemetry.Tracker
}

// NewServer builds and configures a new HTTP Server instance.
func NewServer(pool *pgxpool.Pool, logger *slog.Logger) *Server {
	// Disable Gin's default logger in favor of structured slog
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()

	s := &Server{
		router:  router,
		queries: store.New(pool),
		pool:    pool,
		logger:  logger,
	}

	// Attach custom slog logging and panic recovery
	router.Use(SlogMiddleware(logger))
	router.Use(SlogRecovery(logger))

	// Register routes
	s.registerRoutes()

	return s
}

// SetTracker configures the real-time telemetry tracker for dashboard endpoints.
func (s *Server) SetTracker(tracker *telemetry.Tracker) {
	s.tracker = tracker
}

// Router returns the underlying gin.Engine for testing or running.
func (s *Server) Router() *gin.Engine {
	return s.router
}

func (s *Server) registerRoutes() {
	s.router.GET("/healthz", s.HealthzHandler)

	// Dashboard and Telemetry routes
	s.router.GET("/", s.DashboardPageHandler)
	s.router.GET("/dashboard", s.DashboardPageHandler)
	s.router.GET("/api/dashboard/stats", s.DashboardStatsHandler)
	s.router.GET("/api/dashboard/events", s.DashboardEventsHandler)

	jobsGroup := s.router.Group("/jobs")
	{
		jobsGroup.POST("", s.CreateJobHandler)
		jobsGroup.GET("", s.ListJobsHandler)
		jobsGroup.GET("/:id", s.GetJobByIDHandler)
		jobsGroup.DELETE("/:id", s.CancelJobHandler)
		jobsGroup.POST("/:id/retry", s.RequeueDeadJobHandler)
	}
}
