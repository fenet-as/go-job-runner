package telemetry

import (
	"sync"
	"time"
)

// WorkerState captures real-time worker execution telemetry.
type WorkerState struct {
	ID          string     `json:"id"`
	Status      string     `json:"status"` // "idle" | "busy"
	JobID       *int64     `json:"job_id,omitempty"`
	JobType     string     `json:"job_type,omitempty"`
	Attempt     int        `json:"attempt,omitempty"`
	MaxAttempts int        `json:"max_attempts,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
}

// BufferState captures the live status of the job channel buffer.
type BufferState struct {
	Length   int `json:"length"`
	Capacity int `json:"capacity"`
}

// DBStats captures job count aggregations from PostgreSQL.
type DBStats struct {
	Queued    int64 `json:"queued"`
	Running   int64 `json:"running"`
	Succeeded int64 `json:"succeeded"`
	Failed    int64 `json:"failed"`
	Dead      int64 `json:"dead"`
	Total     int64 `json:"total"`
}

// DashboardStats contains a complete snapshot of backend telemetry.
type DashboardStats struct {
	Workers   []WorkerState `json:"workers"`
	Buffer    BufferState   `json:"buffer"`
	DB        DBStats       `json:"db"`
	Timestamp time.Time     `json:"timestamp"`
}

// DashboardEvent represents a real-time event streamed to SSE clients.
type DashboardEvent struct {
	Type        string     `json:"type"` // "job_dispatched", "worker_busy", "worker_idle", "job_completed", "job_retried", "job_dead", "buffer_update"
	WorkerID    string     `json:"worker_id,omitempty"`
	JobID       int64      `json:"job_id,omitempty"`
	JobType     string     `json:"job_type,omitempty"`
	Attempt     int        `json:"attempt,omitempty"`
	MaxAttempts int        `json:"max_attempts,omitempty"`
	DurationMs  int64      `json:"duration_ms,omitempty"`
	NextRunAt   *time.Time `json:"next_run_at,omitempty"`
	Error       string     `json:"error,omitempty"`
	BufferLen   int        `json:"buffer_len"`
	BufferCap   int        `json:"buffer_cap"`
	Timestamp   time.Time  `json:"timestamp"`
}

// Tracker maintains thread-safe in-memory status of workers and backpressure.
type Tracker struct {
	mu          sync.RWMutex
	workers     map[string]*WorkerState
	workerOrder []string
	buffer      BufferState
	broadcaster *Broadcaster
}

// NewTracker creates a new telemetry tracker.
func NewTracker(workerCount int, bufferCap int) *Tracker {
	t := &Tracker{
		workers:     make(map[string]*WorkerState),
		workerOrder: make([]string, 0, workerCount),
		buffer: BufferState{
			Length:   0,
			Capacity: bufferCap,
		},
		broadcaster: NewBroadcaster(),
	}

	for i := 0; i < workerCount; i++ {
		wID := formatWorkerID(i)
		t.workerOrder = append(t.workerOrder, wID)
		t.workers[wID] = &WorkerState{
			ID:     wID,
			Status: "idle",
		}
	}

	return t
}

func formatWorkerID(index int) string {
	return "worker-" + padZero(index, 2)
}

func padZero(n int, width int) string {
	s := ""
	for w := 1; w < width; w++ {
		if n < 10 {
			s += "0"
		}
	}
	return s + string(rune('0'+n%10))
}

// Subscribe returns an SSE event channel and an unsubscribe cleanup closure.
func (t *Tracker) Subscribe() (<-chan DashboardEvent, func()) {
	return t.broadcaster.Subscribe()
}

// SetWorkerBusy marks a worker as executing a specific job.
func (t *Tracker) SetWorkerBusy(workerID string, jobID int64, jobType string, attempt, maxAttempts int) {
	now := time.Now()
	t.mu.Lock()
	state, exists := t.workers[workerID]
	if !exists {
		state = &WorkerState{ID: workerID}
		t.workers[workerID] = state
		t.workerOrder = append(t.workerOrder, workerID)
	}
	state.Status = "busy"
	state.JobID = &jobID
	state.JobType = jobType
	state.Attempt = attempt
	state.MaxAttempts = maxAttempts
	state.StartedAt = &now
	bufLen := t.buffer.Length
	bufCap := t.buffer.Capacity
	t.mu.Unlock()

	t.broadcaster.Broadcast(DashboardEvent{
		Type:        "worker_busy",
		WorkerID:    workerID,
		JobID:       jobID,
		JobType:     jobType,
		Attempt:     attempt,
		MaxAttempts: maxAttempts,
		BufferLen:   bufLen,
		BufferCap:   bufCap,
		Timestamp:   now,
	})
}

// SetWorkerIdle marks a worker as free and waiting for jobs.
func (t *Tracker) SetWorkerIdle(workerID string) {
	now := time.Now()
	t.mu.Lock()
	if state, exists := t.workers[workerID]; exists {
		state.Status = "idle"
		state.JobID = nil
		state.JobType = ""
		state.Attempt = 0
		state.MaxAttempts = 0
		state.StartedAt = nil
	}
	bufLen := t.buffer.Length
	bufCap := t.buffer.Capacity
	t.mu.Unlock()

	t.broadcaster.Broadcast(DashboardEvent{
		Type:      "worker_idle",
		WorkerID:  workerID,
		BufferLen: bufLen,
		BufferCap: bufCap,
		Timestamp: now,
	})
}

// RecordDispatch emits an event when the dispatcher feeds a job into the buffered channel.
func (t *Tracker) RecordDispatch(jobID int64, jobType string, bufLen, bufCap int) {
	now := time.Now()
	t.mu.Lock()
	t.buffer.Length = bufLen
	t.buffer.Capacity = bufCap
	t.mu.Unlock()

	t.broadcaster.Broadcast(DashboardEvent{
		Type:      "job_dispatched",
		JobID:     jobID,
		JobType:   jobType,
		BufferLen: bufLen,
		BufferCap: bufCap,
		Timestamp: now,
	})
}

// RecordSuccess records job success.
func (t *Tracker) RecordSuccess(jobID int64, workerID string, duration time.Duration) {
	now := time.Now()
	t.mu.RLock()
	bufLen := t.buffer.Length
	bufCap := t.buffer.Capacity
	t.mu.RUnlock()

	t.broadcaster.Broadcast(DashboardEvent{
		Type:       "job_completed",
		JobID:      jobID,
		WorkerID:   workerID,
		DurationMs: duration.Milliseconds(),
		BufferLen:  bufLen,
		BufferCap:  bufCap,
		Timestamp:  now,
	})
}

// RecordRetry records when a job failed and was rescheduled.
func (t *Tracker) RecordRetry(jobID int64, workerID string, attempt, maxAttempts int, nextRunAt time.Time, reason string) {
	now := time.Now()
	t.mu.RLock()
	bufLen := t.buffer.Length
	bufCap := t.buffer.Capacity
	t.mu.RUnlock()

	t.broadcaster.Broadcast(DashboardEvent{
		Type:        "job_retried",
		JobID:       jobID,
		WorkerID:    workerID,
		Attempt:     attempt,
		MaxAttempts: maxAttempts,
		NextRunAt:   &nextRunAt,
		Error:       reason,
		BufferLen:   bufLen,
		BufferCap:   bufCap,
		Timestamp:   now,
	})
}

// RecordDead records when a job exhausted retries and moved to DLQ.
func (t *Tracker) RecordDead(jobID int64, workerID string, attempts, maxAttempts int, reason string) {
	now := time.Now()
	t.mu.RLock()
	bufLen := t.buffer.Length
	bufCap := t.buffer.Capacity
	t.mu.RUnlock()

	t.broadcaster.Broadcast(DashboardEvent{
		Type:        "job_dead",
		JobID:       jobID,
		WorkerID:    workerID,
		Attempt:     attempts,
		MaxAttempts: maxAttempts,
		Error:       reason,
		BufferLen:   bufLen,
		BufferCap:   bufCap,
		Timestamp:   now,
	})
}

// UpdateBuffer updates current channel occupancy.
func (t *Tracker) UpdateBuffer(bufLen, bufCap int) {
	t.mu.Lock()
	t.buffer.Length = bufLen
	t.buffer.Capacity = bufCap
	t.mu.Unlock()
}

// Snapshot returns an atomic read of the current tracker states.
func (t *Tracker) Snapshot() DashboardStats {
	t.mu.RLock()
	defer t.mu.RUnlock()

	workers := make([]WorkerState, 0, len(t.workerOrder))
	for _, id := range t.workerOrder {
		if w, ok := t.workers[id]; ok {
			workers = append(workers, *w)
		}
	}

	return DashboardStats{
		Workers:   workers,
		Buffer:    t.buffer,
		Timestamp: time.Now(),
	}
}
