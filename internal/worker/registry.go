package worker

import (
	"context"
	"fmt"
	"sync"
)

// Handler defines the function signature for executing a background job.
type Handler func(ctx context.Context, payload []byte) error

// Registry stores and resolves job handlers by job type in a thread-safe manner.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry creates a new handler registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]Handler),
	}
}

// Register registers a handler for a specific job type.
func (r *Registry) Register(jobType string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[jobType] = h
}

// Get retrieves the handler for the given job type.
func (r *Registry) Get(jobType string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[jobType]
	return h, ok
}

// ErrUnknownJobType indicates that no handler is registered for a job type.
type ErrUnknownJobType struct {
	JobType string
}

func (e ErrUnknownJobType) Error() string {
	return fmt.Sprintf("no handler registered for job type: %s", e.JobType)
}
