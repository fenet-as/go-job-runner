package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

// EchoPayload defines the payload for echo jobs.
type EchoPayload struct {
	Message string `json:"message"`
}

// EchoHandler handles echo jobs by returning nil.
func EchoHandler(ctx context.Context, payload []byte) error {
	var p EchoPayload
	if len(payload) > 0 {
		_ = json.Unmarshal(payload, &p)
	}
	return nil
}

// SleepPayload defines the payload for sleep jobs.
type SleepPayload struct {
	Seconds float64 `json:"seconds"`
}

// SleepHandler sleeps for the specified duration or until context cancellation.
func SleepHandler(ctx context.Context, payload []byte) error {
	var p SleepPayload
	p.Seconds = 1.0 // default 1 second
	if len(payload) > 0 {
		_ = json.Unmarshal(payload, &p)
	}

	if p.Seconds <= 0 {
		p.Seconds = 1.0
	}

	duration := time.Duration(p.Seconds * float64(time.Second))
	select {
	case <-time.After(duration):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// FailRandomlyPayload defines the payload for fail_randomly jobs.
type FailRandomlyPayload struct {
	FailPercentage int `json:"fail_percentage"`
}

// FailRandomlyHandler fails with a given probability (default 50%).
func FailRandomlyHandler(ctx context.Context, payload []byte) error {
	var p FailRandomlyPayload
	p.FailPercentage = 50 // default 50%
	if len(payload) > 0 {
		_ = json.Unmarshal(payload, &p)
	}

	if p.FailPercentage > 0 && rand.Intn(100) < p.FailPercentage {
		return fmt.Errorf("random simulated failure (probability: %d%%)", p.FailPercentage)
	}

	return nil
}

// RegisterDefaultHandlers registers standard handlers (echo, sleep, fail_randomly).
func RegisterDefaultHandlers(reg *Registry) {
	reg.Register("echo", EchoHandler)
	reg.Register("sleep", SleepHandler)
	reg.Register("fail_randomly", FailRandomlyHandler)
}
