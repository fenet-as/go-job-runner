package worker_test

import (
	"testing"
	"time"

	"github.com/fenet-as/go-job-runner/internal/worker"
)

func TestCalculateBackoffWithJitter_ExponentialGrowth(t *testing.T) {
	base := 1 * time.Second
	max := 60 * time.Second

	// With jitterFrac = 1.0, delay = raw
	tests := []struct {
		attempt       int
		expectedDelay time.Duration
	}{
		{attempt: 0, expectedDelay: 1 * time.Second},
		{attempt: 1, expectedDelay: 2 * time.Second},
		{attempt: 2, expectedDelay: 4 * time.Second},
		{attempt: 3, expectedDelay: 8 * time.Second},
		{attempt: 4, expectedDelay: 16 * time.Second},
		{attempt: 5, expectedDelay: 32 * time.Second},
		{attempt: 6, expectedDelay: 60 * time.Second}, // capped at max
		{attempt: 10, expectedDelay: 60 * time.Second}, // capped at max
	}

	for _, tt := range tests {
		delay := worker.CalculateBackoffWithJitter(tt.attempt, base, max, 1.0)
		if delay != tt.expectedDelay {
			t.Errorf("attempt %d: expected %v, got %v", tt.attempt, tt.expectedDelay, delay)
		}
	}
}

func TestCalculateBackoffWithJitter_JitterBoundaries(t *testing.T) {
	base := 10 * time.Second
	max := 100 * time.Second
	attempt := 1 // raw = 20s

	minDelay := worker.CalculateBackoffWithJitter(attempt, base, max, 0.0)
	if minDelay != 10*time.Second { // 50% of 20s = 10s
		t.Errorf("expected min jitter delay 10s, got %v", minDelay)
	}

	maxDelay := worker.CalculateBackoffWithJitter(attempt, base, max, 1.0)
	if maxDelay != 20*time.Second { // 100% of 20s = 20s
		t.Errorf("expected max jitter delay 20s, got %v", maxDelay)
	}

	midDelay := worker.CalculateBackoffWithJitter(attempt, base, max, 0.5)
	if midDelay != 15*time.Second { // 75% of 20s = 15s
		t.Errorf("expected mid jitter delay 15s, got %v", midDelay)
	}
}

func TestCalculateBackoff_NeverExceedsMax(t *testing.T) {
	base := 1 * time.Second
	max := 30 * time.Second

	for attempt := 0; attempt < 50; attempt++ {
		for i := 0; i < 20; i++ {
			d := worker.CalculateBackoff(attempt, base, max)
			if d > max {
				t.Fatalf("attempt %d: delay %v exceeded max %v", attempt, d, max)
			}
			if d < 0 {
				t.Fatalf("attempt %d: negative delay %v", attempt, d)
			}
		}
	}
}
