package worker

import (
	"math"
	"math/rand"
	"time"
)

// CalculateBackoff computes exponential backoff with random jitter.
// Formula: min(max, base * 2^attempt) with [50%..100%] decorrelated jitter.
func CalculateBackoff(attempt int, base time.Duration, max time.Duration) time.Duration {
	return CalculateBackoffWithJitter(attempt, base, max, rand.Float64())
}

// CalculateBackoffWithJitter calculates backoff with an explicit jitter fraction in [0.0, 1.0]
// for deterministic unit testing.
func CalculateBackoffWithJitter(attempt int, base time.Duration, max time.Duration, jitterFrac float64) time.Duration {
	if base <= 0 {
		base = 1 * time.Second
	}
	if max <= 0 {
		max = 1 * time.Hour
	}
	if attempt < 0 {
		attempt = 0
	}
	// Avoid integer overflow for very high attempt counts
	if attempt > 30 {
		attempt = 30
	}

	// Exponential factor: 2^attempt
	multiplier := math.Pow(2, float64(attempt))
	raw := float64(base) * multiplier

	if raw > float64(max) {
		raw = float64(max)
	}

	// Clamp jitter fraction to [0.0, 1.0]
	if jitterFrac < 0 {
		jitterFrac = 0
	}
	if jitterFrac > 1 {
		jitterFrac = 1
	}

	// Jitter between 50% and 100% of raw exponential delay
	minDelay := raw * 0.5
	jitterRange := raw * 0.5
	actual := minDelay + (jitterRange * jitterFrac)

	return time.Duration(actual)
}
