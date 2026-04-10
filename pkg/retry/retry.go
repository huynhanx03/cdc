package retry

import (
	"context"
	"math/rand/v2"
	"time"
)

// Config controls retry behavior for an operation returning type T.
type Config[T any] struct {
	// MaxAttempts is the maximum number of retries. 0 means infinite.
	MaxAttempts uint
	// IsRetryable classifies errors. Return false to fail fast.
	// Defaults to always retry if nil.
	IsRetryable func(error) bool
	// Delay is the initial delay between retries.
	Delay time.Duration
	// MaxDelay caps the backoff delay.
	MaxDelay time.Duration
	// BackoffMult multiplies the delay after each attempt (e.g. 2.0 for doubling).
	BackoffMult float64
	// OnRetry is called before each retry attempt.
	OnRetry func(attempt uint, err error)
}

// Do executes fn with retries, exponential backoff, and jitter.
// It respects context cancellation between attempts.
func Do[T any](ctx context.Context, cfg Config[T], fn func() (T, error)) (T, error) {
	var (
		lastErr error
		delay   = cfg.Delay
		zero    T
	)

	maxAttempts := cfg.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = ^uint(0) // infinite
	}

	for attempt := uint(1); attempt <= maxAttempts; attempt++ {
		result, err := fn()
		if err == nil {
			return result, nil
		}
		lastErr = err

		// Check if error is retryable
		if cfg.IsRetryable != nil && !cfg.IsRetryable(err) {
			return zero, err
		}

		// Don't sleep after last attempt
		if attempt >= maxAttempts {
			break
		}

		if cfg.OnRetry != nil {
			cfg.OnRetry(attempt, err)
		}

		// Wait with jitter to avoid thundering herd
		var jitter time.Duration
		if maxJitter := int64(delay) / 2; maxJitter > 0 {
			jitter = time.Duration(rand.Int64N(maxJitter))
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(delay + jitter):
		}

		// Exponential backoff
		delay = time.Duration(float64(delay) * cfg.BackoffMult)
		if delay > cfg.MaxDelay && cfg.MaxDelay > 0 {
			delay = cfg.MaxDelay
		}
	}

	return zero, lastErr
}
