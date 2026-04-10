package retry

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

const (
	_defaultBackoffMult = 1.0
)

type Config struct {
	// MaxAttempts is the maximum number of retries. 0 means infinite.
	MaxAttempts uint
	// IsRetryable classifies errors. Return false to fail fast.
	// Defaults to always retry if nil.
	IsRetryable func(error) bool
	// Delay is the initial delay between retries.
	Delay time.Duration
	// MaxDelay caps the backoff delay. 0 means no cap.
	MaxDelay time.Duration
	// BackoffMult multiplies the delay after each failed attempt.
	BackoffMult float64
	// OnRetry is called after a failed attempt and before the next retry.
	// attempt is the attempt number that just failed.
	OnRetry func(attempt uint, err error)
}

// Do executes fn with retries, exponential backoff, and jitter.
// It respects context cancellation between attempts.
func Do[T any](ctx context.Context, cfg Config, fn func() (T, error)) (T, error) {
	var zero T

	maxAttempts := cfg.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = ^uint(0) // infinite
	}

	delay := cfg.cappedDelay(cfg.initialDelay())

	for attempt := uint(1); attempt <= maxAttempts; attempt++ {
		result, err := fn()
		if err == nil {
			return result, nil
		}

		if !cfg.retryable(err) {
			return zero, err
		}

		if attempt == maxAttempts {
			return zero, err
		}

		if cfg.OnRetry != nil {
			cfg.OnRetry(attempt, err)
		}

		if err := wait(ctx, delay+jitter(delay)); err != nil {
			return zero, err
		}

		delay = cfg.nextDelay(delay)
	}

	return zero, nil // unreachable
}

// retryable checks if the error is retryable.
func (c Config) retryable(err error) bool {
	return c.IsRetryable == nil || c.IsRetryable(err)
}

// initialDelay returns the initial delay.
func (c Config) initialDelay() time.Duration {
	if c.Delay < 0 {
		return 0
	}
	return c.Delay
}

// backoffMult returns the backoff multiplier.
func (c Config) backoffMult() float64 {
	if c.BackoffMult <= 0 {
		return _defaultBackoffMult
	}
	return c.BackoffMult
}

// cappedDelay caps the delay at the maximum delay.
func (c Config) cappedDelay(delay time.Duration) time.Duration {
	if c.MaxDelay > 0 && delay > c.MaxDelay {
		return c.MaxDelay
	}
	return delay
}

// nextDelay calculates the next delay using the backoff multiplier.
func (c Config) nextDelay(delay time.Duration) time.Duration {
	next := float64(delay) * c.backoffMult()
	if next > float64(math.MaxInt64) {
		return c.cappedDelay(time.Duration(math.MaxInt64))
	}
	return c.cappedDelay(time.Duration(next))
}

// jitter adds random delay to the delay.
func jitter(delay time.Duration) time.Duration {
	if delay <= 1 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(delay) / 2))
}

// wait blocks until the delay has passed or the context is cancelled.
func wait(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
