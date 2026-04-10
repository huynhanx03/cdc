package errors

import (
	"errors"
	"fmt"
)

// Pipeline errors — use errors.Is() for comparison.
var (
	// ErrSourceStopped is returned when a source voluntarily stops.
	ErrSourceStopped = errors.New("source stopped")

	// ErrSinkUnreachable is returned when a sink connection cannot be established.
	ErrSinkUnreachable = errors.New("sink unreachable")

	// ErrNATSDisconnected is returned when NATS connection is lost.
	ErrNATSDisconnected = errors.New("nats disconnected")

	// ErrLeaderLost is returned when this instance loses leadership.
	ErrLeaderLost = errors.New("leader election lost")

	// ErrSnapshotInvalidated is returned when the snapshot transaction
	// is closed unexpectedly (e.g. coordinator restart).
	ErrSnapshotInvalidated = errors.New("snapshot invalidated")

	// ErrNonRetryable wraps errors that should not be retried.
	ErrNonRetryable = errors.New("non-retryable error")
)

// IsNonRetryable checks if an error is wrapped as non-retryable.
func IsNonRetryable(err error) bool {
	return errors.Is(err, ErrNonRetryable)
}

// Permanent wraps an error as non-retryable.
// Retry frameworks should check IsNonRetryable() and fail fast.
func Permanent(err error) error {
	return fmt.Errorf("%w: %w", ErrNonRetryable, err)
}
