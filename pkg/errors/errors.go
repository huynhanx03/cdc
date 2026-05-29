package errors

import (
	"errors"
	"fmt"
)

// Sentinel errors — use errors.Is() for comparison.
var (
	// ErrSourceStopped is returned when a source voluntarily stops.
	ErrSourceStopped = errors.New("source stopped")

	// ErrSinkUnreachable is returned when a sink connection cannot be established.
	ErrSinkUnreachable = errors.New("sink unreachable")

	// ErrNATSDisconnected is returned when NATS connection is lost.
	ErrNATSDisconnected = errors.New("nats disconnected")

	// ErrSourceConfigRequired is returned when a source API request has no source config.
	ErrSourceConfigRequired = errors.New("source config is required")

	// ErrSinkConfigRequired is returned when a sink API request has no sink config.
	ErrSinkConfigRequired = errors.New("sink config is required")

	// ErrDuplicateConfig is returned when a source, sink, or flow would duplicate an existing config.
	ErrDuplicateConfig = errors.New("duplicate config")

	// ErrValidation is returned for actionable user input errors.
	ErrValidation = errors.New("validation error")

	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = errors.New("not found")

	// ErrNonRetryable wraps errors that should not be retried.
	// Retry frameworks should check IsNonRetryable() and fail fast.
	ErrNonRetryable = errors.New("non-retryable error")
)

const (
	DLQErrorSink      = "sink_error"
	DLQErrorFilter    = "filter_error"
	DLQErrorMapping   = "mapping_error"
	DLQErrorMalformed = "malformed_event"
)

// IsNonRetryable checks if an error is wrapped as non-retryable.
func IsNonRetryable(err error) bool {
	return errors.Is(err, ErrNonRetryable)
}

// Permanent wraps an error as non-retryable.
// Use this to signal that retrying will not help (e.g., invalid config, auth failure).
func Permanent(err error) error {
	return fmt.Errorf("%w: %w", ErrNonRetryable, err)
}
