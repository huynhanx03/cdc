package common

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"strings"
)

const (
	ReasonUnknown         = "unknown"
	ReasonInvalidRecord   = "invalid_record"
	ReasonMissingMetadata = "missing_metadata"
	ReasonCanceled        = "canceled"
	ReasonTimeout         = "timeout"
	ReasonConnection      = "connection"
	ReasonBusy            = "busy"
)

type SinkError struct {
	Retryable bool
	Reason    string
	Err       error
}

func (e *SinkError) Error() string {
	if e == nil || e.Err == nil {
		return "sink error"
	}
	return e.Err.Error()
}

func (e *SinkError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func PermanentError(reason string, err error) error {
	return sinkError(reason, false, err)
}

func RetryableError(reason string, err error) error {
	return sinkError(reason, true, err)
}

func ClassifySinkError(err error) error {
	if err == nil {
		return nil
	}
	var sinkErr *SinkError
	if errors.As(err, &sinkErr) {
		return err
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return RetryableError(ReasonTimeout, err)
	case errors.Is(err, context.Canceled):
		return PermanentError(ReasonCanceled, err)
	case errors.Is(err, sql.ErrConnDone):
		return RetryableError(ReasonConnection, err)
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return RetryableError(ReasonTimeout, err)
	}

	text := strings.ToLower(err.Error())
	switch {
	case containsAny(text, "timeout", "deadline exceeded", "i/o timeout"):
		return RetryableError(ReasonTimeout, err)
	case containsAny(text, "connection refused", "connection reset", "broken pipe", "server closed", "bad connection", "conn done"):
		return RetryableError(ReasonConnection, err)
	case containsAny(text, "deadlock", "lock wait timeout", "too many connections", "temporarily unavailable"):
		return RetryableError(ReasonBusy, err)
	}
	return PermanentError(ReasonUnknown, err)
}

func sinkError(reason string, retryable bool, err error) error {
	if err == nil {
		return nil
	}
	if reason == "" {
		reason = ReasonUnknown
	}
	return &SinkError{Retryable: retryable, Reason: reason, Err: err}
}

func containsAny(text string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}
