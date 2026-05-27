package common

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestClassifySinkErrorRetryableTimeout(t *testing.T) {
	err := ClassifySinkError(fmt.Errorf("write sink: %w", context.DeadlineExceeded))

	var sinkErr *SinkError
	if !errors.As(err, &sinkErr) {
		t.Fatal("expected SinkError")
	}
	if !sinkErr.Retryable || sinkErr.Reason != ReasonTimeout {
		t.Fatalf("sinkErr = %+v", sinkErr)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expected wrapped deadline error")
	}
}

func TestClassifySinkErrorRetryableConnectionText(t *testing.T) {
	err := ClassifySinkError(errors.New("dial tcp: connection refused"))

	var sinkErr *SinkError
	if !errors.As(err, &sinkErr) {
		t.Fatal("expected SinkError")
	}
	if !sinkErr.Retryable || sinkErr.Reason != ReasonConnection {
		t.Fatalf("sinkErr = %+v", sinkErr)
	}
}

func TestPermanentError(t *testing.T) {
	err := PermanentError(ReasonInvalidRecord, errors.New("missing primary key"))

	var sinkErr *SinkError
	if !errors.As(err, &sinkErr) {
		t.Fatal("expected SinkError")
	}
	if sinkErr.Retryable || sinkErr.Reason != ReasonInvalidRecord {
		t.Fatalf("sinkErr = %+v", sinkErr)
	}
}
