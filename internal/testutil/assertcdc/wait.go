package assertcdc

import (
	"testing"
	"time"
)

// Eventually polls check until it succeeds or timeout expires.
func Eventually(t testing.TB, timeout time.Duration, interval time.Duration, check func() error) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		if err := check(); err == nil {
			return
		} else {
			last = err
		}
		time.Sleep(interval)
	}

	t.Fatalf("condition not met within %s: %v", timeout, last)
}
