package retry

import (
	"errors"
	"net"

	cdcerrors "github.com/foden/cdc/pkg/errors"
)

// IsRetryable determines if an error should be retried.
// Non-retryable errors (wrapped with cdcerrors.Permanent) return false.
// Network/timeout errors always return true.
// Unknown errors default to retryable.
func IsRetryable(err error) bool {
	if cdcerrors.IsNonRetryable(err) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return true
}
