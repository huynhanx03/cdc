//go:build integration

package integration

import "testing"

func TestSourceReconnectCleansOldConnection(t *testing.T) {
	t.Skip("pending product harness: requires controlled source connection interruption and status/error observation")
}
