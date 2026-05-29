//go:build integration

package integration

import "testing"

func TestNATSRetentionUsesMinFlowCheckpoint(t *testing.T) {
	t.Skip("pending product implementation: retention controller must compute min checkpoint across active flow/table/partition consumers before this can assert behavior")
}
