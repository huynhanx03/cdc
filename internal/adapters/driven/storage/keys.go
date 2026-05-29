package storage

import (
	"fmt"
	"strconv"

	"github.com/foden/cdc/internal/core/domain"
)

const (
	// BucketName is the NATS KV bucket used for all CDC state persistence.
	BucketName = "CDC_STATE"

	// PrefixSources is the key prefix for source configurations.
	PrefixSources = "sources."

	// PrefixSinks is the key prefix for sink configurations.
	PrefixSinks = "sinks."

	// PrefixFlows is the key prefix for flow configurations.
	PrefixFlows = "flows."

	// PrefixSourceOffsets is the key prefix for source resume offsets.
	PrefixSourceOffsets = "source_offsets."

	// PrefixCheckpoints is the key prefix for per-flow table partition checkpoints.
	PrefixCheckpoints = "checkpoints."
)

func CheckpointKey(checkpoint *domain.Checkpoint) (string, error) {
	if checkpoint == nil {
		return "", fmt.Errorf("checkpoint is nil")
	}
	if checkpoint.FlowID == "" {
		return "", fmt.Errorf("checkpoint flow_id is required")
	}
	if checkpoint.SourceID == "" {
		return "", fmt.Errorf("checkpoint source_id is required")
	}
	if checkpoint.Schema == "" {
		return "", fmt.Errorf("checkpoint schema is required")
	}
	if checkpoint.Table == "" {
		return "", fmt.Errorf("checkpoint table is required")
	}
	if checkpoint.Partition < 0 {
		return "", fmt.Errorf("checkpoint partition must be non-negative")
	}
	return PrefixCheckpoints + checkpoint.FlowID + "." + checkpoint.SourceID + "." + checkpoint.Schema + "." + checkpoint.Table + "." + strconv.Itoa(checkpoint.Partition), nil
}

func LegacyCheckpointKey(flowID string) string {
	return "checkpoint." + flowID
}
