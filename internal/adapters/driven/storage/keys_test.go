package storage

import (
	"testing"

	"github.com/foden/cdc/internal/core/domain"
)

func TestCheckpointKeyIncludesFlowSourceTableAndPartition(t *testing.T) {
	key, err := CheckpointKey(&domain.Checkpoint{
		FlowID:    "flow-1",
		SourceID:  "src-1",
		Schema:    "public",
		Table:     "orders",
		Partition: 3,
	})
	if err != nil {
		t.Fatalf("CheckpointKey failed: %v", err)
	}

	want := "checkpoints.flow-1.src-1.public.orders.3"
	if key != want {
		t.Fatalf("CheckpointKey = %q, want %q", key, want)
	}
}
