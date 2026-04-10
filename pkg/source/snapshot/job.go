package snapshot

import "time"

// PartitionStrategy defines how a table is divided into chunks.
type PartitionStrategy string

const (
	PartitionStrategyAuto         PartitionStrategy = "auto"
	PartitionStrategyIntegerRange PartitionStrategy = "integer_range"
	PartitionStrategyCTIDBlock    PartitionStrategy = "ctid_block"
	PartitionStrategyOffset       PartitionStrategy = "offset"
)

// ChunkStatus represents the processing state of a chunk.
type ChunkStatus string

const (
	ChunkStatusPending    ChunkStatus = "pending"
	ChunkStatusInProgress ChunkStatus = "in_progress"
	ChunkStatusCompleted  ChunkStatus = "completed"
)

// Job tracks the overall snapshot progress for a slot.
type Job struct {
	SlotName        string
	SnapshotID      string
	SnapshotLSN     string
	StartedAt       time.Time
	Completed       bool
	TotalChunks     int
	CompletedChunks int
}

// Chunk represents a partition of a table for parallel processing.
type Chunk struct {
	ID                int64
	SlotName          string
	TableSchema       string
	TableName         string
	TableColumns      []string
	ChunkIndex        int
	ChunkStart        int64
	ChunkSize         int64
	RangeStart        *int64
	RangeEnd          *int64
	BlockStart        *int64
	BlockEnd          *int64
	IsLastChunk       bool
	PartitionStrategy PartitionStrategy
	Status            ChunkStatus
	ClaimedBy         string
	ClaimedAt         *time.Time
	HeartbeatAt       *time.Time
	RowsProcessed     int64
}

func (c *Chunk) hasRangeBounds() bool {
	return c.RangeStart != nil && c.RangeEnd != nil
}
