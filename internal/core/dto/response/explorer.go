package response

import (
	"time"

	"github.com/foden/cdc/internal/core/ports"
)

type PaginationResponse struct {
	TotalRows uint64
	Page      int32
	Limit     int32
	HasNext   bool
	HasPrev   bool
}

type TopicSummary struct {
	Name            string
	MessageCount    uint64
	PartitionCount  int32
	ConsumerCount   uint64
	DLQCount        uint64
	PendingCount    uint64
	AckPendingCount uint64
	FirstSequence   uint64
	LatestSequence  uint64
	LatestEventAt   time.Time
	Health          ExplorerHealthStatus
	Partial         bool
}

type PartitionSummary struct {
	ID              string
	MessageCount    uint64
	Topic           string
	PendingCount    uint64
	AckPendingCount uint64
	FirstSequence   uint64
	LatestSequence  uint64
	LatestEventAt   time.Time
	Health          ExplorerHealthStatus
	Partial         bool
}

type ExplorerHealthStatus string

const (
	ExplorerHealthHealthy ExplorerHealthStatus = "healthy"
	ExplorerHealthIdle    ExplorerHealthStatus = "idle"
	ExplorerHealthLagging ExplorerHealthStatus = "lagging"
	ExplorerHealthStale   ExplorerHealthStatus = "stale"
	ExplorerHealthDLQ     ExplorerHealthStatus = "dlq"
)

type ScanMetadata struct {
	Partial      bool
	ScanLimitHit bool
	ScannedCount uint64
	MatchedCount uint64
	MaxScan      uint64
}

type CDCSubjectParts struct {
	Topic     string
	SourceID  string
	Schema    string
	Table     string
	Partition string
}

type ProjectedMessageItem struct {
	*ports.NATSMessageItem
	Topic           string
	SourceID        string
	Schema          string
	Table           string
	Partition       string
	Op              string
	Key             string
	PayloadSize     uint64
	HeaderCount     uint32
	NATSMsgID       string
	ReprocessedFrom string
	Markers         []string
	IsDLQ           bool
	IsReprocessed   bool
	ChangedFields   []string
}

type ListMessagesResponse struct {
	Data       []ProjectedMessageItem
	TotalCount uint64
	Pagination PaginationResponse
	Scan       ScanMetadata
}

type ListTopicsResponse struct {
	Data       []TopicSummary
	Pagination PaginationResponse
}

type ListPartitionsResponse struct {
	Data       []PartitionSummary
	Pagination PaginationResponse
}

type ListConsumersResponse struct {
	Data       []ports.NATSConsumerSummary
	Pagination PaginationResponse
}

type ExplorerOverviewResponse struct {
	TopicCount             uint64
	PartitionCount         uint64
	ConsumerCount          uint64
	PendingCount           uint64
	AckPendingCount        uint64
	DLQDepth               uint64
	TopicsNeedingAttention []TopicSummary
	RecentDLQ              []DLQMessageSummary
}

type TopicDetailResponse struct {
	Summary    TopicSummary
	Partitions []PartitionSummary
	Scan       ScanMetadata
}

type PartitionDetailResponse struct {
	Summary        PartitionSummary
	RecentMessages []ProjectedMessageItem
	Checkpoints    []CheckpointContext
	Scan           ScanMetadata
}

type MessageDetailResponse struct {
	Item          ProjectedMessageItem
	Before        []byte
	After         []byte
	ChangedFields []string
	Checkpoint    CheckpointContext
}

type CheckpointContext struct {
	ConsumerName       string
	DeliveredStreamSeq uint64
	AckFloorStreamSeq  uint64
	NumPending         uint64
	NumAckPending      uint64
	LagMessages        uint64
	LastDeliveredAt    time.Time
	LastAckAt          time.Time
}

type ConsumerDetailResponse struct {
	Summary        ports.NATSConsumerSummary
	Topics         []TopicSummary
	Partitions     []PartitionSummary
	RecentMessages []ProjectedMessageItem
	Scan           ScanMetadata
}

type DLQMessageSummary struct {
	DLQID           string
	OriginalSubject string
	Reason          string
	ErrorClass      string
	Timestamp       time.Time
}
