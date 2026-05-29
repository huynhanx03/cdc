package ports

import (
	"context"

	"github.com/foden/cdc/internal/core/domain"
	"github.com/nats-io/nats.go/jetstream"
)

// NATSClient abstracts NATS JetStream operations for testability.
type NATSClient interface {
	// Publishing
	PublishBatch(ctx context.Context, subjectFunc func(*domain.Event) string, events []*domain.Event) error
	// Consumer management
	CreateOrUpdateConsumer(ctx context.Context, name string, filterSubjects []string) (jetstream.Consumer, error)
	DeleteConsumer(ctx context.Context, name string) error
	// DLQ
	MoveToDLQ(ctx context.Context, msg jetstream.Msg, opts DLQMoveOptions) error
	ReprocessDLQ(ctx context.Context) (int, error)
	PreviewDLQ(ctx context.Context, ids []string, filter DLQFilter, maxCount uint32) ([]DLQPreviewItem, error)
	ReprocessDLQSelected(ctx context.Context, ids []string, filter DLQFilter, maxCount uint32) (DLQReprocessResult, error)
	ListMessages(ctx context.Context, status domain.MessageStatus, limit int, page int, topic string, partition string) ([]*NATSMessageItem, uint64, error)
	ListMessagesWithFilter(ctx context.Context, status domain.MessageStatus, limit int, page int, filter NATSMessageFilter) ([]*NATSMessageItem, uint64, error)
	ListDLQMessages(ctx context.Context, limit int, page int) ([]*NATSMessageItem, uint64, error)
	ListTopics(ctx context.Context, limit int, page int) ([]string, uint64, error)
	ListPartitions(ctx context.Context, topic string, limit int, page int) ([]string, uint64, error)
	ListConsumers(ctx context.Context, limit int, page int) ([]NATSConsumerSummary, uint64, error)
	// Stream management
	CreateStream(ctx context.Context, subjects []string) error
	CreateDLQStream(ctx context.Context) error
	// Connection
	Health(ctx context.Context) error
	Close()
}

type NATSMessageItem struct {
	Sequence  uint64
	Timestamp int64
	Subject   string
	Data      []byte
	Headers   map[string]string
}

type NATSMessageFilter struct {
	Topic         string
	Partition     string
	SubjectPrefix string

	MinSequence   uint64
	MaxSequence   uint64
	FromTimestamp int64
	ToTimestamp   int64

	HeaderKey    string
	HeaderValue  string
	TextContains string

	JSONPath   string
	JSONEquals string

	Op              string
	SourceID        string
	Schema          string
	Table           string
	ReprocessedOnly bool
	DLQRelatedOnly  bool
	Sort            string
}

type NATSConsumerSummary struct {
	Name               string
	FilterSubjects     []string
	NumPending         uint64
	NumAckPending      uint64
	DeliveredStreamSeq uint64
	AckFloorStreamSeq  uint64
}

type DLQMoveOptions struct {
	FlowID     string
	SourceID   string
	SinkID     string
	Schema     string
	Table      string
	Op         string
	MsgID      string
	Reason     string
	ErrorClass string
	RetryCount uint64
	Timestamp  int64
}

type DLQFilter struct {
	OriginalTopic     string
	OriginalPartition string
	SourceID          string
	Schema            string
	Table             string
	Op                string
	ReasonContains    string
	ErrorClass        string
	HeaderKey         string
	HeaderValue       string
	JSONPath          string
	JSONEquals        string
	TextContains      string
}

type DLQPreviewItem struct {
	DLQID            string
	OriginalSubject  string
	Reason           string
	ErrorClass       string
	DuplicateRisk    string
	BlockedReason    string
	ReplayTarget     string
	MessageSequence  uint64
	MessageTimestamp int64
}

type DLQReprocessResult struct {
	Count             int
	ReprocessedDLQIDs []string
	SkippedDLQIDs     []string
	FailedDLQIDs      []string
}
