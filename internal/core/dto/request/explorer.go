package request

import "github.com/foden/cdc/internal/core/domain"

type ListMessagesRequest struct {
	Status          domain.MessageStatus
	SourceID        string
	Topic           string
	Partition       string
	Schema          string
	Table           string
	Op              string
	SequenceMin     uint64
	SequenceMax     uint64
	TimestampFrom   int64
	TimestampTo     int64
	HeaderKey       string
	HeaderValue     string
	JSONPath        string
	JSONEquals      string
	TextContains    string
	ReprocessedOnly bool
	DLQRelatedOnly  bool
	Sort            string
	Page            int
	Limit           int
}

type ListTopicsRequest struct {
	Page  int
	Limit int
}

type ListPartitionsRequest struct {
	Topic string
	Page  int
	Limit int
}

type ListConsumersRequest struct {
	Page  int
	Limit int
}

type TopicDetailRequest struct {
	Topic string
}

type PartitionDetailRequest struct {
	Topic     string
	Partition string
}

type MessageDetailRequest struct {
	Topic     string
	Partition string
	Sequence  uint64
}

type ConsumerDetailRequest struct {
	ConsumerName string
}
