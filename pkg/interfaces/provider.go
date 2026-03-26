package interfaces

import (
	"context"

	"github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/pkg/models"
)

// Source defines the interface for reading CDC events from a database
type Source interface {
	// Start starts reading events and sending them to the provided channel.
	// initialOffset can be used to resume from a specific position (e.g. LSN or binlog pos).
	Start(pipeline chan<- *models.Event, ackCh <-chan uint64, initialOffset string) error
	// Stop gracefully stops the source
	Stop() error
	// InstanceID returns a unique identifier for this source instance
	InstanceID() string
}

// Sink defines the interface for writing CDC events to a target destination
type Sink interface {
	// Write writes an event to the sink.
	// For bulk operations, the sink itself should implement batching/buffering logic internally.
	Write(event *models.Event) error
	// Flush forces a sync of the current buffer
	Flush() error
	// Close gracefully flushes remaining events and stops the sink
	Close() error
	// Type returns the sink type (e.g. "elasticsearch", "stdout")
	Type() string
	// InstanceID returns a unique identifier for this sink instance
	InstanceID() string
	// Topic returns the NATS topic pattern this sink subscribes to
	Topic() string
}
// PipelineEngine defines the interface for managing the CDC pipeline at runtime
type PipelineEngine interface {
	AddSource(ctx context.Context, src Source) error
	RemoveSource(instanceID string) error
	AddSink(sink Sink)
	RemoveSink(instanceID string) error
	GetStats() (map[string]*models.ComponentStats, map[string]*models.ComponentStats)
	ListMessages(ctx context.Context, status cdcpb.MessageStatus, limit int, page int, topic string, partition string) ([]*models.Message, uint64, error)
	GetConsumerInfo(ctx context.Context, consumerName string) (uint64, uint64, error)
	ListTopics(ctx context.Context, limit int, page int) ([]string, uint64, error)
	ListPartitions(ctx context.Context, topic string, limit int, page int) ([]string, uint64, error)
}
