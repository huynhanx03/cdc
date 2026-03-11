package interfaces

import "github.com/foden/cdc/pkg/models"

// Source defines the interface for reading CDC events from a database
type Source interface {
	// Start starts reading events and sending them to the provided channel
	Start(pipeline chan<- *models.Event, ackCh <-chan uint64) error
	// Stop gracefully stops the source
	Stop() error
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
}
