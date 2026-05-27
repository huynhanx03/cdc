package ports

import (
	"context"

	"github.com/foden/cdc/internal/core/domain"
)

// Sink defines a connection to a destination that receives replicated data.
// Implementations are responsible for:
//   - Accepting batches of CDC events and writing them to the target system
//   - Handling idempotency (deduplication via event MsgID)
//   - Managing destination connections and returning write failures to the flow
//
// Sinks do NOT own flow-level concerns (batching strategy, partition assignment, retries).
// Those are managed by the FlowManager.
type Sink interface {
	// WriteBatch writes a batch of events to the destination.
	// Implementations should be idempotent — duplicate events must not cause errors.
	WriteBatch(ctx context.Context, events []*domain.Event) error

	// Close gracefully shuts down the sink, flushing pending writes.
	Close() error

	// InstanceID returns the unique identifier for this sink instance.
	InstanceID() string

	// Type returns the sink type identifier (e.g., "postgres", "elasticsearch").
	Type() string
}

// SinkConfig holds connection-level fields for a CDC sink.
// This struct is shared across all sink types (Postgres, Elasticsearch, ClickHouse).
// It lives in the interfaces package because multiple packages (sink, discovery,
// server, flow) need to reference it without creating circular imports.
type SinkConfig struct {
	InstanceID  string   `json:"instance_id"`
	Name        string   `json:"name"`
	Type        string   `json:"type"`                   // Use constant.SinkType* for valid values
	Host        string   `json:"host,omitempty"`         // Postgres, ClickHouse
	Port        int      `json:"port,omitempty"`         // Postgres, ClickHouse
	Username    string   `json:"username,omitempty"`     // All types
	Password    string   `json:"password,omitempty"`     // All types
	Database    string   `json:"database,omitempty"`     // Postgres, ClickHouse
	URL         []string `json:"url,omitempty"`          // Elasticsearch: cluster URLs
	APIKey      string   `json:"api_key,omitempty"`      // Elasticsearch: API key auth
	IndexPrefix string   `json:"index_prefix,omitempty"` // Elasticsearch: index naming prefix
}
