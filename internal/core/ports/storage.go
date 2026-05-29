package ports

import (
	"context"

	"github.com/foden/cdc/internal/core/domain"
)

// Store abstracts the persistence layer (backed by NATS KV).
type Store interface {
	// Source CRUD
	PutSource(ctx context.Context, cfg *SourceConfig) error
	GetSource(ctx context.Context, instanceID string) (*SourceConfig, error)
	DeleteSource(ctx context.Context, instanceID string) error
	ListSources(ctx context.Context) ([]*SourceConfig, error)
	// Sink CRUD
	PutSink(ctx context.Context, cfg *SinkConfig) error
	GetSink(ctx context.Context, instanceID string) (*SinkConfig, error)
	DeleteSink(ctx context.Context, instanceID string) error
	ListSinks(ctx context.Context) ([]*SinkConfig, error)
	// Flow CRUD
	PutFlow(ctx context.Context, cfg *FlowConfig) error
	GetFlow(ctx context.Context, flowID string) (*FlowConfig, error)
	DeleteFlow(ctx context.Context, flowID string) error
	ListFlows(ctx context.Context) ([]*FlowConfig, error)
	// Checkpoints and source resume offsets
	SaveCheckpoint(ctx context.Context, checkpoint *domain.Checkpoint) error
	GetCheckpoint(ctx context.Context, flowID string) (*domain.Checkpoint, error)
	SaveSourceOffset(ctx context.Context, sourceID string, offset string) error
	GetSourceOffset(ctx context.Context, sourceID string) (string, error)
}
