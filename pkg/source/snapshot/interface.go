package snapshot

import (
	"context"

	"github.com/foden/cdc/pkg/constant"
)

// Snapshotter is a database-agnostic interface for initial data snapshots.
type Snapshotter interface {
	// Prepare sets up metadata and creates chunks.
	Prepare(ctx context.Context, slotName string, tables []TableInfo) error
	// Execute processes chunks and calls handler for each row.
	Execute(ctx context.Context, handler Handler, slotName string) error
	// Close releases all resources.
	Close()
}

// TableInfo describes a table to snapshot.
type TableInfo struct {
	Schema  string
	Name    string
	Columns []string
}

// Handler processes a single snapshot row.
type Handler func(table, schema string, row map[string]any) error

// NoopSnapshotter is used when snapshot is not needed or not supported.
type NoopSnapshotter struct{}

func (n *NoopSnapshotter) Prepare(_ context.Context, _ string, _ []TableInfo) error {
	return nil
}
func (n *NoopSnapshotter) Execute(_ context.Context, _ Handler, _ string) error {
	return nil
}
func (n *NoopSnapshotter) Close() {}

// NewSnapshotter creates the appropriate snapshotter based on source type.
func NewSnapshotter(ctx context.Context, sourceType constant.SourceType, dsn string, cfg Config) (Snapshotter, error) {
	switch sourceType {
	case constant.SourceTypePostgres:
		return NewPGSnapshotter(ctx, cfg, dsn)
	default:
		return &NoopSnapshotter{}, nil
	}
}
