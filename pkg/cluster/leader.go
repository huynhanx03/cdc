package cluster

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/foden/cdc/pkg/constant"
)

// Elector is a database-agnostic interface for leader election.
type Elector interface {
	Campaign(ctx context.Context) (bool, error)
	Resign(ctx context.Context)
	Close(ctx context.Context)
}

// NewElector creates the appropriate elector based on the source type.
func NewElector(ctx context.Context, sourceType constant.SourceType, dsn, lockKey string, checkInterval time.Duration) (Elector, error) {
	if checkInterval <= 0 {
		checkInterval = 5 * time.Second
	}
	switch sourceType {
	case constant.SourceTypePostgres:
		return newPostgresElector(ctx, dsn, lockKey, checkInterval)
	default:
		slog.Warn("leader election not supported, running as single instance", "source_type", sourceType)
		return &noopElector{}, nil
	}
}

// RunElector continuously monitors leadership and calls callbacks.
// This works with any Elector implementation.
func RunElector(ctx context.Context, elector Elector, interval time.Duration, onLeader, onFollower func()) {
	if _, ok := elector.(*noopElector); ok {
		// No-op elector: always leader
		if onLeader != nil {
			onLeader()
		}
		return
	}

	type state int
	const (
		stateUnknown state = iota
		stateLeader
		stateFollower
	)
	currState := stateUnknown

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		acquired, err := elector.Campaign(ctx)
		if err != nil || !acquired {
			if err != nil {
				slog.Error("leader election check failed", "error", err)
			}
			if currState != stateFollower {
				currState = stateFollower
				if onFollower != nil {
					onFollower()
				}
			}
		} else {
			if currState != stateLeader {
				currState = stateLeader
				if onLeader != nil {
					onLeader()
				}
			}
		}

		select {
		case <-ctx.Done():
			elector.Resign(ctx)
			return
		case <-ticker.C:
		}
	}
}

// noopElector is used when leader election is not supported.
// Always returns true for Campaign (single instance mode).
type noopElector struct {
	isLeader atomic.Bool
}

func (n *noopElector) Campaign(_ context.Context) (bool, error) {
	n.isLeader.Store(true)
	return true, nil
}

func (n *noopElector) Resign(_ context.Context) {
	n.isLeader.Store(false)
}

func (n *noopElector) Close(_ context.Context) {}

func (n *noopElector) IsLeader() bool {
	return n.isLeader.Load()
}
