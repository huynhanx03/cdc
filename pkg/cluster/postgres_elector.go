package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// postgresElector uses PostgreSQL advisory locks for leader election.
// The lock auto-releases when the connection drops → natural failover.
type postgresElector struct {
	dsn      string
	lockID   int64
	isLeader atomic.Bool

	mu   sync.Mutex
	conn *pgx.Conn
}

func newPostgresElector(ctx context.Context, dsn, lockKey string, checkInterval time.Duration) (*postgresElector, error) {
	return &postgresElector{
		dsn:    dsn,
		lockID: hashString(lockKey),
	}, nil
}

func (pe *postgresElector) getConn(ctx context.Context) (*pgx.Conn, error) {
	if pe.conn != nil && pe.conn.Ping(ctx) == nil {
		return pe.conn, nil
	}
	if pe.conn != nil {
		pe.conn.Close(ctx)
	}
	conn, err := pgx.Connect(ctx, pe.dsn)
	if err == nil {
		pe.conn = conn
	} else {
		pe.conn = nil
	}
	return pe.conn, err
}

func (pe *postgresElector) Campaign(ctx context.Context) (bool, error) {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	conn, err := pe.getConn(ctx)
	if err != nil {
		pe.isLeader.Store(false)
		return false, fmt.Errorf("failed to get connection: %w", err)
	}

	var acquired bool
	err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", pe.lockID).Scan(&acquired)
	if err != nil {
		pe.conn.Close(ctx)
		pe.conn = nil
		pe.isLeader.Store(false)
		return false, fmt.Errorf("advisory lock query: %w", err)
	}
	pe.isLeader.Store(acquired)
	return acquired, nil
}

func (pe *postgresElector) Resign(ctx context.Context) {
	if !pe.isLeader.Swap(false) {
		return
	}
	
	pe.mu.Lock()
	defer pe.mu.Unlock()

	if pe.conn != nil && pe.conn.Ping(ctx) == nil {
		_, err := pe.conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", pe.lockID)
		if err != nil {
			slog.Warn("failed to release advisory lock", "error", err)
		}
	}
	slog.Info("leadership resigned")
}

func (pe *postgresElector) Close(ctx context.Context) {
	pe.Resign(ctx)

	pe.mu.Lock()
	defer pe.mu.Unlock()
	if pe.conn != nil {
		pe.conn.Close(ctx)
		pe.conn = nil
	}
}

func (pe *postgresElector) IsLeader() bool {
	return pe.isLeader.Load()
}

// hashString produces a positive int64 hash for advisory lock IDs.
func hashString(s string) int64 {
	var hash int64
	for i := 0; i < len(s); i++ {
		hash = hash*31 + int64(s[i])
	}
	if hash < 0 {
		hash = -hash
	}
	return hash
}
