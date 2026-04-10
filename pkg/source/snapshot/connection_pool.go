package snapshot

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ConnectionPool manages a pool of database connections for chunk processing.
type ConnectionPool struct {
	pool   chan *pgxpool.Conn
	dbPool *pgxpool.Pool
	mu     sync.RWMutex
	closed bool
}

// NewConnectionPool creates a pool with the specified size.
func NewConnectionPool(ctx context.Context, dsn string, size int) (*ConnectionPool, error) {
	if size <= 0 {
		size = 5
	}

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	poolCfg.MaxConns = int32(size + 2)

	dbPool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := dbPool.Ping(ctx); err != nil {
		dbPool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}

	p := &ConnectionPool{
		pool:   make(chan *pgxpool.Conn, size),
		dbPool: dbPool,
	}

	slog.Info("created", "size", size)
	return p, nil
}

// Get retrieves a connection from the pool.
func (p *ConnectionPool) Get(ctx context.Context) (*pgxpool.Conn, error) {
	select {
	case conn := <-p.pool:
		return conn, nil
	default:
		return p.dbPool.Acquire(ctx)
	}
}

// Put returns a connection to the pool.
func (p *ConnectionPool) Put(conn *pgxpool.Conn) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		conn.Release()
		return
	}

	select {
	case p.pool <- conn:
	default:
		conn.Release()
	}
}

// DBPool returns the underlying pgxpool.Pool for direct queries.
func (p *ConnectionPool) DBPool() *pgxpool.Pool {
	return p.dbPool
}

// Close closes all connections.
func (p *ConnectionPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		return
	}
	p.closed = true

	close(p.pool)
	for conn := range p.pool {
		conn.Release()
	}
	p.dbPool.Close()
	slog.Info("closed")
}
