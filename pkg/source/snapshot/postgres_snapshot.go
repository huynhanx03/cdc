package snapshot

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Config holds snapshot configuration.
type Config struct {
	Enabled           bool
	Mode              string // "initial", "never", "snapshot_only"
	ChunkSize         int64
	ClaimTimeout      time.Duration
	HeartbeatInterval time.Duration
	InstanceID        string
}

// PGSnapshotter orchestrates parallel snapshot processing for PostgreSQL.
// Implements the Snapshotter interface.
type PGSnapshotter struct {
	connPool       *ConnectionPool
	cfg            Config
	orderByCache   map[string]string
	orderByMu      sync.RWMutex
	keepaliveMu    sync.Mutex
	exportClosed   bool
	cachedSnapshot string
}

// NewPGSnapshotter creates a new PostgreSQL snapshotter.
func NewPGSnapshotter(ctx context.Context, cfg Config, dsn string) (*PGSnapshotter, error) {
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 8000
	}
	if cfg.ClaimTimeout <= 0 {
		cfg.ClaimTimeout = 30 * time.Second
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 5 * time.Second
	}

	connPool, err := NewConnectionPool(ctx, dsn, 5)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}

	return &PGSnapshotter{
		connPool:     connPool,
		cfg:          cfg,
		orderByCache: make(map[string]string),
	}, nil
}

// Prepare sets up the snapshot: elects coordinator, creates metadata and chunks.
// Must be called BEFORE creating the replication slot.
func (s *PGSnapshotter) Prepare(ctx context.Context, slotName string, tables []TableInfo) error {
	instanceID := s.getInstanceID()
	slog.Info("preparing", "instanceID", instanceID, "tables", len(tables))

	// Try to become coordinator
	lockID := hashString(slotName)
	acquired, err := s.tryAcquireLock(ctx, lockID)
	if err != nil {
		return fmt.Errorf("acquire coordinator lock: %w", err)
	}

	if acquired {
		slog.Info("elected as coordinator", "instanceID", instanceID)
		if err := s.initAsCoordinator(ctx, slotName, tables); err != nil {
			return fmt.Errorf("coordinator init: %w", err)
		}
	} else {
		slog.Info("joining as worker", "instanceID", instanceID)
		if err := s.waitForCoordinator(ctx, slotName); err != nil {
			return fmt.Errorf("wait for coordinator: %w", err)
		}
	}

	return nil
}

// Execute processes snapshot chunks as a worker.
func (s *PGSnapshotter) Execute(ctx context.Context, handler Handler, slotName string) error {
	instanceID := s.getInstanceID()
	start := time.Now()
	slog.Info("executing", "instanceID", instanceID)

	job, err := s.loadJob(ctx, slotName)
	if err != nil || job == nil {
		return fmt.Errorf("job not found - Prepare() must be called first")
	}

	// Claim and process chunks
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	chunkCh := make(chan int64, 1)
	go s.heartbeatWorker(heartbeatCtx, chunkCh)
	defer func() {
		cancelHeartbeat()
		close(chunkCh)
	}()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		chunk, err := s.claimNextChunk(ctx, slotName, instanceID)
		if err != nil {
			return fmt.Errorf("claim chunk: %w", err)
		}
		if chunk == nil {
			break // No more chunks
		}

		slog.Debug("processing chunk",
			"table", fmt.Sprintf("%s.%s", chunk.TableSchema, chunk.TableName),
			"chunkIndex", chunk.ChunkIndex)

		// Notify heartbeat
		select {
		case chunkCh <- chunk.ID:
		default:
		}

		rows, err := s.processChunk(ctx, chunk, handler)
		if err != nil {
			slog.Error("chunk failed", "chunkID", chunk.ID, "error", err)
			continue
		}

		s.markChunkCompleted(ctx, slotName, chunk.ID, rows)
	}

	// Check if all chunks completed
	if allDone, _ := s.checkJobCompleted(ctx, slotName); allDone {
		s.markJobCompleted(ctx, slotName)
		slog.Info("completed", "duration", time.Since(start))
	}

	return nil
}

// Close releases all resources.
func (s *PGSnapshotter) Close() {
	if s.connPool != nil {
		s.connPool.Close()
	}
}

func (s *PGSnapshotter) getInstanceID() string {
	if s.cfg.InstanceID != "" {
		return s.cfg.InstanceID
	}
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}

// loadJob loads the snapshot job metadata.
func (s *PGSnapshotter) loadJob(ctx context.Context, slotName string) (*Job, error) {
	row := s.connPool.DBPool().QueryRow(ctx,
		"SELECT snapshot_id, snapshot_lsn, started_at, completed, total_chunks, completed_chunks FROM cdc_snapshot_job WHERE slot_name = $1",
		slotName)

	var job Job
	job.SlotName = slotName
	err := row.Scan(&job.SnapshotID, &job.SnapshotLSN, &job.StartedAt, &job.Completed, &job.TotalChunks, &job.CompletedChunks)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

// waitForCoordinator polls until the coordinator has initialized.
func (s *PGSnapshotter) waitForCoordinator(ctx context.Context, slotName string) error {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		job, err := s.loadJob(ctx, slotName)
		if err == nil && job != nil && job.SnapshotID != "" && job.SnapshotID != "PENDING" {
			// Check chunks exist
			var hasChunks bool
			s.connPool.DBPool().QueryRow(ctx,
				"SELECT COUNT(*) > 0 FROM cdc_snapshot_chunks WHERE slot_name = $1", slotName).Scan(&hasChunks)
			if hasChunks {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
	return fmt.Errorf("timeout waiting for coordinator")
}

// tryAcquireLock attempts a PostgreSQL advisory lock.
func (s *PGSnapshotter) tryAcquireLock(ctx context.Context, lockID int64) (bool, error) {
	var acquired bool
	err := s.connPool.DBPool().QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&acquired)
	return acquired, err
}

// checkJobCompleted checks if all chunks are done.
func (s *PGSnapshotter) checkJobCompleted(ctx context.Context, slotName string) (bool, error) {
	var total, completed int
	err := s.connPool.DBPool().QueryRow(ctx,
		"SELECT total_chunks, completed_chunks FROM cdc_snapshot_job WHERE slot_name = $1", slotName).Scan(&total, &completed)
	if err != nil {
		return false, err
	}
	return total > 0 && total == completed, nil
}

// markJobCompleted marks the job as done.
func (s *PGSnapshotter) markJobCompleted(ctx context.Context, slotName string) {
	_, err := s.connPool.DBPool().Exec(ctx,
		"UPDATE cdc_snapshot_job SET completed = true WHERE slot_name = $1", slotName)
	if err != nil {
		slog.Warn("failed to mark job completed", "error", err)
	}
}

// claimNextChunk atomically claims a pending chunk using SELECT FOR UPDATE SKIP LOCKED.
func (s *PGSnapshotter) claimNextChunk(ctx context.Context, slotName, instanceID string) (*Chunk, error) {
	timeoutThreshold := time.Now().UTC().Add(-s.cfg.ClaimTimeout)

	var chunk Chunk
	err := s.connPool.DBPool().QueryRow(ctx, `
		WITH available_chunk AS (
			SELECT id FROM cdc_snapshot_chunks
			WHERE slot_name = $1 AND (
				status = 'pending'
				OR (status = 'in_progress' AND heartbeat_at < $2)
			)
			ORDER BY chunk_index LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE cdc_snapshot_chunks c
		SET status = 'in_progress', claimed_by = $3, claimed_at = NOW(), heartbeat_at = NOW()
		FROM available_chunk
		WHERE c.id = available_chunk.id
		RETURNING c.id, c.table_schema, c.table_name, c.chunk_index,
		          c.chunk_start, c.chunk_size, c.range_start, c.range_end,
		          c.block_start, c.block_end, c.partition_strategy
	`, slotName, timeoutThreshold, instanceID).Scan(
		&chunk.ID, &chunk.TableSchema, &chunk.TableName, &chunk.ChunkIndex,
		&chunk.ChunkStart, &chunk.ChunkSize, &chunk.RangeStart, &chunk.RangeEnd,
		&chunk.BlockStart, &chunk.BlockEnd, &chunk.PartitionStrategy,
	)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, nil
		}
		return nil, err
	}
	chunk.SlotName = slotName
	chunk.Status = ChunkStatusInProgress
	return &chunk, nil
}

// processChunk queries data within chunk bounds and calls the handler.
func (s *PGSnapshotter) processChunk(ctx context.Context, chunk *Chunk, handler Handler) (int64, error) {
	query := s.buildChunkQuery(chunk)
	rows, err := s.connPool.DBPool().Query(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("query chunk: %w", err)
	}
	defer rows.Close()

	var count int64
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return count, err
		}

		// Build row map from field descriptions
		fields := rows.FieldDescriptions()
		rowMap := make(map[string]any, len(fields))
		for i, fd := range fields {
			if i < len(values) {
				rowMap[string(fd.Name)] = values[i]
			}
		}

		if handler != nil {
			_ = handler(chunk.TableName, chunk.TableSchema, rowMap)
		}
		count++
	}
	return count, nil
}

// markChunkCompleted marks a chunk as done and increments the job counter.
func (s *PGSnapshotter) markChunkCompleted(ctx context.Context, slotName string, chunkID, rowsProcessed int64) {
	_, err := s.connPool.DBPool().Exec(ctx,
		"UPDATE cdc_snapshot_chunks SET status = 'completed', completed_at = NOW(), rows_processed = $1 WHERE id = $2",
		rowsProcessed, chunkID)
	if err != nil {
		slog.Warn("failed to mark chunk completed", "error", err)
		return
	}

	_, err = s.connPool.DBPool().Exec(ctx,
		"UPDATE cdc_snapshot_job SET completed_chunks = completed_chunks + 1 WHERE slot_name = $1",
		slotName)
	if err != nil {
		slog.Warn("failed to increment completed chunks", "error", err)
	}
}

// heartbeatWorker updates heartbeat for the current chunk.
func (s *PGSnapshotter) heartbeatWorker(ctx context.Context, chunkCh <-chan int64) {
	var activeChunkID int64
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case chunkID := <-chunkCh:
			activeChunkID = chunkID
		case <-ticker.C:
			if activeChunkID > 0 {
				_, err := s.connPool.DBPool().Exec(ctx,
					"UPDATE cdc_snapshot_chunks SET heartbeat_at = NOW() WHERE id = $1", activeChunkID)
				if err != nil {
					slog.Warn("[snapshot-heartbeat] failed", "error", err)
				}
			}
		}
	}
}

// buildChunkQuery constructs the SELECT query for a chunk.
func (s *PGSnapshotter) buildChunkQuery(chunk *Chunk) string {
	switch chunk.PartitionStrategy {
	case PartitionStrategyIntegerRange:
		if chunk.hasRangeBounds() {
			return fmt.Sprintf("SELECT * FROM %s.%s WHERE ctid >= '(0,0)'::tid ORDER BY ctid",
				chunk.TableSchema, chunk.TableName)
		}
	case PartitionStrategyCTIDBlock:
		if chunk.BlockStart != nil {
			if chunk.BlockEnd == nil || chunk.IsLastChunk {
				return fmt.Sprintf("SELECT * FROM %s.%s WHERE ctid >= '(%d,0)'::tid",
					chunk.TableSchema, chunk.TableName, *chunk.BlockStart)
			}
			return fmt.Sprintf("SELECT * FROM %s.%s WHERE ctid >= '(%d,0)'::tid AND ctid < '(%d,0)'::tid",
				chunk.TableSchema, chunk.TableName, *chunk.BlockStart, *chunk.BlockEnd)
		}
	}
	// Default: offset
	return fmt.Sprintf("SELECT * FROM %s.%s ORDER BY ctid LIMIT %d OFFSET %d",
		chunk.TableSchema, chunk.TableName, chunk.ChunkSize, chunk.ChunkStart)
}

// initAsCoordinator creates metadata tables, exports snapshot, and creates chunks.
func (s *PGSnapshotter) initAsCoordinator(ctx context.Context, slotName string, tables []TableInfo) error {
	if err := s.initTables(ctx); err != nil {
		return err
	}

	// Export snapshot
	var snapshotID string
	err := s.connPool.DBPool().QueryRow(ctx,
		"SELECT pg_export_snapshot()").Scan(&snapshotID)
	if err != nil {
		return fmt.Errorf("export snapshot: %w", err)
	}
	s.cachedSnapshot = snapshotID
	slog.Info("[snapshot-coordinator] snapshot exported", "snapshotID", snapshotID)

	// Get current LSN
	var lsn string
	s.connPool.DBPool().QueryRow(ctx, "SELECT pg_current_wal_lsn()").Scan(&lsn)

	// Create chunks for each table
	totalChunks := 0
	for _, table := range tables {
		chunks := s.createChunks(ctx, slotName, table)
		if err := s.saveChunksBatch(ctx, chunks); err != nil {
			return fmt.Errorf("save chunks for %s.%s: %w", table.Schema, table.Name, err)
		}
		totalChunks += len(chunks)
		slog.Info("[snapshot-coordinator] chunks created",
			"table", fmt.Sprintf("%s.%s", table.Schema, table.Name),
			"chunks", len(chunks))
	}

	// Save job
	_, err = s.connPool.DBPool().Exec(ctx,
		"INSERT INTO cdc_snapshot_job (slot_name, snapshot_id, snapshot_lsn, started_at, completed, total_chunks, completed_chunks) VALUES ($1, $2, $3, NOW(), false, $4, 0)",
		slotName, snapshotID, lsn, totalChunks)
	if err != nil {
		return fmt.Errorf("save job: %w", err)
	}

	slog.Info("[snapshot-coordinator] metadata committed", "totalChunks", totalChunks, "lsn", lsn)
	return nil
}

// initTables creates metadata tables if they don't exist.
func (s *PGSnapshotter) initTables(ctx context.Context) error {
	_, err := s.connPool.DBPool().Exec(ctx, `
		CREATE TABLE IF NOT EXISTS cdc_snapshot_job (
			slot_name TEXT PRIMARY KEY,
			snapshot_id TEXT NOT NULL,
			snapshot_lsn TEXT NOT NULL,
			started_at TIMESTAMP NOT NULL,
			completed BOOLEAN DEFAULT FALSE,
			total_chunks INT DEFAULT 0,
			completed_chunks INT DEFAULT 0
		)`)
	if err != nil {
		return fmt.Errorf("create job table: %w", err)
	}

	_, err = s.connPool.DBPool().Exec(ctx, `
		CREATE TABLE IF NOT EXISTS cdc_snapshot_chunks (
			id SERIAL PRIMARY KEY,
			slot_name TEXT NOT NULL,
			table_schema TEXT NOT NULL,
			table_name TEXT NOT NULL,
			chunk_index INT NOT NULL,
			chunk_start BIGINT NOT NULL,
			chunk_size BIGINT NOT NULL,
			range_start BIGINT,
			range_end BIGINT,
			block_start BIGINT,
			block_end BIGINT,
			is_last_chunk BOOLEAN DEFAULT FALSE,
			partition_strategy TEXT DEFAULT 'offset',
			status TEXT DEFAULT 'pending',
			claimed_by TEXT,
			claimed_at TIMESTAMP,
			heartbeat_at TIMESTAMP,
			completed_at TIMESTAMP,
			rows_processed BIGINT DEFAULT 0,
			UNIQUE(slot_name, table_schema, table_name, chunk_index)
		)`)
	if err != nil {
		return fmt.Errorf("create chunks table: %w", err)
	}

	return nil
}

// createChunks divides a table into chunks.
func (s *PGSnapshotter) createChunks(ctx context.Context, slotName string, table TableInfo) []*Chunk {
	// Get row count
	var count int64
	s.connPool.DBPool().QueryRow(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", table.Schema, table.Name)).Scan(&count)

	if count == 0 {
		return []*Chunk{{
			SlotName:          slotName,
			TableSchema:       table.Schema,
			TableName:         table.Name,
			TableColumns:      table.Columns,
			ChunkIndex:        0,
			ChunkSize:         s.cfg.ChunkSize,
			PartitionStrategy: PartitionStrategyOffset,
			Status:            ChunkStatusPending,
		}}
	}

	numChunks := (count + s.cfg.ChunkSize - 1) / s.cfg.ChunkSize
	chunks := make([]*Chunk, 0, numChunks)

	for i := int64(0); i < numChunks; i++ {
		chunks = append(chunks, &Chunk{
			SlotName:          slotName,
			TableSchema:       table.Schema,
			TableName:         table.Name,
			TableColumns:      table.Columns,
			ChunkIndex:        int(i),
			ChunkStart:        i * s.cfg.ChunkSize,
			ChunkSize:         s.cfg.ChunkSize,
			PartitionStrategy: PartitionStrategyOffset,
			Status:            ChunkStatusPending,
		})
	}
	return chunks
}

// saveChunksBatch inserts chunks in batches for performance.
func (s *PGSnapshotter) saveChunksBatch(ctx context.Context, chunks []*Chunk) error {
	if len(chunks) == 0 {
		return nil
	}
	for _, chunk := range chunks {
		_, err := s.connPool.DBPool().Exec(ctx,
			`INSERT INTO cdc_snapshot_chunks (slot_name, table_schema, table_name, chunk_index, chunk_start, chunk_size, partition_strategy, status)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending')`,
			chunk.SlotName, chunk.TableSchema, chunk.TableName, chunk.ChunkIndex,
			chunk.ChunkStart, chunk.ChunkSize, string(chunk.PartitionStrategy))
		if err != nil {
			return err
		}
	}
	return nil
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
