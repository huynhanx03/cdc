package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/constant"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/registry"
)

func init() {
	registry.RegisterSink(constant.SinkTypePostgres.String(), func(cfg *config.SinkConfig) (interfaces.Sink, error) {
		return New(cfg)
	})
}

// PostgresSink writes CDC events to another PostgreSQL database.
type PostgresSink struct {
	pool *pgxpool.Pool
	cfg  *config.SinkConfig

	mu   sync.Mutex
	done chan struct{}

	// tableCache tracks tables we've already ensured exist in this session
	tableCache sync.Map
}

// New creates a new PostgresSink instance.
func New(cfg *config.SinkConfig) (*PostgresSink, error) {
	ctx := context.Background()
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	s := &PostgresSink{
		pool: pool,
		cfg:  cfg,
		done: make(chan struct{}),
	}

	return s, nil
}

// Write applies a single CDC event to the target database.
func (s *PostgresSink) Write(event *models.Event) error {
	ctx := context.Background()

	docMap := event.After
	if event.Op == constant.DeleteAction.String() {
		docMap = event.Before
	}
	if len(docMap) == 0 {
		return nil
	}

	// Apply field mapping if configured
	mappedDoc, err := s.cfg.ApplyFieldMapping(docMap)
	if err != nil {
		return fmt.Errorf("field mapping failed: %w", err)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(mappedDoc, &data); err != nil {
		return fmt.Errorf("unmarshal mapped data failed: %w", err)
	}

	tableName := event.Table
	pk, ok := s.cfg.PrimaryKeys[tableName]
	if !ok {
		pk = "id" // default to "id"
	}

	pkValue, ok := data[pk]
	if !ok && event.Op != constant.CreateAction.String() {
		return fmt.Errorf("primary key %q not found in data for table %q", pk, tableName)
	}

	// Self-healing: Ensure table and primary key exist
	if err := s.ensureTable(ctx, tableName, data, pk); err != nil {
		return fmt.Errorf("failed to ensure table %s: %w", tableName, err)
	}

	switch event.Op {
	case constant.DeleteAction.String():
		query := fmt.Sprintf("DELETE FROM %s WHERE %s = $1", tableName, pk)
		_, err = s.pool.Exec(ctx, query, pkValue)
	case constant.CreateAction.String(), constant.UpdateAction.String(), constant.SnapshotAction.String():
		err = s.upsert(ctx, tableName, pk, data)
	default:
		slog.Warn("unknown operation type", "op", event.Op)
	}

	return err
}

func (s *PostgresSink) ensureTable(ctx context.Context, table string, data map[string]interface{}, pk string) error {
	if _, ok := s.tableCache.Load(table); ok {
		return nil
	}

	// Double-check with DB if not in cache
	var exists bool
	checkQuery := "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)"
	if err := s.pool.QueryRow(ctx, checkQuery, table).Scan(&exists); err != nil {
		return err
	}

	if exists {
		s.tableCache.Store(table, true)
		return nil
	}

	// Create table if it doesn't exist
	slog.Info("self-healing: creating table", "table", table, "pk", pk)
	cols := make([]string, 0, len(data))
	for k, v := range data {
		pgType := s.inferPGType(v)
		colDef := fmt.Sprintf("%s %s", k, pgType)
		if k == pk {
			colDef += " PRIMARY KEY"
		}
		cols = append(cols, colDef)
	}

	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", table, strings.Join(cols, ", "))
	if _, err := s.pool.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("create table failed: %w", err)
	}

	s.tableCache.Store(table, true)
	return nil
}

func (s *PostgresSink) inferPGType(v interface{}) string {
	switch v.(type) {
	case bool:
		return "BOOLEAN"
	case int, int32, int64:
		return "BIGINT"
	case float32, float64:
		return "DOUBLE PRECISION"
	case string:
		return "TEXT"
	case map[string]interface{}, []interface{}:
		return "JSONB"
	default:
		return "TEXT"
	}
}

func (s *PostgresSink) upsert(ctx context.Context, table string, pk string, data map[string]interface{}) error {
	cols := make([]string, 0, len(data))
	vals := make([]interface{}, 0, len(data))
	placeholders := make([]string, 0, len(data))
	updates := make([]string, 0, len(data)-1)

	i := 1
	for k, v := range data {
		cols = append(cols, k)
		vals = append(vals, v)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		if k != pk {
			updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", k, k))
		}
		i++
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
		table,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
		pk,
		strings.Join(updates, ", "),
	)

	_, err := s.pool.Exec(ctx, query, vals...)
	return err
}

func (s *PostgresSink) Flush() error {
	return nil // No buffering in this simple implementation
}

func (s *PostgresSink) Close() error {
	close(s.done)
	s.pool.Close()
	return nil
}

func (s *PostgresSink) InstanceID() string {
	return s.cfg.InstanceID
}

func (s *PostgresSink) Type() string {
	return constant.SinkTypePostgres.String()
}

func (s *PostgresSink) MaxRetries() int32 {
	return s.cfg.MaxRetries
}

func (s *PostgresSink) Topic() string {
	return s.cfg.Topic
}
