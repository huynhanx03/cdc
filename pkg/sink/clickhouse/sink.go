package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/bytedance/sonic"

	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/constant"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/registry"
	"github.com/bytedance/sonic/ast"
)

func init() {
	registry.RegisterSink(constant.SinkTypeClickhouse.String(), func(cfg *config.SinkConfig) (interfaces.Sink, error) {
		return New(cfg)
	})
}

// ClickhouseSink writes CDC events to ClickHouse.
type ClickhouseSink struct {
	conn clickhouse.Conn
	cfg  *config.SinkConfig

	mu      sync.Mutex
	events  []*models.Event
	pending int

	flushTicker *time.Ticker
	done        chan struct{}
}

// New creates a ClickhouseSink and verifies connection.
func New(cfg *config.SinkConfig) (*ClickhouseSink, error) {
	addr := cfg.Host
	if cfg.Port > 0 {
		addr = fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	} else if len(cfg.URL) > 0 {
		addr = cfg.URL[0]
	}

	if addr == "" {
		addr = "127.0.0.1:9000"
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Debug: true,
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout: time.Second * 30,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to open clickhouse connection: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping clickhouse: %w", err)
	}

	s := &ClickhouseSink{
		conn:        conn,
		cfg:         cfg,
		done:        make(chan struct{}),
		flushTicker: time.NewTicker(time.Duration(cfg.FlushIntervalMs) * time.Millisecond),
	}

	go s.flushLoop()
	return s, nil
}

// Write buffers a single event.
func (s *ClickhouseSink) Write(event *models.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.events = append(s.events, event)
	s.pending++

	if s.pending >= int(s.cfg.BatchSize) {
		return s.flushLocked()
	}
	return nil
}

// WriteBatch writes a batch of events to the sink.
func (s *ClickhouseSink) WriteBatch(events []*models.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.events = append(s.events, events...)
	s.pending += len(events)

	if s.pending >= int(s.cfg.BatchSize) {
		return s.flushLocked()
	}
	return nil
}

// Flush forces a sync of the current buffer.
func (s *ClickhouseSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

func (s *ClickhouseSink) flushLocked() error {
	if s.pending == 0 {
		return nil
	}

	// Group events by table as Clickhouse Bulk Insert is per table
	tableEvents := make(map[string][]*models.Event)
	for _, event := range s.events {
		tableName := s.getTableName(event)
		tableEvents[tableName] = append(tableEvents[tableName], event)
	}

	ctx := context.Background()
	for tableName, events := range tableEvents {
		if err := s.flushTable(ctx, tableName, events); err != nil {
			slog.Error("Clickhouse flush table failed", "table", tableName, "error", err)
		}
	}

	// Reset buffer
	s.events = s.events[:0]
	s.pending = 0

	return nil
}

func (s *ClickhouseSink) flushTable(ctx context.Context, tableName string, events []*models.Event) error {
	if len(events) == 0 {
		return nil
	}

	// We'll use the first event to determine columns. 
	// This assumes all events for the same table in a batch have the same schema.
	var firstNode ast.Node
	var err error
	if events[0].Op == "d" {
		firstNode, err = sonic.Get(events[0].Data, "before")
	} else {
		firstNode, err = sonic.Get(events[0].Data, "after")
	}

	if err != nil || !firstNode.Exists() {
		return fmt.Errorf("failed to get first event node: %w", err)
	}

	firstData, _ := firstNode.MarshalJSON()
	var firstMap map[string]interface{}
	if err := sonic.Unmarshal(firstData, &firstMap); err != nil {
		return fmt.Errorf("failed to unmarshal first event: %w", err)
	}

	columns := make([]string, 0, len(firstMap))
	for k := range firstMap {
		columns = append(columns, k)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s)", tableName, strings.Join(columns, ", "))
	batch, err := s.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, event := range events {
		var node ast.Node
		if event.Op == "d" {
			node, err = sonic.Get(event.Data, "before")
		} else {
			node, err = sonic.Get(event.Data, "after")
		}

		if err != nil || !node.Exists() {
			continue
		}

		data, _ := node.MarshalJSON()
		// Apply transformations
		data, _ = s.cfg.ApplyFieldMapping(data)

		var m map[string]interface{}
		_ = sonic.Unmarshal(data, &m)

		args := make([]interface{}, len(columns))
		for i, col := range columns {
			args[i] = m[col]
		}

		if err := batch.Append(args...); err != nil {
			slog.Error("Clickhouse append failed", "error", err)
		}
	}

	return batch.Send()
}

func (s *ClickhouseSink) getTableName(event *models.Event) string {
	// Logic to map instance/table to CH table
	if s.cfg.IndexMapping != nil {
		if t, ok := s.cfg.IndexMapping[fmt.Sprintf("%s.%s", event.InstanceID, event.Table)]; ok {
			return t
		}
		if t, ok := s.cfg.IndexMapping[event.Table]; ok {
			return t
		}
	}

	if s.cfg.Index != "" {
		return s.cfg.Index
	}

	return event.Table
}

// flushLoop runs a background goroutine that flushes the buffer periodically.
func (s *ClickhouseSink) flushLoop() {
	for {
		select {
		case <-s.done:
			return
		case <-s.flushTicker.C:
			if err := s.Flush(); err != nil {
				slog.Error("clickhouse periodic flush failed", "err", err)
			}
		}
	}
}

// Close gracefully flushes remaining events and stops the sink.
func (s *ClickhouseSink) Close() error {
	slog.Info("closing clickhouse sink")
	s.flushTicker.Stop()
	close(s.done)
	err := s.Flush()
	s.conn.Close()
	return err
}

// Type returns the sink type.
func (s *ClickhouseSink) Type() string {
	return constant.SinkTypeClickhouse.String()
}

// InstanceID returns the instance ID of the sink.
func (s *ClickhouseSink) InstanceID() string {
	return s.cfg.InstanceID
}

// Topic returns the NATS topic pattern this sink subscribes to.
func (s *ClickhouseSink) Topic() string {
	if s.cfg.Topic != "" {
		return s.cfg.Topic
	}
	return "cdc.>"
}

// MaxRetries returns the maximum number of delivery attempts.
func (s *ClickhouseSink) MaxRetries() int32 {
	return s.cfg.MaxRetries
}
