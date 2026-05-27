package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	sinkcommon "github.com/foden/cdc/internal/adapters/driven/connector/sink/common"
	"github.com/foden/cdc/internal/adapters/driven/registry"
	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/domain"
	"github.com/foden/cdc/internal/core/ports"
	"github.com/foden/cdc/pkg/utils"
)

var clickhouseCDCColumns = []string{"_cdc_op", "_cdc_ts", "_cdc_deleted", "_cdc_lsn"}

const (
	defaultAddress = "127.0.0.1:9000"
)

func init() {
	registry.RegisterSink(constant.SinkTypeClickhouse.String(), func(cfg *ports.SinkConfig) (ports.Sink, error) {
		return New(cfg)
	})
}

// ClickhouseSink writes CDC events to ClickHouse.
type ClickhouseSink struct {
	conn        clickhouse.Conn
	cfg         *ports.SinkConfig
	schemaCache sync.Map
}

// New creates a ClickhouseSink and verifies connection.
func New(cfg *ports.SinkConfig) (*ClickhouseSink, error) {
	addr := cfg.Host
	if cfg.Port > 0 {
		addr = fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	} else if len(cfg.URL) > 0 {
		addr = cfg.URL[0]
	}

	if addr == "" {
		addr = defaultAddress
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

	return &ClickhouseSink{conn: conn, cfg: cfg}, nil
}

// WriteBatch writes events to ClickHouse grouped by table.
func (s *ClickhouseSink) WriteBatch(ctx context.Context, events []*domain.Event) error {
	// Group events by table as ClickHouse Bulk Insert is per table
	tableEvents := make(map[string][]*domain.Event)
	for _, event := range events {
		tableEvents[event.Table] = append(tableEvents[event.Table], event)
	}

	for tableName, evts := range tableEvents {
		if err := s.writeTable(ctx, tableName, evts); err != nil {
			slog.Error("Clickhouse write table failed", "table", tableName, "error", err)
			return err
		}
	}
	return nil
}

func (s *ClickhouseSink) writeTable(ctx context.Context, tableName string, events []*domain.Event) error {
	if len(events) == 0 {
		return nil
	}

	firstMap, ok, err := sinkcommon.RowMap(events[0])
	if err != nil {
		return fmt.Errorf("failed to unmarshal first event: %w", err)
	}
	if !ok {
		return fmt.Errorf("first event has no row payload")
	}

	columns := s.columnsForTable(tableName, firstMap)

	query := buildInsertSQL(tableName, columns)
	batch, err := s.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, event := range events {
		m, ok, err := sinkcommon.RowMap(event)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		args := clickhouseAppendArgs(columns, m, event)

		if err := batch.Append(args...); err != nil {
			slog.Error("Clickhouse append failed", "error", err)
			return fmt.Errorf("clickhouse append failed: %w", err)
		}
	}

	return batch.Send()
}

func (s *ClickhouseSink) columnsForTable(table string, row map[string]interface{}) []string {
	if cached, ok := s.schemaCache.Load(table); ok {
		return cached.([]string)
	}
	columns := clickhouseColumns(row)
	actual, _ := s.schemaCache.LoadOrStore(table, columns)
	return actual.([]string)
}

func clickhouseColumns(row map[string]interface{}) []string {
	columns := make([]string, 0, len(row)+len(clickhouseCDCColumns))
	for column := range row {
		if isClickhouseCDCColumn(column) {
			continue
		}
		columns = append(columns, column)
	}
	sort.Strings(columns)
	columns = append(columns, clickhouseCDCColumns...)
	return columns
}

func buildInsertSQL(table string, columns []string) string {
	quotedColumns := make([]string, 0, len(columns))
	for _, column := range columns {
		quotedColumns = append(quotedColumns, utils.QuoteIdentifierBacktick(column))
	}
	return fmt.Sprintf("INSERT INTO %s (%s)", utils.QuoteIdentifierBacktick(table), strings.Join(quotedColumns, ", "))
}

func clickhouseAppendArgs(columns []string, row map[string]interface{}, event *domain.Event) []interface{} {
	args := make([]interface{}, len(columns))
	for i, column := range columns {
		switch column {
		case "_cdc_op":
			args[i] = event.Op.String()
		case "_cdc_ts":
			args[i] = clickhouseEventTimestamp(event)
		case "_cdc_deleted":
			args[i] = event.Op == constant.OpDelete
		case "_cdc_lsn":
			args[i] = event.LSN
		default:
			args[i] = row[column]
		}
	}
	return args
}

func clickhouseEventTimestamp(event *domain.Event) time.Time {
	if event == nil || event.TimestampMS <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(event.TimestampMS).UTC()
}

func isClickhouseCDCColumn(column string) bool {
	for _, cdcColumn := range clickhouseCDCColumns {
		if column == cdcColumn {
			return true
		}
	}
	return false
}

// Close closes the ClickHouse connection.
func (s *ClickhouseSink) Close() error {
	return s.conn.Close()
}

// InstanceID returns the sink instance ID.
func (s *ClickhouseSink) InstanceID() string {
	return s.cfg.InstanceID
}

// Type returns the sink type.
func (s *ClickhouseSink) Type() string {
	return constant.SinkTypeClickhouse.String()
}
