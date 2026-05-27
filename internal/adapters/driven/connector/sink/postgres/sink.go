package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/foden/cdc/config"
	sinkcommon "github.com/foden/cdc/internal/adapters/driven/connector/sink/common"
	"github.com/foden/cdc/internal/adapters/driven/registry"
	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/domain"
	"github.com/foden/cdc/internal/core/ports"
	"github.com/foden/cdc/pkg/utils"
)

const postgresMaxParams = 60000

func init() {
	registry.RegisterSink(constant.SinkTypePostgres.String(), func(cfg *ports.SinkConfig) (ports.Sink, error) {
		return New(cfg)
	})
}

// PostgresSink writes CDC events to another PostgreSQL database.
type PostgresSink struct {
	pool          *pgxpool.Pool
	cfg           *ports.SinkConfig
	metadataCache sync.Map
	loadMetadata  func(context.Context, string, string) (sinkcommon.TableMetadata, error)
}

// New creates a new PostgresSink instance.
func New(cfg *ports.SinkConfig) (*PostgresSink, error) {
	ctx := context.Background()
	connStr := config.PostgresDSN(cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Database)

	poolCfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, sinkcommon.PermanentError(sinkcommon.ReasonInvalidRecord, fmt.Errorf("failed to parse connection string: %w", err))
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, sinkcommon.ClassifySinkError(fmt.Errorf("failed to connect to postgres: %w", err))
	}

	sink := &PostgresSink{pool: pool, cfg: cfg}
	sink.loadMetadata = sink.loadTableMetadata
	return sink, nil
}

// WriteBatch writes events to PostgreSQL in a single transaction.
func (s *PostgresSink) WriteBatch(ctx context.Context, events []*domain.Event) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return sinkcommon.ClassifySinkError(fmt.Errorf("begin transaction failed: %w", err))
	}
	defer tx.Rollback(ctx)

	groups := make(map[string]*postgresWriteGroup)
	for _, event := range events {
		data, ok, err := sinkcommon.RowMap(event)
		if err != nil {
			return sinkcommon.PermanentError(sinkcommon.ReasonInvalidRecord, err)
		}
		if !ok {
			continue
		}

		tableName := event.Table
		meta, err := s.metadataForTable(ctx, event.Schema, tableName)
		if err != nil {
			return err
		}

		key := meta.Schema + "." + meta.Table
		group := groups[key]
		if group == nil {
			group = &postgresWriteGroup{meta: meta}
			groups[key] = group
		}
		switch event.Op {
		case constant.OpDelete:
			if _, err := primaryKeyValues(data, meta.PrimaryKeys); err != nil {
				return err
			}
			group.deletes = append(group.deletes, data)
		case constant.OpCreate, constant.OpUpdate, constant.OpSnapshot:
			group.upserts = append(group.upserts, data)
		default:
			slog.Warn("unknown operation type", "op", event.Op)
		}
	}

	for _, group := range groups {
		if err := execPostgresBulk(ctx, tx, group); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return sinkcommon.ClassifySinkError(fmt.Errorf("commit transaction failed: %w", err))
	}
	return nil
}

type postgresExec interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

type postgresWriteGroup struct {
	meta    sinkcommon.TableMetadata
	upserts []map[string]interface{}
	deletes []map[string]interface{}
}

func execPostgresBulk(ctx context.Context, tx postgresExec, group *postgresWriteGroup) error {
	meta := group.meta
	upsertChunk := rowsPerChunk(len(meta.Columns), postgresMaxParams)
	for _, rows := range chunkRows(group.upserts, upsertChunk) {
		query := buildBulkUpsertSQLForRows(meta.Schema+"."+meta.Table, meta.PrimaryKeys, meta.Columns, len(rows))
		args := valuesForRows(rows, meta.Columns)
		if _, err := tx.Exec(ctx, query, args...); err != nil {
			return sinkcommon.ClassifySinkError(fmt.Errorf("bulk upsert failed: %w", err))
		}
	}

	deleteChunk := rowsPerChunk(len(meta.PrimaryKeys), postgresMaxParams)
	for _, rows := range chunkRows(group.deletes, deleteChunk) {
		query := buildBulkDeleteSQLForRows(meta.Schema+"."+meta.Table, meta.PrimaryKeys, len(rows))
		args := valuesForRows(rows, meta.PrimaryKeys)
		if _, err := tx.Exec(ctx, query, args...); err != nil {
			return sinkcommon.ClassifySinkError(fmt.Errorf("bulk delete failed: %w", err))
		}
	}
	return nil
}

func (s *PostgresSink) metadataForTable(ctx context.Context, schema, table string) (sinkcommon.TableMetadata, error) {
	key, base, err := sinkcommon.PostgresTableKey(schema, table)
	if err != nil {
		return sinkcommon.TableMetadata{}, err
	}
	if cached, ok := s.metadataCache.Load(key); ok {
		return cached.(sinkcommon.TableMetadata), nil
	}
	loader := s.loadMetadata
	if loader == nil {
		loader = s.loadTableMetadata
	}
	meta, err := loader(ctx, base.Schema, base.Table)
	if err != nil {
		return sinkcommon.TableMetadata{}, err
	}
	meta.Schema = base.Schema
	meta.Table = base.Table
	if len(meta.Columns) == 0 {
		return sinkcommon.TableMetadata{}, sinkcommon.PermanentError(sinkcommon.ReasonMissingMetadata, fmt.Errorf("postgres sink table %s has no columns or does not exist", key))
	}
	if len(meta.PrimaryKeys) == 0 {
		return sinkcommon.TableMetadata{}, sinkcommon.PermanentError(sinkcommon.ReasonMissingMetadata, fmt.Errorf("postgres sink table %s has no primary key", key))
	}
	qualifiedTable := key
	meta.UpsertSQL = buildUpsertSQLForColumns(qualifiedTable, meta.PrimaryKeys, meta.Columns)
	meta.DeleteSQL = buildDeleteSQL(qualifiedTable, meta.PrimaryKeys)
	actual, _ := s.metadataCache.LoadOrStore(key, meta)
	return actual.(sinkcommon.TableMetadata), nil
}

func (s *PostgresSink) loadTableMetadata(ctx context.Context, schema, table string) (sinkcommon.TableMetadata, error) {
	columns, err := s.queryColumns(ctx, schema, table)
	if err != nil {
		return sinkcommon.TableMetadata{}, err
	}
	primaryKeys, err := s.queryPrimaryKeys(ctx, schema, table)
	if err != nil {
		return sinkcommon.TableMetadata{}, err
	}
	return sinkcommon.TableMetadata{Schema: schema, Table: table, Columns: columns, PrimaryKeys: primaryKeys}, nil
}

func (s *PostgresSink) queryColumns(ctx context.Context, schema, table string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
SELECT column_name
FROM information_schema.columns
WHERE table_schema = $1 AND table_name = $2
ORDER BY ordinal_position`, schema, table)
	if err != nil {
		return nil, sinkcommon.ClassifySinkError(fmt.Errorf("query postgres columns for %s.%s: %w", schema, table, err))
	}
	defer rows.Close()

	columns := make([]string, 0)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("scan postgres column for %s.%s: %w", schema, table, err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read postgres columns for %s.%s: %w", schema, table, err)
	}
	return columns, nil
}

func (s *PostgresSink) queryPrimaryKeys(ctx context.Context, schema, table string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
SELECT kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
 AND tc.table_schema = kcu.table_schema
 AND tc.table_name = kcu.table_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_schema = $1
  AND tc.table_name = $2
ORDER BY kcu.ordinal_position`, schema, table)
	if err != nil {
		return nil, sinkcommon.ClassifySinkError(fmt.Errorf("query postgres primary keys for %s.%s: %w", schema, table, err))
	}
	defer rows.Close()

	primaryKeys := make([]string, 0)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("scan postgres primary key for %s.%s: %w", schema, table, err)
		}
		primaryKeys = append(primaryKeys, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read postgres primary keys for %s.%s: %w", schema, table, err)
	}
	return primaryKeys, nil
}

func buildUpsertSQLForColumns(table string, primaryKeys []string, cols []string) string {
	return buildBulkUpsertSQLForRows(table, primaryKeys, cols, 1)
}

func buildBulkUpsertSQLForRows(table string, primaryKeys []string, cols []string, rowCount int) string {
	pkSet := makeStringSet(primaryKeys)
	updates := make([]string, 0, len(cols)-1)
	for _, col := range cols {
		if !pkSet[col] {
			quoted := utils.QuoteIdentifierDoubleQuote(col)
			updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", quoted, quoted))
		}
	}
	if len(updates) == 0 {
		quotedPK := utils.QuoteIdentifierDoubleQuote(primaryKeys[0])
		updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", quotedPK, quotedPK))
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s",
		utils.QuoteIdentifierDoubleQuote(table),
		quotePostgresIdentifiers(cols),
		postgresValuePlaceholders(rowCount, len(cols), 1),
		quotePostgresIdentifiers(primaryKeys),
		strings.Join(updates, ", "),
	)

	return query
}

func postgresValuePlaceholders(rowCount, colCount, start int) string {
	rows := make([]string, 0, rowCount)
	arg := start
	for i := 0; i < rowCount; i++ {
		cols := make([]string, 0, colCount)
		for j := 0; j < colCount; j++ {
			cols = append(cols, fmt.Sprintf("$%d", arg))
			arg++
		}
		rows = append(rows, fmt.Sprintf("(%s)", strings.Join(cols, ", ")))
	}
	return strings.Join(rows, ", ")
}

func quotePostgresIdentifiers(cols []string) string {
	quoted := make([]string, 0, len(cols))
	for _, col := range cols {
		quoted = append(quoted, utils.QuoteIdentifierDoubleQuote(col))
	}
	return strings.Join(quoted, ", ")
}

func buildDeleteSQL(table string, primaryKeys []string) string {
	return buildBulkDeleteSQLForRows(table, primaryKeys, 1)
}

func buildBulkDeleteSQLForRows(table string, primaryKeys []string, rowCount int) string {
	if rowCount <= 1 {
		return buildSingleDeleteSQL(table, primaryKeys)
	}
	quotedPKs := quotePostgresIdentifiers(primaryKeys)
	values := postgresValuePlaceholders(rowCount, len(primaryKeys), 1)
	return fmt.Sprintf("DELETE FROM %s WHERE (%s) IN (%s)", utils.QuoteIdentifierDoubleQuote(table), quotedPKs, values)
}

func buildSingleDeleteSQL(table string, primaryKeys []string) string {
	clauses := make([]string, 0, len(primaryKeys))
	for i, pk := range primaryKeys {
		clauses = append(clauses, fmt.Sprintf("%s = $%d", utils.QuoteIdentifierDoubleQuote(pk), i+1))
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", utils.QuoteIdentifierDoubleQuote(table), strings.Join(clauses, " AND "))
}

func makeStringSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

func primaryKeyValues(row map[string]interface{}, primaryKeys []string) ([]interface{}, error) {
	values := make([]interface{}, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		value, ok := row[key]
		if !ok || value == nil || value == "" {
			return nil, sinkcommon.PermanentError(sinkcommon.ReasonInvalidRecord, fmt.Errorf("missing primary key column %q", key))
		}
		values = append(values, value)
	}
	return values, nil
}

func valuesForColumns(row map[string]interface{}, columns []string) []interface{} {
	return valuesForRows([]map[string]interface{}{row}, columns)
}

func valuesForRows(rows []map[string]interface{}, columns []string) []interface{} {
	values := make([]interface{}, 0, len(rows)*len(columns))
	for _, row := range rows {
		for _, column := range columns {
			values = append(values, row[column])
		}
	}
	return values
}

func rowsPerChunk(columnCount, maxParams int) int {
	if columnCount <= 0 {
		return 1
	}
	rows := maxParams / columnCount
	if rows < 1 {
		return 1
	}
	return rows
}

func chunkRows(rows []map[string]interface{}, size int) [][]map[string]interface{} {
	if len(rows) == 0 {
		return nil
	}
	if size <= 0 || size >= len(rows) {
		return [][]map[string]interface{}{rows}
	}
	chunks := make([][]map[string]interface{}, 0, (len(rows)+size-1)/size)
	for start := 0; start < len(rows); start += size {
		end := start + size
		if end > len(rows) {
			end = len(rows)
		}
		chunks = append(chunks, rows[start:end])
	}
	return chunks
}

func valuesForColumnsLegacy(row map[string]interface{}, columns []string) []interface{} {
	values := make([]interface{}, 0, len(columns))
	for _, column := range columns {
		values = append(values, row[column])
	}
	return values
}

// Close closes the connection pool.
func (s *PostgresSink) Close() error {
	s.pool.Close()
	return nil
}

// InstanceID returns the sink instance ID.
func (s *PostgresSink) InstanceID() string {
	return s.cfg.InstanceID
}

// Type returns the sink type.
func (s *PostgresSink) Type() string {
	return constant.SinkTypePostgres.String()
}
