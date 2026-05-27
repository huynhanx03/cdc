package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	_ "github.com/go-mysql-org/go-mysql/driver"

	"github.com/foden/cdc/config"
	sinkcommon "github.com/foden/cdc/internal/adapters/driven/connector/sink/common"
	"github.com/foden/cdc/internal/adapters/driven/registry"
	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/domain"
	"github.com/foden/cdc/internal/core/ports"
	"github.com/foden/cdc/pkg/utils"
)

const mysqlMaxParams = 10000

func init() {
	registry.RegisterSink(constant.SinkTypeMySQL.String(), func(cfg *ports.SinkConfig) (ports.Sink, error) {
		return New(cfg)
	})
}

type MySQLSink struct {
	db            *sql.DB
	cfg           *ports.SinkConfig
	metadataCache sync.Map
	loadMetadata  func(context.Context, string, string) (sinkcommon.TableMetadata, error)
}

func New(cfg *ports.SinkConfig) (*MySQLSink, error) {
	dsn := config.MySQLDSN(cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, sinkcommon.ClassifySinkError(fmt.Errorf("failed to open mysql sink: %w", err))
	}
	if err := db.PingContext(context.Background()); err != nil {
		db.Close()
		return nil, sinkcommon.ClassifySinkError(fmt.Errorf("failed to ping mysql sink: %w", err))
	}
	sink := &MySQLSink{db: db, cfg: cfg}
	sink.loadMetadata = sink.loadTableMetadata
	return sink, nil
}

func (s *MySQLSink) WriteBatch(ctx context.Context, events []*domain.Event) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return sinkcommon.ClassifySinkError(fmt.Errorf("begin mysql transaction: %w", err))
	}
	defer tx.Rollback()

	groups := make(map[string]*mysqlWriteGroup)
	for _, event := range events {
		row, ok, err := sinkcommon.RowMap(event)
		if err != nil {
			return sinkcommon.PermanentError(sinkcommon.ReasonInvalidRecord, err)
		}
		if !ok {
			continue
		}

		meta, err := s.metadataForTable(ctx, event.Table)
		if err != nil {
			return err
		}
		key := meta.Schema + "." + meta.Table
		group := groups[key]
		if group == nil {
			group = &mysqlWriteGroup{meta: meta}
			groups[key] = group
		}
		if event.Op == constant.OpDelete {
			if _, err := primaryKeyValues(row, meta.PrimaryKeys); err != nil {
				return err
			}
			group.deletes = append(group.deletes, row)
			continue
		}

		group.upserts = append(group.upserts, row)
	}

	for _, group := range groups {
		if err := execMySQLBulk(ctx, tx, group); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return sinkcommon.ClassifySinkError(fmt.Errorf("commit mysql transaction: %w", err))
	}
	return nil
}

type mysqlExec interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

type mysqlWriteGroup struct {
	meta    sinkcommon.TableMetadata
	upserts []map[string]interface{}
	deletes []map[string]interface{}
}

func execMySQLBulk(ctx context.Context, tx mysqlExec, group *mysqlWriteGroup) error {
	meta := group.meta
	upsertChunk := rowsPerChunk(len(meta.Columns), mysqlMaxParams)
	for _, rows := range chunkRows(group.upserts, upsertChunk) {
		query := buildBulkUpsertSQLForRows(meta.Schema+"."+meta.Table, meta.PrimaryKeys, meta.Columns, len(rows))
		args := valuesForRows(rows, meta.Columns)
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return sinkcommon.ClassifySinkError(fmt.Errorf("mysql bulk upsert table %s: %w", meta.Table, err))
		}
	}

	deleteChunk := rowsPerChunk(len(meta.PrimaryKeys), mysqlMaxParams)
	for _, rows := range chunkRows(group.deletes, deleteChunk) {
		query := buildBulkDeleteSQLForRows(meta.Schema+"."+meta.Table, meta.PrimaryKeys, len(rows))
		args := valuesForRows(rows, meta.PrimaryKeys)
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return sinkcommon.ClassifySinkError(fmt.Errorf("mysql bulk delete table %s: %w", meta.Table, err))
		}
	}
	return nil
}

func (s *MySQLSink) metadataForTable(ctx context.Context, table string) (sinkcommon.TableMetadata, error) {
	database := ""
	if s.cfg != nil {
		database = s.cfg.Database
	}
	key, base, err := sinkcommon.MySQLTableKey(database, table)
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
		return sinkcommon.TableMetadata{}, sinkcommon.PermanentError(sinkcommon.ReasonMissingMetadata, fmt.Errorf("mysql sink table %s has no columns or does not exist", key))
	}
	if len(meta.PrimaryKeys) == 0 {
		return sinkcommon.TableMetadata{}, sinkcommon.PermanentError(sinkcommon.ReasonMissingMetadata, fmt.Errorf("mysql sink table %s has no primary key", key))
	}
	qualifiedTable := key
	meta.UpsertSQL = buildUpsertSQLForColumns(qualifiedTable, meta.PrimaryKeys, meta.Columns)
	meta.DeleteSQL = buildDeleteSQL(qualifiedTable, meta.PrimaryKeys)
	actual, _ := s.metadataCache.LoadOrStore(key, meta)
	return actual.(sinkcommon.TableMetadata), nil
}

func (s *MySQLSink) loadTableMetadata(ctx context.Context, database, table string) (sinkcommon.TableMetadata, error) {
	columns, err := s.queryColumns(ctx, database, table)
	if err != nil {
		return sinkcommon.TableMetadata{}, err
	}
	primaryKeys, err := s.queryPrimaryKeys(ctx, database, table)
	if err != nil {
		return sinkcommon.TableMetadata{}, err
	}
	return sinkcommon.TableMetadata{Schema: database, Table: table, Columns: columns, PrimaryKeys: primaryKeys}, nil
}

func (s *MySQLSink) queryColumns(ctx context.Context, database, table string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT column_name
FROM information_schema.columns
WHERE table_schema = ? AND table_name = ?
ORDER BY ordinal_position`, database, table)
	if err != nil {
		return nil, sinkcommon.ClassifySinkError(fmt.Errorf("query mysql columns for %s.%s: %w", database, table, err))
	}
	defer rows.Close()

	columns := make([]string, 0)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("scan mysql column for %s.%s: %w", database, table, err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read mysql columns for %s.%s: %w", database, table, err)
	}
	return columns, nil
}

func (s *MySQLSink) queryPrimaryKeys(ctx context.Context, database, table string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT column_name
FROM information_schema.key_column_usage
WHERE table_schema = ?
  AND table_name = ?
  AND constraint_name = 'PRIMARY'
ORDER BY ordinal_position`, database, table)
	if err != nil {
		return nil, sinkcommon.ClassifySinkError(fmt.Errorf("query mysql primary keys for %s.%s: %w", database, table, err))
	}
	defer rows.Close()

	primaryKeys := make([]string, 0)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("scan mysql primary key for %s.%s: %w", database, table, err)
		}
		primaryKeys = append(primaryKeys, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read mysql primary keys for %s.%s: %w", database, table, err)
	}
	return primaryKeys, nil
}

func buildUpsertSQLForColumns(table string, primaryKeys []string, cols []string) string {
	return buildBulkUpsertSQLForRows(table, primaryKeys, cols, 1)
}

func buildBulkUpsertSQLForRows(table string, primaryKeys []string, cols []string, rowCount int) string {
	pkSet := makeStringSet(primaryKeys)
	quotedCols := make([]string, 0, len(cols))
	updates := make([]string, 0, len(cols))
	for _, col := range cols {
		quoted := utils.QuoteIdentifierBacktick(col)
		quotedCols = append(quotedCols, quoted)
		if !pkSet[col] {
			updates = append(updates, fmt.Sprintf("%s = VALUES(%s)", quoted, quoted))
		}
	}
	if len(updates) == 0 {
		quotedPK := utils.QuoteIdentifierBacktick(primaryKeys[0])
		updates = append(updates, fmt.Sprintf("%s = %s", quotedPK, quotedPK))
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
		utils.QuoteIdentifierBacktick(table),
		strings.Join(quotedCols, ", "),
		mysqlValuePlaceholders(rowCount, len(cols)),
		strings.Join(updates, ", "),
	)
	return query
}

func mysqlValuePlaceholders(rowCount, colCount int) string {
	rows := make([]string, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		cols := make([]string, 0, colCount)
		for j := 0; j < colCount; j++ {
			cols = append(cols, "?")
		}
		rows = append(rows, fmt.Sprintf("(%s)", strings.Join(cols, ", ")))
	}
	return strings.Join(rows, ", ")
}

func buildDeleteSQL(table string, primaryKeys []string) string {
	return buildBulkDeleteSQLForRows(table, primaryKeys, 1)
}

func buildBulkDeleteSQLForRows(table string, primaryKeys []string, rowCount int) string {
	if rowCount <= 1 {
		return buildSingleDeleteSQL(table, primaryKeys)
	}
	predicates := make([]string, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		predicates = append(predicates, mysqlDeletePredicate(primaryKeys))
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", utils.QuoteIdentifierBacktick(table), strings.Join(predicates, " OR "))
}

func buildSingleDeleteSQL(table string, primaryKeys []string) string {
	clauses := make([]string, 0, len(primaryKeys))
	for _, pk := range primaryKeys {
		clauses = append(clauses, fmt.Sprintf("%s = ?", utils.QuoteIdentifierBacktick(pk)))
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", utils.QuoteIdentifierBacktick(table), strings.Join(clauses, " AND "))
}

func mysqlDeletePredicate(primaryKeys []string) string {
	clauses := make([]string, 0, len(primaryKeys))
	for _, pk := range primaryKeys {
		clauses = append(clauses, fmt.Sprintf("%s = ?", utils.QuoteIdentifierBacktick(pk)))
	}
	return fmt.Sprintf("(%s)", strings.Join(clauses, " AND "))
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

func (s *MySQLSink) Close() error {
	return s.db.Close()
}

func (s *MySQLSink) InstanceID() string {
	return s.cfg.InstanceID
}

func (s *MySQLSink) Type() string {
	return constant.SinkTypeMySQL.String()
}
