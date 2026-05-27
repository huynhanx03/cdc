package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"

	sinkcommon "github.com/foden/cdc/internal/adapters/driven/connector/sink/common"
	"github.com/foden/cdc/internal/core/ports"
)

func TestBuildUpsertSQLQuotesIdentifiers(t *testing.T) {
	query := buildUpsertSQLForColumns("app.us`ers", []string{"id"}, []string{"id", "weird`name"})
	if !strings.HasPrefix(query, "INSERT INTO `app`.`us``ers` (") {
		t.Fatalf("query = %s", query)
	}
	if !strings.Contains(query, "`id`") || !strings.Contains(query, "`weird``name`") {
		t.Fatalf("query does not quote columns: %s", query)
	}
	if !strings.Contains(query, "ON DUPLICATE KEY UPDATE") {
		t.Fatalf("query missing upsert clause: %s", query)
	}
	if !strings.Contains(query, "`weird``name` = VALUES(`weird``name`)") {
		t.Fatalf("query missing update assignment: %s", query)
	}
}

func TestBuildDeleteSQLQuotesIdentifiers(t *testing.T) {
	query := buildDeleteSQL("app.users", []string{"id"})
	want := "DELETE FROM `app`.`users` WHERE `id` = ?"
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

func TestBuildUpsertSQLUsesCompositePrimaryKey(t *testing.T) {
	query := buildUpsertSQLForColumns("app.users", []string{"tenant_id", "user_id"}, []string{"tenant_id", "user_id", "name"})
	if strings.Contains(query, "`tenant_id` = VALUES(`tenant_id`)") || strings.Contains(query, "`user_id` = VALUES(`user_id`)") {
		t.Fatalf("query updates primary key columns: %s", query)
	}
	if !strings.Contains(query, "`name` = VALUES(`name`)") {
		t.Fatalf("query missing non-key update: %s", query)
	}
}

func TestBuildBulkUpsertSQLForRows(t *testing.T) {
	query := buildBulkUpsertSQLForRows("users", []string{"id"}, []string{"id", "name"}, 2)
	want := "INSERT INTO `users` (`id`, `name`) VALUES (?, ?), (?, ?) ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

func TestBuildBulkDeleteSQLForRows(t *testing.T) {
	query := buildBulkDeleteSQLForRows("app.users", []string{"tenant_id", "user_id"}, 2)
	want := "DELETE FROM `app`.`users` WHERE (`tenant_id` = ? AND `user_id` = ?) OR (`tenant_id` = ? AND `user_id` = ?)"
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

func TestBulkChunking(t *testing.T) {
	if got := rowsPerChunk(2, 5); got != 2 {
		t.Fatalf("rowsPerChunk = %d, want 2", got)
	}
	rows := []map[string]interface{}{{"id": 1}, {"id": 2}, {"id": 3}}
	chunks := chunkRows(rows, 2)
	if len(chunks) != 2 || len(chunks[0]) != 2 || len(chunks[1]) != 1 {
		t.Fatalf("chunks = %#v", chunks)
	}
}

func TestBuildDeleteSQLUsesCompositePrimaryKey(t *testing.T) {
	query := buildDeleteSQL("app.users", []string{"tenant_id", "user_id"})
	want := "DELETE FROM `app`.`users` WHERE `tenant_id` = ? AND `user_id` = ?"
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

func TestMetadataCacheLoadsTableOnce(t *testing.T) {
	loads := 0
	sink := &MySQLSink{
		cfg: &ports.SinkConfig{Database: "app"},
		loadMetadata: func(_ context.Context, database, table string) (sinkcommon.TableMetadata, error) {
			loads++
			return sinkcommon.TableMetadata{
				Schema:      database,
				Table:       table,
				Columns:     []string{"tenant_id", "user_id", "name"},
				PrimaryKeys: []string{"tenant_id", "user_id"},
			}, nil
		},
	}

	first, err := sink.metadataForTable(context.Background(), "users")
	if err != nil {
		t.Fatal(err)
	}
	second, err := sink.metadataForTable(context.Background(), "users")
	if err != nil {
		t.Fatal(err)
	}
	if loads != 1 {
		t.Fatalf("loads = %d", loads)
	}
	if first.DeleteSQL != "DELETE FROM `app`.`users` WHERE `tenant_id` = ? AND `user_id` = ?" {
		t.Fatalf("delete sql = %q", first.DeleteSQL)
	}
	if first.UpsertSQL == "" || second.UpsertSQL != first.UpsertSQL {
		t.Fatalf("upsert sql not cached: first=%q second=%q", first.UpsertSQL, second.UpsertSQL)
	}
}

func TestPrimaryKeyValuesRequiresAllKeys(t *testing.T) {
	_, err := primaryKeyValues(map[string]interface{}{"tenant_id": 7}, []string{"tenant_id", "user_id"})
	if err == nil {
		t.Fatal("expected missing key error")
	}
	var sinkErr *sinkcommon.SinkError
	if !errors.As(err, &sinkErr) {
		t.Fatal("expected SinkError")
	}
	if sinkErr.Retryable || sinkErr.Reason != sinkcommon.ReasonInvalidRecord {
		t.Fatalf("sinkErr = %+v", sinkErr)
	}
	values, err := primaryKeyValues(map[string]interface{}{"tenant_id": 7, "user_id": 42}, []string{"tenant_id", "user_id"})
	if err != nil {
		t.Fatal(err)
	}
	if values[0] != 7 || values[1] != 42 {
		t.Fatalf("values = %+v", values)
	}
}
