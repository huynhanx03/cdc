package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	sinkcommon "github.com/foden/cdc/internal/adapters/driven/connector/sink/common"
)

func TestBuildUpsertSQLQuotesIdentifiers(t *testing.T) {
	query := buildUpsertSQLForColumns(`tenant.us"ers`, []string{"id"}, []string{"id", `weird"name`})
	if !strings.HasPrefix(query, `INSERT INTO "tenant"."us""ers" (`) {
		t.Fatalf("query = %s", query)
	}
	if !strings.Contains(query, `"id"`) || !strings.Contains(query, `"weird""name"`) {
		t.Fatalf("query does not quote columns: %s", query)
	}
	if !strings.Contains(query, `ON CONFLICT ("id") DO UPDATE SET`) {
		t.Fatalf("query missing conflict clause: %s", query)
	}
	if !strings.Contains(query, `"weird""name" = EXCLUDED."weird""name"`) {
		t.Fatalf("query missing update assignment: %s", query)
	}
}

func TestBuildDeleteSQLQuotesIdentifiers(t *testing.T) {
	query := buildDeleteSQL("tenant.users", []string{"id"})
	want := `DELETE FROM "tenant"."users" WHERE "id" = $1`
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

func TestBuildUpsertSQLUsesCompositePrimaryKey(t *testing.T) {
	query := buildUpsertSQLForColumns("tenant.users", []string{"tenant_id", "user_id"}, []string{"tenant_id", "user_id", "name"})
	if !strings.Contains(query, `ON CONFLICT ("tenant_id", "user_id") DO UPDATE SET`) {
		t.Fatalf("query missing composite conflict target: %s", query)
	}
	if strings.Contains(query, `"tenant_id" = EXCLUDED."tenant_id"`) || strings.Contains(query, `"user_id" = EXCLUDED."user_id"`) {
		t.Fatalf("query updates primary key columns: %s", query)
	}
}

func TestBuildBulkUpsertSQLForRows(t *testing.T) {
	query := buildBulkUpsertSQLForRows("users", []string{"id"}, []string{"id", "name"}, 2)
	want := `INSERT INTO "users" ("id", "name") VALUES ($1, $2), ($3, $4) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name"`
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

func TestBuildBulkDeleteSQLForRows(t *testing.T) {
	query := buildBulkDeleteSQLForRows("tenant.users", []string{"tenant_id", "user_id"}, 2)
	want := `DELETE FROM "tenant"."users" WHERE ("tenant_id", "user_id") IN (($1, $2), ($3, $4))`
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
	query := buildDeleteSQL("tenant.users", []string{"tenant_id", "user_id"})
	want := `DELETE FROM "tenant"."users" WHERE "tenant_id" = $1 AND "user_id" = $2`
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

func TestMetadataCacheLoadsTableOnce(t *testing.T) {
	loads := 0
	sink := &PostgresSink{
		loadMetadata: func(_ context.Context, schema, table string) (sinkcommon.TableMetadata, error) {
			loads++
			return sinkcommon.TableMetadata{
				Schema:      schema,
				Table:       table,
				Columns:     []string{"tenant_id", "user_id", "name"},
				PrimaryKeys: []string{"tenant_id", "user_id"},
			}, nil
		},
	}

	first, err := sink.metadataForTable(context.Background(), "tenant", "users")
	if err != nil {
		t.Fatal(err)
	}
	second, err := sink.metadataForTable(context.Background(), "tenant", "users")
	if err != nil {
		t.Fatal(err)
	}
	if loads != 1 {
		t.Fatalf("loads = %d", loads)
	}
	if first.DeleteSQL != `DELETE FROM "tenant"."users" WHERE "tenant_id" = $1 AND "user_id" = $2` {
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
