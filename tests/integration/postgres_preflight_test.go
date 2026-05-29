//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	testcontainers "github.com/foden/cdc/internal/testutil/containers"
	"github.com/jackc/pgx/v5"
)

func TestPostgresPreflightDatabaseFacts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pg := testcontainers.StartPostgres(ctx, t)
	defer func() { _ = pg.Cleanup(context.Background()) }()

	conn, err := pgx.Connect(ctx, pg.DSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer conn.Close(ctx)

	var walLevel string
	if err := conn.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		t.Fatalf("show wal_level: %v", err)
	}
	if walLevel != "logical" {
		t.Fatalf("wal_level = %q, want logical", walLevel)
	}

	if _, err := conn.Exec(ctx, `
		CREATE TABLE no_pk_orders (id bigint, amount numeric(38, 10));
		CREATE TABLE full_identity_orders (id bigint, amount numeric(38, 10));
		ALTER TABLE full_identity_orders REPLICA IDENTITY FULL;
	`); err != nil {
		t.Fatalf("create preflight tables: %v", err)
	}

	var noPKIdentity string
	if err := conn.QueryRow(ctx, `SELECT relreplident::text FROM pg_class WHERE oid = 'no_pk_orders'::regclass`).Scan(&noPKIdentity); err != nil {
		t.Fatalf("query no_pk replica identity: %v", err)
	}
	if noPKIdentity != "d" {
		t.Fatalf("no_pk_orders relreplident = %q, want default d", noPKIdentity)
	}

	var fullIdentity string
	if err := conn.QueryRow(ctx, `SELECT relreplident::text FROM pg_class WHERE oid = 'full_identity_orders'::regclass`).Scan(&fullIdentity); err != nil {
		t.Fatalf("query full replica identity: %v", err)
	}
	if fullIdentity != "f" {
		t.Fatalf("full_identity_orders relreplident = %q, want full f", fullIdentity)
	}
}

func TestPostgresPreflightValidationQualityGate(t *testing.T) {
	t.Skip("pending product implementation: ValidateFlow/Postgres preflight must reject update/delete flows on tables without PK or REPLICA IDENTITY FULL")
}
