//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	postgressink "github.com/foden/cdc/internal/adapters/driven/connector/sink/postgres"
	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/domain"
	"github.com/foden/cdc/internal/core/ports"
	testcontainers "github.com/foden/cdc/internal/testutil/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresSinkWritesCreateUpdateDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	postgresContainer := testcontainers.StartPostgres(ctx, t)
	defer func() { _ = postgresContainer.Cleanup(context.Background()) }()

	db := newPostgresPool(t, ctx, postgresContainer.DSN)
	defer db.Close()
	createOrdersTable(t, ctx, db)

	sink := newPostgresSink(t, postgresContainer, "sink-write")
	defer func() { _ = sink.Close() }()

	if err := sink.WriteBatch(ctx, []*domain.Event{ordersEvent(constant.OpCreate, 1, "new", 10, "offset-1")}); err != nil {
		t.Fatalf("write create: %v", err)
	}
	assertOrder(t, ctx, db, 1, "new", 10)

	if err := sink.WriteBatch(ctx, []*domain.Event{ordersEvent(constant.OpUpdate, 1, "paid", 20, "offset-2")}); err != nil {
		t.Fatalf("write update: %v", err)
	}
	assertOrder(t, ctx, db, 1, "paid", 20)

	deleteEvent := &domain.Event{
		InstanceID: "src",
		Schema:     "public",
		Table:      "orders",
		Op:         constant.OpDelete,
		Offset:     "offset-3",
		MessageID:  "delete-1",
		Data:       []byte(`{"before":{"id":1}}`),
	}
	if err := sink.WriteBatch(ctx, []*domain.Event{deleteEvent}); err != nil {
		t.Fatalf("write delete: %v", err)
	}
	assertOrderDeleted(t, ctx, db, 1)
}

func newPostgresPool(t testing.TB, ctx context.Context, dsn string) *pgxpool.Pool {
	t.Helper()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	return pool
}

func createOrdersTable(t testing.TB, ctx context.Context, db *pgxpool.Pool) {
	t.Helper()

	_, err := db.Exec(ctx, `
CREATE TABLE public.orders (
  id INTEGER PRIMARY KEY,
  status TEXT NOT NULL,
  amount INTEGER NOT NULL
)`)
	if err != nil {
		t.Fatalf("create orders table: %v", err)
	}
}

func newPostgresSink(t testing.TB, postgresContainer *testcontainers.RunningPostgres, instanceID string) *postgressink.PostgresSink {
	t.Helper()

	sink, err := postgressink.New(&ports.SinkConfig{
		InstanceID: instanceID,
		Type:       constant.SinkTypePostgres.String(),
		Host:       postgresContainer.Host,
		Port:       postgresContainer.Port,
		Username:   postgresContainer.User,
		Password:   postgresContainer.Password,
		Database:   postgresContainer.Database,
	})
	if err != nil {
		t.Fatalf("new postgres sink: %v", err)
	}
	return sink
}

func ordersEvent(op constant.Op, id int, status string, amount int, offset string) *domain.Event {
	return &domain.Event{
		InstanceID: "src",
		Schema:     "public",
		Table:      "orders",
		Op:         op,
		Offset:     offset,
		MessageID:  offset,
		Data:       []byte(fmt.Sprintf(`{"after":{"id":%d,"status":%q,"amount":%d}}`, id, status, amount)),
	}
}

func assertOrder(t testing.TB, ctx context.Context, db *pgxpool.Pool, id int, wantStatus string, wantAmount int) {
	t.Helper()

	var status string
	var amount int
	if err := db.QueryRow(ctx, `SELECT status, amount FROM public.orders WHERE id = $1`, id).Scan(&status, &amount); err != nil {
		t.Fatalf("query order %d: %v", id, err)
	}
	if status != wantStatus || amount != wantAmount {
		t.Fatalf("order %d = status %q amount %d, want %q/%d", id, status, amount, wantStatus, wantAmount)
	}
}

func assertOrderDeleted(t testing.TB, ctx context.Context, db *pgxpool.Pool, id int) {
	t.Helper()

	var count int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM public.orders WHERE id = $1`, id).Scan(&count); err != nil {
		t.Fatalf("count order %d: %v", id, err)
	}
	if count != 0 {
		t.Fatalf("order %d count = %d, want 0", id, count)
	}
}
