//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/foden/cdc/config"
	postgressink "github.com/foden/cdc/internal/adapters/driven/connector/sink/postgres"
	natsadapter "github.com/foden/cdc/internal/adapters/driven/nats"
	"github.com/foden/cdc/internal/adapters/driven/storage"
	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/domain"
	"github.com/foden/cdc/internal/core/flow"
	"github.com/foden/cdc/internal/core/ports"
	coreruntime "github.com/foden/cdc/internal/core/runtime"
	"github.com/foden/cdc/internal/testutil/assertcdc"
	testcontainers "github.com/foden/cdc/internal/testutil/containers"
	"github.com/foden/cdc/pkg/utils"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"
)

func TestFlowWizardCreateValidateRunWorkflow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	natsContainer := testcontainers.StartNATS(ctx, t)
	defer func() { _ = natsContainer.Cleanup(context.Background()) }()
	postgresContainer := testcontainers.StartPostgres(ctx, t)
	defer func() { _ = postgresContainer.Cleanup(context.Background()) }()

	client := newE2ENATSClient(t, natsContainer.URL, "CDC_E2E_FLOW")
	defer client.Close()
	if err := client.CreateStream(ctx, []string{"cdc.>"}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	db := newPostgresPool(t, ctx, postgresContainer.DSN)
	defer db.Close()
	createOrdersTable(t, ctx, db)

	store, err := storage.NewNATSKVStore(ctx, client.JetStream())
	if err != nil {
		t.Fatalf("new nats kv store: %v", err)
	}
	sink := newPostgresSink(t, postgresContainer, "e2e-flow-sink")
	defer func() { _ = sink.Close() }()

	flowConfig := e2eFlowConfig("e2e-flow", "public.orders")
	worker := startWorker(t, ctx, flowConfig, sink, flow.NewPoolManager(), store, client)
	defer worker.Stop()

	publishFlowEvent(t, ctx, client, "orders", 101, "confirmed", 35, "e2e-offset-1")
	waitForOrderAndCheckpoint(t, ctx, db, store, flowConfig.FlowID, 101, "confirmed", 35, "e2e-offset-1")
}

func TestExplorerInspectMessageAndConsumerWorkflow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	natsContainer := testcontainers.StartNATS(ctx, t)
	defer func() { _ = natsContainer.Cleanup(context.Background()) }()

	client := newE2ENATSClient(t, natsContainer.URL, "CDC_E2E_EXPLORER")
	defer client.Close()
	if err := client.CreateStream(ctx, []string{"cdc.>"}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	events := []*domain.Event{
		e2eEvent("orders", constant.OpCreate, 201, "paid", 99, "explorer-offset-1"),
		e2eEvent("orders", constant.OpUpdate, 202, "pending", 42, "explorer-offset-2"),
	}
	if err := client.PublishBatch(ctx, func(event *domain.Event) string {
		return fmt.Sprintf("cdc.%s.%s.%s.%d", event.InstanceID, event.Schema, event.Table, event.Partition)
	}, events); err != nil {
		t.Fatalf("publish explorer events: %v", err)
	}

	consumer, err := client.CreateOrUpdateConsumer(ctx, "flow-e2e-explorer", []string{"cdc.src.public.orders.>"})
	if err != nil {
		t.Fatalf("create explorer consumer: %v", err)
	}
	batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatalf("fetch explorer message: %v", err)
	}
	for msg := range batch.Messages() {
		if err := msg.Ack(); err != nil {
			t.Fatalf("ack explorer message: %v", err)
		}
		break
	}

	messages, total, err := client.ListMessagesWithFilter(ctx, domain.MessageStatusAll, 10, 1, ports.NATSMessageFilter{
		Topic:      "cdc.src.public.orders",
		JSONPath:   "after.status",
		JSONEquals: "paid",
	})
	if err != nil {
		t.Fatalf("list explorer messages: %v", err)
	}
	if total != 1 || len(messages) != 1 {
		t.Fatalf("explorer messages = %d/%d, want 1/1", len(messages), total)
	}
	if messages[0].Headers[constant.HeaderOffset] != "explorer-offset-1" {
		t.Fatalf("explorer offset = %q", messages[0].Headers[constant.HeaderOffset])
	}

	assertcdc.Eventually(t, 10*time.Second, 100*time.Millisecond, func() error {
		consumers, total, err := client.ListConsumers(ctx, 10, 1)
		if err != nil {
			return err
		}
		if total != 1 || len(consumers) != 1 {
			return fmt.Errorf("consumers = %d/%d, want 1/1", len(consumers), total)
		}
		if consumers[0].Name != "flow-e2e-explorer" {
			return fmt.Errorf("consumer name = %q", consumers[0].Name)
		}
		if consumers[0].AckFloorStreamSeq == 0 {
			return fmt.Errorf("consumer ack floor was not reported")
		}
		return nil
	})
}

func TestDLQRecoveryWorkflow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	natsContainer := testcontainers.StartNATS(ctx, t)
	defer func() { _ = natsContainer.Cleanup(context.Background()) }()
	postgresContainer := testcontainers.StartPostgres(ctx, t)
	defer func() { _ = postgresContainer.Cleanup(context.Background()) }()

	client := newE2ENATSClient(t, natsContainer.URL, "CDC_E2E_DLQ")
	defer client.Close()
	if err := client.CreateStream(ctx, []string{"cdc.>"}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if err := client.CreateDLQStream(ctx); err != nil {
		t.Fatalf("create dlq stream: %v", err)
	}

	db := newPostgresPool(t, ctx, postgresContainer.DSN)
	defer db.Close()

	store, err := storage.NewNATSKVStore(ctx, client.JetStream())
	if err != nil {
		t.Fatalf("new nats kv store: %v", err)
	}
	sink := newPostgresSink(t, postgresContainer, "e2e-dlq-sink")
	defer func() { _ = sink.Close() }()

	flowConfig := e2eFlowConfig("e2e-dlq", "public.missing_orders")
	worker := startWorker(t, ctx, flowConfig, sink, flow.NewPoolManager(), store, client)
	defer worker.Stop()

	publishFlowEvent(t, ctx, client, "missing_orders", 301, "failed", 1, "dlq-offset-1")

	var dlqID string
	assertcdc.Eventually(t, 30*time.Second, 250*time.Millisecond, func() error {
		messages, total, err := client.ListDLQMessages(ctx, 10, 1)
		if err != nil {
			return err
		}
		if total != 1 || len(messages) != 1 {
			return fmt.Errorf("dlq messages = %d/%d, want 1/1", len(messages), total)
		}
		dlqID = messages[0].Headers["Nats-Msg-Id"]
		if dlqID == "" {
			return fmt.Errorf("dlq id header is empty")
		}
		return nil
	})

	preview, err := client.PreviewDLQ(ctx, []string{dlqID}, ports.DLQFilter{}, 10)
	if err != nil {
		t.Fatalf("preview dlq: %v", err)
	}
	if len(preview) != 1 || preview[0].DLQID != dlqID {
		t.Fatalf("preview = %+v, want dlq id %q", preview, dlqID)
	}

	createDestinationTable(t, ctx, db, "missing_orders")

	result, err := client.ReprocessDLQSelected(ctx, []string{dlqID}, ports.DLQFilter{}, 10)
	if err != nil {
		t.Fatalf("reprocess selected dlq: %v", err)
	}
	if result.Count != 1 {
		t.Fatalf("reprocess count = %d, want 1", result.Count)
	}

	reprocessed, total, err := client.ListMessagesWithFilter(ctx, domain.MessageStatusAll, 10, 1, ports.NATSMessageFilter{
		HeaderKey:   "X-DLQ-Reprocessed-From",
		HeaderValue: dlqID,
	})
	if err != nil {
		t.Fatalf("list reprocessed messages: %v", err)
	}
	if total != 1 || len(reprocessed) != 1 {
		t.Fatalf("reprocessed messages = %d/%d, want 1/1", len(reprocessed), total)
	}

	waitForTableOrderAndCheckpoint(t, ctx, db, "missing_orders", store, flowConfig.FlowID, 301, "failed", 1, "dlq-offset-1")
}

func newE2ENATSClient(t testing.TB, serverURL string, streamName string) *natsadapter.Client {
	t.Helper()

	client, err := natsadapter.NewClient(&config.NATSConfig{
		URL:                   serverURL,
		StreamName:            fmt.Sprintf("%s_%d", streamName, time.Now().UnixNano()),
		RetentionDays:         1,
		MaxReconnects:         -1,
		ReconnectWaitMs:       100,
		ReconnectBufferSizeMb: 8,
		MaxAckPending:         10,
		AckWaitMs:             1_000,
		MaxDeliver:            3,
	})
	if err != nil {
		t.Fatalf("new nats client: %v", err)
	}
	return client
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

	createDestinationTable(t, ctx, db, "orders")
}

func createDestinationTable(t testing.TB, ctx context.Context, db *pgxpool.Pool, table string) {
	t.Helper()

	_, err := db.Exec(ctx, fmt.Sprintf(`
CREATE TABLE public.%s (
  id INTEGER PRIMARY KEY,
  status TEXT NOT NULL,
  amount INTEGER NOT NULL
)`, utils.QuoteIdentifierDoubleQuote(table)))
	if err != nil {
		t.Fatalf("create destination table %s: %v", table, err)
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

func e2eFlowConfig(flowID string, table string) *ports.FlowConfig {
	return &ports.FlowConfig{
		FlowID:      flowID,
		Name:        flowID,
		SourceID:    "src",
		SinkID:      flowID + "-sink",
		SourceTable: table,
		SinkTable:   table,
		Status:      ports.FlowStatusRunning,
		Options: &ports.FlowOptions{
			BatchSize:       1,
			FlushIntervalMs: 100,
			PoolSize:        1,
			PartitionCount:  1,
		},
	}
}

func startWorker(
	t testing.TB,
	ctx context.Context,
	flowConfig *ports.FlowConfig,
	sink *postgressink.PostgresSink,
	poolManager *flow.PoolManager,
	store ports.Store,
	client ports.NATSClient,
) *flow.FlowWorker {
	t.Helper()

	worker, err := flow.StartFlowWorker(ctx, flowConfig, sink, poolManager, store, client, 3, coreruntime.NewMetrics())
	if err != nil {
		t.Fatalf("start worker: %v", err)
	}
	return worker
}

func publishFlowEvent(t testing.TB, ctx context.Context, client *natsadapter.Client, table string, id int, status string, amount int, offset string) {
	t.Helper()

	event := e2eEvent(table, constant.OpCreate, id, status, amount, offset)
	subject := flow.CDCSubject("src", "public", table, "0")
	if err := client.Publish(ctx, subject, event); err != nil {
		t.Fatalf("publish flow event: %v", err)
	}
}

func e2eEvent(table string, op constant.Op, id int, status string, amount int, offset string) *domain.Event {
	return &domain.Event{
		InstanceID: "src",
		Schema:     "public",
		Table:      table,
		Op:         op,
		Offset:     offset,
		LSN:        uint64(id),
		Partition:  0,
		MessageID:  offset,
		Data: []byte(fmt.Sprintf(
			`{"op":%q,"after":{"id":%d,"status":%q,"amount":%d},"source":{"schema":"public","table":%q},"ts_ms":%d}`,
			op,
			id,
			status,
			amount,
			table,
			time.Now().UnixMilli(),
		)),
	}
}

func waitForOrderAndCheckpoint(
	t testing.TB,
	ctx context.Context,
	db *pgxpool.Pool,
	store ports.Store,
	flowID string,
	id int,
	wantStatus string,
	wantAmount int,
	wantPosition string,
) {
	t.Helper()

	waitForTableOrderAndCheckpoint(t, ctx, db, "orders", store, flowID, id, wantStatus, wantAmount, wantPosition)
}

func waitForTableOrderAndCheckpoint(
	t testing.TB,
	ctx context.Context,
	db *pgxpool.Pool,
	table string,
	store ports.Store,
	flowID string,
	id int,
	wantStatus string,
	wantAmount int,
	wantPosition string,
) {
	t.Helper()

	assertcdc.Eventually(t, 20*time.Second, 100*time.Millisecond, func() error {
		var status string
		var amount int
		query := fmt.Sprintf(
			`SELECT status, amount FROM public.%s WHERE id = $1`,
			utils.QuoteIdentifierDoubleQuote(table),
		)
		if err := db.QueryRow(ctx, query, id).Scan(&status, &amount); err != nil {
			return err
		}
		if status != wantStatus || amount != wantAmount {
			return fmt.Errorf("order = %s/%d, want %s/%d", status, amount, wantStatus, wantAmount)
		}
		checkpoint, err := store.GetCheckpoint(ctx, flowID)
		if err != nil {
			return err
		}
		if checkpoint == nil {
			return fmt.Errorf("checkpoint is nil")
		}
		if checkpoint.Position != wantPosition {
			return fmt.Errorf("checkpoint = %q, want %q", checkpoint.Position, wantPosition)
		}
		return nil
	})
}
