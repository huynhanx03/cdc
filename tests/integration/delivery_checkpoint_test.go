//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	postgressink "github.com/foden/cdc/internal/adapters/driven/connector/sink/postgres"
	"github.com/foden/cdc/internal/adapters/driven/storage"
	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/domain"
	"github.com/foden/cdc/internal/core/flow"
	"github.com/foden/cdc/internal/core/ports"
	coreruntime "github.com/foden/cdc/internal/core/runtime"
	"github.com/foden/cdc/internal/testutil/assertcdc"
	testcontainers "github.com/foden/cdc/internal/testutil/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFlowWorkerCheckpointSurvivesWorkerRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	natsContainer := testcontainers.StartNATS(ctx, t)
	defer func() { _ = natsContainer.Cleanup(context.Background()) }()
	postgresContainer := testcontainers.StartPostgres(ctx, t)
	defer func() { _ = postgresContainer.Cleanup(context.Background()) }()

	client := newIntegrationNATSClient(t, natsContainer.URL, "CDC_CHECKPOINT_RECOVERY")
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
	sink := newPostgresSink(t, postgresContainer, "checkpoint-sink")
	defer func() { _ = sink.Close() }()

	flowConfig := checkpointFlowConfig()
	poolManager := flow.NewPoolManager()
	runtimeMetrics := coreruntime.NewMetrics()

	worker := startCheckpointWorker(t, ctx, flowConfig, sink, poolManager, store, client, runtimeMetrics)
	publishFlowEvent(t, ctx, client, 1, "new", 10, "offset-1")
	waitForOrderAndCheckpoint(t, ctx, db, store, flowConfig.FlowID, 1, "new", 10, "offset-1")
	worker.Stop()

	worker = startCheckpointWorker(t, ctx, flowConfig, sink, poolManager, store, client, runtimeMetrics)
	defer worker.Stop()
	publishFlowEvent(t, ctx, client, 1, "paid", 20, "offset-2")
	waitForOrderAndCheckpoint(t, ctx, db, store, flowConfig.FlowID, 1, "paid", 20, "offset-2")
}

func checkpointFlowConfig() *ports.FlowConfig {
	return &ports.FlowConfig{
		FlowID:      "flow-checkpoint-recovery",
		Name:        "Checkpoint recovery",
		SourceID:    "src",
		SinkID:      "checkpoint-sink",
		SourceTable: "public.orders",
		SinkTable:   "public.orders",
		Status:      ports.FlowStatusRunning,
		Options: &ports.FlowOptions{
			BatchSize:       1,
			FlushIntervalMs: 100,
			PoolSize:        1,
			PartitionCount:  1,
		},
	}
}

func startCheckpointWorker(
	t testing.TB,
	ctx context.Context,
	flowConfig *ports.FlowConfig,
	sink *postgressink.PostgresSink,
	poolManager *flow.PoolManager,
	store ports.Store,
	client ports.NATSClient,
	runtimeMetrics *coreruntime.Metrics,
) *flow.FlowWorker {
	t.Helper()

	worker, err := flow.StartFlowWorker(ctx, flowConfig, sink, poolManager, store, client, 3, runtimeMetrics)
	if err != nil {
		t.Fatalf("start flow worker: %v", err)
	}
	return worker
}

func publishFlowEvent(
	t testing.TB,
	ctx context.Context,
	client interface {
		Publish(context.Context, string, *domain.Event) error
	},
	id int,
	status string,
	amount int,
	offset string,
) {
	t.Helper()

	event := ordersEvent(constant.OpCreate, id, status, amount, offset)
	event.LSN = uint64(id)
	event.Partition = 0
	subject := flow.CDCSubject("src", "public", "orders", "0")
	if err := client.Publish(ctx, subject, event); err != nil {
		t.Fatalf("publish flow event: %v", err)
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

	assertcdc.Eventually(t, 20*time.Second, 100*time.Millisecond, func() error {
		var status string
		var amount int
		if err := db.QueryRow(ctx, `SELECT status, amount FROM public.orders WHERE id = $1`, id).Scan(&status, &amount); err != nil {
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
			return fmt.Errorf("checkpoint position = %q, want %q", checkpoint.Position, wantPosition)
		}
		return nil
	})
}
