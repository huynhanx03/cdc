//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	natsadapter "github.com/foden/cdc/internal/adapters/driven/nats"
	"github.com/foden/cdc/internal/core/domain"
	"github.com/foden/cdc/internal/core/ports"
	testcontainers "github.com/foden/cdc/internal/testutil/containers"
	gnats "github.com/nats-io/nats.go"
)

func TestDLQReprocessRepublishesWithDeterministicID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	natsContainer := testcontainers.StartNATS(ctx, t)
	defer func() { _ = natsContainer.Cleanup(context.Background()) }()

	client := newIntegrationNATSClient(t, natsContainer.URL, "CDC_DLQ_RECOVERY")
	defer client.Close()
	if err := client.CreateStream(ctx, []string{"cdc.>"}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if err := client.CreateDLQStream(ctx); err != nil {
		t.Fatalf("create dlq stream: %v", err)
	}

	env := natsadapter.DLQEnvelope{
		ID:              "dlq-1",
		MsgID:           "src-123",
		OriginalSubject: "cdc.src.public.orders.0",
		OriginalHeaders: map[string]string{
			"Nats-Msg-Id": "src-123",
		},
		Payload:    json.RawMessage(`{"after":{"id":42}}`),
		Reason:     "sink_error",
		ErrorClass: "sink",
		RetryCount: 1,
		FailedAt:   time.Now().UnixMilli(),
	}
	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	if _, err := client.JetStream().PublishMsg(ctx, &gnats.Msg{
		Subject: "dlq.cdc.src.public.orders.0",
		Data:    data,
	}); err != nil {
		t.Fatalf("publish dlq envelope: %v", err)
	}

	count, err := client.ReprocessDLQ(ctx)
	if err != nil {
		t.Fatalf("reprocess dlq: %v", err)
	}
	if count != 1 {
		t.Fatalf("reprocess count = %d, want 1", count)
	}

	messages, total, err := client.ListMessagesWithFilter(ctx, domain.MessageStatusAll, 10, 1, ports.NATSMessageFilter{
		HeaderKey:   "X-DLQ-Reprocessed-From",
		HeaderValue: "dlq-1",
	})
	if err != nil {
		t.Fatalf("list reprocessed messages: %v", err)
	}
	if total != 1 || len(messages) != 1 {
		t.Fatalf("reprocessed messages = %d/%d, want 1/1", len(messages), total)
	}
	gotID := messages[0].Headers["Nats-Msg-Id"]
	if gotID == "src-123" {
		t.Fatalf("reprocess reused original Nats-Msg-Id")
	}
	if !strings.Contains(gotID, ".reprocess.2.") {
		t.Fatalf("reprocess id = %q, want attempt marker .reprocess.2.", gotID)
	}
}

func TestDLQDryRunDoesNotMutate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	natsContainer := testcontainers.StartNATS(ctx, t)
	defer func() { _ = natsContainer.Cleanup(context.Background()) }()

	client := newIntegrationNATSClient(t, natsContainer.URL, "CDC_DLQ_PREVIEW")
	defer client.Close()
	if err := client.CreateStream(ctx, []string{"cdc.>"}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if err := client.CreateDLQStream(ctx); err != nil {
		t.Fatalf("create dlq stream: %v", err)
	}

	publishDLQEnvelope(t, ctx, client, natsadapter.DLQEnvelope{
		ID:              "dlq-preview",
		MsgID:           "src-preview",
		OriginalSubject: "cdc.src.public.orders.0",
		OriginalHeaders: map[string]string{"Nats-Msg-Id": "src-preview"},
		Payload:         json.RawMessage(`{"after":{"id":100}}`),
		Reason:          "sink_error",
		ErrorClass:      "sink",
		RetryCount:      1,
		FailedAt:        time.Now().UnixMilli(),
	})

	preview, err := client.PreviewDLQ(ctx, []string{"dlq-preview"}, ports.DLQFilter{}, 10)
	if err != nil {
		t.Fatalf("preview dlq: %v", err)
	}
	if len(preview) != 1 {
		t.Fatalf("preview count = %d, want 1", len(preview))
	}
	if preview[0].DLQID != "dlq-preview" || preview[0].OriginalSubject != "cdc.src.public.orders.0" {
		t.Fatalf("preview item = %+v", preview[0])
	}

	messages, total, err := client.ListMessagesWithFilter(ctx, domain.MessageStatusAll, 10, 1, ports.NATSMessageFilter{
		HeaderKey:   "X-DLQ-Reprocessed-From",
		HeaderValue: "dlq-preview",
	})
	if err != nil {
		t.Fatalf("list reprocessed messages: %v", err)
	}
	if total != 0 || len(messages) != 0 {
		t.Fatalf("dry-run published reprocessed messages = %d/%d, want 0/0", len(messages), total)
	}
}

func TestDLQSelectedReprocess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	natsContainer := testcontainers.StartNATS(ctx, t)
	defer func() { _ = natsContainer.Cleanup(context.Background()) }()

	client := newIntegrationNATSClient(t, natsContainer.URL, "CDC_DLQ_SELECTED")
	defer client.Close()
	if err := client.CreateStream(ctx, []string{"cdc.>"}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if err := client.CreateDLQStream(ctx); err != nil {
		t.Fatalf("create dlq stream: %v", err)
	}

	publishDLQEnvelope(t, ctx, client, natsadapter.DLQEnvelope{
		ID:              "dlq-selected",
		MsgID:           "src-selected",
		OriginalSubject: "cdc.src.public.orders.0",
		OriginalHeaders: map[string]string{"Nats-Msg-Id": "src-selected"},
		Payload:         json.RawMessage(`{"after":{"id":201}}`),
		Reason:          "sink_error",
		ErrorClass:      "sink",
		RetryCount:      1,
		FailedAt:        time.Now().UnixMilli(),
	})
	publishDLQEnvelope(t, ctx, client, natsadapter.DLQEnvelope{
		ID:              "dlq-unselected",
		MsgID:           "src-unselected",
		OriginalSubject: "cdc.src.public.orders.1",
		OriginalHeaders: map[string]string{"Nats-Msg-Id": "src-unselected"},
		Payload:         json.RawMessage(`{"after":{"id":202}}`),
		Reason:          "sink_error",
		ErrorClass:      "sink",
		RetryCount:      1,
		FailedAt:        time.Now().UnixMilli(),
	})

	result, err := client.ReprocessDLQSelected(ctx, []string{"dlq-selected"}, ports.DLQFilter{}, 10)
	if err != nil {
		t.Fatalf("selected reprocess: %v", err)
	}
	if result.Count != 1 {
		t.Fatalf("reprocess count = %d, want 1", result.Count)
	}
	if len(result.ReprocessedDLQIDs) != 1 || result.ReprocessedDLQIDs[0] != "dlq-selected" {
		t.Fatalf("reprocessed ids = %+v", result.ReprocessedDLQIDs)
	}

	selected, selectedTotal, err := client.ListMessagesWithFilter(ctx, domain.MessageStatusAll, 10, 1, ports.NATSMessageFilter{
		HeaderKey:   "X-DLQ-Reprocessed-From",
		HeaderValue: "dlq-selected",
	})
	if err != nil {
		t.Fatalf("list selected reprocessed messages: %v", err)
	}
	if selectedTotal != 1 || len(selected) != 1 {
		t.Fatalf("selected reprocessed messages = %d/%d, want 1/1", len(selected), selectedTotal)
	}

	unselected, unselectedTotal, err := client.ListMessagesWithFilter(ctx, domain.MessageStatusAll, 10, 1, ports.NATSMessageFilter{
		HeaderKey:   "X-DLQ-Reprocessed-From",
		HeaderValue: "dlq-unselected",
	})
	if err != nil {
		t.Fatalf("list unselected reprocessed messages: %v", err)
	}
	if unselectedTotal != 0 || len(unselected) != 0 {
		t.Fatalf("unselected reprocessed messages = %d/%d, want 0/0", len(unselected), unselectedTotal)
	}
}

func publishDLQEnvelope(t *testing.T, ctx context.Context, client *natsadapter.Client, env natsadapter.DLQEnvelope) {
	t.Helper()

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	if _, err := client.JetStream().PublishMsg(ctx, &gnats.Msg{
		Subject: "dlq." + env.OriginalSubject,
		Data:    data,
	}); err != nil {
		t.Fatalf("publish dlq envelope: %v", err)
	}
}
