//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/domain"
	testcontainers "github.com/foden/cdc/internal/testutil/containers"
	"github.com/nats-io/nats.go/jetstream"
)

func TestNATSPublishBatchFetchRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	natsContainer := testcontainers.StartNATS(ctx, t)
	defer func() { _ = natsContainer.Cleanup(context.Background()) }()

	client := newIntegrationNATSClient(t, natsContainer.URL, "CDC_NATS_PUBLISH_FETCH")
	defer client.Close()
	if err := client.CreateStream(ctx, []string{"cdc.>"}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	events := []*domain.Event{
		{
			InstanceID: "src",
			Schema:     "public",
			Table:      "orders",
			Op:         constant.OpCreate,
			Offset:     "lsn-1",
			LSN:        1,
			Partition:  0,
			MessageID:  "src-orders-1",
			Data:       []byte(`{"op":"c","after":{"id":1,"status":"new"}}`),
		},
		{
			InstanceID: "src",
			Schema:     "public",
			Table:      "orders",
			Op:         constant.OpUpdate,
			Offset:     "lsn-2",
			LSN:        2,
			Partition:  1,
			MessageID:  "src-orders-2",
			Data:       []byte(`{"op":"u","after":{"id":1,"status":"paid"}}`),
		},
	}

	err := client.PublishBatch(ctx, func(event *domain.Event) string {
		return fmt.Sprintf("cdc.%s.%s.%s.%d", event.InstanceID, event.Schema, event.Table, event.Partition)
	}, events)
	if err != nil {
		t.Fatalf("publish batch: %v", err)
	}

	consumer, err := client.CreateOrUpdateConsumer(ctx, "flow-publish-fetch", []string{"cdc.src.public.orders.>"})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	batch, err := consumer.Fetch(2, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatalf("fetch messages: %v", err)
	}

	got := 0
	for msg := range batch.Messages() {
		got++
		headers := msg.Headers()
		if headers.Get(constant.HeaderInstanceID) != "src" {
			t.Fatalf("instance header = %q, want src", headers.Get(constant.HeaderInstanceID))
		}
		if headers.Get(constant.HeaderSchema) != "public" || headers.Get(constant.HeaderTable) != "orders" {
			t.Fatalf("table headers schema=%q table=%q", headers.Get(constant.HeaderSchema), headers.Get(constant.HeaderTable))
		}
		if headers.Get(constant.HeaderOffset) == "" {
			t.Fatalf("offset header is empty")
		}
		if headers.Get("Nats-Msg-Id") == "" {
			t.Fatalf("Nats-Msg-Id header is empty")
		}
		if len(msg.Data()) == 0 {
			t.Fatalf("message payload is empty")
		}
		if err := msg.Ack(); err != nil {
			t.Fatalf("ack message: %v", err)
		}
	}
	if got != 2 {
		t.Fatalf("fetched messages = %d, want 2", got)
	}
}
