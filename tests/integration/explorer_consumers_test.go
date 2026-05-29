//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/testutil/assertcdc"
	testcontainers "github.com/foden/cdc/internal/testutil/containers"
	"github.com/nats-io/nats.go/jetstream"
)

func TestExplorerConsumerLag(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	natsContainer := testcontainers.StartNATS(ctx, t)
	defer func() { _ = natsContainer.Cleanup(context.Background()) }()

	client := newIntegrationNATSClient(t, natsContainer.URL, "CDC_EXPLORER_CONSUMERS")
	defer client.Close()
	if err := client.CreateStream(ctx, []string{"cdc.>"}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	for i := 0; i < 3; i++ {
		publishExplorerMessage(t, ctx, client, fmt.Sprintf("cdc.src.public.orders.%d", i%2), map[string]string{
			constant.HeaderInstanceID: "src",
			constant.HeaderSchema:     "public",
			constant.HeaderTable:      "orders",
			constant.HeaderOp:         "c",
			constant.HeaderPartition:  fmt.Sprintf("%d", i%2),
		}, fmt.Sprintf(`{"op":"c","after":{"id":%d},"source":{"schema":"public","table":"orders"}}`, i+1))
	}

	consumer, err := client.CreateOrUpdateConsumer(ctx, "flow-explorer-lag", []string{"cdc.src.public.orders.>"})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatalf("fetch one message: %v", err)
	}
	for msg := range batch.Messages() {
		if err := msg.Ack(); err != nil {
			t.Fatalf("ack message: %v", err)
		}
		break
	}

	assertcdc.Eventually(t, 10*time.Second, 100*time.Millisecond, func() error {
		consumers, total, err := client.ListConsumers(ctx, 10, 1)
		if err != nil {
			return err
		}
		if total != 1 {
			return fmt.Errorf("consumer total = %d, want 1", total)
		}
		if len(consumers) != 1 {
			return fmt.Errorf("consumers = %d, want 1", len(consumers))
		}
		consumer := consumers[0]
		if consumer.Name != "flow-explorer-lag" {
			return fmt.Errorf("consumer name = %q", consumer.Name)
		}
		if len(consumer.FilterSubjects) != 1 || consumer.FilterSubjects[0] != "cdc.src.public.orders.>" {
			return fmt.Errorf("filter subjects = %+v", consumer.FilterSubjects)
		}
		if consumer.DeliveredStreamSeq == 0 {
			return fmt.Errorf("delivered stream sequence was not reported")
		}
		if consumer.AckFloorStreamSeq == 0 {
			return fmt.Errorf("ack floor stream sequence was not reported")
		}
		if consumer.NumPending == 0 {
			return fmt.Errorf("pending lag was not reported")
		}
		return nil
	})
}
