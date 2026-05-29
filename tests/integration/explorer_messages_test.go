//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/foden/cdc/config"
	natsadapter "github.com/foden/cdc/internal/adapters/driven/nats"
	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/domain"
	"github.com/foden/cdc/internal/core/ports"
	testcontainers "github.com/foden/cdc/internal/testutil/containers"
	gnats "github.com/nats-io/nats.go"
)

func TestExplorerMessageSearch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	natsContainer := testcontainers.StartNATS(ctx, t)
	defer func() { _ = natsContainer.Cleanup(context.Background()) }()

	client := newIntegrationNATSClient(t, natsContainer.URL, "CDC_EXPLORER_MESSAGES")
	defer client.Close()
	if err := client.CreateStream(ctx, []string{"cdc.>"}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	publishExplorerMessage(t, ctx, client, "cdc.src.public.orders.0", map[string]string{
		constant.HeaderInstanceID: "src",
		constant.HeaderSchema:     "public",
		constant.HeaderTable:      "orders",
		constant.HeaderOp:         "c",
		constant.HeaderPartition:  "0",
	}, `{"op":"c","after":{"id":1,"status":"paid"},"source":{"schema":"public","table":"orders"}}`)
	publishExplorerMessage(t, ctx, client, "cdc.src.public.orders.1", map[string]string{
		constant.HeaderInstanceID: "src",
		constant.HeaderSchema:     "public",
		constant.HeaderTable:      "orders",
		constant.HeaderOp:         "u",
		constant.HeaderPartition:  "1",
	}, `{"op":"u","after":{"id":2,"status":"pending"},"source":{"schema":"public","table":"orders"}}`)
	publishExplorerMessage(t, ctx, client, "cdc.src.public.customers.0", map[string]string{
		constant.HeaderInstanceID: "src",
		constant.HeaderSchema:     "public",
		constant.HeaderTable:      "customers",
		constant.HeaderOp:         "d",
		constant.HeaderPartition:  "0",
	}, `{"op":"d","before":{"id":7,"tier":"gold"},"source":{"schema":"public","table":"customers"}}`)

	cases := []struct {
		name        string
		filter      ports.NATSMessageFilter
		wantTotal   uint64
		wantSubject string
	}{
		{
			name: "json path and topic",
			filter: ports.NATSMessageFilter{
				Topic:      "cdc.src.public.orders",
				JSONPath:   "after.status",
				JSONEquals: "paid",
			},
			wantTotal:   1,
			wantSubject: "cdc.src.public.orders.0",
		},
		{
			name: "header table and hard limit",
			filter: ports.NATSMessageFilter{
				HeaderKey:   constant.HeaderTable,
				HeaderValue: "orders",
			},
			wantTotal:   2,
			wantSubject: "cdc.src.public.orders.0",
		},
		{
			name: "operation delete",
			filter: ports.NATSMessageFilter{
				Op: "d",
			},
			wantTotal:   1,
			wantSubject: "cdc.src.public.customers.0",
		},
		{
			name: "partition",
			filter: ports.NATSMessageFilter{
				Topic:     "cdc.src.public.orders",
				Partition: "1",
			},
			wantTotal:   1,
			wantSubject: "cdc.src.public.orders.1",
		},
		{
			name: "partition op and json path",
			filter: ports.NATSMessageFilter{
				Topic:      "cdc.src.public.orders",
				Partition:  "1",
				Op:         "u",
				JSONPath:   "after.status",
				JSONEquals: "pending",
			},
			wantTotal:   1,
			wantSubject: "cdc.src.public.orders.1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			messages, total, err := client.ListMessagesWithFilter(ctx, domain.MessageStatusAll, 1, 1, tc.filter)
			if err != nil {
				t.Fatalf("list messages: %v", err)
			}
			if total != tc.wantTotal {
				t.Fatalf("total = %d, want %d", total, tc.wantTotal)
			}
			if len(messages) != 1 {
				t.Fatalf("messages = %d, want 1", len(messages))
			}
			if messages[0].Subject != tc.wantSubject {
				t.Fatalf("subject = %q, want %q", messages[0].Subject, tc.wantSubject)
			}
		})
	}
}

func newIntegrationNATSClient(t *testing.T, url string, streamName string) *natsadapter.Client {
	t.Helper()

	client, err := natsadapter.NewClient(&config.NATSConfig{
		URL:                   url,
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

func publishExplorerMessage(t *testing.T, ctx context.Context, client *natsadapter.Client, subject string, headers map[string]string, payload string) {
	t.Helper()

	msg := &gnats.Msg{
		Subject: subject,
		Data:    []byte(payload),
		Header:  make(gnats.Header),
	}
	for key, value := range headers {
		msg.Header.Set(key, value)
	}
	if _, err := client.JetStream().PublishMsg(ctx, msg); err != nil {
		t.Fatalf("publish %s: %v", subject, err)
	}
}
