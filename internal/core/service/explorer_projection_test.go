package service

import (
	"reflect"
	"testing"

	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/ports"
)

func TestParseCDCSubject(t *testing.T) {
	parsed := ParseCDCSubject("cdc.src.public.orders.3")

	if parsed.Topic != "cdc.src.public.orders" {
		t.Fatalf("topic = %q, want cdc.src.public.orders", parsed.Topic)
	}
	if parsed.SourceID != "src" || parsed.Schema != "public" || parsed.Table != "orders" || parsed.Partition != "3" {
		t.Fatalf("parsed subject = %+v", parsed)
	}
}

func TestProjectMessageItemExtractsMetadata(t *testing.T) {
	item := &ports.NATSMessageItem{
		Sequence:  42,
		Timestamp: 1710000000000,
		Subject:   "cdc.src.public.orders.1",
		Data:      []byte(`{"op":"u","before":{"id":7,"status":"new"},"after":{"id":7,"status":"paid"},"source":{"schema":"public","table":"orders"}}`),
		Headers: map[string]string{
			constant.HeaderOp:        "u",
			"Nats-Msg-Id":            "orders-7",
			"X-DLQ-Reprocessed-From": "dlq-1",
		},
	}

	projected := ProjectMessageItem(item)

	if projected.Topic != "cdc.src.public.orders" {
		t.Fatalf("topic = %q", projected.Topic)
	}
	if projected.Partition != "1" || projected.Op != "u" || projected.Key != "orders-7" {
		t.Fatalf("metadata = partition:%q op:%q key:%q", projected.Partition, projected.Op, projected.Key)
	}
	if projected.PayloadSize != uint64(len(item.Data)) || projected.HeaderCount != 3 {
		t.Fatalf("size/header count = %d/%d", projected.PayloadSize, projected.HeaderCount)
	}
	if !projected.IsReprocessed || projected.ReprocessedFrom != "dlq-1" {
		t.Fatalf("reprocess metadata = %+v", projected)
	}
	if !reflect.DeepEqual(projected.ChangedFields, []string{"status"}) {
		t.Fatalf("changed fields = %+v, want [status]", projected.ChangedFields)
	}
}

func TestChangedFields(t *testing.T) {
	fields := ChangedFields(
		[]byte(`{"id":1,"status":"new","amount":10}`),
		[]byte(`{"id":1,"status":"paid","amount":10,"updated_at":"2026-05-28T00:00:00Z"}`),
	)

	want := []string{"status", "updated_at"}
	if !reflect.DeepEqual(fields, want) {
		t.Fatalf("fields = %+v, want %+v", fields, want)
	}
}
