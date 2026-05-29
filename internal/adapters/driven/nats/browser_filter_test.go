package nats

import (
	"testing"

	"github.com/foden/cdc/internal/core/constant"
)

func TestExplorerMessageFilterMatchesCDCMetadataAndPayload(t *testing.T) {
	message := &MessageItem{
		Sequence:  42,
		Timestamp: 1_700_000_000_000,
		Subject:   "cdc.src.public.orders.0",
		Headers: map[string]string{
			constant.HeaderInstanceID: "src",
			constant.HeaderSchema:     "public",
			constant.HeaderTable:      "orders",
			constant.HeaderOp:         "c",
			constant.HeaderPartition:  "0",
			"content-type":            "application/json",
		},
		Data: []byte(`{"op":"c","after":{"id":1001,"status":"paid"},"source":{"schema":"public","table":"orders"}}`),
	}

	filter := ExplorerMessageFilter{
		Topic:         "cdc.src.public.orders",
		Partition:     "0",
		MinSequence:   40,
		MaxSequence:   45,
		FromTimestamp: 1_699_999_999_999,
		ToTimestamp:   1_700_000_000_001,
		HeaderKey:     "content-type",
		HeaderValue:   "application/json",
		TextContains:  "paid",
		JSONPath:      "after.status",
		JSONEquals:    "paid",
		Op:            "c",
		SourceID:      "src",
		Schema:        "public",
		Table:         "orders",
		SubjectPrefix: "cdc.src.public.orders",
	}

	if !filter.Matches(message) {
		t.Fatalf("filter should match CDC message metadata and payload")
	}
}

func TestExplorerMessageFilterRejectsMismatches(t *testing.T) {
	message := &MessageItem{
		Sequence:  42,
		Timestamp: 1_700_000_000_000,
		Subject:   "cdc.src.public.orders.0",
		Headers: map[string]string{
			constant.HeaderInstanceID: "src",
			constant.HeaderSchema:     "public",
			constant.HeaderTable:      "orders",
			constant.HeaderOp:         "c",
			constant.HeaderPartition:  "0",
		},
		Data: []byte(`{"op":"c","after":{"id":1001,"status":"paid"},"source":{"schema":"public","table":"orders"}}`),
	}

	cases := map[string]ExplorerMessageFilter{
		"wrong json value":  {JSONPath: "after.status", JSONEquals: "cancelled"},
		"missing json path": {JSONPath: "after.total", JSONEquals: "10"},
		"wrong sequence":    {MinSequence: 43},
		"wrong header":      {HeaderKey: constant.HeaderSchema, HeaderValue: "inventory"},
		"wrong op":          {Op: "d"},
		"wrong source":      {SourceID: "other"},
		"wrong table":       {Table: "payments"},
		"wrong partition":   {Topic: "cdc.src.public.orders", Partition: "1"},
		"wrong text":        {TextContains: "refunded"},
	}

	for name, filter := range cases {
		t.Run(name, func(t *testing.T) {
			if filter.Matches(message) {
				t.Fatalf("filter unexpectedly matched")
			}
		})
	}
}

func TestExplorerFilterSubjectUsesTopicAndPartition(t *testing.T) {
	if got := (ExplorerMessageFilter{Topic: "src.public.orders"}).NATSFilterSubject(); got != "cdc.src.public.orders.>" {
		t.Fatalf("topic filter = %q", got)
	}
	if got := (ExplorerMessageFilter{Topic: "cdc.src.public.orders", Partition: "0"}).NATSFilterSubject(); got != "cdc.src.public.orders.0" {
		t.Fatalf("topic partition filter = %q", got)
	}
	if got := (ExplorerMessageFilter{SubjectPrefix: "cdc.src.public.orders"}).NATSFilterSubject(); got != "cdc.src.public.orders.>" {
		t.Fatalf("subject prefix filter = %q", got)
	}
}
