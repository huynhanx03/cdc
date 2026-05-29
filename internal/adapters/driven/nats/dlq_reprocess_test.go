package nats

import (
	"testing"
	"time"

	"github.com/foden/cdc/internal/core/ports"
)

func TestDLQReprocessIDDeterministic(t *testing.T) {
	id1 := deterministicReprocessID("src-123", 1)
	id2 := deterministicReprocessID("src-123", 1)
	if id1 != id2 {
		t.Fatalf("same original message and attempt produced different ids: %q != %q", id1, id2)
	}

	time.Sleep(time.Nanosecond)
	id3 := deterministicReprocessID("src-123", 1)
	if id1 != id3 {
		t.Fatalf("reprocess id changed over time: %q != %q", id1, id3)
	}

	nextAttempt := deterministicReprocessID("src-123", 2)
	if id1 == nextAttempt {
		t.Fatalf("different attempts produced same id: %q", id1)
	}
}

func TestBuildReprocessMsgUsesDeterministicID(t *testing.T) {
	env := DLQEnvelope{
		ID:              "dlq-1",
		MsgID:           "src-123",
		RetryCount:      2,
		OriginalSubject: "cdc.src.public.users.0",
		OriginalHeaders: map[string]string{
			"Nats-Msg-Id": "src-123",
		},
		Payload: []byte(`{"after":{"id":42}}`),
	}

	msg1, err := buildReprocessMsg(env)
	if err != nil {
		t.Fatalf("buildReprocessMsg failed: %v", err)
	}
	msg2, err := buildReprocessMsg(env)
	if err != nil {
		t.Fatalf("buildReprocessMsg failed: %v", err)
	}

	if msg1.Header.Get("Nats-Msg-Id") != msg2.Header.Get("Nats-Msg-Id") {
		t.Fatalf("reprocess id is not deterministic: %q != %q", msg1.Header.Get("Nats-Msg-Id"), msg2.Header.Get("Nats-Msg-Id"))
	}
	if msg1.Header.Get("Nats-Msg-Id") == "src-123" {
		t.Fatalf("reprocess reused original Nats-Msg-Id and can be deduped")
	}
}

func TestDLQEnvelopeMatchesSelectedID(t *testing.T) {
	env := DLQEnvelope{ID: "dlq-1", OriginalSubject: "cdc.src.public.orders.0", Reason: "sink_error"}

	if !dlqEnvelopeMatches(env, map[string]bool{"dlq-1": true}, ports.DLQFilter{}) {
		t.Fatal("selected DLQ envelope did not match")
	}
	if dlqEnvelopeMatches(env, map[string]bool{"dlq-2": true}, ports.DLQFilter{}) {
		t.Fatal("unselected DLQ envelope matched")
	}
}

func TestDLQEnvelopeMatchesFilter(t *testing.T) {
	env := DLQEnvelope{
		ID:              "dlq-1",
		OriginalSubject: "cdc.src.public.orders.0",
		SourceID:        "src",
		Schema:          "public",
		Table:           "orders",
		Op:              "u",
		Reason:          "sink timeout",
		ErrorClass:      "sink_error",
		OriginalHeaders: map[string]string{"trace-id": "abc"},
		Payload:         []byte(`{"after":{"status":"pending"}}`),
	}

	filter := ports.DLQFilter{
		OriginalTopic:  "cdc.src.public.orders",
		SourceID:       "src",
		Schema:         "public",
		Table:          "orders",
		Op:             "u",
		ReasonContains: "timeout",
		ErrorClass:     "sink_error",
		HeaderKey:      "trace-id",
		HeaderValue:    "abc",
		JSONPath:       "after.status",
		JSONEquals:     "pending",
	}

	if !dlqEnvelopeMatches(env, nil, filter) {
		t.Fatal("DLQ envelope should match filter")
	}
	filter.JSONEquals = "paid"
	if dlqEnvelopeMatches(env, nil, filter) {
		t.Fatal("DLQ envelope matched wrong JSON value")
	}
}
