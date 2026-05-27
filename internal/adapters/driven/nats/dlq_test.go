package nats

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/foden/cdc/internal/core/ports"
	cdcerrors "github.com/foden/cdc/pkg/errors"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type fakeJetStreamMsg struct {
	subject string
	data    []byte
	headers nats.Header
	meta    *jetstream.MsgMetadata
	acked   bool
	naked   bool
	termed  bool
}

func (m *fakeJetStreamMsg) Metadata() (*jetstream.MsgMetadata, error) { return m.meta, nil }
func (m *fakeJetStreamMsg) Data() []byte                              { return m.data }
func (m *fakeJetStreamMsg) Headers() nats.Header                      { return m.headers }
func (m *fakeJetStreamMsg) Subject() string                           { return m.subject }
func (m *fakeJetStreamMsg) Reply() string                             { return "" }
func (m *fakeJetStreamMsg) Ack() error                                { m.acked = true; return nil }
func (m *fakeJetStreamMsg) DoubleAck(context.Context) error           { m.acked = true; return nil }
func (m *fakeJetStreamMsg) Nak() error                                { m.naked = true; return nil }
func (m *fakeJetStreamMsg) NakWithDelay(time.Duration) error          { m.naked = true; return nil }
func (m *fakeJetStreamMsg) InProgress() error                         { return nil }
func (m *fakeJetStreamMsg) Term() error                               { m.termed = true; return nil }
func (m *fakeJetStreamMsg) TermWithReason(string) error               { m.termed = true; return nil }

func TestBuildDLQEnvelopePreservesOriginalMessage(t *testing.T) {
	msg := &fakeJetStreamMsg{
		subject: "cdc.src.public.users.0",
		data:    []byte(`{"after":{"id":42}}`),
		headers: nats.Header{
			"cdc-instance-id": []string{"src"},
			"cdc-schema":      []string{"public"},
			"cdc-table":       []string{"users"},
			"cdc-op":          []string{"c"},
			"Nats-Msg-Id":     []string{"src-123"},
		},
		meta: &jetstream.MsgMetadata{NumDelivered: 7},
	}

	env, err := buildDLQEnvelope(msg, ports.DLQMoveOptions{
		FlowID:     "flow-1",
		SourceID:   "source-override",
		SinkID:     "sink-1",
		Schema:     "warehouse",
		Table:      "users_archive",
		Op:         "u",
		MsgID:      "event-1",
		Reason:     "sink_error: duplicate key",
		ErrorClass: cdcerrors.DLQErrorSink,
		RetryCount: 3,
		Timestamp:  123456,
	})
	if err != nil {
		t.Fatalf("buildDLQEnvelope failed: %v", err)
	}

	if env.OriginalSubject != msg.subject {
		t.Fatalf("OriginalSubject = %q, want %q", env.OriginalSubject, msg.subject)
	}
	if string(env.Payload) != string(msg.data) {
		t.Fatalf("Payload = %s, want %s", env.Payload, msg.data)
	}
	if env.OriginalHeaders["Nats-Msg-Id"] != "src-123" {
		t.Fatalf("Nats-Msg-Id header was not preserved: %+v", env.OriginalHeaders)
	}
	if env.SourceID != "source-override" || env.Schema != "warehouse" || env.Table != "users_archive" || env.Op != "u" || env.MsgID != "event-1" {
		t.Fatalf("source routing metadata not preserved: %+v", env)
	}
	if env.DeliveryCount != 7 || env.RetryCount != 3 || env.FailedAt != 123456 {
		t.Fatalf("retry metadata not preserved: %+v", env)
	}
	if env.Reason != "sink_error: duplicate key" || env.ErrorClass != cdcerrors.DLQErrorSink {
		t.Fatalf("failure metadata not preserved: %+v", env)
	}
}

func TestBuildReprocessMsgPublishesOriginalPayloadToOriginalSubject(t *testing.T) {
	env := DLQEnvelope{
		ID:              "dlq-1",
		OriginalSubject: "cdc.src.public.users.0",
		OriginalHeaders: map[string]string{
			"cdc-instance-id": "src",
			"Nats-Msg-Id":     "src-123",
		},
		Payload: json.RawMessage(`{"after":{"id":42}}`),
	}

	msg, err := buildReprocessMsg(env)
	if err != nil {
		t.Fatalf("buildReprocessMsg failed: %v", err)
	}

	if msg.Subject != env.OriginalSubject {
		t.Fatalf("Subject = %q, want %q", msg.Subject, env.OriginalSubject)
	}
	if string(msg.Data) != string(env.Payload) {
		t.Fatalf("Data = %s, want %s", msg.Data, env.Payload)
	}
	if msg.Header.Get("cdc-instance-id") != "src" {
		t.Fatalf("original header not preserved: %+v", msg.Header)
	}
	if msg.Header.Get("X-DLQ-Reprocessed-From") != "dlq-1" {
		t.Fatalf("missing reprocess header: %+v", msg.Header)
	}
	if msg.Header.Get("Nats-Msg-Id") == "src-123" {
		t.Fatalf("reprocess reused original Nats-Msg-Id and can be deduped")
	}
}
