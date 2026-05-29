package flow

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	sinkcommon "github.com/foden/cdc/internal/adapters/driven/connector/sink/common"
	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/domain"
	"github.com/foden/cdc/internal/core/ports"
	coreruntime "github.com/foden/cdc/internal/core/runtime"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/panjf2000/ants/v2"
)

type workerTestMsg struct {
	meta  *jetstream.MsgMetadata
	data  []byte
	acked bool
	naked bool
	calls *[]string
}

func (m *workerTestMsg) Metadata() (*jetstream.MsgMetadata, error) { return m.meta, nil }
func (m *workerTestMsg) Data() []byte {
	if len(m.data) > 0 {
		return m.data
	}
	return []byte(`{"ok":true}`)
}
func (m *workerTestMsg) Headers() nats.Header {
	return nats.Header{
		constant.HeaderOffset: []string{"42"},
		constant.HeaderSchema: []string{"public"},
		constant.HeaderTable:  []string{"users"},
		constant.HeaderOp:     []string{"c"},
	}
}
func (m *workerTestMsg) Subject() string { return "cdc.src.public.users.0" }
func (m *workerTestMsg) Reply() string   { return "" }
func (m *workerTestMsg) Ack() error {
	m.acked = true
	if m.calls != nil {
		*m.calls = append(*m.calls, "ack")
	}
	return nil
}
func (m *workerTestMsg) DoubleAck(context.Context) error  { return nil }
func (m *workerTestMsg) Nak() error                       { m.naked = true; return nil }
func (m *workerTestMsg) NakWithDelay(time.Duration) error { m.naked = true; return nil }
func (m *workerTestMsg) InProgress() error                { return nil }
func (m *workerTestMsg) Term() error                      { return nil }
func (m *workerTestMsg) TermWithReason(string) error      { return nil }

type workerTestNATS struct {
	dlqMoves int
	dlqErr   error
	dlqOpts  []ports.DLQMoveOptions
}

type failingPoolManager struct{}

func (f *failingPoolManager) CreatePool(string, int) (*ants.Pool, error) {
	return nil, errors.New("shared pool rejected")
}

func (f *failingPoolManager) ReleasePool(string) {}

func (n *workerTestNATS) PublishBatch(context.Context, func(*domain.Event) string, []*domain.Event) error {
	return nil
}
func (n *workerTestNATS) CreateOrUpdateConsumer(context.Context, string, []string) (jetstream.Consumer, error) {
	return nil, nil
}
func (n *workerTestNATS) DeleteConsumer(context.Context, string) error { return nil }
func (n *workerTestNATS) MoveToDLQ(_ context.Context, msg jetstream.Msg, opts ports.DLQMoveOptions) error {
	n.dlqMoves++
	n.dlqOpts = append(n.dlqOpts, opts)
	if n.dlqErr != nil {
		return n.dlqErr
	}
	return msg.Ack()
}
func (n *workerTestNATS) ReprocessDLQ(context.Context) (int, error) { return 0, nil }
func (n *workerTestNATS) PreviewDLQ(context.Context, []string, ports.DLQFilter, uint32) ([]ports.DLQPreviewItem, error) {
	return nil, nil
}
func (n *workerTestNATS) ReprocessDLQSelected(context.Context, []string, ports.DLQFilter, uint32) (ports.DLQReprocessResult, error) {
	return ports.DLQReprocessResult{}, nil
}
func (n *workerTestNATS) ListMessages(context.Context, domain.MessageStatus, int, int, string, string) ([]*ports.NATSMessageItem, uint64, error) {
	return nil, 0, nil
}
func (n *workerTestNATS) ListMessagesWithFilter(context.Context, domain.MessageStatus, int, int, ports.NATSMessageFilter) ([]*ports.NATSMessageItem, uint64, error) {
	return nil, 0, nil
}
func (n *workerTestNATS) ListDLQMessages(context.Context, int, int) ([]*ports.NATSMessageItem, uint64, error) {
	return nil, 0, nil
}
func (n *workerTestNATS) ListTopics(context.Context, int, int) ([]string, uint64, error) {
	return nil, 0, nil
}
func (n *workerTestNATS) ListPartitions(context.Context, string, int, int) ([]string, uint64, error) {
	return nil, 0, nil
}
func (n *workerTestNATS) ListConsumers(context.Context, int, int) ([]ports.NATSConsumerSummary, uint64, error) {
	return nil, 0, nil
}
func (n *workerTestNATS) CreateStream(context.Context, []string) error { return nil }
func (n *workerTestNATS) CreateDLQStream(context.Context) error        { return nil }
func (n *workerTestNATS) Health(context.Context) error                 { return nil }
func (n *workerTestNATS) Close()                                       {}

func TestStartFlowWorkerReturnsErrorWhenFallbackPoolCreationFails(t *testing.T) {
	originalNewAntsPool := newAntsPool
	newAntsPool = func(int, ...ants.Option) (*ants.Pool, error) {
		return nil, errors.New("isolated pool failed")
	}
	t.Cleanup(func() { newAntsPool = originalNewAntsPool })

	worker, err := StartFlowWorker(
		context.Background(),
		&FlowConfig{FlowID: "flow-1", SourceTable: "public.users", SinkTable: "public.users"},
		nil,
		&failingPoolManager{},
		nil,
		&workerTestNATS{},
		3,
		&coreruntime.Metrics{},
	)
	if err == nil {
		t.Fatal("expected pool creation error")
	}
	if worker != nil {
		t.Fatal("worker should be nil when pool creation fails")
	}
}

func TestHandleFailureUsesConfiguredMaxDeliver(t *testing.T) {
	natsClient := &workerTestNATS{}
	worker := &FlowWorker{
		flow:       &FlowConfig{FlowID: "flow-1", SinkID: "sink-1"},
		natsClient: natsClient,
		maxDeliver: 3,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	retryMsg := &workerTestMsg{meta: &jetstream.MsgMetadata{NumDelivered: 2}}
	worker.handleFailure(context.Background(), []jetstream.Msg{retryMsg}, nil, errors.New("retryable"))
	if !retryMsg.naked {
		t.Fatal("message below maxDeliver should be NAKed")
	}
	if natsClient.dlqMoves != 0 {
		t.Fatalf("DLQ moves = %d, want 0", natsClient.dlqMoves)
	}

	dlqMsg := &workerTestMsg{meta: &jetstream.MsgMetadata{NumDelivered: 3}}
	worker.handleFailure(context.Background(), []jetstream.Msg{dlqMsg}, nil, errors.New("retryable"))
	if natsClient.dlqMoves != 1 {
		t.Fatalf("DLQ moves = %d, want 1", natsClient.dlqMoves)
	}
}

func TestHandleFailureMovesNonRetryableSinkErrorToDLQImmediately(t *testing.T) {
	natsClient := &workerTestNATS{}
	worker := &FlowWorker{
		flow:       &FlowConfig{FlowID: "flow-1", SourceID: "source-1", SinkID: "sink-1"},
		natsClient: natsClient,
		maxDeliver: 3,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	msg := &workerTestMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}
	ev := &domain.Event{Schema: "public", Table: "users", Op: constant.OpCreate, MessageID: "msg-1"}
	err := sinkcommon.PermanentError(sinkcommon.ReasonMissingMetadata, errors.New("missing pk"))

	worker.handleFailure(context.Background(), []jetstream.Msg{msg}, []*domain.Event{ev}, err)

	if natsClient.dlqMoves != 1 {
		t.Fatalf("DLQ moves = %d, want 1", natsClient.dlqMoves)
	}
	if msg.naked {
		t.Fatal("non-retryable sink error should not be NAKed after DLQ move")
	}
	if got := natsClient.dlqOpts[0]; got.FlowID != "flow-1" || got.SourceID != "source-1" || got.SinkID != "sink-1" || got.Schema != "public" || got.Table != "users" || got.Op != "c" || got.MsgID != "msg-1" || got.RetryCount != 1 {
		t.Fatalf("DLQ opts = %+v", got)
	}
}

func TestHandleFailureDoesNotAckWhenMoveToDLQFails(t *testing.T) {
	natsClient := &workerTestNATS{dlqErr: errors.New("dlq down")}
	worker := &FlowWorker{
		flow:       &FlowConfig{FlowID: "flow-1", SourceID: "source-1", SinkID: "sink-1"},
		natsClient: natsClient,
		maxDeliver: 3,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	msg := &workerTestMsg{meta: &jetstream.MsgMetadata{NumDelivered: 3}}

	worker.handleFailure(context.Background(), []jetstream.Msg{msg}, nil, errors.New("retryable"))

	if msg.acked {
		t.Fatal("message acked when DLQ move failed")
	}
	if !msg.naked {
		t.Fatal("message should be NAKed when DLQ move failed")
	}
}

func TestApplySinkTableUsesFlowSinkTable(t *testing.T) {
	event := &domain.Event{Schema: "public", Table: "users"}

	applySinkTable(event, "warehouse.customers")

	if event.Schema != "warehouse" || event.Table != "customers" {
		t.Fatalf("event table = %s.%s", event.Schema, event.Table)
	}
}

func TestApplySinkTableClearsSchemaWhenSinkTableUnqualified(t *testing.T) {
	event := &domain.Event{Schema: "public", Table: "users"}

	applySinkTable(event, "customers")

	if event.Schema != "" || event.Table != "customers" {
		t.Fatalf("event table = %s.%s", event.Schema, event.Table)
	}
}

type orderStore struct {
	*mockStore
	calls      []string
	err        error
	checkpoint *domain.Checkpoint
}

func (s *orderStore) SaveCheckpoint(_ context.Context, checkpoint *domain.Checkpoint) error {
	s.calls = append(s.calls, "checkpoint")
	s.checkpoint = checkpoint
	return s.err
}

func newOrderStore() *orderStore {
	return &orderStore{mockStore: newMockStore()}
}

type orderSink struct {
	calls *[]string
	err   error
}

func (s orderSink) WriteBatch(context.Context, []*domain.Event) error {
	*s.calls = append(*s.calls, "sink")
	return s.err
}
func (s orderSink) Close() error       { return nil }
func (s orderSink) InstanceID() string { return "sink-1" }
func (s orderSink) Type() string       { return "postgres" }

type isolatingSink struct {
	calls int
}

func (s *isolatingSink) WriteBatch(_ context.Context, events []*domain.Event) error {
	s.calls++
	for _, event := range events {
		if string(event.Data) == `{"bad":true}` {
			return sinkcommon.PermanentError(sinkcommon.ReasonInvalidRecord, errors.New("bad row"))
		}
	}
	return nil
}
func (s *isolatingSink) Close() error       { return nil }
func (s *isolatingSink) InstanceID() string { return "sink-1" }
func (s *isolatingSink) Type() string       { return "postgres" }

func TestProcessBatchAcksBeforeSavingCheckpoint(t *testing.T) {
	store := newOrderStore()
	sinkCalls := &store.calls
	worker := &FlowWorker{
		flow:       &FlowConfig{FlowID: "flow-1", SourceID: "src-1", SinkID: "sink-1", SinkTable: "public.users"},
		sink:       orderSink{calls: sinkCalls},
		store:      store,
		natsClient: &workerTestNATS{},
		maxDeliver: 3,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	msg := &workerTestMsg{calls: sinkCalls}

	worker.processBatch(context.Background(), []jetstream.Msg{msg})

	if !msg.acked {
		t.Fatal("message not acked after checkpoint")
	}
	want := []string{"sink", "ack", "checkpoint"}
	if len(store.calls) != len(want) {
		t.Fatalf("calls = %v, want %v", store.calls, want)
	}
	for i := range want {
		if store.calls[i] != want[i] {
			t.Fatalf("calls = %v, want %v", store.calls, want)
		}
	}
	if store.checkpoint == nil {
		t.Fatal("checkpoint was not saved")
	}
	if store.checkpoint.Schema != "public" || store.checkpoint.Table != "users" || store.checkpoint.Partition != 0 {
		t.Fatalf("checkpoint metadata = %+v", store.checkpoint)
	}
}

func TestProcessBatchBisectsNonRetryableSinkBatchFailure(t *testing.T) {
	store := newOrderStore()
	natsClient := &workerTestNATS{}
	sink := &isolatingSink{}
	worker := &FlowWorker{
		flow:           &FlowConfig{FlowID: "flow-1", SourceID: "src-1", SinkID: "sink-1", SinkTable: "public.users"},
		sink:           sink,
		store:          store,
		natsClient:     natsClient,
		runtimeMetrics: coreruntime.NewMetrics(),
		maxDeliver:     3,
		log:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	msgs := []jetstream.Msg{
		&workerTestMsg{data: []byte(`{"ok":1}`), meta: &jetstream.MsgMetadata{NumDelivered: 1}},
		&workerTestMsg{data: []byte(`{"bad":true}`), meta: &jetstream.MsgMetadata{NumDelivered: 1}},
		&workerTestMsg{data: []byte(`{"ok":3}`), meta: &jetstream.MsgMetadata{NumDelivered: 1}},
	}

	worker.processBatch(context.Background(), msgs)

	if sink.calls < 3 {
		t.Fatalf("sink calls = %d, want bisection retries", sink.calls)
	}
	if natsClient.dlqMoves != 1 {
		t.Fatalf("DLQ moves = %d, want 1", natsClient.dlqMoves)
	}
	for i, msg := range msgs {
		if !msg.(*workerTestMsg).acked {
			t.Fatalf("msg %d not acked", i)
		}
	}
}

func TestProcessBatchKeepsAckWhenCheckpointFailsAfterSinkSuccess(t *testing.T) {
	store := newOrderStore()
	store.err = errors.New("checkpoint failed")
	sinkCalls := &store.calls
	worker := &FlowWorker{
		flow:       &FlowConfig{FlowID: "flow-1", SourceID: "src-1", SinkID: "sink-1", SinkTable: "public.users"},
		sink:       orderSink{calls: sinkCalls},
		store:      store,
		natsClient: &workerTestNATS{},
		maxDeliver: 3,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	msg := &workerTestMsg{calls: sinkCalls}

	worker.processBatch(context.Background(), []jetstream.Msg{msg})

	if !msg.acked {
		t.Fatal("message not acked after sink success")
	}
	want := []string{"sink", "ack", "checkpoint"}
	if len(store.calls) != len(want) {
		t.Fatalf("calls = %v, want %v", store.calls, want)
	}
	for i := range want {
		if store.calls[i] != want[i] {
			t.Fatalf("calls = %v, want %v", store.calls, want)
		}
	}
}
