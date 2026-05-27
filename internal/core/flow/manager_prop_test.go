package flow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/domain"
	"github.com/foden/cdc/internal/core/ports"
	"github.com/nats-io/nats.go/jetstream"
	"pgregory.net/rapid"
)

// --- Mock implementations for testing ---

// mockStore is a simple in-memory store implementing ports.Store.
type mockStore struct {
	sources       map[string]*ports.SourceConfig
	sinks         map[string]*ports.SinkConfig
	flows         map[string]*ports.FlowConfig
	offsets       map[string]string
	checkpoints   map[string]*domain.Checkpoint
	sourceOffsets map[string]string
}

func newMockStore() *mockStore {
	return &mockStore{
		sources:       make(map[string]*ports.SourceConfig),
		sinks:         make(map[string]*ports.SinkConfig),
		flows:         make(map[string]*ports.FlowConfig),
		offsets:       make(map[string]string),
		checkpoints:   make(map[string]*domain.Checkpoint),
		sourceOffsets: make(map[string]string),
	}
}

func (s *mockStore) PutSource(_ context.Context, cfg *ports.SourceConfig) error {
	s.sources[cfg.InstanceID] = cfg
	return nil
}
func (s *mockStore) GetSource(_ context.Context, id string) (*ports.SourceConfig, error) {
	return s.sources[id], nil
}
func (s *mockStore) DeleteSource(_ context.Context, id string) error {
	delete(s.sources, id)
	return nil
}
func (s *mockStore) ListSources(_ context.Context) ([]*ports.SourceConfig, error) {
	var result []*ports.SourceConfig
	for _, v := range s.sources {
		result = append(result, v)
	}
	return result, nil
}
func (s *mockStore) PutSink(_ context.Context, cfg *ports.SinkConfig) error {
	s.sinks[cfg.InstanceID] = cfg
	return nil
}
func (s *mockStore) GetSink(_ context.Context, id string) (*ports.SinkConfig, error) {
	return s.sinks[id], nil
}
func (s *mockStore) DeleteSink(_ context.Context, id string) error {
	delete(s.sinks, id)
	return nil
}
func (s *mockStore) ListSinks(_ context.Context) ([]*ports.SinkConfig, error) {
	var result []*ports.SinkConfig
	for _, v := range s.sinks {
		result = append(result, v)
	}
	return result, nil
}
func (s *mockStore) PutFlow(_ context.Context, cfg *ports.FlowConfig) error {
	s.flows[cfg.FlowID] = cfg
	return nil
}
func (s *mockStore) GetFlow(_ context.Context, id string) (*ports.FlowConfig, error) {
	return s.flows[id], nil
}
func (s *mockStore) DeleteFlow(_ context.Context, id string) error {
	delete(s.flows, id)
	return nil
}
func (s *mockStore) ListFlows(_ context.Context) ([]*ports.FlowConfig, error) {
	var result []*ports.FlowConfig
	for _, v := range s.flows {
		result = append(result, v)
	}
	return result, nil
}
func (s *mockStore) SaveOffset(_ context.Context, flowID string, offset string) error {
	s.offsets[flowID] = offset
	return nil
}
func (s *mockStore) GetOffset(_ context.Context, flowID string) (string, error) {
	return s.offsets[flowID], nil
}
func (s *mockStore) SaveCheckpoint(_ context.Context, checkpoint *domain.Checkpoint) error {
	s.checkpoints[checkpoint.FlowID] = checkpoint
	return nil
}
func (s *mockStore) GetCheckpoint(_ context.Context, flowID string) (*domain.Checkpoint, error) {
	return s.checkpoints[flowID], nil
}
func (s *mockStore) SaveSourceOffset(_ context.Context, sourceID string, offset string) error {
	s.sourceOffsets[sourceID] = offset
	return nil
}
func (s *mockStore) GetSourceOffset(_ context.Context, sourceID string) (string, error) {
	return s.sourceOffsets[sourceID], nil
}

// mockRegistry implements ports.Registry for testing.
type mockRegistry struct {
	source ports.Source
}

func (r *mockRegistry) RegisterSource(_ string, _ ports.SourceFactory) {}
func (r *mockRegistry) RegisterSink(_ string, _ ports.SinkFactory)     {}
func (r *mockRegistry) CreateSource(_ *ports.SourceConfig) (ports.Source, error) {
	if r.source != nil {
		return r.source, nil
	}
	return &mockSource{}, nil
}
func (r *mockRegistry) CreateSink(_ *ports.SinkConfig) (ports.Sink, error) {
	return &mockSink{}, nil
}
func (r *mockRegistry) SourceNames() []string { return nil }
func (r *mockRegistry) SinkNames() []string   { return nil }

// mockSink implements ports.Sink for testing.
type mockSink struct{}

func (s *mockSink) WriteBatch(_ context.Context, _ []*domain.Event) error { return nil }
func (s *mockSink) Close() error                                          { return nil }
func (s *mockSink) InstanceID() string                                    { return "mock-sink" }
func (s *mockSink) Type() string                                          { return "mock" }

// mockNATSClient implements ports.NATSClient for testing.
type mockNATSClient struct {
	mu        sync.Mutex
	published []*domain.Event
	publishCh chan []*domain.Event
}

func (n *mockNATSClient) PublishBatch(_ context.Context, _ func(*domain.Event) string, events []*domain.Event) error {
	clones := make([]*domain.Event, 0, len(events))
	for _, ev := range events {
		clones = append(clones, ev.DeepClone())
	}
	n.mu.Lock()
	n.published = append(n.published, clones...)
	n.mu.Unlock()
	if n.publishCh != nil {
		n.publishCh <- clones
	}
	return nil
}
func (n *mockNATSClient) CreateOrUpdateConsumer(_ context.Context, _ string, _ []string) (jetstream.Consumer, error) {
	return &mockConsumer{}, nil
}
func (n *mockNATSClient) DeleteConsumer(_ context.Context, _ string) error { return nil }
func (n *mockNATSClient) MoveToDLQ(_ context.Context, _ jetstream.Msg, _ ports.DLQMoveOptions) error {
	return nil
}
func (n *mockNATSClient) ReprocessDLQ(_ context.Context) (int, error) {
	return 0, nil
}
func (n *mockNATSClient) ListMessages(_ context.Context, _ domain.MessageStatus, _ int, _ int, _ string, _ string) ([]*ports.NATSMessageItem, uint64, error) {
	return nil, 0, nil
}
func (n *mockNATSClient) ListDLQMessages(_ context.Context, _ int, _ int) ([]*ports.NATSMessageItem, uint64, error) {
	return nil, 0, nil
}
func (n *mockNATSClient) ListTopics(_ context.Context, _ int, _ int) ([]string, uint64, error) {
	return nil, 0, nil
}
func (n *mockNATSClient) ListPartitions(_ context.Context, _ string, _ int, _ int) ([]string, uint64, error) {
	return nil, 0, nil
}
func (n *mockNATSClient) ListConsumers(_ context.Context, _ int, _ int) ([]ports.NATSConsumerSummary, uint64, error) {
	return nil, 0, nil
}
func (n *mockNATSClient) CreateStream(_ context.Context, _ []string) error { return nil }
func (n *mockNATSClient) CreateDLQStream(_ context.Context) error          { return nil }
func (n *mockNATSClient) Health(_ context.Context) error                   { return nil }
func (n *mockNATSClient) Close()                                           {}

type mockSource struct {
	started       chan struct{}
	stopCh        chan struct{}
	events        []*domain.Event
	initialOffset string
	syncedTables  []ports.SourceTableRef
}

func (s *mockSource) Start(events chan<- *domain.Event, _ <-chan ports.SourceAck, initialOffset string) error {
	s.initialOffset = initialOffset
	if s.started != nil {
		close(s.started)
	}
	for _, ev := range s.events {
		events <- ev
	}
	return nil
}
func (s *mockSource) Stop() error {
	if s.stopCh != nil {
		close(s.stopCh)
	}
	return nil
}
func (s *mockSource) InstanceID() string { return "src-1" }
func (s *mockSource) SyncSourceTables(_ context.Context, tables []ports.SourceTableRef) error {
	s.syncedTables = append([]ports.SourceTableRef(nil), tables...)
	return nil
}

// mockConsumer implements jetstream.Consumer for testing.
type mockConsumer struct{}

func (c *mockConsumer) Fetch(batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return &mockMessageBatch{}, nil
}
func (c *mockConsumer) FetchBytes(maxBytes int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return &mockMessageBatch{}, nil
}
func (c *mockConsumer) FetchNoWait(batch int) (jetstream.MessageBatch, error) {
	return &mockMessageBatch{}, nil
}
func (c *mockConsumer) Consume(handler jetstream.MessageHandler, opts ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	return nil, nil
}
func (c *mockConsumer) Messages(opts ...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	return nil, nil
}
func (c *mockConsumer) Next(opts ...jetstream.FetchOpt) (jetstream.Msg, error) {
	return nil, context.DeadlineExceeded
}
func (c *mockConsumer) Info(ctx context.Context) (*jetstream.ConsumerInfo, error) {
	return &jetstream.ConsumerInfo{}, nil
}
func (c *mockConsumer) CachedInfo() *jetstream.ConsumerInfo {
	return &jetstream.ConsumerInfo{}
}

// mockMessageBatch implements jetstream.MessageBatch for testing.
type mockMessageBatch struct{}

func (b *mockMessageBatch) Messages() <-chan jetstream.Msg {
	ch := make(chan jetstream.Msg)
	close(ch)
	return ch
}
func (b *mockMessageBatch) Error() error { return nil }

// mockDiscovery implements ports.Discovery for testing.
type mockDiscovery struct{}

func (d *mockDiscovery) TestSourceConnection(_ context.Context, _ *ports.SourceConfig) (int64, error) {
	return 0, nil
}
func (d *mockDiscovery) TestSinkConnection(_ context.Context, _ *ports.SinkConfig) (int64, error) {
	return 0, nil
}
func (d *mockDiscovery) DiscoverSourceTables(_ context.Context, _ *ports.SourceConfig) ([]ports.TableInfo, error) {
	return nil, nil
}
func (d *mockDiscovery) DiscoverSinkTables(_ context.Context, _ *ports.SinkConfig) ([]ports.TableInfo, error) {
	return nil, nil
}

// --- Helper to create a test Manager ---

func newTestManager(store *mockStore) *Manager {
	pm := NewPoolManager()
	return NewManager(store, pm, &mockRegistry{}, &mockNATSClient{}, &mockDiscovery{})
}

func TestCreateFlowStartsSourceAndPublishesEvents(t *testing.T) {
	store := newMockStore()
	store.sources["src-1"] = &ports.SourceConfig{InstanceID: "src-1", Type: "postgres"}
	store.sinks["sink-1"] = &ports.SinkConfig{InstanceID: "sink-1", Type: "postgres"}

	started := make(chan struct{})
	src := &mockSource{
		started: started,
		events: []*domain.Event{{
			Subject:    "cdc.src-1.public.users.0",
			InstanceID: "src-1",
			Schema:     "public",
			Table:      "users",
			Data:       []byte(`{"after":{"id":1}}`),
		}},
	}
	nc := &mockNATSClient{publishCh: make(chan []*domain.Event, 1)}
	mgr := NewManager(store, NewPoolManager(), &mockRegistry{source: src}, nc, &mockDiscovery{})
	defer mgr.Stop()

	_, err := mgr.CreateFlow(context.Background(), &ports.FlowConfig{
		Name:        "users",
		SourceID:    "src-1",
		SinkID:      "sink-1",
		SourceTable: "public.users",
		SinkTable:   "users",
	})
	if err != nil {
		t.Fatalf("CreateFlow failed: %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("source was not started")
	}

	select {
	case batch := <-nc.publishCh:
		if len(batch) != 1 {
			t.Fatalf("published batch length = %d, want 1", len(batch))
		}
		if batch[0].Subject != "cdc.src-1.public.users.0" {
			t.Fatalf("published subject = %q", batch[0].Subject)
		}
	case <-time.After(time.Second):
		t.Fatal("source event was not published to NATS")
	}
}

func TestEnsureSourceRunningUsesStoredSourceOffset(t *testing.T) {
	store := newMockStore()
	store.sources["src-1"] = &ports.SourceConfig{InstanceID: "src-1", Type: constant.SourceTypeMySQL.String()}
	store.sourceOffsets["src-1"] = "mysql-bin.000001:42"
	src := &mockSource{}
	reg := &mockRegistry{source: src}
	mgr := NewManager(store, NewPoolManager(), reg, &mockNATSClient{}, &mockDiscovery{})

	err := mgr.ensureSourceRunning(context.Background(), store.sources["src-1"])
	if err != nil {
		t.Fatal(err)
	}
	if src.initialOffset != "mysql-bin.000001:42" {
		t.Fatalf("initial offset = %q", src.initialOffset)
	}
	mgr.Stop()
}

func TestPublishSourceEventsSavesSourceOffsetAndAcks(t *testing.T) {
	store := newMockStore()
	runtime := &sourceRuntime{
		events: make(chan *domain.Event, 1),
		acks:   make(chan ports.SourceAck, 1),
		done:   make(chan struct{}),
	}
	mgr := NewManager(store, NewPoolManager(), &mockRegistry{}, &mockNATSClient{}, &mockDiscovery{})
	mgr.sourceRuns["src-1"] = runtime
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go mgr.publishSourceEvents(ctx, "src-1", runtime.events, runtime.done)
	runtime.events <- &domain.Event{Subject: "cdc.src.public.users.0", InstanceID: "src-1", Offset: "mysql-bin.000001:99", LSN: 99}
	close(runtime.events)
	<-runtime.done

	if got := store.sourceOffsets["src-1"]; got != "mysql-bin.000001:99" {
		t.Fatalf("source offset = %q", got)
	}
	ack := <-runtime.acks
	if ack.Offset != "mysql-bin.000001:99" || ack.LSN != 99 {
		t.Fatalf("ack = %+v", ack)
	}
}

func TestDesiredSourceTablesIncludesRunningFlowsOnly(t *testing.T) {
	store := newMockStore()
	store.flows["running"] = &ports.FlowConfig{FlowID: "running", SourceID: "src-1", SourceTable: "public.users", Status: ports.FlowStatusRunning}
	store.flows["paused"] = &ports.FlowConfig{FlowID: "paused", SourceID: "src-1", SourceTable: "audit_logs", Status: ports.FlowStatusPaused}
	mgr := NewManager(store, NewPoolManager(), &mockRegistry{}, &mockNATSClient{}, &mockDiscovery{})

	tables, err := mgr.desiredSourceTables(context.Background(), "src-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 {
		t.Fatalf("tables = %+v", tables)
	}
	seen := map[string]bool{}
	for _, table := range tables {
		seen[table.Schema+"."+table.Table] = true
	}
	if !seen["public.users"] || seen["public.audit_logs"] {
		t.Fatalf("tables = %+v", tables)
	}
}

func TestPauseFlowStopsSourceWhenNoRunningTablesRemain(t *testing.T) {
	store := newMockStore()
	store.sources["src-1"] = &ports.SourceConfig{InstanceID: "src-1", Type: constant.SourceTypePostgres.String()}
	store.sinks["sink-1"] = &ports.SinkConfig{InstanceID: "sink-1", Type: constant.SinkTypePostgres.String()}
	store.flows["flow-1"] = &ports.FlowConfig{
		FlowID:      "flow-1",
		SourceID:    "src-1",
		SinkID:      "sink-1",
		SourceTable: "public.users",
		SinkTable:   "public.users",
		Status:      ports.FlowStatusRunning,
	}

	src := &mockSource{stopCh: make(chan struct{})}
	mgr := NewManager(store, NewPoolManager(), &mockRegistry{source: src}, &mockNATSClient{}, &mockDiscovery{})
	mgr.startWorker(store.flows["flow-1"], &mockSink{})
	if err := mgr.ensureSourceRunning(context.Background(), store.sources["src-1"]); err != nil {
		t.Fatal(err)
	}

	if _, err := mgr.PauseFlow(context.Background(), "flow-1"); err != nil {
		t.Fatalf("PauseFlow failed: %v", err)
	}
	select {
	case <-src.stopCh:
	case <-time.After(time.Second):
		t.Fatal("source was not stopped after the last running table was paused")
	}
	if _, ok := mgr.sourceRuns["src-1"]; ok {
		t.Fatal("source runtime still registered")
	}
	if len(src.syncedTables) != 0 {
		t.Fatalf("synced tables = %+v, want empty source table selection", src.syncedTables)
	}
}

func TestCreateFlowGeneratesNameWhenMissing(t *testing.T) {
	store := newMockStore()
	store.sources["src-1"] = &ports.SourceConfig{
		InstanceID: "src-1",
		Name:       "Source DB",
		Type:       "postgres",
	}
	store.sinks["sink-1"] = &ports.SinkConfig{
		InstanceID: "sink-1",
		Name:       "Sink DB",
		Type:       "postgres",
	}

	mgr := NewManager(store, NewPoolManager(), &mockRegistry{}, &mockNATSClient{}, &mockDiscovery{})
	defer mgr.Stop()

	flow, err := mgr.CreateFlow(context.Background(), &ports.FlowConfig{
		SourceID:    "src-1",
		SinkID:      "sink-1",
		SourceTable: "public.orders",
		SinkTable:   "warehouse.orders",
	})
	if err != nil {
		t.Fatalf("CreateFlow failed: %v", err)
	}
	if flow.Name != "sync-source-db-orders-to-sink-db-orders" {
		t.Fatalf("generated name = %q", flow.Name)
	}
}

func TestCreateFlowRejectsDuplicateTableMapping(t *testing.T) {
	store := newMockStore()
	store.sources["src-1"] = &ports.SourceConfig{InstanceID: "src-1", Type: "postgres"}
	store.sinks["sink-1"] = &ports.SinkConfig{InstanceID: "sink-1", Type: "postgres"}
	store.flows["existing"] = &ports.FlowConfig{
		FlowID:      "existing",
		SourceID:    "src-1",
		SinkID:      "sink-1",
		SourceTable: "public.users",
		SinkTable:   "warehouse.users",
		Status:      ports.FlowStatusPaused,
	}
	mgr := NewManager(store, NewPoolManager(), &mockRegistry{}, &mockNATSClient{}, &mockDiscovery{})

	_, err := mgr.CreateFlow(context.Background(), &ports.FlowConfig{
		SourceID:    "src-1",
		SinkID:      "sink-1",
		SourceTable: " public.users ",
		SinkTable:   " warehouse.users ",
	})

	if err == nil || !strings.Contains(err.Error(), "flow mapping") {
		t.Fatalf("err = %v", err)
	}
}

func TestUpdateFlowRejectsDuplicateTableMapping(t *testing.T) {
	store := newMockStore()
	store.sources["src-1"] = &ports.SourceConfig{InstanceID: "src-1", Type: "postgres"}
	store.sinks["sink-1"] = &ports.SinkConfig{InstanceID: "sink-1", Type: "postgres"}
	store.flows["existing"] = &ports.FlowConfig{
		FlowID:      "existing",
		SourceID:    "src-1",
		SinkID:      "sink-1",
		SourceTable: "public.users",
		SinkTable:   "warehouse.users",
		Status:      ports.FlowStatusPaused,
	}
	store.flows["target"] = &ports.FlowConfig{
		FlowID:      "target",
		SourceID:    "src-1",
		SinkID:      "sink-1",
		SourceTable: "public.orders",
		SinkTable:   "warehouse.orders",
		Status:      ports.FlowStatusPaused,
	}
	mgr := NewManager(store, NewPoolManager(), &mockRegistry{}, &mockNATSClient{}, &mockDiscovery{})

	_, err := mgr.UpdateFlow(context.Background(), &ports.FlowConfig{
		FlowID:      "target",
		SourceTable: "public.users",
		SinkTable:   "warehouse.users",
	})

	if err == nil || !strings.Contains(err.Error(), "flow mapping") {
		t.Fatalf("err = %v", err)
	}
}

func TestUpdateFlowAllowsSameTableMappingForSameFlow(t *testing.T) {
	store := newMockStore()
	store.sources["src-1"] = &ports.SourceConfig{InstanceID: "src-1", Type: "postgres"}
	store.sinks["sink-1"] = &ports.SinkConfig{InstanceID: "sink-1", Type: "postgres"}
	store.flows["target"] = &ports.FlowConfig{
		FlowID:      "target",
		SourceID:    "src-1",
		SinkID:      "sink-1",
		SourceTable: "public.users",
		SinkTable:   "warehouse.users",
		Status:      ports.FlowStatusPaused,
	}
	mgr := NewManager(store, NewPoolManager(), &mockRegistry{}, &mockNATSClient{}, &mockDiscovery{})

	_, err := mgr.UpdateFlow(context.Background(), &ports.FlowConfig{
		FlowID:      "target",
		SourceTable: " public.users ",
		SinkTable:   " warehouse.users ",
	})

	if err != nil {
		t.Fatalf("UpdateFlow failed: %v", err)
	}
}

func TestCreateFlowRejectsInvalidFilterExpression(t *testing.T) {
	store := newMockStore()
	if err := store.PutSource(context.Background(), &ports.SourceConfig{InstanceID: "src-1", Type: "postgres"}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutSink(context.Background(), &ports.SinkConfig{InstanceID: "sink-1", Type: "postgres"}); err != nil {
		t.Fatal(err)
	}
	manager := NewManager(store, NewPoolManager(), &mockRegistry{}, &mockNATSClient{}, nil)

	_, err := manager.CreateFlow(context.Background(), &ports.FlowConfig{
		SourceID:    "src-1",
		SinkID:      "sink-1",
		SourceTable: "public.users",
		SinkTable:   "public.users",
		Options:     &ports.FlowOptions{FilterExpression: `data.status ++ "active"`},
	})
	if err == nil || !strings.Contains(err.Error(), "invalid filter expression") {
		t.Fatalf("err = %v, want invalid filter expression", err)
	}
	if flows, _ := store.ListFlows(context.Background()); len(flows) != 0 {
		t.Fatalf("flow persisted despite invalid filter: %+v", flows)
	}
}

// createRunningFlow creates a flow in RUNNING state in the store.
func createRunningFlow(store *mockStore, flowID string) *ports.FlowConfig {
	flow := &ports.FlowConfig{
		FlowID:      flowID,
		Name:        "test-flow",
		SourceID:    "src-1",
		SinkID:      "sink-1",
		SourceTable: "public.users",
		SinkTable:   "users",
		Status:      ports.FlowStatusRunning,
	}
	store.flows[flowID] = flow
	return flow
}

// createPausedFlow creates a flow in PAUSED state in the store.
func createPausedFlow(store *mockStore, flowID string) *ports.FlowConfig {
	flow := &ports.FlowConfig{
		FlowID:      flowID,
		Name:        "test-flow",
		SourceID:    "src-1",
		SinkID:      "sink-1",
		SourceTable: "public.users",
		SinkTable:   "users",
		Status:      ports.FlowStatusPaused,
	}
	store.flows[flowID] = flow
	return flow
}

// --- Property Tests ---

// TestProperty_PauseRequiresRunning verifies that PauseFlow on a non-RUNNING flow
// returns ErrInvalidStateTransition.
// **Validates: Requirements 5.1, 5.4**
func TestProperty_PauseRequiresRunning(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := newMockStore()
		mgr := newTestManager(store)

		flowID := rapid.StringMatching(`[a-z0-9]{8}`).Draw(t, "flowID")
		// Pick a non-RUNNING status
		status := rapid.SampledFrom([]ports.FlowStatus{
			ports.FlowStatusPaused,
			ports.FlowStatusError,
		}).Draw(t, "status")

		store.flows[flowID] = &ports.FlowConfig{
			FlowID:      flowID,
			Name:        "test",
			SourceID:    "src-1",
			SinkID:      "sink-1",
			SourceTable: "public.t",
			SinkTable:   "t",
			Status:      status,
		}

		_, err := mgr.PauseFlow(context.Background(), flowID)
		if !errors.Is(err, ErrInvalidStateTransition) {
			t.Fatalf("expected ErrInvalidStateTransition for status=%q, got: %v", status, err)
		}
	})
}

// TestProperty_ResumeRequiresPaused verifies that ResumeFlow on a non-PAUSED flow
// returns ErrInvalidStateTransition.
// **Validates: Requirements 5.3, 5.5**
func TestProperty_ResumeRequiresPaused(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := newMockStore()
		mgr := newTestManager(store)

		// Need a sink in the store for resume to look up
		store.sinks["sink-1"] = &ports.SinkConfig{InstanceID: "sink-1", Type: "postgres"}

		flowID := rapid.StringMatching(`[a-z0-9]{8}`).Draw(t, "flowID")
		// Pick a non-PAUSED status
		status := rapid.SampledFrom([]ports.FlowStatus{
			ports.FlowStatusRunning,
			ports.FlowStatusError,
		}).Draw(t, "status")

		store.flows[flowID] = &ports.FlowConfig{
			FlowID:      flowID,
			Name:        "test",
			SourceID:    "src-1",
			SinkID:      "sink-1",
			SourceTable: "public.t",
			SinkTable:   "t",
			Status:      status,
		}

		_, err := mgr.ResumeFlow(context.Background(), flowID)
		if !errors.Is(err, ErrInvalidStateTransition) {
			t.Fatalf("expected ErrInvalidStateTransition for status=%q, got: %v", status, err)
		}
	})
}

// TestProperty_PauseResumeRoundTrip verifies that config is unchanged through
// a pause/resume cycle (name, source_id, sink_id, source_table, sink_table, column_mappings).
// **Validates: Requirements 5.1, 5.3**
func TestProperty_PauseResumeRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := newMockStore()
		mgr := newTestManager(store)

		// Set up required source and sink in store
		store.sources["src-1"] = &ports.SourceConfig{InstanceID: "src-1", Type: "postgres"}
		store.sinks["sink-1"] = &ports.SinkConfig{InstanceID: "sink-1", Type: "postgres"}

		flowID := rapid.StringMatching(`[a-z0-9]{8}`).Draw(t, "flowID")
		name := rapid.StringMatching(`[a-z]{3,10}`).Draw(t, "name")
		sourceTable := rapid.StringMatching(`[a-z]{1,5}\.[a-z]{1,5}`).Draw(t, "sourceTable")
		sinkTable := rapid.StringMatching(`[a-z]{1,10}`).Draw(t, "sinkTable")

		// Generate some column mappings
		numMappings := rapid.IntRange(0, 5).Draw(t, "numMappings")
		var mappings []ports.ColumnMapping
		for i := 0; i < numMappings; i++ {
			mappings = append(mappings, ports.ColumnMapping{
				SourceColumn: rapid.StringMatching(`[a-z]{1,8}`).Draw(t, "srcCol"),
				SinkColumn:   rapid.StringMatching(`[a-z]{1,8}`).Draw(t, "sinkCol"),
				Enabled:      rapid.Bool().Draw(t, "enabled"),
			})
		}

		// Create a RUNNING flow directly in the store
		originalFlow := &ports.FlowConfig{
			FlowID:         flowID,
			Name:           name,
			SourceID:       "src-1",
			SinkID:         "sink-1",
			SourceTable:    sourceTable,
			SinkTable:      sinkTable,
			Status:         ports.FlowStatusRunning,
			ColumnMappings: mappings,
		}
		store.flows[flowID] = originalFlow

		// Pause the flow
		pausedFlow, err := mgr.PauseFlow(context.Background(), flowID)
		if err != nil {
			t.Fatalf("PauseFlow failed: %v", err)
		}
		if pausedFlow.Status != ports.FlowStatusPaused {
			t.Fatalf("expected PAUSED status, got %q", pausedFlow.Status)
		}

		// Resume the flow
		resumedFlow, err := mgr.ResumeFlow(context.Background(), flowID)
		if err != nil {
			t.Fatalf("ResumeFlow failed: %v", err)
		}
		if resumedFlow.Status != ports.FlowStatusRunning {
			t.Fatalf("expected RUNNING status after resume, got %q", resumedFlow.Status)
		}

		// Verify config unchanged through the cycle
		if resumedFlow.Name != name {
			t.Fatalf("name changed: %q -> %q", name, resumedFlow.Name)
		}
		if resumedFlow.SourceID != "src-1" {
			t.Fatalf("source_id changed: %q -> %q", "src-1", resumedFlow.SourceID)
		}
		if resumedFlow.SinkID != "sink-1" {
			t.Fatalf("sink_id changed: %q -> %q", "sink-1", resumedFlow.SinkID)
		}
		if resumedFlow.SourceTable != sourceTable {
			t.Fatalf("source_table changed: %q -> %q", sourceTable, resumedFlow.SourceTable)
		}
		if resumedFlow.SinkTable != sinkTable {
			t.Fatalf("sink_table changed: %q -> %q", sinkTable, resumedFlow.SinkTable)
		}
		if len(resumedFlow.ColumnMappings) != len(mappings) {
			t.Fatalf("column_mappings count changed: %d -> %d", len(mappings), len(resumedFlow.ColumnMappings))
		}
		for i, m := range mappings {
			rm := resumedFlow.ColumnMappings[i]
			if m.SourceColumn != rm.SourceColumn || m.SinkColumn != rm.SinkColumn || m.Enabled != rm.Enabled {
				t.Fatalf("column_mapping[%d] changed: %+v -> %+v", i, m, rm)
			}
		}
	})
}
