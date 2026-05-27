// Package flow provides the Flow Manager for orchestrating CDC flow lifecycle operations.
package flow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/foden/cdc/internal/core/domain"
	"github.com/foden/cdc/internal/core/ports"
	coreruntime "github.com/foden/cdc/internal/core/runtime"
	cdcerrors "github.com/foden/cdc/pkg/errors"
	"github.com/foden/cdc/pkg/pool"
	"github.com/foden/cdc/pkg/retry"
	"github.com/google/uuid"
)

// FlowSink is the minimal sink interface needed by flow workers.
// This avoids a circular dependency with the interfaces package.
type FlowSink interface {
	WriteBatch(ctx context.Context, events []*domain.Event) error
	Close() error
	InstanceID() string
}

// SinkProvider is a callback that resolves a sink instance by its ID.
// Used by the flow manager to obtain sink instances for flow workers.
type SinkProvider func(sinkID string) FlowSink

// FlowStatus represents the runtime state of a flow.
type FlowStatus = ports.FlowStatus

const (
	FlowStatusRunning = ports.FlowStatusRunning
	FlowStatusPaused  = ports.FlowStatusPaused
	FlowStatusError   = ports.FlowStatusError
)

// FlowOptions is a type alias for ports.FlowOptions.
type FlowOptions = ports.FlowOptions

// FlowConfig is a type alias for ports.FlowConfig.
type FlowConfig = ports.FlowConfig

// FlowStats is a type alias for ports.FlowStats.
type FlowStats = ports.FlowStats

// ErrFlowNotFound is returned when a flow cannot be found.
var ErrFlowNotFound = errors.New("flow not found")

// ErrInvalidStateTransition is returned when an invalid state transition is attempted.
var ErrInvalidStateTransition = errors.New("invalid state transition")

// Compile-time check that Manager implements ports.FlowManager.
var _ ports.FlowManager = (*Manager)(nil)

// Manager orchestrates flow lifecycle with Pool Manager integration.
// It implements ports.FlowManager.
type Manager struct {
	store           ports.Store
	poolManager     *PoolManager
	sinkPool        *SinkPoolManager
	registry        ports.Registry
	natsClient      ports.NATSClient
	discovery       ports.Discovery
	runtimeRegistry *coreruntime.Registry
	runtimeMetrics  *coreruntime.Metrics
	runtimeView     *coreruntime.View
	maxDeliver      int
	mu              sync.RWMutex
	workers         map[string]*FlowWorker
	sources         map[string]ports.Source // source_id → running source instance
	sourceRuns      map[string]*sourceRuntime
	log             *slog.Logger
}

type sourceRuntime struct {
	source ports.Source
	events chan *domain.Event
	acks   chan ports.SourceAck
	cancel context.CancelFunc
	done   chan struct{}
}

// NewManager creates a new flow Manager with all dependencies.
func NewManager(
	store ports.Store,
	poolManager *PoolManager,
	registry ports.Registry,
	natsClient ports.NATSClient,
	discovery ports.Discovery,
	options ...ManagerOption,
) *Manager {
	m := &Manager{
		store:       store,
		poolManager: poolManager,
		sinkPool:    NewSinkPoolManager(registry, store),
		registry:    registry,
		natsClient:  natsClient,
		discovery:   discovery,
		maxDeliver:  defaultMaxDeliver,
		workers:     make(map[string]*FlowWorker),
		sources:     make(map[string]ports.Source),
		sourceRuns:  make(map[string]*sourceRuntime),
		log:         slog.With("component", "flow_manager"),
	}
	for _, option := range options {
		option(m)
	}
	if m.runtimeRegistry == nil {
		m.runtimeRegistry = coreruntime.DefaultRegistry()
	}
	if m.runtimeMetrics == nil {
		m.runtimeMetrics = coreruntime.DefaultMetrics()
	}
	if m.runtimeView == nil {
		m.runtimeView = coreruntime.NewView(m.runtimeRegistry, m.runtimeMetrics, runtimePoolMetricsProvider{poolManager: m.poolManager})
	}
	if m.maxDeliver <= 0 {
		m.maxDeliver = defaultMaxDeliver
	}
	return m
}

type ManagerOption func(*Manager)

func WithRuntime(
	registry *coreruntime.Registry,
	metrics *coreruntime.Metrics,
	view *coreruntime.View,
) ManagerOption {
	return func(m *Manager) {
		m.runtimeRegistry = registry
		m.runtimeMetrics = metrics
		m.runtimeView = view
	}
}

type runtimePoolMetricsProvider struct {
	poolManager *PoolManager
}

func NewRuntimePoolMetricsProvider(poolManager *PoolManager) coreruntime.PoolSnapshotProvider {
	return runtimePoolMetricsProvider{poolManager: poolManager}
}

func (p runtimePoolMetricsProvider) GetMetrics(flowID string) *coreruntime.PoolMetricsSnapshot {
	if p.poolManager == nil {
		return nil
	}
	metrics := p.poolManager.GetMetrics(flowID)
	if metrics == nil {
		return nil
	}
	return &coreruntime.PoolMetricsSnapshot{
		RunningWorkers:     metrics.RunningWorkers,
		PoolCapacity:       metrics.PoolCapacity,
		UtilizationPercent: metrics.UtilizationPercent,
	}
}

func WithMaxDeliver(maxDeliver int) ManagerOption {
	return func(m *Manager) {
		if maxDeliver > 0 {
			m.maxDeliver = maxDeliver
		}
	}
}

// CreateFlow validates refs, persists config, starts worker with ants pool.
func (m *Manager) CreateFlow(ctx context.Context, cfg *ports.FlowConfig) (*ports.FlowConfig, error) {
	// Validate required fields
	if cfg.SourceID == "" {
		return nil, fmt.Errorf("source_id is required")
	}
	if cfg.SinkID == "" {
		return nil, fmt.Errorf("sink_id is required")
	}
	if cfg.SourceTable == "" {
		return nil, fmt.Errorf("source_table is required")
	}
	if cfg.SinkTable == "" {
		return nil, fmt.Errorf("sink_table is required")
	}
	cfg.SourceID = strings.TrimSpace(cfg.SourceID)
	cfg.SinkID = strings.TrimSpace(cfg.SinkID)
	cfg.SourceTable = strings.TrimSpace(cfg.SourceTable)
	cfg.SinkTable = strings.TrimSpace(cfg.SinkTable)
	if err := validateFlowFilter(cfg); err != nil {
		return nil, err
	}

	// Validate source_id exists
	srcCfg, err := m.store.GetSource(ctx, cfg.SourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to look up source: %w", err)
	}
	if srcCfg == nil {
		return nil, fmt.Errorf("%w: source %q not found", cdcerrors.ErrValidation, cfg.SourceID)
	}

	// Validate sink_id exists
	sinkCfg, err := m.store.GetSink(ctx, cfg.SinkID)
	if err != nil {
		return nil, fmt.Errorf("failed to look up sink: %w", err)
	}
	if sinkCfg == nil {
		return nil, fmt.Errorf("%w: sink %q not found", cdcerrors.ErrValidation, cfg.SinkID)
	}
	if strings.TrimSpace(cfg.Name) == "" {
		cfg.Name = GenerateFlowName(srcCfg, sinkCfg, cfg.SourceTable, cfg.SinkTable)
	} else {
		cfg.Name = strings.TrimSpace(cfg.Name)
	}
	if err := m.validateUniqueFlowMapping(ctx, "", cfg.SourceID, cfg.SinkID, cfg.SourceTable, cfg.SinkTable); err != nil {
		return nil, err
	}

	// Set default FlowOptions
	if cfg.Options == nil {
		cfg.Options = &ports.FlowOptions{}
	}
	if cfg.Options.PartitionCount <= 0 {
		cfg.Options.PartitionCount = 4
	}
	if cfg.Options.PoolSize <= 0 {
		cfg.Options.PoolSize = cfg.Options.PartitionCount
	}

	// Auto-generate column mappings if not provided
	if len(cfg.ColumnMappings) == 0 && m.discovery != nil {
		sourceTables, err := m.discovery.DiscoverSourceTables(ctx, srcCfg)
		if err == nil {
			sinkTables, err := m.discovery.DiscoverSinkTables(ctx, sinkCfg)
			if err == nil {
				sourceColumns := findTableColumns(sourceTables, cfg.SourceTable)
				sinkColumns := findTableColumns(sinkTables, cfg.SinkTable)
				if len(sourceColumns) > 0 && len(sinkColumns) > 0 {
					cfg.ColumnMappings = AutoGenerateMappings(sourceColumns, sinkColumns)
				}
			}
		}
	}

	// Generate flow_id (short UUID)
	flowID := uuid.New().String()[:8]
	now := time.Now().UnixMilli()

	cfg.FlowID = flowID
	cfg.Status = ports.FlowStatusRunning
	cfg.CreatedAt = now
	cfg.UpdatedAt = now

	// Persist to store
	if err := m.store.PutFlow(ctx, cfg); err != nil {
		return nil, fmt.Errorf("failed to persist flow config: %w", err)
	}

	if err := m.reconcileSourceTables(ctx, srcCfg); err != nil {
		m.log.Error("failed to reconcile source tables",
			"flow_id", flowID,
			"source_id", cfg.SourceID,
			"err", err)
		return cfg, nil
	}

	// Acquire shared sink instance from SinkPoolManager
	sink, err := m.sinkPool.Acquire(ctx, cfg.SinkID)
	if err != nil {
		m.log.Error("failed to acquire sink instance", "flow_id", flowID, "sink_id", cfg.SinkID, "err", err)
		return cfg, nil
	}

	m.startWorker(cfg, sink)
	if err := m.ensureSourceRunning(ctx, srcCfg); err != nil {
		m.log.Error("failed to start source",
			"flow_id", flowID,
			"source_id", cfg.SourceID,
			"err", err)
	}

	m.log.Info("flow created",
		"flow_id", flowID,
		"name", cfg.Name,
		"source_id", cfg.SourceID,
		"sink_id", cfg.SinkID,
		"source_table", cfg.SourceTable,
		"sink_table", cfg.SinkTable,
	)

	return cfg, nil
}

var flowNameSanitizer = regexp.MustCompile(`[^a-zA-Z0-9]+`)

func GenerateFlowName(src *ports.SourceConfig, sink *ports.SinkConfig, sourceTable, sinkTable string) string {
	sourceName := componentName(src.Name, src.InstanceID, src.Type, "source")
	sinkName := componentName(sink.Name, sink.InstanceID, sink.Type, "sink")
	sourceTableName := tableName(sourceTable)
	sinkTableName := tableName(sinkTable)

	name := fmt.Sprintf("sync-%s-%s-to-%s-%s", sourceName, sourceTableName, sinkName, sinkTableName)
	name = flowNameSanitizer.ReplaceAllString(strings.ToLower(name), "-")
	name = strings.Trim(name, "-")
	if len(name) > 80 {
		name = strings.TrimRight(name[:80], "-")
	}
	if name == "" {
		return "sync-flow"
	}
	return name
}

func componentName(name, instanceID, connectorType, fallback string) string {
	for _, candidate := range []string{name, instanceID, connectorType, fallback} {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			return trimmed
		}
	}
	return fallback
}

func tableName(table string) string {
	table = strings.TrimSpace(table)
	if table == "" {
		return "table"
	}
	parts := strings.Split(table, ".")
	return parts[len(parts)-1]
}

func (m *Manager) validateUniqueFlowMapping(ctx context.Context, currentFlowID, sourceID, sinkID, sourceTable, sinkTable string) error {
	flows, err := m.store.ListFlows(ctx)
	if err != nil {
		return err
	}
	candidate := flowMappingKey(sourceID, sinkID, sourceTable, sinkTable)
	for _, flow := range flows {
		if flow == nil || flow.FlowID == currentFlowID {
			continue
		}
		if flowMappingKey(flow.SourceID, flow.SinkID, flow.SourceTable, flow.SinkTable) == candidate {
			return fmt.Errorf("%w: flow mapping already exists for source %q table %q to sink %q table %q", cdcerrors.ErrDuplicateConfig, sourceID, sourceTable, sinkID, sinkTable)
		}
	}
	return nil
}

func flowMappingKey(sourceID, sinkID, sourceTable, sinkTable string) string {
	return strings.Join([]string{
		normalizeFlowToken(sourceID),
		normalizeFlowToken(sinkID),
		normalizeFlowToken(sourceTable),
		normalizeFlowToken(sinkTable),
	}, "|")
}

func normalizeFlowToken(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func validateFlowFilter(cfg *ports.FlowConfig) error {
	if cfg == nil || cfg.Options == nil || strings.TrimSpace(cfg.Options.FilterExpression) == "" {
		return nil
	}
	if _, err := NewFilter(cfg.Options.FilterExpression); err != nil {
		return fmt.Errorf("%w: invalid filter expression: %v", cdcerrors.ErrValidation, err)
	}
	return nil
}

// GetFlow retrieves a single flow config from the store.
func (m *Manager) GetFlow(ctx context.Context, flowID string) (*ports.FlowConfig, error) {
	if flowID == "" {
		return nil, fmt.Errorf("flow_id is required")
	}

	flow, err := m.store.GetFlow(ctx, flowID)
	if err != nil {
		return nil, err
	}
	if flow == nil {
		return nil, ErrFlowNotFound
	}

	return flow, nil
}

// ListFlows retrieves all flow configs from the store.
func (m *Manager) ListFlows(ctx context.Context) ([]*ports.FlowConfig, error) {
	return m.store.ListFlows(ctx)
}

// UpdateFlow applies changes and restarts the worker with new config.
func (m *Manager) UpdateFlow(ctx context.Context, cfg *ports.FlowConfig) (*ports.FlowConfig, error) {
	if cfg.FlowID == "" {
		return nil, fmt.Errorf("flow_id is required")
	}

	existing, err := m.store.GetFlow(ctx, cfg.FlowID)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, ErrFlowNotFound
	}

	// Apply updates to existing config
	if cfg.Name != "" {
		existing.Name = cfg.Name
	}
	if cfg.SourceTable != "" {
		existing.SourceTable = strings.TrimSpace(cfg.SourceTable)
	}
	if cfg.SinkTable != "" {
		existing.SinkTable = strings.TrimSpace(cfg.SinkTable)
	}
	if cfg.ColumnMappings != nil {
		existing.ColumnMappings = cfg.ColumnMappings
	}
	if cfg.Options != nil {
		existing.Options = cfg.Options
	}
	if err := validateFlowFilter(existing); err != nil {
		return nil, err
	}
	if err := m.validateUniqueFlowMapping(ctx, existing.FlowID, existing.SourceID, existing.SinkID, existing.SourceTable, existing.SinkTable); err != nil {
		return nil, err
	}
	existing.UpdatedAt = time.Now().UnixMilli()

	// Persist updated config
	if err := m.store.PutFlow(ctx, existing); err != nil {
		return nil, fmt.Errorf("failed to persist updated flow config: %w", err)
	}

	// Restart worker if flow is running
	if existing.Status == ports.FlowStatusRunning {
		m.stopWorker(existing.FlowID)
		m.poolManager.ReleasePool(existing.FlowID)
		m.sinkPool.Release(existing.SinkID)

		sink, err := m.sinkPool.Acquire(ctx, existing.SinkID)
		if err == nil {
			m.startWorker(existing, sink)
			if srcCfg, getErr := m.store.GetSource(ctx, existing.SourceID); getErr == nil && srcCfg != nil {
				if syncErr := m.reconcileSourceTables(ctx, srcCfg); syncErr != nil {
					m.log.Error("failed to reconcile source tables on update", "flow_id", existing.FlowID, "err", syncErr)
				}
				if startErr := m.ensureSourceRunning(ctx, srcCfg); startErr != nil {
					m.log.Error("failed to start source on update", "flow_id", existing.FlowID, "err", startErr)
				}
			}
		} else {
			m.log.Error("failed to acquire sink on update", "flow_id", existing.FlowID, "err", err)
		}
	}

	m.log.Info("flow updated", "flow_id", existing.FlowID)
	return existing, nil
}

// PauseFlow releases pool, stops consumer, sets status=PAUSED (only from RUNNING).
func (m *Manager) PauseFlow(ctx context.Context, flowID string) (*ports.FlowConfig, error) {
	if flowID == "" {
		return nil, fmt.Errorf("flow_id is required")
	}

	flow, err := m.store.GetFlow(ctx, flowID)
	if err != nil {
		return nil, err
	}
	if flow == nil {
		return nil, ErrFlowNotFound
	}

	// Validate state transition: only RUNNING flows can be paused
	if flow.Status != ports.FlowStatusRunning {
		return nil, ErrInvalidStateTransition
	}

	// Stop the flow worker
	m.stopWorker(flowID)

	// Release pool via PoolManager
	m.poolManager.ReleasePool(flowID)

	// Release shared sink connection
	m.sinkPool.Release(flow.SinkID)

	// Update status
	flow.Status = ports.FlowStatusPaused
	flow.UpdatedAt = time.Now().UnixMilli()

	if err := m.store.PutFlow(ctx, flow); err != nil {
		return nil, fmt.Errorf("failed to persist paused flow config: %w", err)
	}
	if srcCfg, err := m.store.GetSource(ctx, flow.SourceID); err == nil && srcCfg != nil {
		if syncErr := m.reconcileSourceTables(ctx, srcCfg); syncErr != nil {
			m.log.Error("failed to reconcile source tables on pause", "flow_id", flow.FlowID, "err", syncErr)
		}
	}

	m.log.Info("flow paused", "flow_id", flowID)
	return flow, nil
}

// ResumeFlow creates new pool, resumes from last offset, sets status=RUNNING (only from PAUSED).
func (m *Manager) ResumeFlow(ctx context.Context, flowID string) (*ports.FlowConfig, error) {
	if flowID == "" {
		return nil, fmt.Errorf("flow_id is required")
	}

	flow, err := m.store.GetFlow(ctx, flowID)
	if err != nil {
		return nil, err
	}
	if flow == nil {
		return nil, ErrFlowNotFound
	}

	// Validate state transition: only PAUSED flows can be resumed
	if flow.Status != ports.FlowStatusPaused {
		return nil, ErrInvalidStateTransition
	}

	flow.Status = ports.FlowStatusRunning
	flow.UpdatedAt = time.Now().UnixMilli()
	if err := m.store.PutFlow(ctx, flow); err != nil {
		return nil, fmt.Errorf("failed to persist resumed flow config: %w", err)
	}

	// Acquire shared sink instance from SinkPoolManager
	sink, err := m.sinkPool.Acquire(ctx, flow.SinkID)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire sink instance: %w", err)
	}

	// Start new FlowWorker (resumes from last offset via store.GetOffset)
	m.startWorker(flow, sink)
	if srcCfg, err := m.store.GetSource(ctx, flow.SourceID); err == nil && srcCfg != nil {
		if err := m.reconcileSourceTables(ctx, srcCfg); err != nil {
			m.log.Error("failed to reconcile source tables on resume", "flow_id", flow.FlowID, "err", err)
		}
		if err := m.ensureSourceRunning(ctx, srcCfg); err != nil {
			m.log.Error("failed to start source on resume", "flow_id", flow.FlowID, "err", err)
		}
	}

	m.log.Info("flow resumed", "flow_id", flowID)
	return flow, nil
}

// DeleteFlow releases pool, deletes consumer, removes from KV.
func (m *Manager) DeleteFlow(ctx context.Context, flowID string) error {
	if flowID == "" {
		return fmt.Errorf("flow_id is required")
	}

	flow, err := m.store.GetFlow(ctx, flowID)
	if err != nil {
		return err
	}
	if flow == nil {
		return ErrFlowNotFound
	}

	// Stop worker if running
	m.stopWorker(flowID)

	// Release pool
	m.poolManager.ReleasePool(flowID)

	// Release shared sink connection
	m.sinkPool.Release(flow.SinkID)

	if err := m.natsClient.DeleteConsumer(ctx, flowConsumerName(flowID)); err != nil {
		return fmt.Errorf("failed to delete flow consumer: %w", err)
	}

	// Delete from store
	if err := m.store.DeleteFlow(ctx, flowID); err != nil {
		return fmt.Errorf("failed to delete flow from store: %w", err)
	}
	if srcCfg, err := m.store.GetSource(ctx, flow.SourceID); err == nil && srcCfg != nil {
		if syncErr := m.reconcileSourceTables(ctx, srcCfg); syncErr != nil {
			m.log.Error("failed to reconcile source tables on delete", "flow_id", flowID, "err", syncErr)
		}
	}

	// Delete offset from store (save empty to clear)
	_ = m.store.SaveOffset(ctx, flowID, "")

	m.log.Info("flow deleted", "flow_id", flowID)
	return nil
}

// GetFlowStats returns per-flow metrics from Pool Manager.
func (m *Manager) GetFlowStats(ctx context.Context, flowID string) (*ports.FlowStats, error) {
	if flowID == "" {
		return nil, fmt.Errorf("flow_id is required")
	}

	flow, err := m.store.GetFlow(ctx, flowID)
	if err != nil {
		return nil, err
	}
	if flow == nil {
		return nil, ErrFlowNotFound
	}

	stats := &ports.FlowStats{}
	if runtimeStats, ok := m.runtimeView.FlowStats(flowID); ok {
		stats = &runtimeStats
	}

	// If flow is paused, return zeroed stats (no active pool)
	if flow.Status == ports.FlowStatusPaused {
		return stats, nil
	}

	return stats, nil
}

// RestoreFlows loads all flows from KV on startup, starts RUNNING flows, skips PAUSED.
func (m *Manager) RestoreFlows(ctx context.Context) error {
	flows, err := m.store.ListFlows(ctx)
	if err != nil {
		return fmt.Errorf("failed to restore flows: %w", err)
	}

	restored := 0
	started := 0

	for _, flow := range flows {
		switch flow.Status {
		case ports.FlowStatusRunning:
			// Acquire shared sink and start worker
			sink, err := m.sinkPool.Acquire(ctx, flow.SinkID)
			if err != nil {
				m.log.Error("failed to acquire sink for flow restore",
					"flow_id", flow.FlowID, "sink_id", flow.SinkID, "err", err)
				continue
			}

			m.startWorker(flow, sink)
			if srcCfg, getErr := m.store.GetSource(ctx, flow.SourceID); getErr == nil && srcCfg != nil {
				if syncErr := m.reconcileSourceTables(ctx, srcCfg); syncErr != nil {
					m.log.Error("failed to reconcile source tables on restore", "flow_id", flow.FlowID, "err", syncErr)
				}
				if startErr := m.ensureSourceRunning(ctx, srcCfg); startErr != nil {
					m.log.Error("failed to start source on restore", "flow_id", flow.FlowID, "err", startErr)
				}
			}
			started++
			m.log.Info("flow restored and worker started", "flow_id", flow.FlowID, "name", flow.Name)

		case ports.FlowStatusPaused:
			// Just load config, no worker
			m.log.Info("flow restored without starting worker", "flow_id", flow.FlowID, "name", flow.Name, "status", flow.Status)
		}
		restored++
	}

	m.log.Info("flows restored from store", "total", restored, "started", started)
	return nil
}

// Stop gracefully stops all workers.
func (m *Manager) Stop() {
	m.mu.Lock()
	for flowID, worker := range m.workers {
		m.log.Info("stopping flow worker on shutdown", "flow_id", flowID)
		worker.Stop()
	}
	m.workers = make(map[string]*FlowWorker)

	for sourceID, runtime := range m.sourceRuns {
		m.log.Info("stopping source on shutdown", "source_id", sourceID)
		runtime.cancel()
		_ = runtime.source.Stop()
		<-runtime.done
	}
	m.sources = make(map[string]ports.Source)
	m.sourceRuns = make(map[string]*sourceRuntime)
	m.mu.Unlock()

	// Close all shared sink connections
	m.sinkPool.CloseAll()

	m.log.Info("flow manager stopped")
}

// --- Internal helpers ---

// RegisterSource registers a running source instance with the flow manager.
func (m *Manager) RegisterSource(sourceID string, src ports.Source) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sources[sourceID] = src
}

// UnregisterSource removes a source instance from the flow manager.
func (m *Manager) UnregisterSource(sourceID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sources, sourceID)
}

func (m *Manager) desiredSourceTables(ctx context.Context, sourceID string) ([]ports.SourceTableRef, error) {
	flows, err := m.store.ListFlows(ctx)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]ports.SourceTableRef)
	for _, flow := range flows {
		if flow == nil || flow.SourceID != sourceID || flow.Status != ports.FlowStatusRunning || strings.TrimSpace(flow.SourceTable) == "" {
			continue
		}
		schema, table := parseSourceTable(flow.SourceTable)
		if schema == "" {
			schema = "public"
		}
		ref := ports.SourceTableRef{Schema: schema, Table: table}
		seen[schema+"."+table] = ref
	}
	tables := make([]ports.SourceTableRef, 0, len(seen))
	for _, table := range seen {
		tables = append(tables, table)
	}
	return tables, nil
}

// reconcileSourceTables updates the source-side table selection from RUNNING flows.
// It is a source lifecycle hook, not FlowWorker event filtering.
func (m *Manager) reconcileSourceTables(ctx context.Context, cfg *ports.SourceConfig) error {
	if cfg == nil {
		return nil
	}
	tables, err := m.desiredSourceTables(ctx, cfg.InstanceID)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		m.stopSourceRuntime(cfg.InstanceID)
	}

	m.mu.RLock()
	runtime := m.sourceRuns[cfg.InstanceID]
	m.mu.RUnlock()

	var src ports.Source
	if runtime != nil {
		src = runtime.source
	} else {
		created, err := m.registry.CreateSource(cfg)
		if err != nil {
			return err
		}
		src = created
	}
	syncer, ok := src.(ports.SourceTableSyncer)
	if !ok {
		return nil
	}
	return syncer.SyncSourceTables(ctx, tables)
}

func (m *Manager) stopSourceRuntime(sourceID string) {
	m.mu.Lock()
	runtime := m.sourceRuns[sourceID]
	if runtime == nil {
		m.mu.Unlock()
		return
	}
	delete(m.sourceRuns, sourceID)
	delete(m.sources, sourceID)
	m.mu.Unlock()

	runtime.cancel()
	_ = runtime.source.Stop()
	<-runtime.done
	m.log.Info("source stopped because no running flow needs it", "source_id", sourceID)
}

func (m *Manager) ensureSourceRunning(ctx context.Context, cfg *ports.SourceConfig) error {
	if cfg == nil {
		return fmt.Errorf("source config is required")
	}

	m.mu.RLock()
	if _, ok := m.sourceRuns[cfg.InstanceID]; ok {
		m.mu.RUnlock()
		return nil
	}
	m.mu.RUnlock()

	src, err := m.registry.CreateSource(cfg)
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runtime := &sourceRuntime{
		source: src,
		events: make(chan *domain.Event, 8192),
		acks:   make(chan ports.SourceAck, 1024),
		cancel: cancel,
		done:   make(chan struct{}),
	}

	m.mu.Lock()
	if _, ok := m.sourceRuns[cfg.InstanceID]; ok {
		m.mu.Unlock()
		cancel()
		return nil
	}
	m.sourceRuns[cfg.InstanceID] = runtime
	m.sources[cfg.InstanceID] = src
	m.mu.Unlock()

	go m.publishSourceEvents(runCtx, cfg.InstanceID, runtime.events, runtime.done)

	initialOffset, err := m.store.GetSourceOffset(ctx, cfg.InstanceID)
	if err != nil {
		cancel()
		<-runtime.done
		m.mu.Lock()
		delete(m.sourceRuns, cfg.InstanceID)
		delete(m.sources, cfg.InstanceID)
		m.mu.Unlock()
		return fmt.Errorf("failed to load source offset for %q: %w", cfg.InstanceID, err)
	}

	if err := src.Start(runtime.events, runtime.acks, initialOffset); err != nil {
		cancel()
		<-runtime.done
		m.mu.Lock()
		delete(m.sourceRuns, cfg.InstanceID)
		delete(m.sources, cfg.InstanceID)
		m.mu.Unlock()
		return err
	}

	m.log.Info("source started", "source_id", cfg.InstanceID, "type", cfg.Type)
	return nil
}

func (m *Manager) publishSourceEvents(ctx context.Context, sourceID string, events <-chan *domain.Event, done chan<- struct{}) {
	defer close(done)

	batch := make([]*domain.Event, 0, 100)
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		toPublish := append([]*domain.Event(nil), batch...)
		batch = batch[:0]

		for {
			publishErr := retry.Do(ctx, retry.DefaultConfig(), func() error {
				return m.natsClient.PublishBatch(ctx, func(ev *domain.Event) string {
					return ev.Subject
				}, toPublish)
			})
			if publishErr == nil {
				break
			}
			m.log.Error("failed to publish source events, retrying batch", "source_id", sourceID, "count", len(toPublish), "err", publishErr)
			if ctx.Err() != nil {
				for _, ev := range toPublish {
					pool.PutEvent(ev)
				}
				return
			}
		}

		var ack ports.SourceAck
		for i := len(toPublish) - 1; i >= 0; i-- {
			ev := toPublish[i]
			if ev == nil || ev.Offset == "" {
				continue
			}
			ack = ports.SourceAck{LSN: ev.LSN, Offset: ev.Offset}
			break
		}

		if ack.Offset != "" {
			if err := m.store.SaveSourceOffset(ctx, sourceID, ack.Offset); err != nil {
				m.log.Error("failed to save source offset", "source_id", sourceID, "offset", ack.Offset, "err", err)
			} else {
				m.mu.RLock()
				runtime := m.sourceRuns[sourceID]
				m.mu.RUnlock()
				if runtime != nil {
					select {
					case runtime.acks <- ack:
					default:
						m.log.Warn("source ack channel full", "source_id", sourceID, "offset", ack.Offset, "lsn", ack.LSN)
					}
				}
			}
		}

		// Return events to the pool to avoid memory leaks
		for _, ev := range toPublish {
			pool.PutEvent(ev)
		}
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case ev, ok := <-events:
			if !ok {
				flush()
				return
			}
			if ev == nil {
				continue
			}
			batch = append(batch, ev)
			if len(batch) >= 100 {
				flush()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(100 * time.Millisecond)
			}
		case <-timer.C:
			flush()
			timer.Reset(100 * time.Millisecond)
		}
	}
}

// startWorker starts a flow worker for the given flow config with the provided sink.
func (m *Manager) startWorker(flow *ports.FlowConfig, sink ports.Sink) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Stop existing worker if any
	if existing, ok := m.workers[flow.FlowID]; ok {
		existing.Stop()
		delete(m.workers, flow.FlowID)
	}

	if err := m.runtimeRegistry.RegisterFlow(flow); err != nil {
		m.log.Error("failed to register flow runtime", "flow_id", flow.FlowID, "err", err)
		return
	}

	// Wrap ports.Sink as FlowSink
	fs := &sinkAdapter{sink: sink}

	worker, err := StartFlowWorker(context.Background(), flow, fs, m.poolManager, m.store, m.natsClient, m.maxDeliver, m.runtimeMetrics)
	if err != nil {
		m.log.Error("failed to start flow worker", "flow_id", flow.FlowID, "err", err)
		return
	}
	m.workers[flow.FlowID] = worker
	m.log.Info("flow worker started", "flow_id", flow.FlowID, "sink_id", flow.SinkID)
}

// stopWorker stops the flow worker for the given flow ID.
func (m *Manager) stopWorker(flowID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if worker, ok := m.workers[flowID]; ok {
		flow := worker.flow
		m.runtimeRegistry.UnregisterFlow(flow.FlowID)
		m.runtimeMetrics.RecordFlowStopped(flow.FlowID)

		worker.Stop()
		delete(m.workers, flowID)
		m.log.Info("flow worker stopped", "flow_id", flowID)
	}
}

// findTableColumns finds columns for a specific table from a list of discovered tables.
func findTableColumns(tables []ports.TableInfo, tableName string) []ports.ColumnInfo {
	// tableName can be "schema.table" or just "table"
	for _, t := range tables {
		fullName := t.Name
		if t.Schema != "" {
			fullName = t.Schema + "." + t.Name
		}
		if strings.EqualFold(fullName, tableName) || strings.EqualFold(t.Name, tableName) {
			return t.Columns
		}
	}
	return nil
}

// parseSourceTable splits a "schema.table" string into schema and table parts.
func parseSourceTable(sourceTable string) (schema, table string) {
	parts := strings.SplitN(sourceTable, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", sourceTable
}

// sinkAdapter wraps ports.Sink to satisfy the FlowSink interface.
type sinkAdapter struct {
	sink ports.Sink
}

func (a *sinkAdapter) WriteBatch(ctx context.Context, events []*domain.Event) error {
	return a.sink.WriteBatch(ctx, events)
}

func (a *sinkAdapter) Close() error {
	return a.sink.Close()
}

func (a *sinkAdapter) InstanceID() string {
	return a.sink.InstanceID()
}
