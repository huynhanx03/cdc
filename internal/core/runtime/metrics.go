package runtime

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type Metrics struct {
	flows   sync.Map
	sources sync.Map
	sinks   sync.Map
}

type flowStats struct {
	written      atomic.Uint64
	filtered     atomic.Uint64
	failed       atomic.Uint64
	dlq          atomic.Uint64
	checkpoint   atomic.Uint64
	retry        atomic.Uint64
	backpressure atomic.Uint64

	throughputBits   atomic.Uint64
	lastThroughputAt atomic.Int64
	lastEventAt      atomic.Int64
	lastLatencyMs    atomic.Int64
	replicationLagMs atomic.Int64
	lastErr          atomic.Value
}

type componentStats struct {
	success        atomic.Uint64
	failed         atomic.Uint64
	throughputBits atomic.Uint64
	lastEventAt    atomic.Int64
	latencyTotal   atomic.Int64
	latencySamples atomic.Uint64
	lastErr        atomic.Value
}

type FlowStatsSnapshot struct {
	EventsPerSecond      float64
	ReplicationLagMs     int64
	TotalEventsProcessed uint64
	FailureCount         uint64
	DLQCount             uint64
	FilteredCount        uint64
	CheckpointSaveCount  uint64
	RetryCount           uint64
	BackpressureCount    uint64
	LastError            string
}

type ComponentStatsSnapshot struct {
	SuccessCount uint64
	FailureCount uint64
	LastError    string
	LastEventAt  int64
	ActiveFlows  int32
	Throughput   float64
	ErrorRate    float64
	AvgLatencyMs int64
}

var defaultMetrics = NewMetrics()

var runtimeMetricNames = []string{
	"events_in_total",
	"events_out_total",
	"sink_write_duration_ms",
	"checkpoint_save_total",
	"retry_total",
	"dlq_total",
	"nats_pending",
	"worker_backpressure_total",
	"source_lag_ms",
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

func DefaultMetrics() *Metrics {
	return defaultMetrics
}

func SetDefaultMetrics(metrics *Metrics) {
	if metrics != nil {
		defaultMetrics = metrics
	}
}

func RuntimeMetricNames() []string {
	return append([]string(nil), runtimeMetricNames...)
}

func (m *Metrics) RecordSourceProduced(sourceID, schema, table string, count uint64, eventTimeMs int64) {
	stats := m.sourceStats(sourceID)
	stats.success.Add(count)
	recordThroughput(&stats.throughputBits, &stats.lastEventAt, count, eventTimeMs)
}

func (m *Metrics) RecordSourceError(sourceID, errorType, message string) {
	stats := m.sourceStats(sourceID)
	stats.failed.Add(1)
	stats.lastErr.Store(message)
}

func (m *Metrics) RecordFlowFiltered(flowID string, count uint64) {
	m.flowStats(flowID).filtered.Add(count)
}

func (m *Metrics) RecordSinkWrite(flowID, sourceID, sinkID string, count uint64, writeLatencyMs int64, eventTimeMs int64) {
	now := time.Now().UnixMilli()
	flow := m.flowStats(flowID)
	flow.written.Add(count)
	recordThroughput(&flow.throughputBits, &flow.lastThroughputAt, count, now)
	flow.lastEventAt.Store(eventTimeMs)
	flow.lastLatencyMs.Store(writeLatencyMs)
	if eventTimeMs > 0 && now >= eventTimeMs {
		flow.replicationLagMs.Store(now - eventTimeMs)
	}

	sink := m.sinkStats(sinkID)
	sink.success.Add(count)
	recordThroughput(&sink.throughputBits, &sink.lastEventAt, count, now)
	recordLatency(&sink.latencyTotal, &sink.latencySamples, writeLatencyMs)
}

func (m *Metrics) RecordFlowFailure(flowID, sourceID, sinkID, errorType, message string, count uint64) {
	flow := m.flowStats(flowID)
	flow.failed.Add(count)
	flow.lastErr.Store(message)

	if sourceID != "" {
		source := m.sourceStats(sourceID)
		source.failed.Add(count)
		source.lastErr.Store(message)
	}
	if sinkID != "" {
		sink := m.sinkStats(sinkID)
		sink.failed.Add(count)
		sink.lastErr.Store(message)
	}
}

func (m *Metrics) RecordDLQ(flowID, sinkID, reason string, count uint64) {
	flow := m.flowStats(flowID)
	flow.dlq.Add(count)
	if reason != "" {
		flow.lastErr.Store(reason)
	}
	if sinkID != "" {
		sink := m.sinkStats(sinkID)
		sink.failed.Add(count)
		if reason != "" {
			sink.lastErr.Store(reason)
		}
	}
}

func (m *Metrics) RecordCheckpointSave(flowID string, count uint64) {
	m.flowStats(flowID).checkpoint.Add(count)
}

func (m *Metrics) RecordRetry(flowID, sinkID, reason string, count uint64) {
	flow := m.flowStats(flowID)
	flow.retry.Add(count)
	if reason != "" {
		flow.lastErr.Store(reason)
	}
	if sinkID != "" {
		sink := m.sinkStats(sinkID)
		sink.failed.Add(count)
		if reason != "" {
			sink.lastErr.Store(reason)
		}
	}
}

func (m *Metrics) RecordBackpressure(flowID string, count uint64) {
	m.flowStats(flowID).backpressure.Add(count)
}

func (m *Metrics) RecordFlowStopped(flowID string) {}

func (m *Metrics) FlowSnapshot(flowID string) (FlowStatsSnapshot, bool) {
	existing, ok := m.flows.Load(flowID)
	if !ok {
		return FlowStatsSnapshot{}, false
	}
	stats := existing.(*flowStats)
	return FlowStatsSnapshot{
		EventsPerSecond:      loadFloat64(stats.throughputBits),
		ReplicationLagMs:     stats.replicationLagMs.Load(),
		TotalEventsProcessed: stats.written.Load(),
		FailureCount:         stats.failed.Load(),
		DLQCount:             stats.dlq.Load(),
		FilteredCount:        stats.filtered.Load(),
		CheckpointSaveCount:  stats.checkpoint.Load(),
		RetryCount:           stats.retry.Load(),
		BackpressureCount:    stats.backpressure.Load(),
		LastError:            loadString(stats.lastErr),
	}, true
}

func (m *Metrics) SourceSnapshot(sourceID string) (ComponentStatsSnapshot, bool) {
	existing, ok := m.sources.Load(sourceID)
	if !ok {
		return ComponentStatsSnapshot{}, false
	}
	return componentSnapshot(existing.(*componentStats)), true
}

func (m *Metrics) SinkSnapshot(sinkID string) (ComponentStatsSnapshot, bool) {
	existing, ok := m.sinks.Load(sinkID)
	if !ok {
		return ComponentStatsSnapshot{}, false
	}
	return componentSnapshot(existing.(*componentStats)), true
}

func (m *Metrics) RangeFlows(fn func(flowID string, stats FlowStatsSnapshot) bool) {
	m.flows.Range(func(key, value any) bool {
		s := value.(*flowStats)
		snapshot := FlowStatsSnapshot{
			EventsPerSecond:      loadFloat64(s.throughputBits),
			ReplicationLagMs:     s.replicationLagMs.Load(),
			TotalEventsProcessed: s.written.Load(),
			FailureCount:         s.failed.Load(),
			DLQCount:             s.dlq.Load(),
			FilteredCount:        s.filtered.Load(),
			CheckpointSaveCount:  s.checkpoint.Load(),
			RetryCount:           s.retry.Load(),
			BackpressureCount:    s.backpressure.Load(),
			LastError:            loadString(s.lastErr),
		}
		return fn(key.(string), snapshot)
	})
}

func (m *Metrics) flowStats(flowID string) *flowStats {
	stats := &flowStats{}
	actual, _ := m.flows.LoadOrStore(flowID, stats)
	return actual.(*flowStats)
}

func (m *Metrics) sourceStats(sourceID string) *componentStats {
	stats := &componentStats{}
	actual, _ := m.sources.LoadOrStore(sourceID, stats)
	return actual.(*componentStats)
}

func (m *Metrics) sinkStats(sinkID string) *componentStats {
	stats := &componentStats{}
	actual, _ := m.sinks.LoadOrStore(sinkID, stats)
	return actual.(*componentStats)
}

func componentSnapshot(stats *componentStats) ComponentStatsSnapshot {
	success := stats.success.Load()
	failed := stats.failed.Load()
	var errorRate float64
	if total := success + failed; total > 0 {
		errorRate = float64(failed) / float64(total) * 100
	}
	return ComponentStatsSnapshot{
		SuccessCount: success,
		FailureCount: failed,
		LastError:    loadString(stats.lastErr),
		LastEventAt:  stats.lastEventAt.Load(),
		Throughput:   loadFloat64(stats.throughputBits),
		ErrorRate:    errorRate,
		AvgLatencyMs: averageLatency(stats.latencyTotal.Load(), stats.latencySamples.Load()),
	}
}

func recordThroughput(bits *atomic.Uint64, lastAt *atomic.Int64, count uint64, nowMs int64) {
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}
	prev := lastAt.Swap(nowMs)
	if prev <= 0 || nowMs <= prev {
		bits.Store(math.Float64bits(float64(count)))
		return
	}
	eventsPerSecond := float64(count) * 1000 / float64(nowMs-prev)
	bits.Store(math.Float64bits(eventsPerSecond))
}

func recordLatency(total *atomic.Int64, samples *atomic.Uint64, latencyMs int64) {
	if latencyMs < 0 {
		return
	}
	total.Add(latencyMs)
	samples.Add(1)
}

func averageLatency(total int64, samples uint64) int64 {
	if samples == 0 {
		return 0
	}
	return total / int64(samples)
}

func loadFloat64(value atomic.Uint64) float64 {
	return math.Float64frombits(value.Load())
}

func loadString(value atomic.Value) string {
	loaded := value.Load()
	if loaded == nil {
		return ""
	}
	text, _ := loaded.(string)
	return text
}
