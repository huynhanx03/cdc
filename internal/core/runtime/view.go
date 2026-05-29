package runtime

import "github.com/foden/cdc/internal/core/ports"

type PoolSnapshotProvider interface {
	GetMetrics(flowID string) *PoolMetricsSnapshot
}

type PoolMetricsSnapshot struct {
	RunningWorkers     int
	PoolCapacity       int
	UtilizationPercent float64
}

type View struct {
	registry *Registry
	metrics  *Metrics
	pools    PoolSnapshotProvider
}

type DashboardSnapshot struct {
	Throughput         float64
	LatencyP99         float64
	ActiveWorkers      uint32
	ChannelUtilization float64
	ErrorRate          float64
	TotalSyncedEvents  uint64
	FailureCount       uint64
}

var defaultView = NewView(defaultRegistry, defaultMetrics, nil)

func NewView(registry *Registry, metrics *Metrics, pools PoolSnapshotProvider) *View {
	return &View{registry: registry, metrics: metrics, pools: pools}
}

func DefaultView() *View {
	return defaultView
}

func SetDefaults(registry *Registry, metrics *Metrics, view *View) {
	SetDefaultRegistry(registry)
	SetDefaultMetrics(metrics)
	if view != nil {
		defaultView = view
		return
	}
	defaultView = NewView(defaultRegistry, defaultMetrics, nil)
}

func (v *View) FlowStats(flowID string) (ports.FlowStats, bool) {
	snapshot, ok := v.metrics.FlowSnapshot(flowID)
	if !ok {
		return ports.FlowStats{}, false
	}
	stats := ports.FlowStats{
		EventsPerSecond:      snapshot.EventsPerSecond,
		ReplicationLagMs:     snapshot.ReplicationLagMs,
		TotalEventsProcessed: snapshot.TotalEventsProcessed,
		FailureCount:         snapshot.FailureCount,
		DLQCount:             snapshot.DLQCount,
		FilteredCount:        snapshot.FilteredCount,
		LastError:            snapshot.LastError,
	}
	if v.pools != nil {
		if pool := v.pools.GetMetrics(flowID); pool != nil {
			stats.RunningWorkers = uint32(pool.RunningWorkers)
			stats.PoolCapacity = uint32(pool.PoolCapacity)
			stats.WorkerUtilization = pool.UtilizationPercent
		}
	}
	return stats, true
}

func (v *View) SourceStats(sourceID string) ComponentStatsSnapshot {
	snapshot, _ := v.metrics.SourceSnapshot(sourceID)
	if v.registry != nil {
		snapshot.ActiveFlows = v.activeFlowsForComponent(sourceID, true)
	}
	return snapshot
}

func (v *View) SinkStats(sinkID string) ComponentStatsSnapshot {
	snapshot, _ := v.metrics.SinkSnapshot(sinkID)
	if v.registry != nil {
		snapshot.ActiveFlows = v.activeFlowsForComponent(sinkID, false)
	}
	return snapshot
}

func (v *View) Dashboard() DashboardSnapshot {
	var snapshot DashboardSnapshot
	v.metrics.RangeFlows(func(_ string, stats FlowStatsSnapshot) bool {
		snapshot.Throughput += stats.EventsPerSecond
		snapshot.TotalSyncedEvents += stats.TotalEventsProcessed
		snapshot.FailureCount += stats.FailureCount
		if float64(stats.ReplicationLagMs) > snapshot.LatencyP99 {
			snapshot.LatencyP99 = float64(stats.ReplicationLagMs)
		}
		return true
	})
	if total := snapshot.TotalSyncedEvents + snapshot.FailureCount; total > 0 {
		snapshot.ErrorRate = float64(snapshot.FailureCount) / float64(total) * 100
	}
	if v.pools == nil || v.registry == nil {
		return snapshot
	}
	v.registry.flows.Range(func(key, _ any) bool {
		pool := v.pools.GetMetrics(key.(string))
		if pool == nil {
			return true
		}
		snapshot.ActiveWorkers += uint32(pool.RunningWorkers)
		if pool.UtilizationPercent > snapshot.ChannelUtilization {
			snapshot.ChannelUtilization = pool.UtilizationPercent
		}
		return true
	})
	return snapshot
}

func (v *View) activeFlowsForComponent(componentID string, source bool) int32 {
	var count int32
	v.registry.flows.Range(func(_, value any) bool {
		info := value.(*FlowRuntimeInfo)
		if source && info.SourceID == componentID {
			count++
		}
		if !source && info.SinkID == componentID {
			count++
		}
		return true
	})
	return count
}
