package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/foden/cdc/internal/core/dto/request"
	"github.com/foden/cdc/internal/core/dto/response"
	"github.com/foden/cdc/internal/core/ports"
	coreruntime "github.com/foden/cdc/internal/core/runtime"
)

type DashboardService struct {
	store       ports.Store
	flowManager ports.FlowManager
	natsClient  ports.NATSClient
	metrics     ports.MetricsReader
	runtimeView *coreruntime.View
	p99Window   string
	startTime   time.Time
}

const (
	defaultDashboardP99Window       = "5m"
	dashboardPrometheusQueryTimeout = 2 * time.Second
	dashboardNATSHealthTimeout      = 750 * time.Millisecond
)

func NewDashboardService(
	store ports.Store,
	flowManager ports.FlowManager,
	natsClient ports.NATSClient,
	runtimeView *coreruntime.View,
	metrics ports.MetricsReader,
	p99Window string,
) *DashboardService {
	if runtimeView == nil {
		runtimeView = coreruntime.DefaultView()
	}
	if p99Window == "" {
		p99Window = defaultDashboardP99Window
	}
	return &DashboardService{
		store:       store,
		flowManager: flowManager,
		natsClient:  natsClient,
		metrics:     metrics,
		runtimeView: runtimeView,
		p99Window:   p99Window,
		startTime:   time.Now(),
	}
}

func (s *DashboardService) Summary(
	ctx context.Context,
	_ request.DashboardSummaryRequest,
) (response.DashboardSummaryResponse, error) {
	inventory, err := s.systemInventory(ctx)
	if err != nil {
		return response.DashboardSummaryResponse{}, err
	}
	telemetry, err := s.liveTelemetry(ctx)
	if err != nil {
		return response.DashboardSummaryResponse{}, err
	}

	return response.DashboardSummaryResponse{
		Inventory: inventory,
		Telemetry: telemetry,
	}, nil
}

func (s *DashboardService) systemInventory(ctx context.Context) (response.DashboardSystemInventoryResponse, error) {
	sources, err := s.store.ListSources(ctx)
	if err != nil {
		return response.DashboardSystemInventoryResponse{}, err
	}
	sinks, err := s.store.ListSinks(ctx)
	if err != nil {
		return response.DashboardSystemInventoryResponse{}, err
	}
	flows, err := s.store.ListFlows(ctx)
	if err != nil {
		return response.DashboardSystemInventoryResponse{}, err
	}

	return response.DashboardSystemInventoryResponse{
		SourcesCount: len(sources),
		SinksCount:   len(sinks),
		FlowsCount:   len(flows),
	}, nil
}

func (s *DashboardService) liveTelemetry(ctx context.Context) (response.DashboardLiveTelemetryResponse, error) {
	snapshot := s.runtimeView.Dashboard()

	return response.DashboardLiveTelemetryResponse{
		Throughput:         snapshot.Throughput,
		LatencyP99:         s.getLatencyP99(ctx, snapshot.LatencyP99),
		ActiveWorkers:      snapshot.ActiveWorkers,
		ChannelUtilization: snapshot.ChannelUtilization,
		NATSHealthy:        s.isNATSHealthy(ctx),
		ErrorRate:          snapshot.ErrorRate,
		TotalSyncedEvents:  snapshot.TotalSyncedEvents,
		FailureCount:       snapshot.FailureCount,
	}, nil
}

func (s *DashboardService) getLatencyP99(ctx context.Context, fallback float64) float64 {
	if s.metrics == nil {
		return fallback
	}
	queryCtx, cancel := context.WithTimeout(ctx, dashboardPrometheusQueryTimeout)
	defer cancel()
	value, err := s.metrics.FlowProcessingLatencyP99(queryCtx, s.p99Window)
	if err != nil {
		slog.Warn("dashboard telemetry: prometheus p99 unavailable", "err", err)
		return fallback
	}
	return value
}

func (s *DashboardService) isNATSHealthy(ctx context.Context) bool {
	if s.natsClient == nil {
		return false
	}
	healthCtx, cancel := context.WithTimeout(ctx, dashboardNATSHealthTimeout)
	defer cancel()
	if err := s.natsClient.Health(healthCtx); err != nil {
		slog.Warn("dashboard telemetry: nats health unavailable", "err", err)
		return false
	}
	return true
}
