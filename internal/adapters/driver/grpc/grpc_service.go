package drivergrpc

import (
	"context"
	"errors"
	"log/slog"

	cdcpb "github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/internal/core/dto/request"
	"github.com/foden/cdc/internal/core/dto/response"
	"github.com/foden/cdc/internal/core/ports"
	coreruntime "github.com/foden/cdc/internal/core/runtime"
	"github.com/foden/cdc/internal/core/service"
	cdcerrors "github.com/foden/cdc/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CDCService implements the cdcpb.CDCServiceServer interface using the new architecture.
type CDCService struct {
	cdcpb.UnimplementedCDCServiceServer

	sourceService    *service.SourceService
	sinkService      *service.SinkService
	flowService      *service.FlowService
	discoveryService *service.DiscoveryService
	metricsService   *service.MetricsService
	dashboardService *service.DashboardService
	dlqService       *service.DLQService
	explorerService  *service.ExplorerService
}

// NewCDCService creates a new CDCService with all dependencies injected.
func NewCDCService(
	store ports.Store,
	flowManager ports.FlowManager,
	registry ports.Registry,
	discovery ports.Discovery,
	natsClient ports.NATSClient,
	runtimeView *coreruntime.View,
	metricsReader ports.MetricsReader,
	p99Window string,
) *CDCService {
	return &CDCService{
		sourceService:    service.NewSourceService(store, discovery),
		sinkService:      service.NewSinkService(store, discovery),
		flowService:      service.NewFlowService(flowManager),
		discoveryService: service.NewDiscoveryService(store, discovery),
		metricsService:   service.NewMetricsService(store, flowManager, runtimeView),
		dashboardService: service.NewDashboardService(store, flowManager, natsClient, runtimeView, metricsReader, p99Window),
		dlqService:       service.NewDLQService(natsClient),
		explorerService:  service.NewExplorerService(natsClient),
	}
}

// ============================================================
// Health & Metrics
// ============================================================

// HealthCheck returns the service health status and uptime in seconds.
func (s *CDCService) HealthCheck(ctx context.Context, req *cdcpb.HealthCheckRequest) (*cdcpb.HealthCheckResponse, error) {
	health := s.metricsService.HealthCheck(ctx, request.HealthCheckRequest{})
	return &cdcpb.HealthCheckResponse{
		Status:  health.Status,
		Version: health.Version,
		Uptime:  health.Uptime,
	}, nil
}

// GetStats returns per-source and per-sink success/failure counts.
// Counts are derived from flow stats for each running flow, aggregated
// by the source and sink that each flow connects.
func (s *CDCService) GetStats(ctx context.Context, req *cdcpb.GetStatsRequest) (*cdcpb.GetStatsResponse, error) {
	stats, err := s.metricsService.Stats(ctx, request.GetStatsRequest{})
	if err != nil {
		slog.Error("GetStats: failed to load stats", "err", err)
		return nil, status.Errorf(codes.Internal, "failed to load stats: %v", err)
	}

	return &cdcpb.GetStatsResponse{
		SourceStats: componentStatsMapToProto(stats.SourceStats),
		SinkStats:   componentStatsMapToProto(stats.SinkStats),
	}, nil
}

// ============================================================
// DLQ
// ============================================================

// ReprocessDLQ triggers reprocessing of messages in the dead letter queue.
// The NATSClient.ReprocessDLQ method requires a handler function; since we
// only need a count, we use a no-op handler that accepts all events.
func (s *CDCService) ReprocessDLQ(ctx context.Context, req *cdcpb.ReprocessDLQRequest) (*cdcpb.ReprocessDLQResponse, error) {
	result, err := s.dlqService.Reprocess(ctx, request.ReprocessDLQRequest{})
	if err != nil {
		slog.Error("ReprocessDLQ: failed to reprocess DLQ", "err", err)
		return nil, status.Errorf(codes.Internal, "failed to reprocess DLQ: %v", err)
	}

	return &cdcpb.ReprocessDLQResponse{Count: result.Count}, nil
}

func invalidArgumentIfRequired(err error) error {
	if errors.Is(err, cdcerrors.ErrSourceConfigRequired) || errors.Is(err, cdcerrors.ErrSinkConfigRequired) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, cdcerrors.ErrDuplicateConfig) {
		return status.Error(codes.AlreadyExists, err.Error())
	}
	if errors.Is(err, cdcerrors.ErrValidation) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return nil
}

func componentStatsMapToProto(stats map[string]*response.ComponentStats) map[string]*cdcpb.ComponentStats {
	result := make(map[string]*cdcpb.ComponentStats, len(stats))
	for id, stat := range stats {
		result[id] = &cdcpb.ComponentStats{
			SuccessCount: stat.SuccessCount,
			FailureCount: stat.FailureCount,
			LastError:    stat.LastError,
			PartitionLag: stat.PartitionLag,
			LastEventAt:  stat.LastEventAt,
			ActiveFlows:  stat.ActiveFlows,
			Throughput:   stat.Throughput,
			ErrorRate:    stat.ErrorRate,
			AvgLatencyMs: stat.AvgLatencyMs,
		}
	}
	return result
}
