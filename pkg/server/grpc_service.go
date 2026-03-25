package server

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	cdcpb "github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/nats"
	"github.com/foden/cdc/pkg/registry"
	"github.com/foden/cdc/pkg/utils"
	"github.com/foden/cdc/version"
)

type GRPCService struct {
	cdcpb.UnimplementedCDCServiceServer
	appCfg *config.Config
	engine interfaces.PipelineEngine

	natsClient *nats.Client
	startTime  time.Time
}

func NewGRPCService(appCfg *config.Config, natsClient *nats.Client, engine interfaces.PipelineEngine) *GRPCService {
	return &GRPCService{
		appCfg:     appCfg,
		natsClient: natsClient,
		engine:     engine,
		startTime:  time.Now(),
	}
}

func (s *GRPCService) HealthCheck(_ context.Context, _ *cdcpb.HealthCheckRequest) (*cdcpb.HealthCheckResponse, error) {

	return &cdcpb.HealthCheckResponse{
		Status:  "ok",
		Version: version.Version,
		Uptime:  int64(time.Since(s.startTime).Seconds()),
	}, nil
}

func (s *GRPCService) GetConfig(_ context.Context, _ *cdcpb.GetConfigRequest) (*cdcpb.GetConfigResponse, error) {
	resp := &cdcpb.GetConfigResponse{
		AvailableSources: registry.SourceNames(),
		AvailableSinks:   registry.SinkNames(),
		Config: &cdcpb.AppConfig{
			Name:    s.appCfg.Name,
			LogMode: s.appCfg.LogMode,
			Sources: utils.Map(s.appCfg.Sources, func(src config.SourceConfig, _ int) *cdcpb.SourceConfig {
				return toSourceProto(src)
			}),
			Sinks: utils.Map(s.appCfg.Sinks, func(sc config.SinkConfig, _ int) *cdcpb.SinkConfig {
				return toSinkProto(sc)
			}),
		},
	}
	return resp, nil
}

func (s *GRPCService) AddSource(ctx context.Context, req *cdcpb.AddSourceRequest) (*cdcpb.AddSourceResponse, error) {
	sCfg, err := toSourceConfig(req.Source)
	if err != nil {
		return nil, fmt.Errorf("invalid source config: %w", err)
	}

	src, err := registry.CreateSource(sCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create source: %w", err)
	}

	if err := s.engine.AddSource(ctx, src); err != nil {
		return nil, err
	}

	// Persist updated configuration
	sCfg.InstanceID = src.InstanceID()
	updated := false
	for i, sc := range s.appCfg.Sources {
		if sc.InstanceID == sCfg.InstanceID {
			s.appCfg.Sources[i] = *sCfg
			updated = true
			break
		}
	}
	if !updated {
		s.appCfg.Sources = append(s.appCfg.Sources, *sCfg)
	}

	if err := s.natsClient.SaveConfig(ctx, s.appCfg); err != nil {
		slog.Error("failed to persist config", "err", err)
	}

	return &cdcpb.AddSourceResponse{InstanceId: src.InstanceID()}, nil
}

func (s *GRPCService) RemoveSource(ctx context.Context, req *cdcpb.RemoveSourceRequest) (*cdcpb.RemoveSourceResponse, error) {
	if err := s.engine.RemoveSource(req.InstanceId); err != nil {
		return nil, err
	}

	// Persist updated configuration
	var newSources []config.SourceConfig
	for _, sc := range s.appCfg.Sources {
		if sc.InstanceID != req.InstanceId {
			newSources = append(newSources, sc)
		}
	}
	s.appCfg.Sources = newSources
	if err := s.natsClient.SaveConfig(ctx, s.appCfg); err != nil {
		slog.Error("failed to persist config", "err", err)
	}

	return &cdcpb.RemoveSourceResponse{Success: true}, nil
}

func (s *GRPCService) AddSink(ctx context.Context, req *cdcpb.AddSinkRequest) (*cdcpb.AddSinkResponse, error) {
	sCfg, err := toSinkConfig(req.Sink)
	if err != nil {
		return nil, fmt.Errorf("invalid sink config: %w", err)
	}

	sink, err := registry.CreateSink(sCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create sink: %w", err)
	}

	s.engine.AddSink(sink)

	// Persist updated configuration
	updated := false
	for i, sc := range s.appCfg.Sinks {
		if sc.InstanceID == sCfg.InstanceID {
			s.appCfg.Sinks[i] = *sCfg
			updated = true
			break
		}
	}
	if !updated {
		s.appCfg.Sinks = append(s.appCfg.Sinks, *sCfg)
	}

	if err := s.natsClient.SaveConfig(ctx, s.appCfg); err != nil {
		slog.Error("failed to persist config", "err", err)
	}

	return &cdcpb.AddSinkResponse{InstanceId: sink.InstanceID()}, nil
}

func (s *GRPCService) RemoveSink(ctx context.Context, req *cdcpb.RemoveSinkRequest) (*cdcpb.RemoveSinkResponse, error) {
	if err := s.engine.RemoveSink(req.InstanceId); err != nil {
		return nil, err
	}

	// Persist updated configuration
	var newSinks []config.SinkConfig
	for _, sc := range s.appCfg.Sinks {
		if sc.InstanceID != req.InstanceId {
			newSinks = append(newSinks, sc)
		}
	}
	s.appCfg.Sinks = newSinks
	if err := s.natsClient.SaveConfig(ctx, s.appCfg); err != nil {
		slog.Error("failed to persist config", "err", err)
	}

	return &cdcpb.RemoveSinkResponse{Success: true}, nil
}

func (s *GRPCService) GetStats(_ context.Context, _ *cdcpb.GetStatsRequest) (*cdcpb.GetStatsResponse, error) {
	srcStats, snkStats := s.engine.GetStats()

	resp := &cdcpb.GetStatsResponse{
		SourceStats: make(map[string]*cdcpb.ComponentStats),
		SinkStats:   make(map[string]*cdcpb.ComponentStats),
	}

	for k, v := range srcStats {
		resp.SourceStats[k] = &cdcpb.ComponentStats{
			SuccessCount: v.SuccessCount,
			FailureCount: v.FailureCount,
			LastError:    v.LastError,
		}
	}

	for k, v := range snkStats {
		resp.SinkStats[k] = &cdcpb.ComponentStats{
			SuccessCount: v.SuccessCount,
			FailureCount: v.FailureCount,
			LastError:    v.LastError,
		}
	}

	return resp, nil
}
func (s *GRPCService) ListMessages(ctx context.Context, req *cdcpb.ListMessagesRequest) (*cdcpb.ListMessagesResponse, error) {
	messages, totalCount, err := s.engine.ListMessages(ctx, req.Status, req.Offset, int(req.Limit))
	if err != nil {
		return nil, err
	}

	pbMessages := make([]*cdcpb.MessageItem, len(messages))
	for i, msg := range messages {
		pbMessages[i] = &cdcpb.MessageItem{
			Sequence:  msg.Sequence,
			Timestamp: strconv.FormatInt(msg.Timestamp, 10),
			Subject:   msg.Subject,
			Data:      msg.Data,
			Headers:   msg.Headers,
		}
	}

	return &cdcpb.ListMessagesResponse{
		Messages:   pbMessages,
		TotalCount: totalCount,
	}, nil
}

func (s *GRPCService) GetConsumerInfo(ctx context.Context, _ *cdcpb.GetConsumerInfoRequest) (*cdcpb.GetConsumerInfoResponse, error) {
	ackFloor, pendingCount, err := s.engine.GetConsumerInfo(ctx, "pipeline-worker")
	if err != nil {
		return nil, err
	}

	return &cdcpb.GetConsumerInfoResponse{
		AckFloor:     ackFloor,
		PendingCount: pendingCount,
	}, nil
}
