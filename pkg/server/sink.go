package server

import (
	"context"
	"fmt"
	"log/slog"

	cdcpb "github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/registry"
)

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
