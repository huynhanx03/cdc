package server

import (
	"context"
	"fmt"
	"log/slog"

	cdcpb "github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/registry"
)

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
