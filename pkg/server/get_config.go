package server

import (
	"context"

	cdcpb "github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/registry"
	"github.com/foden/cdc/pkg/utils"
)

// GetConfig returns the current configuration of the CDC service
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
