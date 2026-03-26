package server

import (
	"context"
	"time"

	cdcpb "github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/version"
)

// HealthCheck checks the health of the CDC service
func (s *GRPCService) HealthCheck(_ context.Context, _ *cdcpb.HealthCheckRequest) (*cdcpb.HealthCheckResponse, error) {
	return &cdcpb.HealthCheckResponse{
		Status:  "ok",
		Version: version.Version,
		Uptime:  int64(time.Since(s.startTime).Seconds()),
	}, nil
}
