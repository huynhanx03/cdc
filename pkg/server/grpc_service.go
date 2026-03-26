package server

import (
	"time"

	cdcpb "github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/nats"
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
