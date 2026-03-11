package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/logger"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/pipeline"
	"github.com/foden/cdc/pkg/registry"
	"github.com/foden/cdc/pkg/server"
	"github.com/foden/cdc/pkg/wal"

	_ "github.com/foden/cdc/pkg/source/postgres"

	_ "github.com/foden/cdc/pkg/sink/elasticsearch"
	_ "github.com/foden/cdc/pkg/sink/stdout"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "Path to config file")
	flag.Parse()

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}
	logger.Init(cfg.LogMode)
	slog.Info("cdclight starting", "name", cfg.Name, "source", cfg.Source.Type)

	// Create Source via registry
	src, err := registry.CreateSource(&cfg.Source)
	if err != nil {
		slog.Error("failed to create source", "err", err)
		os.Exit(1)
	}

	// Create Sinks via registry
	var sinkList []interfaces.Sink
	for _, sCfg := range cfg.Sinks {
		s, err := registry.CreateSink(&sCfg)
		if err != nil {
			slog.Error("failed to create sink", "sink_type", sCfg.Type, "err", err)
			continue
		}
		sinkList = append(sinkList, s)
	}

	// Initialize WAL Queue
	manager, err := wal.OpenManager[*models.Event](cfg.WAL.Dir, cfg.WAL.MaxSegmentSize, cfg.WAL.RetentionHours)
	if err != nil {
		slog.Error("failed to open WAL manager", "err", err)
		os.Exit(1)
	}

	// ── Start gRPC + REST server ───────────────────────────
	appServer := server.NewAppServer(
		server.ServerConfig{
			GRPCPort: cfg.Server.GRPCPort,
			HTTPPort: cfg.Server.HTTPPort,
		},
		cfg,
		manager,
	)

	if err := appServer.Start(); err != nil {
		slog.Error("failed to start gRPC/REST server", "err", err)
		os.Exit(1)
	}

	// ── Start CDC pipeline engine ──────────────────────────
	engine := pipeline.NewEngine(
		cfg.Pipeline.ChannelBufferSize,
		cfg.Pipeline.WorkerCount,
		src,
		sinkList,
		manager,
	)

	errCh := make(chan error, 1)

	go func() {
		if err := engine.Start(); err != nil {
			slog.Error("engine error", "err", err)
			errCh <- err
		}
	}()

	// ── Graceful shutdown ──────────────────────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
		slog.Info("shutdown signal received")
	case err := <-errCh:
		slog.Error("shutting down due to engine error", "err", err)
	}

	engine.Stop()
	appServer.Stop()
	manager.Close()

	slog.Info("cdclight shutdown completed")
}
