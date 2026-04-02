package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/logger"
	"github.com/foden/cdc/pkg/nats"
	"github.com/foden/cdc/pkg/pipeline"
	"github.com/foden/cdc/pkg/registry"
	"github.com/foden/cdc/pkg/server"

	_ "github.com/foden/cdc/pkg/source/mysql"
	_ "github.com/foden/cdc/pkg/source/postgres"
	_ "github.com/foden/cdc/pkg/source/rest"

	_ "github.com/foden/cdc/pkg/sink/elasticsearch"
	_ "github.com/foden/cdc/pkg/sink/stdout"
	_ "github.com/foden/cdc/pkg/sink/postgres"
	_ "github.com/foden/cdc/pkg/sink/webhook"
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
	slog.Info("cdclight starting", "name", cfg.Name)

	// Initialize NATS client early to restore configuration
	natsClient, err := nats.NewClient(cfg.NATS.URL, cfg.NATS.StreamName)
	if err != nil {
		slog.Error("failed to create NATS client", "err", err)
		os.Exit(1)
	}
	defer natsClient.Close()

	// Restore configuration from NATS KV if it exists
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var persistedCfg config.Config
	found, err := natsClient.GetConfig(ctx, &persistedCfg)
	if err != nil {
		slog.Warn("failed to restore configuration from NATS KV, using static config", "err", err)
	} else if found {
		slog.Info("restored configuration from NATS KV", "sources", len(persistedCfg.Sources), "sinks", len(persistedCfg.Sinks))
		// Use persisted config for sources and sinks
		cfg.Sources = persistedCfg.Sources
		cfg.Sinks = persistedCfg.Sinks
		// We could also override pipeline settings if stored
	}

	// Create Sources via registry
	var sourceList []interfaces.Source
	for i := range cfg.Sources {
		src, err := registry.CreateSource(&cfg.Sources[i])
		if err != nil {
			slog.Error("failed to create source", "type", cfg.Sources[i].Type, "err", err)
			os.Exit(1)
		}
		sourceList = append(sourceList, src)
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

	// Create stream if it doesn't exist with retention
	retention := time.Duration(cfg.NATS.RetentionDays) * 24 * time.Hour
	// Use a more generic subject pattern for the stream to capture all CDC events
	if err := natsClient.CreateStream(ctx, []string{"cdc.>"}, retention); err != nil {
		slog.Error("failed to create NATS stream", "err", err)
		os.Exit(1)
	}

	// Start CDC pipeline engine
	engine := pipeline.NewEngine(
		cfg.Pipeline.ChannelBufferSize,
		cfg.Pipeline.WorkerCount,
		sourceList,
		sinkList,
		natsClient,
	)

	// Start gRPC + REST server
	appServer := server.NewAppServer(
		server.ServerConfig{
			GRPCPort: cfg.Server.GRPCPort,
			HTTPPort: cfg.Server.HTTPPort,
		},
		cfg,
		natsClient,
		engine,
	)

	if err := appServer.Start(); err != nil {
		slog.Error("failed to start gRPC/REST server", "err", err)
		os.Exit(1)
	}

	errCh := make(chan error, 1)
	go func() {
		if err := engine.Start(); err != nil {
			slog.Error("engine error", "err", err)
			errCh <- err
		}
	}()

	// Graceful shutdown
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
	// natsClient.Close() is called via defer

	slog.Info("cdclight shutdown completed")
}
