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
	"github.com/foden/cdc/pkg/pipeline"
	"github.com/foden/cdc/pkg/registry"

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
			os.Exit(1)
		}
		sinkList = append(sinkList, s)
	}

	engine := pipeline.NewEngine(
		cfg.Pipeline.ChannelBufferSize,
		cfg.Pipeline.WorkerCount,
		src,
		sinkList...,
	)

	if err := engine.Start(); err != nil {
		slog.Error("engine error", "err", err)
		os.Exit(1)
	}

	// Graceful shutdown - block main thread until signal received
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("shutdown signal received")
	engine.Stop()

	slog.Info("cdclight shutdown completed")
}
