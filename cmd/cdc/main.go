package main

import (
	"context"
	"flag"
	"fmt"
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

	// Drivers registration
	_ "github.com/foden/cdc/pkg/sink/elasticsearch"
	_ "github.com/foden/cdc/pkg/sink/postgres"
	_ "github.com/foden/cdc/pkg/sink/stdout"
	_ "github.com/foden/cdc/pkg/sink/webhook"
	_ "github.com/foden/cdc/pkg/source/mysql"
	_ "github.com/foden/cdc/pkg/source/postgres"
	_ "github.com/foden/cdc/pkg/source/rest"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "Path to config file")
	flag.Parse()

	// 1. Load static config from file
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		slog.Error("failed to load initial config", "err", err)
		os.Exit(1)
	}
	logger.Init(cfg.LogMode)
	slog.Info("cdclight starting", "name", cfg.Name)

	// 2. Initialize NATS early
	natsClient, err := nats.NewClient(&cfg.NATS)
	if err != nil {
		slog.Error("failed to create NATS client", "err", err)
		os.Exit(1)
	}
	defer natsClient.Close()

	// 3. Restore Config from NATS KV (Override static config if exists)
	if err := restoreConfig(natsClient, cfg); err != nil {
		slog.Warn("config restoration skipped, using local file", "err", err)
	}

	// 4. Create Sources & Sinks from the final config
	sources, sinks, err := buildRegistry(cfg)
	if err != nil {
		slog.Error("failed to initialize registry", "err", err)
		os.Exit(1)
	}

	// 5. Initialize Streams
	setupNatsInfrastructure(natsClient)

	// 6. Execution & Graceful Shutdown
	run(cfg, natsClient, sources, sinks)
}

// restoreConfig fetches persisted configuration and RE-COMPILES CEL programs.
func restoreConfig(client *nats.Client, cfg *config.Config) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var persistedCfg config.Config
	found, err := client.GetConfig(ctx, &persistedCfg)
	if err != nil || !found {
		return err
	}

	slog.Info("restoring config from NATS KV",
		"sources",
		len(persistedCfg.Sources),
		"sinks",
		len(persistedCfg.Sinks))
	cfg.Sources = persistedCfg.Sources
	cfg.Sinks = persistedCfg.Sinks

	// RE-WARM CEL Programs: Because 'programs' field is private and not stored in KV.
	for i := range cfg.Sinks {
		if err := cfg.Sinks[i].CompileTransformations(); err != nil {
			return fmt.Errorf("failed to re-compile sink %s: %w", cfg.Sinks[i].InstanceID, err)
		}
	}
	return nil
}

// buildRegistry creates runtime objects from config data.
func buildRegistry(cfg *config.Config) ([]interfaces.Source, []interfaces.Sink, error) {
	var sources []interfaces.Source
	for i := range cfg.Sources {
		src, err := registry.CreateSource(&cfg.Sources[i])
		if err != nil {
			return nil, nil, fmt.Errorf("source %s error: %w", cfg.Sources[i].InstanceID, err)
		}
		sources = append(sources, src)
	}

	var sinks []interfaces.Sink
	for i := range cfg.Sinks {
		snk, err := registry.CreateSink(&cfg.Sinks[i])
		if err != nil {
			slog.Warn("skipping faulty sink", "id", cfg.Sinks[i].InstanceID, "err", err)
			continue
		}
		sinks = append(sinks, snk)
	}
	return sources, sinks, nil
}

// setupNatsInfrastructure sets up the NATS environment.
func setupNatsInfrastructure(client *nats.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = client.CreateStream(ctx, []string{"cdc.>"})
	_ = client.CreateDLQStream(ctx)
}

// run starts the application and handles graceful shutdown.
func run(cfg *config.Config, natsClient *nats.Client, sources []interfaces.Source, sinks []interfaces.Sink) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	engine := pipeline.NewEngine(cfg, sources, sinks, natsClient)
	appServer := server.NewAppServer(
		server.ServerConfig{GRPCPort: cfg.Server.GRPCPort, HTTPPort: cfg.Server.HTTPPort},
		cfg, natsClient, engine,
	)

	_ = appServer.Start()

	errCh := make(chan error, 1)
	go func() {
		if err := engine.Start(); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		slog.Info("shutting down...")
	case err := <-errCh:
		slog.Error("engine failure", "err", err)
	}

	engine.Stop()
	appServer.Stop()
}
