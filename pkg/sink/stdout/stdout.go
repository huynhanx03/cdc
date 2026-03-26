package stdout

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/constant"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/registry"
)

func init() {
	registry.RegisterSink(constant.SinkTypeStdout.String(), func(cfg *config.SinkConfig) (interfaces.Sink, error) {
		return New(cfg), nil
	})
}

// StdoutSink writes events to terminal output for debugging.
type StdoutSink struct {
	instanceID string
	cfg        *config.SinkConfig
}

// New creates a new stdout sink.
func New(cfg *config.SinkConfig) *StdoutSink {
	return &StdoutSink{
		instanceID: cfg.InstanceID,
		cfg:        cfg,
	}
}

// Write prints the event as JSON to stdout.
func (s *StdoutSink) Write(event *models.Event) error {
	b, err := json.MarshalIndent(event, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode event: %w", err)
	}
	slog.Info("event captured",
		"instance", event.InstanceID,
		"op", event.Op,
		"db", event.Database,
		"table", event.Table,
		"payload", string(b),
	)
	return nil
}

// Close is a no-op for stdout.
func (s *StdoutSink) Close() error {
	slog.Info("stdout sink closed")
	return nil
}

// Flush is a no-op for stdout since Writes are immediate.
func (s *StdoutSink) Flush() error {
	return nil
}

// Type returns the sink type name.
func (s *StdoutSink) Type() string {
	return constant.SinkTypeStdout.String()
}

// Topic returns the NATS topic pattern this sink subscribes to
func (s *StdoutSink) Topic() string {
	if s.cfg.Topic != "" {
		return s.cfg.Topic
	}
	return "cdc.>"
}

// InstanceID returns the unique identifier for this sink.
func (s *StdoutSink) InstanceID() string {
	return s.instanceID
}
