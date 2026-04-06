package config

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/spf13/viper"
)

// Config is the main configuration struct for the CDC application.
type Config struct {
	Name     string         `mapstructure:"name"`
	LogMode  string         `mapstructure:"log_mode"` // "json" or "text"
	Sources  []SourceConfig `mapstructure:"sources"`
	Pipeline PipelineConfig `mapstructure:"pipeline"`
	UI       UIConfig       `mapstructure:"ui"`
	Server   ServerConfig   `mapstructure:"server"`
	Sinks    []SinkConfig   `mapstructure:"sinks"`
	NATS     NATSConfig     `mapstructure:"nats"`
}

// SourceConfig holds the configuration for the CDC source.
type SourceConfig struct {
	InstanceID        string            `mapstructure:"instance_id" json:"instance_id,omitempty"`
	Name              string            `mapstructure:"name" json:"name,omitempty"`
	Type              string            `mapstructure:"type" json:"type"`
	Topic             string            `mapstructure:"topic" json:"topic,omitempty"`
	Host              string            `mapstructure:"host" json:"host"`
	Port              int               `mapstructure:"port" json:"port"`
	Username          string            `mapstructure:"username"`
	Password          string            `mapstructure:"password"`
	Database          string            `mapstructure:"database"`
	Tables            []string          `mapstructure:"tables"`
	SlotName          string            `mapstructure:"slot_name"`
	PublicationName   string            `mapstructure:"publication_name"`
	SnapshotMode      string            `mapstructure:"snapshot_mode" json:"snapshot_mode,omitempty"`
	URL               string            `mapstructure:"url" json:"url,omitempty"`
	Headers           map[string]string `mapstructure:"headers" json:"headers,omitempty"`
	PollingIntervalMs int               `mapstructure:"polling_interval_ms" json:"polling_interval_ms,omitempty"`
}

// PipelineConfig holds the configuration for the CDC pipeline.
type PipelineConfig struct {
	ChannelBufferSize int      `mapstructure:"channel_buffer_size"`
	WorkerCount       int      `mapstructure:"worker_count"`
	BatchSize         int      `mapstructure:"batch_size"`
	FlushIntervalMs   int      `mapstructure:"flush_interval_ms"`
	SubjectFilter     []string `mapstructure:"subject_filter"`
}

// UIConfig holds the configuration for the HTTP UI Server.
type UIConfig struct {
	Enabled bool `mapstructure:"enabled"`
	Port    int  `mapstructure:"port"`
}

// ServerConfig holds gRPC + REST gateway configuration.
type ServerConfig struct {
	GRPCPort int `mapstructure:"grpc_port"`
	HTTPPort int `mapstructure:"http_port"`
}

// SinkConfig holds the configuration for the CDC sink.
type SinkConfig struct {
	InstanceID      string            `mapstructure:"instance_id" json:"instance_id,omitempty"`
	Name            string            `mapstructure:"name" json:"name,omitempty"`
	Type            string            `mapstructure:"type"`
	Topic           string            `mapstructure:"topic"`
	URL             []string          `mapstructure:"url"`
	Host            string            `mapstructure:"host"`
	Port            int               `mapstructure:"port"`
	Database        string            `mapstructure:"database"`
	Username        string            `mapstructure:"username"`
	Password        string            `mapstructure:"password"`
	APIKey          string            `mapstructure:"api_key"`
	Headers         map[string]string `mapstructure:"headers"`
	Index           string            `mapstructure:"index"`
	IndexMapping    map[string]string `mapstructure:"index_mapping"`
	FieldMapping    map[string]string `mapstructure:"field_mapping" json:"field_mapping,omitempty"`
	Transformations map[string]string `mapstructure:"transformations" json:"transformations,omitempty"`
	PrimaryKeys     map[string]string `mapstructure:"primary_keys" json:"primary_keys,omitempty"`
	IndexPrefix     string            `mapstructure:"index_prefix"`
	BatchSize       int32             `mapstructure:"batch_size"`
	FlushIntervalMs int32             `mapstructure:"flush_interval_ms"`
	MaxRetries      int32             `mapstructure:"max_retries"`
	RetryBaseMs     int32             `mapstructure:"retry_base_ms"`

	// Internal compiled programs
	programs     map[string]cel.Program
	programsOnce sync.Once
}

// NATSConfig holds the configuration for NATS JetStream.
type NATSConfig struct {
	Enabled            bool   `mapstructure:"enabled"`
	URL                string `mapstructure:"url"`
	StreamName         string `mapstructure:"stream_name"`
	RetentionDays      int32  `mapstructure:"retention_days"`
	MaxReconnects      int    `mapstructure:"max_reconnects"`
	ReconnectWaitMs    int    `mapstructure:"reconnect_wait_ms"`
	ReconnectBufSizeMb int    `mapstructure:"reconnect_buffer_size_mb"`

	MaxAckPending int `mapstructure:"max_ack_pending"`
	AckWaitMs     int `mapstructure:"ack_wait_ms"`
	MaxDeliver    int `mapstructure:"max_deliver"`
}

// LoadConfig loads the configuration from the given path.
func LoadConfig(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	cfg.applyDefaults()

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validation error: %w", err)
	}

	// Warm up CEL programs
	for i := range cfg.Sinks {
		if err := cfg.Sinks[i].CompileTransformations(); err != nil {
			return nil, fmt.Errorf("sink %s CEL error: %w", cfg.Sinks[i].InstanceID, err)
		}
	}

	return &cfg, nil
}

// applyDefaults applies default values to the configuration.
func (c *Config) applyDefaults() {
	if c.LogMode == "" {
		c.LogMode = "text"
	}

	// Pipeline defaults
	if c.Pipeline.ChannelBufferSize <= 0 {
		c.Pipeline.ChannelBufferSize = 10000
	}
	if c.Pipeline.WorkerCount <= 0 {
		c.Pipeline.WorkerCount = 4
	}
	if c.Pipeline.BatchSize <= 0 {
		c.Pipeline.BatchSize = 100
	}
	if c.Pipeline.FlushIntervalMs <= 0 {
		c.Pipeline.FlushIntervalMs = 1000
	}
	if len(c.Pipeline.SubjectFilter) == 0 {
		c.Pipeline.SubjectFilter = []string{"cdc.>"}
	}

	// UI & Server defaults
	if c.UI.Port <= 0 {
		c.UI.Port = 9092
	}
	if c.Server.GRPCPort <= 0 {
		c.Server.GRPCPort = 9090
	}
	if c.Server.HTTPPort <= 0 {
		c.Server.HTTPPort = 9091
	}

	for i := range c.Sources {
		s := &c.Sources[i]
		if s.InstanceID == "" {
			s.InstanceID = fmt.Sprintf("source_%d", i)
		}
		if s.SlotName == "" {
			s.SlotName = "cdc_slot"
		}
		if s.PublicationName == "" {
			s.PublicationName = "cdc_pub"
		}
		if s.PollingIntervalMs <= 0 {
			s.PollingIntervalMs = 5000
		}
	}

	for i := range c.Sinks {
		s := &c.Sinks[i]
		if s.InstanceID == "" {
			s.InstanceID = fmt.Sprintf("sink_%d", i)
		}
		if s.BatchSize <= 0 {
			s.BatchSize = 500
		}
		if s.FlushIntervalMs <= 0 {
			s.FlushIntervalMs = 1000
		}
		if s.IndexPrefix == "" {
			s.IndexPrefix = "cdc_"
		}
		if s.MaxRetries <= 0 {
			s.MaxRetries = 10
		}
		if s.RetryBaseMs <= 0 {
			s.RetryBaseMs = 1000
		}
	}

	if c.NATS.Enabled {
		if c.NATS.URL == "" {
			c.NATS.URL = "nats://127.0.0.1:4222"
		}
		if c.NATS.StreamName == "" {
			c.NATS.StreamName = "CDC_EVENTS"
		}
		if c.NATS.RetentionDays <= 0 {
			c.NATS.RetentionDays = 7
		}
		if c.NATS.MaxReconnects == 0 {
			c.NATS.MaxReconnects = -1
		}
		if c.NATS.ReconnectWaitMs <= 0 {
			c.NATS.ReconnectWaitMs = 2000
		}
		if c.NATS.ReconnectBufSizeMb <= 0 {
			c.NATS.ReconnectBufSizeMb = 64
		}
		if c.NATS.MaxAckPending <= 0 {
			c.NATS.MaxAckPending = 256
		}
		if c.NATS.AckWaitMs <= 0 {
			c.NATS.AckWaitMs = 30000 // 30s
		}
		if c.NATS.MaxDeliver <= 0 {
			c.NATS.MaxDeliver = 10
		}
	}
}

// validate validates the configuration.
func (c *Config) validate() error {
	if len(c.Sources) == 0 {
		return fmt.Errorf("at least one source is required")
	}
	for i, s := range c.Sources {
		if s.Type == "" || s.Host == "" || s.Database == "" {
			return fmt.Errorf("source[%d] requires type, host, and database", i)
		}
	}
	if len(c.Sinks) == 0 {
		return fmt.Errorf("at least one sink is required")
	}
	return nil
}

// CompileTransformations builds CEL programs once.
// We use sync.Once to ensure thread-safety and avoid redundant compilations.
func (s *SinkConfig) CompileTransformations() error {
	var err error
	s.programsOnce.Do(func() {
		if len(s.Transformations) == 0 {
			return
		}
		env, envErr := cel.NewEnv(
			cel.Variable("data", cel.MapType(cel.StringType, cel.AnyType)),
		)
		if envErr != nil {
			err = envErr
			return
		}
		s.programs = make(map[string]cel.Program)
		for field, expr := range s.Transformations {
			ast, iss := env.Compile(expr)
			if iss.Err() != nil {
				err = fmt.Errorf("compilation error %s: %w", field, iss.Err())
				return
			}
			prog, progErr := env.Program(ast)
			if progErr != nil {
				err = fmt.Errorf("program error %s: %w", field, progErr)
				return
			}
			s.programs[field] = prog
		}
	})
	return err
}

// ApplyFieldMapping transforms the record payload.
func (s *SinkConfig) ApplyFieldMapping(data []byte) ([]byte, error) {
	if len(s.FieldMapping) == 0 && len(s.Transformations) == 0 {
		return data, nil
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	// 1. Rename fields
	for src, dest := range s.FieldMapping {
		if val, ok := m[src]; ok {
			m[dest] = val
			delete(m, src)
		}
	}

	// 2. CEL Transformations
	if len(s.programs) > 0 {
		input := map[string]interface{}{"data": m}
		for field, prog := range s.programs {
			out, _, err := prog.Eval(input)
			if err != nil {
				return nil, fmt.Errorf("eval error on %s: %w", field, err)
			}
			m[field] = out.Value()
		}
	}

	return json.Marshal(m)
}
