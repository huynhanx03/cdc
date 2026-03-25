package config

import (
	"fmt"

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
	InstanceID      string   `mapstructure:"instance_id" json:"instance_id,omitempty"`
	Type            string   `mapstructure:"type" json:"type"`
	Host            string   `mapstructure:"host" json:"host"`
	Port            int      `mapstructure:"port" json:"port"`
	Username        string   `mapstructure:"username"`
	Password        string   `mapstructure:"password"`
	Database        string   `mapstructure:"database"`
	Tables          []string `mapstructure:"tables"`
	SlotName        string   `mapstructure:"slot_name"`
	PublicationName string   `mapstructure:"publication_name"`
}

// PipelineConfig holds the configuration for the CDC pipeline.
type PipelineConfig struct {
	ChannelBufferSize int      `mapstructure:"channel_buffer_size"`
	WorkerCount       int      `mapstructure:"worker_count"`
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
	Type            string            `mapstructure:"type"`
	URL             []string          `mapstructure:"url"`
	Username        string            `mapstructure:"username"`
	Password        string            `mapstructure:"password"`
	APIKey          string            `mapstructure:"api_key"`
	Index           string            `mapstructure:"index"`
	IndexMapping    map[string]string `mapstructure:"index_mapping"`
	IndexPrefix     string            `mapstructure:"index_prefix"`
	BatchSize       int32             `mapstructure:"batch_size"`
	FlushIntervalMs int32             `mapstructure:"flush_interval_ms"`
	MaxRetries      int32             `mapstructure:"max_retries"`
	RetryBaseMs     int32             `mapstructure:"retry_base_ms"`
}

// NATSConfig holds the configuration for NATS JetStream.
type NATSConfig struct {
	Enabled       bool   `mapstructure:"enabled"`
	URL           string `mapstructure:"url"`
	StreamName    string `mapstructure:"stream_name"`
	RetentionDays int32  `mapstructure:"retention_days"`
}

// LoadConfig loads the configuration from the specified path.
func LoadConfig(path string) (*Config, error) {
	viper.SetConfigFile(path)
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	cfg.applyDefaults()

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config validation error: %w", err)
	}

	return &cfg, nil
}

// applyDefaults applies default values to the configuration.
func (c *Config) applyDefaults() {
	if c.LogMode == "" {
		c.LogMode = "text"
	}
	if c.Pipeline.ChannelBufferSize <= 0 {
		c.Pipeline.ChannelBufferSize = 10000
	}
	if c.Pipeline.WorkerCount <= 0 {
		c.Pipeline.WorkerCount = 4
	}
	for i := range c.Sources {
		if c.Sources[i].SlotName == "" {
			c.Sources[i].SlotName = "cdc_slot"
		}
		if c.Sources[i].PublicationName == "" {
			c.Sources[i].PublicationName = "cdc_pub"
		}
		if c.Sources[i].InstanceID == "" {
			c.Sources[i].InstanceID = fmt.Sprintf("source_%d", i)
		}
	}
	if len(c.Pipeline.SubjectFilter) == 0 {
		c.Pipeline.SubjectFilter = []string{"cdc.>"}
	}

	if c.UI.Port <= 0 {
		c.UI.Port = 8080
	}
	if c.Server.GRPCPort <= 0 {
		c.Server.GRPCPort = 9090
	}
	if c.Server.HTTPPort <= 0 {
		c.Server.HTTPPort = 8080
	}
	for i := range c.Sinks {
		if c.Sinks[i].InstanceID == "" {
			c.Sinks[i].InstanceID = fmt.Sprintf("sink_%d", i)
		}
		if c.Sinks[i].BatchSize <= 0 {
			c.Sinks[i].BatchSize = 500
		}
		if c.Sinks[i].FlushIntervalMs <= 0 {
			c.Sinks[i].FlushIntervalMs = 1000 // 1 second
		}
		if c.Sinks[i].IndexPrefix == "" {
			c.Sinks[i].IndexPrefix = "cdc_"
		}
		if c.Sinks[i].MaxRetries <= 0 {
			c.Sinks[i].MaxRetries = 10
		}
		if c.Sinks[i].RetryBaseMs <= 0 {
			c.Sinks[i].RetryBaseMs = 1000
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
	}
}

// validate validates the configuration.
func (c *Config) validate() error {
	if len(c.Sources) == 0 {
		return fmt.Errorf("at least one source is required")
	}
	for i, s := range c.Sources {
		if s.Type == "" {
			return fmt.Errorf("sources[%d].type is required", i)
		}
		if s.Host == "" {
			return fmt.Errorf("sources[%d].host is required", i)
		}
		if s.Database == "" {
			return fmt.Errorf("sources[%d].database is required", i)
		}
	}
	if len(c.Sinks) == 0 {
		return fmt.Errorf("at least one sink is required")
	}
	return nil
}
