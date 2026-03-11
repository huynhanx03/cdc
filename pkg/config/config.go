package config

import (
	"fmt"

	"github.com/spf13/viper"
)

// Config is the main configuration struct for the CDC application.
type Config struct {
	Name     string         `mapstructure:"name"`
	LogMode  string         `mapstructure:"log_mode"` // "json" or "text"
	Source   SourceConfig   `mapstructure:"source"`
	Pipeline PipelineConfig `mapstructure:"pipeline"`
	WAL      WALConfig      `mapstructure:"wal"`
	UI       UIConfig       `mapstructure:"ui"`
	Sinks    []SinkConfig   `mapstructure:"sinks"`
}

// SourceConfig holds the configuration for the CDC source.
type SourceConfig struct {
	Type            string   `mapstructure:"type"`
	Host            string   `mapstructure:"host"`
	Port            int      `mapstructure:"port"`
	User            string   `mapstructure:"user"`
	Password        string   `mapstructure:"password"`
	Database        string   `mapstructure:"database"`
	Tables          []string `mapstructure:"tables"`
	ServerID        uint32   `mapstructure:"server_id"`
	SlotName        string   `mapstructure:"slot_name"`
	PublicationName string   `mapstructure:"publication_name"`
}

// PipelineConfig holds the configuration for the CDC pipeline.
type PipelineConfig struct {
	ChannelBufferSize int `mapstructure:"channel_buffer_size"`
	WorkerCount       int `mapstructure:"worker_count"`
}

// WALConfig holds the configuration for the Write-Ahead Log queue.
type WALConfig struct {
	Dir            string `mapstructure:"dir"`
	MaxSegmentSize int64  `mapstructure:"max_segment_size"`
	RetentionHours int    `mapstructure:"retention_hours"`
}

// UIConfig holds the configuration for the HTTP UI Server.
type UIConfig struct {
	Enabled bool `mapstructure:"enabled"`
	Port    int  `mapstructure:"port"`
}

// SinkConfig holds the configuration for the CDC sink.
type SinkConfig struct {
	Type          string   `mapstructure:"type"`
	URL           []string `mapstructure:"url"`
	Username      string   `mapstructure:"username"`
	Password      string   `mapstructure:"password"`
	APIKey        string   `mapstructure:"api_key"`
	IndexPrefix   string   `mapstructure:"index_prefix"`
	BatchSize     int      `mapstructure:"batch_size"`
	FlushInterval int      `mapstructure:"flush_interval_ms"`
	MaxRetries    int      `mapstructure:"max_retries"`
	RetryBaseMs   int      `mapstructure:"retry_base_ms"`
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
	if c.Source.SlotName == "" {
		c.Source.SlotName = "cdc_slot"
	}
	if c.Source.PublicationName == "" {
		c.Source.PublicationName = "cdc_pub"
	}
	if c.WAL.Dir == "" {
		c.WAL.Dir = "./data/wal"
	}
	if c.WAL.MaxSegmentSize <= 0 {
		c.WAL.MaxSegmentSize = 10 * 1024 * 1024 // 10MB default
	}
	if c.WAL.RetentionHours <= 0 {
		c.WAL.RetentionHours = 24
	}
	if c.UI.Port <= 0 {
		c.UI.Port = 8080
	}
	for i := range c.Sinks {
		if c.Sinks[i].BatchSize <= 0 {
			c.Sinks[i].BatchSize = 500
		}
		if c.Sinks[i].FlushInterval <= 0 {
			c.Sinks[i].FlushInterval = 1000 // 1 second
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
}

// validate validates the configuration.
func (c *Config) validate() error {
	if c.Source.Type == "" {
		return fmt.Errorf("source.type is required")
	}
	if c.Source.Host == "" {
		return fmt.Errorf("source.host is required")
	}
	if c.Source.Database == "" {
		return fmt.Errorf("source.database is required")
	}
	if len(c.Sinks) == 0 {
		return fmt.Errorf("at least one sink is required")
	}
	return nil
}
