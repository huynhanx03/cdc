package server

import (
	"errors"

	cdcpb "github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/pkg/config"
)

var (
	ErrSourceConfigRequired   = errors.New("source config is required")
	ErrSourceTypeRequired     = errors.New("source type is required")
	ErrSourceHostRequired     = errors.New("source host is required")
	ErrSourcePortRequired     = errors.New("source port must be positive")
	ErrSourceDatabaseRequired = errors.New("source database is required")
	ErrSinkConfigRequired     = errors.New("sink config is required")
	ErrSinkTypeRequired       = errors.New("sink type is required")
	ErrSinkURLRequired        = errors.New("sink URL is required")
)

// toSourceConfig converts proto SourceConfig to internal SourceConfig with validation and defaults.
func toSourceConfig(p *cdcpb.SourceConfig) (*config.SourceConfig, error) {
	if p == nil {
		return nil, ErrSourceConfigRequired
	}

	if p.Type == "" {
		return nil, ErrSourceTypeRequired
	}
	if p.Host == "" {
		return nil, ErrSourceHostRequired
	}
	if p.Port <= 0 {
		return nil, ErrSourcePortRequired
	}
	if p.Database == "" {
		return nil, ErrSourceDatabaseRequired
	}

	c := &config.SourceConfig{
		Type:            p.Type,
		Host:            p.Host,
		Port:            int(p.Port),
		Database:        p.Database,
		Tables:          p.Tables,
		Username:        p.GetUsername(),
		Password:        p.GetPassword(),
		SlotName:        p.GetSlotName(),
		PublicationName: p.GetPublicationName(),
		InstanceID:      p.GetInstanceId(),
	}

	// Apply defaults
	if c.Type == "postgres" {
		if c.SlotName == "" {
			c.SlotName = "cdc_slot"
		}
		if c.PublicationName == "" {
			c.PublicationName = "cdc_pub"
		}
	}

	return c, nil
}

// toSinkConfig converts proto SinkConfig to internal SinkConfig with validation and defaults.
func toSinkConfig(p *cdcpb.SinkConfig) (*config.SinkConfig, error) {
	if p == nil {
		return nil, ErrSinkConfigRequired
	}

	if p.Type == "" {
		return nil, ErrSinkTypeRequired
	}
	if len(p.Url) == 0 {
		return nil, ErrSinkURLRequired
	}

	c := &config.SinkConfig{
		Type:            p.Type,
		URL:             p.Url,
		Username:        p.GetUsername(),
		Password:        p.GetPassword(),
		APIKey:          p.GetApiKey(),
		Index:           p.GetIndex(),
		IndexMapping:    p.IndexMapping,
		IndexPrefix:     p.GetIndexPrefix(),
		BatchSize:       p.GetBatchSize(),
		FlushIntervalMs: p.GetFlushIntervalMs(),
		MaxRetries:      p.GetMaxRetries(),
		RetryBaseMs:     p.GetRetryBaseMs(),
	}

	// Apply defaults
	if c.BatchSize <= 0 {
		c.BatchSize = 500
	}
	if c.FlushIntervalMs <= 0 {
		c.FlushIntervalMs = 1000
	}
	if c.IndexPrefix == "" {
		c.IndexPrefix = "cdc_"
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 3
	}
	if c.RetryBaseMs <= 0 {
		c.RetryBaseMs = 100
	}

	return c, nil
}

// toSourceProto converts internal SourceConfig to proto SourceConfig.
func toSourceProto(c config.SourceConfig) *cdcpb.SourceConfig {
	return &cdcpb.SourceConfig{
		Type:            c.Type,
		Host:            c.Host,
		Port:            int32(c.Port),
		Username:        &c.Username,
		Password:        &c.Password,
		Database:        c.Database,
		Tables:          c.Tables,
		SlotName:        &c.SlotName,
		PublicationName: &c.PublicationName,
		InstanceId:      &c.InstanceID,
	}
}

// toSinkProto converts internal SinkConfig to proto SinkConfig.
func toSinkProto(c config.SinkConfig) *cdcpb.SinkConfig {
	return &cdcpb.SinkConfig{
		Type:            c.Type,
		Url:             c.URL,
		Username:        &c.Username,
		Password:        &c.Password,
		IndexPrefix:     &c.IndexPrefix,
		Index:           &c.Index,
		IndexMapping:    c.IndexMapping,
		BatchSize:       &c.BatchSize,
		FlushIntervalMs: &c.FlushIntervalMs,
		MaxRetries:      &c.MaxRetries,
		RetryBaseMs:     &c.RetryBaseMs,
		ApiKey:          &c.APIKey,
	}
}
