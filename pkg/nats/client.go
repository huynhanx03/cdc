package nats

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/metrics"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Client handles storage and retrieval of events using NATS JetStream
type Client struct {
	nc         *nats.Conn
	js         jetstream.JetStream
	streamName string
	cfg        *config.NATSConfig

	// Cached KV handles
	mu      sync.RWMutex
	buckets map[string]jetstream.KeyValue
}

// NewClient creates a new NATS JetStream client with production-grade resilience.
func NewClient(cfg *config.NATSConfig) (*Client, error) {
	opts := []nats.Option{
		nats.MaxReconnects(cfg.MaxReconnects),
		nats.ReconnectWait(time.Duration(cfg.ReconnectWaitMs) * time.Millisecond),
		nats.ReconnectBufSize(cfg.ReconnectBufSizeMb * 1024 * 1024),
		nats.PingInterval(20 * time.Second),
		nats.MaxPingsOutstanding(5),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				slog.Error("NATS disconnected", "err", err)
			} else {
				slog.Warn("NATS disconnected (graceful)")
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			slog.Info("NATS reconnected", "url", nc.ConnectedUrl())
			metrics.NATSReconnectTotal.Inc()
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			slog.Warn("NATS connection closed permanently")
		}),
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			slog.Error("NATS async error", "err", err, "subject", sub.Subject)
		}),
	}

	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	return &Client{
		nc:         nc,
		js:         js,
		cfg:        cfg,
		streamName: cfg.StreamName,
		buckets:    make(map[string]jetstream.KeyValue),
	}, nil
}

// Close closes the NATS connection
func (c *Client) Close() {
	if c.nc != nil {
		c.nc.Close()
	}
}

// getKV provides a thread-safe helper for accessing KV buckets with lazy initialization.
func (c *Client) getKV(ctx context.Context, bucketName string) (jetstream.KeyValue, error) {
	c.mu.RLock()
	kv, ok := c.buckets[bucketName]
	c.mu.RUnlock()
	if ok {
		return kv, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Double check
	if kv, ok := c.buckets[bucketName]; ok {
		return kv, nil
	}

	kv, err := c.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: bucketName})
	if err != nil {
		return nil, err
	}

	c.buckets[bucketName] = kv
	return kv, nil
}
