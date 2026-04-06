package nats

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// CreateStream uses retention days from config
func (c *Client) CreateStream(ctx context.Context, subjects []string) error {
	maxAge := time.Duration(c.cfg.RetentionDays) * 24 * time.Hour
	if _, err := c.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       c.streamName,
		Subjects:   subjects,
		MaxAge:     maxAge,
		Duplicates: 2 * time.Minute,
		Storage:    jetstream.FileStorage,
	}); err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}
	return nil
}

// CreateOrUpdateConsumer creates or updates a pull consumer for the stream.
// MaxAckPending controls ordering: set low for strict ordering, higher for throughput.
func (c *Client) CreateOrUpdateConsumer(ctx context.Context, name string, filter []string) (jetstream.Consumer, error) {
	return c.js.CreateOrUpdateConsumer(ctx, c.streamName, jetstream.ConsumerConfig{
		Durable:        name,
		FilterSubjects: filter,
		AckPolicy:      jetstream.AckExplicitPolicy,
		MaxAckPending:  c.cfg.MaxAckPending,
		AckWait:        time.Duration(c.cfg.AckWaitMs) * time.Millisecond,
		MaxDeliver:     c.cfg.MaxDeliver,
		ReplayPolicy:   jetstream.ReplayInstantPolicy,
	})
}

// CreateDLQStream creates a dedicated Dead Letter Queue stream for failed messages.
// It defaults to a 30-day retention unless specified otherwise.
func (c *Client) CreateDLQStream(ctx context.Context) error {
	dlqStreamName := c.streamName + "_DLQ"
	// Use retention days from config if available, otherwise default to 30 days
	retention := 30 * 24 * time.Hour
	if c.cfg.RetentionDays > 0 {
		retention = time.Duration(c.cfg.RetentionDays) * 24 * time.Hour
	}

	if _, err := c.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      dlqStreamName,
		Subjects:  []string{"dlq.>"},
		MaxAge:    retention,
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	}); err != nil {
		return fmt.Errorf("failed to create DLQ stream %s: %w", dlqStreamName, err)
	}
	slog.Info("DLQ stream initialized", "stream", dlqStreamName, "retention", retention)
	return nil
}

// GetStreamInfo fetches detailed metadata about the primary JetStream stream.
func (c *Client) GetStreamInfo(ctx context.Context) (*jetstream.StreamInfo, error) {
	stream, err := c.js.Stream(ctx, c.streamName)
	if err != nil {
		return nil, fmt.Errorf("failed to bind to stream %s: %w", c.streamName, err)
	}
	return stream.Info(ctx)
}

// GetConsumer binds to an existing durable consumer by name.
func (c *Client) GetConsumer(ctx context.Context, name string) (jetstream.Consumer, error) {
	return c.js.Consumer(ctx, c.streamName, name)
}

// GetConsumerInfo returns the current sequence floor and pending message count.
// Useful for monitoring consumer progress and lag.
func (c *Client) GetConsumerInfo(ctx context.Context, name string) (uint64, uint64, error) {
	consumer, err := c.GetConsumer(ctx, name) // Reuse GetConsumer method
	if err != nil {
		return 0, 0, err
	}

	info, err := consumer.Info(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to fetch consumer info for %s: %w", name, err)
	}
	// AckFloor.Stream represents the sequence of the last acknowledged message.
	return info.AckFloor.Stream, info.NumPending, nil
}

// GetStreamStats returns simplified high-level metrics of the stream.
func (c *Client) GetStreamStats(ctx context.Context) (*StreamStats, error) {
	info, err := c.GetStreamInfo(ctx) // Reuse GetStreamInfo method
	if err != nil {
		return nil, err
	}

	return &StreamStats{
		Messages:      info.State.Msgs,
		Bytes:         info.State.Bytes,
		FirstSeq:      info.State.FirstSeq,
		LastSeq:       info.State.LastSeq,
		ConsumerCount: info.State.Consumers,
	}, nil
}
