package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/foden/cdc/pkg/constant"
	"github.com/foden/cdc/pkg/models"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Client handles storage and retrieval of events using NATS JetStream
type Client struct {
	nc         *nats.Conn
	js         jetstream.JetStream
	streamName string
}

// NewClient creates a new NATS JetStream client
func NewClient(url string, streamName string) (*Client, error) {
	nc, err := nats.Connect(url)
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
		streamName: streamName,
	}, nil
}

// Publish sends an event to a NATS JetStream subject
func (c *Client) Publish(ctx context.Context, subject string, event *models.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  make(nats.Header),
	}

	msg.Header.Set(constant.HeaderInstanceID, event.InstanceID)
	if event.LSN > 0 {
		msg.Header.Set(constant.HeaderLSN, strconv.FormatUint(event.LSN, 10))
	}
	if event.Offset != "" {
		msg.Header.Set(constant.HeaderOffset, event.Offset)
	}

	_, err = c.js.PublishMsg(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to publish to NATS: %w", err)
	}

	return nil
}

// Close closes the NATS connection
func (c *Client) Close() {
	if c.nc != nil {
		c.nc.Close()
	}
}

// CreateStream creates or updates a NATS JetStream stream with optional retention (maxAge).
func (c *Client) CreateStream(ctx context.Context, subjects []string, maxAge time.Duration) error {
	_, err := c.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     c.streamName,
		Subjects: subjects,
		MaxAge:   maxAge,
	})
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}
	return nil
}

// GetStreamInfo returns information about the JetStream stream
func (c *Client) GetStreamInfo(ctx context.Context) (*jetstream.StreamInfo, error) {
	stream, err := c.js.Stream(ctx, c.streamName)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream %s: %w", c.streamName, err)
	}
	return stream.Info(ctx)
}

// CreateConsumer creates or updates a pull consumer for the stream
func (c *Client) CreateOrUpdateConsumer(ctx context.Context, name string, filterSubjects []string) (jetstream.Consumer, error) {
	return c.js.CreateOrUpdateConsumer(ctx, c.streamName, jetstream.ConsumerConfig{
		Durable:        name,
		FilterSubjects: filterSubjects,
		AckPolicy:      jetstream.AckExplicitPolicy,
	})
}

// GetConsumer returns an existing consumer
func (c *Client) GetConsumer(ctx context.Context, name string) (jetstream.Consumer, error) {
	return c.js.Consumer(ctx, c.streamName, name)
}

// GetMessageBySequence fetches a single message by its stream sequence
func (c *Client) GetMessageBySequence(ctx context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	stream, err := c.js.Stream(ctx, c.streamName)
	if err != nil {
		return nil, err
	}
	return stream.GetMsg(ctx, seq)
}

// StreamName returns the configured stream name
func (c *Client) StreamName() string {
	return c.streamName
}

// GetStreamStats returns current stream statistics
func (c *Client) GetStreamStats(ctx context.Context) (*StreamStats, error) {
	stream, err := c.js.Stream(ctx, c.streamName)
	if err != nil {
		return nil, err
	}
	info, err := stream.Info(ctx)
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

// GetConsumerInfo returns information about a specific consumer
func (c *Client) GetConsumerInfo(ctx context.Context, name string) (uint64, uint64, error) {
	consumer, err := c.js.Consumer(ctx, c.streamName, name)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get consumer %s: %w", name, err)
	}

	info, err := consumer.Info(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get consumer info: %w", err)
	}

	return info.AckFloor.Stream, info.NumPending, nil
}

// ListMessages fetches messages from the stream within a sequence range
func (c *Client) ListMessages(ctx context.Context, status models.MessageStatus, offset uint64, limit int) ([]*MessageItem, uint64, error) {
	stream, err := c.js.Stream(ctx, c.streamName)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get stream: %w", err)
	}

	ackFloor, _, err := c.GetConsumerInfo(ctx, "pipeline-worker")
	if err != nil {
		// If consumer doesn't exist yet, we can't filter SENT/UNSENT properly.
		// For now, assume floor is 0 so everything is UNSENT if we can't find consumer.
		ackFloor = 0
	}

	var messages []*MessageItem
	currentSeq := offset
	if currentSeq == 0 {
		currentSeq = 1
	}

	info, _ := stream.Info(ctx)
	total := info.State.Msgs

	for len(messages) < limit {
		msg, err := stream.GetMsg(ctx, currentSeq)
		if err != nil {
			if err == jetstream.ErrMsgNotFound {
				if currentSeq > info.State.LastSeq {
					break
				}
				currentSeq++
				continue
			}
			break
		}

		isSent := msg.Sequence <= ackFloor
		if status == models.MessageStatusSent && !isSent {
			currentSeq++
			continue
		}
		if status == models.MessageStatusUnsent && isSent {
			currentSeq++
			continue
		}

		headers := make(map[string]string)
		for k, v := range msg.Header {
			if len(v) > 0 {
				headers[k] = v[0]
			}
		}

		messages = append(messages, &MessageItem{
			Sequence:  msg.Sequence,
			Timestamp: msg.Time.UnixMilli(),
			Subject:   msg.Subject,
			Data:      msg.Data,
			Headers:   headers,
		})
		currentSeq++
	}

	return messages, total, nil
}
