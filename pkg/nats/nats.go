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
		// Set Msg ID for NATS deduplication: InstanceID + Offset is unique
		msg.Header.Set("Nats-Msg-Id", fmt.Sprintf("%s-%s", event.InstanceID, event.Offset))
	}

	_, err = c.js.PublishMsg(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to publish to NATS: %w", err)
	}

	return nil
}

// PublishRaw sends raw data to a NATS subject with optional headers.
func (c *Client) PublishRaw(ctx context.Context, subject string, data []byte, headers nats.Header) error {
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  headers,
	}

	_, err := c.js.PublishMsg(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to publish raw to NATS: %w", err)
	}

	return nil
}

// MoveToDLQ pushes a message to a dead-letter-queue subject and ACKs the original.
func (c *Client) MoveToDLQ(ctx context.Context, msg jetstream.Msg, reason string) error {
	origSubject := msg.Subject()
	dlqSubject := fmt.Sprintf("dlq.%s", origSubject)

	headers := msg.Headers()
	if headers == nil {
		headers = make(nats.Header)
	}
	headers.Set("X-DLQ-Reason", reason)
	headers.Set("X-DLQ-Original-Subject", origSubject)
	headers.Set("X-DLQ-Timestamp", time.Now().Format(time.RFC3339))

	if _, err := c.js.Publish(ctx, dlqSubject, msg.Data(), jetstream.WithMsgID(headers.Get("Nats-Msg-Id"))); err != nil {
		return fmt.Errorf("failed to move to DLQ: %w", err)
	}

	return msg.Ack()
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
		// Ensure strict ordering: only 1 message in flight per consumer instance for this MVP
		// In a real system, we'd partition by table and have 1 consumer per partition.
		MaxDeliver:   -1,
		ReplayPolicy: jetstream.ReplayInstantPolicy,
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

// ListMessages fetches messages from the stream with page-based pagination and optional filters.
// limit is max items per page, page is 1-indexed page number.
func (c *Client) ListMessages(ctx context.Context, status models.MessageStatus, limit int, page int, topic string, partition string) ([]*MessageItem, uint64, error) {
	stream, err := c.js.Stream(ctx, c.streamName)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get stream: %w", err)
	}

	ackFloor, _, err := c.GetConsumerInfo(ctx, "pipeline-worker")
	if err != nil {
		ackFloor = 0
	}

	info, _ := stream.Info(ctx)
	total := info.State.Msgs

	// Compute skip from page
	skip := 0
	if page > 1 && limit > 0 {
		skip = (page - 1) * limit
	}

	var messages []*MessageItem
	currentSeq := info.State.FirstSeq
	if currentSeq == 0 {
		currentSeq = 1
	}

	skipped := 0
	for len(messages) < limit {
		if currentSeq > info.State.LastSeq {
			break
		}

		msg, err := stream.GetMsg(ctx, currentSeq)
		if err != nil {
			if err == jetstream.ErrMsgNotFound {
				currentSeq++
				continue
			}
			break
		}

		// Filter by topic/partition if provided
		if topic != "" && !c.matchesTopic(msg.Subject, topic) {
			currentSeq++
			continue
		}
		if partition != "" && msg.Subject != partition {
			currentSeq++
			continue
		}

		// Filter by status
		isSent := msg.Sequence <= ackFloor
		if status == models.MessageStatusSent && !isSent {
			currentSeq++
			continue
		}
		if status == models.MessageStatusUnsent && isSent {
			currentSeq++
			continue
		}

		// Skip for page-based pagination
		if skipped < skip {
			skipped++
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

// ListTopics returns unique topic names from the stream's subjects with pagination
func (c *Client) ListTopics(ctx context.Context, limit int, page int) ([]string, uint64, error) {
	info, err := c.GetStreamInfo(ctx)
	if err != nil {
		return nil, 0, err
	}

	uniqueTopics := make(map[string]bool)
	for _, subject := range info.Config.Subjects {
		topic := c.extractTopic(subject)
		if topic != "" {
			uniqueTopics[topic] = true
		}
	}

	var topics []string
	for t := range uniqueTopics {
		topics = append(topics, t)
	}

	total := uint64(len(topics))

	// Simple pagination
	start := 0
	if page > 1 && limit > 0 {
		start = (page - 1) * limit
	}
	if start >= len(topics) {
		return []string{}, total, nil
	}
	end := start + limit
	if limit <= 0 || end > len(topics) {
		end = len(topics)
	}

	return topics[start:end], total, nil
}

// ListPartitions returns all subjects matching a topic prefix with pagination
func (c *Client) ListPartitions(ctx context.Context, topic string, limit int, page int) ([]string, uint64, error) {
	info, err := c.GetStreamInfo(ctx)
	if err != nil {
		return nil, 0, err
	}

	var partitions []string
	prefix := "cdc." + topic + "."
	if topic == "" {
		prefix = "cdc."
	}

	for _, subject := range info.Config.Subjects {
		if len(subject) > len(prefix) && subject[:len(prefix)] == prefix {
			partitions = append(partitions, subject)
		}
	}

	total := uint64(len(partitions))

	// Simple pagination
	start := 0
	if page > 1 && limit > 0 {
		start = (page - 1) * limit
	}
	if start >= len(partitions) {
		return []string{}, total, nil
	}
	end := start + limit
	if limit <= 0 || end > len(partitions) {
		end = len(partitions)
	}

	return partitions[start:end], total, nil
}

func (c *Client) extractTopic(subject string) string {
	// Simple extraction assuming "cdc.<topic>.<anything>"
	// In a real system, this would be more robust.
	var parts []string
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == '.' {
			parts = append(parts, subject[start:i])
			start = i + 1
		}
	}
	parts = append(parts, subject[start:])

	if len(parts) >= 2 && parts[0] == "cdc" {
		return parts[1]
	}
	return ""
}

func (c *Client) matchesTopic(subject string, topic string) bool {
	return c.extractTopic(subject) == topic
}
