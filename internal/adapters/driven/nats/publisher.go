package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/domain"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Publish sends a single event to a NATS JetStream subject.
func (c *Client) Publish(ctx context.Context, subject string, event *domain.Event) error {
	msg, err := c.toNatsMsg(subject, event)
	if err != nil {
		return err
	}

	// Synchronous publish: waits for acknowledgement from NATS
	if _, err = c.js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("failed to publish to NATS: %w", err)
	}

	return nil
}

// PublishBatch sends multiple events using Async Publishing for maximum throughput.
// It uses PubAckFuture to track status without blocking on every message.
func (c *Client) PublishBatch(ctx context.Context, subjectFunc func(*domain.Event) string, events []*domain.Event) error {
	if len(events) == 0 {
		return nil
	}

	futures := make([]jetstream.PubAckFuture, 0, len(events))
	for _, ev := range events {
		subject := subjectFunc(ev)
		msg, err := c.toNatsMsg(subject, ev)
		if err != nil {
			return err
		}

		// Non-blocking call
		future, err := c.js.PublishMsgAsync(msg)
		if err != nil {
			return fmt.Errorf("failed to initiate async publish: %w", err)
		}
		futures = append(futures, future)
	}

	// Wait for all acknowledgements or context timeout
	for _, f := range futures {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.Ok():
			// Successfully persisted by NATS
		case err := <-f.Err():
			return fmt.Errorf("async publish failed: %w", err)
		}
	}

	return nil
}

// toNatsMsg transforms an internal Event model to a nats.Msg with CDC-specific headers.
func (c *Client) toNatsMsg(subject string, event *domain.Event) (*nats.Msg, error) {
	var data []byte
	var err error

	if len(event.Data) > 0 {
		data = event.Data
	} else {
		data, err = json.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal event: %w", err)
		}
	}

	headers := make(nats.Header)
	headers.Set(constant.HeaderInstanceID, event.InstanceID)
	// Metadata for zero-unmarshal routing
	headers.Set(constant.HeaderSchema, event.Schema)
	headers.Set(constant.HeaderTable, event.Table)
	headers.Set(constant.HeaderOp, string(event.Op))
	headers.Set(constant.HeaderPartition, strconv.Itoa(event.Partition))

	if event.LSN > 0 {
		headers.Set(constant.HeaderLSN, strconv.FormatUint(event.LSN, 10))
	}

	// Offset is still propagated for checkpointing/resume.
	if event.Offset != "" {
		headers.Set(constant.HeaderOffset, event.Offset)
	}

	// Critical for idempotent publish: Nats-Msg-Id.
	msgID := strings.TrimSpace(event.MessageID)
	if msgID == "" && event.Offset != "" {
		msgID = fmt.Sprintf("%s-%s", event.InstanceID, event.Offset)
	}
	if msgID != "" {
		headers.Set("Nats-Msg-Id", msgID)
	}

	return &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  headers,
	}, nil
}
