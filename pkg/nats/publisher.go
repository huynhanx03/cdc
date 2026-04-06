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

// Publish sends a single event to a NATS JetStream subject.
func (c *Client) Publish(ctx context.Context, subject string, event *models.Event) error {
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
func (c *Client) PublishBatch(ctx context.Context, subjectFunc func(*models.Event) string, events []*models.Event) error {
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
func (c *Client) toNatsMsg(subject string, event *models.Event) (*nats.Msg, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event: %w", err)
	}

	headers := make(nats.Header)
	headers.Set(constant.HeaderInstanceID, event.InstanceID)

	if event.LSN > 0 {
		headers.Set(constant.HeaderLSN, strconv.FormatUint(event.LSN, 10))
	}

	// Critical for Exactly-Once delivery: Nats-Msg-Id
	if event.Offset != "" {
		headers.Set(constant.HeaderOffset, event.Offset)
		// Using InstanceID + Offset ensures that retrying the same record won't create duplicates in NATS
		headers.Set("Nats-Msg-Id", fmt.Sprintf("%s-%s", event.InstanceID, event.Offset))
	}

	return &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  headers,
	}, nil
}

// MoveToDLQ routes a failed message to the Dead Letter Queue subject.
func (c *Client) MoveToDLQ(ctx context.Context, msg jetstream.Msg, reason string) error {
	origSubject := msg.Subject()
	dlqSubject := fmt.Sprintf("dlq.%s", origSubject)

	headers := msg.Headers()
	if headers == nil {
		headers = make(nats.Header)
	}

	// Add troubleshooting metadata to headers
	headers.Set("X-DLQ-Reason", reason)
	headers.Set("X-DLQ-Original-Subject", origSubject)
	headers.Set("X-DLQ-Timestamp", time.Now().Format(time.RFC3339))

	// Re-publish to DLQ with original Msg-ID to maintain deduplication if needed
	if _, err := c.js.Publish(ctx, dlqSubject, msg.Data(), jetstream.WithMsgID(headers.Get("Nats-Msg-Id"))); err != nil {
		return fmt.Errorf("failed to push to DLQ: %w", err)
	}
	// Acknowledge the original message so it's removed from the main stream
	return msg.Ack()
}

// ReprocessDLQ fetches failed messages and attempts to re-process them via a handler function.
func (c *Client) ReprocessDLQ(ctx context.Context, handler func(*models.Event) error) (int, error) {
	// 1. Create or bind to the DLQ consumer
	consumer, err := c.CreateOrUpdateConsumer(ctx, "dlq-reprocessor", []string{"dlq.>"})
	if err != nil {
		return 0, fmt.Errorf("failed to setup DLQ consumer: %w", err)
	}

	// 2. Fetch a batch of failed messages
	batch, err := consumer.Fetch(100, jetstream.FetchMaxWait(1*time.Second))
	if err != nil {
		return 0, err
	}

	successCount := 0
	for msg := range batch.Messages() {
		var ev models.Event
		if err := json.Unmarshal(msg.Data(), &ev); err != nil {
			msg.Term() // Unmarshal error is fatal for this message, don't retry
			continue
		}

		// 3. Execute the custom logic (usually re-publishing to the main pipeline)
		if err := handler(&ev); err != nil {
			msg.Nak() // Processing failed again, keep in DLQ
			continue
		}

		msg.Ack()
		successCount++
	}

	return successCount, nil
}
