package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/ports"
	cdcerrors "github.com/foden/cdc/pkg/errors"
	"github.com/foden/cdc/pkg/retry"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	dlqHeaderReason          = "X-DLQ-Reason"
	dlqHeaderOriginalSubject = "X-DLQ-Original-Subject"
	dlqHeaderTimestamp       = "X-DLQ-Timestamp"
	dlqHeaderReprocessedFrom = "X-DLQ-Reprocessed-From"
	dlqReprocessorConsumer   = "dlq-reprocessor"
)

type DLQEnvelope struct {
	ID              string            `json:"id"`
	FlowID          string            `json:"flow_id,omitempty"`
	SinkID          string            `json:"sink_id,omitempty"`
	SourceID        string            `json:"source_id,omitempty"`
	Schema          string            `json:"schema,omitempty"`
	Table           string            `json:"table,omitempty"`
	Op              string            `json:"op,omitempty"`
	MsgID           string            `json:"msg_id,omitempty"`
	OriginalSubject string            `json:"original_subject"`
	OriginalHeaders map[string]string `json:"original_headers,omitempty"`
	Payload         json.RawMessage   `json:"payload"`
	Reason          string            `json:"reason"`
	ErrorClass      string            `json:"error_class"`
	DeliveryCount   uint64            `json:"delivery_count"`
	RetryCount      uint64            `json:"retry_count"`
	FailedAt        int64             `json:"failed_at"`
}

func (c *Client) dlqStreamName() string {
	return c.streamName + "_DLQ"
}

func (c *Client) MoveToDLQ(ctx context.Context, msg jetstream.Msg, opts ports.DLQMoveOptions) error {
	env, err := buildDLQEnvelope(msg, opts)
	if err != nil {
		return err
	}

	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("failed to marshal DLQ envelope: %w", err)
	}

	headers := make(nats.Header)
	headers.Set(dlqHeaderReason, env.Reason)
	headers.Set(dlqHeaderOriginalSubject, env.OriginalSubject)
	headers.Set(dlqHeaderTimestamp, time.UnixMilli(env.FailedAt).Format(time.RFC3339Nano))
	if env.FlowID != "" {
		headers.Set("X-DLQ-Flow-ID", env.FlowID)
	}
	if env.SinkID != "" {
		headers.Set("X-DLQ-Sink-ID", env.SinkID)
	}

	dlqMsg := &nats.Msg{
		Subject: fmt.Sprintf("dlq.%s", env.OriginalSubject),
		Data:    data,
		Header:  headers,
	}
	if env.ID != "" {
		dlqMsg.Header.Set("Nats-Msg-Id", env.ID)
	}

	err = retry.Do(ctx, retry.DefaultConfig(), func() error {
		if _, err := c.js.PublishMsg(ctx, dlqMsg); err != nil {
			return fmt.Errorf("failed to push to DLQ: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return msg.Ack()
}

func (c *Client) ReprocessDLQ(ctx context.Context) (int, error) {
	stream, err := c.js.Stream(ctx, c.dlqStreamName())
	if err != nil {
		return 0, fmt.Errorf("failed to bind DLQ stream %s: %w", c.dlqStreamName(), err)
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        dlqReprocessorConsumer,
		FilterSubjects: []string{"dlq.>"},
		AckPolicy:      jetstream.AckExplicitPolicy,
		MaxAckPending:  c.cfg.MaxAckPending,
		AckWait:        time.Duration(c.cfg.AckWaitMs) * time.Millisecond,
		MaxDeliver:     c.cfg.MaxDeliver,
		ReplayPolicy:   jetstream.ReplayInstantPolicy,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to setup DLQ consumer: %w", err)
	}

	batch, err := consumer.Fetch(100, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		return 0, err
	}

	successCount := 0
	for msg := range batch.Messages() {
		var env DLQEnvelope
		if err := json.Unmarshal(msg.Data(), &env); err != nil {
			_ = msg.TermWithReason("invalid DLQ envelope: " + err.Error())
			continue
		}

		reprocessMsg, err := buildReprocessMsg(env)
		if err != nil {
			_ = msg.TermWithReason("invalid DLQ envelope: " + err.Error())
			continue
		}

		err = retry.Do(ctx, retry.DefaultConfig(), func() error {
			if _, err := c.js.PublishMsg(ctx, reprocessMsg); err != nil {
				return fmt.Errorf("failed to republish DLQ message: %w", err)
			}
			return nil
		})
		if err != nil {
			_ = msg.Nak()
			continue
		}

		_ = msg.Ack()
		successCount++
	}

	return successCount, nil
}

func buildDLQEnvelope(msg jetstream.Msg, opts ports.DLQMoveOptions) (DLQEnvelope, error) {
	originalSubject := strings.TrimSpace(msg.Subject())
	if originalSubject == "" {
		return DLQEnvelope{}, fmt.Errorf("DLQ original subject is empty")
	}

	payload := json.RawMessage(append([]byte(nil), msg.Data()...))
	if !json.Valid(payload) {
		return DLQEnvelope{}, fmt.Errorf("DLQ payload is not valid JSON")
	}

	headers := headerToMap(msg.Headers())
	msgID := headers["Nats-Msg-Id"]
	if msgID == "" {
		msgID = fmt.Sprintf("%s-%d", originalSubject, time.Now().UnixNano())
	}

	deliveryCount := uint64(0)
	if meta, err := msg.Metadata(); err == nil && meta != nil {
		deliveryCount = meta.NumDelivered
	}

	errorClass := opts.ErrorClass
	if errorClass == "" {
		errorClass = cdcerrors.DLQErrorSink
	}
	failedAt := opts.Timestamp
	if failedAt <= 0 {
		failedAt = time.Now().UnixMilli()
	}
	sourceID := firstNonEmpty(opts.SourceID, headers[constant.HeaderInstanceID])
	schema := firstNonEmpty(opts.Schema, headers[constant.HeaderSchema])
	table := firstNonEmpty(opts.Table, headers[constant.HeaderTable])
	op := firstNonEmpty(opts.Op, headers[constant.HeaderOp])
	optionMsgID := firstNonEmpty(opts.MsgID, msgID)
	retryCount := opts.RetryCount
	if retryCount == 0 {
		retryCount = deliveryCount
	}

	return DLQEnvelope{
		ID:              "dlq-" + msgID,
		FlowID:          opts.FlowID,
		SinkID:          opts.SinkID,
		SourceID:        sourceID,
		Schema:          schema,
		Table:           table,
		Op:              op,
		MsgID:           optionMsgID,
		OriginalSubject: originalSubject,
		OriginalHeaders: headers,
		Payload:         payload,
		Reason:          strings.TrimSpace(opts.Reason),
		ErrorClass:      errorClass,
		DeliveryCount:   deliveryCount,
		RetryCount:      retryCount,
		FailedAt:        failedAt,
	}, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func buildReprocessMsg(env DLQEnvelope) (*nats.Msg, error) {
	if strings.TrimSpace(env.OriginalSubject) == "" {
		return nil, fmt.Errorf("original_subject is required")
	}
	if len(env.Payload) == 0 {
		return nil, fmt.Errorf("payload is required")
	}
	if !json.Valid(env.Payload) {
		return nil, fmt.Errorf("payload is not valid JSON")
	}

	headers := make(nats.Header)
	for k, v := range env.OriginalHeaders {
		if strings.TrimSpace(k) == "" {
			continue
		}
		headers.Set(k, v)
	}
	headers.Set(dlqHeaderReprocessedFrom, env.ID)

	originalMsgID := headers.Get("Nats-Msg-Id")
	if originalMsgID == "" {
		originalMsgID = env.ID
	}
	if originalMsgID != "" {
		headers.Set("Nats-Msg-Id", fmt.Sprintf("%s.reprocess.%d", originalMsgID, time.Now().UnixNano()))
	}

	return &nats.Msg{
		Subject: env.OriginalSubject,
		Data:    append([]byte(nil), env.Payload...),
		Header:  headers,
	}, nil
}

func headerToMap(headers nats.Header) map[string]string {
	result := make(map[string]string, len(headers))
	for key := range headers {
		result[key] = headers.Get(key)
	}
	return result
}
