package nats

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	result, err := c.ReprocessDLQSelected(ctx, nil, ports.DLQFilter{}, 100)
	if err != nil {
		return 0, err
	}
	return result.Count, nil
}

func (c *Client) PreviewDLQ(ctx context.Context, ids []string, filter ports.DLQFilter, maxCount uint32) ([]ports.DLQPreviewItem, error) {
	stream, err := c.js.Stream(ctx, c.dlqStreamName())
	if err != nil {
		return nil, fmt.Errorf("failed to bind DLQ stream %s: %w", c.dlqStreamName(), err)
	}

	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubjects:    []string{"dlq.>"},
		AckPolicy:         jetstream.AckNonePolicy,
		ReplayPolicy:      jetstream.ReplayInstantPolicy,
		InactiveThreshold: 10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to setup DLQ preview consumer: %w", err)
	}

	fetchCount := int(maxCount)
	if fetchCount <= 0 || fetchCount > 100 {
		fetchCount = 100
	}
	batch, err := consumer.Fetch(fetchCount, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		return nil, err
	}

	selected := idSet(ids)
	items := make([]ports.DLQPreviewItem, 0, fetchCount)
	for msg := range batch.Messages() {
		env, err := decodeDLQEnvelope(msg)
		if err != nil || !dlqEnvelopeMatches(env, selected, filter) {
			continue
		}
		item := dlqPreviewItem(env)
		if meta, err := msg.Metadata(); err == nil && meta != nil {
			item.MessageSequence = meta.Sequence.Stream
			item.MessageTimestamp = meta.Timestamp.UnixMilli()
		}
		items = append(items, item)
		if len(items) >= fetchCount {
			break
		}
	}
	return items, nil
}

func (c *Client) ReprocessDLQSelected(ctx context.Context, ids []string, filter ports.DLQFilter, maxCount uint32) (ports.DLQReprocessResult, error) {
	stream, err := c.js.Stream(ctx, c.dlqStreamName())
	if err != nil {
		return ports.DLQReprocessResult{}, fmt.Errorf("failed to bind DLQ stream %s: %w", c.dlqStreamName(), err)
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
		return ports.DLQReprocessResult{}, fmt.Errorf("failed to setup DLQ consumer: %w", err)
	}

	fetchCount := int(maxCount)
	if fetchCount <= 0 || fetchCount > 100 {
		fetchCount = 100
	}
	batch, err := consumer.Fetch(fetchCount, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		return ports.DLQReprocessResult{}, err
	}

	selected := idSet(ids)
	result := ports.DLQReprocessResult{}
	for msg := range batch.Messages() {
		env, err := decodeDLQEnvelope(msg)
		if err != nil {
			_ = msg.TermWithReason("invalid DLQ envelope: " + err.Error())
			continue
		}
		if !dlqEnvelopeMatches(env, selected, filter) {
			result.SkippedDLQIDs = append(result.SkippedDLQIDs, env.ID)
			continue
		}

		reprocessMsg, err := buildReprocessMsg(env)
		if err != nil {
			_ = msg.TermWithReason("invalid DLQ envelope: " + err.Error())
			result.FailedDLQIDs = append(result.FailedDLQIDs, env.ID)
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
			result.FailedDLQIDs = append(result.FailedDLQIDs, env.ID)
			continue
		}

		_ = msg.Ack()
		result.Count++
		result.ReprocessedDLQIDs = append(result.ReprocessedDLQIDs, env.ID)
	}

	return result, nil
}

func idSet(ids []string) map[string]bool {
	if len(ids) == 0 {
		return nil
	}
	result := make(map[string]bool, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id != "" {
			result[id] = true
		}
	}
	return result
}

func decodeDLQEnvelope(msg jetstream.Msg) (DLQEnvelope, error) {
	var env DLQEnvelope
	if err := json.Unmarshal(msg.Data(), &env); err != nil {
		return DLQEnvelope{}, err
	}
	return env, nil
}

func dlqPreviewItem(env DLQEnvelope) ports.DLQPreviewItem {
	risk := "none"
	if strings.TrimSpace(env.MsgID) != "" {
		risk = "possible"
	}
	return ports.DLQPreviewItem{
		DLQID:           env.ID,
		OriginalSubject: env.OriginalSubject,
		Reason:          env.Reason,
		ErrorClass:      env.ErrorClass,
		DuplicateRisk:   risk,
		ReplayTarget:    env.OriginalSubject,
	}
}

func dlqEnvelopeMatches(env DLQEnvelope, selected map[string]bool, filter ports.DLQFilter) bool {
	if len(selected) > 0 && !selected[env.ID] {
		return false
	}
	if filter.OriginalTopic != "" && !subjectHasPrefix(env.OriginalSubject, filter.OriginalTopic) {
		return false
	}
	if filter.OriginalPartition != "" && !dlqPartitionMatches(env, filter.OriginalPartition) {
		return false
	}
	if filter.SourceID != "" && env.SourceID != filter.SourceID {
		return false
	}
	if filter.Schema != "" && env.Schema != filter.Schema {
		return false
	}
	if filter.Table != "" && env.Table != filter.Table {
		return false
	}
	if filter.Op != "" && env.Op != filter.Op {
		return false
	}
	if filter.ErrorClass != "" && env.ErrorClass != filter.ErrorClass {
		return false
	}
	if filter.ReasonContains != "" && !containsFold(env.Reason, filter.ReasonContains) {
		return false
	}
	if filter.HeaderKey != "" {
		got, ok := lookupHeader(env.OriginalHeaders, filter.HeaderKey)
		if !ok {
			return false
		}
		if filter.HeaderValue != "" && got != filter.HeaderValue {
			return false
		}
	}
	if filter.JSONPath != "" {
		value, ok := jsonPathValue(env.Payload, filter.JSONPath)
		if !ok {
			return false
		}
		if filter.JSONEquals != "" && !jsonValueEquals(value, filter.JSONEquals) {
			return false
		}
	}
	if filter.TextContains != "" &&
		!containsFold(env.OriginalSubject, filter.TextContains) &&
		!containsFold(env.Reason, filter.TextContains) &&
		!bytes.Contains(bytes.ToLower(env.Payload), []byte(strings.ToLower(filter.TextContains))) {
		return false
	}
	return true
}

func dlqPartitionMatches(env DLQEnvelope, partition string) bool {
	if got, ok := lookupHeader(env.OriginalHeaders, constant.HeaderPartition); ok && got == partition {
		return true
	}
	parts := strings.Split(env.OriginalSubject, ".")
	return len(parts) > 0 && parts[len(parts)-1] == strings.Trim(partition, ".")
}

func containsFold(haystack string, needle string) bool {
	return strings.Contains(strings.ToLower(haystack), strings.ToLower(strings.TrimSpace(needle)))
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
		originalMsgID = firstNonEmpty(env.MsgID, env.ID, env.OriginalSubject)
	}
	if originalMsgID != "" {
		headers.Set("Nats-Msg-Id", deterministicReprocessID(originalMsgID, env.RetryCount+1))
	}

	return &nats.Msg{
		Subject: env.OriginalSubject,
		Data:    append([]byte(nil), env.Payload...),
		Header:  headers,
	}, nil
}

func deterministicReprocessID(originalMsgID string, attempt uint64) string {
	originalMsgID = strings.TrimSpace(originalMsgID)
	if originalMsgID == "" {
		originalMsgID = "unknown"
	}
	if attempt == 0 {
		attempt = 1
	}

	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d", originalMsgID, attempt)))
	return fmt.Sprintf("%s.reprocess.%d.%x", compactMsgIDComponent(originalMsgID), attempt, sum[:8])
}

func compactMsgIDComponent(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}

	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
		if b.Len() >= 80 {
			break
		}
	}
	if b.Len() == 0 {
		return "unknown"
	}
	return b.String()
}

func headerToMap(headers nats.Header) map[string]string {
	result := make(map[string]string, len(headers))
	for key := range headers {
		result[key] = headers.Get(key)
	}
	return result
}
