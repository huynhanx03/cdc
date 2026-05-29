// Package flow provides the Flow Manager and FlowWorker for orchestrating CDC flow lifecycle operations.
package flow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/bytedance/sonic"

	sinkcommon "github.com/foden/cdc/internal/adapters/driven/connector/sink/common"
	"github.com/foden/cdc/internal/adapters/driven/metrics"
	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/domain"
	"github.com/foden/cdc/internal/core/ports"
	coreruntime "github.com/foden/cdc/internal/core/runtime"
	cdcerrors "github.com/foden/cdc/pkg/errors"
	"github.com/foden/cdc/pkg/pool"
	"github.com/foden/cdc/pkg/retry"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/panjf2000/ants/v2"
)

// defaultPoolSize is the default number of goroutines in a flow's ants pool.
const defaultPoolSize = 4

// defaultMaxDeliver matches the config package default for NATS max_deliver.
const defaultMaxDeliver = 5

const (
	defaultWorkerBatchSize     = 100
	defaultWorkerFlushInterval = time.Second
	backpressureWait           = 50 * time.Millisecond
	fetchErrorBackoff          = 500 * time.Millisecond
)

var newAntsPool = ants.NewPool

// FlowWorker processes events for a single flow using a dedicated ants pool.
// Each FlowWorker owns a NATS durable consumer filtered to its source table
// and submits batch processing tasks to its ants pool.
type flowPoolManager interface {
	CreatePool(flowID string, size int) (*ants.Pool, error)
	ReleasePool(flowID string)
}

type FlowWorker struct {
	flow           *FlowConfig
	sink           FlowSink
	pool           *ants.Pool
	poolManager    flowPoolManager
	store          ports.Store
	natsClient     ports.NATSClient
	runtimeMetrics *coreruntime.Metrics
	filter         *Filter
	mappings       []ports.ColumnMapping
	maxDeliver     int
	log            *slog.Logger
	cancel         context.CancelFunc
	stopped        chan struct{}
}

// StartFlowWorker creates a NATS durable consumer and ants pool for the flow,
// then starts the main processing loop in a goroutine.
func StartFlowWorker(
	ctx context.Context,
	flow *FlowConfig,
	sink FlowSink,
	poolManager flowPoolManager,
	store ports.Store,
	natsClient ports.NATSClient,
	maxDeliver int,
	runtimeMetrics *coreruntime.Metrics,
) (*FlowWorker, error) {
	ctx, cancel := context.WithCancel(ctx)

	// Create component-scoped logger with flow context
	log := slog.With(
		"component", "flow_worker",
		"flow_id", flow.FlowID,
		"source_table", flow.SourceTable,
		"sink_table", flow.SinkTable,
	)

	// Parse and compile filter expression
	var filter *Filter
	if flow.Options != nil && flow.Options.FilterExpression != "" {
		var err error
		filter, err = NewFilter(flow.Options.FilterExpression)
		if err != nil {
			log.Error("failed to compile filter expression, using pass-all",
				"expression", flow.Options.FilterExpression,
				"err", err)
			filter = nil
		}
	}

	// Determine pool size: default to partition count for 1:1 ordering guarantee
	poolSize := defaultPoolSize
	if flow.Options != nil && flow.Options.PoolSize > 0 {
		poolSize = flow.Options.PoolSize
	} else if flow.Options != nil && flow.Options.PartitionCount > 0 {
		poolSize = flow.Options.PartitionCount
	}
	if maxDeliver <= 0 {
		maxDeliver = defaultMaxDeliver
	}

	// Get or create ants pool from PoolManager
	antsPool, err := poolManager.CreatePool(flow.FlowID, poolSize)
	if err != nil {
		log.Error("failed to create shared ants pool, using isolated pool",
			"pool_size", poolSize,
			"err", err)
		// Keep the worker isolated if the shared pool manager rejects this flow.
		antsPool, err = newAntsPool(poolSize)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("create isolated ants pool: %w", err)
		}
	}

	w := &FlowWorker{
		flow:           flow,
		sink:           sink,
		pool:           antsPool,
		poolManager:    poolManager,
		store:          store,
		natsClient:     natsClient,
		runtimeMetrics: runtimeMetrics,
		filter:         filter,
		mappings:       flow.ColumnMappings,
		maxDeliver:     maxDeliver,
		log:            log,
		cancel:         cancel,
		stopped:        make(chan struct{}),
	}

	go w.run(ctx)
	return w, nil
}

// flowConsumerName returns the NATS durable consumer name for this flow.
func flowConsumerName(flowID string) string {
	return fmt.Sprintf("flow-%s", flowID)
}

// run is the main processing loop: fetch batch from NATS → submit to ants pool.
func (w *FlowWorker) run(ctx context.Context) {
	defer close(w.stopped)

	// Parse source_table to get schema.table
	schema, table := parseSourceTable(w.flow.SourceTable)

	filterSubject := CDCFilterSubject(w.flow.SourceID, schema, table)
	consumerName := flowConsumerName(w.flow.FlowID)

	consumer, err := w.natsClient.CreateOrUpdateConsumer(ctx, consumerName, []string{filterSubject})
	if err != nil {
		w.log.Error("failed to create consumer",
			"consumer", consumerName,
			"filter", filterSubject,
			"err", err)
		return
	}

	// Determine batch size
	batchSize := defaultWorkerBatchSize
	if w.flow.Options != nil && w.flow.Options.BatchSize > 0 {
		batchSize = int(w.flow.Options.BatchSize)
	}

	// Determine flush interval (used as FetchMaxWait)
	flushInterval := defaultWorkerFlushInterval
	if w.flow.Options != nil && w.flow.Options.FlushIntervalMs > 0 {
		flushInterval = time.Duration(w.flow.Options.FlushIntervalMs) * time.Millisecond
	}

	w.log.Info("worker started",
		"consumer", consumerName,
		"filter", filterSubject,
		"batch_size", batchSize,
		"flush_interval", flushInterval,
		"pool_size", w.pool.Cap())

	for {
		select {
		case <-ctx.Done():
			w.log.Info("worker stopping due to context cancellation")
			return
		default:
		}

		if w.pool.Free() <= 0 {
			metrics.FlowBackpressureTotal.WithLabelValues(w.flow.FlowID).Inc()
			if w.runtimeMetrics != nil {
				w.runtimeMetrics.RecordBackpressure(w.flow.FlowID, 1)
			}
			time.Sleep(backpressureWait)
			continue
		}

		select {
		case <-ctx.Done():
			w.log.Info("worker stopping before fetch")
			return
		default:
		}

		// Fetch a batch of messages from the NATS consumer
		msgBatch, err := consumer.Fetch(batchSize, jetstream.FetchMaxWait(flushInterval))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Transient fetch error — back off briefly
			time.Sleep(fetchErrorBackoff)
			continue
		}

		// Collect messages from the batch channel
		msgs := make([]jetstream.Msg, 0, batchSize)
		for msg := range msgBatch.Messages() {
			msgs = append(msgs, msg)
		}

		if len(msgs) == 0 {
			continue
		}

		select {
		case <-ctx.Done():
			for _, msg := range msgs {
				_ = msg.Nak()
			}
			return
		default:
		}

		// Submit batch processing as a task to the ants pool
		batchMsgs := msgs // capture for closure
		err = w.pool.Submit(func() {
			w.processBatch(ctx, batchMsgs)
		})
		if err != nil {
			w.log.Error("failed to submit task to pool, NAKing batch",
				"batch_size", len(batchMsgs),
				"err", err)
			// NAK all messages so they can be redelivered
			for _, msg := range batchMsgs {
				_ = msg.Nak()
			}
		}

		// Update pool metrics
		metrics.FlowWorkerPoolActive.WithLabelValues(w.flow.FlowID).Set(float64(w.pool.Running()))
		metrics.FlowWorkerPoolCapacity.WithLabelValues(w.flow.FlowID).Set(float64(w.pool.Cap()))
	}
}

// processBatch handles a batch of NATS messages:
// extract metadata → apply filter → apply column mapping → deep clone → write to sink → ACK/NAK.
func (w *FlowWorker) processBatch(ctx context.Context, msgs []jetstream.Msg) {
	batchStart := time.Now()
	events := make([]*domain.Event, 0, len(msgs))
	passedMsgs := make([]jetstream.Msg, 0, len(msgs))
	poolEvents := make([]*domain.Event, 0, len(msgs))

	for _, msg := range msgs {
		// Extract event metadata from NATS headers (zero-unmarshal pattern)
		ev := w.parseEventFromMsg(msg)

		if w.filter != nil {
			passed, err := w.filter.Evaluate(ev)
			if err != nil {
				w.log.Warn("filter failed, moving event to DLQ",
					"err", err,
					"offset", ev.Offset)
				if dlqErr := w.moveToDLQ(ctx, msg, ev, cdcerrors.DLQErrorFilter, fmt.Sprintf("filter_error: %s", err.Error()), 0); dlqErr != nil {
					w.log.Error("failed to move filter error to DLQ", "err", dlqErr, "offset", ev.Offset)
					_ = msg.Nak()
				} else {
					w.recordDLQ(cdcerrors.DLQErrorFilter, 1)
				}
				pool.PutEvent(ev)
				continue
			}
			if !passed {
				// Event filtered out — ACK it (consumed but not written)
				if w.runtimeMetrics != nil {
					w.runtimeMetrics.RecordFlowFiltered(w.flow.FlowID, 1)
				}
				_ = msg.Ack()
				pool.PutEvent(ev)
				continue
			}
		}

		// Apply column mapping to event data
		if len(w.mappings) > 0 && len(ev.Data) > 0 {
			mapped, err := ApplyColumnMappings(ev.Data, w.mappings)
			if err != nil {
				w.log.Warn("column mapping failed, moving event to DLQ",
					"err", err,
					"offset", ev.Offset)
				if w.runtimeMetrics != nil {
					w.runtimeMetrics.RecordFlowFailure(
						w.flow.FlowID,
						w.flow.SourceID,
						w.flow.SinkID,
						metrics.ReasonMappingError,
						err.Error(),
						1,
					)
				}
				if dlqErr := w.moveToDLQ(ctx, msg, ev, cdcerrors.DLQErrorMapping, fmt.Sprintf("mapping_error: %s", err.Error()), 0); dlqErr != nil {
					w.log.Error("failed to move mapping error to DLQ", "err", dlqErr, "offset", ev.Offset)
					_ = msg.Nak()
				} else {
					w.recordDLQ(cdcerrors.DLQErrorMapping, 1)
				}
				pool.PutEvent(ev)
				continue
			}
			ev.Data = mapped
		}

		applySinkTable(ev, w.flow.SinkTable)
		events = append(events, ev)
		passedMsgs = append(passedMsgs, msg)
		poolEvents = append(poolEvents, ev)
	}

	if len(events) == 0 {
		return
	}

	start := time.Now()
	result := w.writeBatchWithIsolation(ctx, events, passedMsgs)
	duration := time.Since(start)
	metrics.SinkWriteDuration.WithLabelValues(w.sink.InstanceID(), w.sinkType()).Observe(duration.Seconds())

	if len(result.retryMsgs) > 0 || len(result.dlqMsgs) > 0 {
		w.log.Error("sink write failed",
			"batch_size", len(events),
			"err", result.err)
		failedCount := len(result.retryMsgs) + len(result.dlqMsgs)
		metrics.FlowEventsProcessed.WithLabelValues(w.flow.FlowID, metrics.StatusFailure).Add(float64(failedCount))
		if w.runtimeMetrics != nil {
			w.runtimeMetrics.RecordFlowFailure(
				w.flow.FlowID,
				w.flow.SourceID,
				w.flow.SinkID,
				metrics.ReasonSinkWriteFailed,
				result.err.Error(),
				uint64(failedCount),
			)
		}
		if len(result.retryMsgs) > 0 {
			w.handleFailure(ctx, result.retryMsgs, result.retryEvents, result.err)
		}
		for i, msg := range result.dlqMsgs {
			var ev *domain.Event
			if i < len(result.dlqEvents) {
				ev = result.dlqEvents[i]
			}
			if dlqErr := w.moveToDLQ(ctx, msg, ev, cdcerrors.DLQErrorSink, result.err.Error(), deliveryCount(msg)); dlqErr != nil {
				w.log.Error("failed to move isolated sink error to DLQ", "err", dlqErr)
				_ = msg.Nak()
			} else {
				w.recordDLQ(metrics.ReasonIsolatedSinkError, 1)
			}
		}
		// Return original events to pool
		for _, ev := range poolEvents {
			pool.PutEvent(ev)
		}
		events = result.successEvents
		passedMsgs = result.successMsgs
		if len(events) == 0 {
			return
		}
	}

	// Record per-flow metrics
	metrics.FlowEventsProcessed.WithLabelValues(w.flow.FlowID, metrics.StatusSuccess).Add(float64(len(events)))
	metrics.FlowBatchSize.WithLabelValues(w.flow.FlowID).Observe(float64(len(events)))
	metrics.FlowProcessingDuration.WithLabelValues(w.flow.FlowID).Observe(time.Since(batchStart).Seconds())

	if w.runtimeMetrics != nil {
		lastEvent := events[len(events)-1]
		w.runtimeMetrics.RecordSinkWrite(
			w.flow.FlowID,
			w.flow.SourceID,
			w.flow.SinkID,
			uint64(len(events)),
			duration.Milliseconds(),
			eventTimestampMs(lastEvent),
		)
	}

	// ACK after sink success, then persist the flow checkpoint for replay/retention control.
	lastEvent := events[len(events)-1]
	for _, msg := range passedMsgs {
		if err := msg.Ack(); err != nil {
			w.log.Error("failed to ack message",
				"offset", lastEvent.Offset,
				"err", err)
			if w.runtimeMetrics != nil {
				w.runtimeMetrics.RecordFlowFailure(
					w.flow.FlowID,
					w.flow.SourceID,
					w.flow.SinkID,
					metrics.ReasonMessageAckFailed,
					err.Error(),
					uint64(len(events)),
				)
			}
			for _, ev := range poolEvents {
				pool.PutEvent(ev)
			}
			return
		}
	}

	if lastEvent.Offset != "" {
		checkpoint := &domain.Checkpoint{
			FlowID:      w.flow.FlowID,
			SourceID:    w.flow.SourceID,
			Schema:      lastEvent.Schema,
			Table:       lastEvent.Table,
			Partition:   lastEvent.Partition,
			Position:    lastEvent.Offset,
			LastEventID: lastEvent.MessageID,
		}
		if err := w.store.SaveCheckpoint(ctx, checkpoint); err != nil {
			w.log.Error("failed to save checkpoint",
				"offset", lastEvent.Offset,
				"err", err)
			if w.runtimeMetrics != nil {
				w.runtimeMetrics.RecordFlowFailure(
					w.flow.FlowID,
					w.flow.SourceID,
					w.flow.SinkID,
					metrics.ReasonCheckpointSaveFailed,
					err.Error(),
					uint64(len(events)),
				)
			}
			for _, ev := range poolEvents {
				pool.PutEvent(ev)
			}
			return
		}
		if w.runtimeMetrics != nil {
			w.runtimeMetrics.RecordCheckpointSave(w.flow.FlowID, 1)
		}
		metrics.FlowCheckpointSavedTotal.WithLabelValues(w.flow.FlowID).Inc()
	}

	w.log.Debug("batch processed",
		"count", len(events),
		"last_offset", lastEvent.Offset)

	// Return original events to pool
	for _, ev := range poolEvents {
		pool.PutEvent(ev)
	}
}

type sinkWriteResult struct {
	successEvents []*domain.Event
	successMsgs   []jetstream.Msg
	retryEvents   []*domain.Event
	retryMsgs     []jetstream.Msg
	dlqEvents     []*domain.Event
	dlqMsgs       []jetstream.Msg
	err           error
}

func (w *FlowWorker) writeBatchWithIsolation(ctx context.Context, events []*domain.Event, msgs []jetstream.Msg) sinkWriteResult {
	result := sinkWriteResult{err: nil}
	w.writeBatchSegment(ctx, events, msgs, &result)
	return result
}

func (w *FlowWorker) writeBatchSegment(ctx context.Context, events []*domain.Event, msgs []jetstream.Msg, result *sinkWriteResult) {
	if len(events) == 0 {
		return
	}
	classifiedErr := w.writeBatchWithRetry(ctx, events)
	if classifiedErr == nil {
		result.successEvents = append(result.successEvents, events...)
		result.successMsgs = append(result.successMsgs, msgs...)
		return
	}
	result.err = classifiedErr
	var sinkErr *sinkcommon.SinkError
	if !errors.As(classifiedErr, &sinkErr) || sinkErr.Retryable {
		result.retryEvents = append(result.retryEvents, events...)
		result.retryMsgs = append(result.retryMsgs, msgs...)
		return
	}
	if len(events) == 1 {
		result.dlqEvents = append(result.dlqEvents, events...)
		result.dlqMsgs = append(result.dlqMsgs, msgs...)
		return
	}
	mid := len(events) / 2
	w.writeBatchSegment(ctx, events[:mid], msgs[:mid], result)
	w.writeBatchSegment(ctx, events[mid:], msgs[mid:], result)
}

func (w *FlowWorker) writeBatchWithRetry(ctx context.Context, events []*domain.Event) error {
	var classifiedErr error
	err := retry.Do(ctx, retry.DefaultConfig(), func() error {
		writeErr := w.sink.WriteBatch(ctx, events)
		if writeErr == nil {
			return nil
		}
		classifiedErr = sinkcommon.ClassifySinkError(writeErr)
		var sinkErr *sinkcommon.SinkError
		if errors.As(classifiedErr, &sinkErr) && !sinkErr.Retryable {
			return cdcerrors.Permanent(classifiedErr)
		}
		return classifiedErr
	})
	if err == nil {
		return nil
	}
	if classifiedErr == nil {
		return err
	}
	return classifiedErr
}

func deliveryCount(msg jetstream.Msg) uint64 {
	if msg == nil {
		return 0
	}
	meta, err := msg.Metadata()
	if err != nil || meta == nil {
		return 0
	}
	return meta.NumDelivered
}

func applySinkTable(event *domain.Event, sinkTable string) {
	if event == nil || sinkTable == "" {
		return
	}
	schema, table := parseSourceTable(sinkTable)
	event.Schema = schema
	event.Table = table
}

// handleFailure handles batch write failures by NAKing messages or routing to DLQ
// if max retries have been exceeded.
func (w *FlowWorker) handleFailure(ctx context.Context, msgs []jetstream.Msg, batchEvents []*domain.Event, failureErr error) {
	var sinkErr *sinkcommon.SinkError
	nonRetryable := errors.As(failureErr, &sinkErr) && !sinkErr.Retryable

	for i, msg := range msgs {
		var ev *domain.Event
		if i < len(batchEvents) {
			ev = batchEvents[i]
		}
		// Check delivery count from message metadata
		metadata, err := msg.Metadata()
		if err != nil {
			// Can't determine delivery count — NAK for retry
			_ = msg.Nak()
			continue
		}

		if nonRetryable || int(metadata.NumDelivered) >= w.maxDeliver {
			// Max retries exceeded — route to DLQ
			reason := fmt.Sprintf("max deliveries (%d) exceeded for flow %s", w.maxDeliver, w.flow.FlowID)
			metricReason := metrics.ReasonMaxRetriesExceeded
			if nonRetryable {
				reason = failureErr.Error()
				if sinkErr != nil && sinkErr.Reason != "" {
					metricReason = sinkErr.Reason
				} else {
					metricReason = metrics.ReasonNonRetryable
				}
			}
			if dlqErr := w.moveToDLQ(ctx, msg, ev, cdcerrors.DLQErrorSink, reason, metadata.NumDelivered); dlqErr != nil {
				w.log.Error("failed to move message to DLQ",
					"err", dlqErr,
					"delivery_count", metadata.NumDelivered)
				// Last resort: NAK so it's not lost
				_ = msg.Nak()
			} else {
				w.recordDLQ(metricReason, 1)
				w.log.Warn("message moved to DLQ",
					"delivery_count", metadata.NumDelivered,
					"subject", msg.Subject())
			}
		} else {
			// NAK for retry
			metrics.FlowRetryTotal.WithLabelValues(w.flow.FlowID, w.flow.SinkID, metrics.ReasonSinkRetry).Inc()
			if w.runtimeMetrics != nil {
				w.runtimeMetrics.RecordRetry(w.flow.FlowID, w.flow.SinkID, metrics.ReasonSinkRetry, 1)
			}
			_ = msg.Nak()
		}
	}
}

func (w *FlowWorker) sinkType() string {
	if w == nil || w.sink == nil || w.sink.Type() == "" {
		return metrics.SinkTypeUnknown
	}
	return w.sink.Type()
}

func (w *FlowWorker) recordDLQ(reason string, count uint64) {
	if count == 0 {
		return
	}
	metrics.DLQEventsTotal.WithLabelValues(w.flow.FlowID, reason).Add(float64(count))
	if w.runtimeMetrics != nil {
		w.runtimeMetrics.RecordDLQ(w.flow.FlowID, w.flow.SinkID, reason, count)
	}
}

func (w *FlowWorker) moveToDLQ(ctx context.Context, msg jetstream.Msg, ev *domain.Event, errorClass string, reason string, retryCount uint64) error {
	opts := ports.DLQMoveOptions{
		FlowID:     w.flow.FlowID,
		SourceID:   w.flow.SourceID,
		SinkID:     w.flow.SinkID,
		Reason:     reason,
		ErrorClass: errorClass,
		RetryCount: retryCount,
		Timestamp:  time.Now().UnixMilli(),
	}
	if ev != nil {
		opts.Schema = ev.Schema
		opts.Table = ev.Table
		opts.Op = ev.Op.String()
		opts.MsgID = ev.MessageID
	}
	return w.natsClient.MoveToDLQ(ctx, msg, opts)
}

func eventTimestampMs(ev *domain.Event) int64 {
	if ev == nil || ev.TimestampMS <= 0 {
		return time.Now().UnixMilli()
	}
	return ev.TimestampMS
}

func timestampMSFromData(data []byte) int64 {
	if len(data) == 0 {
		return 0
	}
	node, err := sonic.Get(data, "ts_ms")
	if err != nil || !node.Exists() {
		return 0
	}
	ts, err := node.Int64()
	if err != nil || ts <= 0 {
		return 0
	}
	return ts
}

// parseEventFromMsg extracts event metadata from NATS message headers.
// Uses the zero-unmarshal pattern: routing metadata is in headers,
// the raw payload stays as []byte without deserialization.
func (w *FlowWorker) parseEventFromMsg(msg jetstream.Msg) *domain.Event {
	ev := pool.GetEvent()
	headers := msg.Headers()

	ev.InstanceID = headers.Get(constant.HeaderInstanceID)
	ev.Offset = headers.Get(constant.HeaderOffset)
	ev.Schema = headers.Get(constant.HeaderSchema)
	ev.Table = headers.Get(constant.HeaderTable)
	ev.Op = constant.Op(headers.Get(constant.HeaderOp))
	ev.Data = msg.Data()
	ev.MessageID = headers.Get("Nats-Msg-Id")
	if partitionStr := headers.Get(constant.HeaderPartition); partitionStr != "" {
		ev.Partition, _ = strconv.Atoi(partitionStr)
	}

	if lsnStr := headers.Get(constant.HeaderLSN); lsnStr != "" {
		ev.LSN, _ = strconv.ParseUint(lsnStr, 10, 64)
	}
	ev.TimestampMS = timestampMSFromData(ev.Data)

	return ev
}

// Stop cancels the worker context, releases the ants pool via PoolManager,
// and waits for the run loop to exit.
func (w *FlowWorker) Stop() {
	w.cancel()
	w.poolManager.ReleasePool(w.flow.FlowID)
	<-w.stopped
	w.log.Info("worker stopped")
}
