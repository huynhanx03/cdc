package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/pkg/constant"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/nats"
	"github.com/foden/cdc/pkg/metrics"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	_nameErr        = "err"
	_nameBufferSize = "buffer_size"
	// How many events the worker tries to pull from the WAL in one go
	_batchSize = 100
	// Max time a worker waits before flushing a partial batch
	_flushInterval = 1 * time.Second
)

// Engine is the core CDC pipeline that connects a Source to multiple Sinks
// using a WAL-backed queue for reliability.
type Engine struct {
	mu             sync.RWMutex
	sources        []interfaces.Source
	sinks          []interfaces.Sink
	natsClient     *nats.Client
	sourceAckChs   map[string]chan uint64
	eventCh        chan *models.Event

	stopCh         chan struct{}
	partitionCount int
	wg             sync.WaitGroup

	// Metrics
	metricsMu   sync.RWMutex
	sourceStats map[string]*models.ComponentStats
	sinkStats   map[string]*models.ComponentStats

	// Sink workers
	sinkCancels map[string]context.CancelFunc
}

// NewEngine creates a pipeline engine with configurable buffer size and partition count.
func NewEngine(bufferSize, partitionCount int, sources []interfaces.Source, sinks []interfaces.Sink, natsClient *nats.Client) *Engine {
	return &Engine{
		sources:        sources,
		sinks:          sinks,
		natsClient:     natsClient,
		sourceAckChs:   make(map[string]chan uint64),
		eventCh:        make(chan *models.Event, bufferSize),
		stopCh:         make(chan struct{}),
		partitionCount: partitionCount,
		sourceStats:    make(map[string]*models.ComponentStats),
		sinkStats:      make(map[string]*models.ComponentStats),
		sinkCancels:    make(map[string]context.CancelFunc),
	}
}

// Start launches the producer and worker pool, then starts all sources.
func (e *Engine) Start() error {
	slog.Info("starting pipeline engine", "sources", len(e.sources), "partitions", e.partitionCount, _nameBufferSize, cap(e.eventCh))

	// 1. Pipeline Producer (Reads from eventCh, writes to NATS)
	e.wg.Add(1)
	go e.producer()
	metrics.ActiveWorkers.WithLabelValues("producer").Inc()

	// 2. Start workers for existing sinks
	e.mu.Lock()
	if e.sinkCancels == nil {
		e.sinkCancels = make(map[string]context.CancelFunc)
	}
	for _, sink := range e.sinks {
		e.startSinkWorkersUnlocked(sink)
	}
	e.mu.Unlock()

	// 3. Start all sources
	for _, src := range e.sources {
		instanceID := src.InstanceID()
		ackCh := make(chan uint64, 1000)
		e.sourceAckChs[instanceID] = ackCh

		// Fetch initial offset from NATS KV for resumption
		offset, err := e.natsClient.GetOffset(context.Background(), instanceID)
		if err != nil {
			slog.Warn("failed to fetch initial offset", "instance_id", instanceID, _nameErr, err)
		}

		if err := src.Start(e.eventCh, ackCh, offset); err != nil {
			return fmt.Errorf("failed to start source %s: %w", instanceID, err)
		}
	}

	return nil
}

// producer consumes from eventCh (from Source) and pushes to hierarchical subject
func (e *Engine) producer() {
	defer e.wg.Done()
	slog.Debug("pipeline producer started")

	for ev := range e.eventCh {
		topic := ev.Topic
		if topic == "" {
			topic = "cdc" // fallback
		}
		
		subject := fmt.Sprintf("%s.%s.%s.%s",
			topic,
			ev.InstanceID,
			strings.ReplaceAll(ev.Database, ".", "_"),
			strings.ReplaceAll(ev.Table, ".", "_"))

		if err := e.natsClient.Publish(context.Background(), subject, ev); err != nil {
			slog.Error("failed to publish to NATS", _nameErr, err)
			e.updateSourceStats(ev.InstanceID, false, err.Error())
		} else {
			e.updateSourceStats(ev.InstanceID, true, "")
		}
	}
	slog.Debug("pipeline producer exited")
}

// startSinkWorkersUnlocked starts consumer workers for a specific sink.
func (e *Engine) startSinkWorkersUnlocked(sink interfaces.Sink) {
	ctx, cancel := context.WithCancel(context.Background())
	e.sinkCancels[sink.InstanceID()] = cancel

	for i := 0; i < e.partitionCount; i++ {
		e.wg.Add(1)
		go e.worker(ctx, sink, i)
		metrics.ActiveWorkers.WithLabelValues(fmt.Sprintf("worker-%s", sink.InstanceID())).Inc()
	}
}

// worker consumes events from filtered subjects and sends them to the given sink.
func (e *Engine) worker(ctx context.Context, sink interfaces.Sink, partID int) {
	defer e.wg.Done()
	slog.Debug("pipeline worker started", "worker_id", partID, "sink", sink.InstanceID())

	// Use a shared consumer group name unique to this sink to allow multiple sink workers
	// to share load, while different sinks consume independently.
	consumerName := fmt.Sprintf("pipeline-worker-%s", sink.InstanceID())
	topicPattern := []string{sink.Topic()}

	consumer, err := e.natsClient.CreateOrUpdateConsumer(ctx, consumerName, topicPattern)
	if err != nil {
		slog.Error("failed to create NATS consumer", "worker_id", partID, "sink", sink.InstanceID(), _nameErr, err)
		return
	}

	iter, err := consumer.Messages()
	if err != nil {
		slog.Error("failed to get consumer messages", "worker_id", partID, "sink", sink.InstanceID(), _nameErr, err)
		return
	}
	defer iter.Stop()

	// Wait for global stop or context cancellation
	go func() {
		select {
		case <-e.stopCh:
		case <-ctx.Done():
		}
		iter.Stop()
	}()

	var pendingMsgs []jetstream.Msg
	var pendingLSNs []string
	flushTicker := time.NewTicker(_flushInterval)
	defer flushTicker.Stop()

	for {
		var msg jetstream.Msg
		var err error

		select {
		case <-ctx.Done():
			slog.Debug("pipeline worker exiting", "worker_id", partID, "sink", sink.InstanceID())
			return
		case <-e.stopCh:
			return
		case <-flushTicker.C:
			// Time-based flush for partial batches
			if len(pendingMsgs) > 0 {
				e.flushAndAck(sink, pendingMsgs, pendingLSNs, partID)
				pendingMsgs = nil
				pendingLSNs = nil
			}
			continue
		default:
			msg, err = iter.Next()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("failed to get next message", "worker_id", partID, "sink", sink.InstanceID(), _nameErr, err)
				time.Sleep(1 * time.Second)
				continue
			}
		}

		var ev models.Event
		if err := json.Unmarshal(msg.Data(), &ev); err != nil {
			slog.Error("failed to unmarshal NATS message", "worker_id", partID, "sink", sink.InstanceID(), _nameErr, err)
			msg.Term() // Don't retry unmarshalable junk
			continue
		}

		// Process event on this specific sink
		start := time.Now()
		if err := sink.Write(&ev); err != nil {
			slog.Error("sink write error", "worker_id", partID, "instance_id", ev.InstanceID, "table", ev.Table, _nameErr, err)
			e.updateSinkStats(sink.InstanceID(), false, err.Error())
			
			// If max retries reached, move to DLQ instead of infinite retry loop
			if meta, _ := msg.Metadata(); meta != nil && int32(meta.NumDelivered) >= sink.MaxRetries() {
				slog.Warn("max retries reached, moving to DLQ", "instance_id", ev.InstanceID, "table", ev.Table, "num_delivered", meta.NumDelivered)
				if errDLQ := e.natsClient.MoveToDLQ(ctx, msg, fmt.Sprintf("sink_error: %s", err.Error())); errDLQ != nil {
					slog.Error("failed to move to DLQ", _nameErr, errDLQ)
					msg.Nak() // Fallback to retry if DLQ failed
				} else {
					metrics.DLQEventsTotal.WithLabelValues(ev.InstanceID, sink.InstanceID(), "max_retries").Inc()
				}
				continue // MoveToDLQ already ACKs the message
			}

			// Immediately NAK this message and the current batch to trigger retry
			msg.Nak()
			for _, m := range pendingMsgs {
				m.Nak()
			}
			pendingMsgs = nil
			pendingLSNs = nil
		} else {
			duration := time.Since(start)
			metrics.SinkWriteDuration.WithLabelValues(sink.InstanceID(), sink.Type()).Observe(duration.Seconds())
			metrics.WorkerProcessDuration.WithLabelValues(sink.InstanceID()).Observe(duration.Seconds())

			e.updateSinkStats(sink.InstanceID(), true, "")

			// Collect metadata for background acknowledgement and offset persistence
			headers := msg.Headers()
			instID := headers.Get(constant.HeaderInstanceID)
			lsnVal := headers.Get(constant.HeaderLSN)

			if instID != "" && lsnVal != "" {
				pendingLSNs = append(pendingLSNs, fmt.Sprintf("%s:%s", instID, lsnVal))
			}
			pendingMsgs = append(pendingMsgs, msg)

			if len(pendingMsgs) >= _batchSize {
				e.flushAndAck(sink, pendingMsgs, pendingLSNs, partID)
				pendingMsgs = nil
				pendingLSNs = nil
			}
		}
	}
}

// flushAndAck forces a flush on a specific sink and ACKs the messages if successful.
func (e *Engine) flushAndAck(sink interfaces.Sink, msgs []jetstream.Msg, lsnKeys []string, workerID int) {
	var hasError bool
	if err := sink.Flush(); err != nil {
		slog.Error("sink flush error", "worker_id", workerID, "sink", sink.InstanceID(), _nameErr, err)
		hasError = true
	}

	if !hasError {
		// All sinks successfully persisted the batch
		// Track the latest offset for each instance in this batch
		latestOffsets := make(map[string]string)
		highestLSNs := make(map[string]uint64)

		for _, m := range msgs {
			h := m.Headers()
			instID := h.Get(constant.HeaderInstanceID)
			if instID == "" {
				m.Ack()
				continue
			}

			// Capture the latest generic offset (last one in ordered batch is latest)
			if off := h.Get(constant.HeaderOffset); off != "" {
				latestOffsets[instID] = off
			}

			// Capture the highest LSN for Postgres WAL management
			if lsnVal := h.Get(constant.HeaderLSN); lsnVal != "" {
				var lsn uint64
				fmt.Sscanf(lsnVal, "%d", &lsn)
				if current, ok := highestLSNs[instID]; !ok || lsn > current {
					highestLSNs[instID] = lsn
				}
			}

			m.Ack()
		}

		// Persist offsets to NATS KV for persistent tracking
		for instID, offset := range latestOffsets {
			if err := e.natsClient.SaveOffset(context.Background(), instID, offset); err != nil {
				slog.Error("failed to save offset in KV", "instance_id", instID, "offset", offset, _nameErr, err)
			}
		}

		// Send highest LSN back to each Source for WAL management
		for instID, lsn := range highestLSNs {
			e.mu.RLock()
			ch, ok := e.sourceAckChs[instID]
			e.mu.RUnlock()
			if ok {
				select {
				case ch <- lsn:
				default:
					slog.Warn("source ACK channel full", "instance_id", instID)
				}
			}
		}
		slog.Debug("batch flushed and acknowledged", "worker_id", workerID, "count", len(msgs))
	} else {
		// Flush failed, NAK all messages for retry
		slog.Warn("flush failed, NAKing batch for retry", "worker_id", workerID, "count", len(msgs))
		for _, m := range msgs {
			m.Nak()
		}
	}
}

// Stop performs a graceful shutdown:
// 1. Stop the source
// 2. Close event channel which stops Producer
// 3. Stop Workers
// 4. Close Sinks
func (e *Engine) Stop() {
	slog.Info("stopping pipeline engine")

	// Lock only to snapshot data and close channels — no blocking I/O under lock.
	e.mu.Lock()
	sourcesToStop := make([]interfaces.Source, len(e.sources))
	copy(sourcesToStop, e.sources)

	cancelsToCall := make([]context.CancelFunc, 0, len(e.sinkCancels))
	for _, cancel := range e.sinkCancels {
		cancelsToCall = append(cancelsToCall, cancel)
	}

	close(e.eventCh) // Stops producer
	close(e.stopCh)  // Globally stops workers
	e.mu.Unlock()

	// Blocking operations outside lock — no deadlock risk.
	for _, src := range sourcesToStop {
		if err := src.Stop(); err != nil {
			slog.Error("source stop error", "instance_id", src.InstanceID(), _nameErr, err)
		}
	}

	for _, cancel := range cancelsToCall {
		cancel()
	}

	e.wg.Wait()

	for _, s := range e.sinks {
		if err := s.Close(); err != nil {
			slog.Error("sink close error", _nameErr, err)
		}
	}
	slog.Info("pipeline engine stopped")
}

// AddSource dynamically adds and starts a new source.
func (e *Engine) AddSource(ctx context.Context, src interfaces.Source) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	instanceID := src.InstanceID()
	if _, ok := e.sourceAckChs[instanceID]; ok {
		return fmt.Errorf("source %s already exists", instanceID)
	}

	ackCh := make(chan uint64, 1000)
	e.sourceAckChs[instanceID] = ackCh

	offset, err := e.natsClient.GetOffset(ctx, instanceID)
	if err != nil {
		slog.Warn("failed to fetch initial offset for new source", "instance_id", instanceID, _nameErr, err)
	}

	if err := src.Start(e.eventCh, ackCh, offset); err != nil {
		delete(e.sourceAckChs, instanceID)
		return fmt.Errorf("failed to start source %s: %w", instanceID, err)
	}

	e.sources = append(e.sources, src)
	slog.Info("dynamically added source", "instance_id", instanceID)
	return nil
}

// RemoveSource stops and removes a source by instance ID.
func (e *Engine) RemoveSource(instanceID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var found bool
	var srcIdx int
	for i, src := range e.sources {
		if src.InstanceID() == instanceID {
			found = true
			srcIdx = i
			break
		}
	}

	if !found {
		return fmt.Errorf("source %s not found", instanceID)
	}

	src := e.sources[srcIdx]
	if err := src.Stop(); err != nil {
		slog.Error("failed to stop source", "instance_id", instanceID, _nameErr, err)
	}

	e.sources = append(e.sources[:srcIdx], e.sources[srcIdx+1:]...)
	delete(e.sourceAckChs, instanceID)

	slog.Info("dynamically removed source", "instance_id", instanceID)
	return nil
}

// AddSink dynamically adds a new sink.
func (e *Engine) AddSink(sink interfaces.Sink) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.sinkCancels == nil {
		e.sinkCancels = make(map[string]context.CancelFunc)
	}
	e.sinks = append(e.sinks, sink)
	e.startSinkWorkersUnlocked(sink)
	slog.Info("dynamically added sink", "instance_id", sink.InstanceID(), "type", sink.Type())
}

// RemoveSink stops and removes a sink by its unique instance ID.
func (e *Engine) RemoveSink(instanceID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var found bool
	var sinkIdx int
	for i, s := range e.sinks {
		if s.InstanceID() == instanceID {
			sinkIdx = i
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("sink with instance ID %s not found", instanceID)
	}

	if cancel, ok := e.sinkCancels[instanceID]; ok {
		cancel()
		delete(e.sinkCancels, instanceID)
	}

	s := e.sinks[sinkIdx]
	if err := s.Close(); err != nil {
		slog.Error("failed to close sink", "instance_id", instanceID, _nameErr, err)
	}

	e.sinks = append(e.sinks[:sinkIdx], e.sinks[sinkIdx+1:]...)
	slog.Info("dynamically removed sink", "instance_id", instanceID)
	return nil
}

// GetStats returns the current success/failure metrics.
func (e *Engine) GetStats() (map[string]*models.ComponentStats, map[string]*models.ComponentStats) {
	e.metricsMu.RLock()
	defer e.metricsMu.RUnlock()

	// Return copies
	sources := make(map[string]*models.ComponentStats)
	for k, v := range e.sourceStats {
		sources[k] = &models.ComponentStats{
			SuccessCount: v.SuccessCount,
			FailureCount: v.FailureCount,
			LastError:    v.LastError,
		}
	}

	sinks := make(map[string]*models.ComponentStats)
	for k, v := range e.sinkStats {
		sinks[k] = &models.ComponentStats{
			SuccessCount: v.SuccessCount,
			FailureCount: v.FailureCount,
			LastError:    v.LastError,
		}
	}

	return sources, sinks
}

// updateSourceStats updates stats for a specific source
func (e *Engine) updateSourceStats(instanceID string, success bool, errStr string) {
	e.metricsMu.Lock()
	defer e.metricsMu.Unlock()

	if _, ok := e.sourceStats[instanceID]; !ok {
		e.sourceStats[instanceID] = &models.ComponentStats{}
	}
	if success {
		e.sourceStats[instanceID].SuccessCount++
		metrics.EventsProducedTotal.WithLabelValues(instanceID, "success").Inc()
	} else {
		e.sourceStats[instanceID].FailureCount++
		e.sourceStats[instanceID].LastError = errStr
		metrics.EventsProducedTotal.WithLabelValues(instanceID, "failure").Inc()
		metrics.SourceErrorsTotal.WithLabelValues(instanceID, errStr).Inc()
	}
}

// updateSinkStats updates stats for a specific sink instance
func (e *Engine) updateSinkStats(sinkID string, success bool, errStr string) {
	e.metricsMu.Lock()
	defer e.metricsMu.Unlock()

	if _, ok := e.sinkStats[sinkID]; !ok {
		e.sinkStats[sinkID] = &models.ComponentStats{}
	}
	if success {
		e.sinkStats[sinkID].SuccessCount++
		metrics.EventsConsumedTotal.WithLabelValues(sinkID, "success").Inc()
	} else {
		e.sinkStats[sinkID].FailureCount++
		e.sinkStats[sinkID].LastError = errStr
		metrics.EventsConsumedTotal.WithLabelValues(sinkID, "failure").Inc()
		metrics.SinkErrorsTotal.WithLabelValues("", sinkID, errStr).Inc()
	}
}
// ListMessages returns messages from NATS with classification and optional filters
func (e *Engine) ListMessages(ctx context.Context, status cdcpb.MessageStatus, limit int, page int, topic string, partition string) ([]*models.Message, uint64, error) {
	var natsStatus models.MessageStatus
	switch status {
	case cdcpb.MessageStatus_MESSAGE_STATUS_SENT:
		natsStatus = models.MessageStatusSent
	case cdcpb.MessageStatus_MESSAGE_STATUS_UNSENT:
		natsStatus = models.MessageStatusUnsent
	case cdcpb.MessageStatus_MESSAGE_STATUS_UNSPECIFIED:
		natsStatus = models.MessageStatusAll
	default:
		natsStatus = models.MessageStatusAll
	}

	items, total, err := e.natsClient.ListMessages(ctx, natsStatus, limit, page, topic, partition)
	if err != nil {
		return nil, 0, err
	}

	msgs := make([]*models.Message, len(items))
	for i, item := range items {
		msgs[i] = &models.Message{
			Sequence:  item.Sequence,
			Timestamp: item.Timestamp,
			Subject:   item.Subject,
			Data:      item.Data,
			Headers:   item.Headers,
		}
	}

	return msgs, total, nil
}

func (e *Engine) GetConsumerInfo(ctx context.Context, consumerName string) (uint64, uint64, error) {
	return e.natsClient.GetConsumerInfo(ctx, consumerName)
}

func (e *Engine) ListTopics(ctx context.Context, limit int, page int) ([]string, uint64, error) {
	return e.natsClient.ListTopics(ctx, limit, page)
}

func (e *Engine) ListPartitions(ctx context.Context, topic string, limit int, page int) ([]string, uint64, error) {
	return e.natsClient.ListPartitions(ctx, topic, limit, page)
}
