package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cdcpb "github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/constant"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/metrics"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/nats"
	"github.com/foden/cdc/pkg/utils"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	_nameErr        = "err"
	_nameBufferSize = "buffer_size"
)

// EventFilter returns false to drop an event before publishing.
type EventFilter func(event *models.Event) bool

// EventTransformer modifies an event before publishing.
type EventTransformer func(event *models.Event) *models.Event

// Engine is the core CDC pipeline that connects a Source to multiple Sinks
// using a WAL-backed queue for reliability.
type Engine struct {
	mu           sync.RWMutex
	sources      []interfaces.Source
	sinks        []interfaces.Sink
	natsClient   *nats.Client
	sourceAckChs map[string]chan uint64
	eventCh      chan *models.Event

	stopCh         chan struct{}
	stopped        atomic.Bool
	batchSize      int
	flushInterval  time.Duration
	partitionCount int
	wg             sync.WaitGroup

	// Pipeline hooks
	filters      []EventFilter
	transformers []EventTransformer

	// Metrics
	metricsMu   sync.RWMutex
	sourceStats map[string]*models.ComponentStats
	sinkStats   map[string]*models.ComponentStats

	// Sink workers
	sinkCancels map[string]context.CancelFunc
}

// NewEngine creates a pipeline engine with configurable settings.
func NewEngine(cfg *config.Config, sources []interfaces.Source, sinks []interfaces.Sink, natsClient *nats.Client) *Engine {
	return &Engine{
		sources:        sources,
		sinks:          sinks,
		natsClient:     natsClient,
		sourceAckChs:   make(map[string]chan uint64),
		eventCh:        make(chan *models.Event, cfg.Pipeline.ChannelBufferSize),
		stopCh:         make(chan struct{}),
		partitionCount: cfg.Pipeline.WorkerCount,
		batchSize:      cfg.Pipeline.BatchSize,
		flushInterval:  time.Duration(cfg.Pipeline.FlushIntervalMs) * time.Millisecond,
		sourceStats:    make(map[string]*models.ComponentStats),
		sinkStats:      make(map[string]*models.ComponentStats),
		sinkCancels:    make(map[string]context.CancelFunc),
	}
}

// AddFilter adds a filter function that drops events returning false.
func (e *Engine) AddFilter(f EventFilter) {
	e.filters = append(e.filters, f)
}

// AddTransformer adds a transformation function applied before publishing.
func (e *Engine) AddTransformer(t EventTransformer) {
	e.transformers = append(e.transformers, t)
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
		if err := e.startSourceInternal(context.Background(), src); err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) startSourceInternal(ctx context.Context, src interfaces.Source) error {
	instanceID := src.InstanceID()
	ackCh := make(chan uint64, 1000)
	e.sourceAckChs[instanceID] = ackCh

	offset, err := e.natsClient.GetOffset(ctx, instanceID)
	if err != nil {
		slog.Warn("failed to fetch initial offset", "instance_id", instanceID, _nameErr, err)
	}
	initialOffset := utils.DerefString(offset, "")

	if err := src.Start(e.eventCh, ackCh, initialOffset); err != nil {
		return fmt.Errorf("failed to start source %s: %w", instanceID, err)
	}
	return nil
}

// producer consumes from eventCh (from Source) and pushes to hierarchical subject in batches.
// Applies filtering and transformation hooks before publishing.
func (e *Engine) producer() {
	defer e.wg.Done()
	slog.Debug("pipeline producer started")
	// Pre-allocate batch
	batch := make([]*models.Event, 0, e.batchSize)
	flushTicker := time.NewTicker(e.flushInterval)
	// Backpressure monitoring ticker
	backpressureTicker := time.NewTicker(5 * time.Second)

	defer func() {
		flushTicker.Stop()
		backpressureTicker.Stop()
	}()

	subjectFunc := func(ev *models.Event) string {
		topic := ev.Topic
		if topic == "" {
			topic = "cdc"
		}
		return fmt.Sprintf("%s.%s.%s.%s",
			topic,
			ev.InstanceID,
			strings.ReplaceAll(ev.Database, ".", "_"),
			strings.ReplaceAll(ev.Table, ".", "_"))
	}

	for {
		select {
		case ev, ok := <-e.eventCh:
			if !ok {
				// Final flush before exiting
				if len(batch) > 0 {
					e.publishBatch(subjectFunc, batch)
				}
				slog.Debug("pipeline producer exited")
				return
			}

			// Pipeline logic: Filter & Transform
			processedEv := e.applyMiddleware(ev)
			if processedEv == nil {
				continue
			}
			batch = append(batch, processedEv)
			if len(batch) >= e.batchSize {
				e.publishBatchWithRetry(subjectFunc, batch)
				batch = batch[:0] // Reset slice nhưng giữ nguyên capacity
			}

			// Apply filters — skip events that don't pass
			skip := false
			for _, f := range e.filters {
				if !f(ev) {
					skip = true
					break
				}
			}
			if skip {
				continue
			}

			// Apply transformers
			for _, t := range e.transformers {
				ev = t(ev)
			}

			batch = append(batch, ev)
			if len(batch) >= e.batchSize {
				e.publishBatch(subjectFunc, batch)
				batch = batch[:0]
			}
		case <-flushTicker.C:
			if len(batch) > 0 {
				e.publishBatch(subjectFunc, batch)
				batch = batch[:0]
			}
		case <-backpressureTicker.C:
			// Report channel utilization for backpressure monitoring
			if cap(e.eventCh) > 0 {
				metrics.EventChannelUtilization.Set(float64(len(e.eventCh)) / float64(cap(e.eventCh)))
			}
		}
	}
}

func (e *Engine) applyMiddleware(ev *models.Event) *models.Event {
	for _, f := range e.filters {
		if !f(ev) {
			return nil
		}
	}
	for _, t := range e.transformers {
		ev = t(ev)
		if ev == nil {
			return nil
		}
	}
	return ev
}

func (e *Engine) publishBatchWithRetry(subjectFunc func(*models.Event) string, batch []*models.Event) {
	metrics.BatchSizeHistogram.WithLabelValues("producer").Observe(float64(len(batch)))

	const maxRetries = 3
	var err error
	for i := 0; i < maxRetries; i++ {
		err = e.natsClient.PublishBatch(context.Background(), subjectFunc, batch)
		if err == nil {
			for _, ev := range batch {
				e.updateSourceStats(ev.InstanceID, true, "")
			}
			return
		}
		time.Sleep(time.Duration(i+1) * 200 * time.Millisecond) // Exponential backoff nhẹ
	}

	slog.Error("failed to publish batch after retries", _nameErr, err)
	for _, ev := range batch {
		e.updateSourceStats(ev.InstanceID, false, err.Error())
	}
}

func (e *Engine) publishBatch(subjectFunc func(*models.Event) string, batch []*models.Event) {
	metrics.BatchSizeHistogram.WithLabelValues("producer").Observe(float64(len(batch)))
	if err := e.natsClient.PublishBatch(context.Background(), subjectFunc, batch); err != nil {
		slog.Error("failed to publish batch to NATS", _nameErr, err)
		for _, ev := range batch {
			e.updateSourceStats(ev.InstanceID, false, err.Error())
		}
	} else {
		for _, ev := range batch {
			e.updateSourceStats(ev.InstanceID, true, "")
		}
	}
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

// worker consumes events from NATS using Fetch with timeout to avoid the
// iter.Next()-in-default deadlock. This allows time-based flushing to work correctly.
func (e *Engine) worker(ctx context.Context, sink interfaces.Sink, partID int) {
	defer e.wg.Done()
	slog.Debug("pipeline worker started", "worker_id", partID, "sink", sink.InstanceID())

	// Use a shared consumer group name unique to this sink to allow multiple sink workers
	// to share load, while different sinks consume independently.
	consumerName := fmt.Sprintf("pipeline-worker-%s", sink.InstanceID())
	consumer, err := e.natsClient.CreateOrUpdateConsumer(ctx, consumerName, []string{sink.Topic()})
	if err != nil {
		slog.Error("failed to create NATS consumer", "worker_id", partID, "sink", sink.InstanceID(), _nameErr, err)
		return
	}
	// Pre-allocate memory
	pendingMsgs := make([]jetstream.Msg, 0, e.batchSize)
	pendingEvents := make([]*models.Event, 0, e.batchSize)

	for {
		// Check for shutdown
		select {
		case <-ctx.Done():
			if len(pendingMsgs) > 0 {
				e.flushAndAck(sink, pendingMsgs, pendingEvents, partID)
			}
			slog.Debug("pipeline worker exiting", "worker_id", partID, "sink", sink.InstanceID())
			return
		case <-e.stopCh:
			if len(pendingMsgs) > 0 {
				e.flushAndAck(sink, pendingMsgs, pendingEvents, partID)
			}
			return
		default:
		}

		// Fetch a batch of messages with a timeout.
		// This is the fix for the iter.Next()-in-default deadlock:
		// Fetch returns after fetchTimeout even if no messages are available,
		// giving us a natural point to flush partial batches.
		fetchSize := e.batchSize - len(pendingMsgs)
		if fetchSize <= 0 {
			fetchSize = e.batchSize
		}

		msgBatch, err := consumer.Fetch(fetchSize, jetstream.FetchMaxWait(e.flushInterval))
		if err != nil && ctx.Err() == nil {
			slog.Error("failed to fetch messages", "worker_id", partID, "sink", sink.InstanceID(), _nameErr, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for msg := range msgBatch.Messages() {
			var ev models.Event
			if err := json.Unmarshal(msg.Data(), &ev); err != nil {
				slog.Error("failed to unmarshal NATS message", "worker_id", partID, "sink", sink.InstanceID(), _nameErr, err)
				msg.Term() // Don't retry unmarshalable junk
				continue
			}

			// Buffer event and message
			pendingMsgs = append(pendingMsgs, msg)
			pendingEvents = append(pendingEvents, &ev)
		}

		// Flush when batch is full or when Fetch returned (timeout = partial batch flush)
		if len(pendingMsgs) > 0 && (len(pendingMsgs) >= e.batchSize || msgBatch.Error() != nil) {
			metrics.BatchSizeHistogram.WithLabelValues("worker").Observe(float64(len(pendingMsgs)))
			e.flushAndAck(sink, pendingMsgs, pendingEvents, partID)

			pendingMsgs = pendingMsgs[:0]
			pendingEvents = pendingEvents[:0]
		}
	}
}

func (e *Engine) flushAndAck(sink interfaces.Sink, msgs []jetstream.Msg, events []*models.Event, workerID int) {
	start := time.Now()
	// Write batch to sink
	if err := sink.WriteBatch(events); err != nil {
		e.handleSinkError(sink, msgs, err, workerID)
		return
	}
	if err := sink.Flush(); err != nil {
		for _, m := range msgs {
			m.Nak()
		}
		return
	}

	// Update Metrics
	duration := time.Since(start)
	metrics.SinkWriteDuration.WithLabelValues(sink.InstanceID(), sink.Type()).Observe(duration.Seconds() / float64(len(events)))
	e.updateSinkStats(sink.InstanceID(), true, "")

	// Handle Offsets and ACKs
	latestOffsets := make(map[string]string)
	highestLSNs := make(map[string]uint64)

	for _, m := range msgs {
		h := m.Headers()
		instID := h.Get(constant.HeaderInstanceID)
		if instID != "" {
			if off := h.Get(constant.HeaderOffset); off != "" {
				latestOffsets[instID] = off
			}
			if lsnVal := h.Get(constant.HeaderLSN); lsnVal != "" {
				if lsn, err := strconv.ParseUint(lsnVal, 10, 64); err == nil {
					if lsn > highestLSNs[instID] {
						highestLSNs[instID] = lsn
					}
				}
			}
		}
		m.Ack()
	}

	// Save Offset to NATS KV
	for instID, offset := range latestOffsets {
		e.natsClient.SaveOffset(context.Background(), instID, offset)
	}

	// Response to Source channel
	for instID, lsn := range highestLSNs {
		e.mu.RLock()
		ch, ok := e.sourceAckChs[instID]
		e.mu.RUnlock()
		if ok {
			select {
			case ch <- lsn:
			default: // avoid blocking worker if ackCh full
			}
		}
	}
}

// handleSinkError handles errors that occur during sink operations.
func (e *Engine) handleSinkError(sink interfaces.Sink, msgs []jetstream.Msg, err error, workerID int) {
	slog.Error("sink write batch error", "worker_id", workerID, "sink", sink.InstanceID(), _nameErr, err)
	e.updateSinkStats(sink.InstanceID(), false, err.Error())

	for _, m := range msgs {
		if meta, _ := m.Metadata(); meta != nil && int32(meta.NumDelivered) >= sink.MaxRetries() {
			e.natsClient.MoveToDLQ(context.Background(), m, fmt.Sprintf("sink_error: %s", err.Error()))
		} else {
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
	if e.stopped.Swap(true) {
		return // Already stopped
	}
	slog.Info("stopping pipeline engine")

	// Lock only to snapshot data and close channels — no blocking I/O under lock.
	e.mu.Lock()
	sourcesToStop := make([]interfaces.Source, len(e.sources))
	copy(sourcesToStop, e.sources)

	cancelsToCall := make([]context.CancelFunc, 0, len(e.sinkCancels))
	for _, cancel := range e.sinkCancels {
		cancelsToCall = append(cancelsToCall, cancel)
	}

	close(e.stopCh) // Globally stops workers first
	e.mu.Unlock()

	// Stop sources — they may block on pipeline channel, so do this after stopCh
	for _, src := range sourcesToStop {
		if err := src.Stop(); err != nil {
			slog.Error("source stop error", "instance_id", src.InstanceID(), _nameErr, err)
		}
	}

	// Now safe to close the event channel
	close(e.eventCh)

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
	if _, ok := e.sourceAckChs[src.InstanceID()]; ok {
		return fmt.Errorf("source %s already exists", src.InstanceID())
	}
	if err := e.startSourceInternal(ctx, src); err != nil {
		return err
	}
	e.sources = append(e.sources, src)
	return nil
}

// RemoveSource stops and removes a source by instance ID.
func (e *Engine) RemoveSource(instanceID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, src := range e.sources {
		if src.InstanceID() == instanceID {
			src.Stop()
			e.sources = append(e.sources[:i], e.sources[i+1:]...)
			delete(e.sourceAckChs, instanceID)
			return nil
		}
	}
	return fmt.Errorf("source %s not found", instanceID)
}

// AddSink dynamically adds a new sink.
func (e *Engine) AddSink(sink interfaces.Sink) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sinks = append(e.sinks, sink)
	e.startSinkWorkersUnlocked(sink)
}

// RemoveSink stops and removes a sink by its unique instance ID.
func (e *Engine) RemoveSink(instanceID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, s := range e.sinks {
		if s.InstanceID() == instanceID {
			if cancel, ok := e.sinkCancels[instanceID]; ok {
				cancel()
				delete(e.sinkCancels, instanceID)
			}
			s.Close()
			e.sinks = append(e.sinks[:i], e.sinks[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("sink %s not found", instanceID)
}

// GetStats returns the current success/failure metrics.
func (e *Engine) GetStats() (map[string]*models.ComponentStats, map[string]*models.ComponentStats) {
	e.metricsMu.RLock()
	defer e.metricsMu.RUnlock()

	srcCopy := make(map[string]*models.ComponentStats)
	for k, v := range e.sourceStats {
		srcCopy[k] = v
	}

	sinkCopy := make(map[string]*models.ComponentStats)
	for k, v := range e.sinkStats {
		sinkCopy[k] = v
	}

	return srcCopy, sinkCopy
}

// updateSourceStats updates stats for a specific source
func (e *Engine) updateSourceStats(instanceID string, success bool, errStr string) {
	e.metricsMu.Lock()
	defer e.metricsMu.Unlock()
	s, ok := e.sourceStats[instanceID]
	if !ok {
		s = &models.ComponentStats{}
		e.sourceStats[instanceID] = s
	}
	if success {
		s.SuccessCount++
		metrics.EventsProducedTotal.WithLabelValues(instanceID, "success").Inc()
	} else {
		s.FailureCount++
		s.LastError = errStr
		metrics.EventsProducedTotal.WithLabelValues(instanceID, "failure").Inc()
	}
}

// updateSinkStats updates stats for a specific sink instance
func (e *Engine) updateSinkStats(sinkID string, success bool, errStr string) {
	e.metricsMu.Lock()
	defer e.metricsMu.Unlock()
	s, ok := e.sinkStats[sinkID]
	if !ok {
		s = &models.ComponentStats{}
		e.sinkStats[sinkID] = s
	}
	if success {
		s.SuccessCount++
		metrics.EventsConsumedTotal.WithLabelValues(sinkID, "success").Inc()
	} else {
		s.FailureCount++
		s.LastError = errStr
		metrics.EventsConsumedTotal.WithLabelValues(sinkID, "failure").Inc()
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

// ReprocessDLQ pulls messages from the DLQ stream and re-injects them into the main pipeline.
func (e *Engine) ReprocessDLQ(ctx context.Context) (int, error) {
	return e.natsClient.ReprocessDLQ(ctx, func(ev *models.Event) error {
		select {
		case e.eventCh <- ev:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-e.stopCh:
			return fmt.Errorf("engine stopped")
		}
	})
}
