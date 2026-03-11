package pipeline

import (
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/queue"
	"github.com/foden/cdc/pkg/wal"
)

const (
	_nameWorkerID   = "worker_id"
	_nameErr        = "err"
	_nameWorkers    = "workers"
	_nameBufferSize = "buffer_size"

	// How many events the worker tries to pull from the WAL in one go
	_batchSize = 100
)

// Engine is the core CDC pipeline that connects a Source to multiple Sinks
// using a WAL-backed queue for reliability.
type Engine struct {
	source  interfaces.Source
	sinks   []interfaces.Sink
	manager *wal.Manager[*models.Event]

	// channel between Source -> Pipeline (to accept incoming events)
	eventCh chan *models.Event
	ackCh   chan uint64

	stopCh         chan struct{}
	partitionCount int
	wg             sync.WaitGroup
}

// NewEngine creates a pipeline engine with configurable buffer size and partition count.
func NewEngine(bufferSize, partitionCount int, src interfaces.Source, sinks []interfaces.Sink, manager *wal.Manager[*models.Event]) *Engine {
	return &Engine{
		source:         src,
		sinks:          sinks,
		manager:        manager,
		eventCh:        make(chan *models.Event, bufferSize),
		ackCh:          make(chan uint64, bufferSize), // ACKs go to Source
		stopCh:         make(chan struct{}),
		partitionCount: partitionCount,
	}
}

// Start launches the producer and worker pool, then starts the source.
// This method blocks until the source finishes or the engine is stopped.
func (e *Engine) Start() error {
	slog.Info("starting pipeline engine", "partitions", e.partitionCount, _nameBufferSize, cap(e.eventCh))

	// 1. Pipeline Producer (Reads from eventCh, writes to partitioned WAL)
	e.wg.Add(1)
	go e.producer()

	// 2. Launch workers (1 worker per partition ensures ordering)
	for i := 0; i < e.partitionCount; i++ {
		e.wg.Add(1)
		go e.worker(i)
	}

	// 3. Start the source — blocks or launches its own goroutine.
	return e.source.Start(e.eventCh, e.ackCh)
}

func hashPartition(table, key string, numPartitions int) int {
	h := fnv.New32a()
	h.Write([]byte(table))
	h.Write([]byte(key))
	// Prevent negative hash
	val := int(h.Sum32())
	if val < 0 {
		val = -val
	}
	return val % numPartitions
}

// producer consumes from eventCh (from Source) and pushes to right partition WAL
func (e *Engine) producer() {
	defer e.wg.Done()
	slog.Debug("pipeline producer started")

	for ev := range e.eventCh {
		partID := hashPartition(ev.Database, ev.Table, e.partitionCount)

		if err := e.manager.Enqueue(partID, ev); err != nil {
			slog.Error("failed to enqueue to WAL partition", "partition", partID, _nameErr, err)
		}
	}
	slog.Debug("pipeline producer exited")
}

// worker consumes events from its specific partition WAL and fans out to all sinks.
func (e *Engine) worker(partID int) {
	defer e.wg.Done()
	slog.Debug("pipeline worker started", "partition_id", partID)

	for {
		select {
		case <-e.stopCh:
			slog.Debug("pipeline worker exited", "partition_id", partID)
			return
		default:
			// Poll WAL for batch of events for this partition
			events, err := e.manager.DequeueBatch(partID, _batchSize)
			if err == queue.ErrQueueClosed {
				return
			}
			if err != nil {
				slog.Error("failed to dequeue from WAL partition", "partition_id", partID, _nameErr, err)
				time.Sleep(1 * time.Second)
				continue
			}
			if len(events) == 0 {
				time.Sleep(100 * time.Millisecond) // Idle backoff
				continue
			}

			// Process events sequentially in this partition worker
			var hasError bool
			var highestLSN uint64
			for _, ev := range events {
				for _, s := range e.sinks {
					if err := s.Write(ev); err != nil {
						slog.Error("sink write error", "partition_id", partID, _nameErr, err)
						hasError = true
					}
				}
				if ev.LSN > highestLSN {
					highestLSN = ev.LSN
				}
			}
			if !hasError && highestLSN > 0 {
				// Send ACK back to the source
				e.ackCh <- highestLSN
				// Garbage collect WAL segments if fully read
				e.manager.Commit(partID)
			}
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

	if err := e.source.Stop(); err != nil {
		slog.Error("source stop error", _nameErr, err)
	}

	close(e.eventCh) // Stops producer
	close(e.stopCh)  // Stops workers

	e.wg.Wait()

	for _, s := range e.sinks {
		if err := s.Close(); err != nil {
			slog.Error("sink close error", _nameErr, err)
		}
	}
	slog.Info("pipeline engine stopped")
}
