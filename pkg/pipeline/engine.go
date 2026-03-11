package pipeline

import (
	"log/slog"
	"sync"

	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/models"
)

const (
	_nameWorkerID   = "worker_id"
	_nameErr        = "err"
	_nameWorkers    = "workers"
	_nameBufferSize = "buffer_size"
)

// Engine is the core CDC pipeline that connects a Source to multiple Sinks
// using a pool of worker goroutines for high throughput.
type Engine struct {
	source      interfaces.Source
	sinks       []interfaces.Sink
	eventCh     chan *models.Event
	ackCh       chan uint64
	stopCh      chan struct{}
	workerCount int
	wg          sync.WaitGroup
}

// NewEngine creates a pipeline engine with configurable buffer size and worker count.
func NewEngine(bufferSize, workerCount int, src interfaces.Source, sinks ...interfaces.Sink) *Engine {
	return &Engine{
		source:      src,
		sinks:       sinks,
		eventCh:     make(chan *models.Event, bufferSize),
		ackCh:       make(chan uint64, bufferSize), // Buffer ACKs up to the same size as events
		stopCh:      make(chan struct{}),
		workerCount: workerCount,
	}
}

// Start launches the worker pool, then starts the source.
// This method blocks until the source finishes or the engine is stopped.
func (e *Engine) Start() error {
	slog.Info("starting pipeline engine", _nameWorkers, e.workerCount, _nameBufferSize, cap(e.eventCh))

	// Launch workers
	for i := 0; i < e.workerCount; i++ {
		e.wg.Add(1)
		go e.worker(i)
	}

	// Start the source — this typically blocks or launches its own goroutine.
	return e.source.Start(e.eventCh, e.ackCh)
}

// worker consumes events from the channel and fans out to all sinks.
func (e *Engine) worker(id int) {
	defer e.wg.Done()
	slog.Debug("pipeline worker started", _nameWorkerID, id)

	for ev := range e.eventCh {
		var hasError bool
		for _, s := range e.sinks {
			if err := s.Write(ev); err != nil {
				slog.Error("sink write error", _nameWorkerID, id, _nameErr, err)
				hasError = true
			}
		}
		if !hasError && ev.LSN > 0 {
			// Send ACK back to the source so it can advance the commit offset
			e.ackCh <- ev.LSN
		}
	}

	slog.Debug("pipeline worker exited", _nameWorkerID, id)
}

// Stop performs a graceful shutdown:
// 1. Stop the source (no more events produced)
// 2. Drain all remaining events in the channel
// 3. Close sinks (triggers final flush for bulk sinks)
func (e *Engine) Stop() {
	slog.Info("stopping pipeline engine")
	// 1. Stop producing events
	if err := e.source.Stop(); err != nil {
		slog.Error("source stop error", _nameErr, err)
	}
	// 2. Close the channel — workers will drain remaining events and exit
	close(e.eventCh)
	e.wg.Wait()
	// 3. Close all sinks (flush remaining bulk buffers)
	for _, s := range e.sinks {
		if err := s.Close(); err != nil {
			slog.Error("sink close error", _nameErr, err)
		}
	}
	close(e.ackCh)
	slog.Info("pipeline engine stopped")
}
