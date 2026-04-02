package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Source metrics
	EventsProducedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_events_produced_total",
		Help: "Total number of events captured from source and sent to WAL",
	}, []string{"instance_id", "status"}) // status: success/failure

	SourceErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_source_errors_total",
		Help: "Total number of non-terminal errors in source capture",
	}, []string{"instance_id", "error_type"})

	// Sink metrics
	EventsConsumedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_events_consumed_total",
		Help: "Total number of events consumed from WAL and sent to sink",
	}, []string{"instance_id", "status"}) // status: success/failure

	SinkErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_sink_errors_total",
		Help: "Total number of non-terminal errors in sink delivery",
	}, []string{"instance_id", "sink_id", "error_type"})

	// Pipeline metrics
	DLQEventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_dlq_events_total",
		Help: "Total number of events moved to the dead-letter-queue",
	}, []string{"instance_id", "sink_id", "reason"})

	ActiveWorkers = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_active_workers",
		Help: "Number of active parallel partition workers in engine",
	}, []string{"type"}) // type: producer/worker

	// Performance metrics
	WorkerProcessDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cdc_worker_process_duration_seconds",
		Help:    "Time spent processing an event in a worker",
		Buckets: prometheus.DefBuckets,
	}, []string{"sink_id"})

	SinkWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cdc_sink_write_duration_seconds",
		Help:    "Time spent writing to a sink",
		Buckets: prometheus.DefBuckets,
	}, []string{"sink_id", "type"})
)
