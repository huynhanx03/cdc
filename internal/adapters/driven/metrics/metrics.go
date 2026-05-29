package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	LabelFlowID     = "flow_id"
	LabelInstanceID = "instance_id"
	LabelReason     = "reason"
	LabelSinkID     = "sink_id"
	LabelStatus     = "status"
	LabelType       = "type"

	StatusSuccess = "success"
	StatusFailure = "failure"

	SinkTypeUnknown = "unknown"

	ReasonMappingError         = "mapping_error"
	ReasonSinkWriteFailed      = "sink_write_failed"
	ReasonIsolatedSinkError    = "isolated_sink_error"
	ReasonMessageAckFailed     = "message_ack_failed"
	ReasonCheckpointSaveFailed = "checkpoint_save_failed"
	ReasonMaxRetriesExceeded   = "max_retries_exceeded"
	ReasonNonRetryable         = "non_retryable"
	ReasonSinkRetry            = "sink_retry"
)

var (
	// Source metrics
	EventsProducedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_events_produced_total",
		Help: "Total number of events captured from source and sent to WAL",
	}, []string{LabelInstanceID, LabelStatus})

	// Flow metrics
	DLQEventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_dlq_events_total",
		Help: "Total number of events moved to the dead-letter-queue",
	}, []string{LabelFlowID, LabelReason})

	FlowCheckpointSavedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_flow_checkpoint_save_total",
		Help: "Total number of durable checkpoints saved by a flow",
	}, []string{LabelFlowID})

	FlowRetryTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_flow_retry_total",
		Help: "Total number of flow message retries by sink and reason",
	}, []string{LabelFlowID, LabelSinkID, LabelReason})

	FlowBackpressureTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_flow_backpressure_total",
		Help: "Total number of times a flow worker detected no free pool capacity",
	}, []string{LabelFlowID})

	// Performance metrics
	SinkWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cdc_sink_write_duration_seconds",
		Help:    "Time spent writing to a sink",
		Buckets: prometheus.DefBuckets,
	}, []string{LabelSinkID, LabelType})

	FlowProcessingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cdc_flow_processing_duration_seconds",
		Help:    "Time from worker batch processing start to successful sink write for a flow",
		Buckets: prometheus.DefBuckets,
	}, []string{LabelFlowID})

	// NATS connection health
	NATSReconnectTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cdc_nats_reconnect_total",
		Help: "Total number of NATS reconnections",
	})

	// Per-flow metrics
	FlowEventsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_flow_events_processed_total",
		Help: "Total number of events processed by a flow",
	}, []string{LabelFlowID, LabelStatus})

	FlowWorkerPoolActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_flow_worker_pool_active",
		Help: "Number of active workers in a flow's worker pool",
	}, []string{LabelFlowID})

	FlowWorkerPoolCapacity = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_flow_worker_pool_capacity",
		Help: "Total capacity of a flow's worker pool",
	}, []string{LabelFlowID})

	FlowBatchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cdc_flow_batch_size",
		Help:    "Distribution of batch sizes processed per flow",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
	}, []string{LabelFlowID})
)
