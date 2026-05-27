package runtime

import "testing"

func TestMetricsRecordSinkWriteSetsFlowAndSinkSnapshots(t *testing.T) {
	m := NewMetrics()
	m.RecordSinkWrite("flow-1", "source-1", "sink-1", 3, 25, 1000)

	flow, ok := m.FlowSnapshot("flow-1")
	if !ok {
		t.Fatal("expected flow snapshot")
	}
	if flow.TotalEventsProcessed != 3 {
		t.Fatalf("TotalEventsProcessed = %d, want 3", flow.TotalEventsProcessed)
	}
	sink, ok := m.SinkSnapshot("sink-1")
	if !ok {
		t.Fatal("expected sink snapshot")
	}
	if sink.SuccessCount != 3 || sink.LastEventAt == 0 {
		t.Fatalf("sink snapshot = %#v", sink)
	}
}

func TestMetricsRecordFailureAndDLQ(t *testing.T) {
	m := NewMetrics()
	m.RecordFlowFailure("flow-1", "source-1", "sink-1", "sink_error", "duplicate key", 2)
	m.RecordDLQ("flow-1", "sink-1", "max_retries", 1)

	flow, ok := m.FlowSnapshot("flow-1")
	if !ok {
		t.Fatal("expected flow snapshot")
	}
	if flow.FailureCount != 2 {
		t.Fatalf("FailureCount = %d, want 2", flow.FailureCount)
	}
	if flow.DLQCount != 1 {
		t.Fatalf("DLQCount = %d, want 1", flow.DLQCount)
	}
	if flow.LastError != "max_retries" {
		t.Fatalf("LastError = %q, want max_retries", flow.LastError)
	}
}

func TestRuntimeMetricNamesCoverCoreSignals(t *testing.T) {
	names := map[string]bool{}
	for _, name := range RuntimeMetricNames() {
		names[name] = true
	}
	for _, want := range []string{
		"events_in_total",
		"events_out_total",
		"sink_write_duration_ms",
		"checkpoint_save_total",
		"retry_total",
		"dlq_total",
		"nats_pending",
		"worker_backpressure_total",
		"source_lag_ms",
	} {
		if !names[want] {
			t.Fatalf("missing metric name %q", want)
		}
	}
}

func TestMetricsRecordCheckpointRetryBackpressure(t *testing.T) {
	m := NewMetrics()
	m.RecordCheckpointSave("flow-1", 1)
	m.RecordRetry("flow-1", "sink-1", "timeout", 2)
	m.RecordBackpressure("flow-1", 3)

	flow, ok := m.FlowSnapshot("flow-1")
	if !ok {
		t.Fatal("expected flow snapshot")
	}
	if flow.CheckpointSaveCount != 1 || flow.RetryCount != 2 || flow.BackpressureCount != 3 {
		t.Fatalf("flow snapshot = %#v", flow)
	}
}

func TestMetricsRecordSourceProduced(t *testing.T) {
	m := NewMetrics()
	m.RecordSourceProduced("source-1", "public", "orders", 5, 1234)

	source, ok := m.SourceSnapshot("source-1")
	if !ok {
		t.Fatal("expected source snapshot")
	}
	if source.SuccessCount != 5 || source.LastEventAt != 1234 {
		t.Fatalf("source snapshot = %#v", source)
	}
}
