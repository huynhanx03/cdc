# Backend Metrics Cleanup Design

## Goal

Make backend metrics cleaner and more truthful, then remove the legacy flow offset write path that is no longer used for recovery. The change keeps behavior scoped to observability and cleanup: source offsets, checkpoints, DLQ, retries, and flow lifecycle semantics remain intact.

## Current Assessment

The system already has a useful metrics foundation:

- Prometheus endpoint at `/metrics`.
- Prometheus counters and histograms for source produced events, flow processed events, DLQ events, sink write duration, flow processing duration, pool active/capacity, and batch size.
- Runtime in-memory snapshots for dashboard, flow detail, source cards, and sink cards.
- Dashboard can query Prometheus for flow processing p99 and falls back to runtime data when Prometheus is unavailable.

The gaps are specific:

- Several metric statuses and reasons are raw strings in workers.
- Worker timing and source publish parameters use magic literals.
- Sink write histogram has a `type` label but records an empty value.
- Runtime component `AvgLatencyMs` stores the last sink latency, not an average.
- Dashboard fallback stores average replication lag in a field called `LatencyP99`, which is misleading when Prometheus is down.
- Retry, checkpoint, and backpressure are only visible in runtime snapshots, not Prometheus.
- `Store.SaveOffset` and `Store.GetOffset` are legacy flow offset APIs. Runtime recovery now uses source offsets and structured checkpoints, so workers should stop writing the old flow offset key.

## Design

Add explicit constants and helper functions for metric statuses, metric reasons, operational worker defaults, source publish defaults, and dashboard probe timeouts. Use these constants at call sites instead of embedded literals.

Prometheus metrics are extended only for signals already recorded in runtime snapshots:

- `cdc_flow_checkpoint_save_total{flow_id}`
- `cdc_flow_retry_total{flow_id,sink_id,reason}`
- `cdc_flow_backpressure_total{flow_id}`

Runtime component latency changes from last-write latency to average write latency by tracking total latency and sample count atomically. Dashboard fallback latency changes from average replication lag to max latest replication lag across flows, which is a safer fallback for a p99-style card than an average.

The flow worker records sink type using `FlowSink.Type()` so the existing sink write histogram label becomes meaningful. The `sinkAdapter` delegates to `ports.Sink.Type()`, and worker tests provide deterministic sink types.

Remove legacy flow offset persistence:

- Remove `SaveOffset` and `GetOffset` from `ports.Store`.
- Remove `NATSKVStore.SaveOffset` and `NATSKVStore.GetOffset`.
- Remove the old worker `SaveOffset` call after checkpoint save.
- Remove the delete-flow offset clear call.
- Keep structured checkpoint read/write and source offset read/write.
- Keep `LegacyCheckpointKey` fallback for old checkpoint data because it is not the same as the removed flow offset key and can still help older deployments start safely.

## Non-Goals

- No schema or proto changes.
- No full metrics backend rewrite.
- No Docker integration benchmarks.
- No behavioral changes to source connector resume, sink retry, DLQ movement, or checkpoint save order.
- No deletion of backward-compatible legacy checkpoint fallback.

## Testing

Use test-first changes for behavior that should be preserved or corrected:

- Runtime component latency reports a real average.
- Dashboard fallback latency uses max replication lag.
- Worker checkpoint path no longer writes legacy flow offsets.

Then run:

- `go test ./internal/core/runtime`
- `go test ./internal/core/flow`
- `make test-unit`
- `make bench-pipeline`
- `git diff --check`
