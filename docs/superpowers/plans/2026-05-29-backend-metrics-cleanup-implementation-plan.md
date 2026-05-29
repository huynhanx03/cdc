# Backend Metrics Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Clean backend metrics, remove avoidable magic values, and delete the unused legacy flow offset persistence path.

**Architecture:** Keep runtime metrics, Prometheus metrics, and service APIs as separate layers. Add constants and narrow helpers at the layer that owns each value, then update call sites without changing public proto contracts. Remove only the old flow offset APIs; source offsets and structured checkpoints stay.

**Tech Stack:** Go, Prometheus client, NATS JetStream KV, existing Go unit tests, `make test-unit`, `make bench-pipeline`.

---

## Task 1: Test Metric Semantics

**Files:**
- Modify: `internal/core/runtime/metrics_test.go`
- Modify: `internal/core/runtime/view_test.go`
- Modify: `internal/core/flow/worker_test.go`

- [x] Add a runtime metrics test proving `AvgLatencyMs` is the arithmetic average across sink writes.
- [x] Add a runtime dashboard test proving the fallback latency card uses the maximum latest replication lag across flows.
- [x] Update the worker checkpoint-ordering test so the expected call order is `sink`, `ack`, `checkpoint`, without a legacy `offset` write.
- [x] Run `go test ./internal/core/runtime ./internal/core/flow` and confirm the new tests fail before implementation.

## Task 2: Clean Runtime Metrics

**Files:**
- Modify: `internal/core/runtime/metrics.go`
- Modify: `internal/core/runtime/view.go`

- [x] Replace last-latency storage with latency total and latency sample count.
- [x] Make `componentSnapshot` return average latency.
- [x] Make `View.Dashboard` use max latest replication lag as the fallback p99-style latency value.
- [x] Keep existing snapshot field names unchanged to avoid API churn.
- [x] Run `go test ./internal/core/runtime`.

## Task 3: Clean Prometheus Metrics And Worker Constants

**Files:**
- Modify: `internal/adapters/driven/metrics/metrics.go`
- Modify: `internal/adapters/driven/metrics/query.go`
- Modify: `internal/core/flow/worker.go`
- Modify: `internal/core/flow/manager.go`
- Modify: `internal/core/flow/worker_test.go`
- Modify: `internal/core/flow/manager_prop_test.go`
- Modify: `internal/adapters/driven/connector/source/postgres/source.go`
- Modify: `internal/adapters/driven/connector/source/mysql/source.go`

- [x] Add metric constants for statuses, sink type fallback, and DLQ/retry/failure reasons.
- [x] Add Prometheus counters for checkpoint saves, retries, and worker backpressure.
- [x] Record sink type in `SinkWriteDuration`.
- [x] Replace worker and source publish magic values with named constants.
- [x] Use metric constants in source adapters and worker paths.
- [x] Run `go test ./internal/core/flow ./internal/adapters/driven/connector/source/postgres ./internal/adapters/driven/connector/source/mysql`.

## Task 4: Remove Legacy Flow Offset API

**Files:**
- Modify: `internal/core/ports/storage.go`
- Modify: `internal/adapters/driven/storage/keys.go`
- Modify: `internal/adapters/driven/storage/nats_kv.go`
- Modify: `internal/core/flow/manager.go`
- Modify: `internal/core/flow/manager_prop_test.go`
- Modify: `internal/core/flow/worker_test.go`
- Modify: `internal/core/service/duplicate_validation_test.go`

- [x] Remove `SaveOffset` and `GetOffset` from `ports.Store`.
- [x] Delete `NATSKVStore.SaveOffset`, `NATSKVStore.GetOffset`, and `PrefixOffsets`.
- [x] Remove legacy flow offset writes from worker and delete-flow cleanup.
- [x] Remove mock store methods that existed only for the legacy API.
- [x] Run `go test ./internal/adapters/driven/storage ./internal/core/flow ./internal/core/service`.

## Task 5: Verify

**Files:**
- No source edits unless verification fails.

- [x] Run `make test-unit`.
- [x] Run `make bench-pipeline`.
- [x] Run `git diff --check`.
- [x] Confirm benchmark output still only includes local source decode and transform benchmarks.
