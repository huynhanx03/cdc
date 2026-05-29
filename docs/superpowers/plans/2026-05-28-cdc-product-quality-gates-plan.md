# CDC Product Quality Gates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add unit, Docker-backed integration, E2E, and benchmark gates so each CDC product phase can prove correctness, reliability, and performance with repeatable evidence.

**Architecture:** Quality gates are split by blast radius: fast unit tests run on every change, Docker-backed integration tests validate real infrastructure behavior, E2E tests validate full product workflows through public APIs, and benchmarks measure each pipeline stage plus end-to-end freshness. Integration and benchmark tests use explicit build tags and Makefile targets so local development stays fast while product readiness remains measurable.

**Tech Stack:** Go `testing`, Go benchmarks, build tags (`integration`, `e2e`), Docker Compose or `testcontainers-go`, NATS JetStream, PostgreSQL, MySQL/MariaDB, ClickHouse, Elasticsearch/OpenSearch, grpc-gateway/REST APIs, Vite/React UI tests later when Node/npm is available.

## Execution Status - 2026-05-28

Completed in this pass:

- Quality gate documentation, Makefile targets, and GitHub Actions `quality` workflow.
- Docker-backed `testcontainers-go` helpers for NATS, PostgreSQL, MySQL, ClickHouse, and Elasticsearch.
- Flow validation unit gates for severity, fatal blocking, filter dry-run, and mapping dry-run.
- Explorer backend filter model with topic, partition, sequence, timestamp, header, text, JSON path, op, source, schema, and table predicates.
- Real NATS integration gates for Explorer message search, Explorer consumer lag, and DLQ reprocess publish behavior.
- Deterministic DLQ reprocess IDs replacing timestamp-only IDs.
- Pipeline benchmarks for source decode and transform stages.
- E2E and integration placeholders for product features that still need public API/UI implementation.

Verified commands:

- `make test-unit`
- `make test-integration`
- `make test-e2e`
- `make bench-pipeline`
- `go test -tags=integration -run '^$' ./benchmarks/pipeline`
- `git diff --check`

Remaining known gaps are represented as skipped tests: full crash/restart harness, retention controller assertion, ValidateFlow public API, DLQ dry-run/selected reprocess API, and UI/API E2E workflows.

---

## Phase Alignment

This quality track gates the product phases:

| Product phase | Required quality gate before the phase is considered done |
|---|---|
| Phase 0: Stabilize current remediation | `go test ./...` passes and generated proto is reproducible. |
| Phase 1: CDC Safety Foundation | Unit + integration tests prove preflight failures, checkpoint ordering, retention safety, and source reconnect behavior. |
| Phase 2: Flow Wizard | Unit + API tests prove `ValidateFlow`, schema diff, mapping dry-run, and filter dry-run produce fatal/warning/pass results correctly. |
| Phase 3: Observability | Unit + integration tests prove lag, pending, retry, DLQ, and freshness metrics are emitted with stable labels. |
| Phase 4: Explorer Backend | Real JetStream integration tests prove topic, partition, consumer, message search, JSON/header/time/sequence filters. |
| Phase 5: Explorer UI | Browser tests prove Redpanda/Kafka-UI-style flows: filter messages, inspect payload, drill into consumers, open DLQ context. |
| Phase 6: DLQ Recovery | E2E tests prove dry-run, selected reprocess, dedupe preview, and guarded bulk reprocess do not corrupt state. |

---

## File Structure

Create or modify these files:

- Create: `docs/QUALITY_GATES.md` - human-readable quality matrix, commands, and readiness thresholds.
- Modify: `Makefile` - add `test-unit`, `test-integration`, `test-e2e`, `bench`, `bench-pipeline`, `test-all`.
- Create: `internal/testutil/containers/` - Docker/testcontainers helpers for NATS, Postgres, MySQL, ClickHouse, Elasticsearch.
- Create: `internal/testutil/assertcdc/` - polling and assertion helpers for eventual CDC checks.
- Create: `tests/integration/` - Docker-backed package tests with `//go:build integration`.
- Create: `tests/e2e/` - public API workflow tests with `//go:build e2e`.
- Create: `benchmarks/pipeline/` - Go benchmark package for source decode, NATS publish/fetch, worker transform, sink writes, E2E freshness.
- Modify: `internal/core/runtime/metrics.go` - add pipeline-stage measurements only when required by benchmark/observability tasks.
- Modify: `internal/adapters/driven/nats/browser.go` - add testable seams for Explorer filtering where needed.
- Modify: `internal/core/flow/worker.go` - expose or isolate stage timing in small helpers before measuring.
- Create later: `.github/workflows/quality.yml` - CI split into fast PR gate, nightly integration, manual benchmark.

---

## Task 1: Quality Matrix And Makefile Targets

**Files:**
- Create: `docs/QUALITY_GATES.md`
- Modify: `Makefile`

- [ ] **Step 1: Document the gates**

Create `docs/QUALITY_GATES.md` with these sections:

```markdown
# CDC Quality Gates

## Commands

| Gate | Command | Expected use |
|---|---|---|
| Unit | `make test-unit` | Every local change and every PR |
| Integration | `make test-integration` | Before merging CDC correctness, source, sink, NATS, Explorer changes |
| E2E | `make test-e2e` | Before releasing product workflow changes |
| Benchmarks | `make bench-pipeline` | Before performance-sensitive changes and release candidates |
| Full | `make test-all` | Release candidate validation |

## Readiness Criteria

- No known data-loss path in crash/restart tests.
- Checkpoints never advance ahead of sink success and message ACK semantics.
- Postgres/MySQL preflight catches missing prerequisites before running flow.
- DLQ reprocess dry-run mutates no messages.
- Explorer filters are server-side, capped, and return deterministic results.
- Benchmarks record source decode, publish, fetch, filter, mapping, sink write, checkpoint, and E2E freshness.
```

- [ ] **Step 2: Add Makefile targets**

Add targets:

```make
test-unit:
	go test ./...

test-integration:
	go test -tags=integration ./tests/integration/...

test-e2e:
	go test -tags=e2e ./tests/e2e/...

bench:
	go test -bench=. -benchmem ./...

bench-pipeline:
	go test -bench=. -benchmem ./benchmarks/pipeline/...

test-all: test-unit test-integration test-e2e
```

- [ ] **Step 3: Verify unit target**

Run: `make test-unit`

Expected: all current Go tests pass.

---

## Task 2: Docker-Backed Test Harness

**Files:**
- Create: `internal/testutil/containers/nats.go`
- Create: `internal/testutil/containers/postgres.go`
- Create: `internal/testutil/containers/mysql.go`
- Create: `internal/testutil/containers/clickhouse.go`
- Create: `internal/testutil/containers/elasticsearch.go`
- Create: `internal/testutil/assertcdc/wait.go`
- Modify: `go.mod`

- [ ] **Step 1: Add testcontainers dependency**

Run:

```bash
go get github.com/testcontainers/testcontainers-go@latest
go get github.com/testcontainers/testcontainers-go/modules/postgres@latest
go get github.com/testcontainers/testcontainers-go/modules/mysql@latest
go get github.com/testcontainers/testcontainers-go/modules/nats@latest
```

Expected: `go.mod` and `go.sum` update.

- [ ] **Step 2: Create container helpers**

Each helper returns connection config plus cleanup:

```go
type RunningPostgres struct {
    Host string
    Port int
    User string
    Password string
    Database string
    Cleanup func(context.Context) error
}
```

Equivalent structs should exist for NATS, MySQL, ClickHouse, and Elasticsearch.

- [ ] **Step 3: Create eventual assertion helper**

`internal/testutil/assertcdc/wait.go`:

```go
func Eventually(t *testing.T, timeout time.Duration, interval time.Duration, check func() error) {
    t.Helper()
    deadline := time.Now().Add(timeout)
    var last error
    for time.Now().Before(deadline) {
        if err := check(); err == nil {
            return
        } else {
            last = err
        }
        time.Sleep(interval)
    }
    t.Fatalf("condition not met within %s: %v", timeout, last)
}
```

- [ ] **Step 4: Verify harness compiles**

Run: `go test ./internal/testutil/...`

Expected: package compiles with no tests or helper tests pass.

---

## Task 3: Safety Foundation Integration Tests

**Files:**
- Create: `tests/integration/postgres_preflight_test.go`
- Create: `tests/integration/delivery_checkpoint_test.go`
- Create: `tests/integration/nats_retention_test.go`
- Create: `tests/integration/reconnect_test.go`

- [ ] **Step 1: Add Postgres prerequisite tests**

Test cases:

- table without PK + update/delete flow -> validation fails with actionable error.
- table with `REPLICA IDENTITY FULL` -> validation passes.
- `numeric(38, 10)` row -> decoded payload preserves decimal as string.

Run: `go test -tags=integration ./tests/integration -run TestPostgresPreflight -v`

Expected: all prerequisite cases pass.

- [ ] **Step 2: Add checkpoint crash tests**

Test cases:

- publish event then fail sink write -> no flow checkpoint advances.
- sink write succeeds and ACK succeeds -> checkpoint advances.
- restart worker after failed sink write -> message replays.

Run: `go test -tags=integration ./tests/integration -run TestDeliveryCheckpoint -v`

Expected: no data loss and checkpoint ordering is proven.

- [ ] **Step 3: Add multi-flow retention test**

Test case:

- one source feeds two flows.
- flow A consumes fast, flow B is paused/slow.
- retention guard computes min checkpoint and refuses unsafe purge for flow B.

Run: `go test -tags=integration ./tests/integration -run TestNATSRetentionUsesMinFlowCheckpoint -v`

Expected: retention never removes messages still needed by slow flow.

---

## Task 4: Flow Wizard API Quality Gates

**Files:**
- Create: `internal/core/service/flow_validation_test.go`
- Create: `tests/integration/flow_validation_api_test.go`
- Modify later: `proto/cdc/v1/flow.proto`
- Modify later: `internal/core/service/flow_validation.go`

- [ ] **Step 1: Unit-test validation result model**

Required result model:

```go
type ValidationSeverity string

const (
    ValidationSeverityPass ValidationSeverity = "PASS"
    ValidationSeverityWarning ValidationSeverity = "WARNING"
    ValidationSeverityFatal ValidationSeverity = "FATAL"
)

type ValidationFinding struct {
    Code string
    Severity ValidationSeverity
    Message string
    Target string
}
```

Tests must prove fatal findings block flow creation and warnings do not.

- [ ] **Step 2: Add dry-run tests**

Test cases:

- `after.status == "paid"` passes sample Debezium payload.
- invalid CEL returns fatal validation finding.
- mapping `amount -> total_amount` modifies `after` only.
- delete mapping uses `before`.

Run: `go test ./internal/core/service -run TestFlowValidation -v`

Expected: dry-runs are deterministic and do not persist flow state.

---

## Task 5: Explorer Backend Quality Gates

**Files:**
- Create: `tests/integration/explorer_messages_test.go`
- Create: `tests/integration/explorer_consumers_test.go`
- Create: `internal/adapters/driven/nats/browser_filter_test.go`
- Modify later: `proto/cdc/v1/explorer.proto`
- Modify later: `internal/adapters/driven/nats/browser.go`

- [ ] **Step 1: Unit-test filter parsing**

Filter dimensions:

- subject/topic/partition
- sequence min/max
- timestamp from/to
- header key/value
- text contains
- JSON path equals
- op/source/schema/table

Run: `go test ./internal/adapters/driven/nats -run TestExplorerFilter -v`

Expected: filters compile into deterministic predicates or NATS filter subjects.

- [ ] **Step 2: Integration-test message search**

Seed JetStream with known messages:

- create/update/delete ops
- multiple subjects and partitions
- headers with source/table/op/partition
- JSON payload with `before`, `after`, `source`

Run: `go test -tags=integration ./tests/integration -run TestExplorerMessageSearch -v`

Expected: each filter returns only the expected messages and respects hard limits.

- [ ] **Step 3: Integration-test consumer lag**

Create durable consumers and publish messages.

Expected assertions:

- delivered stream sequence is reported.
- ack floor is reported.
- ack pending is reported.
- lag per consumer is stable.

Run: `go test -tags=integration ./tests/integration -run TestExplorerConsumerLag -v`

Expected: consumer summaries match JetStream state.

---

## Task 6: DLQ Recovery Quality Gates

**Files:**
- Create: `tests/integration/dlq_recovery_test.go`
- Create: `internal/adapters/driven/nats/dlq_reprocess_test.go`
- Modify later: `proto/cdc/v1/dlq.proto`
- Modify later: `internal/core/service/dlq.go`
- Modify later: `internal/adapters/driven/nats/dlq.go`

- [ ] **Step 1: Unit-test deterministic reprocess IDs**

Test cases:

- same original message + same attempt -> same reprocess ID.
- different attempt -> different reprocess ID.
- timestamp is not used as the only dedupe source.

Run: `go test ./internal/adapters/driven/nats -run TestDLQReprocessID -v`

Expected: IDs are deterministic.

- [ ] **Step 2: Integration-test dry-run**

Seed DLQ with failed messages.

Run dry-run reprocess.

Expected:

- no messages are published to main stream.
- returned preview includes count, subjects, reasons, duplicate risk.

Run: `go test -tags=integration ./tests/integration -run TestDLQDryRunDoesNotMutate -v`

Expected: DLQ and main stream counts are unchanged.

- [ ] **Step 3: Integration-test selected reprocess**

Select two DLQ messages and reprocess them.

Expected:

- selected messages are republished.
- unselected messages stay in DLQ.
- reprocessed messages preserve original headers plus reprocess metadata.

Run: `go test -tags=integration ./tests/integration -run TestDLQSelectedReprocess -v`

Expected: only selected messages move.

---

## Task 7: Pipeline Benchmarks

**Files:**
- Create: `benchmarks/pipeline/source_decode_bench_test.go`
- Create: `benchmarks/pipeline/transform_bench_test.go`
- Create: `benchmarks/pipeline/nats_bench_test.go`
- Create: `benchmarks/pipeline/sink_bench_test.go`
- Create: `benchmarks/pipeline/e2e_freshness_bench_test.go`

- [ ] **Step 1: Benchmark source decode**

Benchmarks:

- Postgres Debezium payload decode.
- MySQL binlog payload decode.
- numeric/time/json/bool-heavy rows.

Run: `go test -bench=BenchmarkSourceDecode -benchmem ./benchmarks/pipeline`

Record: ns/op, B/op, allocs/op.

- [ ] **Step 2: Benchmark transform path**

Benchmarks:

- CEL filter pass.
- CEL filter fail.
- mapping `after` payload.
- mapping delete `before` payload.

Run: `go test -bench=BenchmarkTransform -benchmem ./benchmarks/pipeline`

Record: filter/mapping cost per event.

- [ ] **Step 3: Benchmark NATS publish/fetch**

Benchmarks:

- publish batch sizes 1, 100, 1000.
- fetch batch sizes 1, 100, 1000.
- payload sizes 1KB, 10KB, 100KB.

Run: `go test -tags=integration -bench=BenchmarkNATS -benchmem ./benchmarks/pipeline`

Record: events/sec and bytes/sec.

- [ ] **Step 4: Benchmark sink write**

Benchmarks:

- Postgres batch upsert.
- MySQL batch upsert.
- ClickHouse batch insert.
- Elasticsearch bulk index.

Run: `go test -tags=integration -bench=BenchmarkSinkWrite -benchmem ./benchmarks/pipeline`

Record: rows/sec and p95 write latency.

- [ ] **Step 5: Benchmark E2E freshness**

Scenario:

- insert N rows into source table.
- wait until sink row count reaches N.
- record p50/p95/p99 freshness from source event timestamp to sink durable write.

Run: `go test -tags=integration -bench=BenchmarkE2EFreshness -benchmem ./benchmarks/pipeline`

Record: p50/p95/p99 freshness and throughput.

---

## Task 8: CI And Release Gates

**Files:**
- Create: `.github/workflows/quality.yml`
- Modify: `docs/QUALITY_GATES.md`

- [ ] **Step 1: Add PR unit gate**

CI job:

```yaml
name: quality
on:
  pull_request:
  push:
    branches: [develop, main]
jobs:
  unit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
      - run: make test-unit
```

- [ ] **Step 2: Add nightly integration gate**

CI job:

```yaml
  integration:
    runs-on: ubuntu-latest
    if: github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
      - run: make test-integration
```

- [ ] **Step 3: Add manual benchmark gate**

CI job:

```yaml
  benchmark:
    runs-on: ubuntu-latest
    if: github.event_name == 'workflow_dispatch'
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
      - run: make bench-pipeline
```

---

## Initial Product Readiness Definition

The system is not considered product-grade until these checks exist and pass:

- `make test-unit`
- `make test-integration`
- `make test-e2e`
- `make bench-pipeline`
- Crash/restart test proves no data loss.
- Multi-flow retention test proves slow flows are protected.
- Explorer message search test proves server-side filtering is correct.
- DLQ dry-run test proves no mutation.
- E2E freshness benchmark records p95 and p99 latency.
