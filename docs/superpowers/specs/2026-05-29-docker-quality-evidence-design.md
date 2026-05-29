# Docker Quality Evidence Design

## Goal

Add Docker-backed integration and E2E evidence for the CDC product paths that local unit tests and local benchmarks cannot prove:

- NATS JetStream publish/fetch behavior.
- Real sink writes.
- Checkpoint persistence and restart recovery.
- DLQ movement and selected recovery.
- Dashboard Prometheus p99 query against a real Prometheus container.

## Current State

The repo already has `make test-integration` and `make test-e2e` targets with build tags. Some Docker-backed NATS Explorer and DLQ tests exist, but major quality gates are still placeholders or too narrow:

- NATS publish/fetch is indirectly covered, not explicitly verified as a transport gate.
- Real sink writes are covered mostly by unit-level SQL builders and metadata logic, not a Docker database write.
- Checkpoint quality gate is skipped.
- DLQ recovery has NATS reprocess tests, but not a worker-to-DLQ-to-reprocess workflow.
- Dashboard Prometheus fallback/query behavior has unit coverage, but no real Prometheus query evidence.
- E2E workflow tests are placeholder skips.

## Design

Use Testcontainers as the Docker harness because the project already uses it and it keeps each test isolated. Keep each evidence test narrow enough to debug:

1. `tests/integration/nats_pipeline_test.go`
   - Start real NATS.
   - Create a CDC stream.
   - Publish CDC events through the production NATS adapter.
   - Fetch through a durable consumer.
   - Assert payload, headers, sequence, and ack behavior.

2. `tests/integration/sink_write_test.go`
   - Start real Postgres.
   - Create a destination table.
   - Write create/update/delete CDC events through the production Postgres sink.
   - Assert table state after each write.

3. `tests/integration/delivery_checkpoint_test.go`
   - Replace skipped placeholder with real worker evidence.
   - Start real NATS and real Postgres.
   - Start a real flow worker with a real Postgres sink and NATS KV storage.
   - Publish a CDC event, wait for sink row, wait for structured checkpoint, stop worker.
   - Restart the same durable worker, publish another CDC event, and assert checkpoint advances without replaying the first event.

4. `tests/integration/dashboard_prometheus_test.go`
   - Start an in-process metrics HTTP endpoint with a custom Prometheus registry.
   - Start real Prometheus in Docker and scrape the host endpoint through Testcontainers host port access.
   - Query the production Prometheus metrics reader for p99 flow processing latency.

5. `tests/e2e/workflows_test.go`
   - Replace placeholder skips with Docker workflows:
     - Full CDC pipeline workflow: NATS publish -> flow worker -> Postgres sink -> checkpoint.
     - DLQ recovery workflow: worker routes a sink metadata failure to DLQ, dry-run previews it, selected reprocess republishes it.
     - Explorer workflow: real NATS topic/message/consumer evidence through production adapter.

## Boundaries

- Do not turn Docker evidence into benchmarks.
- Do not require the Vite client.
- Do not require a long-lived Docker Compose stack; tests own their containers and cleanup.
- Keep source connector end-to-end CDC capture out of this pass. The evidence here starts at NATS-published CDC envelopes because source connector Docker capture has its own preflight/reconnect gates.

## Acceptance Criteria

- `make test-integration` runs real Docker-backed tests for NATS publish/fetch, Postgres sink write, checkpoint recovery, DLQ recovery, and Prometheus query.
- `make test-e2e` has real Docker-backed workflows instead of only placeholder skips.
- `make test-unit` still passes.
- `make bench-pipeline` remains local source decode/transform only.
- `git diff --check` passes.
