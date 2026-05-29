# Docker Quality Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Docker-backed integration and E2E tests that prove NATS publish/fetch, real sink write, checkpoint recovery, DLQ recovery, and Prometheus dashboard query behavior.

**Architecture:** Reuse existing Testcontainers helpers and production adapters. Add one Prometheus container helper because dashboard p99 evidence needs a real Prometheus query. Keep tests isolated with unique stream names and per-test Docker containers.

**Tech Stack:** Go `testing`, build tags `integration` and `e2e`, Testcontainers, NATS JetStream, Postgres, Prometheus, production CDC adapters/services.

---

## Task 1: Add Prometheus Testcontainer Helper

**Files:**
- Create: `internal/testutil/containers/prometheus.go`

- [x] Add `RunningPrometheus` with `URL` and `Cleanup`.
- [x] Add `StartPrometheus(ctx, t, scrapePort int)` that writes a Prometheus config scraping `host.testcontainers.internal:<scrapePort>`.
- [x] Configure Testcontainers host port access through `ContainerRequest.HostAccessPorts` so the container can reach the Go test HTTP server.
- [x] Wait for Prometheus `/-/ready`.

## Task 2: Add Integration Evidence Tests

**Files:**
- Create: `tests/integration/nats_pipeline_test.go`
- Create: `tests/integration/sink_write_test.go`
- Replace: `tests/integration/delivery_checkpoint_test.go`
- Create: `tests/integration/dashboard_prometheus_test.go`

- [x] Add NATS publish/fetch round-trip test using production NATS adapter and a durable consumer.
- [x] Add Postgres sink create/update/delete test using production Postgres sink and a real Postgres container.
- [x] Replace skipped checkpoint placeholder with a real NATS + Postgres + worker + NATS KV checkpoint recovery test.
- [x] Add Prometheus p99 query test using a custom registry, host HTTP metrics endpoint, Docker Prometheus, and the production Prometheus reader.
- [x] Run `go test -tags=integration ./tests/integration -run 'TestNATSPublishBatchFetchRoundTrip|TestPostgresSinkWritesCreateUpdateDelete|TestFlowWorkerCheckpointSurvivesWorkerRestart|TestDashboardPrometheusQueryReadsScrapedFlowLatency' -count=1 -v`.

## Task 3: Replace E2E Placeholder Workflows

**Files:**
- Replace: `tests/e2e/workflows_test.go`

- [x] Add `TestFlowWizardCreateValidateRunWorkflow` as a Docker-backed full pipeline workflow through production NATS adapter, flow worker, Postgres sink, and NATS KV checkpoint store.
- [x] Add `TestExplorerInspectMessageAndConsumerWorkflow` as real NATS topic/message/consumer evidence through production adapter calls.
- [x] Add `TestDLQRecoveryWorkflow` as worker-to-DLQ-to-preview-to-selected-reprocess workflow with real NATS and real Postgres sink failure.
- [x] Run `go test -tags=e2e ./tests/e2e`.

## Task 4: Verify

**Files:**
- No source edits unless verification fails.

- [x] Run `make test-unit`.
- [x] Run `make test-integration`.
- [x] Run `make test-e2e`.
- [x] Run `make bench-pipeline`.
- [x] Run `git diff --check`.
- [x] Confirm Docker-backed tests produce non-skipped evidence for the requested quality gates.

## Verification Notes

- The test code compiles and executes under `-tags=integration` and `-tags=e2e`.
- Direct `go test` without the repository Makefile did not inherit the local Colima socket and skipped Docker tests. The repository Make targets set `DOCKER_HOST=unix:///Users/lap14687/.colima/default/docker.sock`, and the Docker-backed integration and E2E suites passed against real containers.
