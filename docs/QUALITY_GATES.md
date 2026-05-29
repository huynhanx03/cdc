# CDC Quality Gates

This document defines the commands and evidence required before a CDC product phase can be called ready. Unit tests keep local iteration fast. Integration and E2E tests use Docker-backed infrastructure to prove behavior against real NATS, databases, and public APIs. Pipeline benchmarks stay local and CPU-bound so performance checks are stable and do not require Docker.

## Commands

| Gate | Command | Expected use |
|---|---|---|
| Unit | `make test-unit` | Every local change and every pull request |
| Integration | `make test-integration` | Before merging CDC correctness, source, sink, NATS, Explorer, or DLQ changes |
| E2E | `make test-e2e` | Before releasing product workflow changes |
| Benchmarks | `make bench-pipeline` | Before performance-sensitive changes and release candidates |
| Full | `make test-all` | Release candidate validation |

## Readiness Criteria

- No known data-loss path in crash/restart tests.
- Checkpoints never advance ahead of sink success and message ACK semantics.
- PostgreSQL and MySQL preflight catches missing prerequisites before running a flow.
- NATS retention is bounded by the minimum checkpoint across all flows that consume a stream.
- DLQ dry-run mutates no messages.
- DLQ selected reprocess preserves original headers and only republishes selected messages.
- Explorer filters run server-side, are capped, and return deterministic results.
- Benchmarks record local source decode, filter, and mapping performance. Docker-backed publish/fetch, sink write, checkpoint, and E2E freshness evidence belongs in integration or E2E tests.

## Product Phase Gates

| Product phase | Required evidence |
|---|---|
| Phase 0: Stabilize current remediation | `make test-unit` passes and generated proto compiles. |
| Phase 1: CDC Safety Foundation | Unit and integration tests prove preflight failures, checkpoint ordering, retention safety, and source reconnect behavior. |
| Phase 2: Flow Wizard | Unit and API tests prove `ValidateFlow`, schema diff, mapping dry-run, and filter dry-run produce fatal/warning/pass results correctly. |
| Phase 3: Observability | Unit and integration tests prove lag, pending, retry, DLQ, and freshness metrics are emitted with stable labels. |
| Phase 4: Explorer Backend | Real JetStream integration tests prove topic, partition, consumer, message search, JSON/header/time/sequence filters. |
| Phase 5: Explorer UI | Browser tests prove message filtering, payload inspection, consumer drill-down, and DLQ context workflows. |
| Phase 6: DLQ Recovery | E2E tests prove dry-run, selected reprocess, dedupe preview, and guarded bulk reprocess do not corrupt state. |

## Interpreting Skips

Integration and E2E tests may skip when Docker is unavailable or when the product feature under test has not been implemented yet. A skipped test is not product-grade evidence. It is a visible gap that must be converted into a passing test before claiming that phase is complete.

## CI Schedule

- Pull requests and pushes to `develop` or `main` run `make test-unit`.
- Nightly and manual `quality` workflow runs can execute Docker-backed integration tests.
- Manual `quality` workflow runs can execute local pipeline benchmarks for release candidates.
