# CDC Explorer Product Design

Date: 2026-05-28

## Summary

CDC Explorer should become a CDC Operations Explorer, not just a NATS message browser. It should let an operator move from system overview to topic, partition, consumer, message, checkpoint, and DLQ recovery context without guessing where data is stuck or why a record failed.

The product target is similar in operational clarity to Redpanda Console or Kafka UI, but tailored for this CDC system:

- CDC subjects encode `source_id`, `schema`, `table`, and `partition`.
- Events carry CDC headers and Debezium-style `before`, `after`, `source`, and `op` payloads.
- Flow consumers, checkpoints, DLQ, and sink write behavior are first-class product concepts.
- Recovery actions must be guarded because reprocessing can duplicate or corrupt downstream data.

Client-side automated tests are explicitly out of scope for this pass per product direction. Backend unit tests, backend integration tests, and manual/frontend build verification remain in scope.

## Current State

The current Explorer has useful building blocks:

- Topics page lists topics and parses `source/schema/table`.
- Topic detail shows partitions and links to related consumers and DLQ.
- Messages page supports topic and partition selection, plus client-side subject/sequence search.
- Message detail drawer shows payload JSON and headers.
- Consumers page lists filter subjects, pending, ack pending, delivered sequence, and ack floor.
- DLQ page lists failed messages and supports a raw `reprocess all` action.
- Backend has a richer `ExplorerMessageFilter` model for topic, partition, sequence, timestamp, header, text, JSON path, op, source, schema, and table predicates.

Important gaps:

- Advanced message filters are not exposed through proto/API/FE.
- Topic `message_count` is not currently backed by accurate backend stats.
- Partition counts do not expose first sequence, last sequence, latest event time, lag, or consumer context.
- Consumer detail route exists in constants but has no page.
- DLQ recovery is too blunt: no selected reprocess, dry-run, dedupe preview, duplicate-risk classification, or confirm token.
- Message detail does not show before/after diff, routing metadata, checkpoint context, or DLQ/reprocessed context.
- There is no Explorer overview command center.
- There is no Explorer metadata index; current behavior depends on bounded JetStream reads and consumer info.

## Product Goals

Explorer must answer these operational questions quickly:

1. Which source/schema/table topics exist and which are unhealthy?
2. Which partitions are active, stale, lagging, or producing DLQ events?
3. Which flow consumers are reading each topic and where are their ack/checkpoint positions?
4. What exactly is inside a message, including op, key, before/after, source metadata, headers, and routing data?
5. Why did a record enter DLQ, and is it safe to reprocess?
6. If the system restarts, what will replay and what has already been committed?

## Non-Goals

- No client-side automated tests in this pass.
- No full payload full-text indexing in the initial implementation.
- No external analytics database requirement for Explorer.
- No destructive replay controls beyond DLQ reprocess. Consumer seek/reset and checkpoint rewrites are intentionally excluded from the first implementation because they are high-risk operations.
- No unrelated frontend redesign outside Explorer.

## Information Architecture

Explorer navigation should become:

```text
Explorer
├── Overview
├── Topics
│   └── Topic Detail
│       ├── Summary
│       ├── Partitions
│       │   └── Partition Detail
│       │       ├── Message Timeline
│       │       └── Lag & Checkpoints
├── Consumers
│   └── Consumer Detail
└── DLQ
    └── DLQ Detail / Reprocess Preview
```

The main workflow should be drill-down friendly:

```text
Overview -> unhealthy topic -> partition -> filtered messages -> message detail
Overview -> lagging topic -> partition -> lag & checkpoints
Overview -> lagging consumer -> consumer detail -> related partition
Overview -> DLQ topic -> failed message -> dry-run reprocess -> selected reprocess
```

Topic and partition are the primary hierarchy. Messages are not a top-level navigation concept in the main product flow; they live inside a partition timeline. A separate global message search can exist as a hidden/operator command later, but it should not be the main Explorer entry point.

## Status Model

Explorer should use consistent status labels:

| Status | Applies to | Meaning |
|---|---|---|
| `healthy` | topic, partition, consumer | No pending lag, recent activity, no DLQ growth. |
| `idle` | topic, partition | No recent events, no lag. |
| `lagging` | topic, partition, consumer | Consumer pending or ack pending is above zero. |
| `stale` | topic, partition | Latest event timestamp is older than the configured stale threshold. |
| `dlq` | topic, partition, message | At least one failed message is present for this subject scope. |
| `recovering` | DLQ message | Reprocess is running or has been requested. |
| `blocked` | DLQ message/action | Dry-run found a high-risk condition. |

Initial thresholds:

- `stale` if no event is seen for 10 minutes and the topic has had messages before.
- `lagging` if `num_pending > 0` or `num_ack_pending > 0`.
- `dlq` if topic-scoped DLQ count is greater than zero.
- `blocked` if a DLQ dry-run detects invalid payload, missing original subject, or a duplicate-risk policy violation.

## Explorer Overview

Route: `/explorer`

Purpose: command center for CDC operations.

Required panels:

- Total topics
- Total partitions
- Total consumers
- Total pending messages
- Total ack pending messages
- DLQ depth
- Topics with highest lag
- Topics with newest DLQ events
- Recent failed messages
- Recent reprocessed messages

Required table: "Topics Needing Attention"

Columns:

- Health
- Topic
- Source
- Schema
- Table
- Partitions
- Message count
- Max pending
- DLQ count
- Latest event time
- Actions

Actions:

- Open topic
- Open highest-lag partition
- Open DLQ recovery filtered by topic, when DLQ count is non-zero

Implementation note:

- Overview should use a dedicated `GetExplorerOverview` API to avoid the frontend calling many list endpoints and stitching inconsistent snapshots.

## Topics Page

Route: `/explorer/topics`

Required filters:

- Text search by topic/source/schema/table.
- Status filter: all, healthy, lagging, dlq, stale, idle.
- Source filter.
- Schema filter.

Required columns:

- Status
- Topic
- Source
- Schema
- Table
- Partition count
- Message count
- Consumer count
- DLQ count
- First sequence
- Last sequence
- Latest event time
- Max pending
- Actions

Actions:

- Copy topic
- Open detail
- Open highest-lag partition

Backend requirements:

- `message_count` must be accurate within the bounded visibility mode. In the initial implementation it can use stream state and subject scans with caps. In indexed mode it must come from the Explorer metadata index.
- `consumer_count` is derived from flow consumers whose filter subjects include the topic prefix.
- `dlq_count` is derived from DLQ subjects matching `dlq.<topic>.>`.

## Topic Detail

Route: `/explorer/topics/:topic`

Purpose: show the health and partition layout of one CDC table/topic. Topic detail should not duplicate message, consumer, or DLQ pages. The operator should choose a partition before inspecting ordered events and checkpoint state.

Header:

- Parsed short name: `<schema>.<table>`
- Full topic
- Source
- Schema
- Table
- Status
- Latest event time
- Message count
- DLQ count
- Max lag

Sections:

1. Summary
2. Partitions

### Summary Section

Fields:

- Source ID
- Schema
- Table
- Status
- Partition count
- Message count
- Consumer count
- DLQ count
- First sequence
- Last sequence
- Latest event time
- Max pending
- Max ack pending

Actions:

- Copy topic

### Partitions Section

Columns:

- Partition ID
- Status
- First sequence
- Last sequence
- Message count
- Latest event time
- Consumer count
- Max pending
- Max ack pending
- DLQ count
- Actions

Actions:

- Open partition detail

## Partition Detail

Route: `/explorer/topics/:topic/partitions/:partition`

Purpose: inspect the ordered lane where CDC event order, replay, lag, and checkpoint state matter. This is the main place to view messages for a topic.

Header:

- Topic
- Partition ID
- Status
- First sequence
- Last sequence
- Message count
- Latest event time
- Max pending
- Max ack pending
- DLQ count

Sections:

1. Message Timeline
2. Lag & Checkpoints

### Message Timeline

The message timeline is pre-filtered to the selected topic and partition.

Columns:

- Time
- Op
- Sequence
- Key/ID
- Payload size
- Header count
- Reprocessed marker
- DLQ-related marker

Actions:

- Open message detail drawer
- Copy sequence
- Copy subject

### Lag & Checkpoints

Columns:

- Flow ID
- Consumer name
- Partition
- Ack floor stream sequence
- Delivered stream sequence
- NATS pending
- Source offset or LSN
- Stored checkpoint key
- Stored checkpoint offset
- Updated at
- Replay risk

Replay risk:

- `low`: ack floor and stored checkpoint are aligned.
- `medium`: NATS ack is ahead of stored checkpoint, so restart may replay.
- `high`: stored checkpoint is ahead of confirmed sink/ack evidence or checkpoint data is missing.

## Partition Message Timeline

Routes:

- `/explorer/topics/:topic/partitions/:partition`

The message timeline is scoped to one topic and one partition. This keeps the hierarchy clear: topic shows partitions, partition shows messages.

Optional future route:

- `/explorer/search/messages` for cross-system operator search. This should be hidden from the main sidebar and implemented only if operators need to find records without knowing the topic.

### Server-Side Filters

Required filters:

- Topic, fixed by route
- Partition, fixed by route
- Status: all, sent, unsent
- Op: create, update, delete, snapshot
- Source ID
- Schema
- Table
- Sequence min
- Sequence max
- Timestamp from
- Timestamp to
- Header key
- Header value
- JSON path
- JSON equals
- Text contains
- Reprocessed only
- DLQ-related only
- Sort: newest first, oldest first
- Limit
- Page or cursor

Filter behavior:

- Topic and partition must be pushed down to NATS via filter subject.
- Sequence and timestamp filters should be applied before expensive payload parsing when possible.
- Header filters should be applied before JSON payload parsing.
- JSON path filters should parse only the selected capped window unless indexed field filtering is available.
- Text contains scans subject, headers, and payload within a hard cap.

Initial hard caps:

- Maximum `limit`: 500.
- Maximum fetch window for unindexed post-filtering: 2,000 messages.
- Maximum payload bytes parsed per message in search: 256 KiB.
- If caps are hit, response must return `partial=true` and `scan_limit_hit=true`.

### Timeline Table

Columns:

- Time
- Op
- Sequence
- Key/ID
- Payload size
- Headers count
- Status
- Markers: reprocessed, DLQ-related

Key/ID extraction order:

1. `after.id`
2. `before.id`
3. `after.<primary_key>` if payload shape/index knows primary key
4. `headers["Nats-Msg-Id"]`
5. Empty

### Message Detail Drawer

Tabs:

1. Overview
2. Before/After
3. Payload
4. Headers
5. Source Metadata
6. Routing
7. Checkpoint Context
8. Raw

Overview fields:

- Subject
- Topic
- Partition
- Sequence
- Timestamp
- Op
- Source ID
- Schema
- Table
- Key/ID
- Payload size
- Header count
- NATS message ID
- Reprocessed-from DLQ ID, if present

Before/After tab:

- Show `before` and `after` side by side.
- Highlight changed fields for update events.
- Show tombstone/delete state clearly for delete events.

Payload tab:

- JSON viewer
- Copy payload
- Copy compact JSON

Headers tab:

- Search header keys
- Copy all headers
- Copy individual header values

Source Metadata tab:

- Show Debezium `source` fields: connector, db, schema, table, LSN, binlog file/pos, tx id, source timestamp.

Routing tab:

- Subject
- Topic
- Partition
- Header routing metadata
- Filter subject that matched, when available

Checkpoint Context tab:

- Related consumers
- Ack floor
- Delivered sequence
- Pending count
- Stored checkpoint key and offset, when available

Raw tab:

- Base64 or raw bytes display
- Copy raw bytes as base64
- Copy subject
- Copy sequence

## Consumers

Routes:

- `/explorer/consumers`
- `/explorer/consumers/:consumer`

### Consumers List

Filters:

- Topic
- Source
- Schema
- Table
- Status
- Text search by consumer name/filter subject

Columns:

- Status
- Consumer name
- Flow ID
- Filter subjects
- Related topic count
- Pending
- Ack pending
- Delivered stream sequence
- Ack floor stream sequence
- Estimated lag
- Last active time
- Actions

Estimated lag:

- Primary: `NumPending + NumAckPending`.
- If stream last sequence is available: `stream_last_sequence - ack_floor_stream_sequence`.
- Display both if they differ, because pending and sequence distance answer different questions.

### Consumer Detail

Header:

- Consumer name
- Flow ID
- Status
- Pending
- Ack pending
- Delivered sequence
- Ack floor
- Estimated lag

Sections:

- Filter subjects
- Related topics
- Per-topic pending summary
- Per-partition lag summary, when derivable
- Recent messages matching filter
- DLQ messages matching filter
- Checkpoint context

Actions:

- Open flow detail
- Open matching messages
- Open matching DLQ
- Copy consumer name
- Copy filter subjects

No reset/seek action in this phase.

## DLQ Recovery Console

Routes:

- `/explorer/dlq`
- `/explorer/dlq/:dlqId` if stable IDs are exposed

### DLQ List Filters

- Original topic
- Original partition
- Source ID
- Schema
- Table
- Op
- Reason contains
- Error class
- Retry count min/max
- Delivery count min/max
- Failed timestamp from/to
- Header key/value
- JSON path equals
- Text contains
- Duplicate risk
- Reprocessed status

### DLQ Table

Columns:

- Select checkbox
- Failed at
- Status
- Original topic
- Partition
- Op
- Source/schema/table
- Reason
- Error class
- Retry count
- Delivery count
- Duplicate risk
- Payload size
- Actions

### DLQ Detail

Tabs:

1. Failure
2. Original Payload
3. Original Headers
4. Reprocess Preview
5. Related Messages

Failure tab:

- DLQ ID
- Reason
- Error class
- Failed at
- Retry count
- Delivery count
- Original subject
- Original NATS message ID
- Flow ID
- Sink ID

Reprocess Preview tab:

- Target subject
- Deterministic reprocess message ID
- Duplicate risk
- Expected affected message count
- Warnings
- Blocking findings

### DLQ Actions

Actions:

- Dry-run selected
- Reprocess selected
- Dry-run current filter
- Reprocess current filter with confirm token
- Copy original subject
- Open original topic
- Open related messages

Guardrails:

- Disable direct unconfirmed `reprocess all`.
- Bulk reprocess requires dry-run first.
- Dry-run returns a confirm token tied to filter hash, selected IDs, count, and expiration.
- Reprocess with confirm token must fail if DLQ count or selected set changed.
- Maximum selected reprocess default: 500.
- Maximum filter-based bulk reprocess default: 5,000.
- Reprocess response must include count succeeded, count skipped, count failed, and failure reasons.

Duplicate-risk classification:

| Risk | Meaning |
|---|---|
| `none` | Original message ID absent from main stream lookup window and deterministic reprocess ID is new. |
| `possible` | Lookup window could not prove absence or sink idempotency is unknown. |
| `high` | Original or deterministic reprocess ID is already visible, or same DLQ envelope was reprocessed before. |
| `blocked` | Payload invalid, original subject missing, or policy forbids reprocess. |

## API Contract

Proto should be expanded while keeping existing fields backwards-compatible.

### New/Expanded Messages

`ExplorerOverview`

- totals: topics, partitions, consumers, pending, ack pending, DLQ depth
- top lagging topics
- top DLQ topics
- recent DLQ messages
- recent reprocessed messages

`TopicSummary`

- name
- source_id
- schema
- table
- status
- message_count
- partition_count
- consumer_count
- dlq_count
- first_sequence
- last_sequence
- latest_event_timestamp
- max_pending
- max_ack_pending

`TopicDetail`

- summary
- partitions

`PartitionDetail`

- summary
- message_timeline
- lag_checkpoints

`PartitionSummary`

- topic
- id
- status
- message_count
- first_sequence
- last_sequence
- latest_event_timestamp
- consumer_count
- max_pending
- max_ack_pending
- dlq_count

`MessageFilter`

- status
- topic
- partition
- source_id
- schema
- table
- op
- sequence_min
- sequence_max
- timestamp_from
- timestamp_to
- header_key
- header_value
- json_path
- json_equals
- text_contains
- reprocessed_only
- dlq_related_only
- sort
- pagination

`MessageItem`

- existing fields
- op
- source_id
- schema
- table
- partition
- key
- payload_size
- header_count
- nats_msg_id
- reprocessed_from
- markers

`MessageDetail`

- item
- decoded_payload
- before
- after
- changed_fields
- source_metadata
- routing_metadata
- checkpoint_context

`ConsumerSummary`

- existing fields
- flow_id
- status
- related_topics
- estimated_lag
- last_active_timestamp

`ConsumerDetail`

- summary
- filter_subjects
- related_topics
- partition_lag
- recent_messages
- recent_dlq_messages
- checkpoint_context

`DLQMessage`

- existing fields
- dlq_id
- flow_id
- source_id
- sink_id
- schema
- table
- op
- msg_id
- retry_count
- delivery_count
- error_class
- failed_at
- duplicate_risk
- reprocessed_count
- last_reprocessed_at

`DLQDryRunRequest`

- selected_dlq_ids
- filter
- max_count

`DLQDryRunResponse`

- confirm_token
- expires_at
- matched_count
- preview_items
- risk_counts
- warnings
- blocking_findings

`DLQReprocessRequest`

- selected_dlq_ids
- filter
- confirm_token
- dry_run
- max_count

`DLQReprocessResponse`

- succeeded_count
- skipped_count
- failed_count
- failures
- reprocessed_message_ids

### Endpoints

Existing endpoints can remain. Add:

- `GET /api/v1/explorer/overview`
- `GET /api/v1/topics/{topic}`
- `GET /api/v1/topics/{topic}/partitions/{partition}`
- `GET /api/v1/topics/{topic}/partitions/{partition}/messages`
- `GET /api/v1/messages/{sequence}` with topic/partition query when needed
- `GET /api/v1/consumers/{consumer}`
- `POST /api/v1/dlq/reprocess/preview`
- `POST /api/v1/dlq/reprocess`

Existing `GET /api/v1/messages` can remain for backwards compatibility or hidden advanced search, but the primary product flow should use partition-scoped message endpoints.

## Backend Architecture

### Phase 1 Backend: Bounded JetStream Read Model

Use current NATS/JetStream state directly:

- Topics: recent subjects from `cdc.>` plus stream info.
- Partitions: recent subjects matching topic prefix.
- Partition messages: ephemeral consumer with topic+partition filter subject, then post-filter.
- Consumers: JetStream consumer info.
- DLQ: DLQ stream messages and envelope fields.

This phase is enough to unblock the rich UI and operational workflows.

Limitations:

- Counts may be approximate when only derived from capped scans.
- JSON path/text filtering scans a bounded window.
- Payload shape inference uses recent messages only.

Response metadata must expose partial results:

- `partial`
- `scan_limit_hit`
- `scanned_count`
- `matched_count`
- `max_scan`

### Phase 2 Backend: Explorer Metadata Index

Add `ExplorerIndexService` after API/UI is stable.

Responsibilities:

- Consume or sample the main CDC stream and DLQ stream.
- Store bounded metadata, not full payloads by default.
- Maintain topic stats, partition stats, op counters, latest event timestamp, DLQ counts, and reprocess markers.
- Support fast overview and topic list without scanning streams.
- Rebuild from recent stream window on startup if index is empty.

Interfaces:

```go
type ExplorerIndexStore interface {
    UpsertMessageMeta(ctx context.Context, meta MessageMeta) error
    UpsertDLQMeta(ctx context.Context, meta DLQMeta) error
    TopicSummaries(ctx context.Context, filter TopicFilter) ([]TopicSummary, error)
    PartitionSummaries(ctx context.Context, topic string) ([]PartitionSummary, error)
    Overview(ctx context.Context) (ExplorerOverview, error)
    MarkReprocessed(ctx context.Context, dlqID string, reprocessID string, timestamp int64) error
}
```

Default storage:

- Start with NATS KV for aggregate stats and reprocess markers.
- Keep message metadata bounded by topic/partition latest windows.
- Do not add an external database dependency in the first product-grade pass.

Future optional storage:

- A dedicated analytical index can be introduced later if full retention search or large historical payload search is required.

## Data Extraction Rules

Subject format:

```text
cdc.<source_id>.<schema>.<table>.<partition>
```

Fallbacks:

- Source ID from header `cdc-instance-id` if subject parse fails.
- Schema from header `cdc-schema`.
- Table from header `cdc-table`.
- Op from header `cdc-op`, then payload `op`.
- Partition from header `cdc-partition`, then subject suffix.

Payload fields:

- `before`: prior row state.
- `after`: current row state.
- `source`: origin metadata.
- `op`: CDC operation.
- `ts_ms`: event timestamp.

Key extraction:

1. `after.id`
2. `before.id`
3. configured primary key from flow/table metadata
4. `Nats-Msg-Id`
5. empty

Changed fields:

- For update events, compare `before` and `after` shallow fields first.
- Nested diff can be added later; initial diff should still mark changed top-level fields correctly.

## Frontend Architecture

Keep the existing React/Vite/TanStack Query structure.

Components to add:

- `ExplorerOverviewPage`
- `TopicHealthBadge`
- `ExplorerFilterBar`
- `PartitionDetailPage`
- `PartitionMessageTimeline`
- `MessageDetailSheet` expanded with tabs
- `ConsumerDetailPage`
- `DLQFilterBar`
- `DLQTable`
- `DLQDryRunDialog`
- `ReprocessConfirmDialog`
- `BeforeAfterDiff`
- `PayloadShapePanel`
- `CheckpointContextPanel`

Design principles:

- Dense, operator-focused UI.
- No marketing/landing-page layout.
- Tables should be scan-friendly and horizontally resilient.
- Use icons for actions: refresh, copy, open, filter, dry-run, reprocess.
- Use badges for status, op, risk, and markers.
- Avoid nested cards; use full-width sections and tables.

State handling:

- Filters should be encoded in URL query params.
- Topic/partition routes prefill filters.
- Detail drawers should not discard filters when closed.
- Refresh actions should preserve filters and selection.
- DLQ selection should survive pagination only within current filter result if selected IDs are explicit.

## Error Handling

Explorer API errors should be actionable:

- Invalid JSON path -> `400` with field `json_path`.
- Invalid sequence range -> `400`.
- Invalid timestamp range -> `400`.
- Scan cap hit -> `200` with `partial=true`, not error.
- Stream unavailable -> `503`.
- Consumer not found -> `404`.
- DLQ dry-run confirm token expired -> `409`.
- DLQ set changed after dry-run -> `409`.

Frontend behavior:

- Show inline filter errors near the filter field.
- Show partial result banner when `scan_limit_hit=true`.
- Disable unsafe actions when backend returns blocking findings.
- Show empty state specific to current filter, not generic no data.

## Guardrails

- All list endpoints enforce maximum limit.
- Payload parsing enforces max bytes per message.
- Bulk DLQ reprocess requires dry-run and confirm token.
- Reprocess all must always be filtered or explicitly confirmed.
- Dangerous actions should show count, risk summary, and exact filter used.
- Secrets in payload/headers should support configurable redaction keys:
  - `password`
  - `secret`
  - `token`
  - `api_key`
  - `authorization`

Initial redaction can be display-only in frontend and response shaping in backend. Original payload must not be mutated.

## Testing Strategy

No client tests in this pass.

Backend unit tests:

- Subject parsing.
- Message filter matching.
- JSON path filter.
- Header filter.
- Sequence/timestamp validation.
- Changed field extraction.
- Duplicate-risk classification.
- Confirm token validation.

Backend integration tests with Docker:

- Explorer overview with seeded NATS messages.
- Topic stats for multiple topics and partitions.
- Partition message timeline with op/source/schema/table/header/json/time/sequence filters.
- Consumer detail with pending and ack pending.
- DLQ list filters.
- DLQ dry-run does not mutate streams.
- DLQ selected reprocess republishes only selected messages.
- Reprocess confirm token fails when DLQ set changes.

Manual/frontend verification:

- `npm run build` when Node is available.
- Open Explorer overview.
- Open topic, click a partition, and filter the partition message timeline by op/header/json path.
- Open message detail and inspect before/after.
- Open consumer detail from global Consumers or partition lag context.
- Dry-run selected DLQ messages and confirm reprocess.

## Implementation Phases

### Phase 1: API Contract And Backend Filter Wiring

Scope:

- Expand proto and generated API types.
- Wire `ListMessages` service to advanced filter model.
- Add request/response DTOs for advanced filters.
- Add response metadata for partial scan/caps.
- Keep existing simple query params backwards-compatible.

Acceptance:

- Existing FE still works.
- Advanced filters pass backend unit tests.
- Integration test proves JSON/header/op/topic/partition filters.

### Phase 2: Topic, Partition, And Overview Stats

Scope:

- Add `GetExplorerOverview`.
- Add accurate topic and partition summary fields as far as JetStream direct mode allows.
- Add topic status computation.
- Add partition status computation.
- Add consumer count and DLQ count per topic.

Acceptance:

- Topics show non-zero real message counts when messages exist.
- Overview identifies lagging consumers and DLQ topics.
- Integration test seeds multiple topics and verifies stats.

### Phase 3: Rich Read-Only Frontend

Scope:

- Add Explorer overview.
- Upgrade topics table.
- Simplify topic detail to summary plus partitions.
- Add partition detail with message timeline and Lag & Checkpoints sections.
- Add advanced message filter bar.
- Expand message detail drawer with before/after, source metadata, routing, checkpoint context, raw.
- Add consumer detail page.

Acceptance:

- Operator can navigate Overview -> Topic -> Partition -> Message Detail.
- Operator can inspect partition lag and checkpoint state next to that partition's messages.
- Operator can filter partition messages by JSON path and header from the UI.
- No client automated tests required.

### Phase 4: DLQ Recovery Console

Scope:

- Add DLQ filters.
- Add selected row state.
- Add dry-run preview API.
- Add selected reprocess API.
- Add confirm token guard for filter-based bulk reprocess.
- Replace direct `reprocess all` button with guarded workflow.

Acceptance:

- Dry-run selected mutates nothing.
- Reprocess selected republishes only selected DLQ messages.
- Duplicate-risk preview is visible before mutation.
- Bulk reprocess cannot run without a fresh confirm token.

### Phase 5: Checkpoint And Replay Context

Scope:

- Add checkpoint context API for topic/partition/consumer/message.
- Show ack floor, delivered sequence, stored checkpoint, source offset/LSN/binlog position where available.
- Add replay-risk classification.

Acceptance:

- Partition detail Lag & Checkpoints section shows flow/consumer checkpoint state.
- Message detail shows whether related consumers are ahead/behind the message sequence.
- Integration tests prove checkpoint context with seeded consumer state.

### Phase 6: Explorer Metadata Index

Scope:

- Add `ExplorerIndexService`.
- Store bounded aggregate stats and reprocess markers.
- Backfill recent stream window on startup.
- Make overview and topic summaries use index when available.
- Keep direct JetStream mode as fallback.

Acceptance:

- Overview loads without scanning large stream windows.
- Topic stats remain stable across refreshes.
- Reprocess markers survive restart.
- Index can rebuild from recent stream window.

### Phase 7: Hardening And Documentation

Scope:

- Document Explorer limits and partial result semantics.
- Add operator docs for DLQ recovery.
- Add quality gates to `docs/QUALITY_GATES.md`.
- Add release checklist for Explorer.

Acceptance:

- Docs explain safe reprocess flow.
- Docs explain why message search can return partial results without index.
- Quality gates include Explorer API and DLQ recovery integration tests.

## File Map

Backend/proto:

- `proto/cdc/v1/explorer.proto`
- `proto/cdc/v1/dlq.proto`
- `proto/cdc/v1/service.proto`
- `api/proto/v1/*`
- `internal/core/dto/request/explorer.go`
- `internal/core/dto/response/explorer.go`
- `internal/core/dto/request/dlq.go`
- `internal/core/dto/response/dlq.go`
- `internal/core/service/explorer.go`
- `internal/core/service/dlq.go`
- `internal/adapters/driver/grpc/handler_explorer.go`
- `internal/adapters/driver/grpc/grpc_service.go`
- `internal/adapters/driven/nats/browser.go`
- `internal/adapters/driven/nats/browser_filter.go`
- `internal/adapters/driven/nats/dlq.go`
- new: `internal/core/service/explorer_projection.go`
- new later: `internal/core/service/explorer_index.go`

Frontend:

- `website/src/config/routes.ts`
- `website/src/lib/api/endpoints.ts`
- `website/src/lib/query/explorer.ts`
- `website/src/types/api.ts`
- `website/src/features/explorer/topics/page.tsx`
- `website/src/features/explorer/topics/detail.tsx`
- `website/src/features/explorer/consumers/page.tsx`
- `website/src/features/explorer/dlq/page.tsx`
- `website/src/features/explorer/components/MessageDetailSheet.tsx`
- new: `website/src/features/explorer/overview/page.tsx`
- new: `website/src/features/explorer/partitions/detail.tsx`
- new: `website/src/features/explorer/consumers/detail.tsx`
- new: `website/src/features/explorer/components/ExplorerFilterBar.tsx`
- new: `website/src/features/explorer/components/PartitionMessageTimeline.tsx`
- new: `website/src/features/explorer/components/BeforeAfterDiff.tsx`
- new: `website/src/features/explorer/components/DLQDryRunDialog.tsx`
- new: `website/src/features/explorer/components/ReprocessConfirmDialog.tsx`
- i18n files under `website/src/lib/i18n/locales/`

Tests:

- `internal/adapters/driven/nats/browser_filter_test.go`
- `internal/adapters/driven/nats/dlq_reprocess_test.go`
- `tests/integration/explorer_messages_test.go`
- `tests/integration/explorer_consumers_test.go`
- `tests/integration/dlq_recovery_test.go`

## Acceptance Criteria

The Explorer upgrade is complete when:

- Overview exposes topic, partition, consumer, pending, ack pending, and DLQ depth.
- Topics page shows real message count, partition count, consumer count, DLQ count, latest event time, and health.
- Topic detail has a focused Summary and Partitions view, without topic-level Messages, Consumers, or DLQ tabs.
- Partition detail has a message timeline and Lag & Checkpoints section.
- Partition messages can be filtered server-side by op, source, schema, table, header, JSON path, text, sequence range, and timestamp range.
- Message detail shows before/after diff, payload, headers, source metadata, routing, checkpoint context, and raw payload.
- Consumers page links to consumer detail.
- Consumer detail shows related topics, pending, ack pending, estimated lag, recent messages, DLQ, and checkpoint context.
- DLQ supports filters, selected rows, dry-run preview, duplicate-risk classification, selected reprocess, and guarded filter-based bulk reprocess.
- Direct unguarded `reprocess all` is removed or replaced by confirm-token workflow.
- Backend integration tests cover message filters, consumer detail, DLQ dry-run, selected reprocess, and confirm-token guard.
- Frontend builds successfully when Node/npm is available.

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Large stream scans make Explorer slow. | Use NATS subject filtering, hard caps, partial result metadata, and later Explorer index. |
| DLQ bulk reprocess duplicates data. | Require dry-run, duplicate-risk preview, confirm token, deterministic IDs, and selected reprocess first. |
| Counts are misleading before indexer exists. | Mark direct-mode stats as bounded/partial when caps are hit. |
| Payload search is expensive. | Apply cheap filters first, cap payload bytes, and defer full payload indexing. |
| Frontend becomes too complex. | Keep topic detail focused on summary and partitions; extract partition timeline, filter bar, message detail, and DLQ dialogs. |
| Checkpoint semantics differ per source. | Display raw offset/LSN/binlog position plus risk classification rather than forcing one numeric lag model. |

## Final Recommendation

Implement Phases 1 through 4 first. That creates a genuinely useful Explorer without introducing a new indexing subsystem too early. Then add Phase 5 checkpoint context and Phase 6 Explorer metadata index once the API and UI workflows are stable.

This order gives the product visible value quickly while keeping the deeper indexing and replay-risk work explicit and testable.
