# CDC Explorer Product Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a focused CDC Operations Explorer where topics lead to partitions, partitions expose message timelines plus lag/checkpoint context, and global Consumers/DLQ remain operational tools.

**Architecture:** The first implementation uses direct JetStream reads with hard scan caps and explicit partial-result metadata. Topic detail stays simple: summary plus partitions. Partition detail becomes the main inspection surface for messages and checkpoint/lag state. DLQ recovery is guarded through dry-run, duplicate-risk preview, selected reprocess, and confirm-token validation.

**Tech Stack:** Go, protobuf/gRPC gateway, NATS JetStream, NATS KV, React/Vite/TanStack Query, existing shadcn-style UI components, Go unit tests, Docker-backed integration tests. No client automated tests are required for this pass.

---

## Scope Boundary

Implement phases 1 through 5 from the spec:

- API/proto contract.
- Backend projections and advanced filters.
- Accurate topic/partition overview in direct JetStream mode.
- Partition detail with message timeline and Lag & Checkpoints.
- Global consumer detail.
- DLQ dry-run and selected reprocess guardrails.
- Frontend build verification only, no client test suite.

Do not implement the Explorer metadata index in this pass. Keep direct JetStream mode as the only data source, but shape responses so the index can be added later without changing frontend concepts.

## File Structure

### Proto/API

- Modify: `proto/cdc/v1/explorer.proto`
  - Add Explorer overview, topic detail, partition detail, filters, message detail, checkpoint context, response metadata.
- Modify: `proto/cdc/v1/dlq.proto`
  - Add DLQ filters, dry-run preview, selected reprocess request/response, duplicate-risk fields.
- Modify: `proto/cdc/v1/service.proto`
  - Add HTTP bindings for overview, topic detail, partition detail, partition messages, message detail, consumer detail, DLQ preview, and guarded reprocess.
- Generated: `api/proto/v1/*`
  - Regenerate with `make gen-proto`.

### Backend DTO/Service

- Modify: `internal/core/dto/request/explorer.go`
- Modify: `internal/core/dto/response/explorer.go`
- Modify: `internal/core/dto/request/dlq.go`
- Modify: `internal/core/dto/response/dlq.go`
- Modify: `internal/core/ports/nats.go`
- Modify: `internal/core/service/explorer.go`
- Modify: `internal/core/service/dlq.go`
- Create: `internal/core/service/explorer_projection.go`
- Create: `internal/core/service/explorer_projection_test.go`
- Create: `internal/core/service/dlq_reprocess_guard.go`
- Create: `internal/core/service/dlq_reprocess_guard_test.go`

### NATS Adapter

- Modify: `internal/adapters/driven/nats/browser.go`
- Modify: `internal/adapters/driven/nats/browser_filter.go`
- Modify: `internal/adapters/driven/nats/dlq.go`
- Create: `internal/adapters/driven/nats/browser_projection.go`
- Create: `internal/adapters/driven/nats/browser_projection_test.go`

### gRPC/REST Handlers

- Modify: `internal/adapters/driver/grpc/handler_explorer.go`
- Modify: `internal/adapters/driver/grpc/grpc_service.go`

### Frontend

- Modify: `website/src/config/routes.ts`
- Modify: `website/src/lib/api/endpoints.ts`
- Modify: `website/src/lib/query/explorer.ts`
- Modify: `website/src/types/api.ts`
- Modify: `website/src/App.tsx`
- Modify: `website/src/features/explorer/topics/page.tsx`
- Modify: `website/src/features/explorer/topics/detail.tsx`
- Modify: `website/src/features/explorer/consumers/page.tsx`
- Modify: `website/src/features/explorer/dlq/page.tsx`
- Modify: `website/src/features/explorer/components/MessageDetailSheet.tsx`
- Create: `website/src/features/explorer/overview/page.tsx`
- Create: `website/src/features/explorer/partitions/detail.tsx`
- Create: `website/src/features/explorer/consumers/detail.tsx`
- Create: `website/src/features/explorer/components/ExplorerFilterBar.tsx`
- Create: `website/src/features/explorer/components/PartitionMessageTimeline.tsx`
- Create: `website/src/features/explorer/components/BeforeAfterDiff.tsx`
- Create: `website/src/features/explorer/components/DLQDryRunDialog.tsx`
- Create: `website/src/features/explorer/components/ReprocessConfirmDialog.tsx`
- Modify: `website/src/lib/i18n/locales/en.json`
- Modify: `website/src/lib/i18n/locales/vi.json`
- Modify: `website/src/lib/i18n/locales/zh.json`

### Backend Tests

- Modify: `internal/adapters/driven/nats/browser_filter_test.go`
- Modify: `tests/integration/explorer_messages_test.go`
- Modify: `tests/integration/explorer_consumers_test.go`
- Modify: `tests/integration/dlq_recovery_test.go`

---

## Task 1: Expand Explorer Proto Contract

**Files:**
- Modify: `proto/cdc/v1/explorer.proto`
- Modify: `proto/cdc/v1/service.proto`

- [ ] **Step 1: Add Explorer enums and metadata messages**

Update `proto/cdc/v1/explorer.proto` after `MessageStatus`:

```proto
enum ExplorerHealthStatus {
  EXPLORER_HEALTH_STATUS_UNSPECIFIED = 0;
  EXPLORER_HEALTH_STATUS_HEALTHY = 1;
  EXPLORER_HEALTH_STATUS_IDLE = 2;
  EXPLORER_HEALTH_STATUS_LAGGING = 3;
  EXPLORER_HEALTH_STATUS_STALE = 4;
  EXPLORER_HEALTH_STATUS_DLQ = 5;
}

enum ExplorerSort {
  EXPLORER_SORT_UNSPECIFIED = 0;
  EXPLORER_SORT_NEWEST_FIRST = 1;
  EXPLORER_SORT_OLDEST_FIRST = 2;
}

message ExplorerScanMetadata {
  bool partial = 1;
  bool scan_limit_hit = 2;
  uint64 scanned_count = 3;
  uint64 matched_count = 4;
  uint64 max_scan = 5;
}
```

- [ ] **Step 2: Replace `ListMessagesRequest` with partition-capable filters**

Keep existing field numbers 1-5 and add new fields after field 5:

```proto
message ListMessagesRequest {
  MessageStatus status = 1;
  optional string source_id = 2;
  optional string topic = 3;
  optional string partition = 4;
  OffsetPaginationRequest pagination = 5;
  optional string schema = 6;
  optional string table = 7;
  optional string op = 8;
  optional uint64 sequence_min = 9;
  optional uint64 sequence_max = 10;
  optional int64 timestamp_from = 11;
  optional int64 timestamp_to = 12;
  optional string header_key = 13;
  optional string header_value = 14;
  optional string json_path = 15;
  optional string json_equals = 16;
  optional string text_contains = 17;
  bool reprocessed_only = 18;
  bool dlq_related_only = 19;
  ExplorerSort sort = 20;
}
```

- [ ] **Step 3: Expand message item and response**

Replace `ListMessagesResponse` and `MessageItem` with:

```proto
message ListMessagesResponse {
  repeated MessageItem data = 1;
  uint64 total_count = 2;
  OffsetPaginationResponse pagination = 3;
  ExplorerScanMetadata scan = 4;
}

message MessageItem {
  uint64 sequence = 1;
  string timestamp = 2;
  string subject = 3;
  bytes data = 4;
  map<string, string> headers = 5;
  string op = 6;
  string source_id = 7;
  string schema = 8;
  string table = 9;
  string partition = 10;
  string key = 11;
  uint64 payload_size = 12;
  uint32 header_count = 13;
  string nats_msg_id = 14;
  string reprocessed_from = 15;
  repeated string markers = 16;
}
```

- [ ] **Step 4: Add overview, topic, partition, and checkpoint messages**

Append to `explorer.proto`:

```proto
message ExplorerOverviewRequest {}

message ExplorerOverviewResponse {
  uint64 topic_count = 1;
  uint64 partition_count = 2;
  uint64 consumer_count = 3;
  uint64 pending_count = 4;
  uint64 ack_pending_count = 5;
  uint64 dlq_depth = 6;
  repeated TopicSummary topics_needing_attention = 7;
  repeated DLQMessageSummary recent_dlq = 8;
}

message TopicDetailRequest {
  string topic = 1;
}

message TopicDetailResponse {
  TopicSummary summary = 1;
  repeated PartitionSummary partitions = 2;
}

message PartitionDetailRequest {
  string topic = 1;
  string partition = 2;
}

message PartitionDetailResponse {
  PartitionSummary summary = 1;
  repeated CheckpointContext checkpoints = 2;
}

message MessageDetailRequest {
  string topic = 1;
  string partition = 2;
  uint64 sequence = 3;
}

message MessageDetailResponse {
  MessageItem item = 1;
  string decoded_payload = 2;
  string before_json = 3;
  string after_json = 4;
  repeated string changed_fields = 5;
  map<string, string> source_metadata = 6;
  map<string, string> routing_metadata = 7;
  repeated CheckpointContext checkpoint_context = 8;
}

message CheckpointContext {
  string flow_id = 1;
  string consumer_name = 2;
  string topic = 3;
  string partition = 4;
  uint64 ack_floor_stream_seq = 5;
  uint64 delivered_stream_seq = 6;
  uint64 nats_pending = 7;
  uint64 nats_ack_pending = 8;
  string source_offset = 9;
  string stored_checkpoint_key = 10;
  string stored_checkpoint_offset = 11;
  int64 updated_at = 12;
  string replay_risk = 13;
}

message DLQMessageSummary {
  uint64 sequence = 1;
  int64 timestamp = 2;
  string original_subject = 3;
  string reason = 4;
  string error_class = 5;
}
```

- [ ] **Step 5: Expand topic and partition summaries**

Replace `TopicSummary` and `PartitionSummary`:

```proto
message TopicSummary {
  string name = 1;
  uint64 message_count = 2;
  int32 partition_count = 3;
  string source_id = 4;
  string schema = 5;
  string table = 6;
  ExplorerHealthStatus status = 7;
  uint64 consumer_count = 8;
  uint64 dlq_count = 9;
  uint64 first_sequence = 10;
  uint64 last_sequence = 11;
  int64 latest_event_timestamp = 12;
  uint64 max_pending = 13;
  uint64 max_ack_pending = 14;
}

message PartitionSummary {
  string id = 1;
  uint64 message_count = 2;
  string topic = 3;
  ExplorerHealthStatus status = 4;
  uint64 first_sequence = 5;
  uint64 last_sequence = 6;
  int64 latest_event_timestamp = 7;
  uint64 consumer_count = 8;
  uint64 max_pending = 9;
  uint64 max_ack_pending = 10;
  uint64 dlq_count = 11;
}
```

- [ ] **Step 6: Expand consumer messages**

Update `ConsumerSummary` and add detail request/response:

```proto
message ConsumerSummary {
  string name = 1;
  repeated string filter_subjects = 2;
  uint64 num_pending = 3;
  uint64 num_ack_pending = 4;
  uint64 delivered_stream_seq = 5;
  uint64 ack_floor_stream_seq = 6;
  string flow_id = 7;
  ExplorerHealthStatus status = 8;
  repeated string related_topics = 9;
  uint64 estimated_lag = 10;
  int64 last_active_timestamp = 11;
}

message ConsumerDetailRequest {
  string consumer_name = 1;
}

message ConsumerDetailResponse {
  ConsumerSummary summary = 1;
  repeated TopicSummary related_topics = 2;
  repeated CheckpointContext checkpoints = 3;
  repeated MessageItem recent_messages = 4;
}
```

- [ ] **Step 7: Add service RPCs**

Modify `proto/cdc/v1/service.proto` Explorer section:

```proto
rpc GetExplorerOverview(ExplorerOverviewRequest) returns (ExplorerOverviewResponse) {
  option (google.api.http) = { get: "/api/v1/explorer/overview" };
}

rpc GetTopicDetail(TopicDetailRequest) returns (TopicDetailResponse) {
  option (google.api.http) = { get: "/api/v1/topics/{topic}" };
}

rpc GetPartitionDetail(PartitionDetailRequest) returns (PartitionDetailResponse) {
  option (google.api.http) = {
    get: "/api/v1/topics/{topic}/partitions/{partition}"
  };
}

rpc GetMessageDetail(MessageDetailRequest) returns (MessageDetailResponse) {
  option (google.api.http) = {
    get: "/api/v1/topics/{topic}/partitions/{partition}/messages/{sequence}"
  };
}

rpc GetConsumerDetail(ConsumerDetailRequest) returns (ConsumerDetailResponse) {
  option (google.api.http) = { get: "/api/v1/consumers/{consumer_name}" };
}
```

- [ ] **Step 8: Run proto lint**

Run: `make proto-lint`

Expected: lint passes. If Docker is unavailable, record the exact Docker error and continue only after the user decides whether to install/start Docker.

- [ ] **Step 9: Generate proto**

Run: `make gen-proto`

Expected: `api/proto/v1` generated files update and `go test ./api/proto/v1` compiles.

- [ ] **Step 10: Commit**

```bash
git add proto/cdc/v1/explorer.proto proto/cdc/v1/service.proto api/proto/v1
git commit -m "feat(explorer): expand explorer api contract"
```

---

## Task 2: Expand DLQ Proto Contract

**Files:**
- Modify: `proto/cdc/v1/dlq.proto`
- Modify: `proto/cdc/v1/service.proto`

- [ ] **Step 1: Add duplicate-risk enum and filter messages**

Append to `dlq.proto`:

```proto
enum DLQDuplicateRisk {
  DLQ_DUPLICATE_RISK_UNSPECIFIED = 0;
  DLQ_DUPLICATE_RISK_NONE = 1;
  DLQ_DUPLICATE_RISK_POSSIBLE = 2;
  DLQ_DUPLICATE_RISK_HIGH = 3;
  DLQ_DUPLICATE_RISK_BLOCKED = 4;
}

message DLQFilter {
  optional string original_topic = 1;
  optional string original_partition = 2;
  optional string source_id = 3;
  optional string schema = 4;
  optional string table = 5;
  optional string op = 6;
  optional string reason_contains = 7;
  optional string error_class = 8;
  optional uint64 retry_count_min = 9;
  optional uint64 retry_count_max = 10;
  optional uint64 delivery_count_min = 11;
  optional uint64 delivery_count_max = 12;
  optional int64 failed_from = 13;
  optional int64 failed_to = 14;
  optional string header_key = 15;
  optional string header_value = 16;
  optional string json_path = 17;
  optional string json_equals = 18;
  optional string text_contains = 19;
  DLQDuplicateRisk duplicate_risk = 20;
}
```

- [ ] **Step 2: Expand DLQMessage**

Replace `DLQMessage`:

```proto
message DLQMessage {
  uint64 sequence = 1;
  int64 timestamp = 2;
  string subject = 3;
  bytes data = 4;
  map<string, string> headers = 5;
  string reason = 6;
  string original_subject = 7;
  string dlq_id = 8;
  string flow_id = 9;
  string source_id = 10;
  string sink_id = 11;
  string schema = 12;
  string table = 13;
  string op = 14;
  string msg_id = 15;
  uint64 retry_count = 16;
  uint64 delivery_count = 17;
  string error_class = 18;
  int64 failed_at = 19;
  DLQDuplicateRisk duplicate_risk = 20;
  uint64 reprocessed_count = 21;
  int64 last_reprocessed_at = 22;
}
```

- [ ] **Step 3: Replace list and reprocess messages**

Replace list and reprocess messages:

```proto
message ListDLQMessagesRequest {
  OffsetPaginationRequest pagination = 1;
  DLQFilter filter = 2;
}

message ListDLQMessagesResponse {
  repeated DLQMessage data = 1;
  OffsetPaginationResponse pagination = 2;
  ExplorerScanMetadata scan = 3;
}

message DLQDryRunRequest {
  repeated string selected_dlq_ids = 1;
  DLQFilter filter = 2;
  uint32 max_count = 3;
}

message DLQDryRunPreviewItem {
  string dlq_id = 1;
  string original_subject = 2;
  string reason = 3;
  DLQDuplicateRisk duplicate_risk = 4;
  string deterministic_reprocess_id = 5;
  repeated string warnings = 6;
  repeated string blocking_findings = 7;
}

message DLQDryRunResponse {
  string confirm_token = 1;
  int64 expires_at = 2;
  uint64 matched_count = 3;
  repeated DLQDryRunPreviewItem preview_items = 4;
  map<string, uint64> risk_counts = 5;
  repeated string warnings = 6;
  repeated string blocking_findings = 7;
}

message ReprocessDLQRequest {
  repeated string selected_dlq_ids = 1;
  DLQFilter filter = 2;
  string confirm_token = 3;
  bool dry_run = 4;
  uint32 max_count = 5;
}

message ReprocessDLQResponse {
  int32 count = 1;
  uint64 succeeded_count = 2;
  uint64 skipped_count = 3;
  uint64 failed_count = 4;
  repeated string failures = 5;
  repeated string reprocessed_message_ids = 6;
}
```

- [ ] **Step 4: Add DLQ preview RPC**

Modify `service.proto` DLQ section:

```proto
rpc PreviewDLQReprocess(DLQDryRunRequest) returns (DLQDryRunResponse) {
  option (google.api.http) = {
    post: "/api/v1/dlq/reprocess/preview"
    body: "*"
  };
}
```

Keep existing `ReprocessDLQ` HTTP path, but its request message now has guard fields.

- [ ] **Step 5: Regenerate and compile**

Run:

```bash
make gen-proto
go test ./api/proto/v1
```

Expected: generated proto package compiles.

- [ ] **Step 6: Commit**

```bash
git add proto/cdc/v1/dlq.proto proto/cdc/v1/service.proto api/proto/v1
git commit -m "feat(dlq): add guarded reprocess api contract"
```

---

## Task 3: Add Explorer DTOs And Projection Helpers

**Files:**
- Modify: `internal/core/dto/request/explorer.go`
- Modify: `internal/core/dto/response/explorer.go`
- Create: `internal/core/service/explorer_projection.go`
- Create: `internal/core/service/explorer_projection_test.go`

- [ ] **Step 1: Write projection tests**

Create `internal/core/service/explorer_projection_test.go`:

```go
package service

import (
	"testing"

	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/ports"
)

func TestParseCDCSubject(t *testing.T) {
	parsed := ParseCDCSubject("cdc.src.public.orders.3")
	if parsed.Topic != "cdc.src.public.orders" || parsed.SourceID != "src" || parsed.Schema != "public" || parsed.Table != "orders" || parsed.Partition != "3" {
		t.Fatalf("unexpected parsed subject: %+v", parsed)
	}
}

func TestProjectMessageItemExtractsMetadata(t *testing.T) {
	item := &ports.NATSMessageItem{
		Sequence:  42,
		Timestamp: 1779966002000,
		Subject:   "cdc.src.public.orders.0",
		Headers: map[string]string{
			constant.HeaderOp:         "u",
			constant.HeaderInstanceID: "src",
			constant.HeaderSchema:     "public",
			constant.HeaderTable:      "orders",
			constant.HeaderPartition:  "0",
			"Nats-Msg-Id":             "msg-42",
		},
		Data: []byte(`{"op":"u","before":{"id":7,"status":"pending"},"after":{"id":7,"status":"paid"},"source":{"schema":"public","table":"orders","lsn":99}}`),
	}

	projected := ProjectMessageItem(item)
	if projected.Op != "u" || projected.SourceID != "src" || projected.Schema != "public" || projected.Table != "orders" || projected.Partition != "0" {
		t.Fatalf("metadata not projected: %+v", projected)
	}
	if projected.Key != "7" || projected.NATSMsgID != "msg-42" || projected.PayloadSize == 0 || projected.HeaderCount != 5 {
		t.Fatalf("message identity not projected: %+v", projected)
	}
}

func TestChangedFieldsShallow(t *testing.T) {
	fields := ChangedFields(
		[]byte(`{"id":1,"status":"pending","amount":"10"}`),
		[]byte(`{"id":1,"status":"paid","amount":"10"}`),
	)
	if len(fields) != 1 || fields[0] != "status" {
		t.Fatalf("changed fields = %+v, want [status]", fields)
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
go test ./internal/core/service -run 'Test(ParseCDCSubject|ProjectMessageItem|ChangedFields)' -v
```

Expected: build fails because `ParseCDCSubject`, `ProjectMessageItem`, and `ChangedFields` do not exist.

- [ ] **Step 3: Add DTO fields**

Modify `internal/core/dto/response/explorer.go` with:

```go
type ExplorerHealthStatus string

const (
	ExplorerHealthHealthy ExplorerHealthStatus = "healthy"
	ExplorerHealthIdle    ExplorerHealthStatus = "idle"
	ExplorerHealthLagging ExplorerHealthStatus = "lagging"
	ExplorerHealthStale   ExplorerHealthStatus = "stale"
	ExplorerHealthDLQ     ExplorerHealthStatus = "dlq"
)

type ScanMetadata struct {
	Partial      bool
	ScanLimitHit bool
	ScannedCount uint64
	MatchedCount uint64
	MaxScan      uint64
}

type ProjectedMessageItem struct {
	*ports.NATSMessageItem
	Op              string
	SourceID        string
	Schema          string
	Table           string
	Partition       string
	Key             string
	PayloadSize     uint64
	HeaderCount     uint32
	NATSMsgID       string
	ReprocessedFrom string
	Markers         []string
}

type TopicDetailResponse struct {
	Summary    TopicSummary
	Partitions []PartitionSummary
}

type PartitionDetailResponse struct {
	Summary     PartitionSummary
	Checkpoints []CheckpointContext
}

type CheckpointContext struct {
	FlowID                 string
	ConsumerName           string
	Topic                  string
	Partition              string
	AckFloorStreamSeq      uint64
	DeliveredStreamSeq     uint64
	NATSPending            uint64
	NATSAckPending         uint64
	SourceOffset           string
	StoredCheckpointKey    string
	StoredCheckpointOffset string
	UpdatedAt              int64
	ReplayRisk             string
}
```

Ensure existing response structs continue compiling by adding imports for `github.com/foden/cdc/internal/core/ports` if needed.

- [ ] **Step 4: Add request filters**

Modify `internal/core/dto/request/explorer.go`:

```go
type ListMessagesRequest struct {
	Status          domain.MessageStatus
	Topic           string
	Partition       string
	SourceID        string
	Schema          string
	Table           string
	Op              string
	SequenceMin     uint64
	SequenceMax     uint64
	TimestampFrom   int64
	TimestampTo     int64
	HeaderKey       string
	HeaderValue     string
	JSONPath        string
	JSONEquals      string
	TextContains    string
	ReprocessedOnly bool
	DLQRelatedOnly  bool
	Sort            string
	Page            int
	Limit           int
}

type TopicDetailRequest struct {
	Topic string
}

type PartitionDetailRequest struct {
	Topic     string
	Partition string
}

type MessageDetailRequest struct {
	Topic     string
	Partition string
	Sequence  uint64
}

type ConsumerDetailRequest struct {
	ConsumerName string
}
```

- [ ] **Step 5: Implement projection helpers**

Create `internal/core/service/explorer_projection.go`:

```go
package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/dto/response"
	"github.com/foden/cdc/internal/core/ports"
)

type CDCSubjectParts struct {
	Stream    string
	SourceID  string
	Schema    string
	Table     string
	Partition string
	Topic     string
}

func ParseCDCSubject(subject string) CDCSubjectParts {
	parts := strings.Split(subject, ".")
	result := CDCSubjectParts{Stream: firstPart(parts, 0), Topic: subject}
	if len(parts) >= 4 {
		result.SourceID = parts[1]
		result.Schema = parts[2]
		result.Table = parts[3]
		result.Topic = strings.Join(parts[:4], ".")
	}
	if len(parts) >= 5 {
		result.Partition = parts[4]
	}
	return result
}

func ProjectMessageItem(item *ports.NATSMessageItem) response.ProjectedMessageItem {
	if item == nil {
		return response.ProjectedMessageItem{}
	}
	parts := ParseCDCSubject(item.Subject)
	op := firstNonEmptyString(item.Headers[constant.HeaderOp], payloadString(item.Data, "op"))
	sourceID := firstNonEmptyString(item.Headers[constant.HeaderInstanceID], parts.SourceID)
	schema := firstNonEmptyString(item.Headers[constant.HeaderSchema], parts.Schema, payloadString(item.Data, "source.schema"))
	table := firstNonEmptyString(item.Headers[constant.HeaderTable], parts.Table, payloadString(item.Data, "source.table"))
	partition := firstNonEmptyString(item.Headers[constant.HeaderPartition], parts.Partition)
	key := firstNonEmptyString(payloadString(item.Data, "after.id"), payloadString(item.Data, "before.id"), item.Headers["Nats-Msg-Id"])
	markers := make([]string, 0, 2)
	if item.Headers["X-DLQ-Reprocessed-From"] != "" {
		markers = append(markers, "reprocessed")
	}
	if strings.HasPrefix(item.Subject, "dlq.") || item.Headers["X-DLQ-Reason"] != "" {
		markers = append(markers, "dlq")
	}
	return response.ProjectedMessageItem{
		NATSMessageItem: item,
		Op:              op,
		SourceID:        sourceID,
		Schema:          schema,
		Table:           table,
		Partition:       partition,
		Key:             key,
		PayloadSize:     uint64(len(item.Data)),
		HeaderCount:     uint32(len(item.Headers)),
		NATSMsgID:       item.Headers["Nats-Msg-Id"],
		ReprocessedFrom: item.Headers["X-DLQ-Reprocessed-From"],
		Markers:         markers,
	}
}

func ChangedFields(before []byte, after []byte) []string {
	var beforeMap map[string]any
	var afterMap map[string]any
	if err := json.Unmarshal(before, &beforeMap); err != nil {
		return nil
	}
	if err := json.Unmarshal(after, &afterMap); err != nil {
		return nil
	}
	seen := map[string]bool{}
	for key := range beforeMap {
		seen[key] = true
	}
	for key := range afterMap {
		seen[key] = true
	}
	fields := make([]string, 0, len(seen))
	for key := range seen {
		if !reflect.DeepEqual(beforeMap[key], afterMap[key]) {
			fields = append(fields, key)
		}
	}
	sort.Strings(fields)
	return fields
}

func ExtractBeforeAfter(payload []byte) ([]byte, []byte) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(payload, &root); err != nil {
		return nil, nil
	}
	return bytes.TrimSpace(root["before"]), bytes.TrimSpace(root["after"])
}

func payloadString(data []byte, path string) string {
	value, ok := payloadValue(data, path)
	if !ok || value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func payloadValue(data []byte, path string) (any, bool) {
	var root any
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, false
	}
	current := root
	for _, part := range strings.Split(path, ".") {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = obj[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func firstPart(parts []string, index int) string {
	if index < 0 || index >= len(parts) {
		return ""
	}
	return parts[index]
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
```

- [ ] **Step 6: Run projection tests**

Run:

```bash
go test ./internal/core/service -run 'Test(ParseCDCSubject|ProjectMessageItem|ChangedFields)' -v
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add internal/core/dto/request/explorer.go internal/core/dto/response/explorer.go internal/core/service/explorer_projection.go internal/core/service/explorer_projection_test.go
git commit -m "feat(explorer): add projection DTOs"
```

---

## Task 4: Wire Advanced Message Filters To NATS Client Port

**Files:**
- Modify: `internal/core/ports/nats.go`
- Modify: `internal/adapters/driven/nats/browser_filter.go`
- Modify: `internal/adapters/driven/nats/browser.go`
- Modify: `internal/core/service/explorer.go`
- Modify: `internal/adapters/driven/nats/browser_filter_test.go`

- [ ] **Step 1: Extend NATS client port**

Modify `internal/core/ports/nats.go`:

```go
type NATSMessageFilter struct {
	Topic           string
	Partition       string
	SubjectPrefix   string
	MinSequence     uint64
	MaxSequence     uint64
	FromTimestamp   int64
	ToTimestamp     int64
	HeaderKey       string
	HeaderValue     string
	TextContains    string
	JSONPath        string
	JSONEquals      string
	Op              string
	SourceID        string
	Schema          string
	Table           string
	ReprocessedOnly bool
	DLQRelatedOnly  bool
	Sort            string
}

type NATSListMessagesResult struct {
	Messages     []*NATSMessageItem
	Total        uint64
	ScannedCount uint64
	MatchedCount uint64
	MaxScan      uint64
	Partial      bool
	ScanLimitHit bool
}
```

Add to `NATSClient`:

```go
ListMessagesWithFilter(ctx context.Context, status domain.MessageStatus, limit int, page int, filter NATSMessageFilter) (NATSListMessagesResult, error)
```

- [ ] **Step 2: Update mock implementations**

Update test mocks in:

- `internal/core/flow/worker_test.go`
- `internal/core/flow/manager_prop_test.go`

Add method:

```go
func (n *workerTestNATS) ListMessagesWithFilter(context.Context, domain.MessageStatus, int, int, ports.NATSMessageFilter) (ports.NATSListMessagesResult, error) {
	return ports.NATSListMessagesResult{}, nil
}
```

Use the local mock receiver names in each file.

- [ ] **Step 3: Move filter type to ports at adapter boundary**

Change `internal/adapters/driven/nats/browser_filter.go`:

```go
type ExplorerMessageFilter = ports.NATSMessageFilter
```

Import `github.com/foden/cdc/internal/core/ports`. Keep existing methods on the alias in package `nats` by changing method receivers to use a local named type if Go rejects alias methods:

```go
type explorerMessageFilter ports.NATSMessageFilter

func normalizeFilter(filter ports.NATSMessageFilter) explorerMessageFilter {
	return explorerMessageFilter(filter)
}
```

Then update internal calls to `normalizeFilter(filter).Matches(item)`.

- [ ] **Step 4: Update `ListMessagesWithFilter` signature**

Modify `internal/adapters/driven/nats/browser.go`:

```go
func (c *Client) ListMessagesWithFilter(ctx context.Context, status domain.MessageStatus, limit int, page int, filter ports.NATSMessageFilter) (ports.NATSListMessagesResult, error)
```

Return:

```go
return ports.NATSListMessagesResult{
	Messages:     result,
	Total:        matched,
	ScannedCount: scanned,
	MatchedCount: matched,
	MaxScan:      uint64(fetchCount),
	Partial:      scanned >= uint64(fetchCount) && matched == uint64(len(result)),
	ScanLimitHit: scanned >= uint64(fetchCount),
}, nil
```

Track `scanned` inside the fetch loop.

- [ ] **Step 5: Wire service to advanced filter**

Modify `internal/core/service/explorer.go` `Messages`:

```go
result, err := s.natsClient.ListMessagesWithFilter(ctx, req.Status, normalizedLimit(req.Limit), normalizedPage(req.Page), ports.NATSMessageFilter{
	Topic:           req.Topic,
	Partition:       req.Partition,
	SourceID:        req.SourceID,
	Schema:          req.Schema,
	Table:           req.Table,
	Op:              req.Op,
	MinSequence:     req.SequenceMin,
	MaxSequence:     req.SequenceMax,
	FromTimestamp:   req.TimestampFrom,
	ToTimestamp:     req.TimestampTo,
	HeaderKey:       req.HeaderKey,
	HeaderValue:     req.HeaderValue,
	TextContains:    req.TextContains,
	JSONPath:        req.JSONPath,
	JSONEquals:      req.JSONEquals,
	ReprocessedOnly: req.ReprocessedOnly,
	DLQRelatedOnly:  req.DLQRelatedOnly,
	Sort:            req.Sort,
})
```

Return projected messages and scan metadata.

- [ ] **Step 6: Run unit tests**

Run:

```bash
go test ./internal/adapters/driven/nats -run 'TestExplorerMessageFilter|TestExplorerFilterSubject' -v
go test ./internal/core/service -run Test -v
go test ./internal/core/flow -run Test -v
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add internal/core/ports/nats.go internal/adapters/driven/nats/browser_filter.go internal/adapters/driven/nats/browser.go internal/core/service/explorer.go internal/adapters/driven/nats/browser_filter_test.go internal/core/flow/worker_test.go internal/core/flow/manager_prop_test.go
git commit -m "feat(explorer): wire advanced message filters"
```

---

## Task 5: Add Topic And Partition Backend Projections

**Files:**
- Create: `internal/adapters/driven/nats/browser_projection.go`
- Create: `internal/adapters/driven/nats/browser_projection_test.go`
- Modify: `internal/core/ports/nats.go`
- Modify: `internal/core/service/explorer.go`

- [ ] **Step 1: Add NATS projection result types**

Modify `internal/core/ports/nats.go`:

```go
type NATSTopicSummary struct {
	Name                 string
	SourceID             string
	Schema               string
	Table                string
	MessageCount          uint64
	PartitionCount        int32
	ConsumerCount         uint64
	DLQCount              uint64
	FirstSequence         uint64
	LastSequence          uint64
	LatestEventTimestamp  int64
	MaxPending            uint64
	MaxAckPending         uint64
}

type NATSPartitionSummary struct {
	ID                   string
	Topic                string
	MessageCount         uint64
	ConsumerCount        uint64
	DLQCount             uint64
	FirstSequence        uint64
	LastSequence         uint64
	LatestEventTimestamp int64
	MaxPending           uint64
	MaxAckPending        uint64
}
```

Add port methods:

```go
ListTopicSummaries(ctx context.Context, limit int, page int) ([]NATSTopicSummary, uint64, error)
GetTopicSummary(ctx context.Context, topic string) (NATSTopicSummary, error)
ListPartitionSummaries(ctx context.Context, topic string, limit int, page int) ([]NATSPartitionSummary, uint64, error)
GetPartitionSummary(ctx context.Context, topic string, partition string) (NATSPartitionSummary, error)
```

- [ ] **Step 2: Write projection tests**

Create `internal/adapters/driven/nats/browser_projection_test.go`:

```go
package nats

import "testing"

func TestSummarizeSubjectsGroupsTopicAndPartitions(t *testing.T) {
	messages := []*MessageItem{
		{Sequence: 10, Timestamp: 1000, Subject: "cdc.src.public.orders.0"},
		{Sequence: 11, Timestamp: 1100, Subject: "cdc.src.public.orders.1"},
		{Sequence: 12, Timestamp: 1200, Subject: "cdc.src.public.customers.0"},
	}

	topics := summarizeTopics(messages, nil, nil)
	if len(topics) != 2 {
		t.Fatalf("topics = %d, want 2", len(topics))
	}
	if topics[0].Name != "cdc.src.public.orders" || topics[0].PartitionCount != 2 || topics[0].MessageCount != 2 {
		t.Fatalf("orders summary incorrect: %+v", topics[0])
	}
}

func TestSummarizePartitionsTracksSequences(t *testing.T) {
	messages := []*MessageItem{
		{Sequence: 10, Timestamp: 1000, Subject: "cdc.src.public.orders.0"},
		{Sequence: 12, Timestamp: 1200, Subject: "cdc.src.public.orders.0"},
		{Sequence: 11, Timestamp: 1100, Subject: "cdc.src.public.orders.1"},
	}

	partitions := summarizePartitions("cdc.src.public.orders", messages, nil, nil)
	if len(partitions) != 2 {
		t.Fatalf("partitions = %d, want 2", len(partitions))
	}
	if partitions[0].ID != "0" || partitions[0].FirstSequence != 10 || partitions[0].LastSequence != 12 || partitions[0].LatestEventTimestamp != 1200 {
		t.Fatalf("partition summary incorrect: %+v", partitions[0])
	}
}
```

- [ ] **Step 3: Run tests to verify failure**

Run:

```bash
go test ./internal/adapters/driven/nats -run 'TestSummarize' -v
```

Expected: build fails because summary helpers do not exist.

- [ ] **Step 4: Implement summary helpers**

Create `internal/adapters/driven/nats/browser_projection.go` with helpers that:

- parse subject with `parseSubjectParts`.
- aggregate by topic and partition.
- compute first sequence, last sequence, latest timestamp, and message count.
- merge consumer and DLQ counts from maps.

Code skeleton:

```go
package nats

import (
	"sort"
	"strings"

	"github.com/foden/cdc/internal/core/ports"
)

type subjectParts struct {
	topic     string
	sourceID  string
	schema    string
	table     string
	partition string
}

func parseSubjectParts(subject string) subjectParts {
	parts := strings.Split(subject, ".")
	result := subjectParts{topic: subject}
	if len(parts) >= 4 {
		result.sourceID = parts[1]
		result.schema = parts[2]
		result.table = parts[3]
		result.topic = strings.Join(parts[:4], ".")
	}
	if len(parts) >= 5 {
		result.partition = parts[4]
	}
	return result
}
```

Implement `summarizeTopics` and `summarizePartitions` using maps and sort by name/id.

- [ ] **Step 5: Implement NATS methods**

In `browser_projection.go`, add:

```go
func (c *Client) ListTopicSummaries(ctx context.Context, limit int, page int) ([]ports.NATSTopicSummary, uint64, error)
func (c *Client) GetTopicSummary(ctx context.Context, topic string) (ports.NATSTopicSummary, error)
func (c *Client) ListPartitionSummaries(ctx context.Context, topic string, limit int, page int) ([]ports.NATSPartitionSummary, uint64, error)
func (c *Client) GetPartitionSummary(ctx context.Context, topic string, partition string) (ports.NATSPartitionSummary, error)
```

Use `listStreamMessages(ctx, c.streamName, "cdc.>", 500, 1)` for initial direct mode. Use topic-specific filter for partition summaries. Return capped/visible summaries.

- [ ] **Step 6: Wire ExplorerService topics/partitions to summary methods**

Update `internal/core/service/explorer.go`:

- `Topics` calls `ListTopicSummaries`.
- `Partitions` calls `ListPartitionSummaries`.
- Add `TopicDetail(ctx, req)` and `PartitionDetail(ctx, req)`.

- [ ] **Step 7: Run tests**

Run:

```bash
go test ./internal/adapters/driven/nats -run 'TestSummarize' -v
go test ./internal/core/service -run Test -v
```

Expected: pass.

- [ ] **Step 8: Commit**

```bash
git add internal/core/ports/nats.go internal/adapters/driven/nats/browser_projection.go internal/adapters/driven/nats/browser_projection_test.go internal/core/service/explorer.go
git commit -m "feat(explorer): add topic partition projections"
```

---

## Task 6: Add Explorer Overview Service

**Files:**
- Modify: `internal/core/dto/response/explorer.go`
- Modify: `internal/core/service/explorer.go`
- Modify: `internal/adapters/driver/grpc/handler_explorer.go`

- [ ] **Step 1: Add response DTO**

Add to `response/explorer.go`:

```go
type ExplorerOverviewResponse struct {
	TopicCount       uint64
	PartitionCount   uint64
	ConsumerCount    uint64
	PendingCount     uint64
	AckPendingCount  uint64
	DLQDepth         uint64
	AttentionTopics  []TopicSummary
	RecentDLQ        []DLQMessageSummary
}

type DLQMessageSummary struct {
	Sequence        uint64
	Timestamp       int64
	OriginalSubject string
	Reason          string
	ErrorClass      string
}
```

- [ ] **Step 2: Implement overview method**

Add to `ExplorerService`:

```go
func (s *ExplorerService) Overview(ctx context.Context) (response.ExplorerOverviewResponse, error) {
	if s.natsClient == nil {
		return response.ExplorerOverviewResponse{}, nil
	}
	topics, _, err := s.natsClient.ListTopicSummaries(ctx, 500, 1)
	if err != nil {
		return response.ExplorerOverviewResponse{}, err
	}
	consumers, _, err := s.natsClient.ListConsumers(ctx, 500, 1)
	if err != nil {
		return response.ExplorerOverviewResponse{}, err
	}
	var pending uint64
	var ackPending uint64
	for _, consumer := range consumers {
		pending += consumer.NumPending
		ackPending += consumer.NumAckPending
	}
	var partitionCount uint64
	for _, topic := range topics {
		partitionCount += uint64(topic.PartitionCount)
	}
	return response.ExplorerOverviewResponse{
		TopicCount:      uint64(len(topics)),
		PartitionCount:  partitionCount,
		ConsumerCount:   uint64(len(consumers)),
		PendingCount:    pending,
		AckPendingCount: ackPending,
		AttentionTopics: topTopicsNeedingAttention(topics, 10),
	}, nil
}
```

Implement `topTopicsNeedingAttention` sorting by DLQ count, max pending, latest event timestamp.

- [ ] **Step 3: Add gRPC handler**

Add to `handler_explorer.go`:

```go
func (s *CDCService) GetExplorerOverview(ctx context.Context, _ *cdcpb.ExplorerOverviewRequest) (*cdcpb.ExplorerOverviewResponse, error) {
	result, err := s.explorerService.Overview(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get explorer overview: %v", err)
	}
	return explorerOverviewToProto(result), nil
}
```

Create mapping helper `explorerOverviewToProto`.

- [ ] **Step 4: Run compile tests**

Run:

```bash
go test ./internal/core/service ./internal/adapters/driver/grpc
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add internal/core/dto/response/explorer.go internal/core/service/explorer.go internal/adapters/driver/grpc/handler_explorer.go
git commit -m "feat(explorer): add overview service"
```

---

## Task 7: Add Partition Message Detail And Checkpoint Context

**Files:**
- Modify: `internal/core/service/explorer.go`
- Modify: `internal/core/dto/response/explorer.go`
- Modify: `internal/adapters/driver/grpc/handler_explorer.go`

- [ ] **Step 1: Add unit tests for before/after detail**

Extend `explorer_projection_test.go`:

```go
func TestExtractBeforeAfterAndChangedFields(t *testing.T) {
	payload := []byte(`{"before":{"id":1,"status":"pending"},"after":{"id":1,"status":"paid"}}`)
	before, after := ExtractBeforeAfter(payload)
	fields := ChangedFields(before, after)
	if string(before) != `{"id":1,"status":"pending"}` {
		t.Fatalf("before = %s", before)
	}
	if string(after) != `{"id":1,"status":"paid"}` {
		t.Fatalf("after = %s", after)
	}
	if len(fields) != 1 || fields[0] != "status" {
		t.Fatalf("changed fields = %+v", fields)
	}
}
```

- [ ] **Step 2: Add service method**

Add to `ExplorerService`:

```go
func (s *ExplorerService) MessageDetail(ctx context.Context, req request.MessageDetailRequest) (response.MessageDetailResponse, error) {
	messages, err := s.natsClient.ListMessagesWithFilter(ctx, domain.MessageStatusAll, 1, 1, ports.NATSMessageFilter{
		Topic:       req.Topic,
		Partition:   req.Partition,
		MinSequence: req.Sequence,
		MaxSequence: req.Sequence,
	})
	if err != nil {
		return response.MessageDetailResponse{}, err
	}
	if len(messages.Messages) == 0 {
		return response.MessageDetailResponse{}, ErrNotFound
	}
	item := ProjectMessageItem(messages.Messages[0])
	before, after := ExtractBeforeAfter(item.Data)
	return response.MessageDetailResponse{
		Item:           item,
		DecodedPayload: string(item.Data),
		BeforeJSON:     string(before),
		AfterJSON:      string(after),
		ChangedFields:  ChangedFields(before, after),
		SourceMetadata: sourceMetadata(item.Data),
		RoutingMetadata: routingMetadata(item),
	}, nil
}
```

If `ErrNotFound` does not exist in `service`, use `fmt.Errorf("message not found")` and map it to `codes.NotFound` in the handler by matching the message.

- [ ] **Step 3: Add checkpoint context method**

Add:

```go
func (s *ExplorerService) CheckpointsForPartition(ctx context.Context, topic string, partition string) ([]response.CheckpointContext, error)
```

Initial implementation uses `ListConsumers` and includes consumers whose filter subject matches the topic. Set `ReplayRisk`:

```go
func replayRisk(ackFloor uint64, delivered uint64, pending uint64) string {
	if pending > 0 {
		return "medium"
	}
	if delivered > 0 && ackFloor == 0 {
		return "high"
	}
	return "low"
}
```

- [ ] **Step 4: Wire partition detail to checkpoints**

`PartitionDetail` should call `GetPartitionSummary` and `CheckpointsForPartition`.

- [ ] **Step 5: Add gRPC handlers**

Add handlers for:

- `GetPartitionDetail`
- `GetMessageDetail`
- `GetConsumerDetail`

Map DTOs to proto with explicit helper functions.

- [ ] **Step 6: Run tests**

Run:

```bash
go test ./internal/core/service ./internal/adapters/driver/grpc
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add internal/core/service/explorer.go internal/core/service/explorer_projection.go internal/core/service/explorer_projection_test.go internal/core/dto/response/explorer.go internal/adapters/driver/grpc/handler_explorer.go
git commit -m "feat(explorer): add partition detail and message detail"
```

---

## Task 8: Add DLQ Guard Service

**Files:**
- Modify: `internal/core/dto/request/dlq.go`
- Modify: `internal/core/dto/response/dlq.go`
- Create: `internal/core/service/dlq_reprocess_guard.go`
- Create: `internal/core/service/dlq_reprocess_guard_test.go`
- Modify: `internal/core/service/dlq.go`

- [ ] **Step 1: Add guard tests**

Create `internal/core/service/dlq_reprocess_guard_test.go`:

```go
package service

import (
	"testing"
	"time"
)

func TestDLQConfirmTokenRoundTrip(t *testing.T) {
	guard := NewDLQReprocessGuard([]byte("test-secret"), time.Minute)
	token, err := guard.Issue(DLQReprocessPlan{
		SelectedIDs: []string{"dlq-1", "dlq-2"},
		Count:       2,
		FilterHash:  "abc",
		Now:         1000,
	})
	if err != nil {
		t.Fatalf("Issue failed: %v", err)
	}
	plan, err := guard.Verify(token, 1000)
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}
	if plan.Count != 2 || plan.FilterHash != "abc" {
		t.Fatalf("verified plan mismatch: %+v", plan)
	}
}

func TestDLQConfirmTokenExpires(t *testing.T) {
	guard := NewDLQReprocessGuard([]byte("test-secret"), time.Second)
	token, err := guard.Issue(DLQReprocessPlan{SelectedIDs: []string{"dlq-1"}, Count: 1, FilterHash: "abc", Now: 1000})
	if err != nil {
		t.Fatalf("Issue failed: %v", err)
	}
	if _, err := guard.Verify(token, 3000); err == nil {
		t.Fatalf("expired token verified successfully")
	}
}
```

- [ ] **Step 2: Implement guard**

Create `internal/core/service/dlq_reprocess_guard.go`:

```go
package service

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

type DLQReprocessPlan struct {
	SelectedIDs []string `json:"selected_ids"`
	Count       uint64   `json:"count"`
	FilterHash  string   `json:"filter_hash"`
	Now         int64    `json:"now"`
	ExpiresAt   int64    `json:"expires_at"`
}

type DLQReprocessGuard struct {
	secret []byte
	ttl    time.Duration
}

func NewDLQReprocessGuard(secret []byte, ttl time.Duration) *DLQReprocessGuard {
	if len(secret) == 0 {
		secret = []byte("cdc-dlq-reprocess-guard")
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &DLQReprocessGuard{secret: secret, ttl: ttl}
}

func (g *DLQReprocessGuard) Issue(plan DLQReprocessPlan) (string, error) {
	slices.Sort(plan.SelectedIDs)
	plan.ExpiresAt = plan.Now + g.ttl.Milliseconds()
	payload, err := json.Marshal(plan)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, g.secret)
	mac.Write(payload)
	sig := mac.Sum(nil)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func (g *DLQReprocessGuard) Verify(token string, now int64) (DLQReprocessPlan, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return DLQReprocessPlan{}, fmt.Errorf("invalid confirm token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return DLQReprocessPlan{}, fmt.Errorf("invalid confirm token payload: %w", err)
	}
	gotSig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return DLQReprocessPlan{}, fmt.Errorf("invalid confirm token signature: %w", err)
	}
	mac := hmac.New(sha256.New, g.secret)
	mac.Write(payload)
	if !hmac.Equal(gotSig, mac.Sum(nil)) {
		return DLQReprocessPlan{}, fmt.Errorf("invalid confirm token signature")
	}
	var plan DLQReprocessPlan
	if err := json.Unmarshal(payload, &plan); err != nil {
		return DLQReprocessPlan{}, err
	}
	if now > plan.ExpiresAt {
		return DLQReprocessPlan{}, fmt.Errorf("confirm token expired")
	}
	return plan, nil
}
```

- [ ] **Step 3: Add DLQ request/response DTOs**

Modify `request/dlq.go`:

```go
type DLQFilter struct {
	OriginalTopic     string
	OriginalPartition string
	SourceID          string
	Schema            string
	Table             string
	Op                string
	ReasonContains    string
	ErrorClass        string
	HeaderKey         string
	HeaderValue       string
	JSONPath          string
	JSONEquals        string
	TextContains      string
}

type DLQDryRunRequest struct {
	SelectedDLQIDs []string
	Filter         DLQFilter
	MaxCount       uint32
}

type ReprocessDLQRequest struct {
	SelectedDLQIDs []string
	Filter         DLQFilter
	ConfirmToken   string
	DryRun         bool
	MaxCount       uint32
}
```

Modify `response/dlq.go`:

```go
type DLQDuplicateRisk string

const (
	DLQDuplicateRiskNone     DLQDuplicateRisk = "none"
	DLQDuplicateRiskPossible DLQDuplicateRisk = "possible"
	DLQDuplicateRiskHigh     DLQDuplicateRisk = "high"
	DLQDuplicateRiskBlocked  DLQDuplicateRisk = "blocked"
)

type DLQDryRunPreviewItem struct {
	DLQID             string
	OriginalSubject   string
	Reason            string
	DuplicateRisk     DLQDuplicateRisk
	BlockedReason     string
	ReplayTarget      string
	MessageSequence   uint64
	MessageTimestamp  time.Time
}

type DLQDryRunResponse struct {
	SelectedCount uint32
	PreviewCount  uint32
	BlockedCount  uint32
	PreviewItems  []DLQDryRunPreviewItem
	ConfirmToken  string
	Warnings      []string
}

type ReprocessDLQResponse struct {
	Count              int32
	ReprocessedDLQIDs  []string
	SkippedDLQIDs      []string
	FailedDLQIDs       []string
	DryRun             bool
}
```

Add `time` to the import list.

- [ ] **Step 4: Add service methods**

Modify `DLQService`:

```go
func (s *DLQService) PreviewReprocess(ctx context.Context, req request.DLQDryRunRequest) (response.DLQDryRunResponse, error)
func (s *DLQService) Reprocess(ctx context.Context, req request.ReprocessDLQRequest) (response.ReprocessDLQResponse, error)
```

Keep old behavior compatible: if `ConfirmToken == ""`, `SelectedDLQIDs` empty, and no filter is set, return an error requiring preview rather than reprocessing all.

- [ ] **Step 5: Run tests**

Run:

```bash
go test ./internal/core/service -run 'TestDLQConfirm|TestDLQ' -v
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add internal/core/dto/request/dlq.go internal/core/dto/response/dlq.go internal/core/service/dlq.go internal/core/service/dlq_reprocess_guard.go internal/core/service/dlq_reprocess_guard_test.go
git commit -m "feat(dlq): add reprocess guard service"
```

---

## Task 9: Add DLQ Adapter Support For Selected Reprocess

**Files:**
- Modify: `internal/core/ports/nats.go`
- Modify: `internal/adapters/driven/nats/dlq.go`
- Modify: `internal/adapters/driven/nats/dlq_reprocess_test.go`
- Modify: `tests/integration/dlq_recovery_test.go`

- [ ] **Step 1: Extend port**

Add to `NATSClient`:

```go
PreviewDLQ(ctx context.Context, ids []string, filter DLQFilter, maxCount uint32) ([]DLQPreviewItem, error)
ReprocessDLQSelected(ctx context.Context, ids []string, filter DLQFilter, maxCount uint32) (DLQReprocessResult, error)
```

Add supporting port types:

```go
type DLQFilter struct {
	OriginalTopic     string
	OriginalPartition string
	SourceID          string
	Schema            string
	Table             string
	Op                string
	ReasonContains    string
	ErrorClass        string
}

type DLQPreviewItem struct {
	DLQID                    string
	OriginalSubject          string
	Reason                   string
	DuplicateRisk            string
	DeterministicReprocessID string
	Warnings                 []string
	BlockingFindings         []string
}

type DLQReprocessResult struct {
	SucceededCount        uint64
	SkippedCount          uint64
	FailedCount           uint64
	Failures              []string
	ReprocessedMessageIDs []string
}
```

- [ ] **Step 2: Add unit test for selected match**

Extend `dlq_reprocess_test.go`:

```go
func TestDLQEnvelopeMatchesSelectedID(t *testing.T) {
	env := DLQEnvelope{ID: "dlq-1", OriginalSubject: "cdc.src.public.orders.0", Reason: "sink_error"}
	if !dlqEnvelopeMatches(env, map[string]bool{"dlq-1": true}, ports.DLQFilter{}) {
		t.Fatalf("selected envelope did not match")
	}
	if dlqEnvelopeMatches(env, map[string]bool{"dlq-2": true}, ports.DLQFilter{}) {
		t.Fatalf("unselected envelope matched")
	}
}
```

- [ ] **Step 3: Implement match helpers**

In `dlq.go`, add:

```go
func dlqEnvelopeMatches(env DLQEnvelope, selected map[string]bool, filter ports.DLQFilter) bool {
	if len(selected) > 0 && !selected[env.ID] {
		return false
	}
	if filter.OriginalTopic != "" && !strings.HasPrefix(env.OriginalSubject, filter.OriginalTopic) {
		return false
	}
	if filter.OriginalPartition != "" && !strings.HasSuffix(env.OriginalSubject, "."+filter.OriginalPartition) {
		return false
	}
	if filter.SourceID != "" && env.SourceID != filter.SourceID {
		return false
	}
	if filter.Schema != "" && env.Schema != filter.Schema {
		return false
	}
	if filter.Table != "" && env.Table != filter.Table {
		return false
	}
	if filter.Op != "" && env.Op != filter.Op {
		return false
	}
	if filter.ErrorClass != "" && env.ErrorClass != filter.ErrorClass {
		return false
	}
	if filter.ReasonContains != "" && !strings.Contains(strings.ToLower(env.Reason), strings.ToLower(filter.ReasonContains)) {
		return false
	}
	return true
}
```

- [ ] **Step 4: Implement preview and selected reprocess**

Add `PreviewDLQ` and `ReprocessDLQSelected` by reusing the existing DLQ stream consumer, but only acting on matched envelopes. `PreviewDLQ` must not publish or ack. `ReprocessDLQSelected` should publish matched envelopes and ack only successful matched DLQ messages.

- [ ] **Step 5: Preserve old `ReprocessDLQ` as compatibility wrapper**

Keep:

```go
func (c *Client) ReprocessDLQ(ctx context.Context) (int, error)
```

Implement it by calling selected reprocess with empty filter and max count 100 only if existing API compatibility requires it. The service should no longer call it for guarded reprocess.

- [ ] **Step 6: Run unit tests**

Run:

```bash
go test ./internal/adapters/driven/nats -run 'TestDLQ|TestBuildReprocess' -v
```

Expected: pass.

- [ ] **Step 7: Add integration tests**

Update `tests/integration/dlq_recovery_test.go`:

- convert `TestDLQDryRunDoesNotMutate` from skipped to real test.
- convert `TestDLQSelectedReprocess` from skipped to real test.

Assertions:

- dry-run returns preview and main stream count stays unchanged.
- selected reprocess republishes selected DLQ entries only.
- unselected DLQ messages are not acked.

- [ ] **Step 8: Run integration test**

Run:

```bash
make test-integration
```

Expected: integration package passes.

- [ ] **Step 9: Commit**

```bash
git add internal/core/ports/nats.go internal/adapters/driven/nats/dlq.go internal/adapters/driven/nats/dlq_reprocess_test.go tests/integration/dlq_recovery_test.go
git commit -m "feat(dlq): support selected guarded reprocess"
```

---

## Task 10: Wire New gRPC/REST Handlers

**Files:**
- Modify: `internal/adapters/driver/grpc/handler_explorer.go`
- Modify: `internal/adapters/driver/grpc/grpc_service.go`

- [ ] **Step 1: Add mapping helpers**

In `handler_explorer.go`, add helpers:

```go
func explorerHealthToProto(status response.ExplorerHealthStatus) cdcpb.ExplorerHealthStatus
func scanMetadataToProto(scan response.ScanMetadata) *cdcpb.ExplorerScanMetadata
func projectedMessagesToProto(messages []response.ProjectedMessageItem) []*cdcpb.MessageItem
func topicSummaryToProto(topic response.TopicSummary) *cdcpb.TopicSummary
func partitionSummaryToProto(partition response.PartitionSummary) *cdcpb.PartitionSummary
func checkpointContextToProto(ctx response.CheckpointContext) *cdcpb.CheckpointContext
```

- [ ] **Step 2: Update `ListMessages` request mapping**

Map all new proto fields into `request.ListMessagesRequest`.

- [ ] **Step 3: Add handlers**

Add:

```go
func (s *CDCService) GetExplorerOverview(ctx context.Context, req *cdcpb.ExplorerOverviewRequest) (*cdcpb.ExplorerOverviewResponse, error)
func (s *CDCService) GetTopicDetail(ctx context.Context, req *cdcpb.TopicDetailRequest) (*cdcpb.TopicDetailResponse, error)
func (s *CDCService) GetPartitionDetail(ctx context.Context, req *cdcpb.PartitionDetailRequest) (*cdcpb.PartitionDetailResponse, error)
func (s *CDCService) GetMessageDetail(ctx context.Context, req *cdcpb.MessageDetailRequest) (*cdcpb.MessageDetailResponse, error)
func (s *CDCService) GetConsumerDetail(ctx context.Context, req *cdcpb.ConsumerDetailRequest) (*cdcpb.ConsumerDetailResponse, error)
func (s *CDCService) PreviewDLQReprocess(ctx context.Context, req *cdcpb.DLQDryRunRequest) (*cdcpb.DLQDryRunResponse, error)
```

- [ ] **Step 4: Update `ReprocessDLQ`**

Map selected IDs, filter, confirm token, dry_run, and max_count into service request.

- [ ] **Step 5: Run compile tests**

Run:

```bash
go test ./internal/adapters/driver/grpc
go test ./...
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add internal/adapters/driver/grpc/handler_explorer.go internal/adapters/driver/grpc/grpc_service.go
git commit -m "feat(explorer): wire explorer grpc handlers"
```

---

## Task 11: Frontend Routes And API Types

**Files:**
- Modify: `website/src/config/routes.ts`
- Modify: `website/src/lib/api/endpoints.ts`
- Modify: `website/src/types/api.ts`
- Modify: `website/src/lib/query/explorer.ts`
- Modify: `website/src/App.tsx`

- [ ] **Step 1: Update route constants**

Modify `routes.ts`:

```ts
EXPLORER: '/explorer',
EXPLORER_TOPICS: '/explorer/topics',
EXPLORER_TOPIC_DETAIL: '/explorer/topics/:topic',
EXPLORER_TOPIC_PARTITION: '/explorer/topics/:topic/partitions/:partition',
EXPLORER_CONSUMERS: '/explorer/consumers',
EXPLORER_CONSUMER_DETAIL: '/explorer/consumers/:consumer',
EXPLORER_DLQ: '/explorer/dlq',
```

Do not add `EXPLORER_MESSAGES` to sidebar navigation.

- [ ] **Step 2: Add endpoints**

Modify `endpoints.ts`:

```ts
explorerOverview: '/api/v1/explorer/overview',
topicDetail: (topic: string) => `/api/v1/topics/${encodeURIComponent(topic)}` as const,
partitionDetail: (topic: string, partition: string) =>
  `/api/v1/topics/${encodeURIComponent(topic)}/partitions/${encodeURIComponent(partition)}` as const,
partitionMessages: (topic: string, partition: string) =>
  `/api/v1/topics/${encodeURIComponent(topic)}/partitions/${encodeURIComponent(partition)}/messages` as const,
messageDetail: (topic: string, partition: string, sequence: number | string) =>
  `/api/v1/topics/${encodeURIComponent(topic)}/partitions/${encodeURIComponent(partition)}/messages/${sequence}` as const,
consumerDetail: (consumer: string) => `/api/v1/consumers/${encodeURIComponent(consumer)}` as const,
dlqPreview: '/api/v1/dlq/reprocess/preview',
```

- [ ] **Step 3: Expand TypeScript API types**

Replace the Explorer and DLQ type blocks in `website/src/types/api.ts` with:

```ts
export type ExplorerHealthStatus = 'healthy' | 'idle' | 'lagging' | 'stale' | 'dlq';
export type ExplorerSort = 'newest' | 'oldest';
export type DLQDuplicateRisk = 'none' | 'possible' | 'high' | 'blocked';

export interface ExplorerScanMetadata {
  partial: boolean;
  scan_limit_hit: boolean;
  scanned_count: number;
  matched_count: number;
  max_scan: number;
}

export interface CheckpointContext {
  consumer_name: string;
  delivered_stream_seq: number;
  ack_floor_stream_seq: number;
  num_pending: number;
  num_ack_pending: number;
  lag_messages: number;
  last_delivered_at?: string | number;
  last_ack_at?: string | number;
}

export interface TopicSummary {
  name: string;
  stream_name?: string;
  partition_count: number;
  message_count: number;
  consumer_count: number;
  dlq_count: number;
  pending_count: number;
  ack_pending_count: number;
  first_sequence: number;
  latest_sequence: number;
  latest_event_at?: string | number;
  health: ExplorerHealthStatus;
  partial: boolean;
}

export interface PartitionSummary {
  id: string;
  topic: string;
  message_count: number;
  pending_count: number;
  ack_pending_count: number;
  first_sequence: number;
  latest_sequence: number;
  latest_event_at?: string | number;
  health: ExplorerHealthStatus;
  partial: boolean;
}

export interface MessageItem {
  sequence: number;
  timestamp: string | number;
  subject: string;
  topic?: string;
  partition?: string;
  source_id?: string;
  schema?: string;
  table?: string;
  op?: string;
  key?: string;
  data: string;
  headers: Record<string, string>;
  size_bytes?: number;
  changed_fields?: string[];
  is_dlq?: boolean;
  is_reprocessed?: boolean;
  reprocessed_from?: string;
  checkpoint?: CheckpointContext;
}

export interface ListMessagesResponse {
  data: MessageItem[];
  total_count: number;
  pagination?: OffsetPaginationResponse;
  scan?: ExplorerScanMetadata;
}

export interface ExplorerOverviewResponse {
  topic_count: number;
  partition_count: number;
  consumer_count: number;
  pending_count: number;
  ack_pending_count: number;
  dlq_depth: number;
  topics_needing_attention: TopicSummary[];
  recent_dlq: DLQMessageSummary[];
}

export interface TopicDetailResponse {
  summary: TopicSummary;
  partitions: PartitionSummary[];
  scan?: ExplorerScanMetadata;
}

export interface PartitionDetailResponse {
  summary: PartitionSummary;
  checkpoints: CheckpointContext[];
  scan?: ExplorerScanMetadata;
}

export interface MessageDetailResponse {
  item?: MessageItem;
  before?: Record<string, unknown>;
  after?: Record<string, unknown>;
  changed_fields?: string[];
  checkpoint?: CheckpointContext;
  scan?: ExplorerScanMetadata;
}

export interface ConsumerSummary {
  name: string;
  filter_subjects?: string[];
  num_pending: number;
  num_ack_pending: number;
  delivered_stream_seq: number;
  ack_floor_stream_seq: number;
  lag_messages: number;
  replay_risk?: 'low' | 'medium' | 'high';
  last_delivered_at?: string | number;
  last_ack_at?: string | number;
}

export interface ConsumerDetailResponse {
  summary: ConsumerSummary;
  topics: TopicSummary[];
  partitions: PartitionSummary[];
  recent_messages: MessageItem[];
  scan?: ExplorerScanMetadata;
}

export interface DLQMessageSummary {
  dlq_id: string;
  original_subject: string;
  reason: string;
  error_class?: string;
  duplicate_risk: DLQDuplicateRisk;
  timestamp?: string | number;
}

export interface DLQMessage extends MessageItem {
  dlq_id: string;
  reason?: string;
  original_subject?: string;
  error_class?: string;
  duplicate_risk?: DLQDuplicateRisk;
  blocked_reason?: string;
}

export interface ListDLQMessagesResponse {
  data: DLQMessage[];
  pagination?: OffsetPaginationResponse;
  scan?: ExplorerScanMetadata;
}

export interface ExplorerMessageFilters {
  status?: number;
  op?: string;
  sequence_min?: string;
  sequence_max?: string;
  timestamp_from?: string;
  timestamp_to?: string;
  header_key?: string;
  header_value?: string;
  json_path?: string;
  json_equals?: string;
  text_contains?: string;
  sort?: ExplorerSort;
  page?: number;
  limit?: number;
}

export interface DLQFilter {
  original_topic?: string;
  original_partition?: string;
  source_id?: string;
  schema?: string;
  table?: string;
  op?: string;
  reason_contains?: string;
  error_class?: string;
  header_key?: string;
  header_value?: string;
  json_path?: string;
  json_equals?: string;
  text_contains?: string;
}

export interface DLQDryRunRequest {
  selected_dlq_ids?: string[];
  filter?: DLQFilter;
  max_count?: number;
}

export interface DLQDryRunPreviewItem {
  dlq_id: string;
  original_subject: string;
  reason: string;
  duplicate_risk: DLQDuplicateRisk;
  blocked_reason?: string;
  replay_target?: string;
  message_sequence: number;
  message_timestamp?: string | number;
}

export interface DLQDryRunResponse {
  selected_count: number;
  preview_count: number;
  blocked_count: number;
  preview_items: DLQDryRunPreviewItem[];
  confirm_token: string;
  warnings: string[];
}

export interface ReprocessDLQRequest {
  selected_dlq_ids?: string[];
  filter?: DLQFilter;
  confirm_token: string;
  dry_run?: boolean;
  max_count?: number;
}

export interface ReprocessDLQResponse {
  count: number;
  reprocessed_dlq_ids?: string[];
  skipped_dlq_ids?: string[];
  failed_dlq_ids?: string[];
  dry_run?: boolean;
}
```

- [ ] **Step 4: Add query hooks**

Add these keys to `explorerKeys` in `query/explorer.ts`:

```ts
overview: () => ['explorer', 'overview'] as const,
topicDetail: (topic: string) => ['explorer', 'topic', topic] as const,
partitionDetail: (topic: string, partition: string) => ['explorer', 'partition', topic, partition] as const,
partitionMessages: (topic: string, partition: string, filters: ExplorerMessageFilters) =>
  ['explorer', 'partitionMessages', topic, partition, filters] as const,
consumerDetail: (consumer: string) => ['explorer', 'consumer', consumer] as const,
```

Add these hooks to `query/explorer.ts`:

```ts
export function useExplorerOverview() {
  return useQuery({
    queryKey: explorerKeys.overview(),
    queryFn: () => api.get<ExplorerOverviewResponse>(ENDPOINTS.explorerOverview),
    refetchInterval: POLLING.PARTITIONS,
  });
}

export function useTopicDetail(topic: string) {
  return useQuery({
    queryKey: explorerKeys.topicDetail(topic),
    queryFn: () => api.get<TopicDetailResponse>(ENDPOINTS.topicDetail(topic)),
    enabled: topic.length > 0,
    refetchInterval: POLLING.PARTITIONS,
  });
}

export function usePartitionDetail(topic: string, partition: string) {
  return useQuery({
    queryKey: explorerKeys.partitionDetail(topic, partition),
    queryFn: () => api.get<PartitionDetailResponse>(ENDPOINTS.partitionDetail(topic, partition)),
    enabled: topic.length > 0 && partition.length > 0,
    refetchInterval: POLLING.PARTITIONS,
  });
}

export function usePartitionMessages(topic: string, partition: string, filters: ExplorerMessageFilters) {
  return useQuery({
    queryKey: explorerKeys.partitionMessages(topic, partition, filters),
    queryFn: () =>
      api.get<ListMessagesResponse>(ENDPOINTS.partitionMessages(topic, partition), {
        status: filters.status,
        op: filters.op,
        sequence_min: filters.sequence_min,
        sequence_max: filters.sequence_max,
        timestamp_from: filters.timestamp_from,
        timestamp_to: filters.timestamp_to,
        header_key: filters.header_key,
        header_value: filters.header_value,
        json_path: filters.json_path,
        json_equals: filters.json_equals,
        text_contains: filters.text_contains,
        sort: filters.sort,
        'pagination.page': filters.page ?? 1,
        'pagination.limit': filters.limit ?? 50,
      }),
    enabled: topic.length > 0 && partition.length > 0,
    refetchInterval: POLLING.MESSAGES,
  });
}

export function useConsumerDetail(consumer: string) {
  return useQuery({
    queryKey: explorerKeys.consumerDetail(consumer),
    queryFn: () => api.get<ConsumerDetailResponse>(ENDPOINTS.consumerDetail(consumer)),
    enabled: consumer.length > 0,
    refetchInterval: POLLING.PARTITIONS,
  });
}

export function useDLQPreview() {
  return useMutation({
    mutationFn: (request: DLQDryRunRequest) =>
      api.post<DLQDryRunResponse>(ENDPOINTS.dlqPreview, request),
  });
}

export function useReprocessDLQ() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (request: ReprocessDLQRequest) =>
      api.post<ReprocessDLQResponse>(ENDPOINTS.dlqReprocess, request),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['messages'] });
      qc.invalidateQueries({ queryKey: ['dlqMessages'] });
      qc.invalidateQueries({ queryKey: ['explorer'] });
    },
  });
}
```

- [ ] **Step 5: Wire lazy routes**

Modify `App.tsx`:

```ts
const ExplorerOverviewPage = lazy(() => import('@/features/explorer/overview/page'));
const ExplorerPartitionDetailPage = lazy(() => import('@/features/explorer/partitions/detail'));
const ExplorerConsumerDetailPage = lazy(() => import('@/features/explorer/consumers/detail'));
```

Route `/explorer` should render overview, not redirect to topics.

- [ ] **Step 6: Run TypeScript build**

Run:

```bash
cd website && npm run build
```

Expected: build passes. If `npm` is unavailable, record that frontend build could not be verified.

- [ ] **Step 7: Commit**

```bash
git add website/src/config/routes.ts website/src/lib/api/endpoints.ts website/src/types/api.ts website/src/lib/query/explorer.ts website/src/App.tsx
git commit -m "feat(explorer): add frontend api routes"
```

---

## Task 12: Build Explorer Overview And Focused Topic Detail

**Files:**
- Create: `website/src/features/explorer/overview/page.tsx`
- Modify: `website/src/features/explorer/topics/page.tsx`
- Modify: `website/src/features/explorer/topics/detail.tsx`
- Modify: i18n locale files

- [ ] **Step 1: Create overview page**

Create `overview/page.tsx` with:

- metric strip: topics, partitions, consumers, pending, ack pending, DLQ depth.
- table "Topics needing attention".
- action buttons to open topic and highest-lag partition.

Use existing `Table`, `Button`, `Badge`, and `MetricCard` style patterns.

- [ ] **Step 2: Upgrade topics table**

Add columns:

- status
- consumer count
- DLQ count
- first sequence
- last sequence
- latest event time
- max pending

Keep actions:

- copy topic
- open topic detail
- open highest-lag partition when partition data is available.

- [ ] **Step 3: Simplify topic detail**

Remove links/cards for topic-level messages, consumers, and DLQ. Topic detail should show:

- summary metrics.
- partitions table.

Partition row click navigates to `/explorer/topics/:topic/partitions/:partition`.

- [ ] **Step 4: Add i18n strings**

Add keys under `explorer`:

```json
{
  "overview": "Overview",
  "topicsNeedingAttention": "Topics needing attention",
  "ackPending": "Ack pending",
  "maxPending": "Max pending",
  "firstSequence": "First sequence",
  "lastSequence": "Last sequence",
  "latestEvent": "Latest event",
  "openPartition": "Open partition"
}
```

Translate equivalent values in `vi.json` and `zh.json`.

- [ ] **Step 5: Run frontend build**

Run:

```bash
cd website && npm run build
```

Expected: build passes or npm availability is documented.

- [ ] **Step 6: Commit**

```bash
git add website/src/features/explorer/overview/page.tsx website/src/features/explorer/topics/page.tsx website/src/features/explorer/topics/detail.tsx website/src/lib/i18n/locales
git commit -m "feat(explorer): add overview and focused topics"
```

---

## Task 13: Build Partition Detail And Message Timeline

**Files:**
- Create: `website/src/features/explorer/partitions/detail.tsx`
- Create: `website/src/features/explorer/components/ExplorerFilterBar.tsx`
- Create: `website/src/features/explorer/components/PartitionMessageTimeline.tsx`
- Modify: `website/src/features/explorer/components/MessageDetailSheet.tsx`

- [ ] **Step 1: Create filter bar**

Create `ExplorerFilterBar.tsx`:

```tsx
import { X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import type { ExplorerMessageFilters } from '@/types/api';

const OP_OPTIONS = [
  { value: 'all', label: 'All ops' },
  { value: 'c', label: 'Create' },
  { value: 'u', label: 'Update' },
  { value: 'd', label: 'Delete' },
] as const;

interface ExplorerFilterBarProps {
  value: ExplorerMessageFilters;
  onChange: (value: ExplorerMessageFilters) => void;
}

export function ExplorerFilterBar({ value, onChange }: ExplorerFilterBarProps) {
  const patch = (next: Partial<ExplorerMessageFilters>) =>
    onChange({ ...value, ...next, page: 1 });
  const clear = () => onChange({ sort: 'newest', page: 1, limit: value.limit ?? 50 });

  return (
    <div className="grid gap-3 rounded-lg border border-border bg-card p-3 lg:grid-cols-6">
      <Select value={value.op ?? 'all'} onValueChange={(op) => patch({ op: op === 'all' ? undefined : op })}>
        <SelectTrigger className="w-full">
          <SelectValue placeholder="Operation" />
        </SelectTrigger>
        <SelectContent>
          {OP_OPTIONS.map((option) => (
            <SelectItem key={option.value} value={option.value}>
              {option.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      <Input
        value={value.sequence_min ?? ''}
        onChange={(event) => patch({ sequence_min: event.target.value || undefined })}
        placeholder="Seq min"
      />
      <Input
        value={value.sequence_max ?? ''}
        onChange={(event) => patch({ sequence_max: event.target.value || undefined })}
        placeholder="Seq max"
      />
      <Input
        value={value.text_contains ?? ''}
        onChange={(event) => patch({ text_contains: event.target.value || undefined })}
        placeholder="Payload contains"
      />
      <Select
        value={value.sort ?? 'newest'}
        onValueChange={(sort) => patch({ sort: sort as ExplorerMessageFilters['sort'] })}
      >
        <SelectTrigger className="w-full">
          <SelectValue placeholder="Sort" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="newest">Newest</SelectItem>
          <SelectItem value="oldest">Oldest</SelectItem>
        </SelectContent>
      </Select>
      <Button type="button" variant="outline" onClick={clear}>
        <X className="h-4 w-4" />
        Clear
      </Button>

      <Input
        value={value.timestamp_from ?? ''}
        onChange={(event) => patch({ timestamp_from: event.target.value || undefined })}
        placeholder="Timestamp from"
      />
      <Input
        value={value.timestamp_to ?? ''}
        onChange={(event) => patch({ timestamp_to: event.target.value || undefined })}
        placeholder="Timestamp to"
      />
      <Input
        value={value.header_key ?? ''}
        onChange={(event) => patch({ header_key: event.target.value || undefined })}
        placeholder="Header key"
      />
      <Input
        value={value.header_value ?? ''}
        onChange={(event) => patch({ header_value: event.target.value || undefined })}
        placeholder="Header value"
      />
      <Input
        value={value.json_path ?? ''}
        onChange={(event) => patch({ json_path: event.target.value || undefined })}
        placeholder="JSON path"
      />
      <Input
        value={value.json_equals ?? ''}
        onChange={(event) => patch({ json_equals: event.target.value || undefined })}
        placeholder="JSON equals"
      />
    </div>
  );
}
```

- [ ] **Step 2: Create message timeline table**

Create `PartitionMessageTimeline.tsx`:

```tsx
import { useMemo, useState } from 'react';
import { Badge } from '@/components/ui/badge';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { usePartitionMessages } from '@/lib/query/explorer';
import type { ExplorerMessageFilters, MessageItem } from '@/types/api';
import { formatBytes, formatTime, messageSize } from '../shared';
import { ExplorerFilterBar } from './ExplorerFilterBar';
import { MessageDetailSheet } from './MessageDetailSheet';

export function PartitionMessageTimeline({ topic, partition }: { topic: string; partition: string }) {
  const [filters, setFilters] = useState<ExplorerMessageFilters>({
    sort: 'newest',
    page: 1,
    limit: 50,
  });
  const [selectedMessage, setSelectedMessage] = useState<MessageItem | null>(null);
  const { data, isLoading } = usePartitionMessages(topic, partition, filters);
  const rows = useMemo(() => data?.data ?? [], [data]);

  return (
    <div className="space-y-3">
      <ExplorerFilterBar value={filters} onChange={setFilters} />
      <div className="overflow-hidden rounded-lg border border-border bg-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Time</TableHead>
              <TableHead>Op</TableHead>
              <TableHead className="text-right">Sequence</TableHead>
              <TableHead>Key/ID</TableHead>
              <TableHead className="text-right">Size</TableHead>
              <TableHead>Headers</TableHead>
              <TableHead>Markers</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              Array.from({ length: 8 }).map((_, index) => (
                <TableRow key={index}>
                  <TableCell colSpan={7}>
                    <div className="h-6 animate-pulse rounded bg-muted" />
                  </TableCell>
                </TableRow>
              ))
            ) : rows.length === 0 ? (
              <TableRow>
                <TableCell colSpan={7} className="h-36 text-center text-sm text-muted-foreground">
                  No messages match these filters.
                </TableCell>
              </TableRow>
            ) : (
              rows.map((message) => (
                <TableRow
                  key={`${message.subject}-${message.sequence}`}
                  className="cursor-pointer"
                  onClick={() => setSelectedMessage(message)}
                >
                  <TableCell className="whitespace-nowrap text-xs">{formatTime(message.timestamp)}</TableCell>
                  <TableCell>
                    <Badge variant="outline">{message.op || '-'}</Badge>
                  </TableCell>
                  <TableCell className="text-right font-mono text-xs">{message.sequence}</TableCell>
                  <TableCell className="max-w-[220px] truncate font-mono text-xs">
                    {message.key || message.headers?.['Nats-Msg-Id'] || '-'}
                  </TableCell>
                  <TableCell className="text-right text-xs">
                    {formatBytes(message.size_bytes ?? messageSize(message.data))}
                  </TableCell>
                  <TableCell className="text-xs">{Object.keys(message.headers ?? {}).length}</TableCell>
                  <TableCell>
                    <div className="flex flex-wrap gap-1">
                      {message.changed_fields?.length ? <Badge variant="secondary">diff</Badge> : null}
                      {message.is_reprocessed ? <Badge variant="secondary">reprocessed</Badge> : null}
                      {message.is_dlq ? <Badge variant="destructive">dlq</Badge> : null}
                    </div>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>
      <MessageDetailSheet message={selectedMessage} onOpenChange={(open) => !open && setSelectedMessage(null)} />
    </div>
  );
}
```

- [ ] **Step 3: Create partition detail page**

Page sections:

- header summary.
- `PartitionMessageTimeline`.
- Lag & Checkpoints table.

Use `usePartitionDetail(topic, partition)`.

- [ ] **Step 4: Expand message detail sheet**

Add tabs:

- Overview
- Before/After
- Payload
- Headers
- Source Metadata
- Routing
- Checkpoint Context
- Raw

Use existing `JsonViewer`.

- [ ] **Step 5: Run build**

Run:

```bash
cd website && npm run build
```

Expected: build passes or npm availability is documented.

- [ ] **Step 6: Commit**

```bash
git add website/src/features/explorer/partitions/detail.tsx website/src/features/explorer/components/ExplorerFilterBar.tsx website/src/features/explorer/components/PartitionMessageTimeline.tsx website/src/features/explorer/components/MessageDetailSheet.tsx
git commit -m "feat(explorer): add partition message timeline"
```

---

## Task 14: Build Consumer Detail

**Files:**
- Modify: `website/src/features/explorer/consumers/page.tsx`
- Create: `website/src/features/explorer/consumers/detail.tsx`

- [ ] **Step 1: Update consumers list**

Make consumer rows clickable. Add columns:

- flow id
- estimated lag
- related topic count
- last active time

- [ ] **Step 2: Create consumer detail page**

Sections:

- summary metrics.
- filter subjects.
- related topics table.
- checkpoint context table.
- recent messages table if backend returns data.

Actions:

- copy consumer name.
- open related topic.
- open related partition when available.

- [ ] **Step 3: Run build**

Run:

```bash
cd website && npm run build
```

Expected: build passes or npm availability is documented.

- [ ] **Step 4: Commit**

```bash
git add website/src/features/explorer/consumers/page.tsx website/src/features/explorer/consumers/detail.tsx
git commit -m "feat(explorer): add consumer detail"
```

---

## Task 15: Build Guarded DLQ Recovery UI

**Files:**
- Modify: `website/src/features/explorer/dlq/page.tsx`
- Create: `website/src/features/explorer/components/DLQDryRunDialog.tsx`
- Create: `website/src/features/explorer/components/ReprocessConfirmDialog.tsx`
- Modify: `website/src/lib/query/explorer.ts`

- [ ] **Step 1: Replace direct reprocess-all button**

Remove direct `reprocessAll` action. Add:

- selected row checkboxes.
- dry-run selected button.
- dry-run current filter button.

- [ ] **Step 2: Add DLQ filters**

Filters:

- original topic
- source/schema/table
- op
- reason contains
- error class
- failed from/to
- text contains

- [ ] **Step 3: Add dry-run dialog**

`DLQDryRunDialog` shows:

- matched count.
- risk counts.
- preview items.
- warnings.
- blocking findings.
- confirm token expiry.

If blocking findings exist, disable reprocess.

- [ ] **Step 4: Add confirm dialog**

`ReprocessConfirmDialog` requires explicit confirmation text:

```text
REPROCESS
```

It calls guarded `ReprocessDLQ` with selected IDs/filter and confirm token.

- [ ] **Step 5: Run build**

Run:

```bash
cd website && npm run build
```

Expected: build passes or npm availability is documented.

- [ ] **Step 6: Commit**

```bash
git add website/src/features/explorer/dlq/page.tsx website/src/features/explorer/components/DLQDryRunDialog.tsx website/src/features/explorer/components/ReprocessConfirmDialog.tsx website/src/lib/query/explorer.ts
git commit -m "feat(dlq): add guarded recovery ui"
```

---

## Task 16: Backend Integration Gates

**Files:**
- Modify: `tests/integration/explorer_messages_test.go`
- Modify: `tests/integration/explorer_consumers_test.go`
- Modify: `tests/integration/dlq_recovery_test.go`

- [ ] **Step 1: Add partition message filter integration test**

Add this case to `TestExplorerMessageSearch` in `tests/integration/explorer_messages_test.go`:

```go
{
	name: "partition op and json path",
	filter: natsadapter.ExplorerMessageFilter{
		Topic:      "cdc.src.public.orders",
		Partition:  "1",
		Op:         "u",
		JSONPath:   "after.status",
		JSONEquals: "pending",
	},
	wantTotal:   1,
	wantSubject: "cdc.src.public.orders.1",
},
```

The existing table loop calls `ListMessagesWithFilter`; keep that path so the test remains Docker-backed and does not require starting the HTTP gateway. Expected result: exactly one message, and its subject is `cdc.src.public.orders.1`.

- [ ] **Step 2: Add topic/partition summary integration test**

Seed three messages across two partitions. Assert:

- topic message count is 3.
- partition count is 2.
- partition 0 latest sequence is greater than first sequence.

- [ ] **Step 3: Add consumer detail integration test**

Create durable consumer, fetch one message, ack it, assert:

- consumer detail returns ack floor.
- pending is non-zero when messages remain.
- related topic includes `cdc.src.public.orders`.

- [ ] **Step 4: Add DLQ guarded integration tests**

Assert:

- preview returns confirm token and does not publish.
- selected reprocess publishes selected only.
- stale/invalid confirm token returns error.

- [ ] **Step 5: Run integration**

Run:

```bash
make test-integration
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add tests/integration/explorer_messages_test.go tests/integration/explorer_consumers_test.go tests/integration/dlq_recovery_test.go
git commit -m "test(explorer): add explorer integration gates"
```

---

## Task 17: Documentation And Quality Gates

**Files:**
- Modify: `docs/QUALITY_GATES.md`
- Create: `docs/EXPLORER_OPERATIONS.md`

- [ ] **Step 1: Add operator docs**

Create `docs/EXPLORER_OPERATIONS.md`:

```markdown
# Explorer Operations

## Primary Drilldown

Use `Explorer -> Topics -> Topic Detail -> Partition Detail`.

Topic detail shows the CDC table health and partitions. Partition detail shows ordered messages and Lag & Checkpoints.

## DLQ Recovery

Use dry-run before reprocess. Dry-run returns duplicate-risk findings and a confirm token. Reprocess selected messages only after reviewing the preview.

## Partial Search Results

In direct JetStream mode, payload filters run over a capped scan window. When the cap is hit, the response marks `partial=true` and `scan_limit_hit=true`.
```

- [ ] **Step 2: Update quality gates**

Add Explorer readiness criteria to `docs/QUALITY_GATES.md`:

- partition message timeline filters pass integration.
- topic/partition stats are non-zero and accurate for seeded data.
- DLQ dry-run mutates no messages.
- selected reprocess republishes only selected DLQ messages.

- [ ] **Step 3: Commit**

```bash
git add docs/QUALITY_GATES.md docs/EXPLORER_OPERATIONS.md
git commit -m "docs(explorer): add explorer operations guide"
```

---

## Task 18: Final Verification

**Files:**
- No code changes unless verification exposes failures.

- [ ] **Step 1: Run backend unit tests**

Run:

```bash
make test-unit
```

Expected: all Go packages pass.

- [ ] **Step 2: Run integration tests**

Run:

```bash
make test-integration
```

Expected: integration package passes.

- [ ] **Step 3: Run frontend build**

Run:

```bash
cd website && npm run build
```

Expected: build passes. If `npm` is unavailable in the shell, record the exact command failure.

- [ ] **Step 4: Run proto lint**

Run:

```bash
make proto-lint
```

Expected: proto lint passes.

- [ ] **Step 5: Run diff check**

Run:

```bash
git diff --check
git status --short
```

Expected: no whitespace errors; status shows only intentional changes.

- [ ] **Step 6: Final commit**

If earlier tasks were not committed individually:

```bash
git add proto api internal tests website docs
git commit -m "feat(explorer): build cdc operations explorer"
```

---

## Self-Review Checklist

- Spec coverage:
  - Overview: Tasks 1, 6, 12.
  - Topics and focused topic detail: Tasks 5, 12.
  - Partition detail with message timeline and Lag & Checkpoints: Tasks 4, 7, 13.
  - Consumer detail: Tasks 1, 7, 10, 14.
  - DLQ recovery: Tasks 2, 8, 9, 10, 15.
  - Backend integration gates: Task 16.
  - Documentation: Task 17.

- Type consistency:
  - Proto `MessageItem` maps to `response.ProjectedMessageItem`.
  - Proto `ExplorerScanMetadata` maps to `response.ScanMetadata`.
  - Proto `DLQDryRunRequest` maps to `request.DLQDryRunRequest`.
  - Proto `ReprocessDLQRequest` maps to `request.ReprocessDLQRequest`.

- Scope kept:
  - No client automated tests.
  - No Explorer metadata index in this pass.
  - No top-level Messages sidebar route in the main hierarchy.
