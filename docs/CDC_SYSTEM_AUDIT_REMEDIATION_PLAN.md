# CDC System Audit Remediation Plan

Tài liệu này diễn giải chi tiết các hạng mục trong `docs/CDC_SYSTEM_AUDIT_REPORT.md`: cần làm gì, làm như nào, vì sao cần làm, vì sao cách làm này hợp lý, rủi ro nếu không làm, và cách verify.

Mục tiêu không phải làm tất cả cùng lúc. Mục tiêu là có roadmap rõ ràng để sửa theo thứ tự an toàn: **correctness trước, build/runtime sau đó, performance/observability/reliability tiếp theo, cuối cùng mới refactor/product/docs/future work**.

---

## 1. Nguyên tắc ưu tiên

### P0: phải sửa trước khi tin hệ thống chạy đúng

P0 gồm delivery guarantee, checkpoint, Postgres replication correctness, transform/filter correctness, Docker/Makefile runtime. Các lỗi này có thể gây mất dữ liệu, duplicate khó kiểm soát, checkpoint sai, hoặc không build/chạy được.

**Vì sao ưu tiên P0:** CDC là hệ thống dữ liệu. Nếu delivery/checkpoint sai thì mọi tối ưu performance đều vô nghĩa vì hệ thống có thể báo đã xử lý trong khi sink chưa ghi xong.

### P1: sửa sau P0 để hệ thống ổn định hơn và scale được

P1 gồm correctness sâu hơn, lifecycle/shutdown, NATS subject/offset, sink correctness, performance chính, metrics/logs/tracing.

**Vì sao P1 sau P0:** Các lỗi này thường không phá ngay mọi flow, nhưng khi chạy production sẽ gây leak resource, lag, khó debug, hoặc sai dữ liệu ở edge cases.

### P2: reliability/config/testing/refactor để vận hành lâu dài

P2 gồm NATS capacity, DLQ reprocess, reconnect, type mapping, config validation, frontend DX, testing, architecture refactor.

**Vì sao P2:** Đây là lớp làm hệ thống bền, dễ vận hành, dễ mở rộng. Không nên làm trước P0/P1 vì dễ refactor trên nền correctness chưa chắc.

### P3: product/ops/docs/future capabilities

P3 gồm UI nâng cao, API product, reconciliation, HA, guardrails, connector extensibility.

**Vì sao P3:** Cần sau khi core correctness/reliability đã ổn. Làm sớm sẽ tốn công đổi lại khi core thay đổi.

---

## 2. P0 — Delivery, checkpoint, và guarantee

Audit items: #11, #12, #13, #14, #15

### Vấn đề

Hiện audit cảnh báo source offset/checkpoint có thể được lưu trước khi sink xử lý xong. Nếu điều này xảy ra, hệ thống có thể restart và bỏ qua event chưa thực sự ghi xuống sink.

Ngoài ra, nếu hệ thống đang claim `exactly-once` nhưng chưa có end-to-end exactly-once thật thì docs/marketing/API đang mô tả sai guarantee.

### Cần làm gì

1. Không save source offset trước khi sink write thành công.
2. Chuyển checkpoint sang mô hình **per-flow checkpoint** thay vì global source offset.
3. Chỉ checkpoint sau khi:
   - NATS message đã được fetch.
   - Transform/filter xử lý xong.
   - Sink `WriteBatch` thành công.
   - Message được ack thành công hoặc ack order được định nghĩa rõ.
4. NATS retention phải dựa trên **min checkpoint của tất cả flow đang consume stream đó**.
5. Bỏ claim `exactly-once` nếu chưa chứng minh được. Nên ghi rõ hiện tại là **at-least-once delivery + idempotent sink nếu sink/table hỗ trợ primary key/upsert**.

### Làm như nào

Thiết kế checkpoint key mới:

```text
checkpoint/<flow_id>/<source_id>/<schema>/<table>/<partition>
```

Nội dung checkpoint:

```json
{
  "flow_id": "...",
  "source_id": "...",
  "schema": "public",
  "table": "orders",
  "partition": 3,
  "nats_sequence": 123456,
  "source_lsn": "...",
  "source_offset": "...",
  "sink_instance_id": "...",
  "updated_at": "..."
}
```

Flow xử lý batch nên theo thứ tự:

```text
Fetch NATS messages
  -> parse event
  -> filter/mapping
  -> sink.WriteBatch(events)
  -> if success: Ack messages
  -> Save per-flow checkpoint
  -> update metrics
```

Nếu save checkpoint sau ack fail thì có risk duplicate hoặc reprocess. Cần quyết định rõ thứ tự. Khuyến nghị:

```text
sink success -> ack NATS -> save checkpoint
```

Vì NATS durable consumer vẫn là source-of-truth cho unacked messages. Checkpoint trong store dùng để observe/recover/control retention, không nên là cơ chế duy nhất quyết định replay.

### Vì sao nên làm vậy

CDC correctness phải dựa trên điểm đã durable ở sink hoặc ít nhất đã qua sink write thành công. Global source offset không đủ vì một source có thể cấp dữ liệu cho nhiều flow. Flow A đã xử lý xong không có nghĩa Flow B cũng xong.

### Rủi ro nếu không làm

- Mất dữ liệu sau restart.
- Một flow chậm bị bỏ qua vì global source offset đã tiến theo flow nhanh.
- NATS retention xóa message mà flow chậm chưa consume.
- Người dùng hiểu nhầm guarantee exactly-once.

### Verify

- Integration test: crash sau source publish nhưng trước sink write -> restart phải replay.
- Integration test: 2 flow consume cùng source, flow A nhanh flow B chậm -> retention không được xóa message của flow B.
- Unit test checkpoint key per flow/table/partition.
- Docs guarantee table phải ghi rõ at-least-once/idempotent semantics.

---

## 3. P0/P1 — Postgres source correctness

Audit items: #16, #17, #18, #19, #20, #33, #34, #44, #45, #122, #123, #124, #125, #129

### Vấn đề

Postgres logical replication có các điểm rất nhạy:

- WAL flush/apply LSN không được report vượt quá điểm đã durable checkpoint.
- `flushedLSN == 0` fallback về `clientLSN` có thể báo tiến độ sai.
- Update/delete có thể không có `OldTuple` nếu replica identity không đủ.
- Publication phải tồn tại và đúng table list trước `StartReplication`.
- Numeric/decimal không được parse sang float64 vì mất precision.
- Replication slot/connection error handling phải rõ.

### Cần làm gì

1. Tách rõ các loại LSN:
   - `received_lsn`: đã đọc từ WAL.
   - `published_lsn`: đã publish vào NATS thành công.
   - `flow_checkpoint_lsn`: flow đã sink thành công.
   - `reported_flush_lsn`: LSN báo về Postgres.
2. Không report flush/apply vượt quá durable boundary đã định nghĩa.
3. Nếu `flushedLSN == 0`, không fallback mù sang `clientLSN`. Chỉ report khi có checkpoint thật.
4. Nil-check `OldTuple` cho update/delete.
5. Validate replica identity trước start CDC:
   - Delete/update cần PK hoặc `REPLICA IDENTITY FULL` tùy use case.
6. Ensure publication tồn tại trước `StartReplication`.
7. Khi flow table list thay đổi, update publication.
8. Preserve `numeric/decimal` dạng string hoặc decimal type, không float64.
9. Slot creation chỉ ignore lỗi `already exists`; lỗi khác phải return.
10. Reconnect phải close old connection và cleanup nếu start fail giữa chừng.

### Làm như nào

#### LSN model

Tạo struct nội bộ:

```go
type PostgresReplicationProgress struct {
    ReceivedLSN   uint64
    PublishedLSN  uint64
    DurableLSN    uint64
    LastFlushSent uint64
}
```

Khi nhận WAL:

```text
read WAL -> decode -> publish NATS success -> update published_lsn
```

Khi flow sink xong:

```text
worker checkpoint -> checkpoint service updates durable_lsn per flow
```

Postgres source chỉ nên report flush theo min durable/published boundary đã an toàn. Nếu project chọn NATS là durable boundary, report flush sau NATS publish success là chấp nhận được cho source-level durability, nhưng docs phải ghi rõ sink-level delivery vẫn at-least-once.

#### Replica identity validation

Trước khi start source/table:

```sql
SELECT relreplident
FROM pg_class
WHERE oid = 'schema.table'::regclass;
```

Mapping:

```text
d = default; old tuple only includes key columns if PK exists
f = full; old tuple includes full row
i = index; custom replica identity index
n = nothing; update/delete old values unavailable
```

Nếu flow cần delete/update và table không đủ identity, fail fast với message rõ:

```text
Table public.orders does not have enough replica identity for UPDATE/DELETE CDC. Add primary key or set REPLICA IDENTITY FULL.
```

#### Numeric handling

Trong OID parser:

```text
numeric/decimal -> string
int -> int64
float4/float8 -> float64
json/jsonb -> raw JSON
bool -> bool
```

Không dùng `float64` cho `numeric` vì tiền/decimal sẽ mất precision.

### Vì sao nên làm vậy

Postgres WAL là log tuyến tính. Nếu báo flush sai, Postgres có thể recycle WAL mà downstream chưa durable. Replica identity sai sẽ làm delete/update thiếu key, sink không thể xóa/update đúng row. Numeric mất precision là data corruption âm thầm.

### Rủi ro nếu không làm

- WAL bị recycle trước khi downstream xử lý.
- Delete/update silently sai hoặc thiếu primary key.
- Decimal tiền tệ sai số.
- Publication thiếu table làm flow tưởng chạy nhưng không nhận event.
- Reconnect leak connection/slot state.

### Verify

- Test table không PK + delete/update -> fail rõ.
- Test `REPLICA IDENTITY FULL` -> old tuple đủ dữ liệu.
- Test numeric `1234567890.123456789` giữ nguyên precision.
- Test create slot error khác `already exists` -> return error.
- Integration restart source after reconnect -> no leaked old connection.

---

## 4. P0/P1 — Transform, mapping, filter correctness

Audit items: #21, #22, #23, #24, #107, #108, #109, #169, #170

### Vấn đề

Audit nói mapping đang apply vào envelope thay vì `after/before`. Với Debezium payload, dữ liệu row nằm trong:

```json
{
  "before": {...},
  "after": {...},
  "op": "u",
  "source": {...}
}
```

Nếu mapping/filter dùng sai path như `data.status` thay vì `after.status`, flow sẽ transform/filter sai hoặc silently drop data.

### Cần làm gì

1. Chuẩn hóa transform input:
   - create/update/snapshot: mapping apply vào `after`.
   - delete: mapping apply vào `before` hoặc tombstone model rõ ràng.
2. Filter engine expose variables rõ:
   - `before`
   - `after`
   - `op`
   - `source`
   - `schema`
   - `table`
3. Docs filter phải dùng ví dụ đúng:

```text
after.status == "paid"
op == "u"
source.db == "app"
```

4. Filter eval error không được silently drop data.
5. Tách metrics:
   - filtered count
   - filter error count
6. Bad filter nên:
   - fail create/update flow lúc validate nếu expression compile fail.
   - nếu runtime eval fail do payload bất thường: DLQ hoặc pause flow theo config.

### Làm như nào

Tạo transform context:

```go
type TransformContext struct {
    Before map[string]any
    After  map[string]any
    Op     constant.Op
    Source domain.SourceMetadata
    Schema string
    Table  string
}
```

Mapping function nhận context:

```go
ApplyColumnMappings(ctx TransformContext, mapping MappingConfig) (TransformResult, error)
```

Filter compile validate khi create flow:

```text
NewFilter(expression, allowedVariables)
```

Filter eval:

```text
result, err := filter.Evaluate(ctx)
if err != nil -> DLQ/pause, not ack-as-success
if !result -> ack as filtered + metric filtered_count
```

### Vì sao nên làm vậy

Transform/filter là logic business của người dùng. Sai path có thể làm mất dữ liệu hợp lệ hoặc ghi sai dữ liệu sang sink. Biến rõ ràng giúp UI/docs/API thống nhất, ít nhầm.

### Rủi ro nếu không làm

- Mapping không tác động row thật.
- Filter sai nhưng không ai biết.
- Data bị drop silently.
- Người dùng config theo docs nhưng runtime hiểu khác.

### Verify

- Unit test Debezium create/update/delete thật.
- Test `after.status == "paid"` pass đúng.
- Test bad expression fail lúc create flow.
- Test runtime filter error -> DLQ/pause + metric error.

---

## 5. P0 — Docker, Makefile, runtime path

Audit items: #25, #26, #27, #28, #29, #234, #245, #248, #249, #250, #251, #252, #253

### Vấn đề

Dockerfile/Makefile đang trỏ sai frontend path (`client/` thay vì `website/`), sai framework output (`.next` trong khi frontend là Vite), config path không tồn tại.

### Cần làm gì

1. Dockerfile build frontend từ `website/`.
2. Vì Vite output mặc định là `dist/`, không dùng `.next`.
3. Runtime serve static bằng nginx/caddy hoặc embed/static server rõ ràng.
4. Copy config từ path đúng:

```text
config/config.example.yaml
config/config.yaml
```

5. Makefile default:

```make
CONFIG_FILE ?= config/config.yaml
COMPOSE_FILE ?= deploy/docker-compose.yaml
```

6. Add healthcheck, non-root user, pin base image versions.

### Làm như nào

Docker multi-stage đề xuất:

```text
stage 1: build Go backend
stage 2: build Vite frontend from website/
stage 3: runtime minimal image
```

Nếu frontend serve riêng:

```text
nginx/caddy serves website/dist
backend container runs Go API
```

Nếu single container:

```text
Go binary serves embedded/static dist
```

Khuyến nghị clean hơn: **separate frontend static container + backend container** trong compose dev/prod.

### Vì sao nên làm vậy

Build pipeline phải phản ánh cấu trúc repo thật. Nếu Docker/Makefile sai, người mới không chạy được, CI/CD không đáng tin.

### Rủi ro nếu không làm

- `docker build` fail.
- `make up` fail.
- Deploy image thiếu frontend hoặc config.
- Runtime chạy bằng root/base image floating gây rủi ro security.

### Verify

```bash
make up
docker compose -f deploy/docker-compose.yaml up --build
curl /health
curl frontend URL
```

---

## 6. P1/P2 — DB sink identifier quoting, metadata, primary key correctness

Audit items: #30, #31, #32, #139, #140, #141, #142, #172, #226

### Vấn đề

Identifier như `schema.table` không được quote cả chuỗi. Nếu quote cả `"schema.table"`, DB hiểu đó là một identifier có dấu chấm trong tên, không phải schema + table.

Ngoài ra primary key empty string không nên bị reject nếu DB cho phép. Chỉ missing/nil mới sai. Metadata cache cần invalidation khi DDL/schema mismatch.

### Cần làm gì

1. Tận dụng helper đã có trong `pkg/utils/quoteident.go`:

```go
utils.QuoteIdentifierDoubleQuote("public.users") // -> "public"."users"
utils.QuoteIdentifierBacktick("app.users")       // -> `app`.`users`
```

Hai helper này đã split identifier dạng `schema.table` / `db.table` bằng `strings.SplitN(name, ".", 2)` rồi quote từng phần, nên không cần tạo parser mới nếu use case chỉ có 1 cấp qualifier.

2. Chỉ bổ sung parser/helper mới nếu sau này cần support identifier phức tạp hơn như:
   - `catalog.schema.table`
   - quoted input sẵn như `"public"."users"`
   - table name thật sự có dấu `.` trong tên
   - validate identifier rỗng/sai format

3. Unit test quote identifier nên tập trung vào helper hiện có trong `pkg/utils` và các sink đang gọi đúng helper đó.
4. `primaryKeyValues` chỉ reject:
   - key không tồn tại
   - value == nil

Không reject `""` nếu column cho phép empty string.

5. Metadata cache invalidation:
   - invalidate theo error code schema mismatch.
   - optional TTL.
   - manual refresh endpoint sau này.

### Làm như nào

Dùng helper hiện có trước, không tạo abstraction mới khi chưa cần:

```go
// Postgres
qualified := utils.QuoteIdentifierDoubleQuote(table)

// MySQL
qualified := utils.QuoteIdentifierBacktick(table)
```

Việc cần kiểm tra trong code là mọi nơi build SQL cho `schema.table` / `db.table` đều gọi đúng helper này, thay vì tự nối chuỗi hoặc quote thủ công. Nếu gặp identifier nhiều hơn 2 phần, lúc đó mới mở rộng `pkg/utils/quoteident.go` để parse/quote tổng quát hơn.

Cache invalidation:

```text
write fails with undefined column / unknown column / schema mismatch
  -> delete metadataCache[table]
  -> retry metadata load once
  -> if still fail, return error/DLQ
```

### Vì sao nên làm vậy

Identifier quoting bug gây query sai table hoặc fail runtime. DDL cache stale là lỗi production phổ biến khi schema thay đổi.

### Rủi ro nếu không làm

- Sink không ghi được table có schema/db.
- Table/column name reserved keyword fail.
- Empty string PK hợp lệ bị drop.
- ALTER TABLE làm flow lỗi cho tới restart.

### Verify

- Unit test quote `public.users`, `app.users`, reserved words.
- Test empty string PK accepted.
- Integration ALTER TABLE -> cache invalidated/reloaded.

---

## 7. P1 — NATS consumer lifecycle, subject encoding, retention

Audit items: #35, #36, #59, #60, #61, #62, #63, #110, #111, #112, #113, #114, #115, #171, #227

### Vấn đề

NATS subject token không nên dùng raw schema/table vì `.` tách token, `*` và `>` là wildcard. Durable consumer cần cleanup khi delete flow. Pause flow cần semantics rõ.

Retention/capacity cần config để tránh stream đầy disk hoặc xóa quá sớm.

### Cần làm gì

1. Encode subject tokens:

```text
raw schema: public
raw table: order.items
encoded subject: cdc.<source_id>.<encoded_schema>.<encoded_table>.<partition>
```

2. Lưu raw schema/table trong header/payload.
3. Durable consumer lifecycle:
   - delete flow -> delete durable consumer.
   - pause flow -> quyết định rõ:
     - giữ consumer để giữ vị trí, hoặc
     - delete consumer và dùng checkpoint để resume.
4. Offset/checkpoint per flow/table/partition.
5. NATS stream config:
   - MaxBytes
   - MaxMsgs
   - Replicas
   - Discard policy
   - Duplicate window configurable
6. Alert stream gần đầy.

### Làm như nào

Subject encoder:

```go
func EncodeSubjectToken(raw string) string
func DecodeSubjectToken(encoded string) (string, error)
```

Nên dùng base64url no padding hoặc percent-encoding an toàn cho NATS token.

Consumer policy:

```text
DeleteFlow: DeleteConsumer(flow durable)
PauseFlow: keep durable by default; stop worker only
ResumeFlow: reuse durable consumer
```

Nếu chọn delete consumer khi pause thì phải có checkpoint replay chắc chắn. Khuyến nghị ban đầu: **pause giữ consumer**.

### Vì sao nên làm vậy

NATS subject sai có thể route sai event hoặc wildcard match ngoài ý muốn. Consumer không delete sẽ leak durable state. Retention không config sẽ gây mất event hoặc đầy disk.

### Rủi ro nếu không làm

- Table có dấu `.` bị hiểu thành nhiều token.
- Consumer leak sau delete flow.
- Pause/resume behavior mơ hồ gây duplicate/loss.
- Stream đầy disk hoặc retention xóa dữ liệu flow chậm.

### Verify

- Unit test subject encode/decode với `.`, `*`, `>`, unicode.
- Integration delete flow -> consumer gone.
- Pause/resume test không mất event.
- NATS stream config test.

---

## 8. P1/P2 — Flow lifecycle, shutdown, context propagation

Audit items: #37, #38, #39, #40, #41, #64, #65, #66, #146, #147, #148, #202, #224

### Vấn đề

Flow manager/worker/source lifecycle dễ gặp:

- double release pool.
- stop worker/source khi đang giữ mutex.
- create flow success dù worker/source/sink start fail.
- dùng `context.Background()` trong hot path nên shutdown không hủy DB/HTTP write.

### Cần làm gì

1. Chỉ một owner release pool.
2. Không giữ mutex khi gọi stop/start blocking operations.
3. `CreateFlow` phải validate trước persist/running.
4. Nếu start fail:
   - flow status `ERROR`
   - lưu reason
   - không báo success giả.
5. Sink interface đổi từ:

```go
WriteBatch(events []*Event) error
```

sang:

```go
WriteBatch(ctx context.Context, events []*Event) error
```

6. Add per-sink write timeout.

### Làm như nào

Lifecycle pattern:

```go
manager.mu.Lock()
worker := manager.workers[id]
delete(manager.workers, id)
manager.mu.Unlock()

ctx, cancel := context.WithTimeout(parent, shutdownTimeout)
defer cancel()
worker.Stop(ctx)
```

Create flow pattern:

```text
validate source/sink/table/mapping
create sink
start/check source capability
create worker
if all ok -> persist RUNNING
if fail -> persist ERROR + reason or return error before persist
```

Context propagation:

```text
HTTP/gRPC request ctx -> service -> flow manager -> worker -> sink/source/store/nats
```

### Vì sao nên làm vậy

Lifecycle bug thường gây deadlock, goroutine leak, connection leak, hoặc UI báo running nhưng thực tế flow chết.

### Rủi ro nếu không làm

- Deadlock khi shutdown.
- Worker pool release double panic hoặc undefined state.
- Sink write treo không hủy được khi shutdown.
- Flow status sai.

### Verify

- Unit test stop không deadlock.
- Test CreateFlow start fail -> ERROR/reason.
- Test sink write respects context timeout.
- Race test nếu có thể: `go test -race`.

---

## 9. P1/P2 — Event pool contract

Audit items: #42, #43, #218

### Vấn đề

Event pooling giúp performance nhưng nguy hiểm nếu sink giữ reference event sau `WriteBatch`. Sau khi worker trả event về pool, data có thể bị reset/reuse.

### Cần làm gì

Chọn một contract rõ:

Option A — performance contract:

```text
Sink không được giữ reference tới event hoặc event.Data sau khi WriteBatch return.
```

Option B — safety contract:

```text
Worker deep clone event trước sink boundary.
```

Khuyến nghị: **Option A**, vì CDC hot path cần tránh clone payload lớn. Nhưng phải document và test.

### Làm như nào

1. Document ở interface `FlowSink`/`ports.Sink`:

```go
// WriteBatch must not retain events or event.Data after returning.
```

2. Nếu sink cần async write, sink phải tự copy data nội bộ.
3. Add test fake sink giữ event sau return để chứng minh worker reset pool sau write; hoặc static review sinks hiện tại không async-retain.

### Vì sao nên làm vậy

Vừa giữ performance vừa tránh bug use-after-free logic.

### Rủi ro nếu không làm

- Data corruption khó debug.
- Race khi sink async dùng event đã reset.

### Verify

- Unit test/event pool lifecycle.
- Code review all sinks không retain references.

---

## 10. P1/P2 — Source performance and worker backpressure

Audit items: #69, #70, #71, #72, #73, #74, #75, #76, #77, #190, #192, #193

### Vấn đề

Source decode/serialize hiện có thể single-thread bottleneck. Worker có thể fetch thêm NATS messages khi pool đã saturated, làm client-side unacked tăng. Batch size cố định không phản ứng sink latency.

### Cần làm gì

1. Source decode/serialize worker pool theo partition.
2. Preserve ordering theo primary key/partition.
3. Config concurrency cho Postgres/MySQL source.
4. Worker acquire semaphore trước khi fetch NATS messages.
5. Bound unacked messages phía client.
6. Tune `MaxAckPending = batch_size * worker_concurrency * safety_factor`.
7. Adaptive batch size theo sink latency.
8. Benchmark source decode, NATS publish/fetch, transform cost.

### Làm như nào

Worker fetch pattern:

```text
semaphore acquire worker slot
fetch batch
submit processBatch
on process done release slot
```

Source partitioned worker:

```text
WAL/binlog reader remains linear
  -> decode event
  -> route by partition key to partition worker
  -> partition worker serializes/publishes preserving partition order
```

Ordering guarantee:

```text
Events with same primary key must go to same partition and same worker queue.
```

Adaptive batch:

```text
if sink latency low and backlog high -> increase batch up to max
if sink latency high/errors -> decrease batch
```

### Vì sao nên làm vậy

CDC needs controlled backpressure. Fetching more than processing capacity increases memory and redelivery risk. Parallelism must preserve per-key ordering.

### Rủi ro nếu không làm

- Memory grows with unacked messages.
- NATS redelivery storm.
- Source bottleneck at high write volume.
- Ordering bug if parallelized naively.

### Verify

- Benchmark throughput before/after.
- Test same PK ordering preserved.
- Test pool saturation does not fetch more messages.
- Metrics for ack pending/backlog.

---

## 11. P1 — SQL sink performance: Postgres/MySQL bulk writes

Audit items: #78, #79, #80, #81, #82, #83, #84, #191

Chi tiết riêng đã có ở `docs/BULK_SQL_SINK_OPTIMIZATION_NOTES.md`. Tóm tắt remediation:

### Vấn đề

Hiện SQL sinks ghi row-by-row:

```text
1000 events -> 1000 Exec calls
```

Dù nằm trong transaction, vẫn tốn network roundtrip/driver overhead.

### Cần làm gì

1. Group events theo table.
2. Split theo op:
   - upsert: create/update/snapshot
   - delete
3. Postgres:
   - multi-row `INSERT ... ON CONFLICT`.
   - bulk delete bằng `(pk1, pk2) IN (...)`.
4. MySQL:
   - multi-row `INSERT ... ON DUPLICATE KEY UPDATE`.
   - bulk delete bằng OR predicates để tương thích.
5. Chunk theo parameter limit.
6. Prepared statement cache hoặc generated SQL cache theo table + chunk size.
7. Sau đó thêm bisect fallback để isolate bad rows.

### Vì sao nên làm vậy

Đây là performance win lớn nhất cho SQL sinks. Giảm `Exec` từ N rows xuống vài statements.

### Rủi ro nếu không làm

- Sink DB trở thành bottleneck.
- High latency khi batch lớn.
- Connection pool pressure cao.

### Verify

- Benchmark sink write throughput.
- Integration test composite PK.
- Test mixed op batch.
- Test parameter chunking.
- Test one bad row -> bisect/DLQ.

---

## 12. P1/P2 — ClickHouse sink correctness/performance

Audit items: #49, #50, #51, #85, #143, #144, #145, #177

### Vấn đề

ClickHouse sink không nên derive schema từ first row rồi cache mãi. First row có thể thiếu nullable column hoặc khác schema thực tế. Hardcoded `Debug: true` có thể log SQL/data và giảm performance.

### Cần làm gì

1. Load schema thật từ ClickHouse target table:

```sql
DESCRIBE TABLE db.table
```

hoặc query `system.columns`.

2. Validate required columns:
   - row columns tồn tại trong target.
   - `_cdc_op`, `_cdc_ts`, `_cdc_deleted`, `_cdc_lsn` tồn tại nếu append metadata mode.
3. Cache schema nhưng invalidate khi insert fail do schema mismatch.
4. Tối ưu insert columnar thay vì map per row nếu driver hỗ trợ.
5. Tắt `Debug: true` mặc định. Chỉ bật qua config.

### Làm như nào

Schema cache:

```go
type ClickHouseTableSchema struct {
    Columns []ColumnInfo
    ColumnSet map[string]ColumnInfo
    InsertColumns []string
}
```

On insert fail:

```text
if error indicates unknown/missing column/type mismatch:
    invalidate schema cache
    reload schema once
    retry once
```

### Vì sao nên làm vậy

ClickHouse target schema là source-of-truth. First row không đủ tin cậy. Debug logging trong production có thể lộ data và làm chậm.

### Rủi ro nếu không làm

- Insert fail khi row thiếu/thừa columns.
- Cache stale sau ALTER TABLE.
- Log sensitive data.
- Performance thấp do row map conversion.

### Verify

- Integration Postgres -> ClickHouse.
- Test target table thiếu `_cdc_*` -> fail clear.
- Test ALTER TABLE -> cache reload.
- Benchmark batch insert.

---

## 13. P1/P2 — Elasticsearch sink correctness/performance

Audit items: #52, #53, #54, #55, #56, #57, #58, #86, #87, #149, #150, #178

### Vấn đề

Elasticsearch bulk metadata line không nên build bằng string concat nếu chứa dynamic values chưa escape. Delete event thiếu document ID không được silently skip. Index name replace `.` thành `_` có thể collision.

### Cần làm gì

1. JSON encode metadata line chuẩn.
2. Validate index name trước bulk.
3. Cho phép config document ID mapping:

```yaml
elasticsearch:
  document_id_fields: [id, uuid]
```

4. Delete thiếu ID phải DLQ hoặc error, không skip thành công.
5. Tránh index collision:
   - dùng delimiter an toàn/encoding.
   - hoặc config explicit index name per flow.
6. Tune bulk size theo byte + doc count.
7. Retry/backoff cho partial failures.
8. `Info()` phải check response status và auth/cluster error.

### Làm như nào

Bulk action encode:

```go
meta := map[string]any{
  "index": map[string]any{"_index": index, "_id": id},
}
json.Encoder.Encode(meta)
```

Partial failure handling:

```text
bulk response errors=true
  -> inspect each item
  -> retry transient status 429/503
  -> DLQ permanent mapping/id errors
```

Index naming:

```text
index = configured prefix + base64url(instanceID) + "-" + base64url(schema.table)
```

### Vì sao nên làm vậy

Bulk API là JSON protocol. String concat có thể sinh JSON invalid nếu `_id`/index chứa ký tự cần escape. Silent skip delete làm sink lệch dữ liệu.

### Rủi ro nếu không làm

- JSON injection/invalid bulk payload.
- Delete không chạy nhưng message ack thành công.
- Index collision giữa `a.b` và `a_b`.
- Partial failure bị coi là success.

### Verify

- Unit test metadata JSON escaping.
- Test delete missing ID -> DLQ/error.
- Test partial bulk 429 retry.
- Test index collision cases.

---

## 14. P1/P2 — Storage API and list performance

Audit items: #67, #68, #88, #89, #90, #91, #205

### Vấn đề

Storage not found cần typed error để map sang HTTP/gRPC 404. List operations không nên scan toàn bộ NATS KV nếu số lượng source/sink/flow tăng.

### Cần làm gì

1. Thêm typed error:

```go
var ErrNotFound = errors.New("not found")
```

hoặc custom:

```go
type NotFoundError struct { Resource string; Key string }
```

2. API map:
   - gRPC NotFound
   - HTTP 404
3. Tách bucket hoặc prefix index:

```text
sources/<id>
sinks/<id>
flows/<id>
indexes/flows_by_source/<source_id>/<flow_id>
```

4. Watch/cache cho list operations.
5. Pagination API.

### Vì sao nên làm vậy

Không phân biệt not found với internal error làm client xử lý sai. Scan toàn bộ KV sẽ chậm theo dữ liệu.

### Rủi ro nếu không làm

- UI báo lỗi 500 cho resource không tồn tại.
- List API chậm dần.
- NATS KV pressure cao.

### Verify

- Unit test storage ErrNotFound mapping.
- API test 404.
- Benchmark list 10k flows.

---

## 15. P1/P2 — Observability: metrics, logs, tracing

Audit items: #92-#104, #106, #222, #223

### Vấn đề

CDC cần thấy lag, pending, retries, DLQ, end-to-end freshness. Nếu thiếu metric/log/trace, production issue sẽ khó debug.

### Cần làm gì

Metrics:

- source lag
- Postgres WAL lag
- MySQL binlog lag
- NATS stream pending
- consumer ack pending
- sink retry count
- DLQ depth
- end-to-end lag/freshness histogram
- fix sink duration label `type`
- labels: source type, sink type, table, flow

Logs:

- structured access log HTTP gateway
- request ID propagated REST/gRPC/service logs
- response header `X-Request-Id`

Tracing:

```text
source publish -> NATS -> worker -> sink write
```

### Làm như nào

Metric labels phải kiểm soát cardinality:

```text
OK: flow_id, source_id, sink_id, source_type, sink_type, table
Avoid: raw error message, SQL, dynamic user values
```

Trace context:

```text
event header carries trace_id/span_id or correlation_id
```

### Vì sao nên làm vậy

CDC failures thường là lag/backpressure/retry. Metrics cho biết hệ thống đang chậm ở source, NATS, worker hay sink.

### Rủi ro nếu không làm

- Không biết flow đang lag ở đâu.
- DLQ tăng nhưng không alert.
- Không trace được event đi qua pipeline.

### Verify

- Prometheus scrape contains expected metrics.
- Test request ID appears in logs and response header.
- Trace sample shows source->sink path.

---

## 16. P2 — NATS reliability and DLQ reprocess

Audit items: #116-#121, #117, #118, #119, #120, #121

### Vấn đề

DLQ retention cần tách main retention. Reprocess dùng timestamp message ID có thể duplicate khó kiểm soát. Product cần dry-run/dedupe/confirm cho bulk reprocess.

### Cần làm gì

1. Config riêng:

```yaml
dlq_retention_days: 30
main_retention_days: 7
```

2. Deterministic reprocess ID:

```text
reprocess:<original_msg_id>:<attempt>
```

hoặc hash payload + original sequence.

3. Dry-run reprocess:
   - count messages
   - show affected flows/tables
   - validate target flow exists
4. Dedupe preview.
5. Bulk reprocess requires confirmation.

### Vì sao nên làm vậy

DLQ là safety net. Reprocess sai có thể duplicate dữ liệu hoặc làm lỗi lặp lại.

### Rủi ro nếu không làm

- DLQ bị xóa quá sớm.
- Reprocess duplicate uncontrolled.
- Người dùng bulk replay nhầm.

### Verify

- Test reprocess deterministic ID.
- Test dry-run không publish.
- Test confirm required.

---

## 17. P2 — MySQL source DDL, reconnect, schema cache

Audit items: #46, #47, #48, #126, #127, #128, #176, #181, #292, #294

### Vấn đề

MySQL source không nên ignore DDL. ALTER TABLE làm schema cache stale. Reconnect loop không được return sau một retry cycle. Stop phải tránh close channel twice.

### Cần làm gì

1. Capture DDL/schema change event.
2. Refresh schema cache khi ALTER TABLE.
3. Emit schema-change event để downstream biết.
4. Reconnect loop chạy liên tục với backoff cho tới context cancel hoặc fatal error.
5. Nếu reconnect fail lâu, set source status `ERROR`.
6. `Stop()` dùng `sync.Once`.
7. Guardrail kiểm tra binlog config đúng.
8. Alert binlog purge risk.

### Làm như nào

DDL handling:

```text
On DDL event:
  parse affected db/table
  invalidate schema cache
  emit schema_change event
  continue CDC or pause affected flows depending config
```

Reconnect:

```go
for ctx not canceled {
    err := connectAndRun()
    if fatal config error -> set ERROR return
    backoff sleep
}
```

### Vì sao nên làm vậy

Schema thay đổi là chuyện thường. Nếu cache stale, decode sai columns. Reconnect là core reliability cho CDC.

### Rủi ro nếu không làm

- Data map sai column sau ALTER.
- Source chết sau retry đầu.
- Panic close channel twice.
- Binlog bị purge trước khi CDC đọc.

### Verify

- Integration ALTER TABLE then update.
- Kill MySQL connection -> reconnect.
- Double Stop no panic.

---

## 19. P2 — Type mapping and validation

Audit items: #136, #137, #138, #180

### Vấn đề

Source/sink type mismatch có thể làm write fail runtime. Numeric/time/json/bool cần mapping rõ.

### Cần làm gì

1. Discover source schema and sink schema.
2. Validate mapping compatibility before create/update flow.
3. Suggest casts/conversions:
   - numeric -> decimal/string
   - timestamp -> timestamp/date/string
   - json -> json/text
4. Unit/integration tests for numeric/time/json/bool.

### Làm như nào

Compatibility matrix:

```text
source_type, sink_type, compatible, requires_cast, risk
```

Flow create validation:

```text
if incompatible -> reject with actionable message
if risky -> warn or require confirm
```

### Vì sao nên làm vậy

Failing at flow create is better than failing after messages enter pipeline.

### Rủi ro nếu không làm

- Runtime sink errors.
- DLQ noise.
- Data truncation/precision loss.

### Verify

- Unit matrix tests.
- Integration numeric precision.

---

## 20. P2 — Config, environment, public errors

Audit items: #151-#157, #153-#155, #215, #216, #228

### Vấn đề

Config/env defaults cần rõ. Public API không nên trả raw DB/SQL/DSN error. Prod-safe defaults khác dev defaults.

### Cần làm gì

1. Centralize defaults.
2. Tách dev defaults và prod-safe defaults.
3. Env override naming document rõ.
4. Validate startup config:
   - retention too low
   - ack wait too low
   - batch/worker invalid
5. Redact secrets in errors/logs.
6. Public error taxonomy:
   - validation
   - not found
   - conflict
   - transient
   - permanent

### Vì sao nên làm vậy

Config sai có thể gây loss/retry storm. Raw errors có thể leak credentials/table names/SQL.

### Rủi ro nếu không làm

- Dangerous config accepted.
- Secret leak to UI/API logs.
- Client không biết lỗi retry được không.

### Verify

- Unit config validation.
- Secret redaction tests.
- API error response tests.

---

## 21. P2 — Frontend API client and DX

Audit items: #161-#165

### Vấn đề

Frontend long-running calls nên abort được. GET idempotent có thể retry backoff. Vite dev nên có API base/proxy chuẩn.

### Cần làm gì

1. API client support `AbortController`.
2. Retry idempotent GET với exponential backoff.
3. Default API base phù hợp Vite dev.
4. Add Vite proxy tới backend local.
5. Verify/pin React/Vite/TS/ESLint versions.

### Vì sao nên làm vậy

UI dev/run ổn định hơn, tránh request treo khi user chuyển page.

### Rủi ro nếu không làm

- Memory leak request.
- UI fail do backend cold start/network transient.
- Dev phải config tay.

### Verify

- UI request abort on unmount.
- GET retry test/mock.
- Vite dev proxy works.

---

## 22. P2/P3 — Testing strategy

Audit items: #169-#188, #190-#193, #317-#321

### Cần làm gì

Unit tests:

- mapping Debezium envelope
- filter CEL vars
- subject encoding
- identifier quoting
- DSN escaping
- secret redaction

Integration tests:

- Postgres source -> Postgres sink
- MySQL source -> MySQL sink
- Postgres source -> ClickHouse sink
- source -> Elasticsearch sink
- delete/update replica identity default/full
- numeric precision
- schema change ALTER TABLE
- crash after publish before sink write
- restart mid-batch
- NATS retention expiry

Chaos tests:

- kill NATS
- kill source DB
- long sink outage
- reconnect longer than duplicate window

Benchmarks:

- source decode throughput
- sink write throughput
- NATS publish/fetch throughput
- transform filter/mapping cost

Connector certification:

- inserts/updates/deletes
- composite PK
- DDL/schema evolution
- high-volume workload
- failover/reconnect

### Vì sao nên làm vậy

CDC bugs thường chỉ lộ ở integration/chaos. Unit test không đủ chứng minh delivery correctness.

### Verify

CI stages:

```text
unit -> integration docker compose -> chaos optional/nightly -> benchmark manual/nightly
```

---

## 23. P2 — Architecture/refactor/interfaces

Audit items: #199-#216, #201-#214

### Vấn đề

Core responsibilities đang có dấu hiệu lẫn:

- domain event vs Debezium envelope
- source publisher vs flow manager
- worker lifecycle vs flow CRUD
- checkpoint service chưa tách rõ
- DLQ service có thể lẫn trong NATS manager
- connector capabilities chưa rõ

### Cần làm gì

1. Tách domain event nội bộ khỏi Debezium transport envelope.
2. Chuẩn hóa internal event schema.
3. Tách services:
   - SourcePublisher
   - FlowLifecycleManager
   - CheckpointService
   - DLQService
   - ConnectorFactory/Registry
4. Interfaces:

```go
type Source interface {
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
    Health(ctx context.Context) HealthStatus
    Lag(ctx context.Context) LagStatus
}

type Sink interface {
    WriteBatch(ctx context.Context, events []*Event) error
    Capabilities() SinkCapabilities
    TestConnection(ctx context.Context) error
    DiscoverSchema(ctx context.Context) (...)
}
```

5. Error taxonomy and retry classification.

### Vì sao nên làm vậy

Clear boundaries giúp test dễ, thay connector dễ, tránh flow manager thành god object.

### Rủi ro nếu không làm

- Function quá dài, khó maintain.
- Connector mới khó thêm.
- Retry sai vì không biết transient/permanent.

### Verify

- Unit tests per service.
- No cyclic dependency.
- Connector capability tests.

---

## 24. P2/P3 — Cleanup/code quality

Audit items: #217-#230, #220, #221

### Cần làm gì

1. Xóa comment sai về exactly-once.
2. Xóa dead code quanh pool clone hoặc các path cũ không còn dùng.
3. Rename variables:
   - `sourceOffset`
   - `sinkCheckpoint`
   - `natsSequence`
4. Giảm function quá dài trong flow manager/source connectors.
5. Chuẩn hóa logging fields:

```text
flow_id, source_id, sink_id, table, partition
```

6. Metric labels tránh cardinality cao.
7. Context propagation, bỏ `context.Background()` trong hot path.
8. Retry/backoff helper dùng chung.
9. Table identifier parser dùng chung.
10. Subject encoder/decoder dùng chung.
11. Secret redactor dùng chung.
12. JSON encode/decode helper cho payload.
13. Lint rule chống string concat JSON.

### Vì sao nên làm vậy

Cleanup giúp giảm bug tương lai và làm code dễ review. Nhưng nên làm sau correctness chính để tránh refactor trên nền logic sai.

### Verify

- Lint/static checks.
- Tests unchanged pass.
- Code review checklist.

---

## 25. P3 — Product/UI/API improvements

Audit items: #255-#270

### Cần làm gì

UI Flow Wizard:

- hiển thị DB prerequisites
- kiểm tra quyền DB
- preview schema diff
- estimate throughput/lag risk
- recommend batch size/workers/partitions

UI runtime:

- hiển thị flow status `ERROR` + reason
- source lag/sink lag/DLQ depth
- live test event
- dry-run mapping/filter
- replay DLQ an toàn

API:

- validate flow before create
- discover source schema
- compare source/sink schema
- flow health summary
- DLQ stats
- lag by table

### Vì sao nên làm vậy

Product features này giúp người dùng tự diagnose trước khi tạo flow và vận hành flow an toàn hơn.

### Rủi ro nếu làm quá sớm

Nếu backend validation/checkpoint/metrics chưa ổn, UI sẽ hiển thị dữ liệu không đáng tin.

### Verify

- E2E flow wizard.
- API contract tests.
- UI state for ERROR/reason.

---

## 26. P3 — Data quality, repair, reconciliation

Audit items: #271-#275

### Cần làm gì

1. Row count reconciliation.
2. Checksum reconciliation theo window.
3. Sampled compare source vs sink.
4. Drift detection.
5. Repair job từ source sang sink.

### Vì sao nên làm vậy

CDC at-least-once/idempotent vẫn có thể lệch do schema drift, sink outage, manual edits. Reconciliation giúp phát hiện và sửa drift.

### Verify

- Known drift dataset -> detect.
- Repair job restores rows.

---

## 26. P3 — HA, guardrails, operations

Audit items: #286-#296

### Cần làm gì

HA:

- leader election cho source readers
- horizontal scaling workers theo partition assignment
- NATS cluster replicas
- graceful failover source reader
- idempotent recovery after crash

Guardrails:

- refuse start nếu Postgres `wal_level` chưa đúng
- refuse start nếu MySQL binlog config chưa đúng
- alert replication slot bloat
- alert binlog purge risk
- auto-pause source khi sink outage quá lâu
- alert DLQ rate spike

### Vì sao nên làm vậy

Đây là lớp vận hành production cho chính hệ thống CDC: đảm bảo source reader không chạy trùng, worker scale được theo partition, failover không làm mất/duplicate dữ liệu ngoài mức guarantee đã công bố, và hệ thống tự cảnh báo trước khi replication slot/binlog/DLQ gây sự cố.

### Verify

- Failover tests.
- Config guardrail tests.
- Kill active source reader -> standby takeover.
- Sink outage dài -> auto-pause/alert đúng ngưỡng.

---

## 28. P3 — Docs and connector extensibility

Audit items: #231-#240, #311, #317-#321

### Cần làm gì

Docs:

- support matrix source/sink
- MySQL sink docs
- Docker/compose quickstart
- NATS sizing guide
- retention/duplicate-window guide
- delivery guarantee table
- schema evolution behavior

Extensibility:

- Kafka source/sink
- connector certification suite

### Vì sao nên làm vậy

Docs phải phản ánh behavior thật sau P0/P1 fixes. Connector certification giúp thêm connector mới mà không phá guarantee.

### Verify

- Quickstart fresh machine.
- Docs examples match config/schema.
- Certification suite pass for each connector.

---

## 29. Thứ tự triển khai khuyến nghị

### Phase 1 — Data correctness foundation

1. Delivery/checkpoint/guarantee (#11-#15)
2. Postgres LSN/replica identity/publication correctness (#16-#20, #33-#34)
3. Transform/filter Debezium correctness (#21-#24, #107-#109)
4. Docker/Makefile runtime fix (#25-#29)

### Phase 2 — Runtime safety

1. Identifier quoting and metadata invalidation (#30-#32, #141-#143)
2. NATS subject encoding and consumer lifecycle (#35-#36, #59-#63)
3. Flow lifecycle/shutdown/context (#37-#41, #64-#66, #146-#148)
4. Event pool contract (#42-#43)

### Phase 3 — Performance core

1. Worker backpressure (#73-#77)
2. SQL sink bulk writes (#78-#84)
3. ClickHouse schema/insert optimization (#49-#51, #85)
4. Elasticsearch bulk correctness/retry (#52-#58, #86-#87)
5. Storage list optimization (#88-#91)

### Phase 4 — Observability and reliability

1. Metrics/logs/tracing (#92-#106)
2. NATS reliability config (#110-#115)
3. DLQ retention/reprocess (#116-#121)
4. Source reconnect/DDL handling (#122-#129, #46-#48)

### Phase 5 — Productization and hardening

1. Type mapping (#136-#138)
2. Config validation and public error handling (#151-#157)
3. Testing/integration/chaos/benchmarks (#169-#193)
4. Architecture refactor (#199-#216)
5. UI/API/reconciliation/HA/docs (#231+)

---

## 30. Definition of Done chung

Một remediation item chỉ nên được coi là xong khi có đủ:

1. Code change hoặc config/doc change đúng scope.
2. Unit test cho logic mới.
3. Integration test nếu liên quan delivery/source/sink.
4. Metrics/logs nếu liên quan runtime behavior.
5. Docs update nếu behavior hoặc guarantee đổi.
6. Không có claim guarantee cao hơn khả năng thực tế.

---

## 31. Kết luận

Các audit items có thể gộp thành 8 trục chính:

1. Delivery/checkpoint correctness.
2. Source correctness and schema evolution.
3. Transform/filter correctness.
4. Sink correctness and performance.
5. NATS/storage lifecycle and retention.
6. Runtime lifecycle/context/reliability.
7. Observability/testing.
8. Product/docs/ops/HA.

Nên xử lý theo thứ tự trên. Đừng bắt đầu bằng UI/product hoặc refactor lớn khi delivery/checkpoint chưa chắc, vì CDC sai guarantee sẽ làm mọi tầng phía trên không đáng tin.
