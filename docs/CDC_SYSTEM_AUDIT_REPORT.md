## Danh sách cần làm

| # | Mức | Nhóm | Khu vực | Việc cần làm |
|---:|---|---|---|---|
| 11 | P0 | Correctness | Delivery | Sửa logic save source offset: không checkpoint trước khi sink xử lý xong. |
| 12 | P0 | Correctness | Delivery | Bỏ claim exactly-once nếu chưa có end-to-end exactly-once thật. |
| 13 | P0 | Correctness | Delivery | Thiết kế checkpoint per-flow thay vì global source offset. |
| 14 | P0 | Correctness | Delivery | Thêm checkpoint sau sink ack thành công. |
| 15 | P0 | Correctness | Delivery | Ràng buộc NATS retention theo min checkpoint của các flow. |
| 16 | P0 | Correctness | Postgres source | Không report WAL flush/apply vượt quá LSN đã durable checkpoint. |
| 17 | P0 | Correctness | Postgres source | Sửa trường hợp `flushedLSN == 0` đang fallback về `clientLSN`. |
| 18 | P0 | Correctness | Postgres source | Nil-check `OldTuple` cho update/delete. |
| 19 | P0 | Correctness | Postgres source | Validate replica identity trước khi chạy CDC. |
| 20 | P0 | Correctness | Postgres source | Báo lỗi rõ khi table không đủ replica identity cho delete/update. |
| 21 | P0 | Correctness | Transform | Sửa mapping đang apply vào envelope thay vì `after/before`. |
| 22 | P0 | Correctness | Transform | Sửa filter docs/implementation: dùng `after.status` thay vì `data.status` nếu payload Debezium. |
| 23 | P0 | Correctness | Transform | Thêm biến filter rõ ràng: `before`, `after`, `op`, `source`, `table`. |
| 24 | P0 | Correctness | Transform | Thêm test transform với payload Debezium thật. |
| 25 | P0 | Build | Docker | Sửa Dockerfile đang trỏ sai `client/`; repo dùng `website/`. |
| 26 | P0 | Build | Docker | Sửa Dockerfile đang dùng Next `.next`; frontend là Vite. |
| 27 | P0 | Build | Docker | Sửa copy config path từ root sang `config/config.example.yaml`. |
| 28 | P0 | Runtime | Makefile | Sửa `CONFIG_FILE := deploy/app/config.yaml` vì path không tồn tại. |
| 29 | P0 | Runtime | Makefile | Đổi default config sang `config/config.yaml` hoặc config path hợp lệ. |
| 30 | P1 | Correctness | DB sink | Sửa quote `schema.table` trong Postgres sink. |
| 31 | P1 | Correctness | DB sink | Sửa quote `db.table` trong MySQL sink. |
| 32 | P1 | Correctness | DB sink | Quote từng phần identifier thay vì quote cả chuỗi. |
| 33 | P1 | Correctness | Postgres source | Ensure publication tồn tại trước StartReplication. |
| 34 | P1 | Correctness | Postgres source | Update publication khi danh sách table thay đổi. |
| 35 | P1 | Correctness | NATS | Delete durable consumer khi delete flow. |
| 36 | P1 | Correctness | NATS | Định nghĩa rõ pause flow giữ consumer hay xóa consumer. |
| 37 | P1 | Correctness | Lifecycle | Sửa double release pool giữa manager và worker. |
| 38 | P1 | Correctness | Lifecycle | Chỉ một nơi sở hữu việc release pool. |
| 39 | P1 | Correctness | Shutdown | Không giữ mutex khi stop worker/source. |
| 40 | P1 | Correctness | Shutdown | Copy references dưới lock rồi unlock trước khi stop. |
| 41 | P1 | Correctness | Shutdown | Thêm shutdown timeout sạch cho source/sink/worker. |
| 42 | P1 | Correctness | Event pool | Làm rõ contract sink không được giữ reference event sau `WriteBatch`. |
| 43 | P1 | Correctness | Event pool | Hoặc deep clone event trước khi đưa qua sink boundary. |
| 44 | P1 | Correctness | Postgres type | Không parse `numeric` sang `float64`. |
| 45 | P1 | Correctness | Postgres type | Preserve decimal/numeric dạng string hoặc decimal lib. |
| 46 | P1 | Correctness | MySQL source | Không ignore DDL event. |
| 47 | P1 | Correctness | MySQL source | Refresh schema cache khi ALTER TABLE. |
| 48 | P1 | Correctness | MySQL source | Emit schema-change event. |
| 49 | P1 | Correctness | ClickHouse sink | Không derive schema từ first row rồi cache mãi. |
| 50 | P1 | Correctness | ClickHouse sink | Load schema thật từ ClickHouse target table. |
| 51 | P1 | Correctness | ClickHouse sink | Validate required columns trước insert. |
| 52 | P1 | Correctness | Elasticsearch sink | Không build bulk metadata JSON bằng string concat. |
| 53 | P1 | Correctness | Elasticsearch sink | JSON encode metadata line chuẩn. |
| 54 | P1 | Correctness | Elasticsearch sink | Validate index name trước khi bulk. |
| 55 | P1 | Correctness | Elasticsearch sink | Delete event không được silently skip khi thiếu document ID. |
| 56 | P1 | Correctness | Elasticsearch sink | Cho phép cấu hình key mapping cho document ID. |
| 57 | P1 | Correctness | Elasticsearch sink | Đẩy event thiếu key vào DLQ thay vì ack thành công. |
| 58 | P1 | Correctness | Elasticsearch sink | Tránh collision index khi replace `.` thành `_`. |
| 59 | P1 | Correctness | NATS subject | Không dùng raw schema/table trong NATS subject. |
| 60 | P1 | Correctness | NATS subject | Encode subject token để tránh dot/wildcard `*`/`>`. |
| 61 | P1 | Correctness | NATS subject | Lưu raw table/schema trong header/payload. |
| 62 | P1 | Correctness | Offset | Không dùng offset global per source cho mọi flow. |
| 63 | P1 | Correctness | Offset | Thêm offset/checkpoint per flow, per table, per partition. |
| 64 | P1 | Correctness | Flow create | `CreateFlow` không nên success nếu worker/source/sink start fail. |
| 65 | P1 | Correctness | Flow create | Nếu start fail, set flow status `ERROR` + reason. |
| 66 | P1 | Correctness | Flow create | Validate source/sink/table/mapping trước khi persist running flow. |
| 67 | P1 | Correctness | Storage | Thêm typed `ErrNotFound`. |
| 68 | P1 | Correctness | Storage | Map not found sang gRPC/HTTP 404 đúng. |
| 69 | P1 | Performance | Source | Tách source decode/serialize sang worker pool partitioned. |
| 70 | P1 | Performance | Source | Preserve ordering theo primary key/partition. |
| 71 | P1 | Performance | Source | Configurable concurrency cho Postgres source. |
| 72 | P1 | Performance | Source | Configurable concurrency cho MySQL source. |
| 73 | P1 | Performance | Worker | Không fetch thêm batch khi pool đang saturated. |
| 74 | P1 | Performance | Worker | Acquire semaphore trước khi fetch NATS messages. |
| 75 | P1 | Performance | Worker | Bound client-side unacked messages. |
| 76 | P1 | Performance | Worker | Tune `MaxAckPending` theo batch size * workers. |
| 77 | P1 | Performance | Worker | Thêm adaptive batch size theo sink latency. |
| 78 | P1 | Performance | Postgres sink | Không row-by-row `Exec` cho từng event. |
| 79 | P1 | Performance | Postgres sink | Batch theo table/op. |
| 80 | P1 | Performance | Postgres sink | Dùng prepared statement cache. |
| 81 | P1 | Performance | Postgres sink | Dùng COPY/temp table + merge cho bulk upsert. |
| 82 | P1 | Performance | MySQL sink | Không row-by-row `Exec`. |
| 83 | P1 | Performance | MySQL sink | Multi-row `INSERT ... ON DUPLICATE KEY UPDATE`. |
| 84 | P1 | Performance | MySQL sink | Batch delete theo key list. |
| 85 | P1 | Performance | ClickHouse sink | Tối ưu batch insert columnar thay vì từng row map. |
| 86 | P1 | Performance | Elasticsearch sink | Tune bulk size theo byte + doc count. |
| 87 | P1 | Performance | Elasticsearch sink | Thêm retry/backoff cho bulk partial failures. |
| 88 | P1 | Performance | Storage | Không list NATS KV bằng scan toàn bộ key. |
| 89 | P1 | Performance | Storage | Tách bucket hoặc prefix index cho source/sink/flow. |
| 90 | P1 | Performance | Storage | Thêm cache/watch cho list operations. |
| 91 | P1 | Performance | Storage | Thêm pagination cho API list source/sink/flow. |
| 92 | P1 | Observability | Metrics | Thêm source lag metric. |
| 93 | P1 | Observability | Metrics | Thêm Postgres WAL lag metric. |
| 94 | P1 | Observability | Metrics | Thêm MySQL binlog lag metric. |
| 95 | P1 | Observability | Metrics | Thêm NATS stream pending metric. |
| 96 | P1 | Observability | Metrics | Thêm consumer ack pending metric. |
| 97 | P1 | Observability | Metrics | Thêm sink retry count metric. |
| 98 | P1 | Observability | Metrics | Thêm DLQ depth metric. |
| 99 | P1 | Observability | Metrics | Thêm end-to-end lag/freshness histogram. |
| 100 | P1 | Observability | Metrics | Fix sink duration label `type` đang empty. |
| 101 | P1 | Observability | Metrics | Label metrics theo source type/sink type/table/flow. |
| 102 | P1 | Observability | Logs | Thêm structured access log cho HTTP gateway. |
| 103 | P1 | Observability | Logs | Propagate request ID qua REST/gRPC/service logs. |
| 104 | P1 | Observability | Logs | Thêm response header `X-Request-Id`. |
| 106 | P1 | Observability | Tracing | Trace source publish → NATS → worker → sink write. |
| 107 | P1 | Correctness | Filter | Phân biệt filtered count và filter error count. |
| 108 | P1 | Correctness | Filter | Bad filter không được silently drop data. |
| 109 | P1 | Correctness | Filter | Filter eval error nên DLQ hoặc pause flow. |
| 110 | P2 | Reliability | NATS | Config `MaxBytes` cho stream. |
| 111 | P2 | Reliability | NATS | Config `MaxMsgs` cho stream. |
| 112 | P2 | Reliability | NATS | Config replicas cho JetStream. |
| 113 | P2 | Reliability | NATS | Config discard policy. |
| 114 | P2 | Reliability | NATS | Alert khi stream gần đầy disk. |
| 115 | P2 | Reliability | NATS | Duplicate window không hardcode 2 phút. |
| 116 | P2 | Reliability | DLQ | Tách `dlq_retention_days` khỏi main retention. |
| 117 | P2 | Correctness | DLQ | Reprocess không nên tạo msg ID theo timestamp. |
| 118 | P2 | Correctness | DLQ | Thêm deterministic reprocess ID để giảm duplicate. |
| 119 | P2 | Product | DLQ | Thêm dry-run reprocess. |
| 120 | P2 | Product | DLQ | Thêm dedupe preview trước reprocess. |
| 121 | P2 | Product | DLQ | Thêm bulk reprocess có confirm. |
| 122 | P2 | Correctness | Postgres source | Không ignore error khi create replication slot. |
| 123 | P2 | Correctness | Postgres source | Chỉ ignore lỗi slot already exists. |
| 124 | P2 | Reliability | Postgres source | Close old replication connection khi reconnect. |
| 125 | P2 | Reliability | Postgres source | Cleanup connection nếu start replication fail nửa chừng. |
| 126 | P2 | Reliability | MySQL source | Reconnect loop không được return sau một retry cycle. |
| 127 | P2 | Reliability | MySQL source | Nếu reconnect fail liên tục, set source status `ERROR`. |
| 128 | P2 | Reliability | MySQL source | `Stop()` dùng `sync.Once` để tránh close channel twice. |
| 129 | P2 | Reliability | Postgres source | Audit `Stop()` Postgres để tránh double close. |
| 130 | P2 | Correctness | Snapshot | Làm rõ snapshot support hiện tại. |
| 131 | P2 | Correctness | Snapshot | Thêm initial snapshot cho Postgres. |
| 132 | P2 | Correctness | Snapshot | Thêm initial snapshot cho MySQL. |
| 133 | P2 | Correctness | Snapshot | Snapshot phải resumable. |
| 134 | P2 | Correctness | Snapshot | Snapshot cần watermark/cutover sang WAL/binlog an toàn. |
| 135 | P2 | Performance | Snapshot | Parallel chunk snapshot theo PK range. |
| 136 | P2 | Correctness | Type mapping | Enforce type compatibility khi create/update flow. |
| 137 | P2 | Correctness | Type mapping | Gợi ý cast/convert khi mapping type lệch. |
| 138 | P2 | Correctness | Type mapping | Thêm test cho mapping numeric/time/json/bool. |
| 139 | P2 | Correctness | DB sink | Empty-string primary key không nên bị reject nếu DB cho phép. |
| 140 | P2 | Correctness | DB sink | Chỉ reject missing/nil primary key. |
| 141 | P2 | Correctness | Postgres sink | Invalidate table metadata cache khi ALTER TABLE/error. |
| 142 | P2 | Correctness | MySQL sink | Invalidate table metadata cache khi ALTER TABLE/error. |
| 143 | P2 | Correctness | ClickHouse sink | Invalidate schema cache khi insert fail do schema mismatch. |
| 144 | P2 | Performance | ClickHouse sink | Tắt hardcoded `Debug: true`. |
| 145 | P2 | Security | ClickHouse sink | Không log SQL/data ở debug mặc định. |
| 146 | P2 | Reliability | Sinks | Đổi sink interface thành `WriteBatch(ctx, events)`. |
| 147 | P2 | Reliability | Sinks | Pass context shutdown vào DB/HTTP writes. |
| 148 | P2 | Reliability | Sinks | Thêm per-sink write timeout. |
| 149 | P2 | Correctness | Elasticsearch sink | Check `Info()` response status. |
| 150 | P2 | Correctness | Elasticsearch sink | Fail fast nếu Elasticsearch auth/cluster error. |
| 151 | P2 | Security | Errors | Không trả raw DB/SQL/DSN error ra client. |
| 152 | P2 | Security | Errors | Chuẩn hóa public error code/message. |
| 153 | P2 | Config | Prometheus | Default Prometheus URL trong container không nên là localhost. |
| 154 | P2 | Config | Env | Bind đầy đủ env vars cho runtime knobs. |
| 155 | P2 | Config | Env | Document env override naming rõ ràng. |
| 156 | P2 | Config | Config validate | Validate retention/batch/worker/ack wait khi startup. |
| 157 | P2 | Config | Config validate | Reject config nguy hiểm: retention quá thấp, ack wait quá thấp. |
| 161 | P2 | Frontend | API client | Abort/cancel request cho long-running calls. |
| 162 | P2 | Frontend | API client | Retry idempotent GET với backoff. |
| 163 | P2 | Frontend | DX | Sửa API base default cho Vite dev. |
| 164 | P2 | Frontend | DX | Thêm Vite proxy tới backend local. |
| 165 | P2 | Frontend | Dependencies | Verify/pin React/Vite/TS/ESLint versions. |
| 169 | P2 | Testing | Unit | Thêm unit test cho mapping Debezium envelope. |
| 170 | P2 | Testing | Unit | Thêm unit test cho filter CEL vars. |
| 171 | P2 | Testing | Unit | Thêm unit test cho subject encoding. |
| 172 | P2 | Testing | Unit | Thêm unit test cho identifier quoting. |
| 173 | P2 | Testing | Unit | Thêm unit test cho DSN escaping. |
| 174 | P2 | Testing | Unit | Thêm unit test cho secret redaction. |
| 175 | P2 | Testing | Integration | Test Postgres source → Postgres sink. |
| 176 | P2 | Testing | Integration | Test MySQL source → MySQL sink. |
| 177 | P2 | Testing | Integration | Test Postgres source → ClickHouse sink. |
| 178 | P2 | Testing | Integration | Test source → Elasticsearch sink. |
| 179 | P2 | Testing | Integration | Test delete/update với replica identity default/full. |
| 180 | P2 | Testing | Integration | Test decimal/numeric precision. |
| 181 | P2 | Testing | Integration | Test schema change ALTER TABLE. |
| 182 | P2 | Testing | Integration | Test crash after publish before sink write. |
| 183 | P2 | Testing | Integration | Test restart mid-batch. |
| 184 | P2 | Testing | Integration | Test NATS retention expiry scenario. |
| 185 | P2 | Testing | Chaos | Test kill NATS. |
| 186 | P2 | Testing | Chaos | Test kill source DB. |
| 187 | P2 | Testing | Chaos | Test sink outage dài. |
| 188 | P2 | Testing | Chaos | Test reconnect lâu hơn duplicate window. |
| 190 | P2 | Testing | Benchmark | Benchmark source decode throughput. |
| 191 | P2 | Testing | Benchmark | Benchmark sink write throughput. |
| 192 | P2 | Testing | Benchmark | Benchmark NATS publish/fetch throughput. |
| 193 | P2 | Testing | Benchmark | Benchmark transform filter/mapping cost. |
| 199 | P2 | Refactor | Architecture | Tách rõ domain event khỏi Debezium transport envelope. |
| 200 | P2 | Refactor | Architecture | Chuẩn hóa event schema nội bộ. |
| 201 | P2 | Refactor | Architecture | Tách source publisher khỏi flow manager. |
| 202 | P2 | Refactor | Architecture | Tách worker lifecycle khỏi flow CRUD service. |
| 203 | P2 | Refactor | Architecture | Tách offset/checkpoint service riêng. |
| 204 | P2 | Refactor | Architecture | Tách DLQ service riêng khỏi NATS manager nếu đang lẫn trách nhiệm. |
| 205 | P2 | Refactor | Architecture | Tách storage repository interface rõ lỗi typed. |
| 206 | P2 | Refactor | Architecture | Tách connector registry/factory khỏi manager orchestration. |
| 207 | P2 | Refactor | Interfaces | Source interface cần expose health/status/lag. |
| 208 | P2 | Refactor | Interfaces | Sink interface cần expose type/capabilities. |
| 209 | P2 | Refactor | Interfaces | Sink capabilities: supports upsert/delete/schema evolution/bulk. |
| 210 | P2 | Refactor | Interfaces | Connector interface cần `ValidateConfig`. |
| 211 | P2 | Refactor | Interfaces | Connector interface cần `TestConnection(ctx)`. |
| 212 | P2 | Refactor | Interfaces | Connector interface cần `DiscoverSchema(ctx)`. |
| 213 | P2 | Refactor | Errors | Chuẩn hóa error taxonomy: validation, not found, conflict, transient, permanent. |
| 214 | P2 | Refactor | Errors | Thêm retry classification cho sink/source errors. |
| 215 | P2 | Refactor | Config | Gom config defaults vào một nơi. |
| 216 | P2 | Refactor | Config | Tách dev defaults và prod-safe defaults. |
| 217 | P2 | Cleanup | Code | Xóa comment sai/misleading về exactly-once. |
| 218 | P2 | Cleanup | Code | Xóa dead code nếu có quanh snapshot/pool clone. |
| 219 | P2 | Cleanup | Code | Rename variables để rõ `sourceOffset` vs `sinkCheckpoint`. |
| 220 | P2 | Cleanup | Code | Giảm function quá dài trong flow manager. |
| 221 | P2 | Cleanup | Code | Giảm function quá dài trong source connectors. |
| 222 | P2 | Cleanup | Code | Chuẩn hóa logging fields: `flow_id`, `source_id`, `sink_id`, `table`, `partition`. |
| 223 | P2 | Cleanup | Code | Chuẩn hóa metric labels tránh cardinality cao. |
| 224 | P2 | Cleanup | Code | Chuẩn hóa context propagation, bỏ `context.Background()` trong hot path. |
| 225 | P2 | Cleanup | Code | Chuẩn hóa retry/backoff helper dùng chung. |
| 226 | P2 | Cleanup | Code | Chuẩn hóa table identifier parser dùng chung. |
| 227 | P2 | Cleanup | Code | Chuẩn hóa subject encoder/decoder dùng chung. |
| 228 | P2 | Cleanup | Code | Chuẩn hóa secret redactor dùng chung. |
| 229 | P2 | Cleanup | Code | Chuẩn hóa JSON encode/decode helper cho payload. |
| 230 | P2 | Cleanup | Code | Thêm lint rule chống string concat JSON. |
| 231 | P3 | Docs | README | Cập nhật support matrix source/sink. |
| 232 | P3 | Docs | README | Thêm MySQL sink vào docs nếu đã support. |
| 234 | P3 | Docs | README | Sửa Docker/compose quickstart. |
| 237 | P3 | Docs | README | Thêm NATS sizing guide. |
| 238 | P3 | Docs | README | Thêm retention/duplicate-window guide. |
| 239 | P3 | Docs | README | Thêm delivery guarantee table. |
| 240 | P3 | Docs | README | Thêm schema evolution behavior. |
| 245 | P3 | Ops | Compose | `make up` dùng đúng `deploy/docker-compose.yaml`. |
| 246 | P3 | Ops | Compose | Bind NATS port vào `127.0.0.1` cho dev. |
| 247 | P3 | Ops | Compose | Thêm NATS auth trong compose non-dev. |
| 248 | P3 | Ops | Docker | Đổi Node image sang LTS nếu còn cần Node runtime. |
| 249 | P3 | Ops | Docker | Nếu Vite static, serve bằng nginx/caddy thay Node runtime. |
| 250 | P3 | Ops | Docker | Multi-stage build tối giản image size. |
| 251 | P3 | Ops | Docker | Add healthcheck cho app container. |
| 252 | P3 | Ops | Docker | Add non-root user trong runtime image. |
| 253 | P3 | Ops | Docker | Pin base image versions. |
| 254 | P3 | Ops | Docker | Add SBOM/vuln scan trong CI. |
| 255 | P3 | Product | UI | Flow wizard hiển thị DB prerequisites. |
| 256 | P3 | Product | UI | Flow wizard kiểm tra quyền DB. |
| 257 | P3 | Product | UI | Flow wizard preview schema diff. |
| 258 | P3 | Product | UI | Flow wizard estimate throughput/lag risk. |
| 259 | P3 | Product | UI | Flow wizard recommend batch size/workers/partitions. |
| 260 | P3 | Product | UI | UI hiển thị flow status `ERROR` + reason. |
| 261 | P3 | Product | UI | UI hiển thị source lag/sink lag/DLQ depth. |
| 262 | P3 | Product | UI | UI hiển thị live test event. |
| 263 | P3 | Product | UI | UI cho phép dry-run mapping/filter. |
| 264 | P3 | Product | UI | UI cho phép replay DLQ an toàn. |
| 265 | P3 | Product | API | Add endpoint validate flow trước create. |
| 266 | P3 | Product | API | Add endpoint discover source schema. |
| 267 | P3 | Product | API | Add endpoint compare source/sink schema. |
| 268 | P3 | Product | API | Add endpoint flow health summary. |
| 269 | P3 | Product | API | Add endpoint DLQ stats. |
| 270 | P3 | Product | API | Add endpoint lag by table. |
| 271 | P3 | Data quality | Reconcile | Thêm row count reconciliation. |
| 272 | P3 | Data quality | Reconcile | Thêm checksum reconciliation theo window. |
| 273 | P3 | Data quality | Reconcile | Thêm sampled compare source vs sink. |
| 274 | P3 | Data quality | Reconcile | Thêm drift detection. |
| 275 | P3 | Data quality | Repair | Thêm repair job từ source sang sink. |
| 276 | P3 | Performance | Hot path | Profiling CPU source decode. |
| 277 | P3 | Performance | Hot path | Profiling allocation event encode/decode. |
| 278 | P3 | Performance | Hot path | Giảm allocation trong event construction. |
| 279 | P3 | Performance | Hot path | Reuse buffers an toàn trong serializers. |
| 280 | P3 | Performance | Hot path | Compression tuning cho NATS payload nếu payload lớn. |
| 281 | P3 | Performance | Hot path | Message size guardrail. |
| 282 | P3 | Performance | Hot path | Large transaction handling/backpressure. |
| 283 | P3 | Performance | Hot path | Parallelize per table nếu ordering cho phép. |
| 284 | P3 | Performance | Hot path | Adaptive worker scaling theo lag. |
| 285 | P3 | Performance | Hot path | Adaptive sink concurrency theo error rate. |
| 286 | P3 | Reliability | HA | Leader election cho source readers. |
| 287 | P3 | Reliability | HA | Horizontal scaling workers theo partition assignment. |
| 288 | P3 | Reliability | HA | NATS cluster replicas config. |
| 289 | P3 | Reliability | HA | Graceful failover source reader. |
| 290 | P3 | Reliability | HA | Idempotent recovery after crash. |
| 291 | P3 | Reliability | Guardrails | Refuse start nếu Postgres wal_level chưa đúng. |
| 292 | P3 | Reliability | Guardrails | Refuse start nếu MySQL binlog config chưa đúng. |
| 293 | P3 | Reliability | Guardrails | Alert replication slot bloat. |
| 294 | P3 | Reliability | Guardrails | Alert binlog purge risk. |
| 295 | P3 | Reliability | Guardrails | Auto-pause source khi sink outage quá lâu. |
| 296 | P3 | Reliability | Guardrails | Alert nếu DLQ rate tăng đột biến. |
| 307 | P3 | Security | Supply chain | Enable dependency scanning. |
| 308 | P3 | Security | Supply chain | Enable secret scanning. |
| 309 | P3 | Security | Supply chain | Generate SBOM. |
| 310 | P3 | Security | Supply chain | Container vuln scan. |
| 311 | P3 | Extensibility | Connectors | Add Kafka source/sink. |
| 317 | P3 | Extensibility | Connectors | Certification: inserts/updates/deletes. |
| 318 | P3 | Extensibility | Connectors | Certification: composite primary keys. |
| 319 | P3 | Extensibility | Connectors | Certification: DDL/schema evolution. |
| 320 | P3 | Extensibility | Connectors | Certification: high-volume workload. |
| 321 | P3 | Extensibility | Connectors | Certification: failover/reconnect. |



