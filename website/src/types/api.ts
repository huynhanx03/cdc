/**
 * TypeScript interfaces mirroring proto message definitions.
 * Single source of truth for all API response/request types.
 */

// ─── Health ──────────────────────────────────────────────────────────

export interface HealthCheckResponse {
  status: string;
  uptime?: number | string; // seconds since start; int64 may be encoded as string by gRPC gateway
  version?: string;
}

// ─── Source ──────────────────────────────────────────────────────────

export interface SourceConfig {
  type: 'postgres' | 'mysql';
  host: string;
  port: number;
  username?: string;
  password?: string;
  database: string;
  instance_id: string;
  name?: string;
}

export interface ListSourcesResponse {
  sources: SourceConfig[];
}

// ─── Sink ────────────────────────────────────────────────────────────

export interface SinkConfig {
  type: 'postgres' | 'mysql' | 'elasticsearch' | 'clickhouse';
  host?: string;
  port?: number;
  username?: string;
  password?: string;
  database?: string;
  instance_id: string;
  name?: string;
  url?: string[];       // elasticsearch
  api_key?: string;     // elasticsearch
  index_prefix?: string; // elasticsearch
}

export interface ListSinksResponse {
  sinks: SinkConfig[];
}

// ─── Flow ────────────────────────────────────────────────────────────

export type FlowStatus = 'FLOW_STATUS_RUNNING' | 'FLOW_STATUS_PAUSED' | 'FLOW_STATUS_ERROR' | 'FLOW_STATUS_UNSPECIFIED';

export interface FlowOptions {
  batch_size: number;
  flush_interval_ms: number;
  filter_expression: string;
  partition_count: number;
}

export interface ColumnMapping {
  source_column: string;
  sink_column: string;
  source_type: string;
  sink_type: string;
  enabled: boolean;
}

export interface FlowConfig {
  flow_id: string;
  name: string;
  source_id: string;
  sink_id: string;
  source_table: string;
  sink_table: string;
  status: FlowStatus;
  created_at: number;
  updated_at: number;
  options?: FlowOptions;
  column_mappings?: ColumnMapping[];
}

export interface ListFlowsResponse {
  flows: FlowConfig[];
}

export interface CreateFlowRequest {
  name?: string;
  source_id: string;
  sink_id: string;
  source_table: string;
  sink_table: string;
  options?: Partial<FlowOptions>;
  column_mappings?: ColumnMapping[];
}

export interface CreateFlowResponse {
  flow_id: string;
  status: FlowStatus;
}

export interface GetFlowStatsResponse {
  events_per_second: number;
  replication_lag_ms: number;
  total_events_processed: number;
  running_workers: number;
  pool_capacity: number;
  worker_utilization: number;
  failure_count: number;
  dlq_count: number;
  filtered_count: number;
  last_error: string;
}

// ─── Dashboard Aggregates ────────────────────────────────────────────

export interface DashboardSystemInventoryResponse {
  sources_count: number;
  sinks_count: number;
  flows_count: number;
}

export interface DashboardLiveTelemetryResponse {
  throughput: number;
  latency_p99: number;
  active_workers: number;
  channel_utilization: number;
  nats_healthy: boolean;
  error_rate: number;
  total_synced_events: number;
  failure_count: number;
}

export interface DashboardSummaryResponse {
  inventory?: DashboardSystemInventoryResponse;
  telemetry?: DashboardLiveTelemetryResponse;
}

// ─── Component Stats ─────────────────────────────────────────────────

/** Stats for a single source or sink component. */
export interface ComponentStats {
  success_count: number;
  failure_count: number;
  last_error: string;
  partition_lag: Record<number, number>;
  last_event_at: number;
  active_flows: number;
  throughput: number;
  error_rate: number;
  avg_latency_ms: number;
}

/** Aggregated stats response keyed by component instance ID. */
export interface GetStatsResponse {
  source_stats: Record<string, ComponentStats>;
  sink_stats: Record<string, ComponentStats>;
}

// ─── Connection Testing ──────────────────────────────────────────────

export interface TestConnectionResponse {
  success: boolean;
  message: string;
  latency_ms?: number;
  latencyMs?: number;
}

// ─── Table Discovery ─────────────────────────────────────────────────

export interface ColumnInfo {
  name: string;
  type: string;
  is_primary_key: boolean;
  is_nullable: boolean;
}

export interface TableInfo {
  schema: string;
  name: string;
  columns: ColumnInfo[];
}

export interface DiscoverTablesResponse {
  tables: TableInfo[];
}

// ─── Explorer ────────────────────────────────────────────────────────

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
  message_count?: number;
  partition_count?: number;
  consumer_count?: number;
  dlq_count?: number;
  pending_count?: number;
  ack_pending_count?: number;
  first_sequence?: number;
  latest_sequence?: number;
  latest_event_at?: string | number;
  health?: ExplorerHealthStatus;
  partial?: boolean;
}

export interface PartitionSummary {
  id: string;
  message_count?: number;
  topic: string;
  pending_count?: number;
  ack_pending_count?: number;
  first_sequence?: number;
  latest_sequence?: number;
  latest_event_at?: string | number;
  health?: ExplorerHealthStatus;
  partial?: boolean;
}

export interface MessageItem {
  sequence: number;
  timestamp: string | number;
  subject: string;
  data: string;
  headers: Record<string, string>;
  op?: string;
  source_id?: string;
  schema?: string;
  table?: string;
  partition?: string;
  key?: string;
  payload_size?: number;
  header_count?: number;
  nats_msg_id?: string;
  reprocessed_from?: string;
  markers?: string[];
}

export interface DLQMessageSummary {
  dlq_id: string;
  original_subject: string;
  reason: string;
  error_class?: string;
  timestamp?: string | number;
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
  summary?: TopicSummary;
  partitions: PartitionSummary[];
  scan?: ExplorerScanMetadata;
}

export interface PartitionDetailResponse {
  summary?: PartitionSummary;
  recent_messages?: MessageItem[];
  checkpoints: CheckpointContext[];
  scan?: ExplorerScanMetadata;
}

export interface ConsumerSummary {
  name: string;
  filter_subjects?: string[];
  num_pending?: number;
  num_ack_pending?: number;
  delivered_stream_seq?: number;
  ack_floor_stream_seq?: number;
  lag_messages?: number;
  replay_risk?: string;
  last_delivered_at?: string | number;
  last_ack_at?: string | number;
}

export interface ConsumerDetailResponse {
  summary?: ConsumerSummary;
  topics: TopicSummary[];
  partitions: PartitionSummary[];
  recent_messages: MessageItem[];
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

export interface DLQMessage extends MessageItem {
  dlq_id?: string;
  reason?: string;
  original_subject?: string;
  error_class?: string;
  duplicate_risk?: DLQDuplicateRisk;
  blocked_reason?: string;
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
  confirm_token?: string;
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

export interface OffsetPaginationResponse {
  total_rows: number;
  page: number;
  limit: number;
  has_next: boolean;
  has_prev: boolean;
}

export interface ListTopicsResponse {
  data: TopicSummary[];
  pagination?: OffsetPaginationResponse;
}

export interface ListPartitionsResponse {
  data: PartitionSummary[];
  pagination?: OffsetPaginationResponse;
}

export interface ListMessagesResponse {
  data: MessageItem[];
  total_count: number;
  pagination?: OffsetPaginationResponse;
  scan?: ExplorerScanMetadata;
}

export interface ListDLQMessagesResponse {
  data: DLQMessage[];
  pagination?: OffsetPaginationResponse;
  scan?: ExplorerScanMetadata;
}

export interface ListConsumersResponse {
  data: ConsumerSummary[];
  pagination?: OffsetPaginationResponse;
}
