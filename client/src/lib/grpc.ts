import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';

// ── Proto loader ─────────────────────────────────────────────────────────────

const PROTO_PATH = path.resolve(process.cwd(), '../api/proto/v1/cdc.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: true,
  includeDirs: [path.resolve(process.cwd(), '../third_party')],
});

const protoDescriptor = grpc.loadPackageDefinition(packageDefinition) as any;
const cdcPackage = protoDescriptor.cdc.v1;

const target = process.env.GRPC_API_URL || 'localhost:9090';

// Singleton client — avoid creating a new one on every hot reload
const globalForGrpc = global as unknown as { cdcClient: any };

export const client =
  globalForGrpc.cdcClient ||
  new cdcPackage.CDCService(target, grpc.credentials.createInsecure());

if (process.env.NODE_ENV !== 'production') {
  globalForGrpc.cdcClient = client;
}

// ── Shared Types ─────────────────────────────────────────────────────────────

export interface ComponentStats {
  success_count: number;
  failure_count: number;
  last_error: string;
}

export interface GetStatsResponse {
  source_stats: Record<string, ComponentStats>;
  sink_stats: Record<string, ComponentStats>;
}

export interface HealthCheckResponse {
  status: string;
  version: string;
  uptime: number; // seconds
}

export interface SourceConfig {
  type: string;
  host: string;
  port: number;
  username?: string;
  password?: string;
  database: string;
  tables: string[];
  slot_name?: string;
  publication_name?: string;
  instance_id?: string;
  name?: string;
  topic?: string;
}

export interface SinkConfig {
  type: string;
  url: string[];
  username?: string;
  password?: string;
  index_prefix?: string;
  index?: string;
  index_mapping?: Record<string, string>;
  batch_size?: number;
  flush_interval_ms?: number;
  max_retries?: number;
  retry_base_ms?: number;
  api_key?: string;
  instance_id?: string;
  name?: string;
  topic?: string;
}

export interface AppConfig {
  name: string;
  log_mode: string;
  sources: SourceConfig[];
  sinks: SinkConfig[];
}

export interface GetConfigResponse {
  config: AppConfig;
  available_sources: string[];
  available_sinks: string[];
}

// ── Pagination Types ─────────────────────────────────────────────────────────

export interface PaginationResponse {
  total_rows: number;
  limit: number;
  page: number;
  has_next: boolean;
  has_prev: boolean;
}

// ── Explorer Types ────────────────────────────────────────────────────────────

export interface TopicSummary {
  name: string;
  message_count: number;
  partition_count: number;
}

export interface PartitionSummary {
  id: string;
  message_count: number;
  topic: string;
}

export interface MessageItem {
  sequence: number;
  timestamp: string; // ms since epoch as string
  subject: string;
  data: string; // base64 encoded bytes from grpc-gateway
  headers: Record<string, string>;
}

export interface ListTopicsResponse {
  data: TopicSummary[];
  pagination: PaginationResponse;
}

export interface ListPartitionsResponse {
  data: PartitionSummary[];
  pagination: PaginationResponse;
}

export interface ListMessagesResponse {
  data: MessageItem[];
  total_count: number;
  pagination: PaginationResponse;
}

export interface ConsumerInfoResponse {
  ack_floor: number;
  pending_count: number;
}

// ── gRPC wrappers ─────────────────────────────────────────────────────────────

function call<T>(method: string, req: unknown): Promise<T> {
  return new Promise((resolve, reject) => {
    client[method](req, (err: grpc.ServiceError, response: T) => {
      if (err) return reject(err);
      resolve(response);
    });
  });
}

// Dashboard
export const getStats = () => call<GetStatsResponse>('GetStats', {});
export const healthCheck = () => call<HealthCheckResponse>('HealthCheck', {});
export const getConfig = () => call<GetConfigResponse>('GetConfig', {});

// Sources / Sinks
export const addSource = (source: SourceConfig) =>
  call<{ instance_id: string }>('AddSource', { source });
export const removeSource = (instance_id: string) =>
  call<{ success: boolean }>('RemoveSource', { instance_id });
export const addSink = (sink: SinkConfig) =>
  call<{ instance_id: string }>('AddSink', { sink });
export const removeSink = (instance_id: string) =>
  call<{ success: boolean }>('RemoveSink', { instance_id });

// Topics
export const listTopics = (limit = 20, page = 1) =>
  call<ListTopicsResponse>('ListTopics', {
    pagination: { limit, page },
  });

// Partitions
export const listPartitions = (topic: string, limit = 50, page = 1) =>
  call<ListPartitionsResponse>('ListPartitions', {
    topic,
    pagination: { limit, page },
  });

// Messages
// status: 0=ALL, 1=SENT, 2=UNSENT
export const listMessages = (params: {
  status?: number;
  topic?: string;
  partition?: string;
  limit?: number;
  page?: number;
}) =>
  call<ListMessagesResponse>('ListMessages', {
    status: params.status ?? 0,
    topic: params.topic ?? '',
    partition: params.partition ?? '',
    pagination: {
      limit: params.limit ?? 20,
      page: params.page ?? 1,
    },
  });

// Consumer
export const getConsumerInfo = (consumer_name = '') =>
  call<ConsumerInfoResponse>('GetConsumerInfo', { consumer_name });
