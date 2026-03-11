import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';

// Define expected interfaces matching protobuf
export interface TopicSummary {
  name: string;
  partitionCount: number;
  messageCount: number;
}

export interface PartitionSummary {
  id: number;
  sizeBytes: number;
  segmentCount: number;
  earliestOffset: number;
  latestOffset: number;
}

export interface SegmentInfo {
  baseOffset: number;
  sizeBytes: number;
}

export interface TopicDetail {
  name: string;
  partitionCount: number;
  messageCount: number;
  partitions: PartitionSummary[];
}

export interface PartitionDetail {
  id: number;
  sizeBytes: number;
  segmentCount: number;
  earliestOffset: number;
  latestOffset: number;
  segments: SegmentInfo[];
}

export interface MessageItem {
  offset: number;
  timestamp: number;
  key: string;
  value: Buffer | string;
}

export interface StatsResponse {
  totalEnqueued: number;
  totalDequeued: number;
  pending: number;
  segmentsCount: number;
  totalSizeMb: number;
  partitionCount: number;
}

export interface HealthCheckResponse {
  status: string;
  version: string;
  uptime: number;
}

export interface SourceConfig {
  type: string;
  host: string;
  port: number;
  database: string;
  tables: string[];
}

export interface SinkConfig {
  type: string;
  url: string[];
  indexPrefix: string;
}

export interface AppConfig {
  name: string;
  logMode: string;
  source: SourceConfig;
  sinks: SinkConfig[];
}

export interface GetConfigResponse {
  config: AppConfig;
  availableSources: string[];
  availableSinks: string[];
}

const PROTO_PATH = path.resolve(process.cwd(), 'proto/cdc.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: true,
  includeDirs: [
    path.resolve(process.cwd(), '../third_party')
  ],
});

const protoDescriptor = grpc.loadPackageDefinition(packageDefinition) as any;
const cdcPackage = protoDescriptor.cdc.v1;

const target = process.env.GRPC_API_URL || 'localhost:9090';

// In Next.js App Router, we avoid creating a new client on every hot reload in development
const globalForGrpc = global as unknown as { cdcClient: any };

export const client = globalForGrpc.cdcClient || new cdcPackage.CDCService(
  target,
  grpc.credentials.createInsecure()
);

if (process.env.NODE_ENV !== 'production') {
  globalForGrpc.cdcClient = client;
}

// Promisified wrappers for gRPC methods

export async function getStats(): Promise<StatsResponse> {
  return new Promise((resolve, reject) => {
    client.GetStats({}, (err: grpc.ServiceError, response: any) => {
      if (err) return reject(err);
      resolve({
        totalEnqueued: response.total_enqueued,
        totalDequeued: response.total_dequeued,
        pending: response.pending,
        segmentsCount: response.segments_count,
        totalSizeMb: response.total_size_mb,
        partitionCount: response.partition_count,
      });
    });
  });
}

export async function healthCheck(): Promise<HealthCheckResponse> {
  return new Promise((resolve, reject) => {
    client.HealthCheck({}, (err: grpc.ServiceError, response: any) => {
      if (err) return reject(err);
      resolve({
        status: response.status,
        version: response.version,
        uptime: response.uptime,
      });
    });
  });
}

export async function listTopics(): Promise<{ topics: TopicSummary[] }> {
  return new Promise((resolve, reject) => {
    client.ListTopics({}, (err: grpc.ServiceError, response: any) => {
      if (err) return reject(err);
      resolve({
        topics: (response.topics || []).map((t: any) => ({
          name: t.name,
          partitionCount: t.partition_count,
          messageCount: t.message_count,
        })),
      });
    });
  });
}

export async function getTopic(name: string): Promise<TopicDetail> {
  return new Promise((resolve, reject) => {
    client.GetTopic({ name }, (err: grpc.ServiceError, response: any) => {
      if (err) return reject(err);
      resolve({
        name: response.name,
        partitionCount: response.partition_count,
        messageCount: response.message_count,
        partitions: (response.partitions || []).map((p: any) => ({
          id: p.id,
          sizeBytes: p.size_bytes,
          segmentCount: p.segment_count,
          earliestOffset: p.earliest_offset,
          latestOffset: p.latest_offset,
        })),
      });
    });
  });
}

export async function listPartitions(topic?: string): Promise<{ partitions: PartitionSummary[] }> {
  return new Promise((resolve, reject) => {
    client.ListPartitions({ topic: topic || '' }, (err: grpc.ServiceError, response: any) => {
      if (err) return reject(err);
      resolve({
        partitions: (response.partitions || []).map((p: any) => ({
          id: p.id,
          sizeBytes: p.size_bytes,
          segmentCount: p.segment_count,
          earliestOffset: p.earliest_offset,
          latestOffset: p.latest_offset,
        })),
      });
    });
  });
}

export async function getPartition(id: number, topic?: string): Promise<PartitionDetail> {
  return new Promise((resolve, reject) => {
    client.GetPartition({ id, topic: topic || '' }, (err: grpc.ServiceError, response: any) => {
      if (err) return reject(err);
      resolve({
        id: response.id,
        sizeBytes: response.size_bytes,
        segmentCount: response.segment_count,
        earliestOffset: response.earliest_offset,
        latestOffset: response.latest_offset,
        segments: (response.segments || []).map((s: any) => ({
          baseOffset: s.base_offset,
          sizeBytes: s.size_bytes,
        })),
      });
    });
  });
}

export async function getMessages(partitionId?: number, offset?: number, limit?: number): Promise<{ messages: MessageItem[] }> {
  return new Promise((resolve, reject) => {
    const payload: any = {
      offset: offset || 0,
      limit: limit || 50,
    };
    if (partitionId !== undefined && partitionId !== null) {
      payload.partition_id = partitionId;
    }
    client.GetMessages(payload, (err: grpc.ServiceError, response: any) => {
      if (err) return reject(err);
      resolve({
        messages: (response.messages || []).map((m: any) => ({
          offset: m.offset,
          timestamp: m.timestamp,
          key: m.key,
          value: m.value, // Buffer from gRPC, can use base64 or string parsing later
        })),
      });
    });
  });
}

export async function getConfig(): Promise<GetConfigResponse> {
  return new Promise((resolve, reject) => {
    client.GetConfig({}, (err: grpc.ServiceError, response: any) => {
      if (err) return reject(err);
      resolve({
        config: {
          name: response.config.name,
          logMode: response.config.log_mode,
          source: {
            type: response.config.source.type,
            host: response.config.source.host,
            port: response.config.source.port,
            database: response.config.source.database,
            tables: response.config.source.tables || [],
          },
          sinks: (response.config.sinks || []).map((s: any) => ({
            type: s.type,
            url: s.url || [],
            indexPrefix: s.index_prefix,
          })),
        },
        availableSources: response.available_sources || [],
        availableSinks: response.available_sinks || [],
      });
    });
  });
}
