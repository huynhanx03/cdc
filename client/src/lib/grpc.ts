import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';

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
  uptime: number;
}

export interface SourceConfig {
  type: string;
  host: string;
  port: number;
  database: string;
  tables: string[];
  instance_id?: string;
}

export interface SinkConfig {
  type: string;
  url: string[];
  index_prefix?: string;
  instance_id?: string;
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

// ... existing code ...

export async function addSource(source: SourceConfig): Promise<string> {
  return new Promise((resolve, reject) => {
    client.AddSource({ source }, (err: grpc.ServiceError, response: any) => {
      if (err) return reject(err);
      resolve(response.instance_id);
    });
  });
}

export async function removeSource(instance_id: string): Promise<boolean> {
  return new Promise((resolve, reject) => {
    client.RemoveSource({ instance_id }, (err: grpc.ServiceError, response: any) => {
      if (err) return reject(err);
      resolve(response.success);
    });
  });
}

export async function addSink(sink: SinkConfig): Promise<string> {
  return new Promise((resolve, reject) => {
    client.AddSink({ sink }, (err: grpc.ServiceError, response: any) => {
      if (err) return reject(err);
      resolve(response.instance_id);
    });
  });
}

export async function removeSink(instance_id: string): Promise<boolean> {
  return new Promise((resolve, reject) => {
    client.RemoveSink({ instance_id }, (err: grpc.ServiceError, response: any) => {
      if (err) return reject(err);
      resolve(response.success);
    });
  });
}

export interface PartitionSummary {
  id: number;
  source_id: string;
  total_messages: number;
  last_sequence: number;
}

export interface ListPartitionsResponse {
  partitions: PartitionSummary[];
}

export interface GetMessagesResponse {
  messages: any[];
  total_count: number;
  ack_floor: number;
  pending_count: number;
}

export async function listPartitions(): Promise<ListPartitionsResponse> {
  return new Promise((resolve, reject) => {
    client.ListPartitions({}, (err: grpc.ServiceError, response: any) => {
      if (err) return reject(err);
      resolve(response);
    });
  });
}

export async function getMessages(source_id?: string, offset?: number, limit?: number, status?: number): Promise<GetMessagesResponse> {
  return new Promise((resolve, reject) => {
    client.GetMessages({ source_id, offset, limit, status }, (err: grpc.ServiceError, response: any) => {
      if (err) return reject(err);
      resolve(response);
    });
  });
}

const PROTO_PATH = path.resolve(process.cwd(), '../api/proto/v1/cdc.proto');

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

export async function getStats(): Promise<GetStatsResponse> {
  return new Promise((resolve, reject) => {
    client.GetStats({}, (err: grpc.ServiceError, response: any) => {
      if (err) return reject(err);
      resolve(response);
    });
  });
}

export async function healthCheck(): Promise<HealthCheckResponse> {
  return new Promise((resolve, reject) => {
    client.HealthCheck({}, (err: grpc.ServiceError, response: any) => {
      if (err) return reject(err);
      resolve(response);
    });
  });
}

export async function getConfig(): Promise<GetConfigResponse> {
  return new Promise((resolve, reject) => {
    client.GetConfig({}, (err: grpc.ServiceError, response: any) => {
      if (err) return reject(err);
      resolve(response);
    });
  });
}
