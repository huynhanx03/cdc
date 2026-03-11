// ───────────────────────────────────────────────────────────────
//  CDC API Client — talks to the grpc-gateway REST proxy
// ───────────────────────────────────────────────────────────────

const API_BASE = "";

async function request<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API ${path} failed: ${res.status} — ${text}`);
  }
  return res.json() as Promise<T>;
}

// ── Types ────────────────────────────────────────────────────

export interface TopicSummary {
  name: string;
  partitionCount: number;
  messageCount: string; // uint64 comes as string from grpc-gateway
}

export interface PartitionSummary {
  id: number;
  sizeBytes: string;
  segmentCount: number;
  earliestOffset: string;
  latestOffset: string;
}

export interface TopicDetail {
  name: string;
  partitionCount: number;
  messageCount: string;
  partitions: PartitionSummary[];
}

export interface SegmentInfo {
  baseOffset: string;
  sizeBytes: string;
}

export interface PartitionDetail {
  id: number;
  sizeBytes: string;
  segmentCount: number;
  earliestOffset: string;
  latestOffset: string;
  segments: SegmentInfo[];
}

export interface MessageItem {
  offset: string;
  timestamp: string;
  key: string;
  value: string; // base64 encoded from grpc-gateway
}

export interface StatsResponse {
  totalEnqueued: string;
  totalDequeued: string;
  pending: string;
  segmentsCount: number;
  totalSizeMb: string;
  partitionCount: number;
}

export interface HealthCheckResponse {
  status: string;
  version: string;
  uptime: string;
}

// ── API Functions ────────────────────────────────────────────

export async function listTopics(): Promise<TopicSummary[]> {
  const data = await request<{ topics?: TopicSummary[] }>("/api/v1/topics");
  return data.topics || [];
}

export async function getTopic(name: string): Promise<TopicDetail> {
  return request<TopicDetail>(`/api/v1/topics/${encodeURIComponent(name)}`);
}

export async function listPartitions(topic?: string): Promise<PartitionSummary[]> {
  const q = topic ? `?topic=${encodeURIComponent(topic)}` : "";
  const data = await request<{ partitions?: PartitionSummary[] }>(`/api/v1/partitions${q}`);
  return data.partitions || [];
}

export async function getPartition(id: number): Promise<PartitionDetail> {
  return request<PartitionDetail>(`/api/v1/partitions/${id}`);
}

export async function getMessages(
  partitionId: number,
  offset = 0,
  limit = 50
): Promise<MessageItem[]> {
  const q = `?partition_id=${partitionId}&offset=${offset}&limit=${limit}`;
  const data = await request<{ messages?: MessageItem[] }>(`/api/v1/messages${q}`);
  return data.messages || [];
}

export async function getStats(): Promise<StatsResponse> {
  return request<StatsResponse>("/api/v1/stats");
}

export async function healthCheck(): Promise<HealthCheckResponse> {
  return request<HealthCheckResponse>("/api/v1/health");
}

// ── Helpers ──────────────────────────────────────────────────

/** Decode base64-encoded value from grpc-gateway */
export function decodeValue(b64: string): string {
  if (!b64) return "";
  try {
    return atob(b64);
  } catch {
    return b64;
  }
}

/** Format bytes to human-readable */
export function formatBytes(bytes: number | string): string {
  const b = typeof bytes === "string" ? parseInt(bytes, 10) : bytes;
  if (!b || b === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(b) / Math.log(1024));
  return `${(b / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

/** Format large number with commas */
export function formatNumber(n: number | string): string {
  const num = typeof n === "string" ? parseInt(n, 10) : n;
  if (isNaN(num)) return "0";
  return num.toLocaleString();
}
