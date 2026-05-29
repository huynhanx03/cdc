/** All REST endpoints — mirrors the proto service definition. */
export const ENDPOINTS = {
  // System
  health: '/api/v1/health',
  stats: '/api/v1/stats',
  dashboard: '/api/v1/dashboard',

  // Sources CRUD + Test
  sources: '/api/v1/sources',
  sourceById: (id: string) => `/api/v1/sources/${id}` as const,
  testSource: '/api/v1/test/source',
  discoverTables: (id: string) => `/api/v1/discover/tables/${id}` as const,

  // Sinks CRUD + Test
  sinks: '/api/v1/sinks',
  sinkById: (id: string) => `/api/v1/sinks/${id}` as const,
  testSink: '/api/v1/test/sink',
  discoverSinkTables: (id: string) => `/api/v1/discover/sink-tables/${id}` as const,

  // Flows CRUD + Lifecycle
  flows: '/api/v1/flows',
  flowById: (id: string) => `/api/v1/flows/${id}` as const,
  flowPause: (id: string) => `/api/v1/flows/${id}/pause` as const,
  flowResume: (id: string) => `/api/v1/flows/${id}/resume` as const,
  flowStats: (id: string) => `/api/v1/flows/${id}/stats` as const,

  // Explorer
  explorerOverview: '/api/v1/explorer/overview',
  topics: '/api/v1/topics',
  topicDetail: (topic: string) => `/api/v1/topics/${encodeURIComponent(topic)}` as const,
  consumers: '/api/v1/consumers',
  consumerDetail: (consumer: string) => `/api/v1/consumers/${encodeURIComponent(consumer)}` as const,
  partitions: '/api/v1/partitions',
  partitionDetail: (topic: string, partition: string) =>
    `/api/v1/topics/${encodeURIComponent(topic)}/partitions/${encodeURIComponent(partition)}` as const,
  partitionMessages: (topic: string, partition: string) =>
    `/api/v1/topics/${encodeURIComponent(topic)}/partitions/${encodeURIComponent(partition)}/messages` as const,
  messageDetail: (topic: string, partition: string, sequence: number | string) =>
    `/api/v1/topics/${encodeURIComponent(topic)}/partitions/${encodeURIComponent(partition)}/messages/${sequence}` as const,
  messages: '/api/v1/messages',
  consumer: '/api/v1/consumer',

  // DLQ
  dlqMessages: '/api/v1/dlq/messages',
  dlqPreview: '/api/v1/dlq/reprocess/preview',
  dlqReprocess: '/api/v1/dlq/reprocess',
} as const;
