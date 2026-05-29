import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { api } from '@/lib/api/client';
import { ENDPOINTS } from '@/lib/api/endpoints';
import { POLLING } from '@/config/constants';
import type {
  ListTopicsResponse,
  ListPartitionsResponse,
  ListMessagesResponse,
  ListDLQMessagesResponse,
  ListConsumersResponse,
  ReprocessDLQResponse,
  ExplorerOverviewResponse,
  TopicDetailResponse,
  PartitionDetailResponse,
  ConsumerDetailResponse,
  ExplorerMessageFilters,
  DLQDryRunRequest,
  DLQDryRunResponse,
  ReprocessDLQRequest,
} from '@/types/api';

/** Query key factory for explorer queries. */
export const explorerKeys = {
  overview: () => ['explorer', 'overview'] as const,
  topics: (page?: number) => ['topics', page] as const,
  topicDetail: (topic: string) => ['explorer', 'topic', topic] as const,
  consumers: (page?: number) => ['consumers', page] as const,
  consumerDetail: (consumer: string) => ['explorer', 'consumer', consumer] as const,
  partitions: (topic: string, page?: number) => ['partitions', topic, page] as const,
  partitionDetail: (topic: string, partition: string) => ['explorer', 'partition', topic, partition] as const,
  partitionMessages: (topic: string, partition: string, filters: ExplorerMessageFilters) =>
    ['explorer', 'partitionMessages', topic, partition, filters] as const,
  messages: (params: Record<string, unknown>) => ['messages', params] as const,
  dlqMessages: (page?: number) => ['dlqMessages', page] as const,
};

export function useExplorerOverview() {
  return useQuery({
    queryKey: explorerKeys.overview(),
    queryFn: () => api.get<ExplorerOverviewResponse>(ENDPOINTS.explorerOverview),
    refetchInterval: POLLING.PARTITIONS,
  });
}

/** Fetches topic list with 10s polling. */
export function useTopics(page = 1, limit = 25) {
  return useQuery({
    queryKey: explorerKeys.topics(page),
    queryFn: () =>
      api.get<ListTopicsResponse>(ENDPOINTS.topics, {
        'pagination.page': page,
        'pagination.limit': limit,
      }),
    refetchInterval: POLLING.TOPICS,
  });
}

/** Fetches partitions for a specific topic with 5s polling. */
export function usePartitions(topic: string, page = 1, limit = 25) {
  return useQuery({
    queryKey: explorerKeys.partitions(topic, page),
    queryFn: () =>
      api.get<ListPartitionsResponse>(ENDPOINTS.partitions, {
        topic,
        'pagination.page': page,
        'pagination.limit': limit,
      }),
    enabled: !!topic,
    refetchInterval: POLLING.PARTITIONS,
  });
}

export function useTopicDetail(topic: string) {
  return useQuery({
    queryKey: explorerKeys.topicDetail(topic),
    queryFn: () => api.get<TopicDetailResponse>(ENDPOINTS.topicDetail(topic)),
    enabled: !!topic,
    refetchInterval: POLLING.PARTITIONS,
  });
}

export function usePartitionDetail(topic: string, partition: string) {
  return useQuery({
    queryKey: explorerKeys.partitionDetail(topic, partition),
    queryFn: () => api.get<PartitionDetailResponse>(ENDPOINTS.partitionDetail(topic, partition)),
    enabled: !!topic && !!partition,
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
    enabled: !!topic && !!partition,
    refetchInterval: POLLING.MESSAGES,
  });
}

/** Fetches flow consumers with lag/pending summary. */
export function useConsumers(page = 1, limit = 25) {
  return useQuery({
    queryKey: explorerKeys.consumers(page),
    queryFn: () =>
      api.get<ListConsumersResponse>(ENDPOINTS.consumers, {
        'pagination.page': page,
        'pagination.limit': limit,
      }),
    refetchInterval: POLLING.PARTITIONS,
  });
}

export function useConsumerDetail(consumer: string) {
  return useQuery({
    queryKey: explorerKeys.consumerDetail(consumer),
    queryFn: () => api.get<ConsumerDetailResponse>(ENDPOINTS.consumerDetail(consumer)),
    enabled: !!consumer,
    refetchInterval: POLLING.PARTITIONS,
  });
}

/** Fetches messages with filtering — manual refresh only (no auto-polling). */
export function useMessages(params: {
  status?: number;
  topic?: string;
  partition?: string;
  page?: number;
  limit?: number;
}) {
  const { status, topic, partition, page = 1, limit = 25 } = params;
  return useQuery({
    queryKey: explorerKeys.messages(params),
    queryFn: () =>
      api.get<ListMessagesResponse>(ENDPOINTS.messages, {
        status,
        topic,
        partition,
        'pagination.page': page,
        'pagination.limit': limit,
      }),
    refetchInterval: POLLING.MESSAGES, // 0 = manual only
  });
}

/** Fetches dead-letter queue messages. */
export function useDLQMessages(page = 1, limit = 25) {
  return useQuery({
    queryKey: explorerKeys.dlqMessages(page),
    queryFn: () =>
      api.get<ListDLQMessagesResponse>(ENDPOINTS.dlqMessages, {
        'pagination.page': page,
        'pagination.limit': limit,
      }),
    refetchInterval: POLLING.MESSAGES,
  });
}

/** Mutates to trigger DLQ reprocessing. */
export function useReprocessDLQ() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (request?: ReprocessDLQRequest) => api.post<ReprocessDLQResponse>(ENDPOINTS.dlqReprocess, request ?? {}),
    onSuccess: () => {
      // Invalidate messages since they might get cleared or status updated
      qc.invalidateQueries({ queryKey: ['messages'] });
      qc.invalidateQueries({ queryKey: ['dlqMessages'] });
      qc.invalidateQueries({ queryKey: ['explorer'] });
    },
  });
}

export function useDLQPreview() {
  return useMutation({
    mutationFn: (request: DLQDryRunRequest) => api.post<DLQDryRunResponse>(ENDPOINTS.dlqPreview, request),
  });
}
