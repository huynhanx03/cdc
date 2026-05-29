import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { api } from '@/lib/api/client';
import { ENDPOINTS } from '@/lib/api/endpoints';
import { POLLING } from '@/config/constants';
import { SINK_CONNECTOR_TYPES, SOURCE_CONNECTOR_TYPES } from '@/config/connectors';
import type {
  SourceConfig,
  SinkConfig,
  TestConnectionResponse,
  DiscoverTablesResponse,
  ListSourcesResponse,
  ListSinksResponse,
  ListFlowsResponse,
  FlowConfig,
  GetFlowStatsResponse,
  GetStatsResponse,
} from '@/types/api';

// ─── Query Keys ──────────────────────────────────────────────────────

export const managerKeys = {
  sources: ['sources'] as const,
  sinks: ['sinks'] as const,
  flows: ['flows'] as const,
  flow: (id: string) => ['flow', id] as const,
  flowStats: (id: string) => ['flowStats', id] as const,
  stats: ['stats'] as const,
  sourceTables: (id: string) => ['sourceTables', id] as const,
  sinkTables: (id: string) => ['sinkTables', id] as const,
};

/** Composite runtime config assembled from the backend list endpoints. */
export function useConfig() {
  const sourcesQuery = useSources();
  const sinksQuery = useSinks();
  const flowsQuery = useFlows();

  return {
    data: {
      config: {
        sources: sourcesQuery.data?.sources ?? [],
        sinks: sinksQuery.data?.sinks ?? [],
        flows: flowsQuery.data?.flows ?? [],
      },
      available_sources: [...SOURCE_CONNECTOR_TYPES],
      available_sinks: [...SINK_CONNECTOR_TYPES],
    },
    isLoading:
      sourcesQuery.isLoading || sinksQuery.isLoading || flowsQuery.isLoading,
    isFetching:
      sourcesQuery.isFetching || sinksQuery.isFetching || flowsQuery.isFetching,
    refetch: () =>
      Promise.all([
        sourcesQuery.refetch(),
        sinksQuery.refetch(),
        flowsQuery.refetch(),
      ]),
  };
}

/** Fetches aggregate source/sink component stats. */
export function useStats() {
  return useQuery({
    queryKey: managerKeys.stats,
    queryFn: () => api.get<GetStatsResponse>(ENDPOINTS.stats),
    refetchInterval: POLLING.STATS,
  });
}

// ─── Sources ─────────────────────────────────────────────────────────

/** Fetches all sources. */
export function useSources() {
  return useQuery({
    queryKey: managerKeys.sources,
    queryFn: () => api.get<ListSourcesResponse>(ENDPOINTS.sources),
    refetchInterval: POLLING.FLOWS,
  });
}

/** Add a new source connector. */
export function useAddSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (source: Partial<SourceConfig>) =>
      api.post<{ instance_id: string }>(ENDPOINTS.sources, { source }),
    onSuccess: () => qc.invalidateQueries({ queryKey: managerKeys.sources }),
  });
}

/** Update an existing source connector. */
export function useUpdateSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (source: SourceConfig) =>
      api.put<{ success: boolean }>(
        ENDPOINTS.sourceById(source.instance_id),
        { source },
      ),
    onSuccess: () => qc.invalidateQueries({ queryKey: managerKeys.sources }),
  });
}

/** Remove a source connector by instance ID. */
export function useRemoveSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (instanceId: string) =>
      api.del<{ success: boolean }>(ENDPOINTS.sourceById(instanceId)),
    onSuccess: () => qc.invalidateQueries({ queryKey: managerKeys.sources }),
  });
}

/** Test a source connection (returns success/message/latency without throwing). */
export function useTestSourceConnection() {
  return useMutation({
    mutationFn: (source: Partial<SourceConfig>) =>
      api.post<TestConnectionResponse>(ENDPOINTS.testSource, { source }),
  });
}

/** Discover tables for a registered source. */
export function useDiscoverSourceTables(sourceId: string) {
  return useQuery({
    queryKey: managerKeys.sourceTables(sourceId),
    queryFn: () =>
      api.get<DiscoverTablesResponse>(ENDPOINTS.discoverTables(sourceId)),
    enabled: !!sourceId,
  });
}

// ─── Sinks ───────────────────────────────────────────────────────────

/** Fetches all sinks. */
export function useSinks() {
  return useQuery({
    queryKey: managerKeys.sinks,
    queryFn: () => api.get<ListSinksResponse>(ENDPOINTS.sinks),
    refetchInterval: POLLING.FLOWS,
  });
}

/** Add a new sink connector. */
export function useAddSink() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (sink: Partial<SinkConfig>) =>
      api.post<{ instance_id: string }>(ENDPOINTS.sinks, { sink }),
    onSuccess: () => qc.invalidateQueries({ queryKey: managerKeys.sinks }),
  });
}

/** Update an existing sink connector. */
export function useUpdateSink() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (sink: SinkConfig) =>
      api.put<{ success: boolean }>(
        ENDPOINTS.sinkById(sink.instance_id),
        { sink },
      ),
    onSuccess: () => qc.invalidateQueries({ queryKey: managerKeys.sinks }),
  });
}

/** Remove a sink connector by instance ID. */
export function useRemoveSink() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (instanceId: string) =>
      api.del<{ success: boolean }>(ENDPOINTS.sinkById(instanceId)),
    onSuccess: () => qc.invalidateQueries({ queryKey: managerKeys.sinks }),
  });
}

/** Test a sink connection (returns success/message/latency without throwing). */
export function useTestSinkConnection() {
  return useMutation({
    mutationFn: (sink: Partial<SinkConfig>) =>
      api.post<TestConnectionResponse>(ENDPOINTS.testSink, { sink }),
  });
}

/** Discover tables for a registered sink. */
export function useDiscoverSinkTables(sinkId: string) {
  return useQuery({
    queryKey: managerKeys.sinkTables(sinkId),
    queryFn: () =>
      api.get<DiscoverTablesResponse>(ENDPOINTS.discoverSinkTables(sinkId)),
    enabled: !!sinkId,
  });
}

// ─── Flows ───────────────────────────────────────────────────────────

/** Fetches all flows with polling. */
export function useFlows() {
  return useQuery({
    queryKey: managerKeys.flows,
    queryFn: () => api.get<ListFlowsResponse>(ENDPOINTS.flows),
    refetchInterval: POLLING.FLOWS,
  });
}

/** Fetches a single flow by ID. */
export function useFlow(flowId: string) {
  return useQuery({
    queryKey: managerKeys.flow(flowId),
    queryFn: () =>
      api.get<{ flow: FlowConfig }>(ENDPOINTS.flowById(flowId)),
    enabled: !!flowId,
  });
}

/** Create a new flow. */
export function useCreateFlow() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (payload: {
      name?: string;
      source_id: string;
      sink_id: string;
      source_table: string;
      sink_table: string;
      column_mappings?: Array<{
        source_column: string;
        sink_column: string;
        source_type: string;
        sink_type: string;
        enabled: boolean;
      }>;
      options?: {
        batch_size?: number;
        flush_interval_ms?: number;
        filter_expression?: string;
        partition_count?: number;
      };
    }) => api.post<{ flow_id: string; status: string }>(ENDPOINTS.flows, payload),
    onSuccess: () => qc.invalidateQueries({ queryKey: managerKeys.flows }),
  });
}

/** Update an existing flow. */
export function useUpdateFlow() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (payload: {
      flow_id: string;
      name?: string;
      source_table?: string;
      sink_table?: string;
      column_mappings?: Array<{
        source_column: string;
        sink_column: string;
        source_type: string;
        sink_type: string;
        enabled: boolean;
      }>;
      options?: {
        batch_size?: number;
        flush_interval_ms?: number;
        filter_expression?: string;
        partition_count?: number;
      };
    }) =>
      api.put<{ flow: FlowConfig }>(
        ENDPOINTS.flowById(payload.flow_id),
        payload,
      ),
    onSuccess: () => qc.invalidateQueries({ queryKey: managerKeys.flows }),
  });
}

/** Delete a flow by ID. */
export function useDeleteFlow() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (flowId: string) =>
      api.del<{ success: boolean }>(ENDPOINTS.flowById(flowId)),
    onSuccess: () => qc.invalidateQueries({ queryKey: managerKeys.flows }),
  });
}

/** Pause a running flow. */
export function usePauseFlow() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (flowId: string) =>
      api.post<{ status: string }>(ENDPOINTS.flowPause(flowId)),
    onSuccess: () => qc.invalidateQueries({ queryKey: managerKeys.flows }),
  });
}

/** Resume a paused flow. */
export function useResumeFlow() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (flowId: string) =>
      api.post<{ status: string }>(ENDPOINTS.flowResume(flowId)),
    onSuccess: () => qc.invalidateQueries({ queryKey: managerKeys.flows }),
  });
}

/** Fetches real-time stats for a single flow. */
export function useFlowStats(flowId: string) {
  return useQuery({
    queryKey: managerKeys.flowStats(flowId),
    queryFn: () =>
      api.get<GetFlowStatsResponse>(ENDPOINTS.flowStats(flowId)),
    enabled: !!flowId,
    refetchInterval: POLLING.FLOW_STATS,
  });
}
