import { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router-dom';
import {
  ArrowRight,
  ArrowUpRight,
  Columns3,
  Filter,
  FolderSync,
  GitFork,
  Layers3,
  Pause,
  Play,
  Plus,
  RefreshCw,
  Trash2,
} from 'lucide-react';
import { toast } from 'sonner';

import {
  useDeleteFlow,
  useFlows,
  usePauseFlow,
  useResumeFlow,
  useSinks,
  useSources,
} from '@/lib/query/manager';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { StatusBadge, type Status } from '@/components/shared/StatusBadge';
import { labelKeyForFlowStatus, statusForFlow } from '@/config/status';
import { FlowWizard } from './FlowWizard';
import { ROUTES } from '@/config/routes';
import type { FlowConfig } from '@/types/api';
import { DeleteConfirmDialog } from '../components/DeleteConfirmDialog';

function statusToBadge(status: FlowConfig['status']): Status {
  return statusForFlow(status);
}

function flowOption(flow: FlowConfig, key: 'batch_size' | 'partition_count') {
  return flow.options?.[key] ?? '-';
}

export default function FlowsPage() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const [wizardOpen, setWizardOpen] = useState(false);
  const [deleteFlowId, setDeleteFlowId] = useState<string | null>(null);

  const {
    data: flowsData,
    isLoading: flowsLoading,
    refetch: refetchFlows,
    isFetching,
  } = useFlows();
  const { data: sourcesData } = useSources();
  const { data: sinksData } = useSinks();

  const pauseMutation = usePauseFlow();
  const resumeMutation = useResumeFlow();
  const deleteMutation = useDeleteFlow();

  const sourcesMap = useMemo(() => {
    if (!sourcesData?.sources) return new Map();
    return new Map(sourcesData.sources.map((s) => [s.instance_id, s]));
  }, [sourcesData]);

  const sinksMap = useMemo(() => {
    if (!sinksData?.sinks) return new Map();
    return new Map(sinksData.sinks.map((s) => [s.instance_id, s]));
  }, [sinksData]);

  const handlePause = async (flowId: string) => {
    try {
      await pauseMutation.mutateAsync(flowId);
      toast.success(t('manager.flows.toast.paused'));
    } catch {
      toast.error(t('manager.flows.toast.pauseFailed'));
    }
  };

  const handleResume = async (flowId: string) => {
    try {
      await resumeMutation.mutateAsync(flowId);
      toast.success(t('manager.flows.toast.resumed'));
    } catch {
      toast.error(t('manager.flows.toast.resumeFailed'));
    }
  };

  const handleDelete = async () => {
    if (!deleteFlowId) return;
    try {
      await deleteMutation.mutateAsync(deleteFlowId);
      toast.success(t('manager.flows.toast.deleted'));
      setDeleteFlowId(null);
    } catch {
      toast.error(t('manager.flows.toast.deleteFailed'));
    }
  };

  const flows = flowsData?.flows ?? [];

  return (
    <div className="flex min-h-[calc(100vh-7.5rem)] flex-col space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="flex items-center gap-2 text-2xl font-bold tracking-tight text-foreground">
            <GitFork className="h-6 w-6 text-amber-500 dark:text-amber-400" />
            {t('manager.flows.title')}
          </h1>
          <p className="mt-1 max-w-2xl text-xs text-muted-foreground">
            {t('manager.flows.desc')}
          </p>
        </div>

        <div className="flex items-center gap-2">
          <button
            onClick={() => refetchFlows()}
            className="inline-flex h-9 w-9 cursor-pointer items-center justify-center rounded-lg border border-border text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
            title={t('common.refresh')}
          >
            <RefreshCw className={`h-4 w-4 ${isFetching ? 'animate-spin' : ''}`} />
          </button>
          <Button
            onClick={() => setWizardOpen(true)}
            className="h-9 cursor-pointer bg-amber-500 text-xs font-semibold text-slate-950 hover:bg-amber-400"
          >
            <Plus className="mr-1 h-4 w-4" />
            {t('manager.flows.create')}
          </Button>
        </div>
      </div>

      {flowsLoading ? (
        <div className="grid gap-4 lg:grid-cols-2">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="h-60 animate-pulse rounded-lg border border-border bg-muted/40" />
          ))}
        </div>
      ) : flows.length === 0 ? (
        <div className="flex flex-1 flex-col items-center justify-center rounded-lg border border-dashed border-border bg-card p-8 text-center">
          <div className="mb-4 rounded-full border border-border bg-muted p-3.5">
            <GitFork className="h-6 w-6 text-muted-foreground" />
          </div>
          <h3 className="mb-1 text-sm font-semibold text-foreground">
            {t('manager.flows.noFlows')}
          </h3>
          <p className="mb-4 max-w-xs text-xs text-muted-foreground">
            {t('manager.flows.noFlowsDesc')}
          </p>
          <Button
            onClick={() => setWizardOpen(true)}
            className="h-8 cursor-pointer bg-amber-500 text-xs font-semibold text-slate-950 hover:bg-amber-400"
          >
            <Plus className="mr-1 h-3.5 w-3.5" />
            {t('manager.flows.create')}
          </Button>
        </div>
      ) : (
        <div className="grid gap-4 lg:grid-cols-2">
          {flows.map((flow) => {
            const src = sourcesMap.get(flow.source_id);
            const sink = sinksMap.get(flow.sink_id);
            const isRunning = flow.status === 'FLOW_STATUS_RUNNING';
            const hasFilter = Boolean(flow.options?.filter_expression);
            const mappingCount = flow.column_mappings?.filter((mapping) => mapping.enabled).length ?? 0;
            const totalMappings = flow.column_mappings?.length ?? 0;

            return (
              <article
                key={flow.flow_id}
                className="relative overflow-hidden rounded-lg border border-border bg-card p-4 shadow-sm shadow-black/5 transition-colors duration-200 hover:border-amber-500/40 hover:bg-card/95"
              >
                <div className="pointer-events-none absolute inset-x-0 top-0 h-px bg-gradient-to-r from-amber-500/15 via-transparent to-transparent" />

                <div className="space-y-4">
                  <div className="flex items-start justify-between gap-3">
                    <div className="flex min-w-0 items-start gap-3">
                      <div className="mt-0.5 flex h-10 w-10 shrink-0 items-center justify-center rounded-lg border border-amber-500/20 bg-amber-500/10 text-amber-600 dark:text-amber-400">
                        <FolderSync className="h-4.5 w-4.5" />
                      </div>
                      <div className="min-w-0 space-y-0.5">
                        <p className="inline-flex max-w-full truncate rounded border border-border bg-muted/30 px-1.5 py-0.5 font-mono text-[10px] font-medium leading-3 text-muted-foreground">
                          {flow.flow_id}
                        </p>
                        <h2 className="truncate text-[15px] font-semibold leading-5 text-foreground">
                          {flow.name || flow.flow_id}
                        </h2>
                      </div>
                    </div>

                    <div className="flex w-36 shrink-0 flex-col items-end gap-1">
                      <div className="flex items-center gap-1.5">
                        {isRunning ? (
                          <button
                            onClick={() => handlePause(flow.flow_id)}
                            disabled={pauseMutation.isPending}
                            className="cursor-pointer rounded-md border border-border bg-background/60 p-1.5 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground disabled:cursor-not-allowed disabled:opacity-50"
                            title={t('manager.flows.tooltips.pause')}
                          >
                            <Pause className="h-3.5 w-3.5" />
                          </button>
                        ) : (
                          <button
                            onClick={() => handleResume(flow.flow_id)}
                            disabled={resumeMutation.isPending}
                            className="cursor-pointer rounded-md border border-border bg-background/60 p-1.5 text-emerald-600 transition-colors hover:bg-emerald-500/10 hover:text-emerald-500 disabled:cursor-not-allowed disabled:opacity-50"
                            title={t('manager.flows.tooltips.resume')}
                          >
                            <Play className="h-3.5 w-3.5" />
                          </button>
                        )}
                        <button
                          onClick={() => navigate(ROUTES.MANAGER_FLOW_DETAIL.replace(':id', flow.flow_id))}
                          className="cursor-pointer rounded-md border border-border bg-background/60 p-1.5 text-amber-600 transition-colors hover:bg-amber-500/10 hover:text-amber-500 dark:text-amber-400"
                          title={t('manager.flows.tooltips.details')}
                        >
                          <ArrowUpRight className="h-3.5 w-3.5" />
                        </button>
                        <button
                          onClick={() => setDeleteFlowId(flow.flow_id)}
                          disabled={deleteMutation.isPending}
                          className="cursor-pointer rounded-md border border-border bg-background/60 p-1.5 text-muted-foreground transition-colors hover:border-destructive/30 hover:bg-destructive/10 hover:text-destructive disabled:cursor-not-allowed disabled:opacity-50"
                          title={t('manager.flows.tooltips.delete')}
                        >
                          <Trash2 className="h-3.5 w-3.5" />
                        </button>
                      </div>
                      <StatusBadge
                        status={statusToBadge(flow.status)}
                        label={t(labelKeyForFlowStatus(flow.status))}
                        className="shrink-0"
                      />
                    </div>
                  </div>

                  <div className="rounded-lg border border-border bg-muted/20 px-3 py-2.5">
                    <div className="grid grid-cols-[1fr_auto_1fr] items-center gap-3">
                      <div className="min-w-0">
                        <p className="truncate text-sm font-semibold text-foreground">
                          {src?.name || src?.database || flow.source_id}
                        </p>
                        <p className="truncate font-mono text-[11px] text-muted-foreground">
                          {flow.source_table}
                        </p>
                      </div>
                      <div className="flex h-7 w-7 items-center justify-center rounded-full border border-amber-500/20 bg-amber-500/10 text-amber-600 dark:text-amber-400">
                        <ArrowRight className="h-3.5 w-3.5" />
                      </div>
                      <div className="min-w-0 text-right">
                        <p className="truncate text-sm font-semibold text-foreground">
                          {sink?.name || sink?.database || flow.sink_id}
                        </p>
                        <p className="truncate font-mono text-[11px] text-muted-foreground">
                          {flow.sink_table || '-'}
                        </p>
                      </div>
                    </div>
                  </div>

                  <div className="grid grid-cols-3 gap-2">
                    <div className="rounded-md border border-border bg-background/40 p-2.5">
                      <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wide text-muted-foreground">
                        <Layers3 className="h-3 w-3" />
                        {t('manager.flows.fields.batchSize')}
                      </div>
                      <p className="mt-1 text-sm font-semibold text-foreground">
                        {flowOption(flow, 'batch_size')}
                      </p>
                    </div>
                    <div className="rounded-md border border-border bg-background/40 p-2.5">
                      <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wide text-muted-foreground">
                        <GitFork className="h-3 w-3" />
                        {t('manager.flows.fields.partitionCount')}
                      </div>
                      <p className="mt-1 text-sm font-semibold text-foreground">
                        {flowOption(flow, 'partition_count')}
                      </p>
                    </div>
                    <div className="rounded-md border border-border bg-background/40 p-2.5">
                      <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wide text-muted-foreground">
                        <Columns3 className="h-3 w-3" />
                        {t('manager.flows.columns')}
                      </div>
                      <p className="mt-1 text-sm font-semibold text-foreground">
                        {mappingCount}/{totalMappings}
                      </p>
                    </div>
                  </div>

                  {hasFilter && (
                    <Badge
                      variant="outline"
                      className="w-fit border-sky-500/25 bg-sky-500/10 text-[10px] text-sky-600 dark:text-sky-400"
                    >
                      <Filter className="mr-1 h-3 w-3" />
                      {t('manager.cards.filterOn')}
                    </Badge>
                  )}
                </div>
              </article>
            );
          })}
        </div>
      )}

      <FlowWizard open={wizardOpen} onOpenChange={setWizardOpen} />
      <DeleteConfirmDialog
        open={deleteFlowId !== null}
        title={t('manager.flows.delete')}
        description={t('manager.flows.confirm.delete')}
        confirmLabel={t('common.delete')}
        cancelLabel={t('common.cancel')}
        loading={deleteMutation.isPending}
        onOpenChange={(open) => !open && setDeleteFlowId(null)}
        onConfirm={handleDelete}
      />
    </div>
  );
}
