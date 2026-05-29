import { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import {
  HardDrive,
  Plus,
  RefreshCw,
  Server,
} from 'lucide-react';
import { toast } from 'sonner';

import { useFlows, useRemoveSink, useSinks, useStats } from '@/lib/query/manager';
import { Button } from '@/components/ui/button';
import { SinkForm } from './SinkForm';
import { connectorLabel } from '@/config/connectors';
import { formatNumber } from '@/lib/format';
import type { SinkConfig } from '@/types/api';
import { ConnectorCard } from '../components/ConnectorCard';
import { DeleteConfirmDialog } from '../components/DeleteConfirmDialog';

function sinkTypeLabel(type: SinkConfig['type']) {
  return connectorLabel(type);
}

function endpointLabel(sink: SinkConfig) {
  if (sink.host) return `${sink.host}:${sink.port}`;
  if (sink.url?.length) return sink.url[0];
  return sink.instance_id;
}

function targetLabel(sink: SinkConfig) {
  return sink.database || sink.index_prefix || sink.url?.[0] || '-';
}

export default function SinksPage() {
  const { t } = useTranslation();

  const [formOpen, setFormOpen] = useState(false);
  const [editingSink, setEditingSink] = useState<SinkConfig | null>(null);
  const [deleteSinkId, setDeleteSinkId] = useState<string | null>(null);

  const { data, isLoading, refetch, isFetching } = useSinks();
  const { data: flowsData } = useFlows();
  const { data: statsData } = useStats();
  const removeMutation = useRemoveSink();

  const usageBySink = useMemo(() => {
    const usage = new Map<string, number>();
    for (const flow of flowsData?.flows ?? []) {
      usage.set(flow.sink_id, (usage.get(flow.sink_id) ?? 0) + 1);
    }
    return usage;
  }, [flowsData]);

  const handleAddClick = () => {
    setEditingSink(null);
    setFormOpen(true);
  };

  const handleEditClick = (sink: SinkConfig) => {
    setEditingSink(sink);
    setFormOpen(true);
  };

  const handleDeleteConfirm = async () => {
    if (!deleteSinkId) return;
    try {
      await removeMutation.mutateAsync(deleteSinkId);
      toast.success(t('manager.sinks.toast.deleted'));
      setDeleteSinkId(null);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : t('manager.sinks.toast.deleteFailed'));
    }
  };

  const sinks = data?.sinks || [];

  return (
    <div className="flex min-h-[calc(100vh-7.5rem)] flex-col space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="flex items-center gap-2 text-2xl font-bold tracking-tight text-foreground">
            <HardDrive className="h-6 w-6 text-violet-500 dark:text-violet-400" />
            {t('manager.sinks.title')}
          </h1>
          <p className="mt-1 max-w-2xl text-xs text-muted-foreground">
            {t('manager.sinks.desc')}
          </p>
        </div>

        <div className="flex items-center gap-2">
          <button
            onClick={() => refetch()}
            className="inline-flex h-9 w-9 cursor-pointer items-center justify-center rounded-lg border border-border text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
            title={t('common.refresh')}
          >
            <RefreshCw className={`h-4 w-4 ${isFetching ? 'animate-spin' : ''}`} />
          </button>
          <Button
            onClick={handleAddClick}
            className="h-9 cursor-pointer bg-violet-500 text-xs font-semibold text-white hover:bg-violet-400"
          >
            <Plus className="mr-1 h-4 w-4" />
            {t('manager.sinks.add')}
          </Button>
        </div>
      </div>

      {isLoading ? (
        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
          {Array.from({ length: 3 }).map((_, i) => (
            <div key={i} className="h-52 animate-pulse rounded-lg border border-border bg-muted/40" />
          ))}
        </div>
      ) : sinks.length === 0 ? (
        <div className="flex flex-1 flex-col items-center justify-center rounded-lg border border-dashed border-border bg-card p-8 text-center">
          <div className="mb-4 rounded-full border border-border bg-muted p-3.5">
            <HardDrive className="h-6 w-6 text-muted-foreground" />
          </div>
          <h3 className="mb-1 text-sm font-semibold text-foreground">
            {t('manager.sinks.noSinks')}
          </h3>
          <p className="mb-4 max-w-xs text-xs text-muted-foreground">
            {t('manager.sinks.noSinksDesc')}
          </p>
          <Button
            onClick={handleAddClick}
            className="h-8 cursor-pointer bg-violet-500 text-xs font-semibold text-white hover:bg-violet-400"
          >
            <Plus className="mr-1 h-3.5 w-3.5" />
            {t('manager.sinks.add')}
          </Button>
        </div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
          {sinks.map((sink) => {
            const usageCount = usageBySink.get(sink.instance_id) ?? 0;
            const stats = statsData?.sink_stats?.[sink.instance_id];
            const activeFlows = stats?.active_flows ?? usageCount;
            const throughput = stats?.throughput ?? 0;
            const errors = stats?.failure_count ?? 0;
            const errorRate = stats?.error_rate ?? 0;

            return (
              <ConnectorCard
                key={sink.instance_id}
                tone="sink"
                icon={Server}
                name={sink.name || targetLabel(sink)}
                endpoint={endpointLabel(sink)}
                typeLabel={sinkTypeLabel(sink.type)}
                instanceId={sink.instance_id}
                metrics={[
                  {
                    label: t('manager.cards.usage'),
                    value: activeFlows > 0
                      ? t('manager.cards.usedByFlows', { count: activeFlows })
                      : t('manager.cards.unused'),
                  },
                  {
                    label: t('manager.cards.throughput'),
                    value: `${formatNumber(throughput)}/s`,
                  },
                  {
                    label: t('manager.cards.errors'),
                    value: errors > 0 ? `${formatNumber(errors)} · ${errorRate.toFixed(2)}%` : '0',
                    tone: errors > 0 ? 'danger' : 'default',
                  },
                ]}
                editLabel={t('manager.sinks.card.editTooltip')}
                deleteLabel={t('manager.sinks.card.deleteTooltip')}
                deleteDisabled={removeMutation.isPending}
                onEdit={() => handleEditClick(sink)}
                onDelete={() => setDeleteSinkId(sink.instance_id)}
              />
            );
          })}
        </div>
      )}

      <SinkForm
        open={formOpen}
        onOpenChange={setFormOpen}
        sinkToEdit={editingSink}
      />
      <DeleteConfirmDialog
        open={deleteSinkId !== null}
        title={t('manager.sinks.delete')}
        description={t('manager.sinks.confirm.delete', { id: deleteSinkId ?? '' })}
        confirmLabel={t('common.delete')}
        cancelLabel={t('common.cancel')}
        loading={removeMutation.isPending}
        onOpenChange={(open) => !open && setDeleteSinkId(null)}
        onConfirm={handleDeleteConfirm}
      />
    </div>
  );
}
