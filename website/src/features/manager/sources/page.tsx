import { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import {
  Database,
  Plus,
  RefreshCw,
  Server,
} from 'lucide-react';
import { toast } from 'sonner';

import { useFlows, useRemoveSource, useSources, useStats } from '@/lib/query/manager';
import { Button } from '@/components/ui/button';
import { SourceForm } from './SourceForm';
import { connectorLabel } from '@/config/connectors';
import { formatNumber } from '@/lib/format';
import type { SourceConfig } from '@/types/api';
import { ConnectorCard } from '../components/ConnectorCard';
import { DeleteConfirmDialog } from '../components/DeleteConfirmDialog';

function sourceTypeLabel(type: SourceConfig['type']) {
  return connectorLabel(type);
}

function endpointLabel(source: SourceConfig) {
  return `${source.host}:${source.port}`;
}

export default function SourcesPage() {
  const { t } = useTranslation();

  const [formOpen, setFormOpen] = useState(false);
  const [editingSource, setEditingSource] = useState<SourceConfig | null>(null);
  const [deleteSourceId, setDeleteSourceId] = useState<string | null>(null);

  const { data, isLoading, refetch, isFetching } = useSources();
  const { data: flowsData } = useFlows();
  const { data: statsData } = useStats();
  const removeMutation = useRemoveSource();

  const usageBySource = useMemo(() => {
    const usage = new Map<string, number>();
    for (const flow of flowsData?.flows ?? []) {
      usage.set(flow.source_id, (usage.get(flow.source_id) ?? 0) + 1);
    }
    return usage;
  }, [flowsData]);

  const handleAddClick = () => {
    setEditingSource(null);
    setFormOpen(true);
  };

  const handleEditClick = (source: SourceConfig) => {
    setEditingSource(source);
    setFormOpen(true);
  };

  const handleDeleteConfirm = async () => {
    if (!deleteSourceId) return;
    try {
      await removeMutation.mutateAsync(deleteSourceId);
      toast.success(t('manager.sources.toast.deleted'));
      setDeleteSourceId(null);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : t('manager.sources.toast.deleteFailed'));
    }
  };

  const sources = data?.sources || [];

  return (
    <div className="flex min-h-[calc(100vh-7.5rem)] flex-col space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="flex items-center gap-2 text-2xl font-bold tracking-tight text-foreground">
            <Database className="h-6 w-6 text-sky-500 dark:text-sky-400" />
            {t('manager.sources.title')}
          </h1>
          <p className="mt-1 max-w-2xl text-xs text-muted-foreground">
            {t('manager.sources.desc')}
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
            className="h-9 cursor-pointer bg-sky-500 text-xs font-semibold text-slate-950 hover:bg-sky-400"
          >
            <Plus className="mr-1 h-4 w-4" />
            {t('manager.sources.add')}
          </Button>
        </div>
      </div>

      {isLoading ? (
        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
          {Array.from({ length: 3 }).map((_, i) => (
            <div key={i} className="h-52 animate-pulse rounded-lg border border-border bg-muted/40" />
          ))}
        </div>
      ) : sources.length === 0 ? (
        <div className="flex flex-1 flex-col items-center justify-center rounded-lg border border-dashed border-border bg-card p-8 text-center">
          <div className="mb-4 rounded-full border border-border bg-muted p-3.5">
            <Database className="h-6 w-6 text-muted-foreground" />
          </div>
          <h3 className="mb-1 text-sm font-semibold text-foreground">
            {t('manager.sources.noSources')}
          </h3>
          <p className="mb-4 max-w-xs text-xs text-muted-foreground">
            {t('manager.sources.noSourcesDesc')}
          </p>
          <Button
            onClick={handleAddClick}
            className="h-8 cursor-pointer bg-sky-500 text-xs font-semibold text-slate-950 hover:bg-sky-400"
          >
            <Plus className="mr-1 h-3.5 w-3.5" />
            {t('manager.sources.add')}
          </Button>
        </div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
          {sources.map((source) => {
            const usageCount = usageBySource.get(source.instance_id) ?? 0;
            const stats = statsData?.source_stats?.[source.instance_id];
            const activeFlows = stats?.active_flows ?? usageCount;
            const errorCount = stats?.failure_count ?? 0;
            const throughput = stats?.throughput ?? 0;

            return (
              <ConnectorCard
                key={source.instance_id}
                tone="source"
                icon={Server}
                name={source.name || source.database}
                endpoint={endpointLabel(source)}
                typeLabel={sourceTypeLabel(source.type)}
                instanceId={source.instance_id}
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
                    value: formatNumber(errorCount),
                    tone: errorCount > 0 ? 'danger' : 'default',
                  },
                ]}
                editLabel={t('manager.sources.card.editTooltip')}
                deleteLabel={t('manager.sources.card.deleteTooltip')}
                deleteDisabled={removeMutation.isPending}
                onEdit={() => handleEditClick(source)}
                onDelete={() => setDeleteSourceId(source.instance_id)}
              />
            );
          })}
        </div>
      )}

      <SourceForm
        open={formOpen}
        onOpenChange={setFormOpen}
        sourceToEdit={editingSource}
      />
      <DeleteConfirmDialog
        open={deleteSourceId !== null}
        title={t('manager.sources.delete')}
        description={t('manager.sources.confirm.delete', { id: deleteSourceId ?? '' })}
        confirmLabel={t('common.delete')}
        cancelLabel={t('common.cancel')}
        loading={removeMutation.isPending}
        onOpenChange={(open) => !open && setDeleteSourceId(null)}
        onConfirm={handleDeleteConfirm}
      />
    </div>
  );
}
