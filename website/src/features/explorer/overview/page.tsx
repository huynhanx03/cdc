import { Link } from 'react-router-dom';
import { AlertTriangle, GitBranch, Inbox, Layers, RadioTower } from 'lucide-react';
import { useTranslation } from 'react-i18next';

import { MetricCard } from '@/components/shared/MetricCard';
import { PageHeader } from '@/components/shared/PageHeader';
import { EmptyTableRow } from '@/components/shared/TableState';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { ROUTES } from '@/config/routes';
import { useExplorerOverview } from '@/lib/query/explorer';
import { formatCount, formatTime } from '../shared';

export default function ExplorerOverviewPage() {
  const { t } = useTranslation();
  const { data, isLoading } = useExplorerOverview();

  return (
    <div className="flex h-full flex-col gap-5">
      <PageHeader title={t('explorer.overviewTitle')} description={t('explorer.overviewDesc')} />

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
        <MetricCard loading={isLoading} title={t('explorer.topics')} value={formatCount(data?.topic_count)} icon={Layers} />
        <MetricCard loading={isLoading} title={t('explorer.partitions')} value={formatCount(data?.partition_count)} icon={GitBranch} />
        <MetricCard loading={isLoading} title={t('explorer.consumers')} value={formatCount(data?.consumer_count)} icon={RadioTower} />
        <MetricCard loading={isLoading} title={t('explorer.pending')} value={formatCount(data?.pending_count)} icon={AlertTriangle} />
        <MetricCard loading={isLoading} title={t('explorer.dlq')} value={formatCount(data?.dlq_depth)} icon={Inbox} />
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <section className="overflow-hidden rounded-lg border border-border bg-card">
          <div className="border-b border-border p-4">
            <h2 className="font-semibold text-foreground">{t('explorer.topicsNeedingAttention')}</h2>
          </div>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>{t('explorer.topic')}</TableHead>
                <TableHead className="text-right">{t('explorer.partitions')}</TableHead>
                <TableHead className="text-right">{t('explorer.pending')}</TableHead>
                <TableHead className="text-right">{t('explorer.dlq')}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {(data?.topics_needing_attention ?? []).length === 0 ? (
                <EmptyTableRow colSpan={4}>{t('explorer.noTopicsNeedAttention')}</EmptyTableRow>
              ) : (
                (data?.topics_needing_attention ?? []).map((topic) => (
                  <TableRow key={topic.name}>
                    <TableCell>
                      <Link className="font-mono text-xs font-semibold text-foreground hover:underline" to={ROUTES.EXPLORER_TOPIC_DETAIL.replace(':topic', encodeURIComponent(topic.name))}>
                        {topic.name}
                      </Link>
                    </TableCell>
                    <TableCell className="text-right">{formatCount(topic.partition_count)}</TableCell>
                    <TableCell className="text-right">{formatCount(topic.pending_count)}</TableCell>
                    <TableCell className="text-right">{formatCount(topic.dlq_count)}</TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </section>

        <section className="overflow-hidden rounded-lg border border-border bg-card">
          <div className="border-b border-border p-4">
            <h2 className="font-semibold text-foreground">{t('explorer.recentDlq')}</h2>
          </div>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>{t('explorer.originalSubject')}</TableHead>
                <TableHead>{t('explorer.reason')}</TableHead>
                <TableHead className="text-right">{t('explorer.time')}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {(data?.recent_dlq ?? []).length === 0 ? (
                <EmptyTableRow colSpan={3}>{t('explorer.dlqClean')}</EmptyTableRow>
              ) : (
                (data?.recent_dlq ?? []).map((item) => (
                  <TableRow key={item.dlq_id || item.original_subject}>
                    <TableCell className="max-w-[320px] truncate font-mono text-xs">{item.original_subject}</TableCell>
                    <TableCell>{item.reason || '-'}</TableCell>
                    <TableCell className="text-right text-xs">{formatTime(item.timestamp ?? '')}</TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </section>
      </div>
    </div>
  );
}
