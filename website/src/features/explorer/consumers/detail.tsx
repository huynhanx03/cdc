import { useNavigate, useParams } from 'react-router-dom';
import { ArrowLeft, RadioTower } from 'lucide-react';
import { useTranslation } from 'react-i18next';

import { MetricTile } from '@/components/shared/MetricTile';
import { PageHeader } from '@/components/shared/PageHeader';
import { EmptyTableRow, LoadingTableRows } from '@/components/shared/TableState';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { ROUTES } from '@/config/routes';
import { useConsumerDetail } from '@/lib/query/explorer';
import { formatCount, formatTime } from '../shared';

export default function ExplorerConsumerDetailPage() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const params = useParams();
  const consumer = decodeURIComponent(params.consumer ?? '');
  const { data, isLoading } = useConsumerDetail(consumer);
  const summary = data?.summary;

  return (
    <div className="flex h-full flex-col gap-5">
      <PageHeader
        title={consumer}
        backAction={(
          <Button
            variant="ghost"
            size="sm"
            className="-ml-2 text-muted-foreground"
            onClick={() => navigate(ROUTES.EXPLORER_CONSUMERS)}
          >
            <ArrowLeft className="h-4 w-4" />
            {t('explorer.consumers')}
          </Button>
        )}
        actions={(
          <Badge variant={(summary?.lag_messages ?? 0) > 0 ? 'destructive' : 'outline'}>
            {t('explorer.replayRisk', { risk: summary?.replay_risk ?? 'low' })}
          </Badge>
        )}
      />

      <div className="grid gap-3 md:grid-cols-5">
        <MetricTile label={t('explorer.pending')} value={formatCount(summary?.num_pending)} />
        <MetricTile label={t('explorer.ackPending')} value={formatCount(summary?.num_ack_pending)} />
        <MetricTile label={t('explorer.deliveredSeq')} value={formatCount(summary?.delivered_stream_seq)} />
        <MetricTile label={t('explorer.ackFloor')} value={formatCount(summary?.ack_floor_stream_seq)} />
        <MetricTile label={t('dashboard.lag')} value={formatCount(summary?.lag_messages)} />
      </div>

      <section className="rounded-lg border border-border bg-card p-4">
        <div className="mb-3 flex items-center gap-2">
          <RadioTower className="h-4 w-4 text-muted-foreground" />
          <h2 className="font-semibold text-foreground">{t('explorer.filterSubjects')}</h2>
        </div>
        <div className="flex flex-wrap gap-2">
          {(summary?.filter_subjects ?? []).length === 0 ? (
            <span className="text-sm text-muted-foreground">{t('explorer.noFilterSubjects')}</span>
          ) : (
            (summary?.filter_subjects ?? []).map((subject) => (
              <Badge key={subject} variant="outline" className="font-mono">
                {subject}
              </Badge>
            ))
          )}
        </div>
      </section>

      <section className="overflow-hidden rounded-lg border border-border bg-card">
        <div className="border-b border-border p-4">
          <h2 className="font-semibold text-foreground">{t('explorer.recentMessages')}</h2>
        </div>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{t('explorer.time')}</TableHead>
              <TableHead>{t('explorer.subject')}</TableHead>
              <TableHead>{t('explorer.operation')}</TableHead>
              <TableHead className="text-right">{t('explorer.sequence')}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              <LoadingTableRows colSpan={4} />
            ) : (data?.recent_messages ?? []).length === 0 ? (
              <EmptyTableRow colSpan={4}>{t('explorer.noRecentMessages')}</EmptyTableRow>
            ) : (
              (data?.recent_messages ?? []).map((message) => (
                <TableRow key={`${message.subject}-${message.sequence}`}>
                  <TableCell className="whitespace-nowrap text-xs">{formatTime(message.timestamp)}</TableCell>
                  <TableCell className="max-w-[560px] truncate font-mono text-xs">{message.subject}</TableCell>
                  <TableCell>{message.op || '-'}</TableCell>
                  <TableCell className="text-right font-mono text-xs">{message.sequence}</TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </section>
    </div>
  );
}
