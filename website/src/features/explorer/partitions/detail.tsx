import { Link, useNavigate, useParams } from 'react-router-dom';
import { ArrowLeft, GitBranch, RadioTower } from 'lucide-react';
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
import { usePartitionDetail } from '@/lib/query/explorer';
import { PartitionMessageTimeline } from '../components/PartitionMessageTimeline';
import { formatCount } from '../shared';

export default function ExplorerPartitionDetailPage() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const params = useParams();
  const topic = decodeURIComponent(params.topic ?? '');
  const partition = decodeURIComponent(params.partition ?? '');
  const { data, isLoading } = usePartitionDetail(topic, partition);
  const summary = data?.summary;
  const topicPath = ROUTES.EXPLORER_TOPIC_DETAIL.replace(':topic', encodeURIComponent(topic));

  return (
    <div className="flex h-full flex-col gap-5">
      <PageHeader
        title={t('explorer.partitionLabel', { partition })}
        description={<span className="font-mono text-xs">{topic}</span>}
        backAction={(
          <Button
            variant="ghost"
            size="sm"
            className="-ml-2 text-muted-foreground"
            onClick={() => navigate(topicPath)}
          >
            <ArrowLeft className="h-4 w-4" />
            {t('explorer.topic')}
          </Button>
        )}
        actions={(
          <>
            <Badge variant="outline">{summary?.health ?? 'direct'}</Badge>
            <Link
              to={ROUTES.EXPLORER_CONSUMERS}
              className="inline-flex h-8 items-center gap-2 rounded-lg border border-border px-3 text-sm text-muted-foreground hover:bg-muted"
            >
              <RadioTower className="h-4 w-4" />
              {t('explorer.consumers')}
            </Link>
          </>
        )}
      />

      <div className="grid gap-3 md:grid-cols-4">
        <MetricTile label={t('explorer.messages')} value={formatCount(summary?.message_count)} />
        <MetricTile label={t('explorer.pending')} value={formatCount(summary?.pending_count)} />
        <MetricTile label={t('explorer.ackPending')} value={formatCount(summary?.ack_pending_count)} />
        <MetricTile label={t('explorer.latestSequence')} value={formatCount(summary?.latest_sequence)} />
      </div>

      <section className="space-y-3">
        <div className="flex items-center gap-2">
          <GitBranch className="h-4 w-4 text-muted-foreground" />
          <h2 className="font-semibold text-foreground">{t('explorer.messageTimeline')}</h2>
        </div>
        <PartitionMessageTimeline topic={topic} partition={partition} />
      </section>

      <section className="overflow-hidden rounded-lg border border-border bg-card">
        <div className="border-b border-border p-4">
          <h2 className="font-semibold text-foreground">{t('explorer.lagAndCheckpoints')}</h2>
        </div>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{t('explorer.consumer')}</TableHead>
              <TableHead className="text-right">{t('explorer.delivered')}</TableHead>
              <TableHead className="text-right">{t('explorer.ackFloor')}</TableHead>
              <TableHead className="text-right">{t('explorer.pending')}</TableHead>
              <TableHead className="text-right">{t('explorer.ackPending')}</TableHead>
              <TableHead className="text-right">{t('dashboard.lag')}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              <LoadingTableRows colSpan={6} rows={3} />
            ) : (data?.checkpoints ?? []).length === 0 ? (
              <EmptyTableRow colSpan={6}>{t('explorer.noCheckpointContext')}</EmptyTableRow>
            ) : (
              (data?.checkpoints ?? []).map((checkpoint) => (
                <TableRow key={checkpoint.consumer_name}>
                  <TableCell className="font-mono text-xs">{checkpoint.consumer_name}</TableCell>
                  <TableCell className="text-right">{formatCount(checkpoint.delivered_stream_seq)}</TableCell>
                  <TableCell className="text-right">{formatCount(checkpoint.ack_floor_stream_seq)}</TableCell>
                  <TableCell className="text-right">{formatCount(checkpoint.num_pending)}</TableCell>
                  <TableCell className="text-right">{formatCount(checkpoint.num_ack_pending)}</TableCell>
                  <TableCell className="text-right">{formatCount(checkpoint.lag_messages)}</TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </section>
    </div>
  );
}
