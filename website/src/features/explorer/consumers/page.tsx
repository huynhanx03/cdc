import { useMemo } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { RefreshCw, RadioTower, X } from 'lucide-react';
import { useTranslation } from 'react-i18next';

import { Button } from '@/components/ui/button';
import { ROUTES } from '@/config/routes';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { useConsumers } from '@/lib/query/explorer';
import { StatusBadge } from '../components/StatusBadge';
import { formatCount } from '../shared';

export default function ExplorerConsumersPage() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const topicFilter = searchParams.get('topic') || '';
  const { data, isLoading, isFetching, refetch } = useConsumers(1, 100);
  const consumers = useMemo(() => {
    const rows = data?.data ?? [];
    if (!topicFilter) return rows;
    return rows.filter((consumer) =>
      (consumer.filter_subjects ?? []).some((subject) => subject.startsWith(topicFilter)),
    );
  }, [data, topicFilter]);

  return (
    <div className="flex h-full flex-col gap-5">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight text-foreground">
            {t('explorer.consumers')}
          </h1>
          <p className="mt-1 text-sm text-muted-foreground">
            {t('explorer.consumersDesc')}
          </p>
          {topicFilter ? (
            <button
              type="button"
              onClick={() => setSearchParams({})}
              className="mt-3 inline-flex max-w-full cursor-pointer items-center gap-2 rounded-full border border-sky-500/25 bg-sky-500/10 px-3 py-1 text-xs text-sky-700 transition-colors hover:bg-sky-500/15 dark:text-sky-300"
            >
              <span className="truncate font-mono">{topicFilter}</span>
              <X className="h-3.5 w-3.5" />
            </button>
          ) : null}
        </div>
        <Button variant="outline" size="sm" onClick={() => refetch()}>
          <RefreshCw className={`h-4 w-4 ${isFetching ? 'animate-spin' : ''}`} />
        </Button>
      </div>

      <div className="overflow-hidden rounded-lg border border-border bg-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{t('explorer.consumer')}</TableHead>
              <TableHead>{t('explorer.filterSubjects')}</TableHead>
              <TableHead className="text-right">{t('explorer.pending')}</TableHead>
              <TableHead className="text-right">{t('explorer.ackPending')}</TableHead>
              <TableHead className="text-right">{t('explorer.deliveredSeq')}</TableHead>
              <TableHead className="text-right">{t('explorer.ackFloor')}</TableHead>
              <TableHead>{t('dashboard.status')}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              Array.from({ length: 5 }).map((_, index) => (
                <TableRow key={index}>
                  <TableCell colSpan={7}>
                    <div className="h-6 animate-pulse rounded bg-muted" />
                  </TableCell>
                </TableRow>
              ))
            ) : consumers.length === 0 ? (
              <TableRow>
                <TableCell colSpan={7} className="h-40 text-center text-sm text-muted-foreground">
                  <RadioTower className="mx-auto mb-3 h-8 w-8 opacity-50" />
                  {t('explorer.noConsumers')}
                </TableCell>
              </TableRow>
            ) : (
              consumers.map((consumer) => {
                const pending = consumer.num_pending ?? 0;
                const ackPending = consumer.num_ack_pending ?? 0;
                const lagging = pending > 0 || ackPending > 0;
                return (
                  <TableRow
                    key={consumer.name}
                    className="cursor-pointer"
                    onClick={() => navigate(ROUTES.EXPLORER_CONSUMER_DETAIL.replace(':consumer', encodeURIComponent(consumer.name)))}
                  >
                    <TableCell className="font-mono text-xs font-semibold">{consumer.name}</TableCell>
                    <TableCell>
                      <div className="space-y-1">
                        {(consumer.filter_subjects ?? []).map((subject) => (
                          <div key={subject} className="font-mono text-[11px] text-muted-foreground">
                            {subject}
                          </div>
                        ))}
                      </div>
                    </TableCell>
                    <TableCell className="text-right">{formatCount(pending)}</TableCell>
                    <TableCell className="text-right">{formatCount(ackPending)}</TableCell>
                    <TableCell className="text-right font-mono text-xs">{formatCount(consumer.delivered_stream_seq)}</TableCell>
                    <TableCell className="text-right font-mono text-xs">{formatCount(consumer.ack_floor_stream_seq)}</TableCell>
                    <TableCell>
                      <StatusBadge status={lagging ? 'lagging' : 'active'} />
                    </TableCell>
                  </TableRow>
                );
              })
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
