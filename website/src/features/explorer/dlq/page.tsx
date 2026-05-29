import { useMemo, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Inbox, RefreshCw, X } from 'lucide-react';
import { toast } from 'sonner';
import { useTranslation } from 'react-i18next';

import { MetricTile } from '@/components/shared/MetricTile';
import { PageHeader } from '@/components/shared/PageHeader';
import { EmptyTableRow, LoadingTableRows } from '@/components/shared/TableState';
import { Button } from '@/components/ui/button';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { useDLQMessages, useDLQPreview, useReprocessDLQ } from '@/lib/query/explorer';
import type { DLQMessage } from '@/types/api';
import { DLQDryRunButton } from '../components/DLQDryRunDialog';
import { MessageDetailSheet } from '../components/MessageDetailSheet';
import { ReprocessConfirmDialog } from '../components/ReprocessConfirmDialog';
import { StatusBadge } from '../components/StatusBadge';
import { formatBytes, formatTime, messageSize } from '../shared';

export default function ExplorerDLQPage() {
  const { t } = useTranslation();
  const [searchParams, setSearchParams] = useSearchParams();
  const topicFilter = searchParams.get('topic') || '';
  const [selectedMessage, setSelectedMessage] = useState<DLQMessage | null>(null);
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const { data, isLoading, isFetching, refetch } = useDLQMessages(1, 100);
  const previewMutation = useDLQPreview();
  const reprocessMutation = useReprocessDLQ();
  const messages = useMemo(() => {
    const rows = data?.data ?? [];
    if (!topicFilter) return rows;
    return rows.filter((message) => {
      const originalSubject =
        message.original_subject || message.headers?.['X-DLQ-Original-Subject'] || message.subject;
      return originalSubject.startsWith(topicFilter);
    });
  }, [data, topicFilter]);

  const selectedMessages = useMemo(
    () => messages.filter((message) => selectedIds.includes(dlqID(message))),
    [messages, selectedIds],
  );

  const previewSelected = async () => {
    try {
      await previewMutation.mutateAsync({
        selected_dlq_ids: selectedIds,
        filter: topicFilter ? { original_topic: topicFilter } : undefined,
        max_count: selectedIds.length || 100,
      });
      setConfirmOpen(true);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : t('explorer.previewDlqFailed'));
    }
  };

  const confirmReprocess = async () => {
    const preview = previewMutation.data;
    if (!preview?.confirm_token) return;
    try {
      const result = await reprocessMutation.mutateAsync({
        selected_dlq_ids: selectedIds,
        filter: topicFilter ? { original_topic: topicFilter } : undefined,
        confirm_token: preview.confirm_token,
        max_count: selectedIds.length || 100,
      });
      toast.success(t('explorer.reprocessedCount', { count: result.count || 0 }));
      setSelectedIds([]);
      setConfirmOpen(false);
      refetch();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : t('explorer.reprocessDlqFailed'));
    }
  };

  return (
    <div className="flex h-full flex-col gap-5">
      <PageHeader
        title={t('explorer.dlq')}
        description={t('explorer.dlqDesc')}
        eyebrow={topicFilter ? (
          <button
            type="button"
            onClick={() => setSearchParams({})}
            className="inline-flex max-w-full cursor-pointer items-center gap-2 rounded-full border border-rose-500/25 bg-rose-500/10 px-3 py-1 text-xs normal-case text-rose-700 transition-colors hover:bg-rose-500/15 dark:text-rose-300"
          >
            <span className="truncate font-mono">{topicFilter}</span>
            <X className="h-3.5 w-3.5" />
          </button>
        ) : null}
        actions={(
          <>
            <Button variant="outline" size="sm" onClick={() => refetch()}>
              <RefreshCw className={`h-4 w-4 ${isFetching ? 'animate-spin' : ''}`} />
              {t('explorer.refresh')}
            </Button>
            <DLQDryRunButton selected={selectedMessages} loading={previewMutation.isPending} onPreview={previewSelected} />
          </>
        )}
      />

      <div className="grid gap-3 md:grid-cols-3">
        <MetricTile label={t('explorer.failedMessages')} value={messages.length.toLocaleString()} />
        <MetricTile label={t('explorer.currentPage')} value={String(data?.pagination?.page ?? 1)} />
        <MetricTile label={t('explorer.pageSize')} value={String(data?.pagination?.limit ?? 100)} />
      </div>

      <div className="overflow-hidden rounded-lg border border-border bg-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-10" />
              <TableHead>{t('explorer.failedAt')}</TableHead>
              <TableHead>{t('explorer.originalSubject')}</TableHead>
              <TableHead>{t('explorer.reason')}</TableHead>
              <TableHead className="text-right">{t('explorer.sequence')}</TableHead>
              <TableHead className="text-right">{t('explorer.size')}</TableHead>
              <TableHead>{t('dashboard.status')}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              <LoadingTableRows colSpan={7} rows={6} />
            ) : messages.length === 0 ? (
              <EmptyTableRow colSpan={7}>
                <Inbox className="mx-auto mb-3 h-8 w-8 opacity-50" />
                {t('explorer.dlqClean')}
              </EmptyTableRow>
            ) : (
              messages.map((message) => (
                <TableRow
                  key={`${message.subject}-${message.sequence}`}
                  className="cursor-pointer"
                  onClick={() => setSelectedMessage(message)}
                >
                  <TableCell onClick={(event) => event.stopPropagation()}>
                    <input
                      type="checkbox"
                      className="h-4 w-4"
                      checked={selectedIds.includes(dlqID(message))}
                      onChange={(event) => {
                        const id = dlqID(message);
                        setSelectedIds((current) =>
                          event.target.checked ? [...new Set([...current, id])] : current.filter((item) => item !== id),
                        );
                      }}
                    />
                  </TableCell>
                  <TableCell className="whitespace-nowrap text-xs">{formatTime(message.timestamp)}</TableCell>
                  <TableCell className="max-w-[420px] truncate font-mono text-xs">
                    {message.original_subject || message.headers?.['X-DLQ-Original-Subject'] || '-'}
                  </TableCell>
                  <TableCell className="max-w-[320px] truncate text-xs">
                    {message.reason || message.headers?.['X-DLQ-Reason'] || '-'}
                  </TableCell>
                  <TableCell className="text-right font-mono text-xs">{message.sequence}</TableCell>
                  <TableCell className="text-right text-xs">{formatBytes(messageSize(message.data))}</TableCell>
                  <TableCell><StatusBadge status="dlq" /></TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      <MessageDetailSheet message={selectedMessage} onOpenChange={(open) => !open && setSelectedMessage(null)} />
      <ReprocessConfirmDialog
        open={confirmOpen}
        preview={previewMutation.data ?? null}
        loading={reprocessMutation.isPending}
        onOpenChange={setConfirmOpen}
        onConfirm={confirmReprocess}
      />
    </div>
  );
}

function dlqID(message: DLQMessage) {
  return message.dlq_id || message.headers?.['Nats-Msg-Id'] || `${message.subject}-${message.sequence}`;
}
