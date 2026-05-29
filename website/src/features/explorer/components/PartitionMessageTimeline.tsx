import { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Badge } from '@/components/ui/badge';
import { EmptyTableRow, LoadingTableRows } from '@/components/shared/TableState';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { usePartitionMessages } from '@/lib/query/explorer';
import type { ExplorerMessageFilters, MessageItem } from '@/types/api';
import { formatBytes, formatTime, messageSize } from '../shared';
import { ExplorerFilterBar } from './ExplorerFilterBar';
import { MessageDetailSheet } from './MessageDetailSheet';

export function PartitionMessageTimeline({ topic, partition }: { topic: string; partition: string }) {
  const { t } = useTranslation();
  const [filters, setFilters] = useState<ExplorerMessageFilters>({
    sort: 'newest',
    page: 1,
    limit: 50,
  });
  const [selectedMessage, setSelectedMessage] = useState<MessageItem | null>(null);
  const { data, isLoading } = usePartitionMessages(topic, partition, filters);
  const rows = useMemo(() => data?.data ?? [], [data]);

  return (
    <div className="space-y-3">
      <ExplorerFilterBar value={filters} onChange={setFilters} />
      <div className="overflow-hidden rounded-lg border border-border bg-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{t('explorer.time')}</TableHead>
              <TableHead>{t('explorer.operation')}</TableHead>
              <TableHead className="text-right">{t('explorer.sequence')}</TableHead>
              <TableHead>{t('explorer.keyOrId')}</TableHead>
              <TableHead className="text-right">{t('explorer.size')}</TableHead>
              <TableHead>{t('explorer.headers')}</TableHead>
              <TableHead>{t('explorer.markers')}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              <LoadingTableRows colSpan={7} rows={8} />
            ) : rows.length === 0 ? (
              <EmptyTableRow colSpan={7}>{t('explorer.noMessagesMatch')}</EmptyTableRow>
            ) : (
              rows.map((message) => (
                <TableRow
                  key={`${message.subject}-${message.sequence}`}
                  className="cursor-pointer"
                  onClick={() => setSelectedMessage(message)}
                >
                  <TableCell className="whitespace-nowrap text-xs">{formatTime(message.timestamp)}</TableCell>
                  <TableCell><Badge variant="outline">{message.op || '-'}</Badge></TableCell>
                  <TableCell className="text-right font-mono text-xs">{message.sequence}</TableCell>
                  <TableCell className="max-w-[220px] truncate font-mono text-xs">
                    {message.key || message.nats_msg_id || '-'}
                  </TableCell>
                  <TableCell className="text-right text-xs">
                    {formatBytes(message.payload_size ?? messageSize(message.data))}
                  </TableCell>
                  <TableCell className="text-xs">{message.header_count ?? Object.keys(message.headers ?? {}).length}</TableCell>
                  <TableCell>
                    <div className="flex flex-wrap gap-1">
                      {(message.markers ?? []).map((marker) => (
                        <Badge key={marker} variant={marker === 'dlq' ? 'destructive' : 'secondary'}>{marker}</Badge>
                      ))}
                    </div>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>
      <MessageDetailSheet message={selectedMessage} onOpenChange={(open) => !open && setSelectedMessage(null)} />
    </div>
  );
}
