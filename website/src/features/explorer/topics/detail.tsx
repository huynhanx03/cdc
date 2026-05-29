import { useMemo } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import {
  ArrowLeft,
  ChevronRight,
  Copy,
  Database,
  GitBranch,
  RefreshCw,
} from 'lucide-react';
import { toast } from 'sonner';
import { useTranslation } from 'react-i18next';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { MetricTile } from '@/components/shared/MetricTile';
import { PageHeader } from '@/components/shared/PageHeader';
import { EmptyTableRow, LoadingTableRows } from '@/components/shared/TableState';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { ROUTES } from '@/config/routes';
import { useTopicDetail } from '@/lib/query/explorer';
import { formatCount, parseSubject } from '../shared';

function partitionPath(topic: string, partition: string) {
  return ROUTES.EXPLORER_TOPIC_PARTITION.replace(':topic', encodeURIComponent(topic)).replace(
    ':partition',
    encodeURIComponent(partition),
  );
}

export default function ExplorerTopicDetailPage() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const params = useParams();
  const topic = decodeURIComponent(params.topic ?? '');
  const parsed = useMemo(() => parseSubject(topic), [topic]);
  const { data, isLoading, isFetching, refetch } = useTopicDetail(topic);
  const partitions = data?.partitions ?? [];
  const summary = data?.summary;

  const copyTopic = async () => {
    await navigator.clipboard.writeText(topic);
    toast.success(t('explorer.topicCopied'));
  };

  if (!topic) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
        {t('explorer.missingTopic')}
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col gap-5">
      <PageHeader
        title={parsed.shortName}
        description={<span className="font-mono text-xs">{topic}</span>}
        backAction={(
          <Button
            variant="ghost"
            size="sm"
            className="-ml-2 text-muted-foreground"
            onClick={() => navigate(ROUTES.EXPLORER_TOPICS)}
          >
            <ArrowLeft className="h-4 w-4" />
            {t('explorer.topics')}
          </Button>
        )}
        actions={(
          <>
            <Badge variant="outline" className="border-sky-500/25 bg-sky-500/10 text-sky-700 dark:text-sky-300">
              {t('explorer.topic')}
            </Badge>
            <Button variant="outline" size="sm" onClick={copyTopic}>
              <Copy className="h-4 w-4" />
              {t('explorer.copy')}
            </Button>
            <Button variant="outline" size="sm" onClick={() => refetch()}>
              <RefreshCw className={`h-4 w-4 ${isFetching ? 'animate-spin' : ''}`} />
              {t('explorer.refresh')}
            </Button>
          </>
        )}
      />

      <div className="grid gap-3 md:grid-cols-4">
        <MetricTile label={t('explorer.source')} value={parsed.sourceId || '-'} icon={<Database className="h-4 w-4" />} />
        <MetricTile label={t('explorer.schema')} value={parsed.schema || '-'} icon={<GitBranch className="h-4 w-4" />} />
        <MetricTile label={t('explorer.table')} value={parsed.table || '-'} icon={<Database className="h-4 w-4" />} />
        <MetricTile label={t('explorer.partitions')} value={formatCount(summary?.partition_count ?? partitions.length)} icon={<GitBranch className="h-4 w-4" />} />
      </div>

      <Card className="overflow-hidden">
        <CardHeader>
          <CardTitle>{t('explorer.partitions')}</CardTitle>
          <CardDescription>{t('explorer.pickPartitionDesc')}</CardDescription>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>{t('explorer.partitionId')}</TableHead>
                <TableHead className="text-right">{t('explorer.messages')}</TableHead>
                <TableHead className="text-right">{t('explorer.pending')}</TableHead>
                <TableHead className="text-right">{t('explorer.latestSequence')}</TableHead>
                <TableHead className="w-20 text-right">{t('explorer.open')}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <LoadingTableRows colSpan={5} rows={4} />
              ) : partitions.length === 0 ? (
                <EmptyTableRow colSpan={5}>{t('explorer.noPartitions')}</EmptyTableRow>
              ) : (
                partitions.map((partition) => (
                  <TableRow
                    key={partition.id}
                    className="cursor-pointer"
                    onClick={() => navigate(partitionPath(topic, partition.id))}
                  >
                    <TableCell>
                      <div className="font-mono text-sm font-semibold text-foreground">
                        {t('explorer.partitionLabel', { partition: partition.id })}
                      </div>
                      <div className="mt-1 font-mono text-[11px] text-muted-foreground">
                        {topic}.{partition.id}
                      </div>
                    </TableCell>
                    <TableCell className="text-right">{formatCount(partition.message_count)}</TableCell>
                    <TableCell className="text-right">{formatCount(partition.pending_count)}</TableCell>
                    <TableCell className="text-right">{formatCount(partition.latest_sequence)}</TableCell>
                    <TableCell className="text-right">
                      <ChevronRight className="ml-auto h-4 w-4 text-muted-foreground" />
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
