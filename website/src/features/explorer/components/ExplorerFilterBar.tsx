import { X } from 'lucide-react';
import { useTranslation } from 'react-i18next';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import type { ExplorerMessageFilters } from '@/types/api';

interface ExplorerFilterBarProps {
  value: ExplorerMessageFilters;
  onChange: (value: ExplorerMessageFilters) => void;
}

const OPERATION_OPTIONS = [
  { value: 'all', labelKey: 'explorer.filter.allOps' },
  { value: 'c', labelKey: 'explorer.filter.create' },
  { value: 'u', labelKey: 'explorer.filter.update' },
  { value: 'd', labelKey: 'explorer.filter.delete' },
] as const;

const SORT_OPTIONS = [
  { value: 'newest', labelKey: 'explorer.filter.newest' },
  { value: 'oldest', labelKey: 'explorer.filter.oldest' },
] as const satisfies ReadonlyArray<{ value: NonNullable<ExplorerMessageFilters['sort']>; labelKey: string }>;

export function ExplorerFilterBar({ value, onChange }: ExplorerFilterBarProps) {
  const { t } = useTranslation();
  const patch = (next: Partial<ExplorerMessageFilters>) => onChange({ ...value, ...next, page: 1 });
  const clear = () => onChange({ sort: 'newest', page: 1, limit: value.limit ?? 50 });

  return (
    <div className="grid gap-3 rounded-lg border border-border bg-card p-3 lg:grid-cols-6">
      <Select value={value.op ?? 'all'} onValueChange={(op) => patch({ op: op && op !== 'all' ? op : undefined })}>
        <SelectTrigger className="w-full">
          <SelectValue placeholder={t('explorer.operation')} />
        </SelectTrigger>
        <SelectContent>
          {OPERATION_OPTIONS.map((option) => (
            <SelectItem key={option.value} value={option.value}>{t(option.labelKey)}</SelectItem>
          ))}
        </SelectContent>
      </Select>
      <Input value={value.sequence_min ?? ''} onChange={(event) => patch({ sequence_min: event.target.value || undefined })} placeholder={t('explorer.filter.sequenceMin')} />
      <Input value={value.sequence_max ?? ''} onChange={(event) => patch({ sequence_max: event.target.value || undefined })} placeholder={t('explorer.filter.sequenceMax')} />
      <Input value={value.text_contains ?? ''} onChange={(event) => patch({ text_contains: event.target.value || undefined })} placeholder={t('explorer.filter.payloadContains')} />
      <Select value={value.sort ?? 'newest'} onValueChange={(sort) => patch({ sort: (sort || 'newest') as ExplorerMessageFilters['sort'] })}>
        <SelectTrigger className="w-full">
          <SelectValue placeholder={t('explorer.filter.sort')} />
        </SelectTrigger>
        <SelectContent>
          {SORT_OPTIONS.map((option) => (
            <SelectItem key={option.value} value={option.value}>{t(option.labelKey)}</SelectItem>
          ))}
        </SelectContent>
      </Select>
      <Button type="button" variant="outline" onClick={clear}>
        <X className="h-4 w-4" />
        {t('explorer.filter.clear')}
      </Button>
      <Input value={value.header_key ?? ''} onChange={(event) => patch({ header_key: event.target.value || undefined })} placeholder={t('explorer.filter.headerKey')} />
      <Input value={value.header_value ?? ''} onChange={(event) => patch({ header_value: event.target.value || undefined })} placeholder={t('explorer.filter.headerValue')} />
      <Input value={value.json_path ?? ''} onChange={(event) => patch({ json_path: event.target.value || undefined })} placeholder={t('explorer.filter.jsonPath')} />
      <Input value={value.json_equals ?? ''} onChange={(event) => patch({ json_equals: event.target.value || undefined })} placeholder={t('explorer.filter.jsonEquals')} />
      <Input value={value.timestamp_from ?? ''} onChange={(event) => patch({ timestamp_from: event.target.value || undefined })} placeholder={t('explorer.filter.fromMs')} />
      <Input value={value.timestamp_to ?? ''} onChange={(event) => patch({ timestamp_to: event.target.value || undefined })} placeholder={t('explorer.filter.toMs')} />
    </div>
  );
}
