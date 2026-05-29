import { Eye } from 'lucide-react';
import { useTranslation } from 'react-i18next';

import { Button } from '@/components/ui/button';
import type { DLQMessage } from '@/types/api';

export function DLQDryRunButton({
  selected,
  loading,
  onPreview,
}: {
  selected: DLQMessage[];
  loading?: boolean;
  onPreview: () => void;
}) {
  const { t } = useTranslation();

  return (
    <Button size="sm" onClick={onPreview} disabled={loading || selected.length === 0}>
      <Eye className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
      {t('explorer.previewSelected', { count: selected.length })}
    </Button>
  );
}
