import { useTranslation } from 'react-i18next';

import { Badge } from '@/components/ui/badge';

export function StatusBadge({ status }: { status: 'sent' | 'dlq' | 'pending' | 'active' | 'lagging' }) {
  const { t } = useTranslation();
  const className =
    status === 'sent' || status === 'active'
      ? 'border-emerald-500/25 bg-emerald-500/10 text-emerald-700 dark:text-emerald-400'
      : status === 'dlq'
        ? 'border-rose-500/25 bg-rose-500/10 text-rose-700 dark:text-rose-400'
        : 'border-amber-500/25 bg-amber-500/10 text-amber-700 dark:text-amber-400';

  return (
    <Badge variant="outline" className={className}>
      {t(`explorer.status.${status}`)}
    </Badge>
  );
}
