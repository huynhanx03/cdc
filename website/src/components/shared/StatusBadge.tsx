import { useTranslation } from 'react-i18next';
import { Badge } from '@/components/ui/badge';
import { STATUS_META, type AppStatus } from '@/config/status';
import { cn } from '@/lib/utils';

export type Status = AppStatus;

interface StatusBadgeProps {
  status: Status;
  label?: string;
  showDot?: boolean;
  className?: string;
}

/** Status badge — colored indicator for component state. */
export function StatusBadge({ status, label, showDot = true, className }: StatusBadgeProps) {
  const { t } = useTranslation();
  const config = STATUS_META[status] ?? STATUS_META.idle;

  return (
    <Badge
      variant="outline"
      className={cn('gap-1.5 font-medium', config.className, className)}
    >
      {showDot && (
        <span className={cn('h-1.5 w-1.5 rounded-full', config.dotClassName)} />
      )}
      {label || t(config.labelKey)}
    </Badge>
  );
}
