import type { ReactNode } from 'react';

import { cn } from '@/lib/utils';

interface MetricTileProps {
  label: ReactNode;
  value: ReactNode;
  icon?: ReactNode;
  className?: string;
}

export function MetricTile({ label, value, icon, className }: MetricTileProps) {
  return (
    <div className={cn('rounded-lg border border-border bg-card p-4', className)}>
      <div className="flex items-center justify-between gap-3">
        <div className="min-w-0 text-xs font-semibold uppercase text-muted-foreground">{label}</div>
        {icon ? <div className="shrink-0 text-muted-foreground">{icon}</div> : null}
      </div>
      <div className="mt-2 truncate font-mono text-xl font-semibold text-foreground">{value}</div>
    </div>
  );
}
