import { Link, useLocation } from 'react-router-dom';
import { ChevronRight, Home } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { cn } from '@/lib/utils';

/** Route-to-label mapping for breadcrumb display. */
const SEGMENT_LABELS: Record<string, string> = {
  explorer: 'nav.explorer',
  topics: 'nav.topics',
  partitions: 'explorer.partitions',
  consumers: 'nav.consumers',
  dlq: 'nav.dlq',
  manager: 'nav.manager',
  sources: 'nav.sources',
  sinks: 'nav.sinks',
  flows: 'nav.flows',
};

/** Breadcrumb — auto-generated from current route path. */
export function Breadcrumb() {
  const { t } = useTranslation();
  const location = useLocation();
  const segments = location.pathname.split('/').filter(Boolean);

  if (segments.length === 0) {
    return (
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Home className="h-4 w-4" />
        <span className="font-medium text-foreground">{t('nav.dashboard')}</span>
      </div>
    );
  }

  return (
    <nav className="flex items-center gap-1 text-sm" aria-label={t('nav.breadcrumb')}>
      <Link
        to="/"
        className="flex items-center text-muted-foreground transition-colors hover:text-foreground"
      >
        <Home className="h-4 w-4" />
      </Link>

      {segments.map((segment, i) => {
        const isPartitionGroup =
          segments[0] === 'explorer' && segments[1] === 'topics' && segment === 'partitions';
        const path = isPartitionGroup
          ? '/' + segments.slice(0, 3).join('/')
          : '/' + segments.slice(0, i + 1).join('/');
        const isLast = i === segments.length - 1;
        const labelKey = SEGMENT_LABELS[segment];
        const label = labelKey ? t(labelKey) : decodeURIComponent(segment);

        return (
          <div key={path} className="flex items-center gap-1">
            <ChevronRight className="h-3.5 w-3.5 text-muted-foreground/60" />
            {isLast ? (
              <span className={cn('font-medium text-foreground')}>{label}</span>
            ) : (
              <Link
                to={path}
                className="text-muted-foreground transition-colors hover:text-foreground"
              >
                {label}
              </Link>
            )}
          </div>
        );
      })}
    </nav>
  );
}
