import { useTranslation } from 'react-i18next';

import { JsonViewer } from '@/components/ui/json-viewer';

export function BeforeAfterDiff({ before, after }: { before: unknown; after: unknown }) {
  const { t } = useTranslation();

  return (
    <div className="grid gap-3 lg:grid-cols-2">
      <div className="min-w-0 rounded-lg border border-border bg-muted/20 p-3">
        <div className="mb-2 text-xs font-semibold uppercase text-muted-foreground">{t('explorer.before')}</div>
        <JsonViewer data={before ?? {}} />
      </div>
      <div className="min-w-0 rounded-lg border border-border bg-muted/20 p-3">
        <div className="mb-2 text-xs font-semibold uppercase text-muted-foreground">{t('explorer.after')}</div>
        <JsonViewer data={after ?? {}} />
      </div>
    </div>
  );
}
