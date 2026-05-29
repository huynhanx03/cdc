import { RotateCcw } from 'lucide-react';
import { useTranslation } from 'react-i18next';

import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import type { DLQDryRunResponse } from '@/types/api';

export function ReprocessConfirmDialog({
  open,
  preview,
  loading,
  onOpenChange,
  onConfirm,
}: {
  open: boolean;
  preview: DLQDryRunResponse | null;
  loading?: boolean;
  onOpenChange: (open: boolean) => void;
  onConfirm: () => void;
}) {
  const { t } = useTranslation();

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>{t('explorer.confirmDlqReprocess')}</DialogTitle>
          <DialogDescription>
            {t('explorer.dlqPreviewSummary', {
              count: preview?.preview_count ?? 0,
              blocked: preview?.blocked_count ?? 0,
            })}
          </DialogDescription>
        </DialogHeader>
        <div className="max-h-[360px] overflow-auto rounded-lg border border-border">
          {(preview?.preview_items ?? []).map((item) => (
            <div key={item.dlq_id} className="border-b border-border p-3 last:border-b-0">
              <div className="flex items-center justify-between gap-3">
                <div className="min-w-0">
                  <div className="truncate font-mono text-xs font-semibold">{item.original_subject}</div>
                  <div className="mt-1 text-xs text-muted-foreground">{item.reason || '-'}</div>
                </div>
                <div className="shrink-0 rounded border border-border px-2 py-1 text-xs">{item.duplicate_risk}</div>
              </div>
            </div>
          ))}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>{t('common.cancel')}</Button>
          <Button onClick={onConfirm} disabled={loading || !preview?.confirm_token}>
            <RotateCcw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
            {t('explorer.reprocessSelected')}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
