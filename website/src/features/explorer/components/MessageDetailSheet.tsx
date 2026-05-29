import { Copy } from 'lucide-react';
import { toast } from 'sonner';
import { useTranslation } from 'react-i18next';

import { Button } from '@/components/ui/button';
import { JsonViewer } from '@/components/ui/json-viewer';
import { Sheet, SheetContent, SheetHeader, SheetTitle } from '@/components/ui/sheet';
import type { MessageItem } from '@/types/api';
import { decodePayload, formatBytes, formatTime, messageSize, parseSubject } from '../shared';

export function MessageDetailSheet({
  message,
  onOpenChange,
}: {
  message: MessageItem | null;
  onOpenChange: (open: boolean) => void;
}) {
  const { t } = useTranslation();
  const raw = message ? decodePayload(message.data) : '';
  const parsedSubject = message ? parseSubject(message.subject) : null;
  const json = parseJSON(raw);

  const copy = async (value: string) => {
    await navigator.clipboard.writeText(value);
    toast.success(t('explorer.copied'));
  };

  return (
    <Sheet open={!!message} onOpenChange={onOpenChange}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-2xl">
        <SheetHeader className="border-b pb-4">
          <SheetTitle className="break-all font-mono text-sm">{message?.subject}</SheetTitle>
        </SheetHeader>

        {message && (
          <div className="space-y-5 py-4">
            <section className="grid grid-cols-2 gap-3 text-xs">
              <Info label={t('explorer.sequence')} value={String(message.sequence)} />
              <Info label={t('explorer.topic')} value={parsedSubject?.topic || '-'} />
              <Info label={t('explorer.partitionId')} value={parsedSubject?.partition || '-'} />
              <Info label={t('explorer.timestamp')} value={formatTime(message.timestamp)} />
              <Info label={t('explorer.source')} value={parsedSubject?.sourceId || '-'} />
              <Info label={t('explorer.size')} value={formatBytes(messageSize(message.data))} />
            </section>

            <section>
              <div className="mb-2 flex items-center justify-between">
                <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                  {t('explorer.payload')}
                </h3>
                <Button size="xs" variant="outline" onClick={() => copy(raw)}>
                  <Copy className="h-3 w-3" />
                </Button>
              </div>
              {json ? (
                <JsonViewer data={json} />
              ) : (
                <pre className="max-h-[420px] overflow-auto rounded border border-border bg-muted/20 p-3 text-xs">
                  {raw}
                </pre>
              )}
            </section>

            <section>
              <div className="mb-2 flex items-center justify-between">
                <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                  {t('explorer.headers')}
                </h3>
                <Button
                  size="xs"
                  variant="outline"
                  onClick={() => copy(JSON.stringify(message.headers || {}, null, 2))}
                >
                  <Copy className="h-3 w-3" />
                </Button>
              </div>
              <JsonViewer data={message.headers || {}} />
            </section>
          </div>
        )}
      </SheetContent>
    </Sheet>
  );
}

function Info({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0 rounded border border-border bg-muted/20 p-3">
      <div className="text-[10px] uppercase tracking-wide text-muted-foreground">{label}</div>
      <div className="mt-1 truncate font-mono text-foreground">{value}</div>
    </div>
  );
}

function parseJSON(value: string): unknown | null {
  if (!value) return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}
