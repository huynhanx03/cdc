import { useCallback, useState, useEffect, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import {
  Database,
  RefreshCw,
  CheckCircle,
  AlertTriangle,
  Plus,
} from 'lucide-react';
import { toast } from 'sonner';

import {
  useAddSource,
  useUpdateSource,
  useTestSourceConnection,
  useConfig,
} from '@/lib/query/manager';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  SOURCE_CONNECTOR_TYPES,
  connectorLabel,
  defaultConnectorPort,
  defaultConnectorUsername,
} from '@/config/connectors';
import type { SourceConfig, TestConnectionResponse } from '@/types/api';

interface SourceFormProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  sourceToEdit: SourceConfig | null;
}

export function SourceForm({ open, onOpenChange, sourceToEdit }: SourceFormProps) {
  const { t } = useTranslation();

  const isEdit = !!sourceToEdit;

  // Mutations & Config
  const addMutation = useAddSource();
  const updateMutation = useUpdateSource();
  const testMutation = useTestSourceConnection();
  const { data: configData } = useConfig();

  const availableTypes = useMemo(() => {
    if (configData?.available_sources && configData.available_sources.length > 0) {
      return configData.available_sources.filter((t): t is SourceConfig['type'] =>
        SOURCE_CONNECTOR_TYPES.includes(t as SourceConfig['type']),
      );
    }
    return [...SOURCE_CONNECTOR_TYPES];
  }, [configData]);

  // Form State — simple DB connection only
  const [name, setName] = useState('');
  const [type, setType] = useState<SourceConfig['type']>('postgres');
  const [host, setHost] = useState('');
  const [port, setPort] = useState(5432);
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [database, setDatabase] = useState('');

  // Connection testing state
  const [testResult, setTestResult] = useState<TestConnectionResponse | null>(null);

  const resetForm = useCallback(() => {
    setName('');
    setType((availableTypes[0] || 'postgres') as SourceConfig['type']);
    setHost('');
    setPort(defaultConnectorPort(availableTypes[0] || 'postgres'));
    setUsername('');
    setPassword('');
    setDatabase('');
    setTestResult(null);
  }, [availableTypes]);

  // Sync edit values
  useEffect(() => {
    if (sourceToEdit) {
      setName(sourceToEdit.name || '');
      setType(sourceToEdit.type || 'postgres');
      setHost(sourceToEdit.host || '');
      setPort(sourceToEdit.port || defaultConnectorPort(sourceToEdit.type));
      setUsername(sourceToEdit.username || '');
      setPassword(sourceToEdit.password || '');
      setDatabase(sourceToEdit.database || '');
    } else {
      resetForm();
    }
    setTestResult(null);
  }, [sourceToEdit, open, resetForm]);

  // Adjust port based on type
  useEffect(() => {
    if (!isEdit) {
      setPort(defaultConnectorPort(type));
    }
    setTestResult(null);
  }, [type, isEdit]);

  const parsePayload = (): Partial<SourceConfig> => {
    const payload: Partial<SourceConfig> = {
      type,
      name: name || undefined,
      host,
      port: Number(port),
      username: username || undefined,
      password: password || undefined,
      database,
    };

    // Include instance_id only when editing
    if (isEdit && sourceToEdit) {
      payload.instance_id = sourceToEdit.instance_id;
    }

    return payload;
  };

  const handleTestConnection = async () => {
    if (!host || !database) {
      toast.error(t('manager.sources.toast.hostDatabaseRequired'));
      return;
    }

    try {
      const payload = parsePayload();
      const res = await testMutation.mutateAsync(payload);
      if (res.success) {
        setTestResult(null);
        toast.success(t('manager.sources.testSuccess'));
      } else {
        setTestResult(res);
        toast.error(t('manager.sources.testFailed') + `: ${res.message}`);
      }
    } catch (err) {
      toast.error(err instanceof Error ? err.message : t('manager.sources.testFailed'));
    }
  };

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!host || !database) {
      toast.error(t('manager.sources.toast.hostDatabaseRequired'));
      return;
    }

    try {
      const payload = parsePayload();
      if (isEdit) {
        await updateMutation.mutateAsync(payload as SourceConfig);
        toast.success(t('common.success'));
      } else {
        await addMutation.mutateAsync(payload);
        toast.success(t('common.success'));
      }
      onOpenChange(false);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : t('manager.sources.toast.deleteFailed'));
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader className="border-b pb-3">
          <DialogTitle className="flex items-center gap-2 text-foreground font-bold">
            <Database className="h-5 w-5 text-sky-400" />
            {isEdit ? t('manager.sources.edit') : t('manager.sources.add')}
          </DialogTitle>
          <DialogDescription className="text-xs text-muted-foreground">
            {isEdit
              ? t('manager.sources.editDesc')
              : t('manager.sources.createDesc')}
          </DialogDescription>
        </DialogHeader>

        <form onSubmit={handleSave} className="space-y-4 py-3">
          {/* Name + Type */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="text-[11px] font-semibold text-muted-foreground mb-1.5 block">
                {t('manager.sources.fields.displayName')}
              </label>
              <Input
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder={t('manager.sources.placeholders.displayName')}
                className="h-9 text-xs"
              />
            </div>
            <div>
              <label className="text-[11px] font-semibold text-muted-foreground mb-1.5 block">
                {t('manager.sources.fields.type')}
              </label>
              <Select value={type} onValueChange={(val) => setType(val as SourceConfig['type'])} disabled={isEdit}>
                <SelectTrigger className="w-full h-9 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {availableTypes.map((tName) => (
                    <SelectItem key={tName} value={tName} className="text-xs">
                      {connectorLabel(tName)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* Host + Port */}
          <div className="grid grid-cols-3 gap-4">
            <div className="col-span-2">
              <label className="text-[11px] font-semibold text-muted-foreground mb-1.5 block">
                {t('manager.sources.fields.host')}
              </label>
              <Input
                value={host}
                onChange={(e) => setHost(e.target.value)}
                placeholder={t('manager.sources.placeholders.host')}
                className="h-9 text-xs"
                required
              />
            </div>
            <div>
              <label className="text-[11px] font-semibold text-muted-foreground mb-1.5 block">
                {t('manager.sources.fields.port')}
              </label>
              <Input
                type="number"
                value={port}
                onChange={(e) => setPort(Number(e.target.value))}
                className="h-9 text-xs"
                required
              />
            </div>
          </div>

          {/* Username + Password + Database */}
          <div className="grid grid-cols-3 gap-4">
            <div>
              <label className="text-[11px] font-semibold text-muted-foreground mb-1.5 block">
                {t('manager.sources.fields.username')}
              </label>
              <Input
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder={defaultConnectorUsername(type)}
                className="h-9 text-xs"
              />
            </div>
            <div>
              <label className="text-[11px] font-semibold text-muted-foreground mb-1.5 block">
                {t('manager.sources.fields.password')}
              </label>
              <Input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder={isEdit
                  ? t('manager.sources.placeholders.editPassword')
                  : t('manager.sources.placeholders.password')}
                className="h-9 text-xs"
              />
            </div>
            <div>
              <label className="text-[11px] font-semibold text-muted-foreground mb-1.5 block">
                {t('manager.sources.fields.database')}
              </label>
              <Input
                value={database}
                onChange={(e) => setDatabase(e.target.value)}
                placeholder={t('manager.sources.placeholders.database')}
                className="h-9 text-xs"
                required
              />
            </div>
          </div>

          {/* Test connection visual results */}
          {testResult && (
            <div className={`rounded-lg border p-3.5 flex gap-3 text-xs ${
              testResult.success
                ? 'border-emerald-500/20 bg-emerald-500/5 text-emerald-400'
                : 'border-rose-500/20 bg-rose-500/5 text-rose-400'
            }`}>
              {testResult.success ? (
                <CheckCircle className="h-4 w-4 shrink-0 text-emerald-400 mt-0.5" />
              ) : (
                <AlertTriangle className="h-4 w-4 shrink-0 text-rose-400 mt-0.5" />
              )}
              <div>
                <span className="font-semibold">
                  {testResult.success
                    ? t('manager.sources.test.successTitle')
                    : t('manager.sources.test.failedTitle')}
                </span>
                <p className="opacity-90 mt-0.5 leading-relaxed">
                  {testResult.message || t('manager.sources.test.successDesc')}
                </p>
                {testResult.success && (testResult.latency_ms ?? testResult.latencyMs) !== undefined && (
                  <span className="inline-block mt-1 font-mono text-[10px] bg-emerald-950/40 border border-emerald-900 px-1.5 py-0.5 rounded">
                    {t('manager.sources.test.latency', { latency: testResult.latency_ms ?? testResult.latencyMs })}
                  </span>
                )}
              </div>
            </div>
          )}
        </form>

        <div className="flex items-center justify-between border-t pt-3">
          <Button
            type="button"
            variant="outline"
            onClick={handleTestConnection}
            disabled={testMutation.isPending}
            className="h-9 text-xs cursor-pointer"
          >
            {testMutation.isPending ? (
              <>
                <RefreshCw className="h-3.5 w-3.5 mr-1.5 animate-spin" />
                {t('manager.sources.testing')}
              </>
            ) : (
              <>
                <RefreshCw className="h-3.5 w-3.5 mr-1.5 text-sky-400" />
                {t('manager.sources.testConn')}
              </>
            )}
          </Button>

          <div className="flex gap-2">
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
              className="h-9 text-xs cursor-pointer"
            >
              {t('common.cancel')}
            </Button>
            <Button
              onClick={handleSave}
              disabled={addMutation.isPending || updateMutation.isPending}
              className="h-9 text-xs bg-sky-500 text-slate-950 hover:bg-sky-400 font-semibold cursor-pointer"
            >
              {addMutation.isPending || updateMutation.isPending ? (
                t('common.saving')
              ) : isEdit ? (
                t('common.save')
              ) : (
                <>
                  <Plus className="h-3.5 w-3.5 mr-1" />
                  {t('common.create')}
                </>
              )}
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
