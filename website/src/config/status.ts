export type AppStatus = 'running' | 'paused' | 'error' | 'idle' | 'healthy' | 'unhealthy';

export interface StatusMeta {
  labelKey: string;
  className: string;
  dotClassName: string;
}

export const STATUS_META: Record<AppStatus, StatusMeta> = {
  running: {
    labelKey: 'common.status.running',
    className: 'bg-emerald-500/10 text-emerald-500 border-emerald-500/20',
    dotClassName: 'bg-emerald-500',
  },
  healthy: {
    labelKey: 'common.status.healthy',
    className: 'bg-emerald-500/10 text-emerald-500 border-emerald-500/20',
    dotClassName: 'bg-emerald-500',
  },
  paused: {
    labelKey: 'common.status.paused',
    className: 'bg-amber-500/10 text-amber-500 border-amber-500/20',
    dotClassName: 'bg-amber-500',
  },
  error: {
    labelKey: 'common.status.error',
    className: 'bg-red-500/10 text-red-500 border-red-500/20',
    dotClassName: 'bg-red-500',
  },
  unhealthy: {
    labelKey: 'common.status.unhealthy',
    className: 'bg-red-500/10 text-red-500 border-red-500/20',
    dotClassName: 'bg-red-500',
  },
  idle: {
    labelKey: 'common.status.idle',
    className: 'bg-muted text-muted-foreground border-border',
    dotClassName: 'bg-muted-foreground',
  },
};

export const FLOW_STATUS_META = {
  FLOW_STATUS_RUNNING: { appStatus: 'healthy', labelKey: 'common.status.running' },
  FLOW_STATUS_PAUSED: { appStatus: 'paused', labelKey: 'common.status.paused' },
  FLOW_STATUS_ERROR: { appStatus: 'unhealthy', labelKey: 'common.status.error' },
  FLOW_STATUS_UNSPECIFIED: { appStatus: 'idle', labelKey: 'common.status.idle' },
} as const satisfies Record<string, { appStatus: AppStatus; labelKey: string }>;

export function statusForFlow(value: string | null | undefined): AppStatus {
  return FLOW_STATUS_META[value as keyof typeof FLOW_STATUS_META]?.appStatus ?? 'idle';
}

export function labelKeyForFlowStatus(value: string | null | undefined): string {
  return FLOW_STATUS_META[value as keyof typeof FLOW_STATUS_META]?.labelKey ?? 'common.status.idle';
}
