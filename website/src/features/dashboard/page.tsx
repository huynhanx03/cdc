import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router-dom';
import {
  Activity,
  Timer,
  AlertTriangle,
  MailWarning,
  Database,
  HardDrive,
  GitBranch,
  Zap,
  RefreshCw,
} from 'lucide-react';
import { Skeleton } from '@/components/ui/skeleton';
import { MetricCard } from '@/components/shared/MetricCard';
import { StatusBadge } from '@/components/shared/StatusBadge';
import { SystemHealthBar } from './components/SystemHealthBar';
import { ROUTES } from '@/config/routes';
import {
  useHealth,
  useDashboardSummary,
} from '@/lib/query/dashboard';
import { formatNumber, formatDuration, formatPercent } from '@/lib/format';

function isHealthyStatus(status?: string) {
  return status === 'healthy' || status === 'ok' || status === 'up';
}

/** Dashboard page — overview cards backed by grouped dashboard API queries. */
export default function DashboardPage() {
  const { t } = useTranslation();
  const navigate = useNavigate();

  const {
    data: health,
    isLoading: healthLoading,
    isFetching: healthFetching,
  } = useHealth();
  const {
    data: summary,
    isLoading: summaryLoading,
    isFetching: summaryFetching,
  } = useDashboardSummary();
  const healthy = isHealthyStatus(health?.status);
  const isRefreshing = healthFetching || summaryFetching;
  const inventory = summary?.inventory;
  const telemetry = summary?.telemetry;
  const dlqCount = telemetry?.failure_count ?? 0;

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-1 sm:flex-row sm:items-center sm:gap-4">
        <h1 className="text-2xl font-bold tracking-tight text-foreground">
          {t('dashboard.title')}
        </h1>
        {health ? (
          <div className="flex items-center gap-3 text-xs text-muted-foreground sm:mt-1">
            <StatusBadge status={healthy ? 'healthy' : 'unhealthy'} />
            {health.version && (
              <span className="font-mono bg-accent/30 text-accent-foreground px-1.5 py-0.5 rounded text-[10px] font-semibold">
                v{health.version}
              </span>
            )}
            {health.uptime !== undefined && health.uptime !== null && (
              <span className="flex items-center gap-1">
                {t('dashboard.uptime')}:
                <span className="font-semibold text-foreground">
                  {formatDuration(health.uptime)}
                </span>
              </span>
            )}
            {isRefreshing && (
              <RefreshCw className="h-3 w-3 animate-spin text-muted-foreground/70" />
            )}
          </div>
        ) : healthLoading ? (
          <Skeleton className="h-5 w-48 sm:mt-1" />
        ) : (
          <div className="flex items-center gap-3 text-xs text-muted-foreground sm:mt-1">
            <StatusBadge status="unhealthy" />
          </div>
        )}
      </div>

      <div className="space-y-2">
        <h2 className="text-[11px] font-bold uppercase tracking-wider text-muted-foreground/80">
          {t('dashboard.systemInventory')}
        </h2>
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {summaryLoading ? (
            Array.from({ length: 4 }).map((_, i) => (
              <Skeleton key={i} className="h-[108px]" />
            ))
          ) : (
            <>
              <MetricCard
                title={t('dashboard.activeSources')}
                value={inventory?.sources_count ?? 0}
                icon={Database}
                iconClassName="bg-blue-500/10 text-blue-500"
                onClick={() => navigate(ROUTES.MANAGER_SOURCES)}
              />
              <MetricCard
                title={t('dashboard.activeSinks')}
                value={inventory?.sinks_count ?? 0}
                icon={HardDrive}
                iconClassName="bg-indigo-500/10 text-indigo-500"
                onClick={() => navigate(ROUTES.MANAGER_SINKS)}
              />
              <MetricCard
                title={t('dashboard.activeFlows')}
                value={inventory?.flows_count ?? 0}
                icon={GitBranch}
                iconClassName="bg-amber-500/10 text-amber-500"
                onClick={() => navigate(ROUTES.MANAGER_FLOWS)}
              />
              <MetricCard
                title={t('dashboard.totalSyncedEvents')}
                value={formatNumber(telemetry?.total_synced_events ?? 0)}
                icon={Zap}
                iconClassName="bg-emerald-500/10 text-emerald-500"
              />
            </>
          )}
        </div>
      </div>

      <div className="space-y-2">
        <h2 className="text-[11px] font-bold uppercase tracking-wider text-muted-foreground/80">
          {t('dashboard.liveTelemetry')}
        </h2>
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {summaryLoading ? (
            Array.from({ length: 4 }).map((_, i) => (
              <Skeleton key={i} className="h-[108px]" />
            ))
          ) : (
            <>
              <MetricCard
                title={t('dashboard.throughput')}
                value={formatNumber(telemetry?.throughput ?? 0)}
                unit={t('dashboard.eventsPerSec')}
                icon={Activity}
                iconClassName="bg-cyan-500/10 text-cyan-500"
              />
              <MetricCard
                title={t('dashboard.latency')}
                value={(telemetry?.latency_p99 ?? 0).toFixed(1)}
                unit={t('dashboard.ms')}
                icon={Timer}
                iconClassName="bg-violet-500/10 text-violet-500"
              />
              <MetricCard
                title={t('dashboard.errorRate')}
                value={formatPercent(telemetry?.error_rate ?? 0)}
                icon={AlertTriangle}
                iconClassName="bg-yellow-500/10 text-yellow-500"
              />
              <MetricCard
                title={t('dashboard.dlqCount')}
                value={formatNumber(telemetry?.failure_count ?? 0)}
                icon={MailWarning}
                iconClassName="bg-red-500/10 text-red-500"
              />
            </>
          )}
        </div>
      </div>

      <SystemHealthBar
        natsConnected={telemetry?.nats_healthy ?? false}
        channelUtilPercent={telemetry?.channel_utilization ?? 0}
        activeWorkers={telemetry?.active_workers ?? 0}
        dlqCount={dlqCount}
        onReprocessDlq={() => navigate(ROUTES.EXPLORER_DLQ)}
      />
    </div>
  );
}
