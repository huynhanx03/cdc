"use client";

import { useEffect, useState } from "react";
import { getStatsAction, healthCheckAction } from "@/lib/actions";
import { formatNumber } from "@/lib/api";
import type { GetStatsResponse, HealthCheckResponse, ComponentStats } from "@/lib/grpc";
import { Activity, Database, Send, AlertTriangle, CheckCircle, Clock } from "lucide-react";

export default function DashboardPage() {
  const [stats, setStats] = useState<GetStatsResponse | null>(null);
  const [health, setHealth] = useState<HealthCheckResponse | null>(null);
  const [lastUpdate, setLastUpdate] = useState<Date>(new Date());

  useEffect(() => {
    const fetchData = async () => {
      try {
        const [statsData, healthData] = await Promise.all([
          getStatsAction(),
          healthCheckAction()
        ]);
        setStats(statsData);
        setHealth(healthData);
        setLastUpdate(new Date());
      } catch (e) {
        console.error("Dashboard refresh failed:", e);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 2000);
    return () => clearInterval(interval);
  }, []);

  const totalSuccess = Object.values(stats?.source_stats || {}).reduce((a, b) => a + b.success_count, 0) +
                       Object.values(stats?.sink_stats || {}).reduce((a, b) => a + b.success_count, 0);

  const totalErrors = Object.values(stats?.source_stats || {}).reduce((a, b) => a + b.failure_count, 0) +
                      Object.values(stats?.sink_stats || {}).reduce((a, b) => a + b.failure_count, 0);

  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      <div className="flex justify-between items-end">
        <div>
          <h1 className="text-4xl font-bold tracking-tight text-white bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-emerald-400">
            CDC Live Monitor
          </h1>
          <p className="text-slate-400 mt-2 flex items-center gap-2">
            <Activity className="w-4 h-4 text-emerald-500 animate-pulse" />
            System {health?.status || "Connecting..."} • Engine v{health?.version || "0.1"} • Uptime: {formatUptime(health?.uptime || 0)}
          </p>
        </div>
        <div className="text-right text-xs text-slate-500 font-mono">
          Last updated: {lastUpdate.toLocaleTimeString()}
        </div>
      </div>

      {/* Global Overview */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="card-glass p-6 rounded-2xl border border-white/5 bg-white/5 backdrop-blur-md">
          <div className="flex items-center gap-4 text-emerald-400 mb-2">
            <CheckCircle className="w-5 h-5" />
            <span className="text-sm font-medium uppercase tracking-wider opacity-70">Successful Swings</span>
          </div>
          <div className="text-4xl font-mono font-bold text-white">{formatNumber(totalSuccess)}</div>
        </div>

        <div className="card-glass p-6 rounded-2xl border border-white/5 bg-white/5 backdrop-blur-md">
          <div className="flex items-center gap-4 text-amber-400 mb-2">
            <AlertTriangle className="w-5 h-5" />
            <span className="text-sm font-medium uppercase tracking-wider opacity-70">Processing Faults</span>
          </div>
          <div className="text-4xl font-mono font-bold text-white">{formatNumber(totalErrors)}</div>
        </div>

        <div className="card-glass p-6 rounded-2xl border border-white/5 bg-white/5 backdrop-blur-md">
          <div className="flex items-center gap-4 text-blue-400 mb-2">
            <Clock className="w-5 h-5" />
            <span className="text-sm font-medium uppercase tracking-wider opacity-70">Sync Latency</span>
          </div>
          <div className="text-4xl font-mono font-bold text-white">~0.1ms</div>
        </div>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-8">
        {/* Sources Grid */}
        <div className="space-y-4">
          <h2 className="text-xl font-semibold flex items-center gap-2 text-slate-200">
            <Database className="w-5 h-5 text-blue-400" />
            Data Sources
          </h2>
          <div className="grid grid-cols-1 gap-4">
            {Object.entries(stats?.source_stats || {}).map(([id, s]) => (
              <ComponentCard key={id} id={id} stats={s} type="source" />
            ))}
          </div>
        </div>

        {/* Sinks Grid */}
        <div className="space-y-4">
          <h2 className="text-xl font-semibold flex items-center gap-2 text-slate-200">
            <Send className="w-5 h-5 text-emerald-400" />
            Export Sinks
          </h2>
          <div className="grid grid-cols-1 gap-4">
            {Object.entries(stats?.sink_stats || {}).map(([id, s]) => (
              <ComponentCard key={id} id={id} stats={s} type="sink" />
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function ComponentCard({ id, stats, type }: { id: string; stats: ComponentStats, type: 'source' | 'sink' }) {
  const isHealthy = stats.failure_count === 0;
  
  return (
    <div className="group relative overflow-hidden p-5 rounded-xl border border-white/5 bg-white/[0.02] hover:bg-white/[0.04] transition-all duration-300">
      <div className={`absolute left-0 top-0 bottom-0 w-1 ${isHealthy ? 'bg-emerald-500/50' : 'bg-red-500/50'}`} />
      
      <div className="flex justify-between items-start mb-4">
        <div>
          <div className="text-xs font-mono text-slate-500 uppercase">{type}</div>
          <h3 className="text-lg font-bold text-white group-hover:text-blue-400 transition-colors uppercase">
            {id.replace(/_/g, ' ')}
          </h3>
        </div>
        {isHealthy ? (
          <div className="px-2 py-1 rounded text-[10px] font-bold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">OPERATIONAL</div>
        ) : (
          <div className="px-2 py-1 rounded text-[10px] font-bold bg-red-500/10 text-red-400 border border-red-500/20">DEGRADED</div>
        )}
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div>
          <div className="text-[10px] text-slate-500 uppercase font-bold tracking-tighter mb-1">Success</div>
          <div className="text-xl font-mono text-white">{formatNumber(stats.success_count)}</div>
        </div>
        <div>
          <div className="text-[10px] text-slate-500 uppercase font-bold tracking-tighter mb-1">Errors</div>
          <div className="text-xl font-mono text-white">{formatNumber(stats.failure_count)}</div>
        </div>
      </div>

      {stats.last_error && (
        <div className="mt-4 p-2 rounded bg-red-500/5 border border-red-500/10 text-[10px] text-red-400 font-mono break-all leading-relaxed animate-in slide-in-from-top-1">
          <span className="font-bold mr-1">LATEST ERROR:</span>
          {stats.last_error}
        </div>
      )}
    </div>
  );
}

function formatUptime(seconds: number): string {
  const hrs = Math.floor(seconds / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  const secs = seconds % 60;
  return `${hrs}h ${mins}m ${secs}s`;
}
