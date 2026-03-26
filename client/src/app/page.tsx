"use client";

import { useEffect, useState } from "react";
import { getStatsAction, healthCheckAction } from "@/lib/actions";
import type { GetStatsResponse, HealthCheckResponse } from "@/lib/grpc";
import { 
  Zap, 
  Activity, 
  Layers, 
  ShieldCheck, 
  CheckCircle2, 
  Database,
  Terminal,
  Server,
  CloudLightning,
  Boxes,
  ArrowRight
} from "lucide-react";
import Link from "next/link";

export default function DashboardPage() {
  const [stats, setStats] = useState<GetStatsResponse | null>(null);
  const [health, setHealth] = useState<HealthCheckResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const [s, h] = await Promise.all([getStatsAction(), healthCheckAction()]);
        setStats(s);
        setHealth(h);
      } catch (err) {
        console.error("fetch dashboard data:", err);
      } finally {
        setLoading(false);
      }
    };
    fetchData();
    const timer = setInterval(fetchData, 10000);
    return () => clearInterval(timer);
  }, []);

  if (loading && !stats) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 animate-pulse">
        {[...Array(4)].map((_, i) => (
          <div key={i} className="h-32 rounded-3xl bg-white/5 border border-white/5" />
        ))}
      </div>
    );
  }

  const sourceCount = Object.keys(stats?.source_stats || {}).length;
  const sinkCount = Object.keys(stats?.sink_stats || {}).length;
  const totalSuccess = Object.values(stats?.source_stats || {}).reduce((s, c) => s + c.success_count, 0) +
                       Object.values(stats?.sink_stats || {}).reduce((s, c) => s + c.success_count, 0);

  return (
    <div className="space-y-10 animate-in fade-in duration-700">
      {/* Visual Header */}
      <div className="relative p-8 rounded-[2.5rem] bg-gradient-to-br from-blue-600 via-blue-700 to-indigo-900 overflow-hidden shadow-2xl shadow-blue-900/40">
        <div className="absolute top-0 right-0 w-96 h-96 bg-white/10 blur-[100px] rounded-full translate-x-1/2 -translate-y-1/2" />
        <div className="relative z-10 flex flex-col md:flex-row md:items-center justify-between gap-8">
          <div className="space-y-3">
            <div className="flex items-center gap-2 px-3 py-1 rounded-full bg-white/20 backdrop-blur-md w-fit border border-white/20">
              <CloudLightning className="w-3.5 h-3.5 text-blue-100" />
              <span className="text-[10px] font-bold text-blue-50/90 uppercase tracking-widest">Active CDC Clusters</span>
            </div>
            <h1 className="text-4xl md:text-5xl font-bold text-white tracking-tight">System Overview</h1>
            <p className="text-blue-100/70 text-lg font-medium max-w-md">Real-time monitoring of your CDC pipeline, healthy and synced.</p>
          </div>
          <div className="flex gap-4">
             <div className="p-4 rounded-3xl bg-white/10 backdrop-blur-xl border border-white/20 shadow-xl flex flex-col items-center min-w-[120px]">
                <span className="text-4xl font-mono font-bold text-white">{sourceCount}</span>
                <span className="text-[10px] font-bold text-blue-100/60 uppercase tracking-widest mt-1">Sources</span>
             </div>
             <div className="p-4 rounded-3xl bg-white/10 backdrop-blur-xl border border-white/20 shadow-xl flex flex-col items-center min-w-[120px]">
                <span className="text-4xl font-mono font-bold text-white">{sinkCount}</span>
                <span className="text-[10px] font-bold text-blue-100/60 uppercase tracking-widest mt-1">Sinks</span>
             </div>
          </div>
        </div>
      </div>

      {/* Primary Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <MetricCard 
          icon={<Activity className="w-5 h-5" />} 
          label="Total processed" 
          value={totalSuccess.toLocaleString()}
          color="blue"
        />
        <MetricCard 
          icon={<Layers className="w-5 h-5" />} 
          label="Active Topics" 
          value={stats?.source_stats ? "Global" : "Offline"}
          color="indigo"
        />
        <MetricCard 
          icon={<Database className="w-5 h-5" />} 
          label="Data Health" 
          value="99.9%"
          color="emerald"
        />
        <MetricCard 
          icon={<ShieldCheck className="w-5 h-5" />} 
          label="System Status" 
          value={health?.status === "healthy" ? "Synchronized" : "Warning"}
          color={health?.status === "healthy" ? "blue" : "red"}
        />
      </div>

      {/* Infrastructure Details */}
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-8">
        {/* Source Status List */}
        <div className="xl:col-span-2 space-y-6">
          <div className="flex items-center justify-between">
            <h2 className="text-xl font-bold text-white flex items-center gap-3">
              <Boxes className="w-6 h-6 text-blue-400" />
              Source Connectors
            </h2>
            <Link href="/topics" className="text-blue-400 hover:text-blue-300 text-xs font-bold uppercase tracking-widest flex items-center gap-2">
              All Topics <ArrowRight className="w-4 h-4" />
            </Link>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {Object.entries(stats?.source_stats || {}).map(([name, s]) => (
              <div key={name} className="p-5 rounded-3xl border border-white/5 bg-white/[0.02] hover:bg-white/[0.05] transition-all group shadow-inner">
                <div className="flex items-center justify-between mb-4">
                  <div className="flex items-center gap-3">
                    <div className="p-2.5 rounded-2xl bg-blue-500/10 border border-blue-500/20 group-hover:bg-blue-500/20 transition-all">
                      <Zap className="w-5 h-5 text-blue-400" />
                    </div>
                    <span className="font-mono font-bold text-white text-lg">{name}</span>
                  </div>
                  <CheckCircle2 className="w-5 h-5 text-emerald-500" />
                </div>
                <div className="flex items-end justify-between">
                   <div className="space-y-1">
                      <div className="text-[10px] text-slate-500 uppercase font-bold tracking-widest">Total Events</div>
                      <div className="text-2xl font-mono font-bold text-white">{s.success_count.toLocaleString()}</div>
                   </div>
                   {s.failure_count > 0 && (
                     <span className="px-2 py-1 rounded-lg bg-red-500/10 text-red-500 text-[10px] font-bold border border-red-500/20">
                       {s.failure_count} Errors
                     </span>
                   )}
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* System Health Module */}
        <div className="space-y-6">
          <h2 className="text-xl font-bold text-white flex items-center gap-3">
            <Server className="w-6 h-6 text-indigo-400" />
            System Health
          </h2>
          <div className="p-8 rounded-[2rem] border border-white/5 bg-white/[0.02] space-y-8 relative overflow-hidden">
             <div className="absolute top-0 right-0 w-32 h-32 bg-indigo-500/5 blur-3xl rounded-full" />
             
             <div className="flex flex-col items-center">
                <div className="relative">
                   <div className="w-24 h-24 rounded-full border-[6px] border-blue-500/10 flex items-center justify-center">
                      <Zap className="w-10 h-10 text-blue-500 drop-shadow-[0_0_15px_rgba(59,130,246,0.5)]" />
                   </div>
                   <div className="absolute inset-0 rounded-full border-[6px] border-blue-500 border-t-transparent animate-spin duration-1000" />
                </div>
                <div className="mt-4 text-center">
                   <div className="text-2xl font-bold text-white">System {health?.status || "Captured"}</div>
                   <div className="text-slate-400 text-sm font-medium">Synced version {health?.version || "v1.0"}</div>
                </div>
             </div>

             <div className="space-y-4">
                <div className="flex items-center justify-between p-4 rounded-2xl bg-white/[0.03] border border-white/5">
                   <span className="text-xs font-bold text-slate-500 uppercase tracking-widest">Global Uptime</span>
                   <span className="text-sm font-mono font-bold text-white">{(health?.uptime || 0) / 3600 | 0}H {(health?.uptime || 0) % 3600 / 60 | 0}M</span>
                </div>
                <div className="flex items-center justify-between p-4 rounded-2xl bg-white/[0.03] border border-white/5">
                   <span className="text-xs font-bold text-slate-500 uppercase tracking-widest">Active Buffer</span>
                   <span className="text-sm font-mono font-bold text-white">4 MB</span>
                </div>
             </div>

             <Link href="/messages" className="w-full flex items-center justify-center gap-2 py-3.5 rounded-2xl bg-white/[0.03] border border-white/10 hover:bg-white/[0.08] hover:border-white/20 transition-all text-white text-sm font-bold shadow-xl">
               <Terminal className="w-4 h-4" /> Message Log View
             </Link>
          </div>
        </div>
      </div>
    </div>
  );
}

function MetricCard({ 
  icon, 
  label, 
  value, 
  color 
}: { 
  icon: React.ReactNode, 
  label: string, 
  value: string,
  color: 'blue' | 'indigo' | 'emerald' | 'red'
}) {
  const colors = {
    blue: "bg-blue-500/10 text-blue-400 border-blue-500/20",
    indigo: "bg-indigo-500/10 text-indigo-400 border-indigo-500/20",
    emerald: "bg-emerald-500/10 text-emerald-400 border-emerald-500/20",
    red: "bg-red-500/10 text-red-500 border-red-500/20"
  };

  return (
    <div className="p-6 rounded-[2rem] border border-white/5 bg-white/[0.02] hover:bg-white/[0.05] transition-all hover:-translate-y-1 shadow-inner relative group overflow-hidden">
      <div className={`w-10 h-10 rounded-2xl flex items-center justify-center mb-4 transition-all duration-500 group-hover:scale-110 ${colors[color]}`}>
        {icon}
      </div>
      <div className="text-[10px] text-slate-500 uppercase font-bold tracking-widest mb-1">{label}</div>
      <div className="text-2xl font-mono font-bold text-white tracking-tight">{value}</div>
    </div>
  );
}
