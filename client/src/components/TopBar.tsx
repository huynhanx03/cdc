"use client";

import { useEffect, useState } from "react";
import { healthCheckAction } from "@/lib/actions";
import type { HealthCheckResponse } from "@/lib/grpc";
import { 
  Bell, 
  Search, 
  Terminal, 
  Clock, 
  BadgeCheck, 
  Activity,
  History,
  ShieldCheck
} from "lucide-react";

export default function TopBar() {
  const [health, setHealth] = useState<HealthCheckResponse | null>(null);

  useEffect(() => {
    const checkHealth = async () => {
      try {
        const h = await healthCheckAction();
        setHealth(h);
      } catch (err) {
        console.error("health check fail:", err);
        setHealth(null);
      }
    };
    checkHealth();
    const timer = setInterval(checkHealth, 30000);
    return () => clearInterval(timer);
  }, []);

  const formatUptime = (seconds: string | number) => {
    const s = typeof seconds === "string" ? parseInt(seconds, 10) : seconds;
    if (isNaN(s) || s <= 0) return "—";
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    if (h > 0) return `${h}h ${m}m ${s % 60}s`;
    return `${m}m ${s % 60}s`;
  };

  return (
    <header className="h-16 flex items-center justify-between px-8 border-b border-white/5 bg-black/20 backdrop-blur-lg sticky top-0 z-50">
      {/* Search Bar / Insight Area */}
      <div className="flex-1 max-w-xl group">
        <div className="relative flex items-center w-full">
          <Search className="absolute left-3 w-4 h-4 text-slate-500 group-focus-within:text-blue-400 transition-colors" />
          <input
            type="text"
            placeholder="Quick search through topics, messages..."
            className="w-full bg-white/[0.03] border border-white/10 rounded-xl py-2 pl-10 pr-4 text-sm font-medium text-white placeholder:text-slate-600 focus:outline-none focus:border-blue-500/50 focus:bg-white/[0.05] transition-all"
          />
          <div className="absolute right-3 flex items-center gap-1 opacity-50 group-focus-within:opacity-100 transition-opacity">
            <kbd className="px-1.5 py-0.5 rounded border border-white/10 bg-white/5 text-[10px] text-slate-500 font-mono">⌘</kbd>
            <kbd className="px-1.5 py-0.5 rounded border border-white/10 bg-white/5 text-[10px] text-slate-500 font-mono">K</kbd>
          </div>
        </div>
      </div>

      {/* Right Side Info Badges */}
      <div className="flex items-center gap-4 ml-6">
        <div className="flex items-center gap-6 px-4 py-1.5 rounded-full border border-white/5 bg-white/[0.02] text-xs font-medium">
          {health ? (
            <>
              <div className="flex items-center gap-2 group transition-colors hover:text-emerald-400 cursor-default">
                <ShieldCheck className="w-3.5 h-3.5 text-emerald-500" />
                <span className="text-slate-300">v{health.version}</span>
              </div>
              
              <div className="flex items-center gap-2 group transition-colors hover:text-blue-400 cursor-default">
                <History className="w-3.5 h-3.5 text-blue-500" />
                <span className="text-slate-300">Uptime: {formatUptime(health.uptime)}</span>
              </div>
            </>
          ) : (
            <div className="flex items-center gap-2 text-rose-500 animate-pulse">
              <Activity className="w-3.5 h-3.5" />
              <span>Offline</span>
            </div>
          )}
        </div>

        {/* Action Icons */}
        <div className="flex items-center gap-2">
          <button className="flex items-center justify-center p-2 rounded-xl border border-white/5 bg-white/[0.02] hover:bg-white/[0.08] transition-all text-slate-400 hover:text-white">
            <Bell className="w-4 h-4" />
          </button>
          
          <button className="flex items-center justify-center p-2 rounded-xl border border-white/5 bg-white/[0.02] hover:bg-white/[0.08] transition-all text-slate-400 hover:text-white">
            <Terminal className="w-4 h-4" />
          </button>
        </div>
      </div>
    </header>
  );
}
