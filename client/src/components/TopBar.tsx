"use client";

import { useEffect, useState } from "react";
import { healthCheck, type HealthCheckResponse } from "@/lib/api";

export default function TopBar() {
  const [health, setHealth] = useState<HealthCheckResponse | null>(null);

  useEffect(() => {
    const fetchHealth = async () => {
      try {
        const h = await healthCheck();
        setHealth(h);
      } catch {
        setHealth(null);
      }
    };
    fetchHealth();
    const interval = setInterval(fetchHealth, 10000);
    return () => clearInterval(interval);
  }, []);

  const formatUptime = (seconds: string | number) => {
    const s = typeof seconds === "string" ? parseInt(seconds, 10) : seconds;
    if (isNaN(s) || s <= 0) return "—";
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    if (h > 0) return `${h}h ${m}m`;
    return `${m}m ${s % 60}s`;
  };

  return (
    <header className="topbar">
      <div className="topbar-left">
        <h1 className="topbar-title">CDC Platform</h1>
      </div>
      <div className="topbar-right">
        {health ? (
          <>
            <span className="topbar-badge success">
              ● {health.status}
            </span>
            <span className="topbar-meta">v{health.version}</span>
            <span className="topbar-meta">
              uptime: {formatUptime(health.uptime)}
            </span>
          </>
        ) : (
          <span className="topbar-badge error">● offline</span>
        )}
      </div>
    </header>
  );
}
