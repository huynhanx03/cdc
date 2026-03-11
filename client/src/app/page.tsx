"use client";

import { useEffect, useState } from "react";
import { formatNumber, formatBytes } from "@/lib/api";
import { getStatsAction } from "@/lib/actions";
import type { StatsResponse } from "@/lib/grpc";

export default function DashboardPage() {
  const [stats, setStats] = useState<StatsResponse | null>(null);

  useEffect(() => {
    const fetchMetrics = async () => {
      try {
        const data = await getStatsAction();
        setStats(data);
      } catch (e) {
        console.error("Failed to fetch pipeline metrics:", e);
      }
    };

    fetchMetrics();
    const interval = setInterval(fetchMetrics, 3000); // Live update every 3s
    return () => clearInterval(interval);
  }, []);

  // Use optional chaining carefully since type parsing numbers can be tricky
  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Dashboard Overview</h1>
        <p className="page-subtitle">Real-time CDC pipeline statistics across all topics and partitions.</p>
      </div>

      <section className="stats-grid">
        <div className="card stat-card">
          <div className="stat-value">{formatNumber(stats?.totalEnqueued || 0)}</div>
          <div className="stat-label">Events Discovered</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value">{formatNumber(stats?.totalDequeued || 0)}</div>
          <div className="stat-label">Events Exported</div>
        </div>
      </section>

      <section className="stats-grid">
        <div className="card stat-card">
          <div className="stat-value">{formatNumber(stats?.partitionCount || 0)}</div>
          <div className="stat-label">Active Partitions</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value">{formatNumber(stats?.segmentsCount || 0)}</div>
          <div className="stat-label">Log Segments</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value">{formatBytes(stats?.totalSizeMb ? Number(stats.totalSizeMb) * 1024 * 1024 : 0)}</div>
          <div className="stat-label">Total Volume</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value">{formatNumber(stats?.pending || 0)}</div>
          <div className="stat-label">Pending / In-flight</div>
        </div>
      </section>
    </div>
  );
}
