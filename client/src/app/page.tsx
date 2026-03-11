"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";

interface Stats {
  total_enqueued: number;
  total_dequeued: number;
  pending: number;
  segments_count: number;
  total_size_mb: number;
}

interface CDCEvent {
  lsn: number;
  time: string;
  table: string; // Used as Topic
  op: string; // Insert, Update, Delete
  data: any;
  old_data: any;
}

export default function DashboardPage() {
  const router = useRouter();

  const [stats, setStats] = useState<Stats | null>(null);
  const [events, setEvents] = useState<CDCEvent[]>([]);
  const [loading, setLoading] = useState(true);

  const checkAuth = () => {
    if (typeof window !== "undefined") {
      const token = localStorage.getItem("cdc_token");
      if (!token) {
        router.push("/login");
      }
    }
  };

  const fetchMetrics = async () => {
    try {
      const [statsRes, eventsRes] = await Promise.all([
        fetch("http://localhost:8080/api/stats"),
        fetch("http://localhost:8080/api/inspect"),
      ]);

      if (statsRes.ok) {
        const statsData = await statsRes.json();
        setStats(statsData);
      }
      
      if (eventsRes.ok) {
        const eventsData = await eventsRes.json();
        setEvents(eventsData || []);
      }
    } catch (e) {
      console.error("Failed to fetch pipeline metrics:", e);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    checkAuth();
    fetchMetrics();
    const interval = setInterval(fetchMetrics, 3000); // Live update every 3s
    return () => clearInterval(interval);
  }, []);

  if (loading) {
    return (
      <div className="auth-container">
        <h2>Loading Pipeline Data...</h2>
      </div>
    );
  }

  return (
    <div className="dashboard-layout">
      {/* Sidebar */}
      <aside className="sidebar">
        <div className="sidebar-header">
          CDC Engine
        </div>
        <ul className="nav-links">
          <li className="nav-item active">Topics & Partitions</li>
          <li className="nav-item">Consumers</li>
          <li className="nav-item">Nodes</li>
        </ul>
        <div style={{ padding: "16px" }}>
          <button 
            className="btn" 
            style={{ background: "transparent", border: "1px solid var(--border-color)", color: "var(--text-secondary)" }}
            onClick={() => {
              localStorage.removeItem("cdc_token");
              router.push("/login");
            }}
          >
            Log Out
          </button>
        </div>
      </aside>

      {/* Main Content */}
      <main className="main-content">
        <div style={{ marginBottom: "32px" }}>
          <h1 className="title">Topics & Partitions Overview</h1>
          <p className="subtitle">Real-time statistics and recent messages across WAL segments.</p>
        </div>

        {/* Stats Grid */}
        <section className="stats-grid">
          <div className="glass-panel stat-card">
            <div className="stat-value">{stats?.total_enqueued || 0}</div>
            <div className="stat-label">Messages Discovered</div>
          </div>
          <div className="glass-panel stat-card">
            <div className="stat-value">{stats?.total_dequeued || 0}</div>
            <div className="stat-label">Messages Processed</div>
          </div>
          <div className="glass-panel stat-card">
            <div className="stat-value">{stats?.pending || 0}</div>
            <div className="stat-label">In-flight / Pending</div>
          </div>
          <div className="glass-panel stat-card">
            <div className="stat-value">{stats?.segments_count || 0}</div>
            <div className="stat-label">Total Partitions (WAL Segs)</div>
          </div>
        </section>

        {/* Messages Table */}
        <section>
          <h2 style={{ marginBottom: "16px" }}>Recent Messages</h2>
          <div className="table-container">
            <table>
              <thead>
                <tr>
                  <th>Topic / Table</th>
                  <th>Partition / LSN</th>
                  <th>Operation</th>
                  <th>Payload</th>
                  <th>Timestamp</th>
                </tr>
              </thead>
              <tbody>
                {events && events.length > 0 ? (
                  events.slice().reverse().map((ev, i) => (
                    <tr key={ev.lsn || i}>
                      <td><strong>{ev.table || "Unknown"}</strong></td>
                      <td>{ev.lsn}</td>
                      <td>
                        <span className="tag" style={{
                           color: ev.op === "i" ? "#2ea043" : ev.op === "u" ? "#dbab09" : "#f85149",
                           background: ev.op === "i" ? "rgba(46,160,67,0.1)" : ev.op === "u" ? "rgba(219,171,9,0.1)" : "rgba(248,81,73,0.1)"
                        }}>
                          {ev.op === "i" ? "INSERT" : ev.op === "u" ? "UPDATE" : ev.op === "d" ? "DELETE" : ev.op}
                        </span>
                      </td>
                      <td style={{ maxWidth: "300px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        {JSON.stringify(ev.data || ev.old_data || {})}
                      </td>
                      <td>{new Date(ev.time).toLocaleTimeString()}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={5} style={{ textAlign: "center", color: "var(--text-secondary)" }}>
                      No messages recorded recently in WAL.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>
      </main>
    </div>
  );
}
