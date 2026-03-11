"use client";

import React, { useEffect, useState, useCallback } from "react";
import { useParams, useRouter } from "next/navigation";
import { formatBytes } from "@/lib/api";
import { getPartitionAction, getMessagesAction } from "@/lib/actions";
import type { PartitionDetail } from "@/lib/grpc";

interface MessageDisplay {
  offset: number;
  timestamp: number;
  key: string;
  value: string;
}

function decodeB64(val: string): string {
  try {
    return atob(val);
  } catch {
    return val;
  }
}

export default function PartitionDetailPage() {
  const { id } = useParams();
  const router = useRouter();
  const partitionId = parseInt(id as string, 10);

  const [partition, setPartition] = useState<PartitionDetail | null>(null);
  const [messages, setMessages] = useState<MessageDisplay[]>([]);
  const [limit, setLimit] = useState(50);
  const [expanded, setExpanded] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);

  const fetchData = useCallback(async () => {
    try {
      const [p, msgsData] = await Promise.all([
        getPartitionAction(partitionId),
        getMessagesAction(partitionId, 0, limit),
      ]);
      setPartition(p);
      setMessages(msgsData.messages || []);
    } catch (e) {
      console.error("fetch partition detail:", e);
    } finally {
      setLoading(false);
    }
  }, [partitionId, limit]);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, [fetchData]);

  if (loading) return <div className="page-title">Loading partition {partitionId}...</div>;
  if (!partition) return <div style={{ color: "var(--error)" }}>Partition not found</div>;

  return (
    <div>
      <div className="page-header">
        <div style={{ display: "flex", alignItems: "center", gap: "16px" }}>
          <button
            className="btn"
            style={{ width: "auto", padding: "8px 16px", background: "rgba(255,255,255,0.1)", color: "#fff" }}
            onClick={() => router.back()}
          >
            ← Back
          </button>
          <h1 className="page-title" style={{ margin: 0 }}>Partition {partition.id}</h1>
        </div>
      </div>

      {/* Partition Stats */}
      <section className="stats-grid" style={{ marginBottom: "40px" }}>
        <div className="card stat-card">
          <div className="stat-value">{partition.segmentCount}</div>
          <div className="stat-label">Segments</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value">{formatBytes(partition.sizeBytes)}</div>
          <div className="stat-label">Total Size</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value" style={{ fontSize: "1.4rem" }}>{partition.earliestOffset}</div>
          <div className="stat-label">Earliest Offset</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value" style={{ fontSize: "1.4rem" }}>{partition.latestOffset}</div>
          <div className="stat-label">Latest Offset</div>
        </div>
      </section>

      {/* Segments List */}
      {partition.segments && partition.segments.length > 0 && (
        <>
          <h2 style={{ marginBottom: "16px" }}>Log Segments ({partition.segments.length})</h2>
          <div className="table-container" style={{ marginBottom: "40px" }}>
            <table>
              <thead>
                <tr>
                  <th>BASE OFFSET</th>
                  <th>SIZE</th>
                </tr>
              </thead>
              <tbody>
                {partition.segments.map((seg, i) => (
                  <tr key={i}>
                    <td style={{ fontFamily: "monospace" }}>{seg.baseOffset}</td>
                    <td>{formatBytes(seg.sizeBytes)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      {/* Messages */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "16px" }}>
        <h2 style={{ margin: 0 }}>Messages ({messages.length})</h2>
        <div style={{ display: "flex", gap: "12px", alignItems: "center" }}>
          <label style={{ color: "var(--text-secondary)", fontSize: "0.9rem" }}>Limit:</label>
          <select
            value={limit}
            onChange={(e) => setLimit(parseInt(e.target.value))}
            style={{
              background: "var(--panel-bg)",
              border: "1px solid var(--border-color)",
              color: "var(--text-primary)",
              padding: "6px 12px",
              borderRadius: "6px",
              cursor: "pointer",
            }}
          >
            <option value={20}>20</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
            <option value={200}>200</option>
          </select>
          <button
            className="btn"
            style={{ width: "auto", padding: "8px 16px" }}
            onClick={fetchData}
          >
            ↻ Refresh
          </button>
        </div>
      </div>

      <div className="table-container">
        <table>
          <thead>
            <tr>
              <th>OFFSET</th>
              <th>TIMESTAMP</th>
              <th>KEY</th>
              <th>VALUE PREVIEW</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {messages.length === 0 ? (
              <tr>
                <td colSpan={5} style={{ textAlign: "center", color: "var(--text-secondary)" }}>
                  No messages in this partition yet.
                </td>
              </tr>
            ) : (
              messages.map((msg) => {
                const rawValue = decodeB64(msg.value);
                const isExpanded = expanded === msg.offset;
                let parsed: string;
                try {
                  parsed = JSON.stringify(JSON.parse(rawValue), null, 2);
                } catch {
                  parsed = rawValue;
                }

                return (
                  <React.Fragment key={msg.offset}>
                    <tr style={{ cursor: "pointer" }} onClick={() => setExpanded(isExpanded ? null : msg.offset)}>
                      <td style={{ fontFamily: "monospace", color: "var(--accent)" }}>{msg.offset}</td>
                      <td style={{ fontSize: "0.85rem", color: "var(--text-secondary)" }}>
                        {msg.timestamp ? new Date(msg.timestamp).toLocaleString() : "—"}
                      </td>
                      <td style={{ fontFamily: "monospace", fontSize: "0.85rem" }}>{msg.key || "—"}</td>
                      <td style={{ maxWidth: "320px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", fontFamily: "monospace", fontSize: "0.85rem", color: "var(--text-secondary)" }}>
                        {(rawValue || "").substring(0, 120)}
                      </td>
                      <td style={{ color: "var(--accent)", fontSize: "0.85rem", whiteSpace: "nowrap" }}>
                        {isExpanded ? "▲ Collapse" : "▼ Expand"}
                      </td>
                    </tr>
                    {isExpanded && (
                      <tr>
                        <td colSpan={5} style={{ background: "rgba(1,4,9,0.6)", padding: "0" }}>
                          <pre className="code-block" style={{ margin: "0", padding: "20px", borderRadius: "0" }}>{parsed}</pre>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
