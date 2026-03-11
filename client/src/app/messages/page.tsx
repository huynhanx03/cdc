"use client";

import React, { useEffect, useState, useCallback } from "react";
import { listPartitionsAction, getMessagesAction } from "@/lib/actions";
import type { PartitionSummary } from "@/lib/grpc";

interface MessageDisplay {
  offset: number;
  timestamp: number;
  key: string;
  value: string; // base64 encoded from server action
}

function decodeB64(val: string): string {
  try {
    return atob(val);
  } catch {
    return val;
  }
}

export default function MessagesPage() {
  const [partitions, setPartitions] = useState<PartitionSummary[]>([]);
  const [selectedPartition, setSelectedPartition] = useState<number | null>(null);
  const [messages, setMessages] = useState<MessageDisplay[]>([]);
  const [limit, setLimit] = useState(50);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(false);
  const [expanded, setExpanded] = useState<number | null>(null);

  useEffect(() => {
    const fetchParts = async () => {
      try {
        const data = await listPartitionsAction();
        const parts = data.partitions || [];
        setPartitions(parts);
        if (parts.length > 0 && selectedPartition === undefined) {
          setSelectedPartition(null);
        }
      } catch (e) {
        console.error("fetch partitions:", e);
      }
    };
    fetchParts();
  }, []);

  const fetchMessages = useCallback(async () => {
    // Global fetch is supported if selectedPartition is null
    setLoading(true);
    try {
      const data = await getMessagesAction(selectedPartition, offset, limit);
      setMessages(data.messages || []);
    } catch (e) {
      console.error("fetch messages:", e);
    } finally {
      setLoading(false);
    }
  }, [selectedPartition, offset, limit]);

  useEffect(() => {
    fetchMessages();
  }, [fetchMessages]);

  const opTag = (msg: MessageDisplay) => {
    let op = "";
    try {
      const raw = decodeB64(msg.value);
      const parsed = JSON.parse(raw);
      op = (parsed?.op || "").toLowerCase();
    } catch { }
    if (op === "i" || op === "insert") return <span className="tag green">INSERT</span>;
    if (op === "u" || op === "update") return <span className="tag yellow">UPDATE</span>;
    if (op === "d" || op === "delete") return <span className="tag red">DELETE</span>;
    return <span className="tag">{op || "?"}</span>;
  };

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Message Browser</h1>
        <p className="page-subtitle">Inspect raw CDC events by partition. Click any row to expand the full payload.</p>
      </div>

      {/* Controls */}
      <div className="card" style={{ marginBottom: "24px", display: "flex", gap: "24px", flexWrap: "wrap", alignItems: "center" }}>
        <div style={{ display: "flex", flexDirection: "column", gap: "6px" }}>
          <label style={{ fontSize: "0.8rem", color: "var(--text-secondary)", textTransform: "uppercase", letterSpacing: "0.5px" }}>Partition</label>
          <select
            value={selectedPartition ?? "all"}
            onChange={(e) => {
              const val = e.target.value;
              setSelectedPartition(val === "all" ? null : parseInt(val));
              setOffset(0);
            }}
            style={{ background: "var(--bg-secondary)", border: "1px solid var(--border-color)", color: "var(--text-primary)", padding: "8px 16px", borderRadius: "6px", minWidth: "160px" }}
          >
            <option value="all">All Partitions</option>
            {partitions.map((p) => (
              <option key={p.id} value={p.id}>Partition {p.id}</option>
            ))}
          </select>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: "6px" }}>
          <label style={{ fontSize: "0.8rem", color: "var(--text-secondary)", textTransform: "uppercase", letterSpacing: "0.5px" }}>Limit</label>
          <select
            value={limit}
            onChange={(e) => { setLimit(parseInt(e.target.value)); setOffset(0); }}
            style={{ background: "var(--bg-secondary)", border: "1px solid var(--border-color)", color: "var(--text-primary)", padding: "8px 16px", borderRadius: "6px" }}
          >
            <option value={20}>20</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
          </select>
        </div>

        <div style={{ display: "flex", gap: "12px", marginLeft: "auto", alignItems: "flex-end" }}>
          <button
            className="btn"
            style={{ width: "auto", padding: "10px 20px", background: "rgba(255,255,255,0.08)", color: "#fff" }}
            disabled={offset === 0}
            onClick={() => setOffset(Math.max(0, offset - limit))}
          >
            ← Prev
          </button>
          <span style={{ color: "var(--text-secondary)", fontSize: "0.9rem", alignSelf: "center", whiteSpace: "nowrap" }}>
            offset {offset}
          </span>
          <button
            className="btn"
            style={{ width: "auto", padding: "10px 20px", background: "rgba(255,255,255,0.08)", color: "#fff" }}
            disabled={messages.length < limit}
            onClick={() => setOffset(offset + limit)}
          >
            Next →
          </button>
          <button className="btn" style={{ width: "auto", padding: "10px 20px" }} onClick={fetchMessages}>
            ↻ Refresh
          </button>
        </div>
      </div>

      {/* Messages Table */}
      <div className="table-container">
        <table>
          <thead>
            <tr>
              <th>OFFSET</th>
              <th>OP</th>
              <th>TIMESTAMP</th>
              <th>KEY</th>
              <th>VALUE PREVIEW</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={6} style={{ textAlign: "center", color: "var(--text-secondary)" }}>
                  Loading messages...
                </td>
              </tr>
            ) : messages.length === 0 ? (
              <tr>
                <td colSpan={6} style={{ textAlign: "center", color: "var(--text-secondary)" }}>
                  No messages found for this partition at offset {offset}.
                </td>
              </tr>
            ) : (
              messages.map((msg) => {
                const rawValue = decodeB64(msg.value);
                const isExpanded = expanded === msg.offset;
                let preview = rawValue;
                let formatted = rawValue;
                try {
                  const p = JSON.parse(rawValue);
                  preview = JSON.stringify(p).substring(0, 100);
                  formatted = JSON.stringify(p, null, 2);
                } catch { }

                return (
                  <React.Fragment key={msg.offset}>
                    <tr
                      style={{ cursor: "pointer" }}
                      onClick={() => setExpanded(isExpanded ? null : msg.offset)}
                    >
                      <td style={{ fontFamily: "monospace", color: "var(--accent)" }}>{msg.offset}</td>
                      <td>{opTag(msg)}</td>
                      <td style={{ fontSize: "0.85rem", color: "var(--text-secondary)", whiteSpace: "nowrap" }}>
                        {msg.timestamp ? new Date(msg.timestamp).toLocaleString() : "—"}
                      </td>
                      <td style={{ fontFamily: "monospace", fontSize: "0.85rem" }}>{msg.key || "—"}</td>
                      <td
                        style={{
                          maxWidth: "280px",
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                          fontFamily: "monospace",
                          fontSize: "0.82rem",
                          color: "var(--text-secondary)",
                        }}
                      >
                        {preview}
                      </td>
                      <td style={{ color: "var(--accent)", fontSize: "0.85rem", whiteSpace: "nowrap" }}>
                        {isExpanded ? "▲" : "▼"}
                      </td>
                    </tr>
                    {isExpanded && (
                      <tr>
                        <td colSpan={6} style={{ padding: 0 }}>
                          <pre className="code-block" style={{ margin: 0, padding: "20px", borderRadius: 0 }}>
                            {formatted}
                          </pre>
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
