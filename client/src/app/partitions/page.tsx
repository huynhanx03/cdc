"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { formatBytes, formatNumber } from "@/lib/api";
import { listPartitionsAction } from "@/lib/actions";
import type { PartitionSummary } from "@/lib/grpc";

export default function PartitionsPage() {
  const router = useRouter();
  const [partitions, setPartitions] = useState<PartitionSummary[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetch = async () => {
      try {
        const data = await listPartitionsAction();
        setPartitions(data.partitions || []);
      } catch (e) {
        console.error("fetch partitions:", e);
      } finally {
        setLoading(false);
      }
    };
    fetch();
    const interval = setInterval(fetch, 5000);
    return () => clearInterval(interval);
  }, []);

  if (loading) return <div className="page-title">Loading partitions...</div>;

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Partitions ({partitions.length})</h1>
        <p className="page-subtitle">Individual WAL log segments grouped per partition. Each partition maps to a physical disk-backed log.</p>
      </div>

      <div className="table-container">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>EARLIEST OFFSET</th>
              <th>LATEST OFFSET</th>
              <th>SEGMENTS</th>
              <th>SIZE</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {partitions.length === 0 ? (
              <tr>
                <td colSpan={6} style={{ textAlign: "center", color: "var(--text-secondary)" }}>
                  No partitions found. Start the CDC engine to begin capturing WAL events.
                </td>
              </tr>
            ) : (
              partitions.map((p) => (
                <tr
                  key={p.id}
                  style={{ cursor: "pointer" }}
                  onClick={() => router.push(`/partitions/${p.id}`)}
                >
                  <td><strong>Partition {p.id}</strong></td>
                  <td style={{ fontFamily: "monospace", fontSize: "0.85rem" }}>{formatNumber(p.earliestOffset)}</td>
                  <td style={{ fontFamily: "monospace", fontSize: "0.85rem" }}>
                    <span className="tag blue">{formatNumber(p.latestOffset)}</span>
                  </td>
                  <td>{p.segmentCount}</td>
                  <td>{formatBytes(p.sizeBytes)}</td>
                  <td>
                    <span style={{ color: "var(--accent)", fontSize: "0.85rem" }}>Inspect →</span>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
