"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import { formatBytes, formatNumber } from "@/lib/api";
import { getTopicAction } from "@/lib/actions";
import type { TopicDetail } from "@/lib/grpc";

export default function TopicDetailPage() {
  const { name } = useParams();
  const router = useRouter();
  const [topic, setTopic] = useState<TopicDetail | null>(null);
  const [loading, setLoading] = useState(true);

  const topicName = decodeURIComponent(name as string);

  useEffect(() => {
    const fetchIt = async () => {
      try {
        const data = await getTopicAction(topicName);
        setTopic(data);
      } catch (err) {
        console.error("fetch topic:", err);
      } finally {
        setLoading(false);
      }
    };
    fetchIt();
  }, [topicName]);

  if (loading) return <div className="page-title">Loading topic {topicName}...</div>;
  if (!topic) return <div className="page-title" style={{ color: "var(--error)" }}>Topic not found</div>;

  return (
    <div>
      <div className="page-header">
        <div style={{ display: "flex", alignItems: "center", gap: "16px" }}>
          <button className="btn" style={{ width: "auto", background: "rgba(255,255,255,0.1)", color: "#fff" }} onClick={() => router.back()}>
             ← Back
          </button>
          <h1 className="page-title" style={{ margin: 0 }}>{topic.name}</h1>
        </div>
        <p className="page-subtitle" style={{ marginTop: "12px" }}>
          This topic holds WAL records for table <code>{topic.name}</code>.
        </p>
      </div>

      <div className="stats-grid">
         <div className="card stat-card">
           <div className="stat-value">{formatNumber(topic.messageCount)}</div>
           <div className="stat-label">Total Messages</div>
         </div>
         <div className="card stat-card">
           <div className="stat-value">{topic.partitionCount}</div>
           <div className="stat-label">Partitions</div>
         </div>
         <div className="card stat-card">
           <div className="stat-value">{formatBytes(topic.partitions.reduce((acc, p) => acc + (p.sizeBytes || 0), 0))}</div>
           <div className="stat-label">Total Size</div>
         </div>
      </div>

      <h2 style={{ marginBottom: "16px", marginTop: "40px" }}>Partitions</h2>
      <div className="table-container">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>EARLIEST LSN</th>
              <th>LATEST LSN</th>
              <th>SEGMENTS</th>
              <th>SIZE</th>
              <th>ACTION</th>
            </tr>
          </thead>
          <tbody>
            {topic.partitions.map((p) => (
              <tr key={p.id}>
                <td><strong>{p.id}</strong></td>
                <td><span className="tag">{p.earliestOffset}</span></td>
                <td><span className="tag blue">{p.latestOffset}</span></td>
                <td>{p.segmentCount}</td>
                <td>{formatBytes(p.sizeBytes)}</td>
                <td>
                  <button className="btn" style={{ padding: "6px 12px", width: "auto" }} onClick={() => router.push(`/partitions/${p.id}`)}>
                    Inspect
                  </button>
                </td>
              </tr>
            ))}
            {topic.partitions.length === 0 && (
              <tr>
                 <td colSpan={6} style={{ textAlign: "center" }}>No partitions found</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
