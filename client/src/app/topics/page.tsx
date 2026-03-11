"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { formatNumber } from "@/lib/api";
import { listTopicsAction } from "@/lib/actions";
import type { TopicSummary } from "@/lib/grpc";

export default function TopicsPage() {
  const [topics, setTopics] = useState<TopicSummary[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchAll = async () => {
      try {
        const data = await listTopicsAction();
        setTopics(data.topics || []);
      } catch (err) {
        console.error("fetch topics:", err);
      } finally {
        setLoading(false);
      }
    };
    fetchAll();
  }, []);

  if (loading) return <div className="page-title">Loading topics...</div>;

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Topics ({topics.length})</h1>
        <p className="page-subtitle">Logical grouping of write-ahead logging (WAL) partitions. Each Postgres table typically maps to one Topic.</p>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(320px, 1fr))", gap: "24px" }}>
        {topics.length === 0 ? (
          <p>No topics discovered yet. Keep the CDC engine running to ingest schemas.</p>
        ) : (
          topics.map((t) => (
            <Link key={t.name} href={`/topics/${encodeURIComponent(t.name)}`} className="topic-card">
              <div className="topic-card-header">
                <h3 className="topic-name">{t.name}</h3>
                <span className="tag blue">Active</span>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", color: "var(--text-secondary)", fontSize: "0.9rem" }}>
                <span>{t.partitionCount} Partitions</span>
                <span>{formatNumber(t.messageCount)} Msgs</span>
              </div>
            </Link>
          ))
        )}
      </div>
    </div>
  );
}
