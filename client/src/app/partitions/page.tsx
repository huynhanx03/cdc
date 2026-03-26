"use client";

import { useEffect, useState, useCallback } from "react";
import Link from "next/link";
import { listPartitionsAction } from "@/lib/actions";
import type { PartitionSummary } from "@/lib/grpc";
import { Network, ChevronLeft, ChevronRight, MessageSquare } from "lucide-react";

export default function PartitionsPage() {
  const [partitions, setPartitions] = useState<PartitionSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [hasNext, setHasNext] = useState(false);
  const [hasPrev, setHasPrev] = useState(false);
  const [page, setPage] = useState(1);
  const limit = 50;
  const [loading, setLoading] = useState(true);
  // Partitions are scoped to a topic; since this is the "all" view we pass empty topic
  // to list all top-level partitions in the NATS stream
  const [topicFilter, setTopicFilter] = useState("");

  const fetchPartitions = useCallback(async () => {
    setLoading(true);
    try {
      const res = await listPartitionsAction(topicFilter, limit, page);
      setPartitions(res.data || []);
      setTotal(res.pagination?.total_rows ?? 0);
      setHasNext(res.pagination?.has_next ?? false);
      setHasPrev(res.pagination?.has_prev ?? false);
    } catch (e) {
      console.error("fetch partitions:", e);
    } finally {
      setLoading(false);
    }
  }, [topicFilter, page]);

  useEffect(() => {
    fetchPartitions();
    const interval = setInterval(fetchPartitions, 5000);
    return () => clearInterval(interval);
  }, [fetchPartitions]);

  const totalPages = Math.ceil(total / limit) || 1;

  return (
    <div className="space-y-6 animate-in fade-in duration-300">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-3xl font-bold text-white flex items-center gap-3">
            <Network className="w-7 h-7 text-emerald-400" />
            Partitions
          </h1>
          <p className="text-slate-400 mt-1 text-sm">
            NATS subjects scoped to each topic. {total} total.
          </p>
        </div>
        {/* Topic filter */}
        <div className="flex flex-col gap-1">
          <label className="text-[10px] text-slate-500 uppercase font-bold tracking-wider">Filter by Topic</label>
          <input
            type="text"
            placeholder="e.g. CDC.db.users"
            value={topicFilter}
            onChange={(e) => { setTopicFilter(e.target.value); setPage(1); }}
            className="px-3 py-2 rounded-lg bg-white/5 border border-white/10 text-white text-sm w-52 focus:outline-none focus:border-emerald-500/50 placeholder:text-slate-600"
          />
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto rounded-xl border border-white/5">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-white/5 text-[10px] text-slate-500 uppercase tracking-wider">
              <th className="px-4 py-3 text-left font-bold">Partition (Subject)</th>
              <th className="px-4 py-3 text-left font-bold">Topic</th>
              <th className="px-4 py-3 text-right font-bold">Messages</th>
              <th className="px-4 py-3 text-left font-bold">Actions</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              [...Array(8)].map((_, i) => (
                <tr key={i} className="border-b border-white/[0.03]">
                  {[...Array(4)].map((__, j) => (
                    <td key={j} className="px-4 py-3">
                      <div className="h-4 rounded bg-white/[0.05] animate-pulse" />
                    </td>
                  ))}
                </tr>
              ))
            ) : partitions.length === 0 ? (
              <tr>
                <td colSpan={4} className="px-4 py-12 text-center text-slate-500">
                  No partitions found. Start the CDC engine to begin capturing WAL events.
                </td>
              </tr>
            ) : (
              partitions.map((p) => (
                <tr
                  key={p.id}
                  className="border-b border-white/[0.03] hover:bg-white/[0.03] transition-colors"
                >
                  <td className="px-4 py-3 font-mono text-xs text-emerald-300">{p.id}</td>
                  <td className="px-4 py-3">
                    <Link
                      href={`/topics/${encodeURIComponent(p.topic)}`}
                      className="font-mono text-xs text-blue-400 hover:text-blue-300 transition-colors"
                    >
                      {p.topic}
                    </Link>
                  </td>
                  <td className="px-4 py-3 font-mono text-xs text-white text-right">
                    {Number(p.message_count).toLocaleString()}
                  </td>
                  <td className="px-4 py-3">
                    <Link
                      href={`/messages?partition=${encodeURIComponent(p.id)}`}
                      className="flex items-center gap-1.5 w-fit px-3 py-1.5 rounded-lg text-xs border border-white/10 bg-white/[0.03] hover:bg-white/[0.08] text-slate-300 transition-colors"
                    >
                      <MessageSquare className="w-3 h-3" />
                      Messages
                    </Link>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-center gap-3">
          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={!hasPrev}
            className="p-2 rounded-lg border border-white/10 bg-white/[0.03] disabled:opacity-30 hover:bg-white/[0.08] transition-colors"
          >
            <ChevronLeft className="w-4 h-4 text-slate-300" />
          </button>
          <span className="text-sm text-slate-400 font-mono">Page {page} / {totalPages}</span>
          <button
            onClick={() => setPage((p) => p + 1)}
            disabled={!hasNext}
            className="p-2 rounded-lg border border-white/10 bg-white/[0.03] disabled:opacity-30 hover:bg-white/[0.08] transition-colors"
          >
            <ChevronRight className="w-4 h-4 text-slate-300" />
          </button>
        </div>
      )}
    </div>
  );
}
