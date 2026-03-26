"use client";

import { useEffect, useState, useCallback } from "react";
import { useParams, useRouter } from "next/navigation";
import Link from "next/link";
import { listPartitionsAction } from "@/lib/actions";
import type { PartitionSummary, PaginationResponse } from "@/lib/grpc";
import { 
  Layers, 
  ChevronLeft, 
  ChevronRight, 
  ArrowLeft, 
  MessageSquare,
  Activity,
  History,
  Box,
  Database,
  ArrowRight
} from "lucide-react";

export default function TopicDetailPage() {
  const { name } = useParams();
  const router = useRouter();
  const topicName = decodeURIComponent(name as string);

  const [partitions, setPartitions] = useState<PartitionSummary[]>([]);
  const [pagination, setPagination] = useState<PaginationResponse | null>(null);
  const [page, setPage] = useState(1);
  const limit = 20;
  const [loading, setLoading] = useState(true);

  const fetchPartitions = useCallback(async () => {
    setLoading(true);
    try {
      const res = await listPartitionsAction(topicName, limit, page);
      setPartitions(res.data || []);
      setPagination(res.pagination || null);
    } catch (err) {
      console.error("fetch partitions:", err);
    } finally {
      setLoading(false);
    }
  }, [topicName, page]);

  useEffect(() => {
    fetchPartitions();
  }, [fetchPartitions]);

  const totalPages = pagination ? Math.ceil(pagination.total_rows / limit) : 1;
  const totalMessages = partitions.reduce((s, p) => s + Number(p.message_count), 0);

  return (
    <div className="space-y-8 animate-in fade-in duration-500 slow">
      {/* Header */}
      <div className="flex items-center gap-6">
        <button
          onClick={() => router.back()}
          className="p-3 rounded-2xl border border-white/5 bg-white/[0.02] hover:bg-white/[0.08] transition-all group active:scale-95"
          title="Back to Topics"
        >
          <ArrowLeft className="w-5 h-5 text-slate-300 group-hover:text-white transition-colors" />
        </button>
        <div className="flex-1">
          <div className="flex items-center gap-2 text-blue-400 font-mono text-[10px] uppercase font-bold tracking-widest mb-1.5">
            <Layers className="w-3.5 h-3.5" /> Topic Registry
          </div>
          <h1 className="text-3xl font-mono font-bold text-white tracking-tight flex items-center gap-3">
            {topicName}
          </h1>
          <p className="text-slate-500 text-sm font-medium mt-1">Detailed partition analysis and message metrics.</p>
        </div>
        <div className="flex items-center gap-3">
          <Link
            href={`/messages?topic=${encodeURIComponent(topicName)}`}
            className="flex items-center gap-2 px-6 py-2.5 rounded-2xl bg-blue-600 hover:bg-blue-500 text-white text-sm font-bold transition-all shadow-lg shadow-blue-600/20 active:scale-95"
          >
            <MessageSquare className="w-4 h-4" /> Browse Messages
          </Link>
        </div>
      </div>

      {/* Summary Micro Stats */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="p-6 rounded-2xl border border-white/5 bg-white/[0.02] relative overflow-hidden group">
          <Database className="absolute -right-4 -bottom-4 w-24 h-24 text-white/[0.03]" />
          <div className="text-[10px] text-slate-500 uppercase font-bold tracking-widest mb-1">Total Partitions</div>
          <div className="text-3xl font-mono font-bold text-white tracking-tighter">{pagination?.total_rows ?? 0}</div>
        </div>
        
        <div className="p-6 rounded-2xl border border-white/5 bg-white/[0.02] relative overflow-hidden group">
          <Activity className="absolute -right-4 -bottom-4 w-24 h-24 text-white/[0.03]" />
          <div className="text-[10px] text-slate-500 uppercase font-bold tracking-widest mb-1">Messages (Filtered)</div>
          <div className="text-3xl font-mono font-bold text-white tracking-tighter">{totalMessages.toLocaleString()}</div>
        </div>

        <div className="p-6 rounded-2xl border border-white/5 bg-white/[0.02] relative overflow-hidden group">
          <History className="absolute -right-4 -bottom-4 w-24 h-24 text-white/[0.03]" />
          <div className="text-[10px] text-slate-500 uppercase font-bold tracking-widest mb-1">Last Update</div>
          <div className="text-3xl font-mono font-bold text-white tracking-tighter">{new Date().toLocaleTimeString('en-US', { hour12: false })}</div>
        </div>
      </div>

      {/* Partitions List Table */}
      <div className="rounded-2xl border border-white/5 bg-white/[0.02] overflow-hidden shadow-2xl shadow-black/40">
        <div className="px-6 py-5 border-b border-white/5 bg-white/[0.01] flex items-center justify-between">
          <h2 className="text-sm font-bold text-white uppercase tracking-wider flex items-center gap-2">
            <Box className="w-4 h-4 text-blue-400" />
            Active Partitions
          </h2>
          <span className="text-[10px] font-mono text-slate-600">Sorting by ID: ASC</span>
        </div>
        
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-white/5 text-[10px] text-slate-500 uppercase font-bold tracking-wider">
                <th className="px-6 py-4 text-left">Partition ID / Subject</th>
                <th className="px-6 py-4 text-left">Origin Topic</th>
                <th className="px-6 py-4 text-center">Status</th>
                <th className="px-6 py-4 text-right">Message Count</th>
                <th className="px-6 py-4 text-center">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/[0.02]">
              {loading ? (
                [...Array(5)].map((_, i) => (
                  <tr key={i} className="animate-pulse">
                    {[...Array(5)].map((__, j) => (
                      <td key={j} className="px-6 py-5">
                        <div className="h-4 rounded bg-white/[0.05]" />
                      </td>
                    ))}
                  </tr>
                ))
              ) : partitions.length === 0 ? (
                <tr>
                  <td colSpan={5} className="px-6 py-20 text-center text-slate-600">
                    <p className="text-sm font-medium">No partitions discovered for this topic.</p>
                  </td>
                </tr>
              ) : (
                partitions.map((p) => (
                  <tr key={p.id} className="group hover:bg-white/[0.015] transition-colors">
                    <td className="px-6 py-5 whitespace-nowrap">
                      <div className="flex items-center gap-2 text-white font-mono font-bold">
                        <div className="w-1.5 h-1.5 rounded-full bg-emerald-500/50 shadow-[0_0_8px_rgba(16,185,129,0.3)]" />
                        {p.id}
                      </div>
                    </td>
                    <td className="px-6 py-5 whitespace-nowrap text-slate-500 font-mono text-xs">
                      {p.topic}
                    </td>
                    <td className="px-6 py-5 whitespace-nowrap text-center">
                      <span className="px-2 py-0.5 rounded-full bg-blue-500/10 text-blue-400 border border-blue-500/20 text-[10px] font-bold uppercase tracking-tighter">
                        Healthy
                      </span>
                    </td>
                    <td className="px-6 py-5 whitespace-nowrap text-right font-mono font-bold text-white">
                      {Number(p.message_count).toLocaleString()}
                    </td>
                    <td className="px-6 py-5 whitespace-nowrap text-center">
                      <Link
                        href={`/messages?partition=${encodeURIComponent(p.id)}`}
                        className="px-4 py-1.5 rounded-xl border border-white/5 bg-white/[0.02] hover:bg-blue-600/10 hover:border-blue-500/30 hover:text-blue-400 text-xs font-bold transition-all inline-flex items-center gap-2 group/btn"
                      >
                        Inspect Messages
                        <ArrowRight className="w-3.5 h-3.5 transition-transform group-hover/btn:translate-x-1" />
                      </Link>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Pagination Bar */}
      {pagination && totalPages > 1 && (
        <div className="flex items-center justify-center gap-4 bg-white/[0.01] p-3 rounded-2xl border border-white/5">
          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={!pagination.has_prev}
            className="p-2 rounded-xl border border-white/5 bg-white/[0.03] disabled:opacity-20 hover:bg-white/[0.08] transition-all"
          >
            <ChevronLeft className="w-4 h-4 text-slate-300" />
          </button>
          <div className="px-4 py-1 rounded-lg bg-white/5 border border-white/5 shadow-inner">
            <span className="text-xs font-mono font-bold tracking-wide">
              <span className="text-blue-400">{page}</span>
              <span className="mx-2 text-slate-700">/</span>
              <span className="text-slate-500">{totalPages}</span>
            </span>
          </div>
          <button
            onClick={() => setPage((p) => p + 1)}
            disabled={!pagination.has_next}
            className="p-2 rounded-xl border border-white/5 bg-white/[0.03] disabled:opacity-20 hover:bg-white/[0.08] transition-all"
          >
            <ChevronRight className="w-4 h-4 text-slate-300" />
          </button>
        </div>
      )}
    </div>
  );
}
