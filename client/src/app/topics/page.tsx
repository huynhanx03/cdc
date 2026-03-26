"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { listTopicsAction } from "@/lib/actions";
import type { TopicSummary, PaginationResponse } from "@/lib/grpc";
import { 
  Layers, 
  ChevronLeft, 
  ChevronRight, 
  ExternalLink,
  MessageSquare,
  ArrowRight,
  Search,
  Plus
} from "lucide-react";
import Link from "next/link";

export default function TopicsPage() {
  const router = useRouter();
  const [topics, setTopics] = useState<TopicSummary[]>([]);
  const [pagination, setPagination] = useState<PaginationResponse | null>(null);
  const [page, setPage] = useState(1);
  const limit = 10;
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchTopics = async () => {
      setLoading(true);
      try {
        const res = await listTopicsAction(limit, page);
        setTopics(res.data || []);
        setPagination(res.pagination || null);
      } catch (err) {
        console.error("fetch topics:", err);
      } finally {
        setLoading(false);
      }
    };
    fetchTopics();
  }, [page]);

  const totalPages = pagination ? Math.ceil(pagination.total_rows / limit) : 1;

  if (loading && topics.length === 0) {
    return (
      <div className="space-y-6 animate-pulse">
        <div className="h-10 w-48 rounded bg-white/5" />
        <div className="h-96 rounded-2xl bg-white/5 border border-white/5" />
      </div>
    );
  }

  return (
    <div className="space-y-8 animate-in fade-in slide-in-from-bottom-2 duration-500">
      {/* Page Heading */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-white flex items-center gap-3">
            <Layers className="w-8 h-8 text-blue-400" />
            Topics Registry
          </h1>
          <p className="text-slate-400 mt-1 text-sm">Monitor and manage all change data capture streams available.</p>
        </div>
        <button className="flex items-center gap-2 px-4 py-2 rounded-xl bg-blue-600 hover:bg-blue-500 text-white text-sm font-bold transition-all shadow-lg shadow-blue-600/20 active:scale-95">
          <Plus className="w-4 h-4" /> Create New
        </button>
      </div>

      {/* Filter & Search Area */}
      <div className="flex flex-col md:flex-row items-center gap-4 bg-white/[0.02] border border-white/5 p-3 rounded-2xl">
        <div className="relative flex-1 group">
          <Search className="absolute left-3 top-2.5 w-4 h-4 text-slate-500 group-focus-within:text-blue-400 transition-colors" />
          <input
            type="text"
            placeholder="Search topics by name..."
            className="w-full bg-white/[0.03] border border-white/5 rounded-xl py-2 pl-10 pr-4 text-sm font-medium text-white placeholder:text-slate-600 focus:outline-none focus:border-blue-500/50 focus:bg-white/[0.05] transition-all"
          />
        </div>
        <div className="flex items-center gap-2 px-3 py-1.5 rounded-xl bg-white/[0.03] border border-white/5 text-xs font-bold text-slate-500 uppercase tracking-widest uppercase">
          <span className="text-blue-400">{pagination?.total_rows ?? 0}</span> Total Topics
        </div>
      </div>

      {/* Topics Table */}
      <div className="rounded-2xl border border-white/5 bg-white/[0.02] overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-white/5 text-[10px] text-slate-500 uppercase font-bold tracking-wider">
                <th className="px-6 py-4 text-left">Internal Name</th>
                <th className="px-6 py-4 text-left">Source Connector</th>
                <th className="px-6 py-4 text-center">Partitions</th>
                <th className="px-6 py-4 text-right">Message Count</th>
                <th className="px-6 py-4 text-right">Last Change</th>
                <th className="px-6 py-4 text-center">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/[0.02]">
              {topics.map((topic) => (
                <tr key={topic.name} className="group hover:bg-white/[0.015] transition-colors">
                  <td className="px-6 py-5 whitespace-nowrap">
                    <Link 
                      href={`/topics/${encodeURIComponent(topic.name)}`}
                      className="text-white font-mono font-bold hover:text-blue-400 transition-colors flex items-center gap-2"
                    >
                      <div className="w-1.5 h-1.5 rounded-full bg-blue-500 animate-pulse" />
                      {topic.name}
                    </Link>
                  </td>
                  <td className="px-6 py-5 whitespace-nowrap text-slate-400 font-medium">
                    Native CDC
                  </td>
                  <td className="px-6 py-5 whitespace-nowrap text-center">
                    <span className="px-2 py-1 rounded-lg bg-white/[0.03] border border-white/5 font-mono text-xs text-slate-300">
                      {topic.partition_count}
                    </span>
                  </td>
                  <td className="px-6 py-5 whitespace-nowrap text-right">
                    <div className="flex items-center justify-end gap-2 text-white font-mono font-bold">
                      {Number(topic.message_count).toLocaleString()}
                      <MessageSquare className="w-3.5 h-3.5 text-slate-700 group-hover:text-blue-500 transition-colors" />
                    </div>
                  </td>
                  <td className="px-6 py-5 whitespace-nowrap text-right text-slate-500 font-mono text-xs">
                    {new Date().toLocaleTimeString()}
                  </td>
                  <td className="px-6 py-5 whitespace-nowrap text-center">
                    <div className="flex items-center justify-center gap-2">
                       <Link
                        href={`/topics/${encodeURIComponent(topic.name)}`}
                        className="p-2 rounded-lg border border-white/5 bg-white/[0.02] hover:bg-white/[0.08] hover:text-blue-400 transition-all"
                        title="View Partitions"
                      >
                        <Layers className="w-4 h-4" />
                      </Link>
                      <Link
                        href={`/messages?topic=${encodeURIComponent(topic.name)}`}
                        className="p-2 rounded-lg border border-white/5 bg-white/[0.02] hover:bg-white/[0.08] hover:text-blue-400 transition-all"
                        title="Browse Messages"
                      >
                        <ArrowRight className="w-4 h-4" />
                      </Link>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Modern Pagination */}
      {pagination && totalPages > 1 && (
        <div className="flex items-center justify-between px-6 py-4 rounded-2xl border border-white/5 bg-white/[0.01]">
          <div className="text-xs font-bold text-slate-500 uppercase tracking-widest">
            Showing <span className="text-white">{(page-1)*limit + 1}</span> - <span className="text-white">{Math.min(page*limit, pagination.total_rows)}</span> of <span className="text-white">{pagination.total_rows}</span>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={!pagination.has_prev}
              className="p-2 rounded-xl border border-white/5 bg-white/[0.03] disabled:opacity-20 hover:bg-blue-600/10 hover:border-blue-500/30 hover:text-blue-400 transition-all"
            >
              <ChevronLeft className="w-4 h-4" />
            </button>
            <div className="flex items-center gap-1.5 px-4 font-mono font-bold text-xs">
              <span className="text-blue-400">{page}</span>
              <span className="text-slate-600">/</span>
              <span className="text-slate-400">{totalPages}</span>
            </div>
            <button
              onClick={() => setPage((p) => p + 1)}
              disabled={!pagination.has_next}
              className="p-2 rounded-xl border border-white/5 bg-white/[0.03] disabled:opacity-20 hover:bg-blue-600/10 hover:border-blue-500/30 hover:text-blue-400 transition-all"
            >
              <ChevronRight className="w-4 h-4" />
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
