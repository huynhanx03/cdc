"use client";

import { useEffect, useState, useCallback, Fragment } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import { listMessagesAction } from "@/lib/actions";
import type { MessageItem, PaginationResponse } from "@/lib/grpc";
import { 
  ChevronDown, 
  ChevronUp, 
  ChevronLeft, 
  ChevronRight, 
  Search, 
  RefreshCcw, 
  FileJson, 
  Copy, 
  Terminal,
  Layers,
  Box,
  CornerDownRight,
  Fingerprint
} from "lucide-react";

export default function MessagesPage() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const topicFilter = searchParams.get("topic") || "";
  const partitionFilter = searchParams.get("partition") || "";

  const [messages, setMessages] = useState<MessageItem[]>([]);
  const [pagination, setPagination] = useState<PaginationResponse | null>(null);
  const [page, setPage] = useState(1);
  const limit = 25;
  const [loading, setLoading] = useState(true);
  const [expandedSeq, setExpandedSeq] = useState<number | null>(null);

  const fetchMessages = useCallback(async () => {
    setLoading(true);
    try {
      const res = await listMessagesAction(topicFilter, partitionFilter, limit, page);
      setMessages(res.data || []);
      setPagination(res.pagination || null);
    } catch (err) {
      console.error("fetch messages:", err);
    } finally {
      setLoading(false);
    }
  }, [topicFilter, partitionFilter, page]);

  useEffect(() => {
    fetchMessages();
  }, [fetchMessages]);

  const totalPages = pagination ? Math.ceil(pagination.total_rows / limit) : 1;

  const toggleExpand = (seq: number) => {
    setExpandedSeq(expandedSeq === seq ? null : seq);
  };

  const decodePayload = (data: any) => {
    if (!data) return "";
    try {
      if (typeof data === "string") {
        // Handle base64 from server-side Buffer.toString('base64')
        const binString = atob(data);
        const bytes = Uint8Array.from(binString, (m) => m.codePointAt(0)!);
        return new TextDecoder().decode(bytes);
      }
      return new TextDecoder().decode(data as Uint8Array);
    } catch (e) {
      if (typeof data === "string") return data;
      return "Unable to decode payload binary data.";
    }
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  return (
    <div className="space-y-8 animate-in fade-in slide-in-from-bottom-2 duration-500">
      {/* Header & Controls */}
      <div className="flex flex-col md:flex-row md:items-end justify-between gap-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-white flex items-center gap-3">
             <Terminal className="w-8 h-8 text-blue-400" />
             Event Explorer
          </h1>
          <div className="flex items-center gap-3 mt-2">
            <span className="text-slate-500 text-sm font-medium">Real-time browse through all captured events.</span>
          </div>
        </div>
        
        <div className="flex items-center gap-2">
          <button 
            onClick={() => fetchMessages()}
            className="p-2.5 rounded-xl border border-white/10 bg-white/[0.03] hover:bg-white/[0.08] active:scale-95 transition-all text-slate-300 hover:text-white"
          >
            <RefreshCcw className={`w-4 h-4 ${loading ? "animate-spin text-blue-400" : ""}`} />
          </button>
        </div>
      </div>

      {/* Filter Bar */}
      <div className="flex flex-col lg:flex-row items-center gap-4 bg-white/[0.02] border border-white/5 p-4 rounded-2xl">
        <div className="relative flex-1 w-full lg:w-auto group">
          <Search className="absolute left-3 top-2.5 w-4 h-4 text-slate-500 group-focus-within:text-blue-400 transition-colors" />
          <input
            type="text"
            placeholder="Search payload contents..."
            className="w-full bg-white/[0.03] border border-white/5 rounded-xl py-2 pl-10 pr-4 text-sm font-medium text-white placeholder:text-slate-600 focus:outline-none focus:border-blue-500/50 focus:bg-white/[0.05] transition-all cursor-not-allowed"
            disabled
          />
        </div>
        
        <div className="flex items-center gap-3 w-full lg:w-auto overflow-x-auto pb-1 lg:pb-0 scrollbar-hide">
           {topicFilter && (
             <div className="flex items-center gap-2 px-3 py-1.5 rounded-xl bg-blue-500/10 border border-blue-500/20 text-xs font-bold text-blue-400 whitespace-nowrap">
               <Layers className="w-3.5 h-3.5" /> Topic: {topicFilter}
             </div>
           )}
           {partitionFilter && (
             <div className="flex items-center gap-2 px-3 py-1.5 rounded-xl bg-purple-500/10 border border-purple-500/20 text-xs font-bold text-purple-400 whitespace-nowrap">
               <Box className="w-3.5 h-3.5" /> Partition: {partitionFilter}
             </div>
           )}
           {!topicFilter && !partitionFilter && (
              <div className="flex items-center gap-2 px-3 py-1.5 rounded-xl bg-white/[0.03] border border-white/5 text-xs font-bold text-slate-500 whitespace-nowrap">
                <Box className="w-3.5 h-3.5" /> All Streams
              </div>
           )}
           <div className="flex items-center gap-2 px-3 py-1.5 rounded-xl bg-white/[0.03] border border-white/5 text-xs font-bold text-slate-500 whitespace-nowrap ml-auto">
             <span className="text-blue-400">{pagination?.total_rows ?? 0}</span> Found
           </div>
        </div>
      </div>

      {/* Messages Table */}
      <div className="rounded-2xl border border-white/5 bg-white/[0.02] overflow-hidden shadow-2xl">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-white/5 text-[10px] text-slate-500 uppercase font-bold tracking-wider">
                <th className="px-6 py-4 text-center w-12">#</th>
                <th className="px-6 py-4 text-left">Sequence</th>
                <th className="px-6 py-4 text-left">Subject</th>
                <th className="px-6 py-4 text-left">Status</th>
                <th className="px-6 py-4 text-right">Timestamp</th>
                <th className="px-6 py-4 text-center w-16">Action</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/[0.01]">
              {loading && messages.length === 0 ? (
                [...Array(10)].map((_, i) => (
                  <tr key={i} className="animate-pulse">
                    {[...Array(6)].map((__, j) => (
                      <td key={j} className="px-6 py-5">
                        <div className="h-4 rounded bg-white/[0.05]" />
                      </td>
                    ))}
                  </tr>
                ))
              ) : messages.length === 0 ? (
                <tr>
                  <td colSpan={6} className="px-6 py-24 text-center">
                    <div className="flex flex-col items-center gap-3 text-slate-600">
                      <Terminal className="w-12 h-12 opacity-20" />
                      <p className="font-medium">No messages flow captured yet.</p>
                    </div>
                  </td>
                </tr>
              ) : (
                messages.map((m) => {
                  const isExpanded = expandedSeq === m.sequence;
                  const rawPayload = decodePayload(m.data);
                  let formattedPayload = rawPayload;
                  try {
                    formattedPayload = JSON.stringify(JSON.parse(rawPayload), null, 2);
                  } catch (e) {
                      // fallback to raw
                  }

                  return (
                    <Fragment key={m.sequence}>
                      <tr 
                        onClick={() => toggleExpand(m.sequence)}
                        className={`group cursor-pointer transition-colors ${isExpanded ? "bg-blue-500/[0.04]" : "hover:bg-white/[0.015]"}`}
                      >
                        <td className="px-6 py-4 text-center">
                           {isExpanded ? <ChevronUp className="w-3.5 h-3.5 text-blue-400 m-auto" /> : <ChevronDown className="w-3.5 h-3.5 text-slate-700 group-hover:text-blue-400 m-auto transition-colors" />}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="font-mono text-xs font-bold text-white transition-colors">{m.sequence}</div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="flex flex-col">
                            <span className="text-xs font-mono font-bold text-slate-400 mb-0.5">{m.subject || "No Subject"}</span>
                            <span className="text-[10px] font-mono text-slate-600 flex items-center gap-1"><Fingerprint className="w-3 h-3" /> Event Hash</span>
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <span className="px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 text-[10px] font-bold uppercase tracking-tight">
                            Captured
                          </span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right text-slate-500 font-mono text-xs">
                          {new Date(parseInt(m.timestamp)).toLocaleString('en-US', { hour12: false })}
                        </td>
                        <td className="px-6 py-4 text-center">
                          <button 
                            onClick={(e) => { e.stopPropagation(); copyToClipboard(rawPayload); }}
                            className="p-1.5 rounded-lg border border-white/5 bg-white/[0.02] hover:bg-white/[0.1] text-slate-500 hover:text-white transition-all shadow-inner"
                          >
                            <Copy className="w-3.5 h-3.5" />
                          </button>
                        </td>
                      </tr>
                      {isExpanded && (
                        <tr className="bg-blue-500/[0.02] border-l-2 border-blue-500">
                          <td colSpan={6} className="px-12 py-8">
                             <div className="space-y-6">
                               <div className="flex items-center justify-between">
                                  <div className="flex items-center gap-4">
                                     <div className="p-2 rounded-xl bg-blue-500/10 border border-blue-500/20">
                                        <FileJson className="w-5 h-5 text-blue-400" />
                                     </div>
                                     <div>
                                        <h3 className="text-sm font-bold text-white">Payload Content</h3>
                                        <p className="text-xs text-slate-500">Structured data captured from the source.</p>
                                     </div>
                                  </div>
                                  <button 
                                    onClick={() => copyToClipboard(rawPayload)}
                                    className="px-3 py-1.5 rounded-lg bg-white/5 border border-white/10 text-xs font-bold text-slate-400 hover:text-white hover:bg-white/10 transition-all flex items-center gap-2"
                                  >
                                    <Copy className="w-3.5 h-3.5" /> Copy JSON
                                  </button>
                               </div>
                               <div className="p-6 rounded-2xl bg-black/40 border border-white/5 font-mono text-xs leading-relaxed text-blue-100/90 shadow-2xl">
                                  <pre className="whitespace-pre-wrap break-all overflow-x-auto max-h-[500px] scrollbar-hide">
                                    {formattedPayload}
                                  </pre>
                               </div>
                               <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                                  <div className="p-4 rounded-xl border border-white/5 bg-white/[0.02]">
                                     <div className="text-[10px] text-slate-600 uppercase font-bold tracking-widest mb-1">Sequence</div>
                                     <div className="text-xs font-mono text-white truncate">{m.sequence}</div>
                                  </div>
                                  <div className="p-4 rounded-xl border border-white/5 bg-white/[0.02]">
                                     <div className="text-[10px] text-slate-600 uppercase font-bold tracking-widest mb-1">Subject</div>
                                     <div className="text-xs font-mono text-white">{m.subject}</div>
                                  </div>
                                  <div className="p-4 rounded-xl border border-white/5 bg-white/[0.02]">
                                     <div className="text-[10px] text-slate-600 uppercase font-bold tracking-widest mb-1">Headers Count</div>
                                     <div className="text-xs font-mono text-white">{Object.keys(m.headers || {}).length}</div>
                                  </div>
                                  <div className="p-4 rounded-xl border border-white/5 bg-white/[0.02]">
                                     <div className="text-[10px] text-slate-600 uppercase font-bold tracking-widest mb-1">Capture Protocol</div>
                                     <div className="text-xs font-mono text-white">cdc-v1-grpc</div>
                                  </div>
                               </div>
                             </div>
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Pagination Bar */}
      {pagination && totalPages > 1 && (
        <div className="flex items-center justify-between px-6 py-4 rounded-2xl border border-white/5 bg-white/[0.01]">
           <div className="text-xs font-bold text-slate-500 uppercase tracking-widest">
            Page <span className="text-white">{page}</span> of <span className="text-white">{totalPages}</span>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={!pagination.has_prev}
              className="p-2.5 rounded-xl border border-white/10 bg-white/[0.03] disabled:opacity-20 hover:bg-blue-600/10 hover:border-blue-500/30 hover:text-blue-400 transition-all font-bold"
            >
              <ChevronLeft className="w-4 h-4" />
            </button>
            <button
              onClick={() => setPage((p) => p + 1)}
              disabled={!pagination.has_next}
              className="p-2.5 rounded-xl border border-white/10 bg-white/[0.03] disabled:opacity-20 hover:bg-blue-600/10 hover:border-blue-500/30 hover:text-blue-400 transition-all font-bold"
            >
              <ChevronRight className="w-4 h-4" />
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
