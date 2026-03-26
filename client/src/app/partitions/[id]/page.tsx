"use client";

import { useParams, useRouter } from "next/navigation";
import Link from "next/link";
import { ArrowLeft, MessageSquare, Network } from "lucide-react";

/**
 * The partition detail page (per-subject view) is intentionally minimal.
 * The server's PartitionSummary only exposes {id, message_count, topic} —
 * there is no per-partition detail endpoint.
 *
 * We redirect the user to the message browser filtered by this partition,
 * which is the most useful action from here.
 */
export default function PartitionDetailPage() {
  const { id } = useParams();
  const router = useRouter();
  const partitionId = decodeURIComponent(id as string);

  return (
    <div className="space-y-6 animate-in fade-in duration-300">
      {/* Header */}
      <div className="flex items-center gap-4">
        <button
          onClick={() => router.back()}
          className="p-2 rounded-lg border border-white/10 bg-white/[0.03] hover:bg-white/[0.08] transition-colors"
        >
          <ArrowLeft className="w-4 h-4 text-slate-300" />
        </button>
        <div>
          <h1 className="text-2xl font-bold text-white flex items-center gap-2">
            <Network className="w-6 h-6 text-emerald-400" />
            <span className="font-mono text-emerald-300">{partitionId}</span>
          </h1>
          <p className="text-slate-400 text-sm mt-0.5">Partition / NATS Subject</p>
        </div>
      </div>

      {/* CTA to message browser */}
      <div className="p-6 rounded-xl border border-blue-500/20 bg-blue-500/5 flex items-center justify-between gap-4">
        <div>
          <div className="text-white font-semibold mb-1">Browse messages for this partition</div>
          <div className="text-slate-400 text-sm font-mono">{partitionId}</div>
        </div>
        <Link
          href={`/messages?partition=${encodeURIComponent(partitionId)}`}
          className="flex items-center gap-2 px-4 py-2.5 rounded-xl bg-blue-500/20 hover:bg-blue-500/30 border border-blue-400/30 text-blue-300 text-sm font-medium transition-colors whitespace-nowrap"
        >
          <MessageSquare className="w-4 h-4" />
          Message Browser →
        </Link>
      </div>
    </div>
  );
}
