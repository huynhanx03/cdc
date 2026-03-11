"use client";

import { useEffect, useState } from "react";
import { getConfigAction } from "@/lib/actions";
import type { GetConfigResponse } from "@/lib/grpc";

export default function ConfigPage() {
  const [config, setConfig] = useState<GetConfigResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchConfig = async () => {
      try {
        const data = await getConfigAction();
        setConfig(data);
      } catch (e) {
        console.error("Failed to fetch config:", e);
      } finally {
        setLoading(false);
      }
    };
    fetchConfig();
  }, []);

  if (loading) return (
    <div className="flex items-center justify-center p-20">
      <div className="animate-pulse text-secondary">Loading system configuration...</div>
    </div>
  );
  
  if (!config) return (
    <div className="card border-error p-8 text-center">
      <h3 className="text-error font-bold text-xl mb-2">Unavailable</h3>
      <p className="text-secondary">Could not fetch configuration from the CDC engine.</p>
    </div>
  );

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">System Configuration</h1>
        <p className="page-subtitle">View active source and sink pipelines currently managed by the CDC engine.</p>
      </div>

      <div className="grid gap-6">
        {/* App Info */}
        <div className="card">
          <div className="flex justify-between items-start mb-4">
            <div>
              <h2 className="text-xl font-bold mb-1">Instance: {config.config.name || "CDC Engine"}</h2>
              <div className="text-sm text-secondary">Operational Mode: Standard</div>
            </div>
            <div className="badge badge-primary">Log Mode: {config.config.logMode}</div>
          </div>
        </div>

        {/* Source */}
        <div className="card">
          <div className="flex justify-between items-center mb-6">
            <h2 className="text-xl font-bold">Source Pipeline</h2>
            <div className="tag blue">{config.config.source.type}</div>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="space-y-4">
              <div className="input-group">
                <label>Connection String Host</label>
                <div className="text-primary font-mono bg-black/20 p-2 rounded border border-white/5">
                  {config.config.source.host}:{config.config.source.port}
                </div>
              </div>
              <div className="input-group">
                <label>Database Name</label>
                <div className="text-primary font-mono bg-black/20 p-2 rounded border border-white/5">
                  {config.config.source.database}
                </div>
              </div>
            </div>
            <div>
              <div className="input-group">
                <label>Tables Tracked ({config.config.source.tables.length})</label>
                <div className="flex flex-wrap gap-2 mt-2">
                  {config.config.source.tables.length > 0 ? (
                    config.config.source.tables.map(t => (
                      <span key={t} className="tag tag-outline">{t}</span>
                    ))
                  ) : (
                    <span className="text-secondary italic text-sm">All tables in database</span>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Sinks */}
        <div className="space-y-4">
          <div className="flex justify-between items-end">
             <h2 className="text-xl font-bold">Output Sinks</h2>
             <span className="text-sm text-secondary">{config.config.sinks.length} active sinks</span>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {config.config.sinks.map((s, idx) => (
              <div key={idx} className="card bg-muted/50">
                <div className="flex justify-between items-center mb-4">
                  <div className="font-bold flex items-center gap-2">
                    <span className="w-2 h-2 rounded-full bg-accent animate-pulse" />
                    {s.type}
                  </div>
                  <div className="text-xs text-secondary px-2 py-1 bg-white/5 rounded">Sink #{idx + 1}</div>
                </div>
                <div className="text-sm space-y-3">
                  <div>
                    <label className="text-xs text-secondary block mb-1">Destination URLs</label>
                    <div className="font-mono text-xs break-all bg-black/20 p-2 rounded border border-white/5">
                      {s.url.join(", ")}
                    </div>
                  </div>
                  {s.indexPrefix && (
                    <div>
                      <label className="text-xs text-secondary block mb-1">Index Prefix</label>
                      <div className="font-mono text-xs bg-black/20 p-2 rounded border border-white/5">
                        {s.indexPrefix}
                      </div>
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Available Plugins */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mt-4">
           <div className="card bg-muted/30 border-dashed">
              <h3 className="font-bold mb-3 text-sm text-secondary uppercase tracking-wider">Storage Loaders Available</h3>
              <div className="flex flex-wrap gap-2">
                {config.availableSources.map(s => <span key={s} className="tag tag-outline ">{s}</span>)}
              </div>
           </div>
           <div className="card bg-muted/30 border-dashed">
              <h3 className="font-bold mb-3 text-sm text-secondary uppercase tracking-wider">Export Sinks Available</h3>
              <div className="flex flex-wrap gap-2">
                {config.availableSinks.map(s => <span key={s} className="tag tag-outline">{s}</span>)}
              </div>
           </div>
        </div>
      </div>
    </div>
  );
}
