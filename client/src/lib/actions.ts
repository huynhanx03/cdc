"use server";

import * as grpc from "@/lib/grpc";

export async function getStatsAction() {
  return serialize(await grpc.getStats());
}

export async function healthCheckAction() {
  return serialize(await grpc.healthCheck());
}

export async function getPrometheusMetricsAction() {
  const url = process.env.METRICS_URL || "http://localhost:9091/metrics";
  try {
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.text();
  } catch (err) {
    console.error("fetch metrics:", err);
    return "";
  }
}

export async function getConfigAction() {
  return serialize(await grpc.getConfig());
}

export async function listTopicsAction(limit?: number, page?: number) {
  return serialize(await grpc.listTopics(limit, page));
}

export async function listPartitionsAction(topic: string, limit?: number, page?: number) {
  return serialize(await grpc.listPartitions(topic, limit, page));
}

function serialize(obj: any): any {
  if (obj === null || obj === undefined) return obj;
  if (obj instanceof Uint8Array || Buffer.isBuffer(obj)) {
    return Buffer.from(obj).toString("base64");
  }
  if (Array.isArray(obj)) {
    return obj.map(serialize);
  }
  if (typeof obj === "object") {
    return Object.fromEntries(
      Object.entries(obj).map(([k, v]) => [k, serialize(v)])
    );
  }
  return obj;
}

export async function listMessagesAction(topic?: string, partition?: string, limit?: number, page?: number) {
  const res = await grpc.listMessages({ topic, partition, limit, page });
  return serialize(res);
}

export async function addSourceAction(source: grpc.SourceConfig) {
  return serialize(await grpc.addSource(source));
}

export async function removeSourceAction(instanceId: string) {
  return serialize(await grpc.removeSource(instanceId));
}
