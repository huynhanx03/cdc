"use server";

import * as grpcLib from './grpc';

export async function getStatsAction() {
  return grpcLib.getStats();
}

export async function healthCheckAction() {
  return grpcLib.healthCheck();
}

export async function getConfigAction() {
  return grpcLib.getConfig();
}

export async function addSourceAction(source: grpcLib.SourceConfig) {
  return grpcLib.addSource(source);
}

export async function removeSourceAction(instanceId: string) {
  return grpcLib.removeSource(instanceId);
}

export async function addSinkAction(sink: grpcLib.SinkConfig) {
  return grpcLib.addSink(sink);
}

export async function removeSinkAction(instanceId: string) {
  return grpcLib.removeSink(instanceId);
}
export async function listPartitionsAction() {
  return grpcLib.listPartitions();
}

export async function getMessagesAction(sourceId?: string, offset?: number, limit?: number, status?: number) {
  return grpcLib.getMessages(sourceId, offset, limit, status);
}
