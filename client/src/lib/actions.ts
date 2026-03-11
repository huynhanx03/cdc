"use server";

import * as grpcLib from './grpc';

export async function getStatsAction() {
  return grpcLib.getStats();
}

export async function listTopicsAction() {
  const result = await grpcLib.listTopics();
  // Server actions must return plain objects
  return result;
}

export async function getTopicAction(name: string) {
  return grpcLib.getTopic(name);
}

export async function listPartitionsAction(topic?: string) {
  return grpcLib.listPartitions(topic);
}

export async function getPartitionAction(id: number, topic?: string) {
  return grpcLib.getPartition(id, topic);
}

export async function getMessagesAction(partitionId?: number | null, offset: number = 0, limit: number = 50) {
  const data = await grpcLib.getMessages(partitionId ?? undefined, offset, limit);
  // Transform buffer values to string so they can cross the network to the client
  return {
    messages: data.messages.map(m => ({
      ...m,
      value: Buffer.isBuffer(m.value) ? m.value.toString('base64') : m.value || ''
    }))
  };
}

export async function healthCheckAction() {
  return grpcLib.healthCheck();
}

export async function getConfigAction() {
  return grpcLib.getConfig();
}
