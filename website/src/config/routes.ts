/**
 * Route paths — single source of truth for navigation.
 * Used by React Router, Sidebar, and Breadcrumb components.
 */
export const ROUTES = {
  DASHBOARD: '/',
  EXPLORER: '/explorer',
  EXPLORER_TOPICS: '/explorer/topics',
  EXPLORER_TOPIC_DETAIL: '/explorer/topics/:topic',
  EXPLORER_TOPIC_PARTITION: '/explorer/topics/:topic/partitions/:partition',
  EXPLORER_CONSUMERS: '/explorer/consumers',
  EXPLORER_CONSUMER_DETAIL: '/explorer/consumers/:consumer',
  EXPLORER_DLQ: '/explorer/dlq',
  MANAGER: '/manager',
  MANAGER_SOURCES: '/manager/sources',
  MANAGER_SINKS: '/manager/sinks',
  MANAGER_FLOWS: '/manager/flows',
  MANAGER_FLOW_DETAIL: '/manager/flows/:id',
} as const;
