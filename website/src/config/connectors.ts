import type { SinkConfig, SourceConfig } from '@/types/api';

type ConnectorType = SourceConfig['type'] | SinkConfig['type'];
type ConnectorTypeWithPort = Exclude<ConnectorType, 'elasticsearch'>;

export const CONNECTOR_LABELS: Record<string, string> = {
  postgres: 'PostgreSQL',
  mysql: 'MySQL',
  elasticsearch: 'Elasticsearch',
  clickhouse: 'ClickHouse',
};

export const SOURCE_CONNECTOR_TYPES = ['postgres', 'mysql'] as const satisfies ReadonlyArray<SourceConfig['type']>;

export const SINK_CONNECTOR_TYPES = [
  'postgres',
  'mysql',
  'elasticsearch',
  'clickhouse',
] as const satisfies ReadonlyArray<SinkConfig['type']>;

export const DEFAULT_CONNECTOR_PORTS = {
  postgres: 5432,
  mysql: 3306,
  clickhouse: 9000,
} as const satisfies Record<ConnectorTypeWithPort, number>;

export const DEFAULT_CONNECTOR_USERNAMES = {
  postgres: 'postgres',
  mysql: 'root',
  clickhouse: 'default',
} as const satisfies Record<ConnectorTypeWithPort, string>;

export function connectorLabel(type: string): string {
  return CONNECTOR_LABELS[type] ?? type.toUpperCase();
}

export function defaultConnectorPort(type: ConnectorType): number {
  if (type === 'elasticsearch') return DEFAULT_CONNECTOR_PORTS.postgres;
  return DEFAULT_CONNECTOR_PORTS[type];
}

export function defaultConnectorUsername(type: ConnectorType): string {
  if (type === 'elasticsearch') return '';
  return DEFAULT_CONNECTOR_USERNAMES[type];
}
