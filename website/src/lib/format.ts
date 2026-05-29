/**
 * Utility functions for formatting display values.
 */
import i18n from './i18n';

type NumericValue = number | string | null | undefined;

function toFiniteNumber(value: NumericValue): number {
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  if (typeof value === 'string') {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

/** Formats large numbers with K/M/B suffixes. */
export function formatNumber(value: NumericValue): string {
  const num = toFiniteNumber(value);
  if (num >= 1_000_000_000) return `${(num / 1_000_000_000).toFixed(1)}B`;
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`;
  if (num >= 1_000) return `${(num / 1_000).toFixed(1)}K`;
  return num.toFixed(num % 1 === 0 ? 0 : 1);
}

/** Formats bytes into human-readable size. */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

/** Formats seconds into human-readable duration (e.g., "2d 5h 30m"). */
export function formatDuration(totalSeconds: NumericValue): string {
  const value = toFiniteNumber(totalSeconds);
  const days = Math.floor(value / 86400);
  const hours = Math.floor((value % 86400) / 3600);
  const minutes = Math.floor((value % 3600) / 60);
  const seconds = Math.floor(value % 60);

  const parts: string[] = [];
  if (days > 0) parts.push(`${days}${i18n.t('common.days')}`);
  if (hours > 0) parts.push(`${hours}${i18n.t('common.hours')}`);
  if (minutes > 0) parts.push(`${minutes}${i18n.t('common.minutes')}`);
  if (parts.length === 0) parts.push(`${seconds}${i18n.t('common.seconds')}`);

  return parts.join(' ');
}

/** Formats a percentage value with fixed decimals. */
export function formatPercent(value: NumericValue, decimals = 2): string {
  return `${toFiniteNumber(value).toFixed(decimals)}%`;
}

/** Sums all partition lag values from a partition_lag map. */
export function sumPartitionLag(lag: Record<number, NumericValue> | undefined): number {
  if (!lag) return 0;
  return Object.values(lag).reduce<number>((sum, v) => sum + toFiniteNumber(v), 0);
}
