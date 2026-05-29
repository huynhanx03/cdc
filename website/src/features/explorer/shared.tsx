export function decodePayload(base64Str: string): string {
  if (!base64Str) return '';
  try {
    return decodeURIComponent(
      atob(base64Str)
        .split('')
        .map((c) => `%${(`00${c.charCodeAt(0).toString(16)}`).slice(-2)}`)
        .join(''),
    );
  } catch {
    try {
      return atob(base64Str);
    } catch {
      return base64Str;
    }
  }
}

export function parseSubject(subject: string) {
  const parts = subject.split('.');
  return {
    stream: parts[0] || '',
    sourceId: parts[1] || '',
    schema: parts[2] || '',
    table: parts[3] || '',
    partition: parts[4] || '',
    topic: parts.length >= 4 ? parts.slice(0, 4).join('.') : subject,
    shortName: parts.length >= 4 ? `${parts[2]}.${parts[3]}` : subject,
  };
}

export function formatBytes(value: number) {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / 1024 / 1024).toFixed(1)} MB`;
}

export function formatCount(value: number | string | null | undefined) {
  const numeric = typeof value === 'string' ? Number(value) : (value ?? 0);
  if (!Number.isFinite(numeric)) return '0';
  return numeric.toLocaleString();
}

export function messageSize(data: string) {
  if (!data) return 0;
  return decodePayload(data).length;
}

export function formatTime(timestamp: string | number) {
  if (!timestamp) return '-';
  const raw = typeof timestamp === 'number' ? timestamp : Number(timestamp);
  const date = Number.isFinite(raw) ? new Date(raw) : new Date(timestamp);
  if (Number.isNaN(date.getTime())) return String(timestamp);
  return date.toLocaleString();
}
