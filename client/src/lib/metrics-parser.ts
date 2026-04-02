export interface PrometheusMetric {
  name: string;
  labels: Record<string, string>;
  value: number;
}

export function parsePrometheusMetrics(text: string): PrometheusMetric[] {
  const lines = text.split('\n');
  const metrics: PrometheusMetric[] = [];

  for (const line of lines) {
    if (line.startsWith('#') || !line.trim()) continue;

    // Pattern: metric_name{label1="val1",label2="val2"} 123.45
    // or:    metric_name 123.45
    const match = line.match(/^([a-zA-Z0-9_]+)(\{.*\})?\s+([0-9\.e+\-]+)$/);
    if (!match) continue;

    const name = match[1];
    const labelsStr = match[2];
    const value = parseFloat(match[3]);

    const labels: Record<string, string> = {};
    if (labelsStr) {
      // Remove { and }
      const inner = labelsStr.slice(1, -1);
      // Split by comma, but be careful with commas inside quotes if any (prometheus labels are usually simple)
      const labelPairs = inner.split(',');
      for (const pair of labelPairs) {
        const [k, v] = pair.split('=');
        if (k && v) {
          labels[k.trim()] = v.trim().replace(/^"(.*)"$/, '$1');
        }
      }
    }

    metrics.push({ name, labels, value });
  }

  return metrics;
}
