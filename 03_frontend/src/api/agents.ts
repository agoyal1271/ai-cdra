import type { Agent, DiscoveredAsset, HealthStatus, SSEEvent, QualityCheckCode, QualityCheckResults } from '../types/agents';

const BASE = '/api';

export async function fetchAgents(): Promise<Agent[]> {
  const res = await fetch(`${BASE}/agents`);
  const data = await res.json();
  return data.agents;
}

export async function fetchHealth(): Promise<HealthStatus> {
  const res = await fetch(`${BASE}/health`);
  return res.json();
}

export async function fetchAssets(): Promise<DiscoveredAsset[]> {
  const res = await fetch(`${BASE}/agents/assets`);
  const data = await res.json();
  return data.assets;
}

export function streamDiscover(
  goal: string,
  onEvent: (event: SSEEvent) => void,
  onError?: (err: Error) => void,
): () => void {
  let aborted = false;
  (async () => {
    try {
      const res = await fetch(`${BASE}/agents/discover`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ goal }),
      });
      const reader = res.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      while (!aborted) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? '';
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            try {
              const event: SSEEvent = JSON.parse(line.slice(6));
              onEvent(event);
            } catch { /* skip malformed */ }
          }
        }
      }
    } catch (e) {
      if (!aborted) onError?.(e as Error);
    }
  })();
  return () => { aborted = true; };
}

export function streamOrchestrate(
  goal: string,
  onEvent: (event: SSEEvent) => void,
  onError?: (err: Error) => void,
): () => void {
  let aborted = false;
  (async () => {
    try {
      const res = await fetch(`${BASE}/agents/orchestrate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ goal }),
      });
      const reader = res.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      while (!aborted) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? '';
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            try {
              const event: SSEEvent = JSON.parse(line.slice(6));
              onEvent(event);
            } catch { /* skip */ }
          }
        }
      }
    } catch (e) {
      if (!aborted) onError?.(e as Error);
    }
  })();
  return () => { aborted = true; };
}

export async function generateQualityCheck(
  tableName: string,
  fields?: Array<{ name: string; type?: string }>,
): Promise<QualityCheckCode> {
  const res = await fetch(`${BASE}/agents/quality-check/generate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ table_name: tableName, fields }),
  });
  return res.json();
}

export async function fetchQualityCheckResults(tableName: string): Promise<QualityCheckResults> {
  const res = await fetch(`${BASE}/agents/quality-check/results?table_name=${encodeURIComponent(tableName)}`);
  return res.json();
}

export function streamExecuteQualityCheck(
  tableName: string,
  engine: 'impala' | 'trino' | 'spark' | 'flink',
  onEvent: (event: any) => void,
  onError?: (err: Error) => void,
): () => void {
  let aborted = false;
  (async () => {
    try {
      const res = await fetch(`${BASE}/agents/quality-check/execute`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table_name: tableName, engine }),
      });
      const reader = res.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      while (!aborted) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? '';
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            try {
              const event = JSON.parse(line.slice(6));
              onEvent(event);
            } catch { /* skip malformed */ }
          }
        }
      }
    } catch (e) {
      if (!aborted) onError?.(e as Error);
    }
  })();
  return () => { aborted = true; };
}

export async function askAsset(
  question: string,
  assetName: string,
  fields: Array<{ name: string; type?: string }>,
  assetType: 'iceberg_table' | 'kafka_topic',
  engine: 'impala' | 'trino' | 'spark' = 'impala',
): Promise<{ sql: string; understanding: string; engine: string }> {
  const res = await fetch(`${BASE}/nl-to-code/ask-asset`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ question, asset_name: assetName, fields, asset_type: assetType, engine }),
  });
  return res.json();
}

export async function fetchAssetLineage(
  assetName: string,
  assetType: 'iceberg_table' | 'kafka_topic',
): Promise<import('../types/agents').OmLineage & { found: boolean }> {
  const omType = assetType === 'kafka_topic' ? 'topic' : 'table';
  const res = await fetch(`${BASE}/openmetadata/lineage?asset=${encodeURIComponent(assetName)}&asset_type=${omType}`);
  return res.json();
}

export async function runAssetSQL(
  sql: string,
  engine: 'impala' | 'trino' | 'spark',
): Promise<{ columns: string[]; rows: string[][]; row_count?: number; executed_on?: string; dialect?: string; error?: string }> {
  const res = await fetch(`${BASE}/nl-to-code/run-asset`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql, engine }),
  });
  return res.json();
}

export async function fetchAllIcebergTables(): Promise<Array<{ name: string; location: string; fields: Array<{ name: string; type: string }>; snapshots: number; mock?: boolean; error?: string }>> {
  const res = await fetch(`${BASE}/iceberg/tables`);
  const data = await res.json();
  return data.tables || [];
}
