import { useState, useCallback, useRef } from 'react';
import { streamDiscover } from '../api/agents';
import type { DiscoveredAsset, SSEEvent } from '../types/agents';

export interface ThoughtLine {
  id: number;
  content: string;
  type: 'thought' | 'warning' | 'complete' | 'error';
  source: string; // 'all' | 'kafka_topic' | 'iceberg_table' | 'ozone_volume' | 'hdfs_path'
}

export function useDiscovery() {
  const [assets, setAssets] = useState<DiscoveredAsset[]>([]);
  const [thoughts, setThoughts] = useState<ThoughtLine[]>([]);
  const [running, setRunning] = useState(false);
  const [enriching, setEnriching] = useState(false);
  const [summary, setSummary] = useState<string | null>(null);
  const [counts, setCounts] = useState<Record<string, number> | null>(null);
  const cancelRef = useRef<(() => void) | null>(null);
  const idRef = useRef(0);

  const discover = useCallback((goal: string) => {
    setAssets([]);
    setThoughts([]);
    setSummary(null);
    setCounts(null);
    setRunning(true);

    cancelRef.current?.();
    cancelRef.current = streamDiscover(
      goal,
      (event: SSEEvent) => {
        const id = ++idRef.current;
        switch (event.type) {
          case 'thought':
            setThoughts(prev => [...prev, { id, content: event.content ?? '', type: 'thought', source: event.source ?? 'all' }]);
            break;
          case 'warning':
            setThoughts(prev => [...prev, { id, content: event.content ?? '', type: 'warning', source: event.source ?? 'all' }]);
            break;
          case 'asset':
            if (event.data) setAssets(prev => [...prev, event.data!]);
            break;
          case 'asset_update':
            if (event.data) {
              setAssets(prev => prev.map(a => a.id === event.data!.id ? event.data! : a));
            }
            break;
          case 'lineage':
            if ((event as any).asset) {
              const assetName = (event as any).asset as string;
              setAssets(prev => prev.map(a =>
                (a.name === assetName || a.id === assetName)
                  ? { ...a, lineage: {
                      entity:     (event as any).entity,
                      upstream:   (event as any).upstream   ?? [],
                      downstream: (event as any).downstream ?? [],
                      edge_count: (event as any).edge_count ?? 0,
                    }}
                  : a
              ));
            }
            break;
          case 'scan_ready':
            // Assets are all visible — unlock the UI but LLM enrichment continues
            setCounts(event.counts ?? null);
            setEnriching(true);
            break;
          case 'complete':
            setSummary(event.summary ?? null);
            setCounts(event.counts ?? null);
            setThoughts(prev => [...prev, { id, content: event.summary ?? 'Complete.', type: 'complete', source: 'all' }]);
            setEnriching(false);
            setRunning(false);
            break;
          case 'error':
            setThoughts(prev => [...prev, { id, content: event.message ?? 'Error', type: 'error', source: 'all' }]);
            setRunning(false);
            break;
          case 'stream_end':
            setRunning(false);
            break;
        }
      },
      (err) => {
        setThoughts(prev => [...prev, { id: ++idRef.current, content: err.message, type: 'error', source: 'all' }]);
        setRunning(false);
      },
    );
  }, []);

  const cancel = useCallback(() => {
    cancelRef.current?.();
    setRunning(false);
  }, []);

  return { assets, thoughts, running, enriching, summary, counts, discover, cancel };
}
