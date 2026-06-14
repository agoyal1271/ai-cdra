import { useState, useCallback, useEffect } from 'react';
import type { DiscoveredAsset } from '../types/agents';

const STORAGE_KEY = 'scout_workspace';

export function useWorkspace() {
  const [pinnedAssets, setPinnedAssets] = useState<DiscoveredAsset[]>([]);
  const [undoQueue, setUndoQueue] = useState<{ id: string; asset: DiscoveredAsset; timeout: ReturnType<typeof setTimeout> } | null>(null);

  useEffect(() => {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) {
        setPinnedAssets(JSON.parse(stored));
      }
    } catch {
      // Silently fail if localStorage is corrupted
    }
  }, []);

  const persist = useCallback((assets: DiscoveredAsset[]) => {
    setPinnedAssets(assets);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(assets));
  }, []);

  const pin = useCallback((asset: DiscoveredAsset) => {
    setPinnedAssets(prev => {
      if (prev.some(a => a.id === asset.id)) return prev;
      const updated = [...prev, asset];
      persist(updated);
      return updated;
    });
    // Clear any pending undo for this asset
    if (undoQueue?.id === asset.id) {
      clearTimeout(undoQueue.timeout);
      setUndoQueue(null);
    }
  }, [persist, undoQueue]);

  const unpin = useCallback((assetId: string) => {
    setPinnedAssets(prev => {
      const asset = prev.find(a => a.id === assetId);
      if (!asset) return prev;

      // Clear previous undo
      if (undoQueue?.id === assetId) {
        clearTimeout(undoQueue.timeout);
      }

      // Start 4s undo window
      const timeout = setTimeout(() => {
        setUndoQueue(null);
      }, 4000);

      setUndoQueue({ id: assetId, asset, timeout });

      // Optimistically remove from UI
      const updated = prev.filter(a => a.id !== assetId);
      persist(updated);
      return updated;
    });
  }, [undoQueue, persist]);

  const undo = useCallback((assetId: string) => {
    if (undoQueue?.id !== assetId) return;

    clearTimeout(undoQueue.timeout);
    const asset = undoQueue.asset;
    const updated = [...pinnedAssets, asset];
    persist(updated);
    setUndoQueue(null);
  }, [undoQueue, pinnedAssets, persist]);

  const isPinned = useCallback((assetId: string) => {
    return pinnedAssets.some(a => a.id === assetId);
  }, [pinnedAssets]);

  const clear = useCallback(() => {
    if (undoQueue) clearTimeout(undoQueue.timeout);
    persist([]);
    setUndoQueue(null);
  }, [persist, undoQueue]);

  return {
    pinnedAssets,
    pin,
    unpin,
    undo,
    isPinned,
    clear,
    pendingUndo: undoQueue,
  };
}
