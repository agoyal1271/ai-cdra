import { useState } from 'react';
import { useWorkspace } from '../../hooks/useWorkspace';
import { AssetPreviewPane } from './AssetPreviewPane';
import { TYPE_STYLES } from '../../constants/design';
import type { DiscoveredAsset } from '../../types/agents';

function groupByNamespace(assets: DiscoveredAsset[]): Record<string, DiscoveredAsset[]> {
  const groups: Record<string, DiscoveredAsset[]> = {};

  for (const asset of assets) {
    const namespace =
      asset.metadata?.sr_info?.namespace ??
      (asset.metadata?.namespace as string | undefined) ??
      'Other';

    if (!groups[namespace]) groups[namespace] = [];
    groups[namespace].push(asset);
  }

  return groups;
}

function formatNumber(n: number | string | undefined): string {
  if (!n) return '';
  const num = typeof n === 'string' ? parseInt(n, 10) : n;
  if (isNaN(num)) return '';
  return num.toLocaleString();
}

function WorkspaceItem({ asset, onUnpin, onSelect }: { asset: DiscoveredAsset; onUnpin: () => void; onSelect: () => void }) {
  const style = TYPE_STYLES[asset.asset_type] ?? TYPE_STYLES.hdfs_path;
  const pipeline = asset.pipeline_suggestion?.recommended_pipeline ?? '—';
  const match = asset.pipeline_suggestion?.confidence_score
    ? Math.round(asset.pipeline_suggestion.confidence_score)
    : null;
  const meta = asset.metadata ?? {};

  const hasPii = asset.pii_risk;
  const fieldCount = (meta.fields as Array<{name: string}>)?.length ??
                     (meta.schema as {fields?: Array<{name: string}>})?.fields?.length ?? 0;
  const rowCount = meta.row_count ? formatNumber(meta.row_count) : null;
  const msgCount = meta.estimated_messages ? formatNumber(meta.estimated_messages) : null;

  return (
    <div className="flex items-center gap-3 px-3 py-2.5 bg-slate-800/40 rounded-lg hover:bg-slate-800/60 transition-colors cursor-pointer" onClick={onSelect}>
      <span className="text-lg flex-shrink-0">{style.icon}</span>

      <div className="flex-1 min-w-0">
        <div className="text-sm font-medium text-white truncate">{asset.name}</div>
        <div className="text-[11px] text-slate-400 flex items-center gap-2 flex-wrap mt-0.5">
          <span className="text-orange-400 font-semibold">→ {pipeline}</span>
          {match && <span>{match}% match</span>}
          {hasPii && <span className="text-red-400">⚠ PII</span>}
          {fieldCount > 0 && <span>{fieldCount} fields</span>}
          {rowCount && <span>~{rowCount} rows</span>}
          {msgCount && <span>~{msgCount} msgs</span>}
        </div>
      </div>

      <div className="flex items-center gap-2 flex-shrink-0">
        <button onClick={(e) => { e.stopPropagation(); onSelect(); }} className="text-[10px] text-slate-400 hover:text-orange-400 transition-colors font-medium">
          View →
        </button>
        <button
          onClick={(e) => { e.stopPropagation(); onUnpin(); }}
          className="text-slate-400 hover:text-red-400 transition-colors"
          title="Remove from workspace"
        >
          ✕
        </button>
      </div>
    </div>
  );
}

export function Workspace() {
  const { pinnedAssets, unpin, pin, isPinned } = useWorkspace();
  const [expandedGroups, setExpandedGroups] = useState<Record<string, boolean>>({});
  const [selectedAsset, setSelectedAsset] = useState<string | null>(null);

  if (pinnedAssets.length === 0) {
    return (
      <div className="h-full flex items-center justify-center">
        <div className="text-center">
          <div className="text-4xl mb-4">📌</div>
          <h2 className="text-xl font-bold text-white mb-2">Your workspace is empty</h2>
          <p className="text-sm text-slate-400 mb-6">Pin assets from Source Scout to start building.</p>
          <p className="text-sm text-slate-500">Use the Source Scout tab to discover and pin assets →</p>
        </div>
      </div>
    );
  }

  const groups = groupByNamespace(pinnedAssets);
  const sortedGroups = Object.entries(groups).sort(([a], [b]) => a.localeCompare(b));
  const selectedAssetObj = pinnedAssets.find(a => a.id === selectedAsset);

  return (
    <div className="h-full flex flex-col overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between gap-4 flex-shrink-0 px-6 py-4 border-b border-slate-700">
        <div>
          <h2 className="text-lg font-bold text-white">My Workspace</h2>
          <p className="text-xs text-slate-400">{pinnedAssets.length} asset{pinnedAssets.length !== 1 ? 's' : ''} pinned</p>
        </div>
        <div className="flex items-center gap-2">
          <button className="text-xs px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg transition-colors flex items-center gap-1.5">
            ⬇ Export JSON
          </button>
          <button className="text-xs px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg transition-colors">
            Clear all
          </button>
        </div>
      </div>

      {/* Three-column layout */}
      <div className="flex flex-1 min-h-0">
        {/* LEFT: Asset list sidebar (20%) */}
        <div className="w-1/5 border-r border-slate-700 overflow-y-auto bg-slate-900/50">
          <div className="p-4 space-y-3">
            {sortedGroups.map(([namespace, assets]) => {
              const isExpanded = expandedGroups[namespace] !== false;
              return (
                <div key={namespace}>
                  <button
                    onClick={() => setExpandedGroups(prev => ({ ...prev, [namespace]: !prev[namespace] }))}
                    className="w-full flex items-center gap-2 px-2 py-1.5 text-left hover:bg-slate-800/40 rounded transition-colors group"
                  >
                    <span className="text-slate-400 group-hover:text-slate-300 text-xs">{isExpanded ? '▼' : '▶'}</span>
                    <span className="font-semibold text-white text-sm">{namespace}</span>
                    <span className="text-xs text-slate-400">({assets.length})</span>
                  </button>
                  {isExpanded && (
                    <div className="space-y-0.5 mt-1">
                      {assets.map(asset => {
                        const style = TYPE_STYLES[asset.asset_type] ?? TYPE_STYLES.hdfs_path;
                        const isSelected = selectedAsset === asset.id;
                        return (
                          <button
                            key={asset.id}
                            onClick={() => setSelectedAsset(asset.id)}
                            className={`w-full flex items-center gap-2 px-2 py-1.5 rounded text-left transition-colors ${
                              isSelected
                                ? 'bg-slate-700/60 border border-slate-600'
                                : 'hover:bg-slate-800/40'
                            }`}
                          >
                            <span className="text-sm flex-shrink-0">{style.icon}</span>
                            <span className="text-xs text-slate-300 truncate font-medium">{asset.name}</span>
                            <span className="text-[10px] text-slate-500 flex-shrink-0">✓</span>
                          </button>
                        );
                      })}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>

        {/* MIDDLE: Asset preview (25%) */}
        <div className="w-1/4 border-r border-slate-700 overflow-y-auto bg-slate-900">
          {selectedAssetObj ? (
            <AssetPreviewPane asset={selectedAssetObj} />
          ) : (
            <div className="h-full flex items-center justify-center text-center">
              <div className="text-slate-500 text-xs">
                <div className="text-2xl mb-2">👈</div>
                <p>Select an asset to preview</p>
              </div>
            </div>
          )}
        </div>

        {/* RIGHT: Asset detail + Quality Check (55%) */}
        <div className="flex-1 overflow-y-auto bg-slate-900/30">
          {selectedAssetObj ? (
            <AssetPreviewPane asset={selectedAssetObj} />
          ) : (
            <div className="h-full flex items-center justify-center text-center">
              <div className="text-slate-500 text-xs">
                <div className="text-2xl mb-2">🔍</div>
                <p>Select an asset to view details</p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
