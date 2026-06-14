import { useState, useEffect } from 'react';
import { Radar, Search, Database, Settings } from 'lucide-react';
import { useDiscovery } from '../../hooks/useDiscovery';
import { useWorkspace } from '../../hooks/useWorkspace';
import { AssetListItem } from './AssetListItem';
import { AssetDetailPanel } from './AssetDetailPanel';
import { SettingsModal } from './SettingsModal';
import { ErrorBoundary } from '../ErrorBoundary';
import { fetchAllIcebergTables } from '../../api/agents';
import type { AssetType, DiscoveredAsset, QualityCheckResults } from '../../types/agents';

const FILTER_OPTIONS: { label: string; value: AssetType | 'all' }[] = [
  { label: 'All', value: 'all' },
  { label: '📨 Kafka', value: 'kafka_topic' },
  { label: '🧊 Iceberg', value: 'iceberg_table' },
  { label: '🪣 Ozone', value: 'ozone_volume' },
  { label: '📁 HDFS', value: 'hdfs_path' },
];

const SUGGESTED_GOALS = [
  'Discover all data sources in the Cloudera platform',
  'Find all Kafka topics and suggest ingestion pipelines',
  'Scan Iceberg catalog and profile all tables',
  'Find all customer-related data assets across Kafka, Iceberg, and Ozone',
];

function detectFilter(goal: string): AssetType | 'all' {
  const g = goal.toLowerCase();
  // Count matches per source — most-mentioned wins
  const scores: Record<AssetType, number> = {
    kafka_topic:   ['kafka','topic','stream','listen','consume','produce','broker','message'].filter(w => g.includes(w)).length,
    iceberg_table: ['iceberg','table','catalog','schema','column','namespace','snapshot'].filter(w => g.includes(w)).length,
    ozone_volume:  ['ozone','volume','bucket','object storage','s3','blobs'].filter(w => g.includes(w)).length,
    hdfs_path:     ['hdfs','hadoop','path','directory','file system','namenode'].filter(w => g.includes(w)).length,
  };
  const best = (Object.entries(scores) as [AssetType, number][]).sort((a, b) => b[1] - a[1])[0];
  return best[1] > 0 ? best[0] : 'all';
}

function groupAssetsByNamespace(assets: any[]) {
  const groups: Record<string, any[]> = {};
  for (const asset of assets) {
    const namespace = (asset.metadata?.sr_info?.namespace ?? asset.metadata?.namespace ?? 'Other') as string;
    if (!groups[namespace]) groups[namespace] = [];
    groups[namespace].push(asset);
  }
  return Object.entries(groups).sort(([a], [b]) => a.localeCompare(b));
}

export function SourceScout() {
  const { assets, thoughts, running, enriching, summary, counts, discover, cancel } = useDiscovery();
  const { pin, unpin, isPinned } = useWorkspace();
  const [goal, setGoal] = useState('');
  const [filter, setFilter] = useState<AssetType | 'all'>('all');
  const [selectedAsset, setSelectedAsset] = useState<string | null>(null);
  const [showAllTables, setShowAllTables] = useState(false);
  const [allTablesList, setAllTablesList] = useState<DiscoveredAsset[]>([]);
  const [loadingTables, setLoadingTables] = useState(false);
  const [qcResultsCache, setQcResultsCache] = useState<Record<string, QualityCheckResults>>(
    {}
  );
  const [showSettings, setShowSettings] = useState(false);

  useEffect(() => {
    if (showAllTables && allTablesList.length === 0 && !loadingTables) {
      setLoadingTables(true);
      fetchAllIcebergTables()
        .then(tables => {
          const assets: DiscoveredAsset[] = tables
            .filter(t => !t.error)
            .map((t, idx) => ({
              id: t.name || `table-${idx}`,
              name: t.name || 'Unknown',
              asset_type: 'iceberg_table',
              metadata: {
                fields: t.fields || [],
                snapshots: t.snapshots,
              },
              pii_risk: false,
              pipeline_suggestion: {
                recommended_pipeline: '—',
                reasoning: '',
                target_format: 'iceberg',
                target_location: '',
                key_considerations: [],
                source_type: 'iceberg_table',
                source_name: t.name || 'Unknown',
              },
            }));
          setAllTablesList(assets);
        })
        .catch(err => {
          console.error('Error fetching Iceberg tables:', err);
          setShowAllTables(false);
        })
        .finally(() => setLoadingTables(false));
    }
  }, [showAllTables, allTablesList.length, loadingTables]);

  const filteredAssets = showAllTables ? allTablesList : (filter === 'all' ? assets : assets.filter(a => a.asset_type === filter));

  const filteredThoughts = filter === 'all' ? thoughts : thoughts.filter(t => {
    if (t.type === 'complete' || t.type === 'error') return true;
    return t.source === filter || t.source === 'all';
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!goal.trim()) return;
    setFilter(detectFilter(goal.trim()));
    discover(goal.trim());
  };

  const handleQcResult = (assetId: string, results: QualityCheckResults) => {
    setQcResultsCache(prev => ({ ...prev, [assetId]: results }));
  };

  const resolvedAsset = selectedAsset
    ? filteredAssets.find(a => a.id === selectedAsset)
    : null;

  return (
    <div className="flex h-full">
      {/* LEFT PANE — search + filters + list */}
      <div className="w-[380px] flex-shrink-0 flex flex-col h-full border-r border-agent-dark-border bg-agent-dark-bg">
        {/* Header */}
        <div className="px-5 pt-5 pb-3 flex-shrink-0">
          <div className="flex items-center gap-2.5 mb-4">
            <Radar size={18} className="text-agent-orange" />
            <div>
              <h2 className="text-sm font-bold text-agent-text-primary">Source Scout</h2>
              <p className="text-xs text-agent-text-secondary">AI-driven data discovery</p>
            </div>
            {running && (
              <div className="ml-auto flex items-center gap-1.5 text-xs text-agent-orange">
                <span className="w-1.5 h-1.5 bg-agent-orange rounded-full animate-pulse" />
                Scanning
              </div>
            )}
            {!running && (
              <button
                onClick={() => setShowSettings(true)}
                className="ml-auto p-1.5 hover:bg-agent-dark-border rounded-lg transition-colors text-agent-text-secondary hover:text-agent-text-primary"
                title="Settings"
              >
                <Settings size={16} />
              </button>
            )}
          </div>

          {/* Search bar */}
          <form onSubmit={handleSubmit} className="flex gap-2">
            <div className="relative flex-1">
              <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-agent-text-secondary" />
              <input
                value={goal}
                onChange={e => setGoal(e.target.value)}
                placeholder="Discover data assets..."
                className="w-full bg-cdp-800 border border-cdp-700 rounded-lg pl-9 pr-3 py-2.5 text-sm text-white placeholder-cdp-600 focus:outline-none focus:border-cloudera transition-colors"
                disabled={running}
              />
            </div>
            {running ? (
              <button
                type="button"
                onClick={cancel}
                className="px-3 py-2 bg-red-800 hover:bg-red-700 text-white text-sm rounded-lg transition-colors"
              >
                Stop
              </button>
            ) : (
              <button
                type="submit"
                disabled={!goal.trim()}
                className="px-4 py-2 bg-cloudera hover:bg-cloudera-hover disabled:opacity-40 text-white text-sm rounded-lg font-semibold transition-colors"
              >
                Scout
              </button>
            )}
          </form>
        </div>

        {/* Suggested goals — only when empty */}
        {filteredAssets.length === 0 && !running && !showAllTables && (
          <div className="px-5 py-2 flex-shrink-0 space-y-2">
            {SUGGESTED_GOALS.slice(0, 3).map(g => (
              <button
                key={g}
                onClick={() => {
                  setGoal(g);
                  setFilter(detectFilter(g));
                  discover(g);
                }}
                className="w-full text-left text-xs bg-agent-dark-surface hover:bg-agent-dark-border border border-agent-dark-border text-agent-text-secondary hover:text-agent-text-primary px-3 py-2 rounded-lg transition-colors"
              >
                {g}
              </button>
            ))}
            <button
              onClick={() => setShowAllTables(true)}
              className="w-full text-left text-xs border border-agent-dark-border text-agent-text-secondary hover:text-agent-text-primary px-3 py-2 rounded-lg transition-colors flex items-center gap-2"
            >
              <Database size={12} /> Browse all Iceberg tables
            </button>
          </div>
        )}

        {/* Filter pills */}
        {(filteredAssets.length > 0 || showAllTables) && (
          <div className="px-5 py-2 flex gap-1 flex-wrap flex-shrink-0">
            {showAllTables && (
              <button
                onClick={() => setShowAllTables(false)}
                className="text-xs px-2.5 py-1 bg-agent-dark-surface hover:bg-agent-dark-border text-agent-text-secondary rounded-full"
              >
                ← Back
              </button>
            )}
            {!showAllTables &&
              FILTER_OPTIONS.map(f => (
                <button
                  key={f.value}
                  onClick={() => setFilter(f.value)}
                  className={`text-xs px-3 py-1 rounded-full transition-colors ${
                    filter === f.value
                      ? 'bg-agent-orange text-white'
                      : 'bg-agent-dark-surface text-agent-text-secondary border border-agent-dark-border hover:border-agent-orange hover:text-agent-text-primary'
                  }`}
                >
                  {f.label}
                </button>
              ))}
          </div>
        )}

        {/* Status strip */}
        {(running || enriching) && (
          <div className="mx-5 mb-2 px-3 py-2 bg-agent-dark-surface border border-agent-dark-border rounded-lg text-xs text-agent-text-secondary flex items-center gap-2 flex-shrink-0">
            <span className="w-1.5 h-1.5 bg-agent-orange rounded-full animate-pulse" />
            {enriching
              ? `AI analyzing ${filteredAssets.length} assets...`
              : 'Scanning assets...'}
          </div>
        )}

        {/* Summary line */}
        {summary && !running && (
          <div className="px-5 pb-1 text-xs text-agent-teal flex-shrink-0 truncate">
            {summary}
          </div>
        )}

        {/* Asset list — compact rows grouped by namespace */}
        <div className="flex-1 overflow-y-auto">
          {loadingTables && (
            <div className="flex items-center justify-center h-full text-[#6a8fa8] text-sm">
              Loading tables...
            </div>
          )}
          {filteredAssets.length === 0 && !running && !loadingTables && (
            <div className="flex items-center justify-center h-full text-[#6a8fa8] text-sm">
              {filteredAssets.length === 0 && assets.length > 0
                ? 'No assets match filter'
                : 'No assets discovered yet'}
            </div>
          )}
          {groupAssetsByNamespace(filteredAssets).map(([namespace, groupAssets]) => (
            <div key={namespace}>
              {/* Namespace header — sticky */}
              <div className="sticky top-0 bg-agent-dark-bg/95 backdrop-blur-sm px-4 py-2 text-xs font-semibold text-agent-text-secondary uppercase tracking-wider border-b border-agent-dark-border/50 flex items-center justify-between">
                <span>{namespace}</span>
                <span className="text-[10px] font-normal">{groupAssets.length}</span>
              </div>
              {groupAssets.map(asset => (
                <AssetListItem
                  key={asset.id}
                  asset={asset}
                  isSelected={selectedAsset === asset.id}
                  isPinned={isPinned(asset.id)}
                  qualityScore={qcResultsCache[asset.id]?.overall_score ?? null}
                  onSelect={() => setSelectedAsset(asset.id)}
                  onPin={() => pin(asset)}
                  onUnpin={() => unpin(asset.id)}
                />
              ))}
            </div>
          ))}
        </div>
      </div>

      {/* RIGHT PANE — asset detail panel, always inline */}
      <div className="flex-1 min-w-0 h-full bg-agent-dark-surface overflow-hidden">
        {resolvedAsset ? (
          <ErrorBoundary fallback={
            <div className="h-full flex items-center justify-center text-center">
              <div className="text-agent-text-secondary">
                <p>Error loading asset details</p>
                <button
                  onClick={() => setSelectedAsset(null)}
                  className="mt-4 px-3 py-1 text-sm bg-agent-dark-border hover:bg-agent-dark-border rounded"
                >
                  Close
                </button>
              </div>
            </div>
          }>
            <AssetDetailPanel
              asset={resolvedAsset}
              isPinned={isPinned(resolvedAsset.id)}
              onPin={() => pin(resolvedAsset)}
              onUnpin={() => unpin(resolvedAsset.id)}
              onClose={() => setSelectedAsset(null)}
              allAssets={filteredAssets}
              onSelectRelated={id => setSelectedAsset(id)}
              onQcResult={handleQcResult}
            />
          </ErrorBoundary>
        ) : (
          <div className="h-full flex items-center justify-center text-center">
            <div className="text-agent-text-secondary">
              <Radar size={36} className="mx-auto mb-4 opacity-40" />
              <p className="text-sm font-semibold text-agent-text-primary">Select an asset</p>
              <p className="text-xs mt-1">Click any row on the left to view details</p>
            </div>
          </div>
        )}
      </div>

      {/* Settings Modal */}
      <SettingsModal isOpen={showSettings} onClose={() => setShowSettings(false)} />
    </div>
  );
}
