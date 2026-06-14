import type { DiscoveredAsset } from '../../types/agents';
import { TYPE_STYLES, PIPELINE_COLORS, PII_NAMES } from '../../constants/design';

interface Props {
  asset: DiscoveredAsset;
  isPinned: boolean;
  onPin: () => void;
  onUnpin: () => void;
  onSelect: () => void;
}

export function AssetCard({ asset, isPinned, onPin, onUnpin, onSelect }: Props) {
  const style = TYPE_STYLES[asset.asset_type] ?? TYPE_STYLES.hdfs_path;
  const pipeline = asset.pipeline_suggestion?.recommended_pipeline ?? '—';
  const pipelineColor = PIPELINE_COLORS[pipeline] ?? 'text-slate-400';
  const meta = asset.metadata ?? {};

  const metaBits: string[] = [];
  if (meta.partitions)         metaBits.push(`${meta.partitions} partitions`);
  if (meta.estimated_messages) metaBits.push(`~${Number(meta.estimated_messages).toLocaleString()} msgs`);
  if (meta.row_count)          metaBits.push(`~${Number(meta.row_count).toLocaleString()} rows`);
  if (meta.size_mb)            metaBits.push(`${meta.size_mb} MB`);
  if (meta.object_count)       metaBits.push(`${Number(meta.object_count).toLocaleString()} objects`);
  if (meta.snapshots)          metaBits.push(`${meta.snapshots} snapshots`);

  const fields: Array<{ name: string; type?: string }> =
    (meta.fields as Array<{ name: string; type?: string }>) ??
    (meta.schema as { fields?: Array<{ name: string; type?: string }> })?.fields ??
    [];

  return (
    <div
      onClick={onSelect}
      className={`rounded-xl border p-4 flex flex-col gap-3 cursor-pointer transition-all ${style.bg} ${style.border} ${
        isPinned ? 'ring-1 ring-orange-500/40' : ''
      }`}
    >

      {/* Header */}
      <div className="flex items-start justify-between gap-2">
        <div className="flex items-center gap-2 min-w-0">
          <span className="text-xl flex-shrink-0">{style.icon}</span>
          <div className="min-w-0">
            <div className="text-[10px] uppercase tracking-wider text-slate-400">{style.label}</div>
            <div className="text-sm font-bold text-white truncate" title={asset.name}>{asset.name}</div>
          </div>
        </div>
        <div className="flex flex-col items-end gap-1 flex-shrink-0">
          <button
            onClick={e => {
              e.stopPropagation();
              isPinned ? onUnpin() : onPin();
            }}
            className={`text-lg transition-colors ${
              isPinned ? 'text-orange-500' : 'text-slate-600 hover:text-orange-400'
            }`}
            title={isPinned ? 'Unpin from workspace' : 'Pin to workspace'}
          >
            {isPinned ? '📌' : '📍'}
          </button>
          {asset.pii_risk && (
            <span className="text-[9px] bg-red-900 text-red-300 px-1.5 py-0.5 rounded font-semibold">⚠ PII</span>
          )}
          {(meta.mock as boolean) && (
            <span className="text-[9px] bg-slate-700 text-slate-400 px-1.5 py-0.5 rounded">MOCK</span>
          )}
        </div>
      </div>

      {/* Stats */}
      {metaBits.length > 0 && (
        <div className="flex flex-wrap gap-1.5">
          {metaBits.map((b, i) => (
            <span key={i} className="text-[10px] bg-slate-800 text-slate-300 px-2 py-0.5 rounded-full">{b}</span>
          ))}
        </div>
      )}

      {/* Schema — full column list, scrollable */}
      {fields.length > 0 && (
        <div>
          <div className="flex items-center justify-between mb-1.5">
            <span className="text-[9px] text-slate-500 uppercase tracking-wider">Schema</span>
            <span className="text-[9px] text-slate-600">{fields.length} column{fields.length !== 1 ? 's' : ''}</span>
          </div>
          <div className="max-h-40 overflow-y-auto flex flex-col gap-px pr-0.5">
            {fields.map((f, i) => {
              const isPii = PII_NAMES.has(f.name.toLowerCase());
              return (
                <div
                  key={i}
                  className={`flex items-center justify-between px-2 py-[5px] rounded text-[10px] ${
                    isPii
                      ? 'bg-red-950/50 border border-red-800/40'
                      : i % 2 === 0 ? 'bg-slate-900/70' : 'bg-slate-800/40'
                  }`}
                >
                  <span className={`font-mono font-medium ${isPii ? 'text-red-300' : 'text-slate-200'}`}>
                    {f.name}
                    {isPii && <span className="ml-1 text-red-400 text-[8px]">⚠</span>}
                  </span>
                  <span className="text-slate-500 font-mono text-[9px] ml-3 flex-shrink-0">
                    {f.type ?? ''}
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Pipeline suggestion — action strip */}
      {asset.pipeline_suggestion && (
        <div className="border-t border-slate-700/60 pt-2 mt-auto">
          <div className="flex items-center justify-between gap-2">
            <div className="flex-1 min-w-0">
              <div className="text-[9px] text-slate-500 uppercase tracking-wider mb-0.5">Pipeline</div>
              <div className={`text-sm font-bold ${pipelineColor}`}>→ {pipeline}</div>
            </div>
            <button onClick={e => { e.stopPropagation(); onSelect(); }} className="text-[10px] text-orange-400 hover:text-orange-300 font-semibold flex-shrink-0">
              Explore →
            </button>
          </div>
          <p className="text-[10px] text-slate-400 mt-1 leading-relaxed line-clamp-2">
            {asset.pipeline_suggestion.reasoning}
          </p>
        </div>
      )}

    </div>
  );
}
