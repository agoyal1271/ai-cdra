import type { DiscoveredAsset } from '../../types/agents';
import { TYPE_STYLES, PII_NAMES } from '../../constants/design';

export function AssetPreviewPane({ asset }: { asset: DiscoveredAsset }) {
  const style = TYPE_STYLES[asset.asset_type] ?? { icon: '📄', label: 'Asset' };
  const meta = asset.metadata ?? {};
  const pipeline = asset.pipeline_suggestion?.recommended_pipeline ?? '—';

  const fields: Array<{ name: string; type?: string }> =
    (meta.fields as Array<{ name: string; type?: string }>) ??
    (meta.schema as { fields?: Array<{ name: string; type?: string }> })?.fields ??
    [];

  const piiFields = fields.filter(f => PII_NAMES.has(f.name.toLowerCase()));
  const rowCount = meta.row_count ? Math.round(meta.row_count as number).toLocaleString() : null;
  const msgCount = meta.estimated_messages ? Math.round(meta.estimated_messages as number).toLocaleString() : null;

  // Estimate freshness
  const freshness = meta.freshness_estimate || 'unknown';
  const age = meta.age_hours ? Math.round(meta.age_hours as number) : null;

  return (
    <div className="h-full flex flex-col p-4">
      {/* Asset header */}
      <div className="mb-6">
        <div className="flex items-center gap-2 mb-3">
          <span className="text-3xl">{style.icon}</span>
          <div>
            <div className="text-sm font-bold text-white">{asset.name}</div>
            <div className="text-[11px] text-slate-400">{style.label}</div>
          </div>
        </div>
      </div>

      {/* Metadata grid */}
      <div className="space-y-4 text-[11px]">
        {/* Pipeline */}
        <div>
          <div className="text-slate-500 uppercase text-[9px] font-semibold tracking-wider mb-1">Pipeline</div>
          <div className="text-orange-400 font-semibold">→ {pipeline}</div>
        </div>

        {/* Size info */}
        {(rowCount || msgCount) && (
          <div>
            <div className="text-slate-500 uppercase text-[9px] font-semibold tracking-wider mb-1">Volume</div>
            <div className="text-slate-300">
              {rowCount && <div>{rowCount} rows</div>}
              {msgCount && <div>~{msgCount} messages</div>}
            </div>
          </div>
        )}

        {/* Schema */}
        <div>
          <div className="text-slate-500 uppercase text-[9px] font-semibold tracking-wider mb-1">Schema</div>
          <div className="text-slate-300">{fields.length} fields</div>
        </div>

        {/* Freshness */}
        <div>
          <div className="text-slate-500 uppercase text-[9px] font-semibold tracking-wider mb-1">Freshness</div>
          <div className="text-slate-300">
            {age ? `${age}h ago` : freshness}
          </div>
        </div>

        {/* PII Risk */}
        {piiFields.length > 0 && (
          <div>
            <div className="text-slate-500 uppercase text-[9px] font-semibold tracking-wider mb-1">PII Fields</div>
            <div className="flex flex-wrap gap-1">
              {piiFields.slice(0, 4).map(f => (
                <span key={f.name} className="px-1.5 py-0.5 bg-red-950/50 border border-red-800/40 rounded text-red-300">
                  {f.name}
                </span>
              ))}
              {piiFields.length > 4 && (
                <span className="px-1.5 py-0.5 text-slate-400 text-[10px]">
                  +{piiFields.length - 4} more
                </span>
              )}
            </div>
          </div>
        )}

        {/* Matching info */}
        {asset.pipeline_suggestion?.confidence_score !== undefined && asset.pipeline_suggestion.confidence_score > 0 && (
          <div>
            <div className="text-slate-500 uppercase text-[9px] font-semibold tracking-wider mb-1">Confidence</div>
            <div className="text-emerald-400 font-semibold">
              {Math.round(asset.pipeline_suggestion.confidence_score)}%
            </div>
          </div>
        )}
      </div>

      {/* Spacer */}
      <div className="flex-1" />

      {/* Footer hint */}
      <div className="border-t border-slate-700 pt-3 text-[10px] text-slate-500">
        👉 View full details in the right pane
      </div>
    </div>
  );
}
