import {
  MessageSquare,
  Database,
  HardDrive,
  FolderOpen,
  Pin,
} from 'lucide-react';
import type { DiscoveredAsset } from '../../types/agents';
import { TYPE_STYLES, getScoreColor, getScoreBarColor } from '../../constants/design';

const ICON_MAP = {
  kafka_topic: MessageSquare,
  iceberg_table: Database,
  ozone_volume: HardDrive,
  hdfs_path: FolderOpen,
} as const;

interface Props {
  asset: DiscoveredAsset;
  isSelected: boolean;
  isPinned: boolean;
  qualityScore?: number | null;
  onSelect: () => void;
  onPin: () => void;
  onUnpin: () => void;
}

export function AssetListItem({
  asset,
  isSelected,
  isPinned,
  qualityScore,
  onSelect,
  onPin,
  onUnpin,
}: Props) {
  const style = TYPE_STYLES[asset.asset_type] ?? TYPE_STYLES.hdfs_path;
  const Icon = ICON_MAP[asset.asset_type] ?? FolderOpen;
  const meta = asset.metadata ?? {};

  const namespace =
    (meta.sr_info as any)?.namespace ?? meta.namespace ?? null;
  const ageHours = meta.age_hours
    ? Math.round(meta.age_hours as number)
    : null;
  const hasPii = asset.pii_risk;

  return (
    <div
      onClick={onSelect}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          onSelect();
        }
      }}
      className={`
        w-full flex items-center gap-3 px-4 py-3 text-left transition-colors
        border-l-2 hover:bg-agent-dark-border cursor-pointer
        ${
          isSelected
            ? 'border-agent-orange bg-agent-orange/10'
            : 'border-transparent'
        }
      `}
    >
      {/* Type icon — colored dot */}
      <div
        className={`w-8 h-8 flex-shrink-0 rounded-md flex items-center justify-center ${style.bg} border ${style.border}`}
      >
        <Icon
          size={14}
          className={style.dot.replace('bg-', 'text-')}
        />
      </div>

      {/* Name + metadata — two rows */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-sm font-semibold text-agent-text-primary truncate">
            {asset.name}
          </span>
          {hasPii && (
            <span className="flex-shrink-0 text-xs px-1.5 py-0.5 bg-red-950/60 border border-red-800/50 text-red-300 rounded font-semibold">
              PII
            </span>
          )}
        </div>
        <div className="text-xs text-agent-text-secondary mt-0.5 flex items-center gap-2">
          <span>{style.label}</span>
          {namespace && <span>· {namespace}</span>}
          {ageHours !== null && <span>· {ageHours}h ago</span>}
        </div>
      </div>

      {/* Quality score badge — visible on row */}
      {qualityScore !== null && qualityScore !== undefined && (
        <div className="flex-shrink-0 flex flex-col items-center gap-0.5">
          <span
            className={`text-xs font-bold tabular-nums ${getScoreColor(
              qualityScore
            )}`}
          >
            {qualityScore}
          </span>
          <div className="w-8 h-1 bg-agent-dark-border rounded-full overflow-hidden">
            <div
              className={`h-full ${getScoreBarColor(qualityScore)} rounded-full`}
              style={{ width: `${qualityScore}%` }}
            />
          </div>
        </div>
      )}

      {/* Pin button — stop propagation */}
      <button
        onClick={e => {
          e.stopPropagation();
          isPinned ? onUnpin() : onPin();
        }}
        className={`flex-shrink-0 p-1.5 rounded transition-colors ${
          isPinned
            ? 'text-agent-orange hover:text-orange-600'
            : 'text-agent-text-secondary hover:text-agent-text-primary'
        }`}
        title={isPinned ? 'Unpin' : 'Pin to Workspace'}
      >
        <Pin size={13} fill={isPinned ? 'currentColor' : 'none'} />
      </button>
    </div>
  );
}
