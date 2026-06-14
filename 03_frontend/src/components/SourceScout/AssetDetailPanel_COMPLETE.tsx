import { useEffect, useRef, useState } from 'react';
import { X, RefreshCw, Code2, Play, Shield, ShieldCheck, Copy, ArrowLeft, FolderOpen, Database } from 'lucide-react';
import type { DiscoveredAsset, QualityCheckCode, QualityCheckResults } from '../../types/agents';
import { generateQualityCheck, fetchQualityCheckResults, streamExecuteQualityCheck } from '../../api/agents';
import { TYPE_STYLES, TYPE_ICONS, PII_NAMES, QC_STATUS_CONFIG, getScoreColor, getScoreBarColor, getScoreLabel } from '../../constants/design';

interface AssetDetailPanelProps {
  asset: DiscoveredAsset;
  isPinned: boolean;
  onPin: () => void;
  onUnpin: () => void;
  onClose?: () => void;
  allAssets?: DiscoveredAsset[];
  onSelectRelated?: (id: string) => void;
  onQcResult?: (assetId: string, results: QualityCheckResults) => void;
}

function getFields(asset: DiscoveredAsset): Array<{ name: string; type: string }> {
  const meta = asset.metadata ?? {};
  return (meta.schema as any)?.fields ?? (meta.fields ?? []) as Array<{ name: string; type: string }>;
}

function schemaMatches(asset1: DiscoveredAsset, asset2: DiscoveredAsset): boolean {
  const fields1 = getFields(asset1);
  const fields2 = getFields(asset2);

  if (fields1.length !== fields2.length) return false;

  return fields1.every((f1, i) => {
    const f2 = fields2[i];
    return f1.name === f2.name && f1.type === f2.type;
  });
}

function findRelatedTables(asset: DiscoveredAsset, allAssets?: DiscoveredAsset[]): DiscoveredAsset[] {
  if (!allAssets || asset.asset_type !== 'kafka_topic') {
    return [];
  }

  return allAssets.filter(
    a => a.asset_type === 'iceberg_table' && schemaMatches(asset, a)
  );
}

type QcState = 'idle' | 'scanning' | 'results' | 'sql';
type ActiveTab = 'overview' | 'quality' | 'access';
type QcError = { message: string; timestamp: number } | null;

/**
 * Safely render metric value with proper type coercion
 */
function formatMetricValue(metricValue: any): string {
  // Handle null/undefined
  if (metricValue == null) return '';

  // Safely coerce to number
  const numVal = Number(metricValue);

  // Check if it's a valid number
  if (!isNaN(numVal)) {
    return `${numVal.toFixed(1)}%`;
  }

  // Fallback for non-numeric values
  return String(metricValue);
}

export function AssetDetailPanel({
  asset,
  isPinned,
  onPin,
  onUnpin,
  onClose,
  allAssets,
  onSelectRelated,
  onQcResult,
}: AssetDetailPanelProps) {
  const [activeTab, setActiveTab] = useState<ActiveTab>('overview');
  const [qcState, setQcState] = useState<QcState>('idle');
  const [qcCode, setQcCode] = useState<QualityCheckCode | null>(null);
  const [qcResults, setQcResults] = useState<QualityCheckResults | null>(null);
  const [qcEngine, setQcEngine] = useState<'impala' | 'trino' | 'spark'>('impala');
  const [qcSteps, setQcSteps] = useState<Array<{ label: string; status: 'pending' | 'running' | 'complete' }>>([]);
  const [qcProgress, setQcProgress] = useState(0);
  const [loadingCode, setLoadingCode] = useState(false);
  const [qcFetchingInitial, setQcFetchingInitial] = useState(true);
  const [qcError, setQcError] = useState<QcError>(null);
  const qcAbortRef = useRef<(() => void) | null>(null);

  const style = TYPE_STYLES[asset.asset_type] ?? TYPE_STYLES.hdfs_path;
  const IconComponent = TYPE_ICONS[asset.asset_type] ?? FolderOpen;
  const meta = asset.metadata ?? {};
  const namespace = (meta.sr_info as any)?.namespace ?? meta.namespace ?? null;
  const fields = (
    (meta.schema as any)?.fields ??
    (meta.fields ?? [])
  ) as Array<{ name: string; type: string }>;
  const piiFields = fields.filter(f => PII_NAMES.has(f.name.toLowerCase()));

  /**
   * CRITICAL: Auto-fetch quality results on mount and automatically switch to Quality tab
   * This ensures DQ results display immediately without manual tab click
   */
  useEffect(() => {
    setQcState('idle');
    setQcCode(null);
    setQcResults(null);
    setQcProgress(0);
    setQcSteps([]);
    setQcFetchingInitial(true);
    setQcError(null);
    setActiveTab('overview');

    // Only fetch if asset has a name
    if (!asset.name) {
      setQcFetchingInitial(false);
      return;
    }

    const fetchResults = async () => {
      try {
        const results = await fetchQualityCheckResults(asset.name);

        if (results && !results.error && results.checks && results.checks.length > 0) {
          // SUCCESS: Results found - set state AND switch to quality tab immediately
          setQcResults(results);
          setQcState('results');
          setActiveTab('quality'); // ← KEY: Auto-switch to show results
          if (onQcResult) onQcResult(asset.id, results);
        } else if (results?.error) {
          // ERROR: API returned error
          setQcError({ message: results.error, timestamp: Date.now() });
        }
      } catch (err) {
        // NETWORK ERROR: Fetch failed
        const errorMsg = err instanceof Error ? err.message : 'Unknown error';
        setQcError({ message: `Failed to fetch: ${errorMsg}`, timestamp: Date.now() });
      } finally {
        setQcFetchingInitial(false);
      }
    };

    fetchResults();
  }, [asset.id, asset.name, onQcResult]);

  // Auto-generate SQL when entering SQL tab
  useEffect(() => {
    if (qcState === 'sql' && !qcCode && !loadingCode) {
      handleGenerateCode();
    }
  }, [qcState, qcCode, loadingCode]);

  const handleScanNow = () => {
    setQcState('scanning');
    setQcProgress(0);
    setQcError(null);

    const isKafka = asset.asset_type === 'kafka_topic';
    const engine = isKafka ? 'flink' : 'impala';

    setQcSteps(
      isKafka
        ? [
            { label: 'Generating Flink SQL', status: 'pending' },
            { label: 'Connecting to Kafka cluster', status: 'pending' },
            { label: 'Creating Flink job', status: 'pending' },
            { label: 'Running schema validation', status: 'pending' },
            { label: 'Running freshness check', status: 'pending' },
            { label: 'Fetching results', status: 'pending' },
          ]
        : [
            { label: 'Generating code', status: 'pending' },
            { label: 'Connecting to Impala', status: 'pending' },
            { label: 'Creating results table', status: 'pending' },
            { label: 'Running volume check', status: 'pending' },
            { label: 'Running completeness checks', status: 'pending' },
            { label: 'Fetching results', status: 'pending' },
          ]
    );

    let stepIndex = 0;

    const abort = streamExecuteQualityCheck(
      asset.name,
      engine,
      (event: any) => {
        if (event.type === 'qc_step') {
          const stepMap: Record<string, number> = {
            'generate_code': 0,
            'connecting': 1,
            'create_table': 2,
            'volume_check': 3,
            'completeness_checks': 4,
            'fetch_results': 5,
          };
          const idx = stepMap[event.step];
          if (idx !== undefined) {
            stepIndex = idx;
            setQcSteps(prev => {
              const next = [...prev];
              if (event.status === 'running') {
                next[idx] = { ...next[idx], status: 'running' };
              } else if (event.status === 'complete') {
                next[idx] = { ...next[idx], status: 'complete' };
              }
              return next;
            });
            setQcProgress(Math.round(((idx + 1) / 6) * 100));
          }
        } else if (event.type === 'qc_results') {
          setQcResults(event.data);
          if (onQcResult) onQcResult(asset.id, event.data);
          setQcState('results');
          setActiveTab('quality'); // ← Also switch tab after scan completes
          setQcError(null);
        } else if (event.type === 'qc_error') {
          setQcError({ message: event.message, timestamp: Date.now() });
          setQcState('idle');
        }
      },
      (err: Error) => {
        const msg = err.message || 'Unknown stream error';
        setQcError({ message: `Scan failed: ${msg}`, timestamp: Date.now() });
        setQcState('idle');
      }
    );
    qcAbortRef.current = abort;
  };

  const handleCancel = () => {
    qcAbortRef.current?.();
    setQcState('idle');
  };

  const handleGenerateCode = async () => {
    setLoadingCode(true);
    try {
      const code = await generateQualityCheck(
        asset.name,
        fields.length > 0 ? fields.map(f => ({ name: f.name, type: f.type })) : undefined
      );
      if (code && !code.error) {
        setQcCode(code);
      }
    } catch (err) {
      console.error('Code generation error:', err);
    } finally {
      setLoadingCode(false);
    }
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text).then(() => {
      alert('Copied to clipboard!');
    });
  };

  return (
    <div className="h-full flex flex-col bg-agent-dark-surface border-l border-agent-dark-border overflow-hidden">
      {/* Header */}
      <div className="px-5 py-4 border-b border-agent-dark-border flex-shrink-0">
        <div className="flex items-center gap-3 mb-3">
          <div className={`w-9 h-9 rounded-lg flex items-center justify-center ${style.bg} border ${style.border}`}>
            <IconComponent size={18} className={style.dot.replace('bg-', 'text-')} />
          </div>
          <div className="flex-1 min-w-0">
            <div className="text-sm font-bold text-agent-text-primary truncate">{asset.name}</div>
            <div className="text-xs text-agent-text-secondary mt-0.5">
              {style.label} {namespace && `· ${namespace}`}
            </div>
          </div>
          {onClose && (
            <button
              onClick={onClose}
              className="text-agent-text-secondary hover:text-agent-text-primary p-1 rounded transition-colors"
            >
              <X size={16} />
            </button>
          )}
        </div>
        <button
          onClick={isPinned ? onUnpin : onPin}
          className={`w-full py-2 rounded-lg text-sm font-semibold transition-colors flex items-center justify-center gap-2 ${
            isPinned
              ? 'bg-agent-dark-border text-agent-text-secondary hover:bg-agent-dark-border/60 hover:text-agent-text-primary'
              : 'bg-agent-orange text-agent-text-primary hover:bg-orange-600'
          }`}
        >
          {isPinned ? '📌 Unpin from Workspace' : '📌 Pin to Workspace'}
        </button>
      </div>

      {/* QUALITY RESULTS BANNER - Always visible on top when results exist */}
      {qcResults && qcResults.checks.length > 0 && (
        <div className="px-5 py-4 bg-gradient-to-r from-agent-dark-border to-agent-dark-border/60 border-b border-agent-dark-border">
          <div className="flex items-center justify-between gap-4">
            <div className="flex-1">
              <div className="text-xs text-agent-text-secondary font-semibold mb-1">Overall Quality Score</div>
              <div className="flex items-baseline gap-2">
                <div className={`text-3xl font-black tabular-nums ${getScoreColor(qcResults.overall_score ?? 0)}`}>
                  {qcResults.overall_score}
                </div>
                <span className="text-sm text-agent-text-secondary">/100</span>
              </div>
              {qcResults.last_run && (
                <div className="text-xs text-agent-text-secondary mt-1">
                  Last run: {new Date(qcResults.last_run).toLocaleDateString()}
                </div>
              )}
            </div>
            <button
              onClick={() => setActiveTab('quality')}
              className="px-4 py-2 text-xs font-semibold rounded-lg bg-agent-orange text-agent-text-primary hover:bg-orange-600 transition-colors whitespace-nowrap"
            >
              View All Checks
            </button>
          </div>
          {/* Score progress bar */}
          <div className="w-full h-2 bg-agent-dark-border rounded-full overflow-hidden mt-3">
            <div
              className={`h-full ${getScoreBarColor(qcResults.overall_score ?? 0)} transition-all`}
              style={{ width: `${qcResults.overall_score ?? 0}%` }}
            />
          </div>
        </div>
      )}

      {/* Tab bar */}
      <div className="flex border-b border-agent-dark-border flex-shrink-0">
        {[
          { id: 'overview', label: 'Overview' },
          { id: 'quality', label: 'Quality', badge: qcResults?.overall_score },
          { id: 'access', label: 'Access', badge: piiFields.length > 0 ? '!' : null },
        ].map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id as ActiveTab)}
            className={`flex-1 py-3 text-xs font-semibold transition-colors relative ${
              activeTab === tab.id
                ? 'text-agent-orange border-b-2 border-agent-orange -mb-px'
                : 'text-agent-text-secondary hover:text-agent-text-primary'
            }`}
          >
            {tab.label}
            {tab.badge !== undefined && tab.badge !== null && (
              <span className={`ml-1.5 text-[10px] px-1 py-0.5 rounded font-bold ${
                typeof tab.badge === 'number'
                  ? `${getScoreColor(tab.badge as number)} bg-agent-dark-border`
                  : 'text-amber-400 bg-agent-dark-border'
              }`}>
                {tab.badge}
              </span>
            )}
          </button>
        ))}
      </div>

      {/* Tab content — scrollable */}
      <div className="flex-1 overflow-y-auto">
        {activeTab === 'overview' && (
          <div className="p-5 space-y-5">
            {asset.pipeline_suggestion && (
              <div>
                <div className="text-xs font-semibold text-agent-text-secondary uppercase tracking-wider mb-2">
                  Pipeline Recommendation
                </div>
                <div className="text-sm font-bold text-agent-orange mb-2">
                  {asset.pipeline_suggestion.recommended_pipeline}
                </div>
                <p className="text-xs text-agent-text-secondary mb-3">{asset.pipeline_suggestion.reasoning}</p>
                {asset.pipeline_suggestion.connector_config && (
                  <div className="bg-agent-dark-bg border border-agent-dark-border rounded-lg p-3 text-xs font-mono text-agent-text-secondary max-h-36 overflow-auto">
                    {typeof asset.pipeline_suggestion.connector_config === 'string'
                      ? asset.pipeline_suggestion.connector_config
                      : JSON.stringify(asset.pipeline_suggestion.connector_config, null, 2)}
                  </div>
                )}
              </div>
            )}

            <div>
              <div className="text-xs font-semibold text-agent-text-secondary uppercase tracking-wider mb-2">
                Schema ({fields.length} columns)
              </div>
              <div className="space-y-1">
                {fields.slice(0, 10).map((field, i) => {
                  const isPii = PII_NAMES.has(field.name.toLowerCase());
                  return (
                    <div
                      key={i}
                      className={`flex items-center justify-between px-3 py-2 rounded-lg text-xs ${
                        isPii
                          ? 'bg-red-950/30 border border-red-800/30'
                          : i % 2 === 0
                          ? 'bg-agent-dark-border'
                          : 'bg-agent-dark-border/60'
                      }`}
                    >
                      <div>
                        <span className={`font-mono font-semibold ${isPii ? 'text-red-300' : 'text-agent-text-primary'}`}>
                          {field.name}
                        </span>
                        {isPii && <span className="ml-2 text-[10px] text-red-400">PII</span>}
                      </div>
                      <span className="text-agent-text-secondary">{field.type}</span>
                    </div>
                  );
                })}
                {fields.length > 10 && (
                  <div className="text-xs text-agent-text-secondary px-3 py-2">+{fields.length - 10} more columns</div>
                )}
              </div>
            </div>

            {asset.asset_type === 'kafka_topic' && (
              (() => {
                const relatedTables = findRelatedTables(asset, allAssets);
                return relatedTables.length > 0 ? (
                  <div>
                    <div className="text-xs font-semibold text-agent-text-secondary uppercase tracking-wider mb-2">
                      Related Iceberg Tables ({relatedTables.length})
                    </div>
                    <div className="space-y-2">
                      {relatedTables.map(table => (
                        <button
                          key={table.id}
                          onClick={() => onSelectRelated?.(table.id)}
                          className="w-full flex items-center gap-2.5 px-3 py-2.5 rounded-lg bg-agent-dark-border border border-agent-dark-border hover:border-agent-orange hover:bg-agent-dark-border/80 transition-colors text-left"
                        >
                          <Database size={13} className="text-blue-400 flex-shrink-0" />
                          <div className="flex-1 min-w-0">
                            <div className="text-xs font-semibold text-agent-text-primary truncate">
                              {table.name}
                            </div>
                            <div className="text-xs text-agent-text-secondary">
                              {getFields(table).length} columns (matching schema)
                            </div>
                          </div>
                          <span className="text-xs text-agent-text-secondary">→</span>
                        </button>
                      ))}
                    </div>
                  </div>
                ) : null;
              })()
            )}
          </div>
        )}

        {/* QUALITY TAB - Main display for DQ results */}
        {activeTab === 'quality' && (
          <div className="p-5 space-y-5">
            {qcFetchingInitial && (
              <div className="flex flex-col gap-4 py-8 text-center">
                <div className="mx-auto w-12 h-12 rounded-lg bg-agent-dark-border flex items-center justify-center">
                  <div className="w-5 h-5 border-2 border-agent-orange border-t-transparent rounded-full animate-spin" />
                </div>
                <p className="text-sm text-agent-text-secondary">Checking for previous scan results...</p>
              </div>
            )}

            {!qcFetchingInitial && qcState === 'idle' && (
              <div>
                {qcError && (
                  <div className="mb-4 p-3 bg-red-950/30 border border-red-800/40 rounded-lg">
                    <p className="text-xs text-red-200">
                      <span className="font-semibold">Error:</span> {qcError.message}
                    </p>
                  </div>
                )}

                <div className="flex flex-col gap-4 py-6 text-center">
                  <div className="text-agent-text-secondary opacity-60">
                    <div className="mx-auto w-12 h-12 rounded-lg bg-agent-dark-border flex items-center justify-center mb-3">
                      📊
                    </div>
                    <p className="text-sm text-agent-text-secondary">
                      {qcError ? 'Last scan failed. Try again.' : 'No quality scan has been run yet'}
                    </p>
                    <p className="text-xs text-agent-text-secondary mt-1">
                      Run a scan to see completeness, nulls, and freshness checks
                    </p>
                  </div>
                </div>
                <div className="space-y-2">
                  <button
                    onClick={handleScanNow}
                    disabled={asset.asset_type !== 'iceberg_table' && asset.asset_type !== 'kafka_topic'}
                    className={`w-full py-3 text-agent-text-primary text-sm font-semibold rounded-lg transition-colors flex items-center justify-center gap-2 ${
                      asset.asset_type !== 'iceberg_table' && asset.asset_type !== 'kafka_topic'
                        ? 'bg-agent-dark-border text-agent-text-secondary cursor-not-allowed'
                        : 'bg-agent-orange hover:bg-orange-700'
                    }`}
                  >
                    <Play size={15} /> Scan Now
                  </button>
                  <button
                    onClick={() => setQcState('sql')}
                    className="w-full py-2 border border-agent-dark-border text-agent-text-secondary hover:text-agent-text-primary text-sm font-semibold rounded-lg transition-colors flex items-center justify-center gap-2"
                  >
                    <Code2 size={13} /> View SQL First
                  </button>
                </div>
              </div>
            )}

            {qcState === 'scanning' && (
              <div className="space-y-4 py-4">
                {qcError && (
                  <div className="p-3 bg-red-950/30 border border-red-800/40 rounded-lg mb-2">
                    <p className="text-xs text-red-200">
                      <span className="font-semibold">Error:</span> {qcError.message}
                    </p>
                  </div>
                )}
                <div>
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-xs font-semibold text-[#c8d8e8]">Scanning {asset.name}...</span>
                    <span className="text-xs text-agent-text-secondary">{qcProgress}%</span>
                  </div>
                  <div className="w-full h-1.5 bg-agent-dark-border rounded-full overflow-hidden">
                    <div
                      className="h-full bg-agent-orange transition-all duration-300 rounded-full"
                      style={{ width: `${qcProgress}%` }}
                    />
                  </div>
                </div>
                <div className="space-y-2">
                  {qcSteps.map((step, i) => (
                    <div key={i} className="flex items-center gap-3 text-xs">
                      <span
                        className={`w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 text-xs font-bold ${
                          step.status === 'complete'
                            ? 'bg-green-500/20 text-green-400'
                            : step.status === 'running'
                            ? 'bg-agent-orange/20 text-agent-orange animate-pulse'
                            : 'bg-agent-dark-border text-agent-text-secondary'
                        }`}
                      >
                        {step.status === 'complete' ? '✓' : step.status === 'running' ? '…' : '·'}
                      </span>
                      <span
                        className={step.status === 'running' ? 'text-agent-text-primary' : 'text-agent-text-secondary'}
                      >
                        {step.label}
                      </span>
                    </div>
                  ))}
                </div>
                <button
                  onClick={handleCancel}
                  className="w-full py-2 border border-agent-dark-border text-agent-text-secondary hover:text-red-400 hover:border-red-800 text-sm rounded-lg transition-colors"
                >
                  Cancel
                </button>
              </div>
            )}

            {/* RESULTS DISPLAY - The main DQ display */}
            {qcState === 'results' && qcResults && (
              <div className="space-y-5">
                {/* Score card */}
                <div className="bg-agent-dark-border border border-agent-dark-border rounded-xl p-4">
                  <div className="flex items-end justify-between mb-3">
                    <div>
                      <div className="text-xs text-agent-text-secondary mb-1">Quality Score</div>
                      <div className={`text-3xl font-black tabular-nums ${getScoreColor(qcResults.overall_score ?? 0)}`}>
                        {qcResults.overall_score}
                        <span className="text-base text-agent-text-secondary font-normal">/100</span>
                      </div>
                      <div className={`text-xs font-semibold mt-1 ${getScoreColor(qcResults.overall_score ?? 0)}`}>
                        {getScoreLabel(qcResults.overall_score ?? 0)}
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="text-xs text-agent-text-secondary">Last run</div>
                      <div className="text-xs text-agent-text-primary">
                        {qcResults.last_run
                          ? new Date(qcResults.last_run).toLocaleString()
                          : 'Unknown'}
                      </div>
                    </div>
                  </div>
                  {/* Score bar */}
                  <div className="w-full h-2.5 bg-agent-dark-border rounded-full overflow-hidden">
                    <div
                      className={`h-full ${getScoreBarColor(qcResults.overall_score ?? 0)} rounded-full transition-all`}
                      style={{ width: `${qcResults.overall_score ?? 0}%` }}
                    />
                  </div>
                </div>

                {/* Check results list */}
                <div>
                  <div className="text-xs font-semibold text-agent-text-secondary uppercase tracking-wider mb-3">
                    Quality Checks ({qcResults.checks.length})
                  </div>
                  <div className="space-y-1">
                    {qcResults.checks.map((check, i) => {
                      const cfg = QC_STATUS_CONFIG[check.status];
                      const metricDisplay = formatMetricValue(check.metric_value);

                      return (
                        <div
                          key={i}
                          className={`flex items-start gap-3 px-3 py-2.5 rounded-lg border ${cfg.bg} ${cfg.border}`}
                        >
                          <span className={`w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 text-xs font-bold ${cfg.color} bg-current/10`}>
                            {cfg.icon}
                          </span>
                          <div className="flex-1 min-w-0">
                            <div className="text-xs font-semibold text-agent-text-primary">
                              {check.column_name || check.check_name}
                            </div>
                            <div className="text-xs text-agent-text-secondary mt-0.5">{check.metric_label}</div>
                          </div>
                          <span className={`text-xs font-bold tabular-nums flex-shrink-0 ${cfg.color}`}>
                            {metricDisplay || check.status}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                </div>

                {/* Actions */}
                <div className="flex gap-2">
                  <button
                    onClick={handleScanNow}
                    disabled={asset.asset_type !== 'iceberg_table' && asset.asset_type !== 'kafka_topic'}
                    className={`flex-1 py-2 border text-sm rounded-lg transition-colors flex items-center justify-center gap-1.5 ${
                      asset.asset_type !== 'iceberg_table' && asset.asset_type !== 'kafka_topic'
                        ? 'border-agent-dark-border text-agent-text-secondary cursor-not-allowed'
                        : 'border-agent-dark-border text-agent-text-secondary hover:text-agent-text-primary hover:border-agent-orange'
                    }`}
                  >
                    <RefreshCw size={13} /> Scan Again
                  </button>
                  <button
                    onClick={() => setQcState('sql')}
                    className="flex-1 py-2 border border-agent-dark-border text-agent-text-secondary hover:text-agent-text-primary text-sm rounded-lg transition-colors flex items-center justify-center gap-1.5"
                  >
                    <Code2 size={13} /> View SQL
                  </button>
                </div>
              </div>
            )}

            {qcState === 'sql' && (
              <div className="space-y-4 py-4">
                <div className="flex items-center justify-between">
                  <div className="text-xs font-semibold text-agent-text-secondary uppercase tracking-wider">
                    {asset.asset_type === 'kafka_topic' ? 'Flink SQL' : 'Quality Check SQL'}
                  </div>
                  <button
                    onClick={() => setQcState(qcResults ? 'results' : 'idle')}
                    className="text-xs text-agent-orange hover:text-agent-orange-hover flex items-center gap-1 transition-colors"
                  >
                    <ArrowLeft size={12} /> Back
                  </button>
                </div>

                {asset.asset_type !== 'kafka_topic' && (
                  <div className="flex gap-1.5">
                    {(['impala', 'trino', 'spark'] as const).map(eng => (
                      <button
                        key={eng}
                        onClick={() => setQcEngine(eng)}
                        className={`flex-1 py-2 text-xs font-semibold rounded-lg transition-colors ${
                          qcEngine === eng
                            ? 'bg-agent-orange text-agent-text-primary'
                            : 'bg-agent-dark-border text-agent-text-secondary border border-agent-dark-border hover:border-agent-orange/60 hover:text-agent-text-primary'
                        }`}
                      >
                        {eng === 'spark' ? 'CDE Spark' : eng.charAt(0).toUpperCase() + eng.slice(1)}
                      </button>
                    ))}
                  </div>
                )}
                {asset.asset_type === 'kafka_topic' && (
                  <div className="flex gap-1.5">
                    <div className="flex-1 py-2 px-3 text-xs font-semibold rounded-lg bg-agent-orange text-agent-text-primary flex items-center justify-center">
                      Flink SQL
                    </div>
                  </div>
                )}

                {loadingCode ? (
                  <div className="h-32 flex items-center justify-center text-xs text-agent-text-secondary">
                    Generating...
                  </div>
                ) : qcCode ? (
                  <div className="relative">
                    <div className="bg-agent-dark-bg border border-agent-dark-border rounded-lg p-4 max-h-56 overflow-auto">
                      <pre className="text-xs font-mono text-[#9ab8cc] whitespace-pre-wrap leading-relaxed">
                        {asset.asset_type === 'kafka_topic'
                          ? (qcCode as any).flink_sql
                          : qcEngine === 'impala'
                          ? qcCode.impala_sql
                          : qcEngine === 'trino'
                          ? qcCode.trino_sql
                          : qcCode.spark_script}
                      </pre>
                    </div>
                    <button
                      onClick={() => {
                        const code = asset.asset_type === 'kafka_topic'
                          ? (qcCode as any).flink_sql
                          : qcEngine === 'impala'
                          ? qcCode.impala_sql
                          : qcEngine === 'trino'
                          ? qcCode.trino_sql
                          : qcCode.spark_script;
                        copyToClipboard(code);
                      }}
                      className="absolute top-3 right-3 p-1.5 bg-agent-dark-border border border-agent-dark-border rounded text-agent-text-secondary hover:text-agent-text-primary transition-colors"
                      title="Copy to clipboard"
                    >
                      <Copy size={12} />
                    </button>
                  </div>
                ) : (
                  <button
                    onClick={handleGenerateCode}
                    className="w-full py-3 bg-agent-orange text-agent-text-primary text-sm font-semibold rounded-lg hover:bg-orange-700 transition-colors"
                  >
                    Generate SQL
                  </button>
                )}

                {qcCode && (
                  <>
                    <p className="text-xs text-agent-text-secondary">
                      Results stored in:{' '}
                      <code className="text-agent-text-secondary bg-agent-dark-border px-1.5 py-0.5 rounded">
                        {qcCode.results_table}
                      </code>
                    </p>
                  </>
                )}
              </div>
            )}
          </div>
        )}

        {activeTab === 'access' && (
          <div className="p-5">
            {piiFields.length > 0 ? (
              <div className="space-y-4">
                <div>
                  <div className="text-xs font-semibold text-agent-text-secondary uppercase tracking-wider mb-3">
                    PII Fields Detected ({piiFields.length})
                  </div>
                  <div className="space-y-2">
                    {piiFields.map((f, i) => (
                      <div key={i} className="flex items-center justify-between px-3 py-2.5 bg-red-950/30 border border-red-800/30 rounded-lg">
                        <div className="flex items-center gap-2">
                          <Shield size={13} className="text-red-400 flex-shrink-0" />
                          <span className="text-sm font-mono font-semibold text-red-200">{f.name}</span>
                        </div>
                        <span className="text-xs text-red-400/80 bg-red-950/50 px-2 py-0.5 rounded">
                          SHA-256
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
                <div className="text-xs text-agent-text-secondary bg-agent-dark-border border border-agent-dark-border rounded-lg p-3 leading-relaxed">
                  These fields require masking before cross-environment transfer. Review your Ranger policies before
                  pipeline execution.
                </div>
              </div>
            ) : (
              <div className="flex flex-col items-center justify-center py-10 text-center">
                <ShieldCheck size={28} className="mb-3 text-green-500/60" />
                <p className="text-sm text-agent-text-secondary">No PII fields detected</p>
                <p className="text-xs text-agent-text-secondary mt-1">
                  Field names were checked against common PII patterns
                </p>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
