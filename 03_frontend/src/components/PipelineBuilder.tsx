import { useEffect, useMemo, useState } from 'react';
import { Wrench, Download, Play, RefreshCw, AlertCircle, CheckCircle2, KeyRound, Eye } from 'lucide-react';

// ─── Types ───────────────────────────────────────────────────────────────────

type SourceType = 'kafka_topic' | 'iceberg_table';
type SinkType = 'adls_iceberg' | 'adls_delta' | 'snowflake';

interface Asset {
  asset_id?: string;
  name?: string;
  asset_type?: string;
  schema?: Array<{ name: string; type: string }>;
}

interface ParamInfo {
  name: string;
  sensitive: boolean;
  description: string;
}

interface BuildSummary {
  flow_name: string;
  processor_count: number;
  processors: string[];
  controller_service_count: number;
  controller_services: string[];
  connection_count: number;
  parameter_count: number;
  parameters_to_fill: ParamInfo[];
}

interface BuildResponse {
  summary: BuildSummary;
  flow: any;
}

// ─── Sink config ─────────────────────────────────────────────────────────────

const SINK_CARDS: Array<{
  id: SinkType;
  label: string;
  blurb: string;
  fields: Array<{ key: string; label: string; placeholder: string }>;
}> = [
  {
    id: 'adls_iceberg',
    label: 'ADLS → Iceberg',
    blurb: 'PutIceberg processor writing to a REST-catalog Iceberg table whose warehouse lives on Azure Data Lake Storage Gen2.',
    fields: [
      { key: 'namespace', label: 'Iceberg namespace', placeholder: 'gold' },
      { key: 'table',     label: 'Iceberg table',     placeholder: 'orders' },
    ],
  },
  {
    id: 'adls_delta',
    label: 'ADLS → Delta',
    blurb: 'ConvertRecord (Avro → Parquet) then PutAzureDataLakeStorage. A Databricks AUTO LOADER / Delta convert step lives downstream.',
    fields: [
      { key: 'container', label: 'ADLS filesystem (container)', placeholder: 'lakehouse' },
      { key: 'path',      label: 'Target path',                 placeholder: 'delta/orders' },
    ],
  },
  {
    id: 'snowflake',
    label: 'Snowflake',
    blurb: 'PutSnowflakeInternalStage + StartSnowflakeIngest (Snowpipe) to land records in a Snowflake table.',
    fields: [
      { key: 'database', label: 'Database', placeholder: 'RAW' },
      { key: 'schema',   label: 'Schema',   placeholder: 'PUBLIC' },
      { key: 'table',    label: 'Table',    placeholder: 'ORDERS' },
    ],
  },
];

// ─── Component ───────────────────────────────────────────────────────────────

export function PipelineBuilder() {
  // Source
  const [assets, setAssets] = useState<Asset[]>([]);
  const [loadingAssets, setLoadingAssets] = useState(false);
  const [sourceType, setSourceType] = useState<SourceType>('kafka_topic');
  const [sourceName, setSourceName] = useState('');
  const [pickedAssetId, setPickedAssetId] = useState<string>('');

  // Sink
  const [sinkType, setSinkType] = useState<SinkType>('adls_iceberg');
  const [sinkFields, setSinkFields] = useState<Record<string, string>>({});

  // Flow
  const [flowName, setFlowName] = useState('');
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<BuildResponse | null>(null);
  const [error, setError] = useState<string>('');
  const [showJson, setShowJson] = useState(false);

  // Load discovered assets on mount
  useEffect(() => {
    refreshAssets();
  }, []);

  const refreshAssets = async () => {
    setLoadingAssets(true);
    try {
      const res = await fetch('/api/agents/assets');
      const data = await res.json();
      setAssets((data.assets || []).filter((a: Asset) =>
        a.asset_type === 'kafka_topic' || a.asset_type === 'iceberg_table'
      ));
    } catch {
      setAssets([]);
    } finally {
      setLoadingAssets(false);
    }
  };

  const filteredAssets = useMemo(
    () => assets.filter(a => a.asset_type === sourceType),
    [assets, sourceType]
  );

  const pickAsset = (assetId: string) => {
    setPickedAssetId(assetId);
    const a = assets.find(x => x.asset_id === assetId || x.name === assetId);
    if (a?.name) setSourceName(a.name);
  };

  const pickedAsset = useMemo(
    () => assets.find(a => a.asset_id === pickedAssetId || a.name === pickedAssetId),
    [assets, pickedAssetId]
  );

  const activeSinkCard = SINK_CARDS.find(c => c.id === sinkType)!;

  const buildPayload = () => ({
    source: {
      type: sourceType,
      name: sourceName.trim(),
      schema: pickedAsset?.schema || null,
    },
    sink: {
      type: sinkType,
      ...sinkFields,
    },
    flow_name: flowName.trim() || null,
  });

  const handleBuild = async () => {
    if (!sourceName.trim()) {
      setError('Pick a discovered source or enter a name.');
      return;
    }
    setBusy(true);
    setError('');
    setResult(null);
    try {
      const res = await fetch('/api/agents/pipeline/builder/build', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildPayload()),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${res.status}`);
      }
      setResult(await res.json());
    } catch (e: any) {
      setError(e.message || String(e));
    } finally {
      setBusy(false);
    }
  };

  const handleDownload = async () => {
    if (!sourceName.trim()) {
      setError('Pick a discovered source or enter a name.');
      return;
    }
    setBusy(true);
    setError('');
    try {
      const res = await fetch('/api/agents/pipeline/builder/download', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildPayload()),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${res.status}`);
      }
      const blob = await res.blob();
      const cd = res.headers.get('content-disposition') || '';
      const match = cd.match(/filename="([^"]+)"/);
      const filename = match?.[1] || 'pipeline.flow.json';
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url; a.download = filename; a.click();
      URL.revokeObjectURL(url);
    } catch (e: any) {
      setError(e.message || String(e));
    } finally {
      setBusy(false);
    }
  };

  // ─── Render ───────────────────────────────────────────────────────────────

  return (
    <div className="h-full overflow-y-auto bg-[#0B1520] text-[#c8d8e8]">
      <div className="max-w-6xl mx-auto p-6 space-y-6">
        {/* Header */}
        <div className="flex items-start gap-4">
          <div className="w-12 h-12 rounded-lg bg-[#0088CC]/15 border border-[#0088CC]/40 flex items-center justify-center">
            <Wrench className="w-6 h-6 text-[#0088CC]" />
          </div>
          <div>
            <h1 className="text-xl font-semibold">Pipeline Builder</h1>
            <p className="text-sm text-[#6a8fa8] mt-1 max-w-2xl">
              Takes a Source-Scout-discovered <em>Kafka topic</em> or <em>Iceberg table in Ozone</em>, plus your chosen sink,
              and emits a downloadable NiFi 1.x flow-definition JSON. Import it via NiFi’s
              <span className="text-[#c8d8e8]"> Upload Flow Definition</span>, then fill in the Parameter Context.
            </p>
          </div>
        </div>

        {/* Step 1: Source */}
        <section className="bg-[#102030] border border-[#1e3a55] rounded-lg p-5">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-sm font-semibold tracking-wide uppercase text-[#6a8fa8]">1 · Source</h2>
            <button
              onClick={refreshAssets}
              className="flex items-center gap-1 text-xs text-[#6a8fa8] hover:text-[#c8d8e8]"
            >
              <RefreshCw className={`w-3 h-3 ${loadingAssets ? 'animate-spin' : ''}`} />
              Refresh from Source Scout
            </button>
          </div>

          <div className="flex gap-2 mb-4">
            {(['kafka_topic', 'iceberg_table'] as SourceType[]).map(t => (
              <button
                key={t}
                onClick={() => { setSourceType(t); setPickedAssetId(''); setSourceName(''); }}
                className={`px-3 py-1.5 rounded text-xs font-medium border transition ${
                  sourceType === t
                    ? 'bg-[#0088CC]/15 border-[#0088CC] text-[#c8d8e8]'
                    : 'bg-[#162840] border-[#1e3a55] text-[#6a8fa8] hover:text-[#c8d8e8]'
                }`}
              >
                {t === 'kafka_topic' ? '📨 Kafka topic' : '🧊 Iceberg table (Ozone)'}
              </button>
            ))}
          </div>

          {filteredAssets.length > 0 ? (
            <div className="mb-3">
              <label className="text-xs text-[#6a8fa8] block mb-1">Discovered by Source Scout</label>
              <select
                value={pickedAssetId}
                onChange={e => pickAsset(e.target.value)}
                className="w-full bg-[#162840] border border-[#1e3a55] rounded px-3 py-2 text-sm focus:outline-none focus:border-[#0088CC]"
              >
                <option value="">— pick an asset —</option>
                {filteredAssets.map(a => (
                  <option key={a.asset_id || a.name} value={a.asset_id || a.name}>
                    {a.name} {a.schema?.length ? `(${a.schema.length} fields)` : ''}
                  </option>
                ))}
              </select>
            </div>
          ) : (
            <p className="text-xs text-[#6a8fa8] mb-3">
              {loadingAssets ? 'Loading…' : 'No discovered assets — run Source Scout first, or type a name below.'}
            </p>
          )}

          <label className="text-xs text-[#6a8fa8] block mb-1">
            {sourceType === 'kafka_topic' ? 'Kafka topic name' : 'Iceberg table (namespace.table)'}
          </label>
          <input
            value={sourceName}
            onChange={e => setSourceName(e.target.value)}
            placeholder={sourceType === 'kafka_topic' ? 'orders' : 'bronze.events'}
            className="w-full bg-[#162840] border border-[#1e3a55] rounded px-3 py-2 text-sm focus:outline-none focus:border-[#0088CC]"
          />

          {pickedAsset?.schema && pickedAsset.schema.length > 0 && (
            <div className="mt-3 text-xs text-[#6a8fa8]">
              <span className="text-[#c8d8e8] font-medium">Schema:</span>{' '}
              {pickedAsset.schema.slice(0, 8).map(f => `${f.name}:${f.type}`).join(', ')}
              {pickedAsset.schema.length > 8 && ` … +${pickedAsset.schema.length - 8} more`}
            </div>
          )}
        </section>

        {/* Step 2: Sink */}
        <section className="bg-[#102030] border border-[#1e3a55] rounded-lg p-5">
          <h2 className="text-sm font-semibold tracking-wide uppercase text-[#6a8fa8] mb-4">2 · Sink</h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-4">
            {SINK_CARDS.map(card => (
              <button
                key={card.id}
                onClick={() => { setSinkType(card.id); setSinkFields({}); }}
                className={`text-left p-3 rounded-lg border transition ${
                  sinkType === card.id
                    ? 'bg-[#0088CC]/10 border-[#0088CC]'
                    : 'bg-[#162840] border-[#1e3a55] hover:border-[#3a5a78]'
                }`}
              >
                <div className="text-sm font-semibold mb-1">{card.label}</div>
                <div className="text-xs text-[#6a8fa8] leading-snug">{card.blurb}</div>
              </button>
            ))}
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            {activeSinkCard.fields.map(f => (
              <div key={f.key}>
                <label className="text-xs text-[#6a8fa8] block mb-1">{f.label}</label>
                <input
                  value={sinkFields[f.key] || ''}
                  onChange={e => setSinkFields({ ...sinkFields, [f.key]: e.target.value })}
                  placeholder={f.placeholder}
                  className="w-full bg-[#162840] border border-[#1e3a55] rounded px-3 py-2 text-sm focus:outline-none focus:border-[#0088CC]"
                />
              </div>
            ))}
            <div>
              <label className="text-xs text-[#6a8fa8] block mb-1">Flow name (optional)</label>
              <input
                value={flowName}
                onChange={e => setFlowName(e.target.value)}
                placeholder="orders-to-iceberg"
                className="w-full bg-[#162840] border border-[#1e3a55] rounded px-3 py-2 text-sm focus:outline-none focus:border-[#0088CC]"
              />
            </div>
          </div>
        </section>

        {/* Actions */}
        <div className="flex items-center gap-3">
          <button
            onClick={handleBuild}
            disabled={busy}
            className="flex items-center gap-2 px-4 py-2 rounded bg-[#0088CC] hover:bg-[#0099DD] disabled:opacity-50 text-white text-sm font-medium"
          >
            <Play className="w-4 h-4" />
            {busy ? 'Building…' : 'Build flow'}
          </button>
          <button
            onClick={handleDownload}
            disabled={busy}
            className="flex items-center gap-2 px-4 py-2 rounded bg-[#162840] border border-[#1e3a55] hover:border-[#3a5a78] disabled:opacity-50 text-sm"
          >
            <Download className="w-4 h-4" />
            Download .flow.json
          </button>
          {error && (
            <span className="flex items-center gap-1.5 text-sm text-red-400">
              <AlertCircle className="w-4 h-4" /> {error}
            </span>
          )}
        </div>

        {/* Result */}
        {result && (
          <section className="bg-[#102030] border border-[#1e3a55] rounded-lg p-5 space-y-4">
            <div className="flex items-center gap-2">
              <CheckCircle2 className="w-5 h-5 text-green-400" />
              <h2 className="text-sm font-semibold">
                Built <span className="text-[#0088CC]">{result.summary.flow_name}</span>
              </h2>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-xs">
              <Stat label="Processors"          value={result.summary.processor_count} />
              <Stat label="Controller services" value={result.summary.controller_service_count} />
              <Stat label="Parameters"          value={result.summary.parameter_count} />
            </div>

            <div>
              <div className="text-xs text-[#6a8fa8] mb-1.5">Processors (in order)</div>
              <div className="flex flex-wrap gap-1.5">
                {result.summary.processors.map((p, i) => (
                  <span key={i} className="px-2 py-1 rounded bg-[#162840] border border-[#1e3a55] text-xs">
                    {p}
                  </span>
                ))}
              </div>
            </div>

            <div>
              <div className="text-xs text-[#6a8fa8] mb-1.5">Parameters to fill in NiFi</div>
              <table className="w-full text-xs">
                <thead className="text-[#6a8fa8]">
                  <tr className="border-b border-[#1e3a55]">
                    <th className="text-left py-1.5 pr-2">Name</th>
                    <th className="text-left py-1.5 pr-2">Description</th>
                    <th className="text-left py-1.5">Sensitive</th>
                  </tr>
                </thead>
                <tbody>
                  {result.summary.parameters_to_fill.map(p => (
                    <tr key={p.name} className="border-b border-[#162840]">
                      <td className="py-1.5 pr-2 font-mono">{p.name}</td>
                      <td className="py-1.5 pr-2 text-[#6a8fa8]">{p.description}</td>
                      <td className="py-1.5">
                        {p.sensitive && (
                          <span className="inline-flex items-center gap-1 text-amber-400">
                            <KeyRound className="w-3 h-3" /> secret
                          </span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <details className="text-xs">
              <summary className="cursor-pointer text-[#6a8fa8] hover:text-[#c8d8e8] flex items-center gap-1.5">
                <Eye className="w-3.5 h-3.5" />
                {showJson ? 'Hide' : 'Show'} raw flow JSON
              </summary>
              <pre
                className="mt-2 p-3 bg-[#0B1520] border border-[#1e3a55] rounded overflow-x-auto max-h-96 font-mono text-[11px] leading-snug"
                onClick={() => setShowJson(!showJson)}
              >
                {JSON.stringify(result.flow, null, 2)}
              </pre>
            </details>

            <div className="text-xs text-[#6a8fa8] border-t border-[#1e3a55] pt-3">
              <span className="text-[#c8d8e8] font-medium">Next:</span> in NiFi, open a process group → right-click →
              <em> Upload Flow Definition</em> → select the downloaded file. Then bind the Parameter Context
              <span className="font-mono"> {result.summary.flow_name}-params</span> and fill in the values above.
            </div>
          </section>
        )}
      </div>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: number }) {
  return (
    <div className="bg-[#162840] border border-[#1e3a55] rounded p-3">
      <div className="text-[10px] uppercase tracking-wide text-[#6a8fa8]">{label}</div>
      <div className="text-lg font-semibold mt-0.5">{value}</div>
    </div>
  );
}
