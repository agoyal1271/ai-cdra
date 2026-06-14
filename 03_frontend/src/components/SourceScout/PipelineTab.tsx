import React, { useState, useEffect } from 'react';
import { AlertCircle, CheckCircle, Copy, Loader2, AlertTriangle, ChevronDown, ExternalLink } from 'lucide-react';
import { DiscoveredAsset, PipelineResult, SSEEvent } from '../../types/agents';

interface PipelineTabProps {
  asset: DiscoveredAsset;
}

type DeploymentMethod = 'nifi' | 'flink' | 'kafka_connect';

export const PipelineTab: React.FC<PipelineTabProps> = ({ asset }) => {
  const [deployMethod, setDeployMethod] = useState<DeploymentMethod>('nifi');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<PipelineResult | null>(null);
  const [thoughts, setThoughts] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [copiedSection, setCopiedSection] = useState<string | null>(null);
  const [expandedSections, setExpandedSections] = useState<Record<string, boolean>>({
    schema: false,
    ddl: false,
    lag: true,
  });

  useEffect(() => {
    if (!result && !loading) {
      generatePipeline();
    }
  }, []);

  const generatePipeline = async () => {
    try {
      setLoading(true);
      setError(null);
      setThoughts([]);
      setResult(null);

      const response = await fetch('/api/agents/pipeline/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ topic: asset.name, target: 'iceberg' }),
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const reader = response.body?.getReader();
      if (!reader) throw new Error('No response body');

      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (line.startsWith('event:')) {
            const eventType = line.substring(6).trim();
            const dataLine = lines.shift();
            if (dataLine?.startsWith('data:')) {
              const dataStr = dataLine.substring(5).trim();
              try {
                const event = JSON.parse(dataStr) as SSEEvent;

                if (event.type === 'thought') {
                  setThoughts(prev => [...prev, event.content || '']);
                } else if (event.type === 'warning') {
                  setThoughts(prev => [...prev, `⚠️  ${event.content}`]);
                } else if (event.type === 'error') {
                  setError(event.content || 'Unknown error');
                }
              } catch {
                // Ignore parse errors
              }
            }
          }
        }
      }
    } catch (err) {
      setError(`Failed to generate pipeline: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setLoading(false);
    }
  };

  const copyToClipboard = (text: string, sectionId: string) => {
    navigator.clipboard.writeText(text);
    setCopiedSection(sectionId);
    setTimeout(() => setCopiedSection(null), 2000);
  };

  const toggleSection = (section: string) => {
    setExpandedSections(prev => ({
      ...prev,
      [section]: !prev[section],
    }));
  };

  const CopyButton = ({ text, sectionId }: { text: string; sectionId: string }) => (
    <button
      onClick={() => copyToClipboard(text, sectionId)}
      className="p-2 bg-agent-dark-bg hover:bg-agent-dark-border rounded-md transition z-10"
      title="Copy to clipboard"
    >
      {copiedSection === sectionId ? (
        <CheckCircle className="w-4 h-4 text-green-500" />
      ) : (
        <Copy className="w-4 h-4 text-agent-text-secondary hover:text-agent-text-primary" />
      )}
    </button>
  );

  const CodeBlock = ({ content, language = 'json' }: { content: string; language?: string }) => (
    <pre className="bg-agent-dark-bg p-3 rounded-md overflow-x-auto text-xs font-mono text-agent-text-secondary max-h-96">
      <code>{content}</code>
    </pre>
  );

  const CollapsibleSection = ({
    title,
    sectionId,
    children,
  }: {
    title: string;
    sectionId: string;
    children: React.ReactNode;
  }) => (
    <div className="border border-agent-dark-border rounded-lg overflow-hidden">
      <button
        onClick={() => toggleSection(sectionId)}
        className="w-full flex items-center justify-between p-4 bg-agent-dark-bg hover:bg-agent-dark-border transition"
      >
        <h3 className="font-semibold text-agent-text-primary">{title}</h3>
        <ChevronDown
          className={`w-5 h-5 text-agent-text-secondary transition transform ${
            expandedSections[sectionId] ? 'rotate-180' : ''
          }`}
        />
      </button>
      {expandedSections[sectionId] && (
        <div className="p-4 bg-agent-dark-surface border-t border-agent-dark-border">{children}</div>
      )}
    </div>
  );

  if (loading) {
    return (
      <div className="space-y-4 p-6 bg-agent-dark-surface">
        {thoughts.map((thought, idx) => (
          <div key={idx} className="flex gap-3 p-3 bg-agent-dark-bg rounded-lg border border-agent-dark-border">
            <div className="text-agent-text-secondary text-sm">{thought}</div>
          </div>
        ))}
        {thoughts.length > 0 && (
          <div className="flex items-center gap-2 p-3 text-agent-text-secondary">
            <Loader2 className="w-4 h-4 animate-spin" />
            <span className="text-sm">Generating pipeline config...</span>
          </div>
        )}
        {thoughts.length === 0 && (
          <div className="flex items-center justify-center p-8">
            <Loader2 className="w-6 h-6 animate-spin text-cloudera" />
            <span className="ml-3 text-agent-text-secondary">Starting pipeline generation...</span>
          </div>
        )}
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-6 bg-agent-dark-surface">
        <div className="flex items-start gap-3 p-4 bg-red-900/20 border border-red-700 rounded-lg">
          <AlertCircle className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" />
          <div>
            <p className="text-red-300 font-medium">Pipeline Generation Failed</p>
            <p className="text-red-200 text-sm mt-1">{error}</p>
          </div>
        </div>
        <button
          onClick={generatePipeline}
          className="mt-4 px-4 py-2 bg-cloudera text-white rounded-md hover:bg-cloudera-hover transition"
        >
          Retry
        </button>
      </div>
    );
  }

  if (!result) {
    return (
      <div className="p-6 bg-agent-dark-surface text-agent-text-secondary">
        No pipeline generated yet.
      </div>
    );
  }

  const piiFields = result.pii_fields || [];

  return (
    <div className="space-y-6 p-6 bg-agent-dark-surface">
      {/* PII Warning - Always on Top */}
      {piiFields.length > 0 && (
        <div className="flex items-start gap-3 p-4 bg-amber-900/20 border border-amber-700 rounded-lg">
          <AlertTriangle className="w-5 h-5 text-amber-500 flex-shrink-0 mt-0.5" />
          <div>
            <p className="text-amber-300 font-medium">PII Fields Detected</p>
            <p className="text-amber-200 text-sm mt-1">
              {piiFields.map(f => f.kafka_field).join(', ')} contain sensitive data. Consider masking before ingest.
            </p>
          </div>
        </div>
      )}

      {/* Deployment Method Selector */}
      <div>
        <label className="block text-sm font-semibold text-agent-text-primary mb-3">Deployment Method</label>
        <div className="flex gap-2">
          {([
            { id: 'nifi', label: '🌊 NiFi (Recommended)', desc: 'Cloudera official' },
            { id: 'flink', label: '⚡ Flink SQL', desc: 'Transformations' },
            { id: 'kafka_connect', label: '🔗 Kafka Connect', desc: 'Advanced' },
          ] as const).map(method => (
            <button
              key={method.id}
              onClick={() => setDeployMethod(method.id)}
              className={`px-4 py-3 rounded-lg font-medium transition text-sm ${
                deployMethod === method.id
                  ? 'bg-cloudera text-white'
                  : 'bg-agent-dark-bg text-agent-text-secondary border border-agent-dark-border hover:border-cloudera'
              }`}
            >
              <div>{method.label}</div>
              <div className={`text-xs mt-0.5 ${deployMethod === method.id ? 'text-white/80' : 'text-agent-text-secondary'}`}>
                {method.desc}
              </div>
            </button>
          ))}
        </div>
      </div>

      {/* NiFi Method Content */}
      {deployMethod === 'nifi' && (
        <div className="space-y-4 border-t border-agent-dark-border pt-6">
          <div className="p-4 bg-blue-900/20 border border-blue-700 rounded-lg">
            <p className="text-blue-300 font-medium">Cloudera's Official Kafka to Iceberg ReadyFlow</p>
            <p className="text-blue-200 text-sm mt-2">
              Uses NiFi ConsumeKafka + PutIceberg processors. Supports both cloud-managed DataFlow and on-prem CFM.
            </p>
            <a
              href={result.nifi_flow.deploy_url}
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-400 text-sm mt-2 inline-flex items-center gap-1 hover:text-blue-300"
            >
              View official docs <ExternalLink className="w-3 h-3" />
            </a>
          </div>

          {/* ReadyFlow Parameters */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <h4 className="font-semibold text-agent-text-primary">ReadyFlow Parameters</h4>
              <CopyButton text={JSON.stringify(result.nifi_flow.parameters, null, 2)} sectionId="nifi_params" />
            </div>
            <div className="relative">
              <CodeBlock content={JSON.stringify(result.nifi_flow.parameters, null, 2)} />
            </div>
          </div>

          {/* Processor Properties */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <h4 className="font-semibold text-agent-text-primary">Processor Properties (On-Prem CFM)</h4>
              <CopyButton
                text={JSON.stringify(result.nifi_flow.processor_properties, null, 2)}
                sectionId="nifi_props"
              />
            </div>
            <div className="relative">
              <CodeBlock content={JSON.stringify(result.nifi_flow.processor_properties, null, 2)} />
            </div>
            <p className="text-xs text-agent-text-secondary mt-2">
              For running on Cloudera Flow Management (CFM) on private clusters
            </p>
          </div>

          {/* Deployment Options */}
          <div className="p-4 bg-agent-dark-bg border border-agent-dark-border rounded-lg">
            <h4 className="font-semibold text-agent-text-primary mb-3">Deployment Options</h4>
            {Object.entries(result.nifi_flow.deployment_options).map(([key, option]) => (
              <div key={key} className="mb-4">
                <p className="font-medium text-agent-text-primary">{option.name}</p>
                <p className="text-sm text-agent-text-secondary mt-1">{option.description}</p>
                <p className="text-sm text-agent-text-secondary mt-2">{option.instructions}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Flink SQL Method Content */}
      {deployMethod === 'flink' && (
        <div className="space-y-4 border-t border-agent-dark-border pt-6">
          <div className="p-4 bg-yellow-900/20 border border-yellow-700 rounded-lg">
            <p className="text-yellow-300 font-medium">Flink SQL for Complex Transformations</p>
            <p className="text-yellow-200 text-sm mt-2">
              Use Cloudera SQL Stream Builder (SSB) or submit raw Flink jobs. Ideal for stateful processing and enrichment.
            </p>
          </div>

          <div>
            <div className="flex items-center justify-between mb-2">
              <h4 className="font-semibold text-agent-text-primary">Flink SQL Job</h4>
              <CopyButton text={result.flink_sql} sectionId="flink_sql" />
            </div>
            <div className="relative">
              <CodeBlock content={result.flink_sql} language="sql" />
            </div>
          </div>

          <div className="p-4 bg-agent-dark-bg border border-agent-dark-border rounded-lg text-sm text-agent-text-secondary">
            <p className="mb-2">
              <strong>Deployment:</strong> Save to a file and submit via Cloudera SQL Stream Builder or Flink CLI
            </p>
            <p className="text-xs">Note: V2 Iceberg upsert is Technical Preview only. Use V1 (append) for production.</p>
          </div>
        </div>
      )}

      {/* Kafka Connect Method Content */}
      {deployMethod === 'kafka_connect' && (
        <div className="space-y-4 border-t border-agent-dark-border pt-6">
          <div className="p-4 bg-purple-900/20 border border-purple-700 rounded-lg">
            <p className="text-purple-300 font-medium">Kafka Connect Sink (Power Users)</p>
            <p className="text-purple-200 text-sm mt-2">
              For teams with Kafka Connect expertise. Requires operational knowledge of Kafka Connect deployment and management.
            </p>
          </div>

          <div>
            <div className="flex items-center justify-between mb-2">
              <h4 className="font-semibold text-agent-text-primary">Connector Config</h4>
              <CopyButton text={result.connect_config} sectionId="connect_config" />
            </div>
            <div className="relative">
              <CodeBlock content={result.connect_config} />
            </div>
          </div>

          <div className="p-4 bg-agent-dark-bg border border-agent-dark-border rounded-lg text-sm text-agent-text-secondary">
            <p className="mb-2">
              <strong>Deployment:</strong> POST to Kafka Connect REST API at /connectors
            </p>
            <p className="text-xs">
              Note: Cloudera has limited Kafka Connect expertise. NiFi is the recommended first choice.
            </p>
          </div>
        </div>
      )}

      {/* Supporting Sections */}
      <div className="space-y-4 border-t border-agent-dark-border pt-6">
        <CollapsibleSection title="Schema Mapping" sectionId="schema">
          <div className="relative">
            <div className="absolute top-2 right-2 z-10">
              <CopyButton
                text={JSON.stringify(
                  result.schema_map.map(f => ({
                    field: f.kafka_field,
                    kafka_type: f.kafka_type,
                    target_column: f.iceberg_column,
                    target_type: f.iceberg_type,
                    nullable: f.nullable,
                    pii: f.pii_risk ? '⚠️' : '✓',
                  })),
                  null,
                  2
                )}
                sectionId="schema_map"
              />
            </div>
            <CodeBlock
              content={JSON.stringify(
                result.schema_map.map(f => ({
                  field: f.kafka_field,
                  kafka_type: f.kafka_type,
                  target_column: f.iceberg_column,
                  target_type: f.iceberg_type,
                  nullable: f.nullable,
                  pii: f.pii_risk ? '⚠️' : '✓',
                })),
                null,
                2
              )}
            />
          </div>
        </CollapsibleSection>

        <CollapsibleSection title="Iceberg DDL (Reference)" sectionId="ddl">
          <div className="relative">
            <div className="absolute top-2 right-2 z-10">
              <CopyButton text={result.iceberg_ddl} sectionId="ddl_sql" />
            </div>
            <CodeBlock content={result.iceberg_ddl} language="sql" />
          </div>
        </CollapsibleSection>

        {/* Consumer Group Lag */}
        <div className="border border-agent-dark-border rounded-lg overflow-hidden">
          <button
            onClick={() => toggleSection('lag')}
            className="w-full flex items-center justify-between p-4 bg-agent-dark-bg hover:bg-agent-dark-border transition"
          >
            <h3 className="font-semibold text-agent-text-primary">Consumer Group Lag</h3>
            <ChevronDown
              className={`w-5 h-5 text-agent-text-secondary transition transform ${
                expandedSections.lag ? 'rotate-180' : ''
              }`}
            />
          </button>
          {expandedSections.lag && (
            <div className="p-4 bg-agent-dark-surface border-t border-agent-dark-border">
              {result.consumer_lag ? (
                <div
                  className={`p-3 rounded-lg ${
                    result.consumer_lag.lag < 10000
                      ? 'bg-green-900/20 border border-green-700'
                      : 'bg-amber-900/20 border border-amber-700'
                  }`}
                >
                  <div className="flex items-center gap-2 mb-1">
                    {result.consumer_lag.lag < 10000 ? (
                      <CheckCircle className="w-5 h-5 text-green-500" />
                    ) : (
                      <AlertTriangle className="w-5 h-5 text-amber-500" />
                    )}
                    <span
                      className={
                        result.consumer_lag.lag < 10000 ? 'text-green-300 font-medium' : 'text-amber-300 font-medium'
                      }
                    >
                      {result.consumer_lag.lag < 10000 ? 'Healthy' : 'Lagging'}
                    </span>
                  </div>
                  <p className="text-sm text-agent-text-secondary">
                    Group: <span className="text-agent-text-primary">{result.consumer_lag.group_id}</span>
                  </p>
                  <p className="text-sm text-agent-text-secondary">
                    Lag: <span className="text-agent-text-primary">{result.consumer_lag.lag.toLocaleString()} messages</span>
                  </p>
                </div>
              ) : (
                <div className="p-3 bg-agent-dark-bg border border-agent-dark-border rounded-lg text-agent-text-secondary text-sm">
                  No consumer group found for this topic. A new pipeline will start from the earliest offset.
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Regenerate Button */}
      <button
        onClick={generatePipeline}
        disabled={loading}
        className="w-full px-4 py-2 bg-cloudera text-white rounded-md hover:bg-cloudera-hover transition font-medium disabled:opacity-50"
      >
        Regenerate Config
      </button>
    </div>
  );
};
