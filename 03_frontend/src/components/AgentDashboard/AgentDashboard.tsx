import { useEffect, useState } from 'react';
import { fetchAgents, fetchHealth } from '../../api/agents';
import type { Agent, HealthStatus } from '../../types/agents';
import { AgentCard } from './AgentCard';

interface Props {
  selectedAgent: string;
  onSelectAgent: (id: string) => void;
}

const SERVICE_ICONS: Record<string, string> = {
  kafka: '📨', iceberg: '🧊', ozone: '🪣', hdfs: '📁', flink: '⚡',
};

export function AgentDashboard({ selectedAgent, onSelectAgent }: Props) {
  const [agents, setAgents] = useState<Agent[]>([]);
  const [health, setHealth] = useState<HealthStatus | null>(null);

  useEffect(() => {
    fetchAgents().then(setAgents).catch(console.error);
    const refresh = () => fetchHealth().then(setHealth).catch(() => setHealth(null));
    refresh();
    const id = setInterval(refresh, 30_000);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="flex flex-col gap-6 p-8">
      {/* Platform header */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-xl font-bold text-agent-text-primary">Cloudera AI Agents</h1>
          <p className="text-sm text-agent-text-secondary mt-0.5">
            Six autonomous agents · self-discovering · self-building · self-healing · self-governing
          </p>
        </div>
        {health && (
          <div className="flex items-center gap-1.5 text-xs">
            <span className={`w-2 h-2 rounded-full ${health.status === 'ok' ? 'bg-green-400' : 'bg-amber-400'}`} />
            <span className={health.status === 'ok' ? 'text-green-400' : 'text-amber-400'}>
              {health.status === 'ok' ? 'All services connected' : 'Some services unavailable'}
            </span>
          </div>
        )}
      </div>

      {/* Service health strip */}
      {health && (
        <div className="flex gap-3 flex-wrap">
          {Object.entries(health.services).map(([name, status]) => (
            <div key={name} className="flex items-center gap-1.5 bg-agent-dark-border border border-agent-dark-border rounded-lg px-3 py-1.5">
              <span>{SERVICE_ICONS[name] ?? '🔧'}</span>
              <span className="text-xs text-agent-text-secondary capitalize">{name}</span>
              <span className={`text-[10px] font-semibold ${status.status === 'ok' ? 'text-green-400' : 'text-[#3a5a78]'}`}>
                {status.status === 'ok' ? '●' : '○'}
              </span>
            </div>
          ))}
        </div>
      )}

      {/* Agent grid */}
      <div>
        <h2 className="text-xs font-semibold text-agent-text-secondary uppercase tracking-wider mb-3">Agent Fleet</h2>
        <div className="grid grid-cols-2 lg:grid-cols-3 gap-4">
          {agents.map(agent => (
            <AgentCard
              key={agent.id}
              agent={agent}
              selected={selectedAgent === agent.id}
              onClick={() => agent.status === 'active' && onSelectAgent(agent.id)}
            />
          ))}
        </div>
      </div>

      {/* Info bar */}
      <div className="rounded-xl bg-agent-dark-border border border-agent-dark-border px-4 py-3 text-xs text-agent-text-secondary">
        <span className="text-agent-orange font-semibold">Phase 1:</span> Source Scout is fully operational.
        Pipeline Builder, Quality Guardian, Pipeline Healer, Semantic Mapper, and Metadata Curator
        are shipping in Phase 2. Click Source Scout to begin discovering your Cloudera data landscape.
      </div>
    </div>
  );
}
