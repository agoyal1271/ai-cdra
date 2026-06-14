import type { Agent } from '../../types/agents';

const ICON_MAP: Record<string, string> = {
  radar: '📡',
  wrench: '🔧',
  shield: '🛡️',
  'heart-pulse': '💓',
  brain: '🧠',
  gavel: '⚖️',
};

const STATUS_STYLES: Record<string, string> = {
  active: 'bg-agent-teal/20 text-agent-teal border border-agent-teal/40',
  coming_soon: 'bg-agent-dark-border text-agent-text-secondary border border-agent-dark-border',
  running: 'bg-agent-orange/20 text-agent-orange border border-agent-orange/40 animate-pulse',
  error: 'bg-red-500/20 text-red-400 border border-red-500/40',
};

const STATUS_LABELS: Record<string, string> = {
  active: 'ACTIVE',
  coming_soon: 'PHASE 2',
  running: 'RUNNING',
  error: 'ERROR',
};

interface Props {
  agent: Agent;
  onClick: () => void;
  selected: boolean;
}

export function AgentCard({ agent, onClick, selected }: Props) {
  const isActive = agent.status === 'active';
  return (
    <button
      onClick={onClick}
      className={`
        flex flex-col gap-3 p-4 rounded-xl border text-left transition-all duration-150
        ${selected ? 'border-agent-orange bg-agent-orange/10 shadow-lg shadow-agent-orange/10' : 'border-agent-dark-border bg-agent-dark-surface hover:border-agent-dark-border/80 hover:bg-agent-dark-surface'}
        ${!isActive ? 'opacity-60' : ''}
      `}
    >
      <div className="flex items-center justify-between">
        <span className="text-2xl">{ICON_MAP[agent.icon] ?? '🤖'}</span>
        <span className={`text-[9px] font-bold px-2 py-0.5 rounded-full ${STATUS_STYLES[agent.status]}`}>
          {STATUS_LABELS[agent.status]}
        </span>
      </div>
      <div>
        <div className="text-[10px] text-agent-teal font-semibold uppercase tracking-wider">{agent.role}</div>
        <div className="text-sm font-bold text-agent-text-primary mt-0.5">{agent.name}</div>
        <div className="text-xs text-agent-orange font-mono mt-0.5">{agent.tagline}</div>
      </div>
      <p className="text-xs text-agent-text-secondary leading-relaxed line-clamp-3">{agent.description}</p>
      {isActive && (
        <div className="flex flex-wrap gap-1 mt-1">
          {agent.tools.slice(0, 3).map(t => (
            <span key={t} className="text-[9px] bg-agent-dark-border text-agent-text-secondary px-1.5 py-0.5 rounded font-mono">{t}</span>
          ))}
          {agent.tools.length > 3 && (
            <span className="text-[9px] text-agent-text-secondary">+{agent.tools.length - 3} more</span>
          )}
        </div>
      )}
    </button>
  );
}
