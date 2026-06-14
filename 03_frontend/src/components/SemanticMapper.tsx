import { AgentPanel } from './AgentPanel';

export function SemanticMapper() {
  return (
    <AgentPanel
      agent={{
        id: 'semantic_mapper',
        name: 'Semantic Mapper',
        tagline: 'NL → METRICS',
        description: 'Maps raw data fields to business semantics, detects conflicting metric definitions, and suggests business glossary terms. Bridges the gap between data and business.',
        icon: '🧠',
      }}
      endpoint="/api/agents/map-semantics"
    />
  );
}
