import { AgentPanel } from './AgentPanel';

export function QualityGuardian() {
  return (
    <AgentPanel
      agent={{
        id: 'quality_guardian',
        name: 'Quality Guardian',
        tagline: 'REAL-TIME QA',
        description: 'Validates data quality, detects anomalies, and auto-quarantines bad records. Profiles data dimensions and generates tests to ensure your data meets SLA requirements.',
        icon: '🛡️',
      }}
      endpoint="/api/agents/validate-quality"
    />
  );
}
