import { AgentPanel } from './AgentPanel';

export function PipelineHealer() {
  return (
    <AgentPanel
      agent={{
        id: 'pipeline_healer',
        name: 'Pipeline Healer',
        tagline: 'SELF-HEALING',
        description: 'Monitors pipeline health 24/7, diagnoses root causes of failures, and auto-remediates issues before they escalate. Enables intelligent, autonomous operations.',
        icon: '💚',
      }}
      endpoint="/api/agents/monitor-health"
    />
  );
}
