import { ArrowRight, Database, Zap, BarChart3, Check } from 'lucide-react';

export function DataFlowChart() {
  const steps = [
    {
      icon: <Database size={16} />,
      label: 'Data Discovery',
      status: 'complete',
      metric: '342 assets',
    },
    {
      icon: <Zap size={16} />,
      label: 'Processing',
      status: 'active',
      metric: '18.4 GB',
    },
    {
      icon: <BarChart3 size={16} />,
      label: 'Quality Analysis',
      status: 'complete',
      metric: '156 checks',
    },
    {
      icon: <Check size={16} />,
      label: 'Insights',
      status: 'pending',
      metric: '23 alerts',
    },
  ];

  return (
    <div className="space-y-3">
      {/* Pipeline Visualization */}
      <div className="bg-agent-dark-surface border border-agent-dark-border rounded-agent-lg p-4">
        <div className="flex items-center justify-between mb-4">
          {steps.map((step, idx) => (
            <div key={idx} className="flex items-center">
              {/* Step Circle */}
              <div
                className={`
                  w-10 h-10 rounded-full flex items-center justify-center border-2
                  transition-all
                  ${
                    step.status === 'complete'
                      ? 'bg-agent-teal/20 border-agent-teal text-agent-teal'
                      : step.status === 'active'
                        ? 'bg-agent-orange/20 border-agent-orange text-agent-orange animate-pulse'
                        : 'bg-agent-dark-border border-agent-dark-border text-agent-text-secondary'
                  }
                `}
              >
                {step.icon}
              </div>

              {/* Arrow */}
              {idx < steps.length - 1 && (
                <div
                  className={`
                    w-8 h-0.5 mx-2 transition-all
                    ${step.status === 'complete' ? 'bg-agent-teal' : 'bg-agent-dark-border'}
                  `}
                />
              )}
            </div>
          ))}
        </div>

        {/* Labels and Metrics */}
        <div className="grid grid-cols-4 gap-2">
          {steps.map((step, idx) => (
            <div key={idx} className="text-center text-xs">
              <p className="font-medium text-agent-text-primary mb-1">{step.label}</p>
              <p
                className={`text-[10px] ${
                  step.status === 'active'
                    ? 'text-agent-orange font-semibold'
                    : 'text-agent-text-secondary'
                }`}
              >
                {step.metric}
              </p>
            </div>
          ))}
        </div>
      </div>

      {/* Details Table */}
      <div className="bg-agent-dark-surface border border-agent-dark-border rounded-agent-lg p-3 text-xs">
        <div className="grid grid-cols-3 gap-2 mb-2 pb-2 border-b border-agent-dark-border">
          <p className="font-semibold text-agent-text-primary">Step</p>
          <p className="font-semibold text-agent-text-primary">Status</p>
          <p className="font-semibold text-agent-text-primary text-right">Duration</p>
        </div>
        <div className="space-y-2">
          <FlowRow step="Data Discovery" status="✓ Complete" duration="2.3s" statusColor="agent-teal" />
          <FlowRow step="Processing" status="⚡ Active" duration="8.1s" statusColor="agent-orange" />
          <FlowRow step="Quality Analysis" status="✓ Complete" duration="5.7s" statusColor="agent-teal" />
          <FlowRow step="Insights Generation" status="⏳ Pending" duration="—" statusColor="agent-text-secondary" />
        </div>
      </div>
    </div>
  );
}

interface FlowRowProps {
  step: string;
  status: string;
  duration: string;
  statusColor: string;
}

function FlowRow({ step, status, duration, statusColor }: FlowRowProps) {
  const colorClass = `text-${statusColor}`;
  return (
    <div className="grid grid-cols-3 gap-2">
      <p className="text-agent-text-secondary">{step}</p>
      <p className={`font-medium ${colorClass}`}>{status}</p>
      <p className="text-agent-text-secondary text-right">{duration}</p>
    </div>
  );
}
