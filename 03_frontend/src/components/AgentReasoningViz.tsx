import { useState } from 'react';
import { ChevronDown, ChevronRight, Zap, Database, Brain, Shield, CheckCircle2, Clock } from 'lucide-react';

interface ReasoningStep {
  id: string;
  stage: string;
  title: string;
  icon: React.ReactNode;
  status: 'pending' | 'running' | 'complete' | 'error';
  systemPrompt?: string;
  input?: any;
  output?: any;
  reasoning?: string;
  children?: ReasoningStep[];
}

interface Props {
  steps: ReasoningStep[];
  currentStageId?: string;
}

function StatusBadge({ status }: { status: 'pending' | 'running' | 'complete' | 'error' }) {
  const styles = {
    pending: 'bg-gray-500/20 text-gray-300',
    running: 'bg-agent-orange/20 text-agent-orange animate-pulse',
    complete: 'bg-green-500/20 text-green-300',
    error: 'bg-red-500/20 text-red-300',
  };
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-semibold ${styles[status]}`}>
      {status.toUpperCase()}
    </span>
  );
}

function ReasoningStepCard({ step, isActive }: { step: ReasoningStep; isActive: boolean }) {
  const [expanded, setExpanded] = useState(isActive);

  return (
    <div className={`border rounded-lg mb-3 transition-all ${
      isActive
        ? 'border-agent-orange bg-agent-orange/5'
        : 'border-agent-dark-border bg-agent-dark-surface/50'
    }`}>
      {/* Header */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full px-4 py-3 flex items-center gap-3 hover:bg-agent-dark-border/50 transition-colors text-left"
      >
        <div className="flex-shrink-0">
          {expanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
        </div>

        {/* Icon and Title */}
        <div className="flex-shrink-0 text-lg">{step.icon}</div>
        <div className="flex-1 min-w-0">
          <h3 className="text-sm font-bold text-agent-text-primary truncate">{step.title}</h3>
          <p className="text-xs text-agent-text-secondary mt-0.5">{step.stage}</p>
        </div>

        {/* Status */}
        <div className="flex-shrink-0">
          <StatusBadge status={step.status} />
        </div>

        {/* Activity Indicator */}
        {step.status === 'running' && (
          <div className="flex-shrink-0 animate-spin">
            <Clock size={16} className="text-agent-orange" />
          </div>
        )}
        {step.status === 'complete' && (
          <div className="flex-shrink-0">
            <CheckCircle2 size={16} className="text-green-400" />
          </div>
        )}
      </button>

      {/* Expanded Content */}
      {expanded && (
        <div className="border-t border-agent-dark-border px-4 py-3 bg-agent-dark-bg/50 space-y-3">
          {/* System Prompt */}
          {step.systemPrompt && (
            <div>
              <div className="text-xs font-semibold text-agent-text-secondary mb-1.5">
                💬 SYSTEM PROMPT
              </div>
              <div className="bg-agent-dark-surface border border-agent-dark-border rounded p-2">
                <pre className="text-[11px] text-agent-text-secondary whitespace-pre-wrap break-words font-mono max-h-32 overflow-y-auto">
                  {step.systemPrompt}
                </pre>
              </div>
            </div>
          )}

          {/* Input Data */}
          {step.input && (
            <div>
              <div className="text-xs font-semibold text-agent-text-secondary mb-1.5">
                📥 INPUT
              </div>
              <div className="bg-agent-dark-surface border border-agent-dark-border rounded p-2">
                <pre className="text-[11px] text-agent-text-secondary whitespace-pre-wrap break-words font-mono max-h-24 overflow-y-auto">
                  {typeof step.input === 'string' ? step.input : JSON.stringify(step.input, null, 2)}
                </pre>
              </div>
            </div>
          )}

          {/* Reasoning */}
          {step.reasoning && (
            <div>
              <div className="text-xs font-semibold text-agent-text-secondary mb-1.5">
                🧠 REASONING
              </div>
              <p className="text-xs text-agent-text-secondary bg-blue-500/10 border border-blue-500/20 rounded p-2">
                {step.reasoning}
              </p>
            </div>
          )}

          {/* Output Data */}
          {step.output && (
            <div>
              <div className="text-xs font-semibold text-agent-text-secondary mb-1.5">
                📤 OUTPUT
              </div>
              <div className="bg-green-500/5 border border-green-500/20 rounded p-2">
                <pre className="text-[11px] text-green-300 whitespace-pre-wrap break-words font-mono max-h-32 overflow-y-auto">
                  {typeof step.output === 'string' ? step.output : JSON.stringify(step.output, null, 2)}
                </pre>
              </div>
            </div>
          )}

          {/* Sub-steps */}
          {step.children && step.children.length > 0 && (
            <div>
              <div className="text-xs font-semibold text-agent-text-secondary mb-2">
                ↳ SUB-STEPS ({step.children.length})
              </div>
              <div className="space-y-2">
                {step.children.map(child => (
                  <ReasoningStepCard key={child.id} step={child} isActive={false} />
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export function AgentReasoningViz({ steps, currentStageId }: Props) {
  return (
    <div className="space-y-2 p-4 bg-agent-dark-bg rounded-lg border border-agent-dark-border">
      {/* Header */}
      <div className="mb-4">
        <h2 className="text-sm font-bold text-agent-text-primary flex items-center gap-2">
          <Zap size={16} />
          Agent Reasoning Process
        </h2>
        <p className="text-xs text-agent-text-secondary mt-1">
          Expand each stage to see system prompts, data flow, and reasoning
        </p>
      </div>

      {/* Progress Bar */}
      <div className="mb-4">
        <div className="flex items-center gap-2 mb-2">
          <span className="text-xs font-semibold text-agent-text-secondary">PIPELINE PROGRESS</span>
          <span className="text-xs text-agent-text-secondary">
            {steps.filter(s => s.status === 'complete').length} / {steps.length}
          </span>
        </div>
        <div className="h-1.5 bg-agent-dark-border rounded-full overflow-hidden">
          <div
            className="h-full bg-gradient-to-r from-agent-orange to-green-500 transition-all duration-300"
            style={{
              width: `${(steps.filter(s => s.status === 'complete').length / steps.length) * 100}%`
            }}
          />
        </div>
      </div>

      {/* Steps Timeline */}
      <div className="space-y-2">
        {steps.map((step, idx) => (
          <div key={step.id}>
            <ReasoningStepCard
              step={step}
              isActive={step.id === currentStageId || step.status === 'running'}
            />

            {/* Connector */}
            {idx < steps.length - 1 && (
              <div className="flex justify-center py-1">
                <div className="w-0.5 h-3 bg-agent-dark-border" />
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
