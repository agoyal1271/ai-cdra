import { useState } from 'react';
import { AgentPanel } from './AgentPanel';

export function MetadataCurator() {
  const [applyRanger, setApplyRanger] = useState(false);
  const [viewMode, setViewMode] = useState<'side-by-side' | 'tabs'>('side-by-side');
  const [activeTab, setActiveTab] = useState<'react' | 'hierarchical'>('react');

  const reactAgent = {
    id: 'metadata_curator_react',
    name: 'ReAct Agent',
    tagline: 'INTERACTIVE',
    description: 'Single agent with deep reasoning loop: Thought → Act → Observe → Reason. Shows field-by-field analysis, PII detection per field, confidence scores.',
    icon: '💭',
  };

  const hierarchicalAgent = {
    id: 'metadata_curator_hierarchical',
    name: 'Hierarchical Agent',
    tagline: 'SCALABLE',
    description: '4 specialized agents in pipeline: Discovery → Classification → Learning → Policy. Parallel processing, handles 1000+ tables efficiently.',
    icon: '🏗️',
  };

  return (
    <div className="flex flex-col h-full">
      {/* Header & Controls */}
      <div className="flex-shrink-0 bg-agent-dark-surface border-b border-agent-dark-border px-8 py-4">
        <div className="space-y-4">
          {/* Title */}
          <div>
            <h2 className="text-sm font-bold text-agent-text-primary">
              🏛️ Metadata Curator: Two Independent AI Agent Architectures
            </h2>
            <p className="text-xs text-agent-text-secondary mt-1">
              Run both agents independently on the same governance task to compare approaches
            </p>
          </div>

          {/* View Mode Selector */}
          <div className="flex items-center gap-4">
            <div>
              <label className="block text-xs font-semibold text-agent-text-secondary uppercase mb-2">
                View Mode
              </label>
              <div className="flex gap-2">
                <button
                  onClick={() => setViewMode('side-by-side')}
                  className={`px-3 py-2 rounded-agent-md text-sm font-semibold transition-colors ${
                    viewMode === 'side-by-side'
                      ? 'bg-agent-orange text-white'
                      : 'bg-agent-dark-border text-agent-text-secondary hover:text-agent-text-primary'
                  }`}
                >
                  ⬅️➡️ Side-by-Side
                </button>
                <button
                  onClick={() => setViewMode('tabs')}
                  className={`px-3 py-2 rounded-agent-md text-sm font-semibold transition-colors ${
                    viewMode === 'tabs'
                      ? 'bg-agent-orange text-white'
                      : 'bg-agent-dark-border text-agent-text-secondary hover:text-agent-text-primary'
                  }`}
                >
                  📑 Tabs
                </button>
              </div>
            </div>

            {/* Ranger Integration */}
            <div className="flex items-center gap-2 pl-6 border-l border-agent-dark-border">
              <input
                type="checkbox"
                id="ranger-toggle"
                checked={applyRanger}
                onChange={e => setApplyRanger(e.target.checked)}
                className="w-4 h-4 rounded cursor-pointer"
              />
              <label htmlFor="ranger-toggle" className="text-sm text-agent-text-secondary cursor-pointer">
                Apply via Ranger API
              </label>
            </div>
          </div>
        </div>
      </div>

      {/* Content Area */}
      {viewMode === 'side-by-side' ? (
        // SIDE-BY-SIDE VIEW: Both agents visible with independent run buttons
        <div className="flex-1 flex gap-4 overflow-hidden p-4 bg-agent-dark-bg">
          {/* ReAct Agent Panel */}
          <div className="flex-1 flex flex-col min-w-0 bg-agent-dark-surface rounded-lg border border-agent-dark-border overflow-hidden">
            <div className="flex-shrink-0 px-4 py-3 bg-gradient-to-r from-agent-orange/20 to-transparent border-b border-agent-dark-border">
              <div className="flex items-center gap-2">
                <span className="text-xl">{reactAgent.icon}</span>
                <div>
                  <h3 className="text-sm font-bold text-agent-text-primary">{reactAgent.name}</h3>
                  <p className="text-xs text-agent-text-secondary">{reactAgent.tagline}</p>
                </div>
              </div>
            </div>
            <div className="flex-1 overflow-hidden">
              <AgentPanel
                agent={reactAgent}
                endpoint="/api/agents/govern-metadata"
                extraParams={{ agent_type: 'react', apply_ranger: applyRanger }}
              />
            </div>
          </div>

          {/* Hierarchical Agent Panel */}
          <div className="flex-1 flex flex-col min-w-0 bg-agent-dark-surface rounded-lg border border-agent-dark-border overflow-hidden">
            <div className="flex-shrink-0 px-4 py-3 bg-gradient-to-r from-blue-500/20 to-transparent border-b border-agent-dark-border">
              <div className="flex items-center gap-2">
                <span className="text-xl">{hierarchicalAgent.icon}</span>
                <div>
                  <h3 className="text-sm font-bold text-agent-text-primary">{hierarchicalAgent.name}</h3>
                  <p className="text-xs text-agent-text-secondary">{hierarchicalAgent.tagline}</p>
                </div>
              </div>
            </div>
            <div className="flex-1 overflow-hidden">
              <AgentPanel
                agent={hierarchicalAgent}
                endpoint="/api/agents/govern-metadata"
                extraParams={{ agent_type: 'hierarchical', apply_ranger: applyRanger }}
              />
            </div>
          </div>
        </div>
      ) : (
        // TAB VIEW: Switch between agents
        <div className="flex-1 flex flex-col overflow-hidden">
          {/* Tabs */}
          <div className="flex-shrink-0 flex gap-1 px-4 pt-4 bg-agent-dark-bg border-b border-agent-dark-border">
            <button
              onClick={() => setActiveTab('react')}
              className={`px-4 py-2 rounded-t-lg font-semibold text-sm transition-colors ${
                activeTab === 'react'
                  ? 'bg-agent-orange text-white'
                  : 'bg-agent-dark-border text-agent-text-secondary hover:text-agent-text-primary'
              }`}
            >
              💭 ReAct Agent
            </button>
            <button
              onClick={() => setActiveTab('hierarchical')}
              className={`px-4 py-2 rounded-t-lg font-semibold text-sm transition-colors ${
                activeTab === 'hierarchical'
                  ? 'bg-agent-orange text-white'
                  : 'bg-agent-dark-border text-agent-text-secondary hover:text-agent-text-primary'
              }`}
            >
              🏗️ Hierarchical Agent
            </button>
          </div>

          {/* Tab Content */}
          <div className="flex-1 overflow-hidden bg-agent-dark-surface flex flex-col">
            {/* Agent Description */}
            <div className="flex-shrink-0 px-4 py-3 bg-agent-dark-border/30 border-b border-agent-dark-border">
              <p className="text-xs text-agent-text-secondary">
                {activeTab === 'react' ? reactAgent.description : hierarchicalAgent.description}
              </p>
            </div>

            {/* Agent Output */}
            <div className="flex-1 overflow-hidden">
              {activeTab === 'react' && (
                <AgentPanel
                  agent={reactAgent}
                  endpoint="/api/agents/govern-metadata"
                  extraParams={{ agent_type: 'react', apply_ranger: applyRanger }}
                />
              )}
              {activeTab === 'hierarchical' && (
                <AgentPanel
                  agent={hierarchicalAgent}
                  endpoint="/api/agents/govern-metadata"
                  extraParams={{ agent_type: 'hierarchical', apply_ranger: applyRanger }}
                />
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
