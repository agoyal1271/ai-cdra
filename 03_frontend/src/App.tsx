import { useState } from 'react';
import { LayoutDashboard, Radar, Pin, Wrench, Shield, Heart, Brain, Gavel } from 'lucide-react';
import { AgentDashboard } from './components/AgentDashboard/AgentDashboard';
import { SourceScout } from './components/SourceScout/SourceScout';
import { PipelineBuilder } from './components/PipelineBuilder';
import { QualityGuardian } from './components/QualityGuardian';
import { PipelineHealer } from './components/PipelineHealer';
import { SemanticMapper } from './components/SemanticMapper';
import { MetadataCurator } from './components/MetadataCurator';
import { Workspace } from './components/Workspace/Workspace';
import { useWorkspace } from './hooks/useWorkspace';

type View = 'dashboard' | 'source_scout' | 'pipeline_builder' | 'quality_guardian' | 'pipeline_healer' | 'semantic_mapper' | 'metadata_curator' | 'workspace';

const NAV_ITEMS = [
  { id: 'dashboard', label: 'Dashboard', Icon: LayoutDashboard },
  { id: 'source_scout', label: 'Source Scout', Icon: Radar },
  { id: 'pipeline_builder', label: 'Pipeline Builder', Icon: Wrench },
  { id: 'quality_guardian', label: 'Quality Guardian', Icon: Shield },
  { id: 'pipeline_healer', label: 'Pipeline Healer', Icon: Heart },
  { id: 'semantic_mapper', label: 'Semantic Mapper', Icon: Brain },
  { id: 'metadata_curator', label: 'Metadata Curator', Icon: Gavel },
  { id: 'workspace', label: 'Workspace', Icon: Pin },
] as const;

export default function App() {
  const [view, setView] = useState<View>('dashboard');
  const { pinnedAssets } = useWorkspace();

  const navigate = (id: View) => setView(id);

  return (
    <div className="flex h-screen bg-agent-dark-bg text-agent-text-primary overflow-hidden">
      {/* Icon-only left nav — 64px wide */}
      <nav className="w-16 flex-shrink-0 bg-agent-dark-surface border-r border-agent-dark-border flex flex-col items-center">

        {/* Logo mark */}
        <div className="h-14 flex items-center justify-center border-b border-agent-dark-border w-full">
          <div className="w-8 h-8 rounded-agent-md bg-agent-orange flex items-center justify-center">
            <span className="text-white text-xs font-black">C</span>
          </div>
        </div>

        {/* Nav icons */}
        <div className="flex-1 flex flex-col items-center py-3 gap-1">
          {NAV_ITEMS.map(({ id, label, Icon }) => {
            const active = view === id;
            return (
              <div key={id} className="relative group">
                <button
                  onClick={() => navigate(id)}
                  className={`
                    w-10 h-10 rounded-agent-md flex items-center justify-center transition-colors
                    ${active
                      ? 'bg-agent-orange text-white'
                      : 'text-agent-text-secondary hover:bg-agent-dark-border hover:text-agent-text-primary'
                    }
                  `}
                  title={label}
                >
                  <Icon size={18} />
                  {id === 'workspace' && pinnedAssets.length > 0 && (
                    <span className="absolute -top-0.5 -right-0.5 w-4 h-4 bg-agent-orange text-white text-[10px] font-bold rounded-full flex items-center justify-center">
                      {pinnedAssets.length}
                    </span>
                  )}
                </button>
                {/* Tooltip */}
                <div className="absolute left-12 top-1/2 -translate-y-1/2 px-2 py-1 bg-agent-dark-surface border border-agent-dark-border text-xs text-agent-text-primary rounded whitespace-nowrap pointer-events-none opacity-0 group-hover:opacity-100 transition-opacity z-50">
                  {label}
                </div>
              </div>
            );
          })}
        </div>

        {/* Bottom — phase label */}
        <div className="py-3 text-[10px] text-agent-text-secondary font-mono text-center leading-tight">
          <div>P1</div>
        </div>
      </nav>

      {/* Main content — fills remaining width */}
      <main className="flex-1 overflow-hidden min-w-0">
        {view === 'dashboard' && (
          <div className="h-full overflow-y-auto">
            <AgentDashboard selectedAgent="" onSelectAgent={id => navigate(id as View)} />
          </div>
        )}
        {/* Keep agents mounted — state persists across tab switches */}
        <div className={view === 'source_scout' ? 'h-full' : 'hidden'}>
          <SourceScout />
        </div>
        <div className={view === 'pipeline_builder' ? 'h-full' : 'hidden'}>
          <PipelineBuilder />
        </div>
        <div className={view === 'quality_guardian' ? 'h-full' : 'hidden'}>
          <QualityGuardian />
        </div>
        <div className={view === 'pipeline_healer' ? 'h-full' : 'hidden'}>
          <PipelineHealer />
        </div>
        <div className={view === 'semantic_mapper' ? 'h-full' : 'hidden'}>
          <SemanticMapper />
        </div>
        <div className={view === 'metadata_curator' ? 'h-full' : 'hidden'}>
          <MetadataCurator />
        </div>
        {view === 'workspace' && (
          <div className="h-full">
            <Workspace />
          </div>
        )}
      </main>
    </div>
  );
}
