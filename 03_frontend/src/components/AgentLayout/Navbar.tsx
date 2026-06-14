import { Menu, Cloud, User, LogOut } from 'lucide-react';

interface NavbarProps {
  onMenuClick: () => void;
}

export function Navbar({ onMenuClick }: NavbarProps) {
  return (
    <nav className="h-16 bg-agent-dark-surface border-b border-agent-dark-border px-6 flex items-center justify-between">
      {/* Left: Menu + Brand */}
      <div className="flex items-center gap-4">
        <button
          onClick={onMenuClick}
          className="p-2 hover:bg-agent-dark-border rounded-agent-md transition-colors"
          title="Toggle sidebar"
        >
          <Menu size={20} className="text-agent-text-secondary" />
        </button>
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 bg-agent-orange rounded-agent-md flex items-center justify-center">
            <span className="text-white font-bold text-sm">AI</span>
          </div>
          <div>
            <h1 className="text-lg font-semibold text-agent-text-primary">CloudData Agent</h1>
            <p className="text-xs text-agent-text-secondary">Enterprise Data Platform</p>
          </div>
        </div>
      </div>

      {/* Center: Status */}
      <div className="flex items-center gap-3 px-4 py-2 bg-agent-orange/10 rounded-agent-md border border-agent-orange/40">
        <div className="w-2 h-2 bg-agent-orange rounded-full animate-pulse" />
        <span className="text-sm font-semibold text-agent-orange">Hybrid Cloud — Active</span>
      </div>

      {/* Right: User Menu */}
      <div className="flex items-center gap-4">
        <button className="p-2 hover:bg-agent-dark-border rounded-agent-md transition-colors">
          <Cloud size={20} className="text-agent-text-secondary hover:text-agent-text-primary" />
        </button>
        <div className="w-px h-6 bg-agent-dark-border" />
        <div className="flex items-center gap-3 px-3 py-2 hover:bg-agent-dark-border rounded-agent-md cursor-pointer transition-colors">
          <div className="w-6 h-6 bg-agent-orange rounded-full flex items-center justify-center">
            <User size={14} className="text-white" />
          </div>
          <span className="text-sm text-agent-text-primary">Admin</span>
        </div>
      </div>
    </nav>
  );
}
