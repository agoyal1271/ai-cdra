import { useState } from 'react';
import {
  Settings,
  Database,
  BookOpen,
  FileText,
  X,
  ChevronRight,
} from 'lucide-react';

interface SidebarProps {
  isOpen: boolean;
  onClose: () => void;
}

interface NavItem {
  id: string;
  label: string;
  icon: React.ReactNode;
  badge?: number;
}

const NAV_ITEMS: NavItem[] = [
  {
    id: 'chat',
    label: 'Chat & Commands',
    icon: <FileText size={18} />,
  },
  {
    id: 'sources',
    label: 'Data Sources',
    icon: <Database size={18} />,
    badge: 12,
  },
  {
    id: 'knowledge',
    label: 'Knowledge Base',
    icon: <BookOpen size={18} />,
    badge: 3,
  },
  {
    id: 'logs',
    label: 'Execution Logs',
    icon: <FileText size={18} />,
  },
  {
    id: 'settings',
    label: 'Agent Settings',
    icon: <Settings size={18} />,
  },
];

export function Sidebar({ isOpen, onClose }: SidebarProps) {
  const [activeItem, setActiveItem] = useState('chat');

  return (
    <>
      {/* Mobile Overlay */}
      {isOpen && (
        <div
          className="fixed inset-0 bg-black/50 lg:hidden z-40"
          onClick={onClose}
        />
      )}

      {/* Sidebar Panel */}
      <aside
        className={`
          fixed lg:static inset-y-16 left-0 w-64 bg-agent-dark-surface border-r border-agent-dark-border
          transform transition-transform duration-300 z-40
          ${isOpen ? 'translate-x-0' : '-translate-x-full lg:translate-x-0'}
          flex flex-col overflow-hidden
        `}
      >
        {/* Header */}
        <div className="h-16 px-6 flex items-center justify-between border-b border-agent-dark-border lg:hidden">
          <span className="text-sm font-semibold text-agent-text-primary">Navigation</span>
          <button
            onClick={onClose}
            className="p-1 hover:bg-agent-dark-border rounded-agent-md transition-colors"
          >
            <X size={18} className="text-agent-text-secondary" />
          </button>
        </div>

        {/* Navigation Items */}
        <nav className="flex-1 px-3 py-4 space-y-1 overflow-y-auto">
          {NAV_ITEMS.map(item => (
            <button
              key={item.id}
              onClick={() => {
                setActiveItem(item.id);
                onClose();
              }}
              className={`
                w-full flex items-center gap-3 px-4 py-3 rounded-agent-md text-sm font-medium
                transition-colors duration-200
                ${
                  activeItem === item.id
                    ? 'bg-agent-orange text-white'
                    : 'text-agent-text-secondary hover:text-agent-text-primary hover:bg-agent-dark-border'
                }
              `}
            >
              <span className="flex-shrink-0">{item.icon}</span>
              <span className="flex-1 text-left">{item.label}</span>
              {item.badge && (
                <span className={`flex-shrink-0 px-2 py-1 text-xs font-semibold rounded-full ${
                  activeItem === item.id
                    ? 'bg-white/30 text-white'
                    : 'bg-agent-orange/20 text-agent-orange'
                }`}>
                  {item.badge}
                </span>
              )}
              <ChevronRight
                size={16}
                className={`flex-shrink-0 transition-opacity ${
                  activeItem === item.id ? 'opacity-100' : 'opacity-0'
                }`}
              />
            </button>
          ))}
        </nav>

        {/* Footer */}
        <div className="px-3 py-4 border-t border-agent-dark-border">
          <div className="px-4 py-3 bg-agent-dark-bg rounded-agent-md text-xs text-agent-text-secondary">
            <p className="font-semibold text-agent-text-primary mb-1">Platform Version</p>
            <p>v2.1.0 — Stable</p>
          </div>
        </div>
      </aside>
    </>
  );
}
