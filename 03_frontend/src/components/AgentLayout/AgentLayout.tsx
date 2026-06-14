import { useState } from 'react';
import {
  Settings,
  Database,
  BookOpen,
  FileText,
  Send,
  Menu,
  X,
  Cloud,
  User,
  BarChart3,
  TrendingUp,
  Zap,
} from 'lucide-react';
import { Navbar } from './Navbar';
import { Sidebar } from './Sidebar';
import { ChatPanel } from './ChatPanel';
import { MetricsPanel } from './MetricsPanel';

export function AgentLayout() {
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [messages, setMessages] = useState<Array<{ id: string; role: 'user' | 'agent'; content: string }>>([
    { id: '1', role: 'agent', content: 'Hello! I\'m your AI Agent. How can I help you discover and analyze data today?' },
  ]);
  const [inputValue, setInputValue] = useState('');

  const handleSendMessage = () => {
    if (!inputValue.trim()) return;

    setMessages(prev => [
      ...prev,
      { id: Date.now().toString(), role: 'user', content: inputValue },
      {
        id: (Date.now() + 1).toString(),
        role: 'agent',
        content: 'Processing your request... Scanning data sources and executing quality checks.',
      },
    ]);
    setInputValue('');
  };

  return (
    <div className="flex h-screen bg-agent-dark-bg text-agent-text-primary">
      {/* Sidebar */}
      <Sidebar isOpen={sidebarOpen} onClose={() => setSidebarOpen(false)} />

      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Navbar */}
        <Navbar onMenuClick={() => setSidebarOpen(!sidebarOpen)} />

        {/* Main Workspace — Two panels */}
        <div className="flex flex-1 overflow-hidden gap-px bg-agent-dark-border">
          {/* Left Panel: Chat Interface */}
          <ChatPanel messages={messages} />

          {/* Divider */}
          <div className="w-px bg-agent-dark-border" />

          {/* Right Panel: Metrics Dashboard */}
          <MetricsPanel />
        </div>
      </div>
    </div>
  );
}
