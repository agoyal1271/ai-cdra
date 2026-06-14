import { TrendingUp, Zap, Database, Activity } from 'lucide-react';
import { MetricCard } from './MetricCard';
import { DataFlowChart } from './DataFlowChart';

export function MetricsPanel() {
  return (
    <div className="flex-1 flex flex-col bg-agent-dark-bg overflow-hidden">
      {/* Header */}
      <div className="h-16 px-6 flex items-center border-b border-agent-dark-border">
        <h2 className="text-lg font-semibold text-agent-text-primary">Data Log & Metrics</h2>
      </div>

      {/* Metrics Grid */}
      <div className="flex-1 overflow-y-auto p-6 space-y-4">
        {/* Key Metrics Row */}
        <div className="grid grid-cols-2 gap-4">
          <MetricCard
            label="Token Spend"
            value="2,847"
            unit="tokens/day"
            icon={<Zap size={16} />}
            trend={12}
            accentColor="orange"
          />
          <MetricCard
            label="Data Ingestion"
            value="18.4 GB"
            unit="processed today"
            icon={<Database size={16} />}
            trend={8}
            accentColor="teal"
          />
        </div>

        <div className="grid grid-cols-2 gap-4">
          <MetricCard
            label="Vector DB Connections"
            value="4/5"
            unit="active connections"
            icon={<Activity size={16} />}
            trend={0}
            accentColor="teal"
          />
          <MetricCard
            label="Quality Checks Executed"
            value="156"
            unit="this session"
            icon={<TrendingUp size={16} />}
            trend={18}
            accentColor="orange"
          />
        </div>

        {/* Data Flow Chart */}
        <div className="mt-6">
          <h3 className="text-sm font-semibold text-agent-text-primary mb-4 flex items-center gap-2">
            <Activity size={14} className="text-agent-orange" />
            Real-time Data Flow
          </h3>
          <DataFlowChart />
        </div>

        {/* Recent Activity Log */}
        <div className="mt-6">
          <h3 className="text-sm font-semibold text-agent-text-primary mb-3 flex items-center gap-2">
            <span>Recent Activity</span>
            <span className="text-agent-orange">●</span>
          </h3>
          <div className="space-y-2 text-xs">
            <ActivityLogEntry
              time="2 min ago"
              action="Completed quality check"
              target="sales_transactions"
              status="success"
            />
            <ActivityLogEntry
              time="5 min ago"
              action="Data discovery scan"
              target="customer_profiles"
              status="success"
            />
            <ActivityLogEntry
              time="12 min ago"
              action="Schema analysis"
              target="product_inventory"
              status="pending"
            />
            <ActivityLogEntry
              time="18 min ago"
              action="Vector embedding"
              target="knowledge_base"
              status="success"
            />
          </div>
        </div>
      </div>
    </div>
  );
}

interface ActivityLogEntryProps {
  time: string;
  action: string;
  target: string;
  status: 'success' | 'pending' | 'error';
}

function ActivityLogEntry({ time, action, target, status }: ActivityLogEntryProps) {
  const statusColors = {
    success: 'text-agent-teal',
    pending: 'text-agent-text-secondary',
    error: 'text-red-400',
  };

  const statusBgColors = {
    success: 'bg-agent-teal/10 border-agent-teal/30',
    pending: 'bg-agent-dark-border border-agent-dark-border',
    error: 'bg-red-950/20 border-red-800/30',
  };

  return (
    <div className={`px-3 py-2 rounded-agent-md border ${statusBgColors[status]} flex items-start gap-3`}>
      <div className={`w-1.5 h-1.5 rounded-full mt-1 flex-shrink-0 ${statusColors[status]}`} />
      <div className="flex-1 min-w-0">
        <p className="text-agent-text-primary">
          <span className="font-medium">{action}</span>
          <span className="text-agent-text-secondary mx-1">·</span>
          <span className="text-agent-text-secondary">{target}</span>
        </p>
        <p className="text-agent-text-secondary text-[10px] mt-0.5">{time}</p>
      </div>
    </div>
  );
}
