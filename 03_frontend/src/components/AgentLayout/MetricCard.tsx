import { TrendingUp, TrendingDown } from 'lucide-react';

interface MetricCardProps {
  label: string;
  value: string | number;
  unit: string;
  icon: React.ReactNode;
  trend?: number;
  accentColor: 'orange' | 'teal';
}

export function MetricCard({
  label,
  value,
  unit,
  icon,
  trend,
  accentColor,
}: MetricCardProps) {
  const colors = {
    orange: {
      bg: 'bg-gradient-to-br from-agent-orange/10 to-orange-950/20',
      border: 'border-agent-orange/30',
      icon: 'text-agent-orange',
      accent: 'text-agent-orange',
    },
    teal: {
      bg: 'bg-gradient-to-br from-agent-teal/10 to-cyan-950/20',
      border: 'border-agent-teal/30',
      icon: 'text-agent-teal',
      accent: 'text-agent-teal',
    },
  };

  const color = colors[accentColor];
  const trendPositive = (trend ?? 0) >= 0;

  return (
    <div
      className={`
        ${color.bg} border ${color.border} rounded-agent-lg p-4
        transition-all hover:shadow-agent-md hover:border-opacity-50
      `}
    >
      {/* Header */}
      <div className="flex items-start justify-between mb-3">
        <div className={`p-2 rounded-agent-md bg-agent-dark-surface border ${color.border}`}>
          <span className={color.icon}>{icon}</span>
        </div>
        {trend !== undefined && trend !== 0 && (
          <div className={`flex items-center gap-1 text-xs font-semibold ${trendPositive ? 'text-green-400' : 'text-red-400'}`}>
            {trendPositive ? <TrendingUp size={12} /> : <TrendingDown size={12} />}
            {Math.abs(trend)}%
          </div>
        )}
      </div>

      {/* Value */}
      <div className="mb-2">
        <p className="text-2xl font-bold text-agent-text-primary">{value}</p>
        <p className="text-xs text-agent-text-secondary">{unit}</p>
      </div>

      {/* Label */}
      <p className="text-xs font-medium text-agent-text-secondary">{label}</p>
    </div>
  );
}
