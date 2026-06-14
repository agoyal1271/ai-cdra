import { MessageSquare, Database, HardDrive, FolderOpen } from 'lucide-react';

// Cloudera Design System constants — single source of truth for colors, icons, and status configs

// Asset type display configuration
export const TYPE_STYLES = {
  kafka_topic: {
    bg: 'bg-[#0a1f2e]',
    border: 'border-[#1a4a6e]',
    dot: 'bg-blue-400',
    icon: '📨',
    label: 'Kafka Topic',
  },
  iceberg_table: {
    bg: 'bg-[#091e1a]',
    border: 'border-[#1a4a3a]',
    dot: 'bg-teal-400',
    icon: '🧊',
    label: 'Iceberg Table',
  },
  ozone_volume: {
    bg: 'bg-[#1a0e2e]',
    border: 'border-[#3a1a5e]',
    dot: 'bg-violet-400',
    icon: '🪣',
    label: 'Ozone Volume',
  },
  hdfs_path: {
    bg: 'bg-[#1e1600]',
    border: 'border-[#4a3a00]',
    dot: 'bg-amber-400',
    icon: '📁',
    label: 'HDFS Path',
  },
} as const;

// Icon map for lucide-react components
export const TYPE_ICONS = {
  kafka_topic: MessageSquare,
  iceberg_table: Database,
  ozone_volume: HardDrive,
  hdfs_path: FolderOpen,
} as const;

// Pipeline tool color mapping (Tailwind text utility)
export const PIPELINE_COLORS: Record<string, string> = {
  'NiFi': 'text-[#0088CC]',
  'Flink SQL': 'text-cyan-400',
  'Kafka Connect': 'text-blue-400',
  'Spark Streaming': 'text-sky-400',
};

// PII field name detection set — checked against field names to flag sensitive data
export const PII_NAMES = new Set([
  'email',
  'ssn',
  'social_security',
  'phone',
  'dob',
  'date_of_birth',
  'member_id',
  'patient_id',
  'credit_card',
]);

// Quality check status visual configuration
export const QC_STATUS_CONFIG = {
  pass: {
    icon: '✓',
    color: 'text-green-400',
    bg: 'bg-green-950/30',
    border: 'border-green-800/40',
    label: 'Pass',
  },
  warn: {
    icon: '!',
    color: 'text-amber-400',
    bg: 'bg-amber-950/30',
    border: 'border-amber-800/40',
    label: 'Warn',
  },
  fail: {
    icon: '✗',
    color: 'text-red-400',
    bg: 'bg-red-950/30',
    border: 'border-red-800/40',
    label: 'Fail',
  },
  info: {
    icon: 'i',
    color: 'text-blue-400',
    bg: 'bg-blue-950/30',
    border: 'border-blue-800/40',
    label: 'Info',
  },
} as const;

// Map quality score to text color (responsive color based on score value)
export function getScoreColor(score: number): string {
  if (score >= 80) return 'text-green-400';
  if (score >= 60) return 'text-amber-400';
  return 'text-red-400';
}

// Map quality score to progress bar color (responsive background)
export function getScoreBarColor(score: number): string {
  if (score >= 80) return 'bg-green-500';
  if (score >= 60) return 'bg-amber-500';
  return 'bg-red-500';
}

// Map quality score to human-readable grade label
export function getScoreLabel(score: number): string {
  if (score >= 90) return 'Excellent';
  if (score >= 80) return 'Good';
  if (score >= 60) return 'Fair';
  if (score >= 40) return 'Poor';
  return 'Critical';
}
