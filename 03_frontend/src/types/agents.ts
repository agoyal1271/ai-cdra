export type AgentStatus = 'active' | 'coming_soon' | 'running' | 'error';

export interface Agent {
  id: string;
  name: string;
  role: string;
  tagline: string;
  description: string;
  status: AgentStatus;
  tools: string[];
  icon: string;
}

export type AssetType = 'kafka_topic' | 'iceberg_table' | 'ozone_volume' | 'hdfs_path';

export interface PipelineSuggestion {
  recommended_pipeline: string;
  reasoning: string;
  target_format: string;
  target_location: string;
  key_considerations: string[];
  sample_config_hint?: string;
  source_type: string;
  source_name: string;
  confidence_score?: number;
  relevance_reason?: string;
  connector_config?: string | Record<string, unknown>;
  active_modules?: string[];
}

export interface OmLineageNode {
  id: string;
  name: string;
  fqn: string;
  entity_type: string;
  description: string;
  service: string;
}

export interface OmLineage {
  entity: { id: string; name: string; fqn: string; entity_type: string };
  upstream: OmLineageNode[];
  downstream: OmLineageNode[];
  edge_count: number;
}

export interface DiscoveredAsset {
  id: string;
  asset_type: AssetType;
  name: string;
  lineage?: OmLineage;
  metadata: Record<string, unknown> & {
    namespace?: string;
    sr_info?: { namespace?: string };
    row_count?: number | string;
    estimated_messages?: number | string;
    freshness_estimate?: string;
    age_hours?: number;
    fields?: Array<{ name: string; type?: string }>;
    schema?: { fields?: Array<{ name: string; type?: string }> };
  };
  pii_risk: boolean;
  pipeline_suggestion: PipelineSuggestion;
}

export type SSEEventType = 'thought' | 'asset' | 'asset_update' | 'warning' | 'scan_ready' | 'complete' | 'error' | 'stub' | 'stream_end' | 'tool_call' | 'tool_result' | 'lineage';

export interface SSEEvent {
  type: SSEEventType;
  agent: string;
  source?: string;
  content?: string;
  asset_type?: AssetType;
  data?: DiscoveredAsset;
  summary?: string;
  counts?: Record<string, number>;
  message?: string;
}

export interface HealthStatus {
  status: 'ok' | 'degraded';
  services: Record<string, { status: string; note?: string; version?: string; jobs?: number }>;
}

export interface QualityCheckCode {
  table_name: string;
  impala_sql: string;
  trino_sql: string;
  spark_script: string;
  results_table: string;
  error?: string;
}

export type QualityCheckStatus = 'pass' | 'warn' | 'fail' | 'info';

export interface QualityCheckRow {
  check_name: string;
  column_name: string | null;
  metric_value: number;
  metric_label: string;
  status: QualityCheckStatus;
  threshold_warn?: number;
  threshold_fail?: number;
}

export interface QualityCheckResults {
  table_name: string;
  last_run: string | null;
  run_id: string;
  overall_score: number;
  checks: QualityCheckRow[];
  error?: string;
}

export interface FieldMapping {
  kafka_field: string;
  kafka_type: string;
  iceberg_column: string;
  iceberg_type: string;
  flink_type?: string;
  nullable: boolean;
  pii_risk: boolean;
}

export interface ConsumerLagInfo {
  group_id: string;
  lag: number;
}

export interface NiFiProcessorProperties {
  [key: string]: string;
}

export interface DeploymentOption {
  name: string;
  description: string;
  instructions: string;
}

export interface NiFiFlowConfig {
  readyflow_name: string;
  parameters: Record<string, string>;
  processor_properties: {
    ConsumeKafka: NiFiProcessorProperties;
    PutIceberg: NiFiProcessorProperties;
  };
  deploy_url: string;
  deployment_options: Record<string, DeploymentOption>;
}

export interface PipelineResult {
  topic: string;
  target: 'iceberg' | 'delta';
  schema_map: FieldMapping[];
  nifi_flow: NiFiFlowConfig;
  flink_sql: string;
  iceberg_ddl: string;
  connect_config: string;
  connector_name: string;
  consumer_lag: ConsumerLagInfo | null;
  pii_fields: FieldMapping[];
}
