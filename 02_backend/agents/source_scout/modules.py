"""
Module Map: each entry is a self-contained reasoning unit.
  keywords      — terms in the goal that activate this module
  prompt_snippet — injected into the system prompt when active
  json_fields   — extra fields the LLM must add to its JSON response
"""
from dataclasses import dataclass


@dataclass
class Module:
    keywords: list[str]
    prompt_snippet: str
    json_fields: dict


MODULE_MAP: dict[str, Module] = {
    "pii": Module(
        keywords=["pii", "sensitive", "privacy", "gdpr", "personal data",
                  "personally identifiable", "email", "phone", "ssn",
                  "person", "name", "names", "customer", "people"],
        prompt_snippet="""\
## PII Analysis
Identify every field that could contain personally identifiable information
(name, email, phone, SSN, DOB, address, member_id, patient_id, credit_card).
For each PII field specify: field name, PII category, and recommended masking
strategy (hash | tokenise | redact | encrypt).""",
        json_fields={
            "pii_fields": [{"field": "<name>", "category": "<type>", "masking": "<strategy>"}],
            "pii_risk_level": "none|low|medium|high",
        },
    ),

    "cost": Module(
        keywords=["cost", "storage cost", "expensive", "optimize storage",
                  "budget", "size", "reduce storage"],
        prompt_snippet="""\
## Cost Optimisation
Estimate monthly storage cost based on schema width and typical data volume.
Recommend the best file format (Parquet / ORC / Avro), compression codec,
and partition strategy to minimise cost on Cloudera.""",
        json_fields={
            "estimated_cost_usd_month": 0.0,
            "recommended_format": "Parquet|ORC|Avro",
            "partition_recommendation": "<e.g. partition by date(created_at)>",
            "optimization_tips": ["<tip>"],
        },
    ),

    "freshness": Module(
        keywords=["fresh", "freshness", "stale", "latency", "lag",
                  "real-time", "realtime", "delay", "how old", "up to date"],
        prompt_snippet="""\
## Data Freshness
Assess how current the data is and the appropriate ingestion cadence.
Choose between streaming, micro-batch, or daily batch based on the source
type and schema signals. State the recommended SLA in minutes.""",
        json_fields={
            "freshness_sla_minutes": 0,
            "ingestion_mode": "streaming|micro-batch|daily-batch",
            "staleness_risk": "low|medium|high",
        },
    ),

    "pipeline": Module(
        keywords=["pipeline", "ingest", "etl", "flow", "nifi",
                  "flink", "spark", "connect", "connector", "how to load"],
        prompt_snippet="""\
## Detailed Pipeline Configuration
Provide a concrete pipeline spec: connector type, parallelism, checkpoint
interval, error-handling strategy (dead-letter-queue / retry / skip),
and the target Iceberg table format.""",
        json_fields={
            "connector_config": {
                "type": "<connector>",
                "parallelism": 1,
                "checkpoint_interval_s": 60,
            },
            "error_handling": "dead-letter-queue|retry|skip",
            "target_format": "Iceberg|Delta|Hudi",
        },
    ),

    "dataquality": Module(
        keywords=["quality", "data quality", "dq", "validation", "anomaly",
                  "null", "drift", "schema drift", "bad data", "corrupt"],
        prompt_snippet="""\
## Data Quality Assessment
Identify the top data-quality risks: nulls, type mismatches, duplicates,
schema drift. Suggest specific dbt tests or Great Expectations checks that
should run before writing to Iceberg.""",
        json_fields={
            "quality_risks": ["<risk description>"],
            "suggested_checks": ["<check description>"],
            "schema_drift_risk": "low|medium|high",
        },
    ),
}
