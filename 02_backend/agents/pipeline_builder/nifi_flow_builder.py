"""
Real NiFi flow-definition builder.

Produces a NiFi 1.x versioned-flow JSON (same format used by NiFi Registry / Cloudera
DataFlow) that can be imported into NiFi via UI ("Upload Flow Definition") or the
REST endpoint POST /process-groups/{id}/process-groups/upload.

Supported combinations:

    Source                          Sink
    ------                          ------
    kafka_topic                     adls_iceberg | adls_delta | snowflake
    iceberg_table  (Ozone-backed)   adls_iceberg | adls_delta | snowflake

All processor `type` values, bundle coords, and property keys are the real ones
shipped with Apache NiFi 1.23.x / Cloudera Flow Management. Sensitive values
(`account.key`, `snowflake.password`, etc.) are bound to a Parameter Context so
they show up in NiFi as `#{param.name}` placeholders the operator fills in.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any


# ─── NiFi component coordinates (NAR bundles) ────────────────────────────────
# Pinned to NiFi 1.23.2 — the version Cloudera Flow Management 2.1.7+ ships.
NIFI_VERSION = "1.23.2"

BUNDLE_STANDARD     = {"group": "org.apache.nifi", "artifact": "nifi-standard-nar",       "version": NIFI_VERSION}
BUNDLE_KAFKA_2_6    = {"group": "org.apache.nifi", "artifact": "nifi-kafka-2-6-nar",      "version": NIFI_VERSION}
BUNDLE_RECORD_SERDE = {"group": "org.apache.nifi", "artifact": "nifi-record-serialization-services-nar", "version": NIFI_VERSION}
BUNDLE_AVRO         = {"group": "org.apache.nifi", "artifact": "nifi-avro-nar",           "version": NIFI_VERSION}
BUNDLE_AZURE        = {"group": "org.apache.nifi", "artifact": "nifi-azure-nar",          "version": NIFI_VERSION}
BUNDLE_ICEBERG      = {"group": "org.apache.nifi", "artifact": "nifi-iceberg-nar",        "version": NIFI_VERSION}
BUNDLE_SNOWFLAKE    = {"group": "org.apache.nifi", "artifact": "nifi-snowflake-nar",      "version": NIFI_VERSION}
BUNDLE_PARQUET      = {"group": "org.apache.nifi", "artifact": "nifi-parquet-nar",        "version": NIFI_VERSION}


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _uuid() -> str:
    return str(uuid.uuid4())


@dataclass
class FlowContext:
    """Mutable bag passed to template fns — collects all components for one flow."""
    pg_id: str = field(default_factory=_uuid)
    pc_id: str = field(default_factory=_uuid)
    processors: list[dict] = field(default_factory=list)
    connections: list[dict] = field(default_factory=list)
    controller_services: list[dict] = field(default_factory=list)
    parameters: dict[str, dict] = field(default_factory=dict)
    next_x: int = 80
    next_y: int = 80

    def add_param(self, name: str, sensitive: bool = False, description: str = "") -> str:
        """Register a parameter and return its `#{name}` reference."""
        self.parameters[name] = {
            "name": name,
            "description": description,
            "sensitive": sensitive,
            "provided": False,
            "value": "",
        }
        return f"#{{{name}}}"

    def add_processor(
        self,
        name: str,
        proc_type: str,
        bundle: dict,
        properties: dict[str, str],
        auto_terminate: list[str] | None = None,
    ) -> str:
        pid = _uuid()
        self.processors.append({
            "identifier": pid,
            "instanceIdentifier": pid,
            "name": name,
            "comments": "",
            "type": proc_type,
            "bundle": bundle,
            "properties": properties,
            "propertyDescriptors": {},
            "schedulingPeriod": "0 sec",
            "schedulingStrategy": "TIMER_DRIVEN",
            "executionNode": "ALL",
            "penaltyDuration": "30 sec",
            "yieldDuration": "1 sec",
            "bulletinLevel": "WARN",
            "runDurationMillis": 25,
            "concurrentlySchedulableTaskCount": 1,
            "autoTerminatedRelationships": auto_terminate or [],
            "position": {"x": self.next_x, "y": self.next_y},
            "componentType": "PROCESSOR",
            "groupIdentifier": self.pg_id,
            "scheduledState": "ENABLED",
            "retryCount": 10,
            "retriedRelationships": [],
            "backoffMechanism": "PENALIZE_FLOWFILE",
            "maxBackoffPeriod": "10 mins",
            "style": {},
        })
        self.next_y += 200
        return pid

    def add_connection(self, src_id: str, dst_id: str, relationships: list[str]) -> None:
        cid = _uuid()
        self.connections.append({
            "identifier": cid,
            "instanceIdentifier": cid,
            "name": "",
            "source": {
                "id": src_id,
                "type": "PROCESSOR",
                "groupId": self.pg_id,
                "name": "",
                "comments": "",
                "instanceIdentifier": src_id,
            },
            "destination": {
                "id": dst_id,
                "type": "PROCESSOR",
                "groupId": self.pg_id,
                "name": "",
                "comments": "",
                "instanceIdentifier": dst_id,
            },
            "selectedRelationships": relationships,
            "labelIndex": 1,
            "zIndex": 0,
            "backPressureObjectThreshold": 10000,
            "backPressureDataSizeThreshold": "1 GB",
            "flowFileExpiration": "0 sec",
            "prioritizers": [],
            "bends": [],
            "loadBalanceStrategy": "DO_NOT_LOAD_BALANCE",
            "loadBalanceCompression": "DO_NOT_COMPRESS",
            "loadBalancePartitionAttribute": "",
            "componentType": "CONNECTION",
            "groupIdentifier": self.pg_id,
        })

    def add_controller_service(
        self,
        name: str,
        svc_type: str,
        bundle: dict,
        properties: dict[str, str],
    ) -> str:
        sid = _uuid()
        self.controller_services.append({
            "identifier": sid,
            "instanceIdentifier": sid,
            "name": name,
            "comments": "",
            "type": svc_type,
            "bundle": bundle,
            "properties": properties,
            "propertyDescriptors": {},
            "controllerServiceApis": [],
            "componentType": "CONTROLLER_SERVICE",
            "groupIdentifier": self.pg_id,
            "scheduledState": "ENABLED",
            "bulletinLevel": "WARN",
        })
        return sid


# ─── Avro schema helper ──────────────────────────────────────────────────────

_AVRO_TYPE_MAP = {
    "string": "string",
    "int": "int",
    "integer": "int",
    "long": "long",
    "bigint": "long",
    "float": "float",
    "double": "double",
    "boolean": "boolean",
    "bool": "boolean",
    "timestamp": {"type": "long", "logicalType": "timestamp-millis"},
    "date":      {"type": "int",  "logicalType": "date"},
    "bytes":     "bytes",
}


def schema_to_avro(record_name: str, schema: list[dict] | dict | None) -> dict:
    """Convert a Source Scout schema (list of {name, type}) to an Avro record schema."""
    if not schema:
        return {"type": "record", "name": record_name, "fields": [
            {"name": "payload", "type": ["null", "string"], "default": None}
        ]}
    fields = schema if isinstance(schema, list) else schema.get("fields", [])
    out = []
    for f in fields:
        col_name = f.get("name") or f.get("kafka_field") or f.get("column")
        col_type = (f.get("type") or "string").lower()
        avro_type = _AVRO_TYPE_MAP.get(col_type, "string")
        out.append({"name": col_name, "type": ["null", avro_type], "default": None})
    return {"type": "record", "name": record_name, "namespace": "com.cloudera.builder", "fields": out}


# ─── Source builders ─────────────────────────────────────────────────────────

def build_kafka_source(ctx: FlowContext, topic: str, group_id: str, avro_schema: dict) -> tuple[str, str]:
    """Add ConsumeKafkaRecord_2_6 + Avro reader/writer services. Returns (processor_id, writer_svc_id)."""
    schema_text = json.dumps(avro_schema)

    reader_svc = ctx.add_controller_service(
        name="AvroReader",
        svc_type="org.apache.nifi.avro.AvroReader",
        bundle=BUNDLE_AVRO,
        properties={
            "schema-access-strategy": "schema-text-property",
            "schema-text": schema_text,
        },
    )
    writer_svc = ctx.add_controller_service(
        name="AvroWriter",
        svc_type="org.apache.nifi.avro.AvroRecordSetWriter",
        bundle=BUNDLE_AVRO,
        properties={
            "Schema Write Strategy": "no-schema",
            "schema-access-strategy": "schema-text-property",
            "schema-text": schema_text,
            "Compression Format": "snappy",
        },
    )

    brokers_param = ctx.add_param("kafka.brokers", description="Comma-separated Kafka bootstrap servers")
    ctx.add_param("kafka.sasl.username", description="Kafka SASL username (if SASL_PLAINTEXT/SSL)")
    ctx.add_param("kafka.sasl.password", sensitive=True, description="Kafka SASL password")

    pid = ctx.add_processor(
        name=f"Consume {topic}",
        proc_type="org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6",
        bundle=BUNDLE_KAFKA_2_6,
        properties={
            "bootstrap.servers": brokers_param,
            "topic": topic,
            "topic_type": "names",
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "record-reader": reader_svc,
            "record-writer": writer_svc,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "#{kafka.sasl.username}",
            "sasl.password": "#{kafka.sasl.password}",
            "honor-transactions": "true",
            "message-demarcator": "",
            "max.poll.records": "10000",
        },
        auto_terminate=["parse.failure"],
    )
    return pid, writer_svc


def build_iceberg_source(ctx: FlowContext, namespace: str, table: str) -> str:
    """Read an existing Iceberg table via QueryDatabaseTableRecord against an Impala/Hive JDBC."""
    jdbc_url    = ctx.add_param("source.jdbc.url",  description="JDBC URL of the source Iceberg query engine (e.g. Impala/Hive)")
    jdbc_user   = ctx.add_param("source.jdbc.user", description="JDBC user")
    jdbc_passwd = ctx.add_param("source.jdbc.password", sensitive=True, description="JDBC password")
    jdbc_driver = ctx.add_param("source.jdbc.driver.class", description="JDBC driver class (e.g. com.cloudera.impala.jdbc41.Driver)")
    jdbc_jars   = ctx.add_param("source.jdbc.driver.jars",  description="Path to JDBC driver .jar files (semicolon-separated)")

    dbcp_svc = ctx.add_controller_service(
        name="SourceDBCPConnectionPool",
        svc_type="org.apache.nifi.dbcp.DBCPConnectionPool",
        bundle=BUNDLE_STANDARD,
        properties={
            "Database Connection URL":    jdbc_url,
            "Database Driver Class Name": jdbc_driver,
            "database-driver-locations":  jdbc_jars,
            "Database User":              jdbc_user,
            "Password":                   jdbc_passwd,
            "Max Wait Time":              "500 millis",
            "Max Total Connections":      "8",
        },
    )
    record_writer = ctx.add_controller_service(
        name="JsonRecordSetWriter",
        svc_type="org.apache.nifi.json.JsonRecordSetWriter",
        bundle=BUNDLE_RECORD_SERDE,
        properties={
            "Schema Write Strategy":  "no-schema",
            "schema-access-strategy": "inherit-record-schema",
            "Output Grouping":        "array",
        },
    )

    pid = ctx.add_processor(
        name=f"Query {namespace}.{table}",
        proc_type="org.apache.nifi.processors.standard.QueryDatabaseTableRecord",
        bundle=BUNDLE_STANDARD,
        properties={
            "Database Connection Pooling Service": dbcp_svc,
            "db-fetch-table-name":  f"{namespace}.{table}",
            "qdbtr-record-writer":  record_writer,
            "Max Wait Time":        "0 seconds",
            "Fetch Size":           "10000",
            "Max Rows Per Flow File": "10000",
        },
    )
    return pid


# ─── Sink builders ───────────────────────────────────────────────────────────

def build_sink_adls_iceberg(ctx: FlowContext, table_namespace: str, table_name: str) -> str:
    """PutIceberg writing to an Iceberg table whose warehouse lives on ADLS."""
    catalog_uri      = ctx.add_param("iceberg.catalog.uri",      description="Iceberg REST catalog URI (e.g. https://catalog.cloudera.com/iceberg)")
    warehouse_path   = ctx.add_param("iceberg.warehouse.path",   description="ADLS warehouse path (abfss://container@account.dfs.core.windows.net/warehouse)")
    adls_account     = ctx.add_param("adls.account.name",        description="Azure storage account name")
    adls_account_key = ctx.add_param("adls.account.key", sensitive=True, description="Azure storage account key")

    catalog_svc = ctx.add_controller_service(
        name="IcebergRESTCatalog",
        svc_type="org.apache.nifi.processors.iceberg.catalog.IcebergCatalogService",
        bundle=BUNDLE_ICEBERG,
        properties={
            "catalog-type":              "rest",
            "catalog-uri":               catalog_uri,
            "warehouse-location":        warehouse_path,
            "io-impl":                   "org.apache.iceberg.azure.adlsv2.ADLSFileIO",
            "adls.account.name":         adls_account,
            "adls.account.key":          adls_account_key,
        },
    )

    pid = ctx.add_processor(
        name=f"Put Iceberg {table_namespace}.{table_name}",
        proc_type="org.apache.nifi.processors.iceberg.PutIceberg",
        bundle=BUNDLE_ICEBERG,
        properties={
            "catalog-service":          catalog_svc,
            "catalog-namespace":        table_namespace,
            "table-name":               table_name,
            "file-format":              "PARQUET",
            "record-reader":            "AvroReader",   # bound at runtime; same reader as source
            "unmatched-column-behavior": "IGNORE_UNMATCHED_COLUMNS",
            "maximum-file-size":        "536870912",
            "number-of-commit-retries": "10",
        },
        auto_terminate=["failure"],
    )
    return pid


def build_sink_adls_delta(ctx: FlowContext, container: str, target_path: str) -> str:
    """Write Parquet files to ADLS at a Delta-ready path. A Databricks AUTO LOADER / OPTIMIZE job converts to Delta."""
    adls_account     = ctx.add_param("adls.account.name", description="Azure storage account name")
    adls_account_key = ctx.add_param("adls.account.key", sensitive=True, description="Azure storage account key")
    adls_filesystem  = ctx.add_param("adls.filesystem",   description=f"ADLS Gen2 filesystem / container (default: {container})")

    # First convert records → Parquet
    parquet_writer = ctx.add_controller_service(
        name="ParquetRecordSetWriter",
        svc_type="org.apache.nifi.parquet.ParquetRecordSetWriter",
        bundle=BUNDLE_PARQUET,
        properties={
            "Schema Write Strategy":  "no-schema",
            "schema-access-strategy": "inherit-record-schema",
            "compression-type":       "SNAPPY",
        },
    )

    convert_pid = ctx.add_processor(
        name="Convert to Parquet",
        proc_type="org.apache.nifi.processors.standard.ConvertRecord",
        bundle=BUNDLE_STANDARD,
        properties={
            "record-reader": "AvroReader",
            "record-writer": parquet_writer,
        },
        auto_terminate=["failure"],
    )

    creds_svc = ctx.add_controller_service(
        name="ADLSCredentialsService",
        svc_type="org.apache.nifi.services.azure.storage.ADLSCredentialsControllerService",
        bundle=BUNDLE_AZURE,
        properties={
            "storage-account-name": adls_account,
            "storage-account-key":  adls_account_key,
        },
    )

    put_pid = ctx.add_processor(
        name="Put ADLS (Delta-ready Parquet)",
        proc_type="org.apache.nifi.processors.azure.storage.PutAzureDataLakeStorage",
        bundle=BUNDLE_AZURE,
        properties={
            "adls-credentials-service": creds_svc,
            "filesystem-name":           adls_filesystem,
            "directory-name":            target_path,
            "file-name":                 "${filename:append('.parquet')}",
            "conflict-resolution-strategy": "replace",
            "writing-strategy":          "writeAndRename",
        },
        auto_terminate=["failure"],
    )
    ctx.add_connection(convert_pid, put_pid, ["success"])
    return convert_pid  # caller wires the source → convert; convert → put already wired


def build_sink_snowflake(ctx: FlowContext, database: str, schema: str, table: str) -> str:
    """Snowflake ingest via PutSnowflakeInternalStage + StartSnowflakeIngest (Snowpipe)."""
    sf_account   = ctx.add_param("snowflake.account",     description="Snowflake account identifier (e.g. xy12345.us-east-1)")
    sf_user      = ctx.add_param("snowflake.user",        description="Snowflake user")
    sf_password  = ctx.add_param("snowflake.password",    sensitive=True, description="Snowflake password (or set private-key params)")
    sf_role      = ctx.add_param("snowflake.role",        description="Snowflake role")
    sf_warehouse = ctx.add_param("snowflake.warehouse",   description="Snowflake virtual warehouse")
    sf_pipe      = ctx.add_param("snowflake.pipe",        description=f"Snowpipe name targeting {database}.{schema}.{table}")

    conn_svc = ctx.add_controller_service(
        name="SnowflakeConnectionProvider",
        svc_type="org.apache.nifi.processors.snowflake.SnowflakeComputingConnectionPool",
        bundle=BUNDLE_SNOWFLAKE,
        properties={
            "snowflake-url":       sf_account,
            "snowflake-user":      sf_user,
            "snowflake-password":  sf_password,
            "snowflake-role":      sf_role,
            "snowflake-warehouse": sf_warehouse,
            "snowflake-database":  database,
            "snowflake-schema":    schema,
        },
    )

    stage_pid = ctx.add_processor(
        name="Put Snowflake Internal Stage",
        proc_type="org.apache.nifi.processors.snowflake.PutSnowflakeInternalStage",
        bundle=BUNDLE_SNOWFLAKE,
        properties={
            "snowflake-connection-provider": conn_svc,
            "stage":                          f"@%{table}",
            "database":                       database,
            "schema":                         schema,
        },
        auto_terminate=["failure"],
    )
    ingest_pid = ctx.add_processor(
        name="Start Snowflake Ingest",
        proc_type="org.apache.nifi.processors.snowflake.StartSnowflakeIngest",
        bundle=BUNDLE_SNOWFLAKE,
        properties={
            "snowflake-connection-provider": conn_svc,
            "snowflake-pipe":                sf_pipe,
        },
        auto_terminate=["failure"],
    )
    ctx.add_connection(stage_pid, ingest_pid, ["success"])
    return stage_pid


# ─── Flow assembly ───────────────────────────────────────────────────────────

SOURCES = ("kafka_topic", "iceberg_table")
SINKS   = ("adls_iceberg", "adls_delta", "snowflake")


def build_flow(
    *,
    source: dict[str, Any],
    sink: dict[str, Any],
    flow_name: str | None = None,
) -> dict[str, Any]:
    """
    source: {"type": "kafka_topic" | "iceberg_table", "name": "...", "schema": [...], ...}
    sink:   {"type": "adls_iceberg" | "adls_delta" | "snowflake", ...sink params}

    Returns a complete NiFi flow-definition JSON ready to be saved as `flow.json`
    and uploaded into NiFi.
    """
    src_type = source.get("type")
    sink_type = sink.get("type")
    if src_type not in SOURCES:
        raise ValueError(f"unsupported source.type={src_type!r}; supported: {SOURCES}")
    if sink_type not in SINKS:
        raise ValueError(f"unsupported sink.type={sink_type!r}; supported: {SINKS}")

    ctx = FlowContext()

    # ── Source ────────────────────────────────────────────────────────────
    if src_type == "kafka_topic":
        topic = source["name"]
        avro  = schema_to_avro(topic.replace("-", "_"), source.get("schema"))
        group_id = source.get("group_id", f"nifi-{topic.replace('.', '_')}-consumer")
        src_pid, _ = build_kafka_source(ctx, topic, group_id, avro)
    else:
        namespace, _, table = source["name"].partition(".")
        if not table:
            namespace, table = "default", source["name"]
        src_pid = build_iceberg_source(ctx, namespace, table)

    # ── Sink ──────────────────────────────────────────────────────────────
    if sink_type == "adls_iceberg":
        ns = sink.get("namespace", "default")
        tb = sink.get("table") or source["name"].split(".")[-1].replace("-", "_")
        sink_entry_pid = build_sink_adls_iceberg(ctx, ns, tb)
    elif sink_type == "adls_delta":
        container = sink.get("container", "lakehouse")
        target    = sink.get("path") or f"delta/{source['name'].replace('.', '/')}"
        sink_entry_pid = build_sink_adls_delta(ctx, container, target)
    else:  # snowflake
        db   = sink.get("database", "RAW")
        sch  = sink.get("schema", "PUBLIC")
        tbl  = sink.get("table") or source["name"].split(".")[-1].upper()
        sink_entry_pid = build_sink_snowflake(ctx, db, sch, tbl)

    ctx.add_connection(src_pid, sink_entry_pid, ["success"])

    flow_name = flow_name or f"{src_type}__{source.get('name','source')}__to__{sink_type}"
    pc_name = f"{flow_name}-params"

    return {
        "flowEncodingVersion": "1.0",
        "flowContents": {
            "identifier": ctx.pg_id,
            "instanceIdentifier": ctx.pg_id,
            "name": flow_name,
            "comments": f"Generated by Cloudera AI Agents — Pipeline Builder.\nSource: {source}\nSink: {sink}",
            "position": {"x": 0, "y": 0},
            "processGroups": [],
            "remoteProcessGroups": [],
            "processors": ctx.processors,
            "inputPorts": [],
            "outputPorts": [],
            "connections": ctx.connections,
            "labels": [],
            "funnels": [],
            "controllerServices": ctx.controller_services,
            "variables": {},
            "parameterContextName": pc_name,
            "defaultFlowFileExpiration": "0 sec",
            "defaultBackPressureObjectThreshold": 10000,
            "defaultBackPressureDataSizeThreshold": "1 GB",
            "componentType": "PROCESS_GROUP",
            "flowFileConcurrency": "UNBOUNDED",
            "flowFileOutboundPolicy": "STREAM_WHEN_AVAILABLE",
        },
        "externalControllerServices": {},
        "parameterContexts": {
            pc_name: {
                "identifier": ctx.pc_id,
                "name": pc_name,
                "description": "Fill these in via NiFi UI → Parameter Contexts before starting the flow.",
                "parameters": list(ctx.parameters.values()),
                "inheritedParameterContexts": [],
                "componentType": "PARAMETER_CONTEXT",
            }
        },
        "latest": True,
    }


def build_flow_summary(flow: dict[str, Any]) -> dict[str, Any]:
    """Concise summary for the UI / decision log."""
    fc = flow["flowContents"]
    return {
        "flow_name": fc["name"],
        "processor_count": len(fc["processors"]),
        "processors": [p["name"] for p in fc["processors"]],
        "controller_service_count": len(fc["controllerServices"]),
        "controller_services": [s["name"] for s in fc["controllerServices"]],
        "connection_count": len(fc["connections"]),
        "parameter_count": sum(len(pc["parameters"]) for pc in flow["parameterContexts"].values()),
        "parameters_to_fill": [
            {"name": p["name"], "sensitive": p["sensitive"], "description": p["description"]}
            for pc in flow["parameterContexts"].values()
            for p in pc["parameters"]
        ],
    }
