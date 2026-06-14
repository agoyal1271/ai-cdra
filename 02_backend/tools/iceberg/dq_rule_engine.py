"""Semantic data quality rule engine for Iceberg tables.

Infers DQ rules from column name + type combination.
Generates Impala-compliant validation SQL.
"""

import re
import os
import logging
from typing import Optional
import uuid
from datetime import datetime

logger = logging.getLogger(__name__)


# ============================================================================
# Type Helpers — recognize column types from Iceberg/Avro type strings
# ============================================================================

def _is_numeric(col_type: str) -> bool:
    """True for int, long, float, double, decimal, numeric."""
    return any(k in col_type.lower() for k in ("int", "long", "float", "double", "decimal", "numeric"))


def _is_string(col_type: str) -> bool:
    """True for string, varchar, char, text."""
    return any(k in col_type.lower() for k in ("string", "varchar", "char", "text"))


def _is_temporal(col_type: str) -> bool:
    """True for timestamp, date, time."""
    return any(k in col_type.lower() for k in ("timestamp", "date", "time"))


# ============================================================================
# SQL Template Builder
# ============================================================================

def _violation_sql(table_name: str, col: str, rule_name: str, violation_condition: str) -> str:
    """Build a single-row SELECT that counts violations and computes violation percentage.

    Returns Impala-compatible SQL with backtick-quoted column names.
    """
    return (
        f"SELECT '{rule_name}' AS check_name, '{col}' AS column_name,\n"
        f"  COUNT(*) AS total_rows,\n"
        f"  COUNT(CASE WHEN {violation_condition} THEN 1 END) AS violation_count,\n"
        f"  CAST(COUNT(CASE WHEN {violation_condition} THEN 1 END) AS DOUBLE)\n"
        f"    / CAST(COUNT(*) AS DOUBLE) * 100 AS violation_pct\n"
        f"FROM {table_name}\n"
        f"WHERE `{col}` IS NOT NULL"
    )


# ============================================================================
# Rule Definition Templates — keyed by rule type
# ============================================================================

_RULE_TEMPLATES = {
    "email_validation": {
        "description": "Must be valid email format",
        "violation_expr": lambda col: f"`{col}` NOT REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{{2,}}$'",
        "threshold_warn_pct": 1,
        "threshold_fail_pct": 5,
    },
    "ipv4_validation": {
        "description": "Must be valid IPv4 address",
        "violation_expr": lambda col: f"`{col}` NOT REGEXP '^([0-9]{{1,3}}\\\\.)'{{3}}[0-9]{{1,3}}$'",
        "threshold_warn_pct": 1,
        "threshold_fail_pct": 5,
    },
    "mac_validation": {
        "description": "Must be valid MAC address",
        "violation_expr": lambda col: f"`{col}` NOT REGEXP '^([0-9A-Fa-f]{{2}}[:-]){{5}}[0-9A-Fa-f]{{2}}$'",
        "threshold_warn_pct": 1,
        "threshold_fail_pct": 5,
    },
    "non_empty": {
        "description": "Must not be empty",
        "violation_expr": lambda col: f"TRIM(`{col}`) = ''",
        "threshold_warn_pct": 1,
        "threshold_fail_pct": 10,
    },
    "not_future": {
        "description": "Timestamp must not be in the future",
        "violation_expr": lambda col: f"`{col}` > now()",
        "threshold_warn_pct": 1,
        "threshold_fail_pct": 5,
    },
    "not_too_old": {
        "description": "Timestamp must not pre-date year 2000",
        "violation_expr": lambda col: f"`{col}` < '2000-01-01'",
        "threshold_warn_pct": 1,
        "threshold_fail_pct": 5,
    },
    "latitude_range": {
        "description": "Latitude must be in [-90, 90]",
        "violation_expr": lambda col: f"`{col}` < -90 OR `{col}` > 90",
        "threshold_warn_pct": 1,
        "threshold_fail_pct": 5,
    },
    "longitude_range": {
        "description": "Longitude must be in [-180, 180]",
        "violation_expr": lambda col: f"`{col}` < -180 OR `{col}` > 180",
        "threshold_warn_pct": 1,
        "threshold_fail_pct": 5,
    },
    "port_range": {
        "description": "Port must be in [1, 65535]",
        "violation_expr": lambda col: f"`{col}` < 1 OR `{col}` > 65535",
        "threshold_warn_pct": 1,
        "threshold_fail_pct": 5,
    },
    "age_range": {
        "description": "Age must be in [0, 150]",
        "violation_expr": lambda col: f"`{col}` < 0 OR `{col}` > 150",
        "threshold_warn_pct": 1,
        "threshold_fail_pct": 5,
    },
    "non_negative": {
        "description": "Must be >= 0",
        "violation_expr": lambda col: f"`{col}` < 0",
        "threshold_warn_pct": 1,
        "threshold_fail_pct": 5,
    },
    "percentage_range": {
        "description": "Must be in [0, 100]",
        "violation_expr": lambda col: f"`{col}` < 0 OR `{col}` > 100",
        "threshold_warn_pct": 1,
        "threshold_fail_pct": 5,
    },
    "not_null": {
        "description": "Must not be null",
        "violation_expr": lambda col: f"`{col}` IS NULL",
        "threshold_warn_pct": 1,
        "threshold_fail_pct": 10,
    },
}


# ============================================================================
# Semantic Column Interpretation — infer what a column represents from name + type
# ============================================================================

def _infer_rules_from_column(col_name: str, col_type: str) -> list[str]:
    """Infer applicable DQ rules from column name + type.

    Returns list of rule keys that should be applied.
    """
    col_lower = col_name.lower()
    rules = []

    # Email-like columns
    if any(k in col_lower for k in ["email", "mail", "contact"]):
        if _is_string(col_type):
            rules.append("email_validation")
            return rules

    # IP address columns
    if any(k in col_lower for k in ["ip", "ipaddr", "ip_address", "source_ip", "dest_ip"]):
        if _is_string(col_type):
            rules.append("ipv4_validation")
            return rules

    # MAC address columns
    if any(k in col_lower for k in ["mac", "mac_address"]):
        if _is_string(col_type):
            rules.append("mac_validation")
            return rules

    # Port columns
    if any(k in col_lower for k in ["port", "_port"]):
        if _is_numeric(col_type):
            rules.append("port_range")
            return rules

    # Latitude columns
    if any(k in col_lower for k in ["lat", "latitude"]):
        if _is_numeric(col_type):
            rules.append("latitude_range")
            return rules

    # Longitude columns
    if any(k in col_lower for k in ["lon", "lng", "longitude"]):
        if _is_numeric(col_type):
            rules.append("longitude_range")
            return rules

    # Age columns
    if any(k in col_lower for k in ["age"]) and not any(k in col_lower for k in ["cage", "page", "stage"]):
        if _is_numeric(col_type):
            rules.append("age_range")
            return rules

    # Percentage/ratio columns
    if any(k in col_lower for k in ["percent", "percentage", "pct", "ratio", "rate", "completion"]):
        if _is_numeric(col_type):
            rules.append("percentage_range")
            return rules

    # Financial amount columns
    if any(k in col_lower for k in ["amount", "price", "cost", "revenue", "salary", "balance", "fee"]):
        if _is_numeric(col_type):
            rules.append("non_negative")
            return rules

    # Counter/count columns
    if any(k in col_lower for k in ["count", "total", "num", "quantity"]):
        if _is_numeric(col_type):
            rules.append("non_negative")
            return rules

    # Temporal columns
    if any(k in col_lower for k in ["timestamp", "created", "updated", "modified", "date", "time", "event_time", "_at"]):
        if _is_temporal(col_type):
            rules.extend(["not_future", "not_too_old"])
            return rules

    # Default rules based on type alone
    if _is_string(col_type):
        rules.append("non_empty")
    elif _is_numeric(col_type):
        rules.append("non_negative")
    elif _is_temporal(col_type):
        rules.extend(["not_future", "not_too_old"])

    return rules


# ============================================================================
# Public Functions
# ============================================================================

def generate_semantic_dq_rules(
    table_name: str,
    fields: list[dict],
) -> list[dict]:
    """Infer and generate DQ rules from column names + types.

    Args:
        table_name: Fully qualified Iceberg table name (e.g. 'demo.network_monitoring')
        fields: List of field dicts: [{"name": str, "type": str}, ...]

    Returns:
        List of rule dicts with keys:
          rule_name, column, description, domain, check_type,
          impala_sql, threshold_warn_pct, threshold_fail_pct
    """
    rules = []
    seen_rule_keys: set = set()

    for field in fields:
        col = field.get("name", "")
        col_type = field.get("type", "string")

        # Infer what rules apply based on column name + type
        rule_keys = _infer_rules_from_column(col, col_type)

        for rule_key in rule_keys:
            # Skip if rule template doesn't exist
            if rule_key not in _RULE_TEMPLATES:
                continue

            # Deduplication: same column + same rule → skip
            key = f"{col}::{rule_key}"
            if key in seen_rule_keys:
                continue
            seen_rule_keys.add(key)

            # Build rule name
            rule_name = f"{col}__{rule_key}"

            # Get rule definition
            rule_def = _RULE_TEMPLATES[rule_key]

            # Build violation condition
            violation_condition = rule_def["violation_expr"](col)

            # Build full SELECT
            sql = _violation_sql(table_name, col, rule_name, violation_condition)

            rules.append({
                "rule_name": rule_name,
                "column": col,
                "description": rule_def["description"],
                "domain": rule_key,  # rule type is the domain
                "check_type": "validation",
                "impala_sql": sql,
                "threshold_warn_pct": rule_def.get("threshold_warn_pct", 1),
                "threshold_fail_pct": rule_def.get("threshold_fail_pct", 5),
            })

    logger.info(
        f"[dq_rule_engine] table={table_name} fields={len(fields)} rules_generated={len(rules)}"
    )
    return rules


def _get_impala_conn():
    """Create Impala connection via Knox. Uses same pattern as quality_code_gen.py."""
    from impala.dbapi import connect as impala_connect

    return impala_connect(
        host=os.getenv("KNOX_HOST", "localhost"),
        port=8443,
        use_http_transport=True,
        http_path="gateway/cdp-proxy-api/impala/",
        auth_mechanism="LDAP",
        user=os.getenv("KNOX_USER", "admin"),
        password=os.getenv("KNOX_PASSWORD", ""),
        timeout=30,
    )


def execute_semantic_dq_rules(
    table_name: str,
    fields: list[dict],
) -> list[dict]:
    """Generate rules, execute each via Impala, write to dq_metric_result_react, return results.

    Synchronous (wraps impyla). Caller must use asyncio.to_thread.

    Returns:
        List of result dicts per rule:
          rule_name, column, description, domain, check_type,
          total_rows, violation_count, violation_pct,
          status ('pass' | 'warn' | 'fail' | 'error'),
          threshold_warn_pct, threshold_fail_pct, impala_sql
    """
    rules = generate_semantic_dq_rules(table_name, fields)
    if not rules:
        return []

    run_id = str(uuid.uuid4())

    try:
        conn = _get_impala_conn()
        cursor = conn.cursor()
    except Exception as e:
        logger.error(f"[dq_rule_engine] Impala connection failed: {e}")
        raise

    # Create results table if not exists
    try:
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS default.dq_metric_result_react ("
            "  run_id STRING, run_timestamp TIMESTAMP, table_name STRING,"
            "  check_name STRING, column_name STRING, metric_value DOUBLE,"
            "  metric_label STRING, status STRING, threshold_warn DOUBLE,"
            "  threshold_fail DOUBLE, engine STRING, agent_name STRING,"
            "  impala_sql STRING"
            ") STORED AS ICEBERG"
            "  LOCATION 's3a://iceberg-warehouse/warehouse/default.db/dq_metric_result_react';"
        )
        logger.debug("[dq_rule_engine] Ensured dq_metric_result_react table exists")
    except Exception as e:
        logger.warning(f"[dq_rule_engine] Could not create results table: {e}")

    results = []
    insert_rows = []

    for rule in rules:
        try:
            cursor.execute(rule["impala_sql"])
            row = cursor.fetchone()

            if row is None:
                # Empty table — all rules pass trivially
                results.append({
                    **rule,
                    "total_rows": 0,
                    "violation_count": 0,
                    "violation_pct": 0.0,
                    "status": "pass",
                })
                insert_rows.append((
                    run_id, datetime.utcnow().isoformat(), table_name,
                    rule["rule_name"], rule["column"], 0.0,
                    "0.0% violations", "pass", rule["threshold_warn_pct"], rule["threshold_fail_pct"],
                    "impala", "source_scout_react", rule["impala_sql"]
                ))
                continue

            col_names = [d[0] for d in cursor.description] if cursor.description else []
            row_dict = dict(zip(col_names, row))

            total_rows = int(row_dict.get("total_rows", 0) or 0)
            violation_count = int(row_dict.get("violation_count", 0) or 0)
            violation_pct = float(row_dict.get("violation_pct", 0.0) or 0.0)

            warn_t = rule["threshold_warn_pct"]
            fail_t = rule["threshold_fail_pct"]

            if violation_pct >= fail_t:
                status = "fail"
            elif violation_pct >= warn_t:
                status = "warn"
            else:
                status = "pass"

            results.append({
                **rule,
                "total_rows": total_rows,
                "violation_count": violation_count,
                "violation_pct": round(violation_pct, 2),
                "status": status,
            })

            insert_rows.append((
                run_id, datetime.utcnow().isoformat(), table_name,
                rule["rule_name"], rule["column"], violation_pct,
                f"{violation_pct:.1f}% violations", status,
                rule["threshold_warn_pct"], rule["threshold_fail_pct"],
                "impala", "source_scout_react", rule["impala_sql"]
            ))

        except Exception as e:
            logger.warning(f"[dq_rule_engine] Rule {rule['rule_name']} failed: {e}")
            results.append({
                **rule,
                "total_rows": None,
                "violation_count": None,
                "violation_pct": None,
                "status": "error",
                "error": str(e),
            })

    # Batch INSERT all results (if we have any)
    if insert_rows:
        try:
            rows_values = []
            for row in insert_rows:
                sql_escaped = row[12].replace(chr(10), ' ').replace("'", "''")
                values = (
                    f"('{row[0]}', '{row[1]}', '{row[2]}', '{row[3]}', '{row[4]}', "
                    f"{row[5]}, '{row[6]}', '{row[7]}', {row[8]}, {row[9]}, "
                    f"'{row[10]}', '{row[11]}', '{sql_escaped}')"
                )
                rows_values.append(values)

            insert_sql = (
                f"INSERT INTO default.dq_metric_result_react"
                f" (run_id, run_timestamp, table_name, check_name, column_name,"
                f"  metric_value, metric_label, status, threshold_warn, threshold_fail,"
                f"  engine, agent_name, impala_sql)"
                f" VALUES "
                + ", ".join(rows_values)
            )
            cursor.execute(insert_sql)
            logger.info(f"[dq_rule_engine] Inserted {len(insert_rows)} DQ results into dq_metric_result_react")
        except Exception as e:
            logger.warning(f"[dq_rule_engine] Could not insert results: {e}")

    try:
        conn.close()
    except Exception:
        pass

    return results
