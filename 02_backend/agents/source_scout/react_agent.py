"""
Source Scout ReAct Agent — LLM reasoning + vector search foundations.

Two core functions:
1. reason() — LLM generates Thought/Action/Action Input given message history
2. semantic_search() — Qdrant vector search for candidate assets
"""

import asyncio
import logging
import re
import json
from typing import Optional, Dict, Set, Tuple, AsyncGenerator

from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage

logger = logging.getLogger(__name__)


REACT_SYSTEM_PROMPT = """You are a data discovery agent for a Cloudera data platform.
Your goal is to help find relevant data assets based on user queries.

Respond in this exact format with no deviation:
Thought: <your reasoning about what to do next>
Action: <tool_name>
Action Input: <JSON object or null>

Available tools:
- semantic_search: Find assets in the catalog via vector search. Input: null
- search_kafka: List all Kafka topics from schema registry. Input: null
- search_iceberg: List all Iceberg tables from the catalog. Input: null
- search_ozone: List all Ozone storage volumes. Input: null
- get_topic_schema: Get detailed schema for a specific Kafka topic. Input: {"topic": "<name>"}
- get_table_schema: Get detailed schema for a specific Iceberg table. Input: {"table": "<name>"}
- generate_dq_rules: Preview semantic data quality rules for an Iceberg table (no execution). Input: {"table": "<db.table>"}
- execute_dq_rules: Run semantic DQ rules against Impala, returns violation counts. Input: {"table": "<db.table>"}
- finish: Return final results with discovered assets. Input: {"assets": [...], "summary": "..."}

Strategy:
1. Start with semantic_search to find relevant assets quickly
2. If semantic_search returns results, examine them with get_*_schema tools
3. If semantic_search returns 0 results, search ALL sources exhaustively:
   - search_kafka (Kafka topics with field schemas shown)
   - search_iceberg (Iceberg tables with column schemas shown)
   - search_ozone (Ozone volumes)
4. Filter results by examining which assets match the user's goal (look for matching field/column names)
5. For discovered Iceberg tables: use generate_dq_rules (preview) or execute_dq_rules (live Impala check)
6. Use finish when you have found all relevant assets or checked their quality

Important: After semantic_search returns 0, you MUST search kafka, iceberg, and ozone before finishing."""


async def reason(messages: list) -> Tuple[str, str, Optional[Dict]]:
    """
    LLM reasoning step: given message history, return Thought/Action/Action Input.

    Args:
        messages: list of SystemMessage and HumanMessage from langchain_core.messages

    Returns:
        (thought, action, action_input) tuple
    """
    from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY

    try:
        llm = ChatOpenAI(
            base_url=LLM_BASE_URL,
            model=LLM_MODEL,
            api_key=LLM_API_KEY,
            temperature=0.2,
        )
        response = await llm.ainvoke(messages)
        thought, action, action_input = parse_react_response(response.content)
        return thought, action, action_input
    except Exception as e:
        logger.error(f"[react] LLM reasoning failed: {e}")
        raise


def parse_react_response(text: str) -> Tuple[str, str, Optional[Dict]]:
    """
    Parse LLM response into Thought / Action / Action Input.

    Expected format:
        Thought: <reasoning>
        Action: <tool_name>
        Action Input: <json or null>

    Returns:
        (thought, action, action_input) with action_input as dict or None
    """
    thought = ""
    action = ""
    action_input = None

    # Extract Thought
    thought_match = re.search(r"Thought:\s*(.+?)(?=Action:|$)", text, re.DOTALL)
    if thought_match:
        thought = thought_match.group(1).strip()

    # Extract Action
    action_match = re.search(r"Action:\s*(\w+)", text)
    if action_match:
        action = action_match.group(1).strip()

    # Extract Action Input
    input_match = re.search(r"Action Input:\s*(.+?)(?=\n\n|$)", text, re.DOTALL)
    if input_match:
        input_str = input_match.group(1).strip()
        if input_str.lower() == "null":
            action_input = None
        else:
            try:
                action_input = json.loads(input_str)
            except json.JSONDecodeError:
                logger.debug(f"[react] Could not parse action input as JSON: {input_str}")
                action_input = {"raw": input_str}

    return thought, action, action_input


async def semantic_search(goal: str) -> Dict[str, Set[str]]:
    """
    Vector search for candidate assets in the data catalog.

    Returns dict with structure:
        {
            "kafka": {topic_names},
            "iceberg": {table_names},
            "ozone": {volume_names}
        }
    """
    try:
        from tools.catalog import catalog_store

        # Check if catalog is available
        stats = await asyncio.to_thread(catalog_store.get_stats)
        if not stats.get("available") or stats.get("total", 0) == 0:
            logger.debug("[react] Catalog unavailable")
            return {}

        # Search all asset types
        asset_types = ["kafka_topic", "iceberg_table", "ozone_volume"]
        results = await asyncio.to_thread(
            catalog_store.search, goal, asset_types, 50
        )

        if not results:
            logger.debug(f"[react] No semantic search results for: {goal}")
            return {}

        # Organize by source type
        prefilter: Dict[str, Set[str]] = {}
        for r in results:
            atype = r.get("asset_type", "")
            name = r.get("name", "")
            if not name:
                continue

            if atype == "kafka_topic":
                prefilter.setdefault("kafka", set()).add(name)
            elif atype == "iceberg_table":
                prefilter.setdefault("iceberg", set()).add(name)
            elif atype == "ozone_volume":
                prefilter.setdefault("ozone", set()).add(name)

        logger.debug(f"[react] Semantic search found {sum(len(v) for v in prefilter.values())} assets")
        return prefilter

    except Exception as e:
        logger.debug(f"[react] Semantic search failed: {e}")
        return {}


async def run_source_scout_react(goal: str) -> AsyncGenerator[dict, None]:
    """
    Simple ReAct loop demonstrating LLM reasoning + vector search.

    Yields SSE events showing the reasoning trace.
    """
    def emit(event_type: str, **kwargs) -> dict:
        return {"type": event_type, "agent": "source_scout_react", **kwargs}

    # Initialize message history with system prompt
    messages = [
        SystemMessage(content=REACT_SYSTEM_PROMPT),
        HumanMessage(content=f"Goal: {goal}"),
    ]

    max_iterations = 15
    iteration = 0

    while iteration < max_iterations:
        iteration += 1

        # LLM Reasoning Step
        try:
            thought, action, action_input = await reason(messages)
            yield emit("thought", content=thought, iteration=iteration)
        except Exception as e:
            yield emit("error", content=f"Reasoning failed: {e}")
            return

        # Check for termination
        if action == "finish":
            assets = (action_input or {}).get("assets", [])
            summary = (action_input or {}).get("summary", "Discovery complete.")
            yield emit("complete", summary=summary, assets_count=len(assets))

            # Fetch OpenMetadata lineage for each discovered asset (non-blocking)
            try:
                from tools.openmetadata.client import health_check, get_lineage_by_name
                if health_check():
                    for asset in assets:
                        name = asset.get("name", "") or asset.get("id", "")
                        atype = "topic" if asset.get("type") == "kafka_topic" else "table"
                        lineage = await asyncio.to_thread(get_lineage_by_name, name, atype)
                        if lineage and (lineage.get("upstream") or lineage.get("downstream")):
                            yield emit("lineage", asset=name, **{
                                k: lineage[k] for k in ("entity", "upstream", "downstream", "edge_count")
                            })
            except Exception as _le:
                logger.debug(f"[react] OM lineage fetch skipped: {_le}")
            return

        # Execute Action
        yield emit("action", tool=action, input=action_input, iteration=iteration)

        observation = ""
        try:
            if action == "semantic_search":
                result = await semantic_search(goal)
                total = sum(len(v) for v in result.values())
                observation = f"Found {total} assets: {result}"

            elif action == "search_kafka":
                from tools.kafka.kafka_tools import get_all_topics_from_schema_registry
                topics_dict = await asyncio.to_thread(get_all_topics_from_schema_registry)
                topic_details = []
                for topic_name, info in topics_dict.items():
                    fields = info.get("fields", [])
                    field_names = [f.get("name", "") for f in fields]
                    topic_details.append(f"{topic_name}: {field_names}")
                observation = f"Found {len(topics_dict)} Kafka topics:\n" + "\n".join(topic_details[:15])
                if len(topic_details) > 15:
                    observation += f"\n... and {len(topic_details)-15} more topics"

            elif action == "search_iceberg":
                from tools.iceberg.iceberg_tools import list_iceberg_tables
                tables = await asyncio.to_thread(list_iceberg_tables)
                table_details = []
                for table in tables:
                    name = table.get("name", "")
                    schema = table.get("schema", {})
                    columns = schema.get("columns", [])
                    col_names = [c.get("name", "") for c in columns]
                    table_details.append(f"{name}: {col_names}")
                observation = f"Found {len(tables)} Iceberg tables:\n" + "\n".join(table_details[:15])
                if len(table_details) > 15:
                    observation += f"\n... and {len(table_details)-15} more tables"

            elif action == "search_ozone":
                from tools.ozone.ozone_tools import list_ozone_volumes
                volumes = await asyncio.to_thread(list_ozone_volumes)
                volume_names = [v.get("name", "") for v in volumes]
                observation = f"Found {len(volume_names)} Ozone volumes: {volume_names}"

            elif action == "get_topic_schema":
                topic = (action_input or {}).get("topic", "")
                if not topic:
                    observation = "Error: 'topic' parameter required in Action Input"
                else:
                    from tools.kafka.kafka_tools import get_all_topics_from_schema_registry
                    topics_dict = await asyncio.to_thread(get_all_topics_from_schema_registry)
                    sr_info = topics_dict.get(topic, {})
                    observation = f"Schema for {topic}: {sr_info}"

            elif action == "get_table_schema":
                table = (action_input or {}).get("table", "")
                if not table:
                    observation = "Error: 'table' parameter required in Action Input"
                else:
                    from tools.iceberg.iceberg_tools import describe_iceberg_table
                    schema = await asyncio.to_thread(describe_iceberg_table, table)
                    observation = f"Schema for {table}: {schema}"

            elif action == "generate_dq_rules":
                table = (action_input or {}).get("table", "")
                if not table:
                    observation = "Error: 'table' parameter required in Action Input"
                else:
                    from tools.iceberg.iceberg_tools import describe_iceberg_table
                    from tools.iceberg.dq_rule_engine import generate_semantic_dq_rules
                    schema = await asyncio.to_thread(describe_iceberg_table, table)
                    fields = schema.get("fields", [])
                    rules = generate_semantic_dq_rules(table, fields)
                    if not rules:
                        observation = f"No semantic DQ rules matched any column in {table}."
                    else:
                        lines = [f"  [{r['domain']}] {r['rule_name']}: {r['description']}" for r in rules[:15]]
                        observation = f"Generated {len(rules)} semantic DQ rules for {table}:\n" + "\n".join(lines)
                        if len(rules) > 15:
                            observation += f"\n  ... and {len(rules)-15} more rules"

            elif action == "execute_dq_rules":
                table = (action_input or {}).get("table", "")
                if not table:
                    observation = "Error: 'table' parameter required in Action Input"
                else:
                    from tools.iceberg.iceberg_tools import describe_iceberg_table
                    from tools.iceberg.dq_rule_engine import execute_semantic_dq_rules
                    try:
                        schema = await asyncio.wait_for(
                            asyncio.to_thread(describe_iceberg_table, table),
                            timeout=15
                        )
                        fields = schema.get("fields", [])
                        results = await asyncio.wait_for(
                            asyncio.to_thread(execute_semantic_dq_rules, table, fields),
                            timeout=60
                        )
                        if not results:
                            observation = f"No semantic DQ rules matched any column in {table}."
                        else:
                            pass_ct = sum(1 for r in results if r["status"] == "pass")
                            warn_ct = sum(1 for r in results if r["status"] == "warn")
                            fail_ct = sum(1 for r in results if r["status"] == "fail")
                            err_ct  = sum(1 for r in results if r["status"] == "error")
                            lines = [f"DQ results for {table}: {pass_ct} PASS, {warn_ct} WARN, {fail_ct} FAIL, {err_ct} ERROR ({len(results)} rules)"]
                            for r in results[:10]:
                                viol, total, pct = r.get("violation_count"), r.get("total_rows"), r.get("violation_pct")
                                status_str = r['status'].upper()
                                if viol is not None:
                                    lines.append(f"[{status_str}] {r['rule_name']}: {viol}/{total} violations ({pct:.1f}%)")
                                    sql = r['impala_sql'].replace(chr(10), ' ')
                                    lines.append(f"  SQL: {sql[:300]}{'...' if len(sql) > 300 else ''}")
                                else:
                                    lines.append(f"[ERROR] {r['rule_name']}: {r.get('error', 'unknown')}")
                            if len(results) > 10:
                                lines.append(f"... and {len(results)-10} more rules")
                            observation = "\n".join(lines)
                            yield emit("dq_results", table=table, results=results, iteration=iteration)
                    except asyncio.TimeoutError:
                        observation = f"DQ rule execution timed out after 60s for {table}. Impala connection may be unavailable."
                    except Exception as e:
                        observation = f"DQ rule execution failed: {str(e)}"

            else:
                observation = f"Unknown action: '{action}'. Available: semantic_search, search_kafka, search_iceberg, search_ozone, get_topic_schema, get_table_schema, generate_dq_rules, execute_dq_rules, finish."

            yield emit("observation", tool=action, summary=observation, iteration=iteration)
        except Exception as e:
            observation = f"Error executing {action}: {str(e)}"
            yield emit("observation", tool=action, summary=observation, error=True, iteration=iteration)

        # Update message history with observation
        messages.append(AIMessage(content=f"Thought: {thought}\nAction: {action}\nAction Input: {action_input}"))
        messages.append(HumanMessage(content=f"Observation: {observation}"))

    # Max iterations reached — force finish
    yield emit("complete", summary=f"Discovery complete after {max_iterations} iterations.", assets_count=0)
