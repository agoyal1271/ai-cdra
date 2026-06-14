"""
Prompt Orchestrator — builds LLM system prompts dynamically from the Module Map.

Flow:
  1. detect_active_modules(goal) → scans goal for module keywords
  2. assemble_system_prompt(...)  → stitches BASE + active snippets + merged JSON schema
  3. parse_llm_response(raw)      → extracts JSON from raw LLM output

Filtering logic (time, name patterns) stays in agent.py — this module only
handles LLM reasoning prompts.
"""
import json
import logging

from agents.source_scout.modules import MODULE_MAP, Module

logger = logging.getLogger(__name__)

# ── Base schema every response must include ───────────────────────────────────
BASE_SCHEMA: dict = {
    "asset_name": "string — exact name of the asset",
    "summary": "one sentence: what data this source contains",
    "recommended_pipeline": "NiFi | Flink SQL | Kafka Connect | Spark Streaming | Spark Batch",
    "reasoning": "why this pipeline suits this source type and schema",
}

BASE_SNIPPET = """\
You are a Cloudera data platform expert.
Analyse the data source described below and produce a pipeline recommendation.
Respond ONLY with compact JSON — no markdown, no explanation outside the JSON."""


# ── Module detection ──────────────────────────────────────────────────────────
def detect_active_modules(goal: str) -> list[tuple[str, Module]]:
    """Returns (name, Module) pairs whose keywords appear in goal."""
    g = goal.lower()
    active = [(name, mod) for name, mod in MODULE_MAP.items()
              if any(kw in g for kw in mod.keywords)]
    if active:
        logger.debug(f"[orchestrator] active modules: {[n for n, _ in active]}")
    else:
        logger.debug("[orchestrator] no modules matched — using base prompt only")
    return active


# ── Prompt assembly ───────────────────────────────────────────────────────────
def assemble_system_prompt(
    goal: str,
    asset_type: str,
    asset_name: str,
    schema_info: dict,
) -> tuple[str, list[str]]:
    """
    Returns (system_prompt, active_module_names).

    The prompt is:
      BASE_SNIPPET
      [goal context line — only when modules are active]
      [module snippet 1]
      [module snippet 2]
      ...
      Asset description + merged JSON schema
    """
    active = detect_active_modules(goal)
    active_names = [name for name, _ in active]

    # Merge JSON schema: base + all active module extensions
    merged_schema: dict = dict(BASE_SCHEMA)
    for _, mod in active:
        merged_schema.update(mod.json_fields)

    parts = [BASE_SNIPPET]

    if active:
        parts.append(f"Goal context from user: {goal}")

    for _, mod in active:
        parts.append(mod.prompt_snippet)

    parts.append(
        f"Asset type   : {asset_type}\n"
        f"Asset name   : {asset_name}\n"
        f"Schema/meta  : {json.dumps(schema_info, default=str)}\n\n"
        f"Respond ONLY with JSON that matches this schema "
        f"(include all module-specific fields shown):\n"
        f"{json.dumps(merged_schema, indent=2)}"
    )

    prompt = "\n\n".join(parts)
    logger.debug(
        f"[orchestrator] assembled prompt for {asset_name!r} "
        f"({len(prompt)} chars, modules={active_names})"
    )
    return prompt, active_names


# ── Response parsing ──────────────────────────────────────────────────────────
def parse_llm_response(raw: str) -> dict:
    """Extracts and parses the JSON object from a raw LLM response."""
    clean = raw.strip()
    if "```" in clean:
        clean = clean.split("```", 1)[1].split("```")[0].lstrip("json").strip()
    s, e = clean.find("{"), clean.rfind("}") + 1
    if s != -1 and e > s:
        clean = clean[s:e]
    try:
        return json.loads(clean)
    except Exception:
        logger.warning(f"[orchestrator] JSON parse failed on: {raw[:120]!r}")
        return {
            "asset_name": "",
            "summary": raw[:200],
            "recommended_pipeline": "Spark Batch",
            "reasoning": "Could not parse LLM response as JSON.",
        }
