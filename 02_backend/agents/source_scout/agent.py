"""
Source Scout agent — execution only.

All reasoning (filtering, module detection, prompt assembly) is delegated
to ScoutSidecar. This file only handles: scan, filter via sidecar, emit, LLM call.
"""
import asyncio
import logging
from typing import AsyncGenerator, Optional, Dict, Set, List, Tuple

from langchain_core.messages import AIMessage
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage

from agents.source_scout.sidecar import ScoutSidecar, get_valid_knox_token
from agents.state import AgentState
from tools.kafka.kafka_tools import get_all_topics_from_schema_registry
from tools.iceberg.iceberg_tools import list_iceberg_tables, describe_iceberg_table, TokenExpiredError
from tools.ozone.ozone_tools import list_ozone_volumes

logger = logging.getLogger(__name__)

PII_KEYWORDS = {"email", "ssn", "social_security", "phone", "dob",
                "date_of_birth", "member_id", "patient_id", "credit_card"}


def _has_pii_risk(fields: List[dict]) -> bool:
    return bool({f.get("name", "").lower() for f in fields} & PII_KEYWORDS)


# ── Source scanners (pure I/O, no reasoning) ──────────────────────────────────

async def _maybe_refresh_sr_cache() -> None:
    """Triggers a synchronous SR re-index if the cache is stale. Called once per run."""
    from config import SCHEMA_REGISTRY_URL
    if not SCHEMA_REGISTRY_URL:
        return
    try:
        from tools.kafka.schema_registry_cache import is_stale
        from tools.kafka.schema_registry_indexer import run_index
        if is_stale():
            logger.info("[agent] SR cache is stale — re-indexing before Kafka scan")
            await asyncio.to_thread(run_index)
    except Exception as e:
        logger.debug(f"[agent] SR cache refresh skipped: {e}")


async def _scan_kafka(names_filter: Set[str] = None) -> List[dict]:
    from config import SCHEMA_REGISTRY_URL
    await _maybe_refresh_sr_cache()

    # Primary path: use SR cache as the topic source — no broker connection required.
    # SR has every registered topic with exact Avro/JSON schema fields.
    sr_topics: dict = {}
    if SCHEMA_REGISTRY_URL:
        sr_topics = await asyncio.to_thread(get_all_topics_from_schema_registry)

    if sr_topics:
        if names_filter:
            sr_topics = {k: v for k, v in sr_topics.items() if k in names_filter}

        assets = []
        for topic_name, sr_info in sr_topics.items():
            # Strip SR subject suffixes — topic name should never include -value/-key
            clean_name = topic_name
            if clean_name.endswith("-value"):
                clean_name = clean_name[:-6]
            elif clean_name.endswith("-key"):
                clean_name = clean_name[:-4]
            schema = {
                "fields": sr_info["fields"],
                "format": sr_info["schema_type"],
                "field_count": sr_info["field_count"],
                "source": "schema_registry",
            }
            assets.append({
                "id": f"kafka::{clean_name}",
                "asset_type": "kafka_topic",
                "name": clean_name,
                "metadata": {"schema": schema, "sr_info": sr_info},
                "pii_risk": _has_pii_risk(schema.get("fields", [])),
                "pipeline_suggestion": None,
            })
        return assets

    return []


async def _scan_iceberg(names_filter: Set[str] = None) -> List[dict]:
    if names_filter:
        # Targeted load — describe only the filtered tables, skip full catalog walk
        tables = []
        for name in names_filter:
            try:
                tbl = await asyncio.to_thread(describe_iceberg_table, name)
                tables.append(tbl)
            except Exception as e:
                logger.debug(f"[agent] skip iceberg {name}: {e}")
    else:
        try:
            tables = await asyncio.to_thread(list_iceberg_tables)
        except TokenExpiredError:
            logger.warning("[agent] Knox JWT expired during Iceberg scan — refreshing token and retrying")
            await asyncio.to_thread(get_valid_knox_token)
            tables = await asyncio.to_thread(list_iceberg_tables)

    return [{
        "id": f"iceberg::{t['name']}",
        "asset_type": "iceberg_table",
        "name": t["name"],
        "metadata": t,
        "pii_risk": _has_pii_risk(t.get("fields", [])),
        "pipeline_suggestion": None,
    } for t in tables]


async def _scan_ozone() -> List[dict]:
    volumes = await asyncio.to_thread(list_ozone_volumes)
    assets = [{
        "id": f"ozone::{v.get('name', 'unknown')}",
        "asset_type": "ozone_volume",
        "name": v.get("name", "unknown"),
        "metadata": {**v, "object_count": None, "total_size_bytes": None, "formats": []},
        "pii_risk": False,
        "pipeline_suggestion": None,
    } for v in volumes]
    try:
        from tools.catalog import catalog_store
        catalog_store.index_ozone_volumes(volumes)
    except Exception:
        pass
    return assets




async def _catalog_search(goal: str, sources: Set[str]) -> Optional[Dict[str, Set[str]]]:
    """
    Query ChromaDB catalog for semantically relevant assets before running full scans.
    Returns {source_key: {asset_name, ...}} pre-filter dict, or None if catalog unavailable.
    """
    try:
        from tools.catalog import catalog_store
        stats = await asyncio.to_thread(catalog_store.get_stats)
        if not stats.get("available") or stats.get("total", 0) == 0:
            return None

        asset_types = []
        if "kafka" in sources:
            asset_types.append("kafka_topic")
        if "iceberg" in sources:
            asset_types.append("iceberg_table")
        if "ozone" in sources:
            asset_types.append("ozone_volume")

        results = await asyncio.to_thread(
            catalog_store.search, goal, asset_types or None, 50
        )
        if not results:
            return None

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

        return prefilter if prefilter else None
    except Exception as e:
        logger.debug(f"[agent] catalog search failed: {e}")
        return None


def _detect_sources(goal: str) -> Set[str]:
    g = goal.lower()
    sources = set()
    if any(w in g for w in ["kafka", "topic", "stream", "listen", "consume", "produce", "broker", "message", "field"]):
        sources.add("kafka")
    if any(w in g for w in ["iceberg", "table", "catalog", "schema", "column", "namespace", "snapshot"]):
        sources.add("iceberg")
    if any(w in g for w in ["ozone", "volume", "bucket", "object storage", "s3", "blobs"]):
        sources.add("ozone")
    return sources or {"kafka", "iceberg", "ozone"}


# ── LLM suggestion (execution only — prompt built by sidecar) ─────────────────

async def _add_suggestion(asset: dict, sidecar: ScoutSidecar) -> dict:
    from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY

    atype = asset["asset_type"]
    name  = asset["name"]
    meta  = asset["metadata"]

    schema_info = {
        "kafka_topic":   lambda: meta.get("schema", {}),
        "iceberg_table": lambda: {"fields": meta.get("fields", [])},
        "ozone_volume":  lambda: {"formats": meta.get("formats", []), "object_count": meta.get("object_count", 0)},
    }.get(atype, lambda: {"type": meta.get("type"), "size_bytes": meta.get("size", 0)})()

    # Deterministic pre-checks — reject without LLM if schema provably doesn't match
    for check_fn, module_tag in [
        (sidecar.check_column_match,        "column_verifier"),
        (sidecar.check_semantic_field_match, "semantic_field_check"),
    ]:
        passes, reason = check_fn(schema_info)
        if not passes:
            logger.debug(f"[agent] {module_tag} rejected {name!r}: {reason}")
            return {**asset, "pipeline_suggestion": {
                "asset_name": name,
                "summary": f"{atype} — schema does not satisfy goal filter",
                "recommended_pipeline": "N/A",
                "reasoning": reason,
                "active_modules": [module_tag],
                "matches_criteria": False,
                "relevance_reason": reason,
            }}

    # Choose prompt strategy — sidecar decides, agent only executes
    if sidecar.has_active_modules():
        system_prompt, active_modules = sidecar.build_system_prompt(atype, name, schema_info)
    elif sidecar.is_goal_specific():
        system_prompt, active_modules = sidecar.build_intent_classifier_prompt(name, schema_info)
    else:
        system_prompt, active_modules = sidecar.build_system_prompt(atype, name, schema_info)

    try:
        llm = ChatOpenAI(base_url=LLM_BASE_URL, model=LLM_MODEL, api_key=LLM_API_KEY, temperature=0.2)
        response = await asyncio.to_thread(
            lambda: llm.invoke([SystemMessage(content=system_prompt), HumanMessage(content="Analyse this asset.")])
        )
        suggestion = sidecar.parse_llm_response(response.content)
        suggestion["active_modules"] = active_modules

        # Normalise confidence_score → matches_criteria boolean (threshold: 60)
        # The LLM returns confidence_score (0-100) from intent classifier or matches_criteria (bool) from module-based prompt.
        confidence_score = suggestion.get("confidence_score")
        matches_criteria = suggestion.get("matches_criteria")

        if confidence_score is not None:
            # Intent classifier: convert confidence to boolean at 70% threshold
            # 70+ = strong semantic match, <70 = false positive risk
            try:
                score = float(confidence_score)
                suggestion["matches_criteria"] = score >= 70
                suggestion["confidence_score"] = score
            except (ValueError, TypeError):
                # Malformed score — treat as unknown
                suggestion["matches_criteria"] = None
                suggestion["confidence_score"] = None
        elif matches_criteria is None:
            # Field missing — treat as unknown, do not discard
            suggestion["matches_criteria"] = None
        elif isinstance(matches_criteria, bool):
            pass  # already a bool, nothing to do
        else:
            # Covers string "false", "0", "no", and any truthy/falsy value
            suggestion["matches_criteria"] = (
                str(matches_criteria).strip().lower() not in ("false", "0", "no", "null", "none", "")
            )

    except Exception as e:
        logger.warning(f"[agent] LLM failed for {name!r}: {e}")
        suggestion = {
            "asset_name": name, "summary": f"{atype} {name}",
            "recommended_pipeline": "Spark Batch", "reasoning": str(e),
            "active_modules": [],
            "matches_criteria": None,   # unknown — do not discard
        }

    return {**asset, "pipeline_suggestion": suggestion}


# ── Main run loop ─────────────────────────────────────────────────────────────

async def run_source_scout(goal: str, config: dict = None) -> AsyncGenerator[dict, None]:
    """Runs Source Scout and yields SSE-ready event dicts."""

    def emit(event_type: str, source: str = "all", **kwargs) -> dict:
        return {"type": event_type, "agent": "source_scout", "source": source, **kwargs}

    # Step 1: Parse intent with semantic cache (handles negation, synonyms, etc.)
    from tools.intent_parser import parse_intent_with_cache, intent_to_metadata_filters

    try:
        structured_intent = await parse_intent_with_cache(goal)
        yield emit("thought", content=f"Intent parsed: {structured_intent}")
    except Exception as e:
        logger.warning(f"[agent] intent parsing failed: {e}, using default")
        structured_intent = {
            "asset_types": ["iceberg_table", "kafka_topic", "ozone_volume"],
            "storage": None,
            "format": None,
            "required_fields": [],
            "pii_only": False,
            "time_filter": None,
        }

    # Step 2: Boot sidecar — all reasoning lives here from this point forward
    sidecar = ScoutSidecar(goal)

    # Override sidecar's metadata filters with parsed intent
    parsed_metadata_filters = intent_to_metadata_filters(structured_intent)
    if parsed_metadata_filters:
        sidecar._metadata_filters = parsed_metadata_filters
        sidecar._metadata_intent = True

    sources = _detect_sources(goal)
    # Optionally respect asset_types from intent
    intent_types = structured_intent.get("asset_types", [])
    if intent_types:
        sources = sources & set(
            {"kafka" if t == "kafka_topic" else "iceberg" if t == "iceberg_table" else "ozone"
             for t in intent_types}
        )
        sources = sources or {"kafka", "iceberg", "ozone"}  # Fallback to all if empty
    source_labels = " + ".join(sorted(sources)) if sources != {"kafka", "iceberg", "ozone"} else "all sources"
    yield emit("thought", content=f"Scanning {source_labels} — goal: \"{goal}\"")

    filter_summary = sidecar.get_filter_summary()
    if filter_summary != "no filters active":
        yield emit("thought", content=f"Sidecar filters: {filter_summary}")

    # ── Catalog pre-filter for specific goals ─────────────────────────────────
    # For targeted queries (field search, PII, etc.) query ChromaDB first so we
    # only describe the relevant assets instead of walking the whole catalog.
    catalog_filter: Dict[str, Set[str]] = {}
    if sidecar.is_goal_specific() or sidecar.has_active_modules():
        cf = await _catalog_search(goal, sources)
        if cf:
            catalog_filter = cf
            total_matches = sum(len(v) for v in cf.values())
            names_preview = ", ".join(
                n for names in cf.values() for n in list(names)[:3]
            )
            yield emit("thought", content=(
                f"Semantic catalog matched {total_matches} asset(s): {names_preview}"
                + ("…" if total_matches > 3 else "")
            ))

    # ── Parallel scans ────────────────────────────────────────────────────────
    async def _labeled(label: str, coro):
        try:    return label, await coro
        except Exception as e:
            logger.warning(f"{label} scan failed: {e}")
            return label, []

    pending = []
    if "kafka"   in sources:
        kf = catalog_filter.get("kafka")
        label_suffix = f" ({len(kf)} from catalog)" if kf else ""
        yield emit("thought", source="kafka_topic",   content=f"Loading Kafka topics from schema registry{label_suffix}")
        pending.append(asyncio.create_task(_labeled("kafka",   _scan_kafka(kf))))
    if "iceberg" in sources:
        iff = catalog_filter.get("iceberg")
        label_suffix = f" ({len(iff)} from catalog)" if iff else ""
        yield emit("thought", source="iceberg_table", content=f"Querying Iceberg REST catalog via Knox{label_suffix}")
        pending.append(asyncio.create_task(_labeled("iceberg", _scan_iceberg(iff))))
    if "ozone"   in sources:
        yield emit("thought", source="ozone_volume",  content="Connecting to Apache Ozone S3-compatible endpoint — listing volumes")
        pending.append(asyncio.create_task(_labeled("ozone",   _scan_ozone())))

    buckets: Dict[str, List] = {}
    for done in asyncio.as_completed(pending):
        label, assets = await done
        buckets[label] = assets

    kafka_assets   = buckets.get("kafka",   [])
    iceberg_assets = buckets.get("iceberg", [])
    ozone_assets   = buckets.get("ozone",   [])

    # ── Metadata intent-based filtering (FIRST routing decision) ──────────────
    has_metadata_intent, metadata_filters = sidecar.get_metadata_intent_and_filters()
    if has_metadata_intent and metadata_filters:
        filter_str = ", ".join(
            f"{k}≠{v[1:]}" if v.startswith("!") else f"{k}={v}"
            for k, v in metadata_filters.items()
        )
        yield emit("thought", content=f"Query is metadata-focused: {filter_str}")

        from tools.intent_extractor import normalize_asset_metadata

        all_assets_to_filter = kafka_assets + iceberg_assets + ozone_assets
        metadata_matched = []
        metadata_rejected = []

        for asset in all_assets_to_filter:
            metadata = asset.get("metadata", {})
            asset_type = asset.get("asset_type", "")

            # Normalize asset metadata
            normalized = normalize_asset_metadata(metadata, asset_type)

            # Check if all expected metadata properties match (with negation support)
            matches = True
            for key, expected_val in metadata_filters.items():
                actual_val = normalized.get(key, "").lower()

                # Support negation: "!ozone" means "NOT ozone"
                if expected_val.startswith("!"):
                    negated_val = expected_val[1:]
                    if actual_val == negated_val:  # If it IS the negated value, don't match
                        matches = False
                        break
                else:
                    # Positive filter: must match exactly
                    if actual_val != expected_val.lower():
                        matches = False
                        break

            if matches:
                metadata_matched.append(asset)
            else:
                metadata_rejected.append(asset)
                atype = asset.get("asset_type", "")
                # Format filter display with negation support
                filter_str = ", ".join(
                    f"{k}≠{v[1:]}" if v.startswith("!") else f"{k}={v}"
                    for k, v in metadata_filters.items()
                )
                yield emit("thought", source=atype,
                          content=f"  Skipping '{asset['name']}' — metadata doesn't match ({filter_str})")

        if metadata_rejected:
            yield emit("thought", content=f"Metadata filter rejected {len(metadata_rejected)} asset(s), {len(metadata_matched)} match criteria")

        # Update assets to only metadata-matching ones
        kafka_assets = [a for a in metadata_matched if a["asset_type"] == "kafka_topic"]
        iceberg_assets = [a for a in metadata_matched if a["asset_type"] == "iceberg_table"]
        ozone_assets = [a for a in metadata_matched if a["asset_type"] == "ozone_volume"]

    # ── Sidecar name/time filters — applied to Kafka and Iceberg ─────────────
    for _assets, _label in [(kafka_assets, "kafka_topic"), (iceberg_assets, "iceberg_table")]:
        if not _assets:
            continue
        kept: List[dict] = []
        dropped: List[Tuple] = []
        for a in _assets:
            ok, reason = sidecar.should_include_asset(a)
            if ok:
                kept.append(a)
            else:
                dropped.append((a, reason))
                logger.debug(f"[agent] SKIP {a['name']} — {reason}")
                yield emit("thought", source=_label,
                           content=f"  Skipping '{a['name']}' — {reason}")
        if dropped:
            yield emit("thought", source=_label,
                       content=f"Sidecar filtered out {len(dropped)} asset(s), {len(kept)} remain")
        if _label == "kafka_topic":
            kafka_assets = kept
        else:
            iceberg_assets = kept

    # ── Per-source thought log ────────────────────────────────────────────────
    if kafka_assets:
        mock_note = " (mock — Kafka is Kerberized)" if kafka_assets[0].get("metadata", {}).get("mock") else ""
        yield emit("thought", source="kafka_topic",
                   content=f"Found {len(kafka_assets)} Kafka topics{mock_note}")
        for a in kafka_assets:
            fields = a.get("metadata", {}).get("schema", {}).get("fields", [])
            pii_note = " ⚠ PII" if a.get("pii_risk") else ""
            yield emit("thought", source="kafka_topic",
                       content=f"  topic/{a['name']}: [{', '.join(f['name'] for f in fields[:4])}]{pii_note}")

    if iceberg_assets:
        yield emit("thought", source="iceberg_table",
                   content=f"Found {len(iceberg_assets)} Iceberg table(s) — reading schemas and snapshot history")
        for a in iceberg_assets:
            fields = a.get("metadata", {}).get("fields", [])
            snaps  = a.get("metadata", {}).get("snapshots", 0)
            pii_note = " ⚠ PII" if a.get("pii_risk") else ""
            yield emit("thought", source="iceberg_table",
                       content=f"  table/{a['name']}: [{', '.join(f['name'] for f in fields[:4])}] — {snaps} snapshot(s){pii_note}")

    if ozone_assets:
        yield emit("thought", source="ozone_volume",
                   content=f"Found {len(ozone_assets)} Ozone volumes")
        for a in ozone_assets:
            yield emit("thought", source="ozone_volume", content=f"  volume/{a['name']}")

    # ── Decide filtering strategy ─────────────────────────────────────────────
    all_assets = kafka_assets + iceberg_assets + ozone_assets

    # Skip LLM if metadata intent already filtered assets (metadata filter is definitive)
    if has_metadata_intent and metadata_filters:
        needs_llm_filter = False
    else:
        needs_llm_filter = sidecar.has_active_modules() or sidecar.is_goal_specific()

    if needs_llm_filter:
        # Run LLM BEFORE emitting assets so only matching ones reach the UI
        mod_names = sidecar.get_active_module_names()
        filter_label = f"[{', '.join(mod_names)}]" if mod_names else "[intent_classifier]"
        yield emit("thought", content=f"Running LLM pre-flight filter {filter_label} — only matching assets will appear...")

        # Limit LLM testing to catalog-filtered candidates only (not all 10k assets)
        if catalog_filter:
            assets_to_test = []
            for asset in all_assets:
                atype = asset["asset_type"]
                name = asset["name"]
                if atype == "kafka_topic" and "kafka" in catalog_filter and name in catalog_filter["kafka"]:
                    assets_to_test.append(asset)
                elif atype == "iceberg_table" and "iceberg" in catalog_filter and name in catalog_filter["iceberg"]:
                    assets_to_test.append(asset)
            # Fallback: if catalog filter empty, test all (shouldn't happen, but safe)
            assets_to_test = assets_to_test or all_assets
            if len(assets_to_test) < len(all_assets):
                yield emit("thought", content=f"Testing {len(assets_to_test)} catalog-matched assets (skipped {len(all_assets) - len(assets_to_test)})")
        else:
            assets_to_test = all_assets

        suggestion_tasks = [asyncio.create_task(_add_suggestion(a, sidecar)) for a in assets_to_test]
        enriched_all = await asyncio.gather(*suggestion_tasks, return_exceptions=True)

        matched, discarded = [], []
        is_generic_scan = not (sidecar.is_goal_specific() or sidecar.has_active_modules())
        for result in enriched_all:
            if isinstance(result, Exception):
                continue
            suggestion = result.get("pipeline_suggestion", {})
            mc = suggestion.get("matches_criteria")
            logger.debug(f"[agent] bouncer: asset={result['name']!r} matches_criteria={mc!r} (type={type(mc).__name__})")
            # For generic scans ("show all", "discover all"), keep all assets regardless of LLM score
            if is_generic_scan:
                matched.append(result)
            # Explicit False means LLM said not relevant — discard.
            # None means field was missing — keep (don't punish for LLM omission).
            elif mc is False:
                reason = suggestion.get("relevance_reason", "not relevant to goal")
                discarded.append(result)
                yield emit("discarded", source=result["asset_type"],
                           asset_type=result["asset_type"],
                           name=result['name'],
                           reason=reason)
            else:
                matched.append(result)

        if discarded:
            yield emit("thought",
                       content=f"LLM pre-flight: {len(discarded)} discarded, {len(matched)} matched goal")

        for asset in matched:
            yield emit("asset", source=asset["asset_type"], asset_type=asset["asset_type"], data=asset)
            if asset.get("pii_risk"):
                yield emit("warning", source=asset["asset_type"],
                           content=f"PII risk in '{asset['name']}' — apply masking before ingestion")
            yield emit("asset_update", source=asset["asset_type"], asset_type=asset["asset_type"], data=asset)

        pii_count     = sum(1 for a in matched if a.get("pii_risk"))
        kafka_count   = sum(1 for a in matched if a["asset_type"] == "kafka_topic")
        iceberg_count = sum(1 for a in matched if a["asset_type"] == "iceberg_table")
        ozone_count   = sum(1 for a in matched if a["asset_type"] == "ozone_volume")
        all_assets_final = matched

    else:
        # Generic goal — emit all assets immediately
        for asset in all_assets:
            yield emit("asset", source=asset["asset_type"], asset_type=asset["asset_type"], data=asset)
            if asset.get("pii_risk"):
                yield emit("warning", source=asset["asset_type"],
                           content=f"PII risk in '{asset['name']}' — apply masking before ingestion")

        pii_count     = sum(1 for a in all_assets if a.get("pii_risk"))
        kafka_count   = len(kafka_assets)
        iceberg_count = len(iceberg_assets)
        ozone_count   = len(ozone_assets)
        all_assets_final = all_assets

        # Skip expensive LLM suggestions for truly generic discovery goals.
        # If user wants recommendations, they can ask a specific goal ("find topics with customer data").
        # Only run LLM if catalog_filter narrowed results, signaling intent to refine results.
        if catalog_filter:
            active_mod_names = sidecar.get_active_module_names()
            mod_label = f" [{', '.join(active_mod_names)}]" if active_mod_names else ""
            yield emit("thought", content=f"Asking LLM for pipeline recommendations{mod_label}...")

            assets_for_suggestions = []
            for asset in all_assets:
                atype = asset["asset_type"]
                name = asset["name"]
                if atype == "kafka_topic" and "kafka" in catalog_filter and name in catalog_filter["kafka"]:
                    assets_for_suggestions.append(asset)
                elif atype == "iceberg_table" and "iceberg" in catalog_filter and name in catalog_filter["iceberg"]:
                    assets_for_suggestions.append(asset)
            assets_for_suggestions = assets_for_suggestions or all_assets

            suggestion_tasks = [asyncio.create_task(_add_suggestion(a, sidecar)) for a in assets_for_suggestions]
            enriched = await asyncio.gather(*suggestion_tasks, return_exceptions=True)

            for result in enriched:
                if isinstance(result, Exception):
                    continue
                yield emit("asset_update", source=result["asset_type"],
                           asset_type=result["asset_type"], data=result)

    yield emit("scan_ready", counts={
        "kafka": kafka_count, "iceberg": iceberg_count,
        "ozone": ozone_count, "pii_flagged": pii_count,
    })

    summary_text = (
        f"Source Scout complete. Discovered {len(all_assets_final)} matching data sources: "
        f"{kafka_count} Kafka topics, {iceberg_count} Iceberg tables, "
        f"{ozone_count} Ozone volumes. "
        f"{pii_count} source(s) flagged for PII."
    )
    yield emit("complete", summary=summary_text, counts={
        "kafka": kafka_count, "iceberg": iceberg_count,
        "ozone": ozone_count, "pii_flagged": pii_count,
    })


# ── LangGraph node wrapper ────────────────────────────────────────────────────

def source_scout_node(state: AgentState) -> dict:
    async def _collect():
        events = []
        async for event in run_source_scout(state.get("goal", "discover all data sources")):
            events.append(event)
        return events

    events     = asyncio.run(_collect())
    discovered = {"kafka": [], "iceberg": [], "ozone": []}
    for ev in events:
        if ev["type"] == "asset":
            t = ev.get("asset_type", "")
            key = next((k for k in discovered if k in t), None)
            if key:
                discovered[key].append(ev["data"])

    summary = next((e["summary"] for e in events if e["type"] == "complete"), "Discovery complete.")
    return {
        "messages": [AIMessage(content=summary)],
        "active_agent": "source_scout",
        "discovered_assets": discovered,
        "sse_events": events,
        "next": "end",
    }
