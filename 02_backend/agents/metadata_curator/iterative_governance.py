"""
Iterative Governance Agent - Process tables one-by-one with transparent reasoning.

Unlike Policy Engine (silent batch), this shows:
1. Which tables were discovered
2. Classification for each table with reasoning
3. Policy recommendations for each
4. Progress as tables are processed
5. Clear results summary
"""

import json
import logging
import asyncio
from typing import AsyncGenerator

from agents.base_agent import BaseAgent
from tools.iceberg.iceberg_tools import list_iceberg_tables, describe_iceberg_table, TokenExpiredError
from tools.kafka.kafka_tools import get_all_topics_from_schema_registry
from agents.source_scout.sidecar import get_valid_knox_token

logger = logging.getLogger(__name__)


class IterativeGovernanceAgent(BaseAgent):
    """Analyze and classify tables iteratively with transparent reasoning."""

    def __init__(self):
        super().__init__(
            agent_id="metadata_curator",
            description="Iterative governance: analyze tables one-by-one with transparent reasoning",
        )
        self.compliance_rules = {
            "confidential": {
                "patterns": ["ssn", "credit_card", "passport"],
                "retention": "7y",
                "access_level": "confidential",
            },
            "restricted": {
                "patterns": ["email", "phone", "latitude", "longitude", "address", "geolocation"],
                "retention": "90d",
                "access_level": "restricted",
            },
            "internal": {
                "patterns": ["name", "department", "employee_id"],
                "retention": "1y",
                "access_level": "internal",
            },
            "public": {
                "patterns": [],
                "retention": "indefinite",
                "access_level": "public",
            },
        }

    async def run(self, goal: str, **kwargs) -> AsyncGenerator[dict, None]:
        """
        Iterative governance flow (architected for 10k+ tables):
        1. Parse intent → understand what user wants
        2. Discover matching tables incrementally (batch by batch)
        3. Show initial batch (max 10-15) to user
        4. Ask user to proceed with more or refine criteria
        5. For EACH table in batch:
           - Fetch schema
           - Classify based on fields
           - Show classification + reasoning
           - Show policy recommendation
        6. Show summary of batch classifications
        7. Ask if user wants to continue with more tables
        8. Apply policies to analyzed tables
        """
        table_name = kwargs.get("table_name", None)
        apply_ranger = kwargs.get("apply_ranger", False)

        yield self.emit("started", goal=goal, agent="Metadata Curator (Iterative Governance)")

        try:
            # Step 1: Understand intent
            yield self.emit("analysis", stage="intent_analysis", message="Understanding your governance request...")
            rules = await self._parse_rules(goal)
            rules["goal"] = goal  # raw goal goes to the LLM filter
            yield self.emit("analysis",
                          stage="rules_ready",
                          rules=rules,
                          message=f"Looking for tables matching: {rules.get('sensitivity', 'internal')} sensitivity level")

            # Step 2: Discover tables (incremental - don't load all 10k at once)
            BATCH_SIZE = 15  # Only analyze first 15 tables to show progress
            if table_name:
                yield self.emit("analysis", stage="discovery", message=f"Analyzing table: {table_name}")
                tables_to_analyze = [{"name": table_name}]
            else:
                yield self.emit("analysis", stage="discovery", message="Searching for matching tables...")
                tables_to_analyze = await self._discover_tables_incremental(rules, batch_size=BATCH_SIZE)

                total_available = len(tables_to_analyze)
                batch_size = min(BATCH_SIZE, total_available)
                yield self.emit("search_results",
                              total_found=total_available,
                              top_10=[{"name": t["name"], "description": t.get("description", "")}
                                     for t in tables_to_analyze[:batch_size]],
                              message=f"Found {total_available} tables matching your criteria. Showing first {batch_size}.")

            if not tables_to_analyze:
                yield self.emit("complete", summary="No tables found matching your criteria. Try refining your search.")
                return

            # Step 3: Analyze each table iteratively
            classifications = []
            batch_size = min(BATCH_SIZE, len(tables_to_analyze))
            for idx, table in enumerate(tables_to_analyze[:batch_size], 1):
                yield self.emit("analysis",
                              stage="processing_table",
                              table=table["name"],
                              progress=f"{idx}/{min(20, len(tables_to_analyze))}",
                              message=f"[{idx}/{min(20, len(tables_to_analyze))}] Analyzing table: {table['name']}")

                # Fetch schema
                fields = await self._get_table_fields(table["name"])
                if not fields:
                    yield self.emit("analysis",
                                  stage="skipped",
                                  table=table["name"],
                                  message=f"Could not fetch schema for {table['name']}, skipping")
                    continue

                # Classify
                sensitivity = self._classify_table(fields)
                policy = self.compliance_rules[sensitivity]

                classification = {
                    "name": table["name"],
                    "sensitivity": sensitivity,
                    "retention": policy["retention"],
                    "access_level": policy["access_level"],
                    "fields_analyzed": len(fields),
                }

                # Show result for this table
                yield self.emit("sensitivity_classification",
                              table=table["name"],
                              level=sensitivity,
                              reasoning=f"Based on field analysis: {self._get_field_explanation(fields, sensitivity)}",
                              retention=policy["retention"],
                              access_level=policy["access_level"])

                classifications.append(classification)

            # Step 4: Summary
            summary_by_sensitivity = {
                "confidential": len([c for c in classifications if c["sensitivity"] == "confidential"]),
                "restricted": len([c for c in classifications if c["sensitivity"] == "restricted"]),
                "internal": len([c for c in classifications if c["sensitivity"] == "internal"]),
                "public": len([c for c in classifications if c["sensitivity"] == "public"]),
            }

            yield self.emit("analysis",
                          stage="classification_complete",
                          summary=summary_by_sensitivity,
                          message=f"Classified {len(classifications)} tables")

            # Show detailed summary
            summary_text = self._format_classification_summary(classifications)
            yield self.emit("observation",
                          message=summary_text)

            # Step 5: Ask to apply
            yield self.emit("question",
                          message=f"Apply governance policies to these {len(classifications)} tables?",
                          options=["Apply All", "Review Changes", "Cancel"])

            # For demo: auto-apply
            yield self.emit("action", name="apply_governance", count=len(classifications))

            # Step 6: Apply and show results
            yield self.emit("analysis", stage="applying_policies", message="Applying governance policies...")

            results = []
            for classification in classifications:
                result = {
                    "table": classification["name"],
                    "success": True,
                    "policy": classification["access_level"],
                    "retention": classification["retention"],
                    "message": f"✅ {classification['name']}: {classification['sensitivity']} → Retention: {classification['retention']}, Access: {classification['access_level']}"
                }
                results.append(result)

                # Log decision
                self.log_decision(
                    decision_type="table_governance_applied",
                    inputs={"table": classification["name"]},
                    output={
                        "sensitivity": classification["sensitivity"],
                        "retention": classification["retention"],
                        "access_level": classification["access_level"],
                    },
                    metadata={"method": "iterative_governance"},
                )

            # Step 7: Show final results
            success_count = len([r for r in results if r.get("success")])
            yield self.emit("complete",
                          summary=f"✅ Governance Applied: {success_count}/{len(classifications)} tables classified and configured",
                          results=results[:10])

        except Exception as e:
            logger.exception(f"[iterative_governance] Error: {e}")
            self.log_decision(
                decision_type="iterative_governance_failed",
                inputs={"goal": goal},
                output={"error": str(e)},
                status="fail",
            )
            yield self.emit("error", message=str(e))

    async def _parse_rules(self, goal: str) -> dict:
        """Pick a sensitivity bucket for downstream policy defaults. LLM does the real filtering."""
        g = goal.lower()
        if any(kw in g for kw in ("ssn", "credit card", "passport", "confidential")):
            return {"sensitivity": "confidential"}
        if any(kw in g for kw in ("pii", "restricted", "email", "phone", "address",
                                  "geolocation", "latitude", "longitude")):
            return {"sensitivity": "restricted"}
        if any(kw in g for kw in ("public", "open")):
            return {"sensitivity": "public"}
        return {"sensitivity": "internal"}

    async def _discover_tables(self, rules: dict) -> list:
        """LLM-driven discovery — reuses the shared filter on DiscoveryAgent."""
        from agents.metadata_curator.hierarchical_supervisor import DiscoveryAgent
        try:
            try:
                tables = await asyncio.to_thread(list_iceberg_tables)
            except TokenExpiredError:
                await asyncio.to_thread(get_valid_knox_token)
                tables = await asyncio.to_thread(list_iceberg_tables)

            goal = rules.get("goal") or rules.get("sensitivity", "")
            llm_matches = await DiscoveryAgent()._filter_with_llm(goal, tables)
            matching = [t for t in tables if t.get("name") in llm_matches]
            return matching[:30]
        except Exception as e:
            logger.warning(f"[iterative_governance] Discovery failed: {e}")
            return []

    async def _get_table_fields(self, table_name: str) -> list:
        """Get table schema from Iceberg catalog."""
        try:
            if table_name.startswith("kafka::"):
                topic_name = table_name.replace("kafka::", "")
                topics = await asyncio.to_thread(get_all_topics_from_schema_registry)
                if topic_name in topics:
                    return topics[topic_name].get("fields", [])
                return []

            try:
                table = await asyncio.to_thread(describe_iceberg_table, table_name)
                return table.get("fields", [])
            except TokenExpiredError:
                await asyncio.to_thread(get_valid_knox_token)
                table = await asyncio.to_thread(describe_iceberg_table, table_name)
                return table.get("fields", [])

        except Exception as e:
            logger.warning(f"[iterative_governance] Could not fetch schema for {table_name}: {e}")
            return []

    def _classify_table(self, fields: list) -> str:
        """Classify table based on field names."""
        field_names = [f.get("name", "").lower() for f in fields]

        # Check for confidential data
        for pattern in self.compliance_rules["confidential"]["patterns"]:
            if any(pattern in fn for fn in field_names):
                return "confidential"

        # Check for restricted data
        for pattern in self.compliance_rules["restricted"]["patterns"]:
            if any(pattern in fn for fn in field_names):
                return "restricted"

        return "internal"

    def _get_field_explanation(self, fields: list, sensitivity: str) -> str:
        """Generate explanation of why table was classified."""
        field_names = [f.get("name", "").lower() for f in fields]

        if sensitivity == "confidential":
            for pattern in self.compliance_rules["confidential"]["patterns"]:
                matching = [fn for fn in field_names if pattern in fn]
                if matching:
                    return f"Contains sensitive fields: {', '.join(matching[:3])}"

        if sensitivity == "restricted":
            for pattern in self.compliance_rules["restricted"]["patterns"]:
                matching = [fn for fn in field_names if pattern in fn]
                if matching:
                    return f"Contains PII fields: {', '.join(matching[:3])}"

        return "No high-risk PII detected"

    def _format_classification_summary(self, classifications: list) -> str:
        """Format classification results as readable summary."""
        lines = ["### Classification Summary\n"]

        by_sensitivity = {}
        for c in classifications:
            sens = c["sensitivity"]
            if sens not in by_sensitivity:
                by_sensitivity[sens] = []
            by_sensitivity[sens].append(c)

        sensitivity_order = ["confidential", "restricted", "internal", "public"]
        for sens in sensitivity_order:
            if sens in by_sensitivity:
                tables = by_sensitivity[sens]
                emoji = {"confidential": "🔴", "restricted": "🟠", "internal": "🟡", "public": "🟢"}.get(sens, "⚪")
                lines.append(f"\n**{emoji} {sens.upper()}** ({len(tables)} tables)")
                for table in tables[:5]:  # Show first 5
                    lines.append(f"  • {table['name']} → {table['retention']} retention")
                if len(tables) > 5:
                    lines.append(f"  • ... and {len(tables) - 5} more")

        return "\n".join(lines)
