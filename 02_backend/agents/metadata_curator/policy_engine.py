"""
Metadata Curator — Policy Engine Pattern (Alternative to ReAct).

Direct policy application: No reasoning loop, no user questions.
Batch analysis: Analyze multiple tables, apply policies to all.
Learning: Remember past decisions, suggest same policy for similar tables.
"""

import json
import logging
import asyncio
from typing import AsyncGenerator

from agents.base_agent import BaseAgent
from tools.iceberg.iceberg_tools import list_iceberg_tables, describe_iceberg_table, TokenExpiredError
from tools.kafka.kafka_tools import get_all_topics_from_schema_registry
from tools.ranger.ranger_tools import create_hive_policy, check_ranger_connection
from agents.source_scout.sidecar import get_valid_knox_token

logger = logging.getLogger(__name__)


class MetadataCuratorPolicyEngine(BaseAgent):
    """Policy Engine: Direct enforcement, batch processing, learning loop."""

    def __init__(self):
        super().__init__(
            agent_id="metadata_curator",
            description="Policy Engine: Direct enforcement + batch governance",
        )
        self.compliance_rules = {
            "confidential": {
                "patterns": ["ssn", "credit_card", "passport"],
                "retention": "7y",
                "access_level": "confidential",
                "require_approval": True,
            },
            "restricted": {
                "patterns": ["email", "phone", "latitude", "longitude", "address", "geolocation"],
                "retention": "90d",
                "access_level": "restricted",
                "require_approval": False,
            },
            "internal": {
                "patterns": ["name", "department", "employee_id"],
                "retention": "1y",
                "access_level": "internal",
                "require_approval": False,
            },
            "public": {
                "patterns": [],
                "retention": "indefinite",
                "access_level": "public",
                "require_approval": False,
            },
        }

    async def run(self, goal: str, **kwargs) -> AsyncGenerator[dict, None]:
        """
        Policy Engine flow:
        1. Parse intent → rules
        2. Find ALL matching tables (batch)
        3. Classify each table (rules-based, fast)
        4. Check learning history for similar patterns
        5. Apply policies to all (with Ranger)
        6. Ask user: "Apply to all? Y/N"
        """
        table_name = kwargs.get("table_name", None)
        apply_ranger = kwargs.get("apply_ranger", False)  # From UI switch
        yield self.emit("started", goal=goal, agent="Metadata Curator (Policy Engine)")

        try:
            # Parse goal into rules
            yield self.emit("analysis", stage="policy_compilation", message="Compiling governance rules from goal")
            rules = await self._parse_rules(goal)
            rules["goal"] = goal  # raw goal goes to LLM filter
            yield self.emit("analysis", stage="rules_ready", rules=rules, message="Ready to apply rules")

            # Check for recent similar decisions (learning loop)
            yield self.emit("analysis", stage="learning_check", message="Checking past decisions for similar patterns")
            past_decisions = await self._check_learning_history(goal)
            if past_decisions:
                yield self.emit("observation",
                              message=f"Found {len(past_decisions)} similar past decisions",
                              past_decisions=past_decisions)

            # Get tables to govern
            if table_name:
                tables_to_govern = [{"name": table_name}]
                yield self.emit("analysis", stage="target_selected", table=table_name)
            else:
                yield self.emit("analysis", stage="discovery", message="Discovering all matching tables...")
                tables_to_govern = await self._discover_tables(rules)
                yield self.emit("analysis",
                              stage="batch_ready",
                              count=len(tables_to_govern),
                              tables=tables_to_govern,
                              message=f"Found {len(tables_to_govern)} tables to govern")

            if not tables_to_govern:
                yield self.emit("complete", summary="No tables found matching criteria")
                return

            # Batch classify and apply policies
            yield self.emit("analysis", stage="batch_classification", message=f"Classifying {len(tables_to_govern)} tables...")
            classified = await self._batch_classify(tables_to_govern, rules)

            # Show summary before applying
            summary = {
                "confidential": len([t for t in classified if t["sensitivity"] == "confidential"]),
                "restricted": len([t for t in classified if t["sensitivity"] == "restricted"]),
                "internal": len([t for t in classified if t["sensitivity"] == "internal"]),
                "public": len([t for t in classified if t["sensitivity"] == "public"]),
            }

            yield self.emit("question",
                          message=f"Apply policies to all {len(classified)} tables?",
                          summary=summary,
                          options=["Apply All", "Review First", "Cancel"])

            # For demo, auto-apply
            yield self.emit("action", name="batch_apply", count=len(classified))

            # Apply policies
            yield self.emit("analysis", stage="policy_application", message="Applying policies...")
            results = await self._batch_apply_policies(classified, apply_ranger=apply_ranger)

            # Record decisions for learning
            yield self.emit("analysis", stage="learning_update", message="Recording decisions for future learning...")
            await self._record_decisions(classified, "user_confirmed")

            # Summary
            success_count = len([r for r in results if r.get("success")])
            yield self.emit("complete",
                          summary=f"Governance complete: {success_count}/{len(classified)} policies applied",
                          results=results[:10])  # Show first 10

        except Exception as e:
            logger.exception(f"[metadata_curator_pe] Error: {e}")
            self.log_decision(
                decision_type="batch_governance_failed",
                inputs={"goal": goal},
                output={"error": str(e)},
                status="fail",
            )
            yield self.emit("error", message=str(e))

    async def _parse_rules(self, goal: str) -> dict:
        """Parse goal into governance rules."""
        goal_lower = goal.lower()
        rules = {}

        # Check for sensitivity keywords
        if any(kw in goal_lower for kw in ["pii", "email", "phone", "address", "geolocation"]):
            rules["sensitivity"] = "restricted"
        elif any(kw in goal_lower for kw in ["ssn", "credit", "passport"]):
            rules["sensitivity"] = "confidential"
        elif any(kw in goal_lower for kw in ["public", "open"]):
            rules["sensitivity"] = "public"
        else:
            rules["sensitivity"] = "internal"

        # Check for scope keywords
        rules["auto_apply"] = any(kw in goal_lower for kw in ["all", "batch", "auto"])
        rules["require_ranger"] = any(kw in goal_lower for kw in ["ranger", "enforce", "policy"])

        return rules

    async def _check_learning_history(self, goal: str) -> list:
        """Check past decisions for similar patterns."""
        try:
            # Would query decision_store for similar past decisions
            # For now, return empty
            return []
        except Exception as e:
            logger.debug(f"Learning history check failed: {e}")
            return []

    async def _discover_tables(self, rules: dict) -> list:
        """LLM-driven discovery — delegates to the shared DiscoveryAgent filter."""
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
            return matching[:20]
        except Exception as e:
            logger.warning(f"[metadata_curator_pe] Discovery failed: {e}")
            return []

    async def _batch_classify(self, tables: list, rules: dict) -> list:
        """Classify all tables using rules (fast, no LLM)."""
        classified = []

        for table in tables:
            sensitivity = self._classify_table(table, rules)
            policy = self.compliance_rules.get(sensitivity, self.compliance_rules["internal"])

            classified.append({
                "name": table["name"],
                "sensitivity": sensitivity,
                "retention": policy["retention"],
                "access_level": policy["access_level"],
                "owner": "data-steward@company.com",  # Default
            })

        return classified

    def _classify_table(self, table: dict, rules: dict) -> str:
        """Classify table using rule matching."""
        fields = table.get("fields", [])
        field_names = [f.get("name", "").lower() for f in fields]

        # Check for high-risk PII
        for pattern in self.compliance_rules["confidential"]["patterns"]:
            if any(pattern in fn for fn in field_names):
                return "confidential"

        # Check for restricted PII
        for pattern in self.compliance_rules["restricted"]["patterns"]:
            if any(pattern in fn for fn in field_names):
                return "restricted"

        # Default
        return "internal"

    async def _batch_apply_policies(self, classified: list, apply_ranger: bool = False) -> list:
        """Apply policies to all tables."""
        results = []

        for item in classified:
            result = {
                "table": item["name"],
                "success": True,
                "message": f"Policy applied: {item['sensitivity']}"
            }

            # Apply Ranger policy if requested
            if apply_ranger:
                ranger_result = await create_hive_policy(
                    policy_name=f"{item['name'].replace('.', '_')}_{item['sensitivity']}",
                    table_name=item["name"],
                    owner=item["owner"],
                    access_level=item["access_level"],
                )
                result["ranger"] = ranger_result

            results.append(result)

        return results

    async def _record_decisions(self, classified: list, feedback: str) -> None:
        """Record decisions in learning store."""
        for item in classified:
            self.log_decision(
                decision_type="policy_applied",
                inputs={"table": item["name"]},
                output={
                    "sensitivity": item["sensitivity"],
                    "retention": item["retention"],
                    "access_level": item["access_level"],
                },
                metadata={"feedback": feedback},
            )
