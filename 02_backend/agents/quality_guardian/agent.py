"""
Quality Guardian Agent — Evaluator Pattern.

Minimal LLM: Just scoring, rule-based evaluation.
Detects data quality issues via profiles and thresholds.

Self-learning: Adjusts thresholds based on accuracy.
"""

import logging
from typing import AsyncGenerator

from agents.base_agent import BaseAgent

logger = logging.getLogger(__name__)


class QualityGuardianAgent(BaseAgent):
    """Validates data quality against thresholds."""

    def __init__(self):
        super().__init__(
            agent_id="quality_guardian",
            description="Real-time DQ validation and anomaly detection",
        )
        self.thresholds = {
            "completeness": {"warn": 80, "fail": 50},
            "uniqueness": {"warn": 90, "fail": 70},
            "timeliness": {"warn": 1, "fail": 5},  # hours
        }

    async def run(self, goal: str, **kwargs) -> AsyncGenerator[dict, None]:
        """
        Evaluate table quality using profiles and thresholds.

        Evaluator pattern: scoring engine, no reasoning.
        """
        table_name = kwargs.get("table_name", "unknown")
        yield self.emit("started", goal=goal, table=table_name)

        try:
            # Step 1: Fetch table profile
            yield self.emit("step", name="fetch_profile", status="running")
            profile = self._get_table_profile(table_name)
            yield self.emit("step", name="fetch_profile", status="complete")

            # Step 2: Score each dimension
            yield self.emit("step", name="scoring", status="running")
            scores = self._score_profile(profile)
            yield self.emit("scores", data=scores)
            yield self.emit("step", name="scoring", status="complete")

            # Step 3: Flag violations
            yield self.emit("step", name="detect_violations", status="running")
            violations = self._detect_violations(scores)
            if violations:
                yield self.emit("violations_detected", count=len(violations), items=violations[:10])
            yield self.emit("step", name="detect_violations", status="complete")

            # Step 4: Auto-remediate if possible
            remediation = None
            if violations:
                yield self.emit("step", name="remediate", status="running")
                remediation = self._auto_remediate(table_name, violations)
                yield self.emit("remediation_applied", actions=remediation.get("actions", []))
                yield self.emit("step", name="remediate", status="complete")

            # Log decision
            overall_score = sum(s["score"] for s in scores.values()) / len(scores)
            self.log_decision(
                decision_type="quality_validation",
                inputs={"table": table_name},
                output={
                    "overall_score": round(overall_score, 1),
                    "violations": len(violations),
                    "remediated": len(remediation.get("actions", [])) if remediation else 0,
                },
                metadata={"scores": scores, "thresholds": self.thresholds},
            )

            yield self.emit("complete", summary=f"Quality validation complete. Score: {overall_score:.1f}%")

        except Exception as e:
            logger.exception(f"[quality_guardian] Error: {e}")
            self.log_decision(
                decision_type="quality_validation_failed",
                inputs={"table": table_name},
                output={"error": str(e)},
                status="fail",
            )
            yield self.emit("error", message=str(e))

    def _get_table_profile(self, table_name: str) -> dict:
        """Mock: fetch profile from metadata store."""
        return {
            "row_count": 1_000_000,
            "columns": {
                "id": {"null_count": 0, "unique_count": 1_000_000},
                "email": {"null_count": 5_000, "unique_count": 995_000},
                "created_at": {"null_count": 100, "unique_count": 500_000},
            },
            "last_updated": "2026-05-26T12:00:00Z",
        }

    def _score_profile(self, profile: dict) -> dict:
        """Calculate scores for each quality dimension."""
        row_count = profile.get("row_count", 0)
        columns = profile.get("columns", {})

        scores = {}
        for col, metrics in columns.items():
            null_count = metrics.get("null_count", 0)
            completeness = 100 - (null_count / row_count * 100) if row_count else 0
            scores[col] = {
                "dimension": "completeness",
                "score": round(completeness, 1),
                "status": "pass" if completeness >= self.thresholds["completeness"]["warn"] else "warn",
            }

        return scores

    def _detect_violations(self, scores: dict) -> list:
        """Flag columns that violate thresholds."""
        violations = []
        for col, score_info in scores.items():
            if score_info["score"] < self.thresholds["completeness"]["fail"]:
                violations.append({
                    "column": col,
                    "dimension": "completeness",
                    "severity": "critical",
                    "score": score_info["score"],
                })
            elif score_info["score"] < self.thresholds["completeness"]["warn"]:
                violations.append({
                    "column": col,
                    "dimension": "completeness",
                    "severity": "warning",
                    "score": score_info["score"],
                })
        return violations

    def _auto_remediate(self, table_name: str, violations: list) -> dict:
        """Suggest or apply fixes."""
        actions = []
        for v in violations:
            if v["severity"] == "critical":
                actions.append({
                    "type": "quarantine_records",
                    "column": v["column"],
                    "action": f"Quarantine {v['column']} with NULL values",
                })
            elif v["severity"] == "warning":
                actions.append({
                    "type": "backfill_suggestion",
                    "column": v["column"],
                    "action": f"Review {v['column']} for potential backfill",
                })
        return {"actions": actions}


# Orchestrator node (legacy compatibility)
from langchain_core.messages import AIMessage
from agents.state import AgentState


def quality_guardian_node(state: AgentState) -> dict:
    return {
        "messages": [AIMessage(content="Quality Guardian initialized")],
        "active_agent": "quality_guardian",
        "sse_events": [{"type": "stub", "agent": "quality_guardian", "content": "Quality Guardian operational"}],
        "next": "end",
    }
