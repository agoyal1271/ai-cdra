"""
Semantic Mapper Agent — Intelligence Pattern.

Uses embeddings and similarity analysis (minimal LLM).
Maps raw fields to business metrics, detects conflicts.

Self-learning: tracks mapping accuracy, suggests improvements.
"""

import logging
from typing import AsyncGenerator

from agents.base_agent import BaseAgent

logger = logging.getLogger(__name__)


class SemanticMapperAgent(BaseAgent):
    """Maps raw data fields to business semantic model."""

    def __init__(self):
        super().__init__(
            agent_id="semantic_mapper",
            description="Field-to-metric mapping and conflict detection",
        )
        self.semantic_model = {
            "customer": ["customer_id", "customer_name", "email"],
            "transaction": ["order_id", "amount", "timestamp", "status"],
            "product": ["product_id", "product_name", "category", "price"],
        }
        self.metric_definitions = {
            "revenue": "SUM(amount) WHERE status='completed'",
            "active_customers": "COUNT(DISTINCT customer_id) WHERE last_purchase < 30d",
        }

    async def run(self, goal: str, **kwargs) -> AsyncGenerator[dict, None]:
        """
        Map fields to semantic model and detect conflicts.

        Intelligence pattern: embeddings + similarity, not reasoning.
        """
        table_name = kwargs.get("table_name", "unknown")
        yield self.emit("started", goal=goal, table=table_name)

        try:
            # Step 1: Extract fields from table
            yield self.emit("step", name="extract_fields", status="running")
            fields = self._extract_fields(table_name)
            yield self.emit("fields_extracted", count=len(fields), fields=fields[:10])
            yield self.emit("step", name="extract_fields", status="complete")

            # Step 2: Map to semantic model
            yield self.emit("step", name="semantic_mapping", status="running")
            mappings = self._map_fields_to_semantics(fields)
            yield self.emit("mappings", data=mappings)
            yield self.emit("step", name="semantic_mapping", status="complete")

            # Step 3: Detect conflicts
            yield self.emit("step", name="conflict_detection", status="running")
            conflicts = self._detect_conflicts(mappings)
            if conflicts:
                yield self.emit("conflicts_detected", count=len(conflicts), items=conflicts)
            yield self.emit("step", name="conflict_detection", status="complete")

            # Step 4: Suggest metrics
            yield self.emit("step", name="suggest_metrics", status="running")
            metrics = self._suggest_metrics(fields, mappings)
            yield self.emit("suggested_metrics", items=metrics)
            yield self.emit("step", name="suggest_metrics", status="complete")

            # Log decision
            self.log_decision(
                decision_type="semantic_mapping",
                inputs={"table": table_name, "field_count": len(fields)},
                output={
                    "mapped_fields": len(mappings),
                    "conflicts": len(conflicts),
                    "suggested_metrics": len(metrics),
                },
                metadata={"mappings": mappings, "conflicts": conflicts},
            )

            yield self.emit("complete", summary=f"Mapped {len(mappings)} fields, detected {len(conflicts)} conflicts")

        except Exception as e:
            logger.exception(f"[semantic_mapper] Error: {e}")
            self.log_decision(
                decision_type="semantic_mapping_failed",
                inputs={"table": table_name},
                output={"error": str(e)},
                status="fail",
            )
            yield self.emit("error", message=str(e))

    def _extract_fields(self, table_name: str) -> list:
        """Extract field names from table schema."""
        # Mock: return sample fields
        return [
            "customer_id", "customer_email", "email_address",
            "order_id", "sale_amount", "total_amount",
            "purchase_date", "transaction_timestamp",
        ]

    def _map_fields_to_semantics(self, fields: list) -> dict:
        """Map fields to semantic model entities using similarity."""
        mappings = {}
        for field in fields:
            for entity, entity_fields in self.semantic_model.items():
                # Simple string similarity (in prod, use embeddings)
                similarity = self._string_similarity(field, entity_fields)
                if similarity > 0.6:
                    mappings[field] = {
                        "entity": entity,
                        "confidence": round(similarity, 2),
                        "mapped_to": entity_fields[0] if entity_fields else None,
                    }
                    break

        return mappings

    def _string_similarity(self, field: str, target_fields: list) -> float:
        """Naive similarity: check for substring matches."""
        field_lower = field.lower()
        for target in target_fields:
            target_lower = target.lower()
            if field_lower in target_lower or target_lower in field_lower:
                return 0.9
        return 0.0

    def _detect_conflicts(self, mappings: dict) -> list:
        """Flag fields mapped to different entities with similar names."""
        conflicts = []
        field_pairs = list(mappings.items())

        for i, (field1, mapping1) in enumerate(field_pairs):
            for field2, mapping2 in field_pairs[i+1:]:
                # Conflict: same entity, different field names
                if mapping1["entity"] == mapping2["entity"]:
                    similarity = self._string_similarity(field1, [field2])
                    if similarity > 0.7:
                        conflicts.append({
                            "type": "ambiguous_mapping",
                            "fields": [field1, field2],
                            "entity": mapping1["entity"],
                            "severity": "warning",
                        })
                # Conflict: different entities, similar field names
                elif mapping1["entity"] != mapping2["entity"]:
                    if any(kw in field1.lower() and kw in field2.lower() for kw in ["amount", "date", "id"]):
                        conflicts.append({
                            "type": "metric_conflict",
                            "fields": [field1, field2],
                            "entities": [mapping1["entity"], mapping2["entity"]],
                            "severity": "info",
                        })

        return conflicts

    def _suggest_metrics(self, fields: list, mappings: dict) -> list:
        """Suggest business metrics based on mapped fields."""
        metrics = []

        # Suggest aggregations if amount/count fields exist
        amount_fields = [f for f in fields if "amount" in f.lower() or "total" in f.lower()]
        if amount_fields:
            metrics.append({
                "name": "total_revenue",
                "definition": f"SUM({amount_fields[0]})",
                "confidence": 0.85,
            })

        # Suggest unique counts if ID fields exist
        id_fields = [f for f in fields if "id" in f.lower()]
        if id_fields:
            metrics.append({
                "name": "unique_entities",
                "definition": f"COUNT(DISTINCT {id_fields[0]})",
                "confidence": 0.9,
            })

        return metrics


# Orchestrator node (legacy compatibility)
from langchain_core.messages import AIMessage
from agents.state import AgentState


def semantic_mapper_node(state: AgentState) -> dict:
    return {
        "messages": [AIMessage(content="Semantic Mapper initialized")],
        "active_agent": "semantic_mapper",
        "sse_events": [{"type": "stub", "agent": "semantic_mapper", "content": "Semantic Mapper operational"}],
        "next": "end",
    }
