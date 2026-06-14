"""
Unified request/response schemas for all agent APIs.

Single source of truth for request validation and response format.
All endpoints should use these Pydantic models.
"""
from typing import Any, Optional
from pydantic import BaseModel, Field


# ────────────────────────────────────────────────────────────────────────────
# Discovery & Asset Management
# ────────────────────────────────────────────────────────────────────────────


class DiscoverRequest(BaseModel):
    """Request to run Source Scout discovery."""

    goal: str = Field(..., description="User's discovery goal (e.g., 'find all Kafka topics')")
    asset_types: Optional[list[str]] = Field(
        default=None,
        description="Filter by asset type: kafka_topic, iceberg_table, ozone_volume, hdfs_path"
    )
    max_results: Optional[int] = Field(default=100, description="Max assets to discover")
    timeout_seconds: Optional[float] = Field(default=60.0, description="Discovery timeout")

    class Config:
        json_schema_extra = {
            "example": {
                "goal": "Find all Kafka topics with customer data",
                "asset_types": ["kafka_topic"],
                "max_results": 50,
            }
        }


class QualityCheckRequest(BaseModel):
    """Request to execute quality checks on an asset."""

    asset_name: str = Field(..., description="Name of the asset to check")
    asset_type: str = Field(..., description="Type: iceberg_table or kafka_topic")
    engine: str = Field(default="impala", description="Query engine: impala, trino, or cde_spark")
    fields: Optional[list[dict[str, str]]] = Field(
        default=None,
        description="Asset schema fields (skip describe if provided)"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "asset_name": "customers",
                "asset_type": "iceberg_table",
                "engine": "impala",
            }
        }


class CatalogRequest(BaseModel):
    """Request to query the semantic catalog."""

    query: str = Field(..., description="Natural language search query")
    asset_type: Optional[str] = Field(default=None, description="Filter by asset type")
    limit: Optional[int] = Field(default=10, description="Max results to return")

    class Config:
        json_schema_extra = {
            "example": {
                "query": "tables stored in ozone with parquet format",
                "limit": 20,
            }
        }


# ────────────────────────────────────────────────────────────────────────────
# Agent Orchestration
# ────────────────────────────────────────────────────────────────────────────


class OrchestrateRequest(BaseModel):
    """Request to orchestrate multi-agent workflow."""

    goal: str = Field(..., description="High-level goal for the agent orchestrator")
    agents: Optional[list[str]] = Field(
        default=None,
        description="Specific agents to run (default: all applicable)"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "goal": "Create an ingestion pipeline for customer data from Kafka",
                "agents": ["source_scout", "pipeline_builder"],
            }
        }


# ────────────────────────────────────────────────────────────────────────────
# Response Models
# ────────────────────────────────────────────────────────────────────────────


class ErrorResponse(BaseModel):
    """Standard error response format."""

    error: dict = Field(..., description="Error details")

    class Config:
        json_schema_extra = {
            "example": {
                "error": {
                    "code": "VALIDATION_ERROR",
                    "message": "Missing required field: asset_type",
                    "details": {"field": "asset_type"},
                }
            }
        }


class DiscoveryEvent(BaseModel):
    """Individual discovery event in SSE stream."""

    event_type: str = Field(..., description="Event type: thought, asset_found, scan_complete, error")
    content: str = Field(..., description="Event content (description or asset JSON)")
    timestamp: float = Field(..., description="Unix timestamp when event occurred")

    class Config:
        json_schema_extra = {
            "example": {
                "event_type": "asset_found",
                "content": '{"name": "orders", "asset_type": "kafka_topic"}',
                "timestamp": 1234567890.0,
            }
        }


class AssetResponse(BaseModel):
    """Response containing discovered asset."""

    id: str
    name: str
    asset_type: str
    description: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None
    pii_risk: Optional[bool] = None
    pipeline_suggestion: Optional[dict] = None

    class Config:
        json_schema_extra = {
            "example": {
                "id": "kafka_orders_v1",
                "name": "orders",
                "asset_type": "kafka_topic",
                "description": "Customer order events",
                "metadata": {
                    "partitions": 10,
                    "replication_factor": 3,
                },
                "pii_risk": True,
                "pipeline_suggestion": {
                    "recommended_pipeline": "Kafka Connect",
                },
            }
        }


class QualityCheckResults(BaseModel):
    """Quality check results for an asset."""

    asset_name: str
    asset_type: str
    passed: bool
    score: Optional[float] = Field(default=None, description="Quality score 0-100")
    checks: Optional[list[dict[str, Any]]] = None
    error: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "asset_name": "customers",
                "asset_type": "iceberg_table",
                "passed": True,
                "score": 95.5,
                "checks": [
                    {"name": "null_check", "status": "pass", "pct": 99.8},
                    {"name": "uniqueness_check", "status": "pass", "pct": 100},
                ],
            }
        }
