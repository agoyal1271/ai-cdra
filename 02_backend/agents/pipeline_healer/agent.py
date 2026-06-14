"""
Pipeline Healer Agent — Reactive Pattern.

State machine: detects failures → diagnoses → heals or escalates.
No reasoning overhead; direct action based on health signals.

Self-healing: retries, restarts, auto-scaling.
"""

import asyncio
import logging
from datetime import datetime
from enum import Enum
from typing import AsyncGenerator

from agents.base_agent import BaseAgent
from agents.decision_store.retry_queue import retry_queue

logger = logging.getLogger(__name__)


class HealthState(Enum):
    """Health signal states."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"
    RECOVERING = "recovering"


class PipelineHealerAgent(BaseAgent):
    """Monitors and auto-heals pipeline failures."""

    def __init__(self):
        super().__init__(
            agent_id="pipeline_healer",
            description="24/7 monitoring and self-healing",
        )
        self.escalation_level = 0  # 0=auto-heal, 1=notify, 2=page

    async def run(self, goal: str, **kwargs) -> AsyncGenerator[dict, None]:
        """
        Monitor pipelines and auto-heal failures.

        Reactive pattern: monitor → detect → diagnose → heal → escalate.
        """
        pipeline_id = kwargs.get("pipeline_id", "unknown")
        yield self.emit("started", goal=goal, pipeline=pipeline_id)

        try:
            # Step 1: Check health
            yield self.emit("step", name="check_health", status="running")
            health = await self._check_health(pipeline_id)
            state = self._determine_state(health)
            yield self.emit("health_check", state=state.value, metrics=health)
            yield self.emit("step", name="check_health", status="complete")

            # Step 2: Diagnose if unhealthy
            diagnosis = None
            if state != HealthState.HEALTHY:
                yield self.emit("step", name="diagnose", status="running")
                diagnosis = await self._diagnose(pipeline_id, health)
                yield self.emit("diagnosis", root_cause=diagnosis["root_cause"], confidence=diagnosis["confidence"])
                yield self.emit("step", name="diagnose", status="complete")

            # Step 3: Heal if possible
            healed = None
            if diagnosis and diagnosis["confidence"] > 0.7:
                yield self.emit("step", name="heal", status="running")
                healed = await self._auto_heal(pipeline_id, diagnosis)
                if healed:
                    yield self.emit("healed", actions=healed)
                yield self.emit("step", name="heal", status="complete")

            # Step 4: Escalate if can't heal
            if diagnosis and not healed:
                yield self.emit("step", name="escalate", status="running")
                await self._escalate(pipeline_id, diagnosis)
                yield self.emit("escalated", level=self.escalation_level)
                yield self.emit("step", name="escalate", status="complete")

            # Log decision
            self.log_decision(
                decision_type="pipeline_health_check",
                inputs={"pipeline": pipeline_id},
                output={
                    "state": state.value,
                    "root_cause": diagnosis["root_cause"] if diagnosis else None,
                    "healed": bool(healed) if diagnosis else False,
                },
                metadata={"health_metrics": health},
            )

            yield self.emit("complete", summary=f"Pipeline health: {state.value}")

        except Exception as e:
            logger.exception(f"[pipeline_healer] Error: {e}")
            self.log_decision(
                decision_type="pipeline_health_check_failed",
                inputs={"pipeline": pipeline_id},
                output={"error": str(e)},
                status="fail",
            )
            yield self.emit("error", message=str(e))

    async def _check_health(self, pipeline_id: str) -> dict:
        """Check health signals: consumer lag, job status, error rate."""
        # Mock health check
        return {
            "consumer_lag": 50000,  # messages
            "job_status": "running",
            "error_rate": 0.02,  # 2%
            "last_processed": datetime.utcnow().isoformat(),
        }

    def _determine_state(self, health: dict) -> HealthState:
        """Classify health into state."""
        lag = health.get("consumer_lag", 0)
        error_rate = health.get("error_rate", 0)

        if lag > 100000 or error_rate > 0.1:
            return HealthState.CRITICAL
        elif lag > 50000 or error_rate > 0.05:
            return HealthState.DEGRADED
        else:
            return HealthState.HEALTHY

    async def _diagnose(self, pipeline_id: str, health: dict) -> dict:
        """Diagnose root cause based on health signals."""
        lag = health.get("consumer_lag", 0)
        error_rate = health.get("error_rate", 0)

        if lag > 100000:
            return {
                "root_cause": "consumer_lag_excessive",
                "confidence": 0.95,
                "suggested_fix": "restart_consumers",
            }
        elif error_rate > 0.1:
            return {
                "root_cause": "high_error_rate",
                "confidence": 0.85,
                "suggested_fix": "scale_workers",
            }
        else:
            return {
                "root_cause": "unknown",
                "confidence": 0.3,
                "suggested_fix": "escalate_to_ops",
            }

    async def _auto_heal(self, pipeline_id: str, diagnosis: dict) -> list:
        """Auto-heal if diagnosis is confident."""
        actions = []
        fix = diagnosis.get("suggested_fix")

        if fix == "restart_consumers":
            actions.append({"action": "restart_kafka_consumers", "pipeline": pipeline_id})
        elif fix == "scale_workers":
            actions.append({"action": "scale_flink_workers", "pipeline": pipeline_id, "scale_factor": 1.5})

        # Fire async tasks for retry queue if integration fails
        for action in actions:
            await retry_queue.enqueue(
                task_id=f"{pipeline_id}_{action['action']}",
                func=self._execute_action,
                args=(action,),
            )

        return actions

    async def _escalate(self, pipeline_id: str, diagnosis: dict) -> None:
        """Escalate: notify ops, create incident."""
        self.escalation_level = 1
        message = f"Pipeline {pipeline_id} requires manual intervention: {diagnosis['root_cause']}"

        # Enqueue notifications for retry
        await retry_queue.enqueue(
            task_id=f"{pipeline_id}_notify_ops",
            func=self._send_notification,
            args=(message,),
        )

    async def _execute_action(self, action: dict) -> None:
        """Execute a remediation action."""
        logger.info(f"[healer] Executing: {action}")
        await asyncio.sleep(1)  # Mock execution

    async def _send_notification(self, message: str) -> None:
        """Send notification (Slack, PagerDuty, etc.)."""
        logger.info(f"[healer] Notification: {message}")
        await asyncio.sleep(1)  # Mock


# Orchestrator node (legacy compatibility)
from langchain_core.messages import AIMessage
from agents.state import AgentState


def pipeline_healer_node(state: AgentState) -> dict:
    return {
        "messages": [AIMessage(content="Pipeline Healer initialized")],
        "active_agent": "pipeline_healer",
        "sse_events": [{"type": "stub", "agent": "pipeline_healer", "content": "Pipeline Healer operational"}],
        "next": "end",
    }
