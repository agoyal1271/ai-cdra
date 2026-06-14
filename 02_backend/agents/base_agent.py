"""
Base Agent — Common interface for all agents.

Provides:
- Decision logging
- Error handling
- Learning (track accuracy)
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator, Dict, Optional

from agents.decision_store.logger import log_decision, get_decision_stats

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """Base class for all autonomous agents."""

    def __init__(self, agent_id: str, description: str):
        self.agent_id = agent_id
        self.description = description
        self.stats = get_decision_stats(agent_id)

    @abstractmethod
    async def run(self, goal: str, **kwargs) -> AsyncGenerator[dict, None]:
        """
        Execute the agent.

        Yields SSE events with type, content, and optional metadata.
        """
        pass

    def log_decision(
        self,
        decision_type: str,
        inputs: Dict[str, Any],
        output: Any,
        metadata: Optional[Dict[str, Any]] = None,
        status: str = "success",
    ) -> str:
        """Log a decision made by this agent."""
        return log_decision(
            agent_id=self.agent_id,
            decision_type=decision_type,
            inputs=inputs,
            output=output,
            metadata=metadata,
            status=status,
        )

    def emit(self, event_type: str, **kwargs) -> dict:
        """Emit an SSE event."""
        return {"type": event_type, "agent": self.agent_id, **kwargs}

    def get_stats(self) -> dict:
        """Get decision stats for self-learning."""
        return {
            "agent_id": self.agent_id,
            "total_decisions": self.stats.get("total", 0),
            "success_rate": (self.stats.get("success", 0) / max(self.stats.get("total", 1), 1)) * 100,
        }
