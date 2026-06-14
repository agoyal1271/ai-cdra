import logging
from typing import Literal

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, END

from agents.state import AgentState
from agents.source_scout.agent import source_scout_node
from agents.pipeline_builder.agent import pipeline_builder_node
from agents.quality_guardian.agent import quality_guardian_node
from agents.pipeline_healer.agent import pipeline_healer_node
from agents.semantic_mapper.agent import semantic_mapper_node
from agents.metadata_curator.agent import metadata_curator_node

logger = logging.getLogger(__name__)

AGENT_NAMES = [
    "source_scout",
    "pipeline_builder",
    "quality_guardian",
    "pipeline_healer",
    "semantic_mapper",
    "metadata_curator",
]

SUPERVISOR_PROMPT = f"""You are the orchestrator for a Cloudera AI agent platform.
Based on the user's goal, decide which specialist agent should handle it.

Available agents:
- source_scout: Discover and catalog data sources (Kafka topics, Iceberg tables, Ozone buckets, HDFS paths)
- pipeline_builder: Build and configure data ingestion pipelines (NiFi, Flink, Kafka Connect)
- quality_guardian: Monitor and enforce data quality on streams and Iceberg tables
- pipeline_healer: Detect and auto-remediate Kafka/Flink/NiFi pipeline failures
- semantic_mapper: Map raw fields to business glossary and semantic models
- metadata_curator: Classify PII, assign data owners, enforce governance policies

Respond with ONLY one of these agent names: {', '.join(AGENT_NAMES)}
Choose the single most relevant agent for the goal.
"""


def supervisor_node(state: AgentState) -> dict:
    """Routes the goal to the appropriate specialist agent."""
    from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY
    goal = state.get("goal", "")
    try:
        llm = ChatOpenAI(
            base_url=LLM_BASE_URL,
            model=LLM_MODEL,
            api_key=LLM_API_KEY,
            temperature=0.2,
        )
        response = llm.invoke([
            SystemMessage(content=SUPERVISOR_PROMPT),
            HumanMessage(content=f"Goal: {goal}"),
        ])
        chosen = response.content.strip().lower()
        if chosen not in AGENT_NAMES:
            chosen = _keyword_route(goal)
    except Exception as e:
        logger.warning(f"LLM routing failed ({e}), using keyword routing")
        chosen = _keyword_route(goal)

    logger.info(f"Supervisor routing '{goal}' → {chosen}")
    return {"next": chosen, "active_agent": chosen}


def _keyword_route(goal: str) -> str:
    goal = goal.lower()
    if any(w in goal for w in ["discover", "scan", "find", "list", "catalog", "explore"]):
        return "source_scout"
    if any(w in goal for w in ["pipeline", "ingest", "nifi", "flink", "connector", "build"]):
        return "pipeline_builder"
    if any(w in goal for w in ["quality", "validate", "anomaly", "dq", "test"]):
        return "quality_guardian"
    if any(w in goal for w in ["heal", "fix", "lag", "failure", "monitor", "health"]):
        return "pipeline_healer"
    if any(w in goal for w in ["semantic", "metric", "glossary", "map", "business"]):
        return "semantic_mapper"
    if any(w in goal for w in ["metadata", "govern", "pii", "owner", "compliance"]):
        return "metadata_curator"
    return "source_scout"


def route(state: AgentState) -> Literal[
    "source_scout", "pipeline_builder", "quality_guardian",
    "pipeline_healer", "semantic_mapper", "metadata_curator", "__end__"
]:
    next_agent = state.get("next", "end")
    if next_agent in ("end", "END", "__end__", ""):
        return "__end__"
    return next_agent  # type: ignore


def build_graph() -> StateGraph:
    graph = StateGraph(AgentState)

    graph.add_node("supervisor", supervisor_node)
    graph.add_node("source_scout", source_scout_node)
    graph.add_node("pipeline_builder", pipeline_builder_node)
    graph.add_node("quality_guardian", quality_guardian_node)
    graph.add_node("pipeline_healer", pipeline_healer_node)
    graph.add_node("semantic_mapper", semantic_mapper_node)
    graph.add_node("metadata_curator", metadata_curator_node)

    graph.set_entry_point("supervisor")
    graph.add_conditional_edges("supervisor", route)

    for agent in AGENT_NAMES:
        graph.add_edge(agent, END)

    return graph.compile()


# Compiled graph — imported by routers
app_graph = build_graph()
