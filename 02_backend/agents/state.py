from typing import Annotated, Any
from typing_extensions import TypedDict
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages


class AgentState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    goal: str
    active_agent: str
    discovered_assets: dict[str, list[dict]]   # kafka/iceberg/ozone/hdfs → list of assets
    artifacts: dict[str, Any]                  # generated configs, flows, jobs
    next: str                                  # routing target
    sse_events: list[dict]                     # events to stream to frontend
