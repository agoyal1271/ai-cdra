"""
Decision Logger — Audit trail for all agent decisions.

Every decision is stored as:
- Markdown file (human readable)
- JSON block (queryable)

Enables self-learning and compliance auditing.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

DECISIONS_DIR = Path(__file__).parent.parent.parent.parent / "decisions"


def log_decision(
    agent_id: str,
    decision_type: str,
    inputs: Dict[str, Any],
    output: Any,
    metadata: Optional[Dict[str, Any]] = None,
    status: str = "success",
) -> str:
    """
    Log a decision with reasoning and outcome.

    Args:
        agent_id: Agent making the decision (e.g., 'pipeline_builder')
        decision_type: Type of decision (e.g., 'generate_config', 'heal_failure')
        inputs: Decision inputs (goal, context, etc.)
        output: Decision output (result, generated code, etc.)
        metadata: Optional additional context (confidence, alternatives considered, etc.)
        status: 'success', 'warn', 'fail'

    Returns:
        Path to the decision file
    """
    # Create agent decision directory
    agent_dir = DECISIONS_DIR / agent_id
    agent_dir.mkdir(parents=True, exist_ok=True)

    # Timestamp filename
    now = datetime.utcnow()
    filename = f"{now.strftime('%Y-%m-%d_%H-%M-%S')}__{decision_type}.md"
    filepath = agent_dir / filename

    # Build decision record
    decision = {
        "timestamp": now.isoformat(),
        "agent_id": agent_id,
        "decision_type": decision_type,
        "status": status,
        "inputs": inputs,
        "output": output,
        "metadata": metadata or {},
    }

    # Format markdown with JSON block
    markdown = f"""# {agent_id.upper()} — {decision_type}

**Status**: {status.upper()}
**Time**: {now.isoformat()}

## Decision

{_format_dict(inputs, indent=0)}

## Output

{_format_dict(output, indent=0)}

## Metadata

{_format_dict(metadata or {}, indent=0)}

---

## Embedded Record

```json
{json.dumps(decision, indent=2)}
```
"""

    filepath.write_text(markdown)
    logger.info(f"[{agent_id}] Decision logged: {filepath}")

    return str(filepath)


def get_decision_stats(agent_id: str) -> Dict[str, int]:
    """Get stats about agent's past decisions."""
    agent_dir = DECISIONS_DIR / agent_id
    if not agent_dir.exists():
        return {"total": 0, "success": 0, "warn": 0, "fail": 0}

    files = list(agent_dir.glob("*.md"))
    stats = {"total": len(files), "success": 0, "warn": 0, "fail": 0}

    for f in files:
        content = f.read_text()
        if "**Status**: SUCCESS" in content:
            stats["success"] += 1
        elif "**Status**: WARN" in content:
            stats["warn"] += 1
        elif "**Status**: FAIL" in content:
            stats["fail"] += 1

    return stats


def _format_dict(obj: Any, indent: int = 0) -> str:
    """Format dict/list for readable markdown."""
    if isinstance(obj, dict):
        lines = []
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                lines.append(f"{'  ' * indent}**{k}**:")
                lines.append(_format_dict(v, indent + 1))
            else:
                lines.append(f"{'  ' * indent}- **{k}**: {v}")
        return "\n".join(lines)
    elif isinstance(obj, list):
        lines = []
        for i, item in enumerate(obj[:5]):  # Limit to first 5
            if isinstance(item, (dict, list)):
                lines.append(f"{'  ' * indent}[{i}]:")
                lines.append(_format_dict(item, indent + 1))
            else:
                lines.append(f"{'  ' * indent}- {item}")
        if len(obj) > 5:
            lines.append(f"{'  ' * indent}... and {len(obj) - 5} more")
        return "\n".join(lines)
    else:
        return str(obj)
