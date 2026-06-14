# Cloudera AI Agents

Lightweight discovery, quality, and governance platform for CDP data assets. Six autonomous agents (different agentic patterns) operate over Iceberg, Kafka, and Ozone, with full transparency into every LLM call and decision.

- **Backend**: FastAPI on `:8000` (uvicorn, auto-reload in dev)
- **Frontend**: React + Vite on `:5173`
- **LLM**: Ollama locally (`:11434`) or Cloudera AI Inference in prod
- **Code**: `02_backend/` (Python), `03_frontend/` (TypeScript)

---

## Run it

```bash
# Prereqs: Ollama running on :11434, .env populated (copy from .env.example)
python launch.py
```

`launch.py` starts both backend + frontend. Open http://localhost:5173.

Env modes:
- **Local dev** — Vite dev server on `:5173`, backend on `:8000`
- **Cloudera AI (CDSW)** — frontend pre-built into `dist/`, FastAPI serves it on `$CDSW_APP_PORT`

Key files:
- `launch.py` — unified launcher
- `02_backend/app.py` — FastAPI entry, lifespan handles Knox JWT refresh + Schema Registry warm-cache
- `02_backend/routers/` — `agents`, `health`, `nl_to_code`, `registry`, `knox`, `pipeline`
- `03_frontend/src/components/AgentPanel.tsx` — main agent UI with transparency view

---

## The 6 Agents

| # | Agent              | Pattern        | Tagline       | What it does                                          |
|---|--------------------|----------------|---------------|-------------------------------------------------------|
| 1 | **Source Scout**     | ReAct          | AUTO-DISCOVER | Finds and profiles Iceberg tables + Kafka topics      |
| 2 | **Pipeline Builder** | Tool-Use       | AUTO-CONFIG   | Generates NiFi flows / Flink SQL / Kafka Connect      |
| 3 | **Quality Guardian** | Evaluator      | REAL-TIME QA  | Scores completeness/uniqueness, auto-remediates       |
| 4 | **Pipeline Healer**  | Reactive       | SELF-HEALING  | State-machine monitor, auto-heal, escalate            |
| 5 | **Semantic Mapper**  | Intelligence   | NL → METRICS  | Embeddings to map fields, detect naming conflicts     |
| 6 | **Metadata Curator** | Policy Engine  | AUTO-GOVERN   | PII scan, sensitivity classification, owner/policy    |

Each lives in `02_backend/agents/<name>/agent.py` and is ~200–300 lines. All inherit from `agents/base_agent.py` and log every decision via `agents/decision_store/logger.py` to `decisions/<agent_id>/<timestamp>__<type>.md` (Markdown + embedded JSON for compliance + self-learning).

### Pattern reference

| Pattern        | When to use                              | LLM cost | Speed |
|----------------|------------------------------------------|----------|-------|
| ReAct          | Multi-step reasoning, exploratory        | High     | Slow  |
| Tool-Use       | Deterministic generation, templates      | Low      | Fast  |
| Evaluator      | Scoring/validation, rule-based           | None     | Fast  |
| Reactive       | Event-driven, state transitions          | None     | RT    |
| Intelligence   | Similarity, embeddings                   | Med      | Med   |
| Policy Engine  | Compliance, rules                        | None     | Fast  |

---

## Metadata Curator — Hierarchical vs ReAct

The Metadata Curator is implemented in **two parallel styles** so they can be compared in the UI side-by-side. Both classify table sensitivity (CONFIDENTIAL / RESTRICTED / INTERNAL / PUBLIC) but differ in granularity:

- **Hierarchical** (`hierarchical_supervisor.py`) — 1 unified system prompt, 1 LLM call per table. Fast, holistic, scales to 100+ tables.
- **ReAct** (`agent.py`) — 3 specialized system prompts (Field Analysis → PII Detection → Sensitivity Classification), 4+ LLM calls per table. Full step-by-step reasoning visible.

The UI's **📋 SYSTEM PROMPT COMPARISON** panel (top of `AgentPanel.tsx`) shows both prompts and lets users expand and compare. Backend logs every Ollama call as `[LLM CALL] System: … User: … Response: …`.

### Transparency events emitted by the Curator

| Event                | Agent         | Shows                                                        |
|----------------------|---------------|--------------------------------------------------------------|
| `discovery_matching` | Hierarchical  | Which N tables were checked, which K matched, and why        |
| `field_detail`       | ReAct         | Per-field type (PII/Identifier/Geo/…), confidence, reason    |
| `field_analysis`     | ReAct         | Summary of all fields for a table                            |
| `metadata_generated` | ReAct         | Final JSON metadata: sensitivity, PII count, owner, policy   |

---

## Architecture

```
                                            ┌─────────────────────┐
   React (Vite) ─── SSE ─────► FastAPI ────►│ Agents (6)          │
   :5173                       :8000        │ ├─ ReAct loop       │
                                            │ ├─ Tool-Use         │
                                            │ ├─ Evaluator        │
                                            │ ├─ Reactive FSM     │
                                            │ ├─ Embeddings       │
                                            │ └─ Policy rules     │
                                            └─────┬───────────────┘
                                                  │
                              ┌───────────────────┼────────────────────┐
                              ▼                   ▼                    ▼
                         Ollama (LLM)     PyIceberg/REST         Kafka + Schema
                         :11434           Catalog (Knox)         Registry
                                          + Ozone S3A
```

### Architecture principles

- **Zero compute in container** — backend never runs queries; it generates SQL/PySpark for Impala/Trino/Spark to execute
- **REST Catalog routing** — Iceberg writes use `STORED AS ICEBERG` flag → Cloudera REST Catalog (not HMS)
- **State-light frontend** — two-pane master-detail, status via SSE
- **Self-healing** — failed external calls (PagerDuty/Slack) queued in `retry_queue.py` with exponential backoff
- **Self-learning** — `get_decision_stats(agent_id)` exposes success rate; agents adjust thresholds from accuracy trends

### Backend layout

```
02_backend/
├── app.py                          FastAPI entry + lifespan
├── config.py                       Env config
├── routers/                        agents, health, nl_to_code, registry, knox, pipeline
├── agents/
│   ├── base_agent.py
│   ├── decision_store/             logger.py, retry_queue.py
│   ├── source_scout/               react_agent.py + agent.py
│   ├── pipeline_builder/agent.py
│   ├── quality_guardian/agent.py
│   ├── pipeline_healer/agent.py
│   ├── semantic_mapper/agent.py
│   └── metadata_curator/
│       ├── agent.py                ReAct (3 specialized prompts)
│       └── hierarchical_supervisor.py  Hierarchical (1 unified prompt)
└── tools/                          iceberg, kafka, ozone, knox helpers
```

### Frontend layout

```
03_frontend/src/
├── App.tsx                         App shell, 64px icon nav
├── components/
│   ├── AgentPanel.tsx              Main agent UI + SystemPromptComparison
│   ├── AgentDashboard/             Fleet grid
│   └── SourceScout/                Two-pane discovery (list + 3-tab detail)
├── hooks/                          useDiscovery (SSE), useWorkspace, useKnoxStatus
└── constants/design.ts             Cloudera color tokens
```

---

## API surface (selected)

```
GET   /api/models                            List Ollama models
GET   /api/agents                            List configured agents
GET   /api/system/knox-status                Knox Gateway config check

POST  /api/agents/quality-check/generate     Returns Impala/Trino/Spark SQL
GET   /api/agents/quality-check/results      Latest run, scores per check
POST  /api/agents/quality-check/execute      SSE stream of progress
```

Quality scoring: Volume = info-only · Completeness Pass <5% null / Warn 5–20% / Fail ≥20% · Overall = avg of non-info checks × 100.

Engine dialect rules:

| Aspect       | Impala               | Trino                       | Spark            |
|--------------|----------------------|-----------------------------|------------------|
| Table quote  | backticks            | double-quotes               | backticks        |
| CREATE TABLE | `STORED AS ICEBERG`  | `WITH (format = 'ICEBERG')` | `USING ICEBERG`  |

---

## Decision logging

Every agent decision is written to `decisions/<agent_id>/<YYYY-MM-DD_HH-MM-SS>__<decision_type>.md` with this shape:

```markdown
# AGENT_ID — decision_type
**Status**: SUCCESS|WARN|FAIL
**Time**: ISO timestamp
## Decision   (inputs)
## Output     (results)
## Metadata   (confidence, accuracy, custom)
## Embedded Record    { JSON record for machine querying }
```

Query via `from agents.decision_store import get_decision_stats; get_decision_stats("pipeline_builder")` → `{total, success, warn, fail}`.

---

## Environment (`.env`)

Copy `.env.example` to `.env`. Key vars:

```
CLOUDERA_AI_URL=http://localhost:11434/v1         # Ollama
CLOUDERA_AI_MODEL=llama3.2:latest
CLOUDERA_AI_KEY=ollama

ICEBERG_CATALOG_TYPE=hadoop|rest|hive
ICEBERG_CATALOG_URI=
ICEBERG_WAREHOUSE=/path/to/iceberg-warehouse

KNOX_JWT=                                          # or KNOX_LOGIN_URL/USERNAME/PASSWORD for auto-refresh
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
OZONE_ENDPOINT=http://localhost:9878
HDFS_WEBHDFS_URL=http://localhost:9870
```

Knox JWT auto-refreshes every 60s via the FastAPI lifespan if `KNOX_LOGIN_URL` is set. Schema Registry warm-indexes on startup (30s timeout) if `SCHEMA_REGISTRY_URL` is set.

---

## Design tokens

Backgrounds: cdp-950 (#0B1520) → cdp-900 → cdp-800 → cdp-700 borders. Primary action: `cloudera` (#0088CC). Text primary `#c8d8e8` / secondary `#6a8fa8`. Status: pass green, warn amber, fail red, info blue. Font Inter / Segoe UI, 12px minimum, weights 400/600/700.

---

## Quick usage

```python
# Source Scout (ReAct)
from agents.source_scout.react_agent import run_source_scout_react
async for event in run_source_scout_react("Find revenue tables"):
    print(event["type"], event.get("content"))

# Pipeline Builder (Tool-Use)
from agents.pipeline_builder.agent import PipelineBuilderAgent
async for event in PipelineBuilderAgent().run("Create Kafka → Iceberg pipeline"):
    print(event)

# Quality Guardian (Evaluator)
from agents.quality_guardian.agent import QualityGuardianAgent
async for event in QualityGuardianAgent().run("Validate quality", table_name="gold.customers"):
    print(event)

# Stats across agents
from agents.decision_store import get_decision_stats
for a in ["source_scout","pipeline_builder","quality_guardian","pipeline_healer","semantic_mapper","metadata_curator"]:
    print(a, get_decision_stats(a))
```
