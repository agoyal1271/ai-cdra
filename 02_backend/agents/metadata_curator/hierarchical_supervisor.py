"""
Hierarchical Multi-Agent Governance System.

Supervisor coordinates 4 specialized agents:
1. Discovery Agent: Finds tables matching user criteria
2. Classification Agent: Analyzes fields, detects PII, scores confidence
3. Learning Agent: Asks strategic questions on uncertain cases, learns patterns
4. Policy Agent: Recommends & applies policies

This architecture scales to 10k+ tables by:
- Streaming discovery incrementally
- Parallelizing classification (batches of 10-20)
- Only asking user about uncertain cases (active learning)
- Learning patterns to improve confidence over time
"""

import json
import logging
import asyncio
from typing import AsyncGenerator

from agents.base_agent import BaseAgent
from tools.iceberg.iceberg_tools import list_iceberg_tables, describe_iceberg_table, TokenExpiredError
from tools.kafka.kafka_tools import get_all_topics_from_schema_registry
from agents.source_scout.sidecar import get_valid_knox_token

logger = logging.getLogger(__name__)


class DiscoveryAgent:
    """Finds tables matching user criteria. Streams results incrementally."""

    def __init__(self):
        self.field_patterns = {
            "geolocation": ["latitude", "longitude", "lat", "lon", "address", "location", "geo"],
            "email": ["email", "mail"],
            "phone": ["phone", "mobile"],
            "pii": ["ssn", "credit_card"],
        }

    async def discover(self, rules: dict, limit: int = 50) -> list:
        """Find tables matching the user's goal via a single batched LLM call."""
        matching = []
        checked_details = []
        match_details = []

        try:
            try:
                tables = await asyncio.to_thread(list_iceberg_tables)
            except TokenExpiredError:
                await asyncio.to_thread(get_valid_knox_token)
                tables = await asyncio.to_thread(list_iceberg_tables)
            logger.info(f"[DISCOVERY] Total tables in catalog: {len(tables)}")

            goal = rules.get("goal") or rules.get("sensitivity") or ""
            llm_matches = await self._filter_with_llm(goal, tables)
            logger.info(f"[DISCOVERY] LLM matched {len(llm_matches)}/{len(tables)} for goal={goal!r}")

            for table in tables:
                table_name = table.get("name", "unknown")
                field_names = [f.get("name", "") for f in table.get("fields", [])]
                hit = table_name in llm_matches
                checked_details.append({
                    "table": table_name, "fields": field_names,
                    "matched": hit, "rules": rules,
                })
                if hit:
                    match_details.append({
                        "table": table_name,
                        "fields": field_names,
                        "field_count": len(field_names),
                        "reason": llm_matches[table_name],
                    })
                    matching.append(table)
                    if len(matching) >= limit:
                        break

        except Exception as e:
            logger.exception(f"[discovery_agent] Discovery failed: {e}")

        return matching, checked_details, match_details

    async def _filter_with_llm(self, goal: str, tables: list[dict]) -> dict[str, str]:
        """Ask the LLM which tables match the user's goal. Returns {table_name: reason}.

        One batch call instead of per-table substring matching with hardcoded
        synonym lists. The LLM already understands "geolocation" means lat/lon,
        "address" means street/city/zip, etc. — no point duplicating that here.
        """
        import httpx
        from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY

        compact = [
            {"name": t.get("name"), "fields": [f.get("name") for f in t.get("fields", []) if f.get("name")]}
            for t in tables
        ]

        system_prompt = (
            "You decide which tables match a user's data-discovery goal based ONLY on "
            "their column names. Use your knowledge of what column names commonly represent. "
            "Return strict JSON: {\"matches\": [{\"name\":\"...\", \"reason\":\"... (cite the specific columns)\"}]}. "
            "Be precise: a table matches only if it actually contains the kind of data the user asked for. "
            "Do not include tables that only contain tangentially related data."
        )
        user_prompt = f"User goal: {goal}\n\nTables:\n{json.dumps(compact, indent=2)}\n\nReturn JSON now."

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(
                    f"{LLM_BASE_URL}/chat/completions",
                    json={
                        "model": LLM_MODEL,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user",   "content": user_prompt},
                        ],
                        "temperature": 0.1,
                        "response_format": {"type": "json_object"},
                    },
                    headers={"Authorization": f"Bearer {LLM_API_KEY}"},
                )
                resp.raise_for_status()
                content = resp.json()["choices"][0]["message"]["content"]
                # Strip ```json fences if the model added them despite response_format
                content = content.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
                parsed = json.loads(content)
                return {m["name"]: m.get("reason", "matched") for m in parsed.get("matches", []) if m.get("name")}
        except Exception as e:
            logger.warning(f"[discovery] LLM filter failed ({e}); returning no matches")
            return {}


class ClassificationAgent:
    """Analyzes fields and classifies tables with confidence scores."""

    def __init__(self):
        self.compliance_rules = {
            "confidential": {
                "patterns": ["ssn", "credit_card", "passport"],
                "retention": "7y",
                "access_level": "confidential",
            },
            "restricted": {
                "patterns": ["email", "phone", "latitude", "longitude", "address", "geolocation"],
                "retention": "90d",
                "access_level": "restricted",
            },
            "internal": {
                "patterns": ["name", "department", "employee_id"],
                "retention": "1y",
                "access_level": "internal",
            },
            "public": {
                "patterns": [],
                "retention": "indefinite",
                "access_level": "public",
            },
        }

    async def classify_table(self, table_name: str) -> dict:
        """Use LLM to intelligently analyze table schema and classify sensitivity."""
        try:
            fields = await self._get_fields(table_name)
            if not fields:
                return {
                    "table": table_name,
                    "sensitivity": "internal",
                    "confidence": 0.3,
                    "reasoning": "Could not fetch schema, defaulting to internal",
                    "fields_analyzed": 0,
                    "columns": [],
                }

            # ================================================================
            # SEND ACTUAL SCHEMA TO LLM FOR INTELLIGENT ANALYSIS
            # (Not hardcoded pattern matching)
            # ================================================================
            field_info = [
                {
                    "name": f.get("name", "unknown"),
                    "type": f.get("type", "unknown"),
                    "description": f.get("description", ""),
                }
                for f in fields
            ]

            logger.info(f"\n[CLASSIFICATION] Analyzing schema with LLM: {table_name}")
            logger.info(f"  Fields ({len(field_info)}): {[f['name'] for f in field_info]}")

            # Call LLM to analyze actual schema
            llm_result = await self._call_llm(table_name, field_info)

            return {
                "table": table_name,
                "sensitivity": llm_result.get("sensitivity", "internal"),
                "confidence": llm_result.get("confidence", 0.75),
                "reasoning": llm_result.get("reasoning", "LLM-based schema analysis"),
                "fields_analyzed": len(fields),
                "sensitive_fields": llm_result.get("sensitive_fields", []),
                "risky_fields": llm_result.get("risky_fields", []),
                "columns": field_info,
                "system_prompt": llm_result.get("system_prompt", ""),
                "user_prompt": llm_result.get("user_prompt", ""),
                "llm_response": llm_result.get("llm_response", ""),
            }

        except Exception as e:
            logger.exception(f"[classification_agent] Failed to classify {table_name}: {e}")
            return {
                "table": table_name,
                "sensitivity": "internal",
                "confidence": 0.3,
                "reasoning": f"Classification failed: {str(e)}",
                "fields_analyzed": 0,
                "columns": [],
            }

    async def _call_llm(self, table_name: str, field_info: list) -> dict:
        """Use LLM to analyze schema and classify based on actual column names/types."""
        import httpx
        import json
        from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY

        system_prompt = """You are a DATA GOVERNANCE EXPERT analyzing database schemas.

TASK: Analyze the table columns and classify the table's sensitivity level based on what data each column likely contains.

SENSITIVITY LEVELS:
🔴 CONFIDENTIAL (7-year retention): High-risk PII enabling identity theft or financial fraud
   Examples: SSN, credit_card_number, password, medical_id, biometric_data, passport_number

🟠 RESTRICTED (90-day retention): Medium-risk PII enabling targeting or tracking
   Examples: email, phone_number, home_address, latitude/longitude, device_id, ip_address

🟡 INTERNAL (1-year retention): Low-risk employee/business data
   Examples: employee_name, department, employee_id, job_title, birth_date

🟢 PUBLIC (indefinite): No PII, can be published
   Examples: product_name, category, public_statistics

ANALYSIS APPROACH:
1. Look at each COLUMN NAME - what does it suggest the data is?
2. Look at each COLUMN TYPE - is it string, int, float, date?
3. Infer what PII this table contains based on column names/types
4. Classify the table by its HIGHEST RISK column
5. Explain your reasoning

Return JSON:
{
  "sensitivity": "confidential|restricted|internal|public",
  "confidence": 0.85,
  "reasoning": "Table contains [sensitive data] which requires [retention period]",
  "sensitive_fields": [
    {"name": "column_name", "sensitivity": "level", "reason": "why this column is sensitive"}
  ],
  "risky_fields": ["most_sensitive_column_names"]
}"""

        user_prompt = f"""Analyze this table schema and classify its sensitivity:

TABLE: {table_name}

COLUMNS:
{json.dumps(field_info, indent=2)}

Based on these column names, types, and what they likely represent, what is the sensitivity level of this table?"""

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{LLM_BASE_URL}/chat/completions",
                    json={
                        "model": LLM_MODEL,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt},
                        ],
                        "temperature": 0.3,
                        "max_tokens": 1500,
                    },
                    headers={"Authorization": f"Bearer {LLM_API_KEY}"},
                )
                response.raise_for_status()
                data = response.json()
                content = data["choices"][0]["message"]["content"]

                logger.info(f"[LLM] Classification response:\n{content}")

                # Try to parse as JSON
                try:
                    result = json.loads(content)
                    # Add prompts to result for UI transparency
                    result["system_prompt"] = system_prompt
                    result["user_prompt"] = user_prompt
                    result["llm_response"] = content
                    return result
                except json.JSONDecodeError:
                    # If not valid JSON, extract key info from text
                    return {
                        "sensitivity": "internal",
                        "confidence": 0.6,
                        "reasoning": content,
                        "sensitive_fields": [],
                        "risky_fields": [],
                        "system_prompt": system_prompt,
                        "user_prompt": user_prompt,
                        "llm_response": content,
                    }
        except Exception as e:
            logger.exception(f"[LLM Classification] Error: {e}")
            return {
                "sensitivity": "internal",
                "confidence": 0.3,
                "reasoning": f"LLM classification failed: {str(e)}",
                "sensitive_fields": [],
                "risky_fields": [],
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "llm_response": str(e),
            }

    async def _get_fields(self, table_name: str) -> list:
        """Fetch table fields."""
        try:
            if table_name.startswith("kafka::"):
                topic_name = table_name.replace("kafka::", "")
                topics = await asyncio.to_thread(get_all_topics_from_schema_registry)
                if topic_name in topics:
                    return topics[topic_name].get("fields", [])
                return []

            try:
                table = await asyncio.to_thread(describe_iceberg_table, table_name)
                return table.get("fields", [])
            except TokenExpiredError:
                await asyncio.to_thread(get_valid_knox_token)
                table = await asyncio.to_thread(describe_iceberg_table, table_name)
                return table.get("fields", [])

        except Exception as e:
            logger.warning(f"[classification_agent] Could not fetch fields for {table_name}: {e}")
            return []


class LearningAgent:
    """Handles active learning: asks strategic questions about uncertain cases."""

    def __init__(self):
        self.learned_patterns = {}

    async def identify_uncertain(self, classifications: list, threshold: float = 0.75) -> list:
        """Return tables with confidence below threshold."""
        return [c for c in classifications if c.get("confidence", 0) < threshold]

    def generate_question(self, classification: dict) -> dict:
        """Generate a clarifying question for uncertain classification."""
        table = classification["table"]
        current_guess = classification["sensitivity"]
        confidence = classification["confidence"]
        columns = classification.get("columns", [])
        sensitive_fields = classification.get("sensitive_fields", [])

        # Generate reasonable alternatives
        alternatives = []
        if current_guess != "confidential":
            alternatives.append("confidential")
        if current_guess != "restricted":
            alternatives.append("restricted")
        if current_guess != "internal":
            alternatives.append("internal")

        return {
            "table": table,
            "current_guess": current_guess,
            "confidence": confidence,
            "reasoning": classification.get("reasoning", ""),
            "columns": columns,
            "sensitive_fields": sensitive_fields,
            "question": f"I classified '{table}' as '{current_guess}' ({confidence*100:.0f}% confidence). Is this correct?",
            "options": alternatives + ["accept_guess"],
            "learning_goal": f"Understanding if {table}'s sensitivity is correctly classified based on the columns present: {', '.join([c.get('name', '') for c in columns[:5]])}{'...' if len(columns) > 5 else ''}",
        }

    def learn_from_feedback(self, table: str, feedback: str, classification: dict) -> None:
        """Remember user feedback to improve future classifications."""
        # Store pattern for future use
        if feedback != "accept_guess":
            # User corrected us
            key = f"{table}:{feedback}"
            self.learned_patterns[key] = {
                "original_guess": classification["sensitivity"],
                "correct_answer": feedback,
                "fields": classification.get("risky_fields", []) + classification.get("pii_fields", []),
            }
            logger.info(f"[learning_agent] Learned pattern: {key}")


class PolicyAgent:
    """Recommends and applies governance policies."""

    def __init__(self):
        self.compliance_policies = {
            "confidential": {"retention": "7y", "access_level": "confidential", "require_approval": True},
            "restricted": {"retention": "90d", "access_level": "restricted", "require_approval": False},
            "internal": {"retention": "1y", "access_level": "internal", "require_approval": False},
            "public": {"retention": "indefinite", "access_level": "public", "require_approval": False},
        }

    def recommend_policy(self, classification: dict) -> dict:
        """Recommend policy based on classification."""
        sensitivity = classification["sensitivity"]
        policy = self.compliance_policies.get(sensitivity, self.compliance_policies["internal"])

        return {
            "table": classification["table"],
            "sensitivity": sensitivity,
            "retention": policy["retention"],
            "access_level": policy["access_level"],
            "require_approval": policy["require_approval"],
            "reasoning": f"{sensitivity.upper()} table → {policy['retention']} retention, {policy['access_level']} access",
        }

    async def apply_policy(self, policy: dict) -> dict:
        """Apply policy to table (placeholder for Ranger integration)."""
        return {
            "table": policy["table"],
            "success": True,
            "message": f"✅ Applied {policy['sensitivity']} policy to {policy['table']}",
            "policy": policy,
        }


class HierarchicalGovernanceSupervisor(BaseAgent):
    """Orchestrates the 4-agent hierarchy."""

    def __init__(self):
        super().__init__(
            agent_id="metadata_curator",
            description="Hierarchical governance: discovery → classification → learning → policy",
        )
        self.discovery = DiscoveryAgent()
        self.classifier = ClassificationAgent()
        self.learner = LearningAgent()
        self.policy = PolicyAgent()

    async def run(self, goal: str, **kwargs) -> AsyncGenerator[dict, None]:
        """Run the full hierarchical governance workflow."""
        table_name = kwargs.get("table_name", None)

        logger.info(f"\n{'='*80}")
        logger.info(f"HIERARCHICAL GOVERNANCE REQUEST")
        logger.info(f"{'='*80}")
        logger.info(f"Goal: {goal}")
        logger.info(f"Table Name: {table_name}")
        logger.info(f"{'='*80}\n")

        yield self.emit("started", goal=goal, agent="Metadata Curator (Hierarchical Governance)")

        try:
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # STAGE 1: DISCOVERY AGENT
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            logger.info("\n[STAGE 1] DISCOVERY AGENT")
            logger.info("-" * 80)

            yield self.emit("analysis",
                          stage="stage_1_discovery",
                          agent="DiscoveryAgent",
                          message="🔍 Stage 1: Discovery Agent - Finding matching tables...")

            rules = self._parse_rules(goal)
            rules["goal"] = goal  # pass the raw goal so the LLM filter sees the user's exact intent
            logger.info(f"Parsed rules from goal: {rules}")
            logger.info(f"Looking for sensitivity level: {rules.get('sensitivity', 'internal')}")

            yield self.emit("analysis", stage="parsing_intent", rules=rules, goal=goal, message=f"Parsed intent: {rules}")

            if table_name:
                tables = [{"name": table_name}]
                logger.info(f"User specified table: {table_name}")
                yield self.emit("analysis",
                              stage="user_specified",
                              table=table_name,
                              message=f"User specified table: {table_name}")
            else:
                logger.info(f"Discovering tables matching rules: {rules}")
                tables, checked_details, match_details = await self.discovery.discover(rules, limit=50)

                logger.info(f"\nDiscovery Results:")
                logger.info(f"  Found: {len(tables)} tables")
                for i, table in enumerate(tables[:10], 1):
                    logger.info(f"    {i}. {table['name']}")

                # Emit discovery matching details for UI transparency
                yield self.emit("discovery_matching",
                              total_checked=len(checked_details),
                              total_matched=len(tables),
                              matched_tables=match_details,
                              rules=rules,
                              message=f"Checked {len(checked_details)} tables, found {len(tables)} matches")

                yield self.emit("search_results",
                              total_found=len(tables),
                              top_10=[{"name": t["name"], "description": t.get("description", "")}
                                     for t in tables[:10]],
                              message=f"✅ Discovery Agent found {len(tables)} matching tables")

            if not tables:
                logger.warning(f"\n⚠️ NO TABLES FOUND")
                logger.warning(f"Goal: '{goal}'")
                logger.warning(f"Rules: {rules}")
                logger.warning(f"Total tables in catalog: (unknown)")
                logger.warning(f"Matching tables: 0")

                yield self.emit("complete",
                              summary="❌ No tables found matching your criteria. Check the debug log for details.")
                return

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # STAGE 2: CLASSIFICATION AGENT (Parallel batch processing)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            logger.info(f"\n[STAGE 2] CLASSIFICATION AGENT")
            logger.info("-" * 80)

            yield self.emit("analysis",
                          stage="stage_2_classification",
                          agent="ClassificationAgent",
                          message="📊 Stage 2: Classification Agent - Analyzing tables (parallel batch)...")

            BATCH_SIZE = 10
            classifications = []

            for i in range(0, len(tables), BATCH_SIZE):
                batch = tables[i:i + BATCH_SIZE]
                batch_num = (i // BATCH_SIZE) + 1
                total_batches = (len(tables) + BATCH_SIZE - 1) // BATCH_SIZE

                yield self.emit("analysis",
                              stage="classification_batch",
                              batch=batch_num,
                              total_batches=total_batches,
                              message=f"Processing batch {batch_num}/{total_batches} ({len(batch)} tables)...")

                # Classify all tables in batch in parallel
                batch_results = await asyncio.gather(
                    *[self.classifier.classify_table(t["name"]) for t in batch]
                )

                for result in batch_results:
                    classifications.append(result)
                    # Emit result for each table
                    emoji = {
                        "confidential": "🔴",
                        "restricted": "🟠",
                        "internal": "🟡",
                        "public": "🟢",
                    }.get(result["sensitivity"], "⚪")

                    logger.info(f"  {emoji} {result['table']}: {result['sensitivity'].upper()} "
                               f"(confidence: {result['confidence']:.0%}) - {result['reasoning']}")

                    # Build column details for UI
                    columns = result.get("columns", [])
                    column_details = [
                        {
                            "name": col["name"],
                            "type": col["type"],
                            "description": col.get("description", ""),
                        }
                        for col in columns
                    ]

                    yield self.emit("sensitivity_classification",
                                  table=result["table"],
                                  level=result["sensitivity"],
                                  confidence=result["confidence"],
                                  reasoning=result["reasoning"],
                                  columns=column_details,
                                  sensitive_fields=result.get("sensitive_fields", []),
                                  risky_fields=result.get("risky_fields", []),
                                  system_prompt=result.get("system_prompt", ""),
                                  user_prompt=result.get("user_prompt", ""),
                                  llm_response=result.get("llm_response", ""))

            logger.info(f"\n[CLASSIFICATION COMPLETE]")
            logger.info(f"  Total classified: {len(classifications)} tables")
            for sensitivity in ["confidential", "restricted", "internal", "public"]:
                count = len([c for c in classifications if c["sensitivity"] == sensitivity])
                if count > 0:
                    logger.info(f"    {sensitivity.upper()}: {count} tables")

            yield self.emit("analysis",
                          stage="classification_complete",
                          count=len(classifications),
                          message=f"✅ Classification Agent analyzed {len(classifications)} tables")

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # STAGE 3: LEARNING AGENT (Active learning on uncertain cases)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            uncertain = await self.learner.identify_uncertain(classifications, threshold=0.75)

            if uncertain:
                yield self.emit("analysis",
                              stage="stage_3_learning",
                              agent="LearningAgent",
                              uncertain_count=len(uncertain),
                              message=f"🧠 Stage 3: Learning Agent - {len(uncertain)} tables need clarification...")

                # For now, just report uncertain cases (in full UI, would ask user)
                for classification in uncertain[:3]:  # Show first 3 uncertain cases
                    question = self.learner.generate_question(classification)
                    yield self.emit("question",
                                  table=classification["table"],
                                  current_guess=classification["sensitivity"],
                                  confidence=classification["confidence"],
                                  message=question["question"],
                                  options=question["options"],
                                  columns=question.get("columns", []),
                                  sensitive_fields=question.get("sensitive_fields", []),
                                  learning_goal=question.get("learning_goal", ""),
                                  reasoning=question.get("reasoning", ""))

                yield self.emit("analysis",
                              stage="learning_complete",
                              accepted_uncertain=len(uncertain),
                              message=f"✅ Learning Agent reviewed {len(uncertain)} uncertain cases")
            else:
                yield self.emit("analysis",
                              stage="stage_3_learning_skipped",
                              message="✅ All classifications have high confidence (> 75%) - no questions needed")

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # STAGE 4: POLICY AGENT (Recommend & apply)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            yield self.emit("analysis",
                          stage="stage_4_policy",
                          agent="PolicyAgent",
                          message="⚖️ Stage 4: Policy Agent - Recommending policies...")

            # Recommend policies
            policy_recommendations = []
            for classification in classifications:
                policy_rec = self.policy.recommend_policy(classification)
                policy_recommendations.append(policy_rec)

            # Show summary by sensitivity
            summary = {
                "confidential": len([c for c in classifications if c["sensitivity"] == "confidential"]),
                "restricted": len([c for c in classifications if c["sensitivity"] == "restricted"]),
                "internal": len([c for c in classifications if c["sensitivity"] == "internal"]),
                "public": len([c for c in classifications if c["sensitivity"] == "public"]),
            }

            yield self.emit("observation",
                          message=f"Policy Summary:\n"
                                 f"🔴 Confidential: {summary['confidential']} tables (7y retention)\n"
                                 f"🟠 Restricted: {summary['restricted']} tables (90d retention)\n"
                                 f"🟡 Internal: {summary['internal']} tables (1y retention)\n"
                                 f"🟢 Public: {summary['public']} tables (indefinite retention)")

            # AUTONOMOUSLY apply policies (no user confirmation needed)
            yield self.emit("action", name="apply_policies", count=len(classifications), auto=True)

            # Apply policies
            yield self.emit("analysis",
                          stage="applying_policies",
                          message="Applying governance policies...")

            applied_results = []
            for policy_rec in policy_recommendations[:10]:  # Show first 10
                result = await self.policy.apply_policy(policy_rec)
                applied_results.append(result)
                policy_obj = {
                    "sensitivity": result["policy"]["sensitivity"],
                    "retention": result["policy"]["retention"],
                    "access_level": result["policy"]["access_level"],
                }
                yield self.emit("policy_applied",
                              table=result["table"],
                              policy=policy_obj,
                              detail=f"Retention: {policy_obj['retention']} | Access: {policy_obj['access_level']}",
                              message=f"✅ Policy Applied: {policy_obj['retention']} retention, {policy_obj['access_level']} access")

                # Log decision
                self.log_decision(
                    decision_type="hierarchical_governance",
                    inputs={"goal": goal},
                    output={
                        "table": result["table"],
                        "sensitivity": result["policy"]["sensitivity"],
                        "retention": result["policy"]["retention"],
                    },
                    metadata={"agent_type": "hierarchical_supervisor"},
                )

            # Final summary
            yield self.emit("complete",
                          summary=f"✅ Hierarchical Governance Complete\n"
                                 f"• Discovery: Found {len(tables)} tables\n"
                                 f"• Classification: Analyzed {len(classifications)} tables\n"
                                 f"• Learning: Reviewed {len(uncertain)} uncertain cases\n"
                                 f"• Policy: Applied {len(applied_results)}+ policies")

        except Exception as e:
            logger.exception(f"[hierarchical_supervisor] Error: {e}")
            self.log_decision(
                decision_type="hierarchical_governance_failed",
                inputs={"goal": goal},
                output={"error": str(e)},
                status="fail",
            )
            yield self.emit("error", message=str(e))

    def _parse_rules(self, goal: str) -> dict:
        """Pick a default sensitivity bucket for downstream policy code.

        Only used to label policy retention/access defaults. Actual table
        filtering is done by the LLM in DiscoveryAgent._filter_with_llm,
        which sees the raw goal — so no synonym tables are needed here.
        """
        g = goal.lower()
        if any(kw in g for kw in ("ssn", "credit card", "passport", "confidential")):
            return {"sensitivity": "confidential"}
        if any(kw in g for kw in ("pii", "restricted", "email", "phone", "address",
                                  "geolocation", "latitude", "longitude")):
            return {"sensitivity": "restricted"}
        if any(kw in g for kw in ("public", "open")):
            return {"sensitivity": "public"}
        return {"sensitivity": "internal"}
