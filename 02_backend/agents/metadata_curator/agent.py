"""
Metadata Curator Agent — ReAct with Real Catalog Discovery.

Uses actual Iceberg catalog + Kafka via Knox authentication.
Discovers real tables, analyzes real schemas, applies AI governance.
"""

import json
import logging
import asyncio
from typing import AsyncGenerator

from agents.base_agent import BaseAgent
from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY
from tools.iceberg.iceberg_tools import list_iceberg_tables, describe_iceberg_table, TokenExpiredError
from tools.kafka.kafka_tools import get_all_topics_from_schema_registry
from agents.source_scout.sidecar import get_valid_knox_token

logger = logging.getLogger(__name__)


class MetadataCuratorAgent(BaseAgent):
    """ReAct agent with real catalog discovery + AI-driven governance."""

    def __init__(self):
        super().__init__(
            agent_id="metadata_curator",
            description="Real catalog discovery + AI-driven governance",
        )
        self.compliance_policies = {
            "confidential": {"retention": "7y", "access_level": "confidential"},
            "restricted": {"retention": "90d", "access_level": "restricted"},
            "internal": {"retention": "1y", "access_level": "internal"},
            "public": {"retention": "indefinite", "access_level": "public"},
        }

    async def run(self, goal: str, **kwargs) -> AsyncGenerator[dict, None]:
        """
        ReAct loop with REAL catalog discovery:
        1. Understand user intent
        2. Search REAL Iceberg + Kafka catalogs
        3. Show top suggestions
        4. Analyze selected table
        5. Apply AI-driven governance
        """
        table_name = kwargs.get("table_name", None)
        yield self.emit("started", goal=goal, agent="Metadata Curator (Real Catalog)")

        try:
            # THOUGHT: Parse user intent
            yield self.emit("thought", message="Analyzing user request to understand intent")
            user_intent = await self._understand_intent(goal)
            yield self.emit("analysis",
                          stage="intent_analysis",
                          intent=user_intent["intent"],
                          keywords=user_intent["keywords"],
                          reasoning=user_intent["reasoning"])

            # ACTION: Check if user provided table name
            if table_name:
                yield self.emit("thought", message=f"User specified table '{table_name}' - using it directly")
                yield self.emit("action", name="use_provided_table", table=table_name)
            else:
                # ACTION: Search REAL catalog
                yield self.emit("thought", message="Searching Iceberg catalog and Kafka topics for matches...")
                yield self.emit("action", name="search_real_catalog", criteria=user_intent["keywords"])

                matching_tables = await self._search_real_catalog(user_intent)
                top_10 = matching_tables[:10]

                yield self.emit("search_results",
                              total_found=len(matching_tables),
                              top_10=top_10,
                              message=f"Found {len(matching_tables)} tables/topics matching your criteria. Showing top 10:")

                # OBSERVATION: Did we find any tables?
                if not top_10:
                    yield self.emit("observation",
                                  message=f"No tables found matching '{user_intent['intent']}'",
                                  suggestion="Try different keywords or use exact table name")
                    yield self.emit("complete", summary="No matching tables found in catalog")
                    return

                # AUTONOMOUSLY analyze all found tables (don't ask user which one)
                yield self.emit("observation",
                              message=f"Found {len(matching_tables)} tables. Analyzing all of them...",
                              tables_found=[t["name"] for t in top_10])

            # AUTONOMOUSLY analyze ALL found tables
            tables_to_analyze = top_10 if not table_name else [{"name": table_name}]
            results_summary = []

            for idx, table_info in enumerate(tables_to_analyze, 1):
                table_name = table_info["name"]

                # ═══════════════════════════════════════════════════════════════════════
                # TABLE HEADER: Clear table identification
                # ═══════════════════════════════════════════════════════════════════════
                yield self.emit("thought",
                              message=f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                yield self.emit("thought",
                              message=f"[{idx}/{len(tables_to_analyze)}] ANALYZING TABLE: {table_name}")
                yield self.emit("thought",
                              message=f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

                # STEP 1: FETCH SCHEMA
                yield self.emit("action",
                              name="fetch_schema",
                              table=table_name,
                              progress=f"{idx}/{len(tables_to_analyze)}")
                yield self.emit("thought", message="Fetching table schema from catalog...")

                fields = await self._get_table_fields(table_name)

                if not fields:
                    yield self.emit("observation",
                                  message=f"⚠️ Could not fetch schema for '{table_name}', skipping")
                    continue

                # Display schema metadata
                field_names = [f.get("name", "") for f in fields]
                field_types = {f.get("name", ""): f.get("type", "unknown") for f in fields}

                yield self.emit("observation",
                              message=f"✅ Schema fetched: {len(fields)} fields",
                              fields_found=field_names,
                              field_types=field_types)

                # STEP 2: FIELD-BY-FIELD ANALYSIS
                yield self.emit("thought",
                              message=f"Analyzing each field for metadata and data types...")

                field_analysis = await self._ai_analyze_fields(table_name, fields)

                # Show detailed field analysis
                if field_analysis.get("fields"):
                    yield self.emit("field_analysis",
                                  table=table_name,
                                  total_fields=len(field_analysis['fields']),
                                  fields=field_analysis['fields'],
                                  message=f"Field-by-field analysis ({len(field_analysis['fields'])} fields):")

                    for field_info in field_analysis["fields"]:
                        field_name = field_info.get("name", "unknown")
                        detected_as = field_info.get("detected_as", "unknown")
                        confidence = field_info.get("confidence", 0)
                        reason = field_info.get("reason", "")

                        emoji = {
                            "PII": "🔴",
                            "Geolocation": "🗺️",
                            "Identifier": "🆔",
                            "Financial": "💰",
                            "Temporal": "📅",
                            "Other": "⚪"
                        }.get(detected_as, "⚪")

                        yield self.emit("field_detail",
                                      table=table_name,
                                      field_name=field_name,
                                      field_type=detected_as,
                                      confidence=confidence,
                                      reason=reason,
                                      emoji=emoji,
                                      message=f"{emoji} {field_name}: {detected_as} ({confidence:.0%}) - {reason}")

                # STEP 3: PII DETECTION
                yield self.emit("thought", message=f"Scanning for PII (Personally Identifiable Information)...")
                pii_detection = await self._ai_detect_pii(field_analysis)

                pii_fields = pii_detection.get("pii_fields", [])
                if pii_fields:
                    yield self.emit("pii_detected",
                                  table=table_name,
                                  count=len(pii_fields),
                                  fields=pii_fields,
                                  reasoning=pii_detection.get("reasoning", ""),
                                  message=f"⚠️ PII DETECTED: {len(pii_fields)} sensitive fields found")

                    # Show each PII field
                    for pii_field in pii_fields[:5]:
                        yield self.emit("observation",
                                      message=f"  🔴 {pii_field.get('name', 'unknown')}: {pii_field.get('pii_type', 'unknown')} (Risk: {pii_field.get('risk', 'unknown')})")
                else:
                    yield self.emit("observation",
                                  message=f"✅ No PII fields detected")

                # STEP 4: SENSITIVITY CLASSIFICATION
                yield self.emit("thought", message=f"Determining sensitivity level based on PII findings...")
                sensitivity = await self._ai_classify_sensitivity(pii_detection)

                sensitivity_emoji = {
                    "confidential": "🔴",
                    "restricted": "🟠",
                    "internal": "🟡",
                    "public": "🟢"
                }.get(sensitivity.get("level", "internal"), "⚪")

                # Prepare column details for UI
                column_details = [
                    {
                        "name": f.get("name", "unknown"),
                        "type": f.get("type", "unknown"),
                        "description": f.get("description", ""),
                    }
                    for f in fields
                ]

                yield self.emit("sensitivity_classification",
                              table=table_name,
                              level=sensitivity.get("level", "internal"),
                              reasoning=sensitivity.get("reasoning", ""),
                              confidence=sensitivity.get("confidence", 0.85),
                              columns=column_details,
                              sensitive_fields=pii_detection.get("pii_fields", []),
                              message=f"{sensitivity_emoji} Classification: {sensitivity.get('level', 'internal').upper()} "
                                     f"(confidence: {sensitivity.get('confidence', 0.85):.0%})")

                # STEP 5: OWNER RECOMMENDATION
                yield self.emit("thought", message=f"Recommending appropriate data owner/steward...")
                owner_suggestion = await self._ai_suggest_owner(table_name, sensitivity, pii_detection)

                yield self.emit("owner_suggestion",
                              table=table_name,
                              owner=owner_suggestion.get("owner", "data-team@company.com"),
                              reasoning=owner_suggestion.get("reasoning", ""),
                              alternatives=owner_suggestion.get("alternatives", []),
                              message=f"👤 Owner: {owner_suggestion.get('owner', 'data-team@company.com')}")

                # STEP 6: POLICY APPLICATION
                yield self.emit("thought", message=f"Applying governance policy...")
                policy = self._apply_policy(sensitivity.get("level", "internal"))

                yield self.emit("policy_applied",
                              table=table_name,
                              policy=policy,
                              detail=f"Retention: {policy['retention']} | Access: {policy['access_level']}",
                              message=f"✅ Policy Applied: {policy['retention']} retention, {policy['access_level']} access")

                # Store result
                results_summary.append({
                    "table": table_name,
                    "sensitivity": sensitivity.get("level", "internal"),
                    "owner": owner_suggestion.get("owner", "data-team@company.com"),
                    "policy": policy["access_level"],
                })

                # STEP 7: GENERATE METADATA
                yield self.emit("thought", message=f"Generating metadata for {table_name}...")
                metadata_generated = await self._generate_metadata(table_name, field_analysis, pii_detection, sensitivity)

                yield self.emit("metadata_generated",
                              table=table_name,
                              metadata=metadata_generated,
                              message=f"✅ Metadata generated: {len(metadata_generated.get('fields', []))} fields documented")

                # Log decision
                self.log_decision(
                    decision_type="metadata_governance",
                    inputs={"goal": goal, "table": table_name, "intent": user_intent["intent"]},
                    output={
                        "pii_fields": len(pii_detection.get("pii_fields", [])),
                        "sensitivity": sensitivity.get("level", "internal"),
                        "owner": owner_suggestion.get("owner", "data-team@company.com"),
                        "policy": policy["access_level"],
                    },
                    metadata={
                        "intent_analysis": user_intent,
                        "field_analysis": field_analysis,
                        "pii_reasoning": pii_detection.get("reasoning", ""),
                        "sensitivity_reasoning": sensitivity.get("reasoning", ""),
                        "owner_reasoning": owner_suggestion.get("reasoning", ""),
                        "metadata_generated": metadata_generated,
                    },
                )

            # Final summary with detailed breakdown
            summary_text = f"\n{'='*80}\n"
            summary_text += f"✅ ReAct Analysis Complete\n"
            summary_text += f"{'='*80}\n\n"
            summary_text += f"Analyzed {len(results_summary)} tables:\n\n"

            # Group by sensitivity
            by_sensitivity = {}
            for result in results_summary:
                sens = result['sensitivity']
                if sens not in by_sensitivity:
                    by_sensitivity[sens] = []
                by_sensitivity[sens].append(result)

            emoji_map = {"confidential": "🔴", "restricted": "🟠", "internal": "🟡", "public": "🟢"}
            for sensitivity in ["confidential", "restricted", "internal", "public"]:
                if sensitivity in by_sensitivity:
                    tables = by_sensitivity[sensitivity]
                    summary_text += f"{emoji_map[sensitivity]} {sensitivity.upper()} ({len(tables)} tables):\n"
                    for result in tables:
                        summary_text += f"    • {result['table']}\n"
                        summary_text += f"      Owner: {result['owner']}\n"
                        summary_text += f"      Policy: {result['policy']}\n"
                    summary_text += "\n"

            summary_text += f"{'='*80}"

            yield self.emit("complete", summary=summary_text)

        except Exception as e:
            logger.exception(f"[metadata_curator] Error: {e}")
            self.log_decision(
                decision_type="metadata_governance_failed",
                inputs={"goal": goal},
                output={"error": str(e)},
                status="fail",
            )
            yield self.emit("error", message=str(e))

    async def _understand_intent(self, goal: str) -> dict:
        """Use LLM to understand user intent from natural language."""
        prompt = f"""Understand what the user is asking and extract their intent.

User Request: "{goal}"

Analyze the request and respond in JSON:
{{
  "intent": "brief statement of what user wants (e.g., 'find tables with geolocation data', 'find PII tables')",
  "keywords": ["keyword1", "keyword2"],
  "reasoning": "explain why you interpreted it this way"
}}"""

        response = await self._call_llm(prompt)
        try:
            return json.loads(response)
        except:
            return {
                "intent": goal,
                "keywords": goal.split(),
                "reasoning": "Could not parse intent, using raw keywords"
            }

    async def _search_real_catalog(self, intent: dict) -> list:
        """Search REAL Iceberg catalog and Kafka topics by name, description, AND fields."""
        keywords = intent.get("keywords", [])
        search_term = " ".join(keywords).lower()

        matching_tables = []

        # Define field patterns for common data types
        field_patterns = {
            "geolocation": ["latitude", "longitude", "lat", "lon", "address", "location", "geo"],
            "email": ["email", "mail", "contact_email"],
            "phone": ["phone", "mobile", "telephone", "contact_phone"],
            "pii": ["ssn", "social_security", "passport", "credit_card"],
            "date": ["date", "timestamp", "created_at", "updated_at"],
            "customer": ["customer", "client", "account"],
        }

        try:
            # Refresh Knox token if needed
            try:
                tables = await asyncio.to_thread(list_iceberg_tables)
            except TokenExpiredError:
                logger.warning("[metadata_curator] Knox JWT expired, refreshing...")
                await asyncio.to_thread(get_valid_knox_token)
                tables = await asyncio.to_thread(list_iceberg_tables)

            # Filter tables by keywords in name/description AND field inspection
            for table in tables:
                table_str = f"{table['name']} {table.get('description', '')}".lower()
                field_names = [f.get("name", "").lower() for f in table.get("fields", [])]
                field_str = " ".join(field_names).lower()

                # Match by table name/description OR by field patterns
                matches_name = any(keyword.lower() in table_str for keyword in keywords)

                # Check if any keyword matches field patterns
                matches_fields = False
                for keyword in keywords:
                    kw_lower = keyword.lower()
                    # Direct field name match
                    if any(kw_lower in fn for fn in field_names):
                        matches_fields = True
                        break
                    # Pattern-based match (e.g., "geolocation" → latitude, longitude)
                    if kw_lower in field_patterns:
                        patterns = field_patterns[kw_lower]
                        if any(pattern in field_str for pattern in patterns):
                            matches_fields = True
                            break

                if matches_name or matches_fields:
                    matching_tables.append({
                        "name": table["name"],
                        "description": table.get("description", ""),
                        "type": "iceberg",
                        "row_count": table.get("row_count", 0),
                        "size_bytes": table.get("size_bytes", 0),
                        "matching_fields": [fn for fn in field_names if any(k.lower() in fn for k in keywords)],
                    })

        except Exception as e:
            logger.warning(f"[metadata_curator] Iceberg scan failed: {e}")

        try:
            # Search Kafka topics
            kafka_topics = await asyncio.to_thread(get_all_topics_from_schema_registry)
            if kafka_topics:
                for topic_name, info in kafka_topics.items():
                    topic_str = f"{topic_name} {' '.join([f['name'] for f in info.get('fields', [])])}".lower()
                    if any(keyword.lower() in topic_str for keyword in keywords):
                        matching_tables.append({
                            "name": f"kafka::{topic_name}",
                            "description": f"Kafka topic with {len(info.get('fields', []))} fields",
                            "type": "kafka",
                            "field_count": len(info.get("fields", [])),
                        })

        except Exception as e:
            logger.warning(f"[metadata_curator] Kafka scan failed: {e}")

        # Sort by relevance
        return sorted(matching_tables,
                     key=lambda x: sum(1 for kw in keywords if kw.lower() in x["name"].lower()),
                     reverse=True)

    async def _get_table_fields(self, table_name: str) -> list:
        """Get REAL table schema from Iceberg catalog."""
        try:
            # Check if it's a Kafka topic
            if table_name.startswith("kafka::"):
                topic_name = table_name.replace("kafka::", "")
                topics = await asyncio.to_thread(get_all_topics_from_schema_registry)
                if topic_name in topics:
                    return topics[topic_name].get("fields", [])
                return []

            # Query Iceberg
            try:
                table = await asyncio.to_thread(describe_iceberg_table, table_name)
                return table.get("fields", [])
            except TokenExpiredError:
                logger.warning("[metadata_curator] Knox JWT expired, refreshing...")
                await asyncio.to_thread(get_valid_knox_token)
                table = await asyncio.to_thread(describe_iceberg_table, table_name)
                return table.get("fields", [])

        except Exception as e:
            logger.warning(f"[metadata_curator] Could not fetch schema for {table_name}: {e}")
            return []

    async def _ai_analyze_fields(self, table_name: str, fields: list) -> dict:
        """Use LLM to analyze REAL field names and types."""
        field_list = json.dumps([{"name": f.get("name"), "type": f.get("type")} for f in fields], indent=2)

        system_prompt = """You are a DATA METADATA ANALYST specializing in field classification.

TASK: Analyze database field names and types to infer what kind of data each represents.

FIELD TYPES TO DETECT:
🆔 IDENTIFIER: Unique IDs, accounts, customer numbers
🔴 PII (Personally Identifiable Information):
   - Email, phone, SSN, credit card, passport, driver license
   - Name + address combinations
   - Biometric data, medical records
🗺️  GEOLOCATION: Latitude, longitude, address, country, city, postal code
💰 FINANCIAL: Amount, price, balance, credit limit, salary
📅 TEMPORAL: Date, timestamp, created_at, updated_at, time
⚪ OTHER: Categories, flags, statuses, descriptions

ANALYSIS METHOD:
1. Read field NAME - what does it suggest?
2. Read field TYPE - is it string/int/float/date?
3. Look for PII indicators in the name
4. Infer what real-world data this likely contains
5. Provide confidence level (0-1)

Return JSON with confidence scores and reasoning."""

        user_prompt = f"""Analyze these REAL table fields from '{table_name}' and classify what data type each represents.

Fields:
{field_list}

For each field, determine if it represents:
- Identifier (ID, account, etc.)
- PII (email, phone, SSN, credit card)
- Geolocation (latitude, longitude, address, country)
- Financial (amount, price, balance)
- Temporal (date, timestamp)
- Other

Respond in JSON format:
{{
  "fields": [
    {{"name": "field_name", "detected_as": "type", "confidence": 0.95, "reason": "why"}}
  ]
}}"""

        response = await self._call_llm(user_prompt, system_prompt=system_prompt)
        try:
            result = json.loads(response)
            # Attach prompts for UI transparency
            result["_system_prompt"] = system_prompt
            result["_user_prompt"] = user_prompt
            return result
        except:
            return {"fields": [{"name": f.get("name"), "detected_as": "unknown"} for f in fields]}

    async def _ai_detect_pii(self, field_analysis: dict) -> dict:
        """Use LLM to detect PII in REAL fields."""
        fields_json = json.dumps(field_analysis.get("fields", []), indent=2)

        system_prompt = """You are a PII (Personally Identifiable Information) DETECTION EXPERT.

TASK: Identify which database fields contain PII based on field classification analysis.

PII CATEGORIES:
🔴 HIGH RISK (Identity Theft): SSN, credit_card, passport, driver_license, password, medical_id, biometric_data
🟠 MEDIUM RISK (Targeting/Tracking): email, phone_number, home_address, latitude/longitude, device_id, ip_address, name+address combo
🟡 LOW RISK (General Info): employee_name (alone), job_title, department, generic identifiers

DETECTION RULES:
1. If field is "PII" type → likely contains PII
2. If field is "Geolocation" type → RESTRICTED (tracking risk)
3. If field contains "SSN", "credit", "password" → CONFIDENTIAL
4. If field contains email/phone → RESTRICTED
5. Consider combinations (name + address = PII)

Return risk assessment with justification."""

        user_prompt = f"""Based on this field analysis of REAL production data, identify which fields contain PII.

Field Analysis:
{fields_json}

PII includes: email, phone, SSN, credit card, passport, driver license, name + address.

Respond in JSON:
{{
  "pii_fields": [
    {{"name": "field_name", "pii_type": "type", "risk": "high/medium/low"}}
  ],
  "reasoning": "explanation of what PII was found"
}}"""

        response = await self._call_llm(user_prompt, system_prompt=system_prompt)
        try:
            result = json.loads(response)
            result["_system_prompt"] = system_prompt
            result["_user_prompt"] = user_prompt
            return result
        except:
            return {"pii_fields": [], "reasoning": "Unable to analyze"}

    async def _ai_classify_sensitivity(self, pii_detection: dict) -> dict:
        """Use LLM to classify sensitivity level."""
        pii_json = json.dumps(pii_detection["pii_fields"], indent=2)

        system_prompt = """You are a DATA GOVERNANCE & COMPLIANCE EXPERT.

TASK: Classify a table's sensitivity level based on PII analysis.

SENSITIVITY CLASSIFICATION:
🔴 CONFIDENTIAL (7-year retention): High-risk PII requiring maximum protection
   Contains: SSN, credit_card, password, medical_id, biometric_data, passport
   Access: Restricted to authorized personnel only
   Use case: Financial, healthcare, identity data

🟠 RESTRICTED (90-day retention): Medium-risk PII requiring standard protection
   Contains: email, phone, home_address, latitude/longitude, device_id, ip_address
   Access: Department-level access
   Use case: Customer contact, location tracking

🟡 INTERNAL (1-year retention): Low-risk business/employee data
   Contains: employee_name, department, job_title, employee_id, birth_date
   Access: Company-wide
   Use case: HR, organizational data

🟢 PUBLIC (indefinite retention): No PII, public information
   Contains: product_name, category, public_statistics
   Access: Public
   Use case: Marketing, public datasets

CLASSIFICATION RULE: Use the HIGHEST RISK PII found in the table."""

        user_prompt = f"""Classify the sensitivity level for a REAL dataset based on this PII analysis.

PII Detected:
{pii_json}

Sensitivity levels:
- confidential: SSN, credit cards, high-risk → 7 year retention
- restricted: Email, phone, geolocation, medical → 90 day retention
- internal: Names without addresses, dept info → 1 year retention
- public: No PII, public data → indefinite retention

Respond in JSON:
{{
  "level": "confidential|restricted|internal|public",
  "confidence": 0.95,
  "reasoning": "explain why"
}}"""

        response = await self._call_llm(user_prompt, system_prompt=system_prompt)
        try:
            result = json.loads(response)
            result["_system_prompt"] = system_prompt
            result["_user_prompt"] = user_prompt
            return result
        except:
            return {"level": "internal", "confidence": 0.5, "reasoning": "Unable to classify"}

    async def _ai_suggest_owner(self, table_name: str, sensitivity: dict, pii_detection: dict) -> dict:
        """Use LLM to suggest appropriate owner."""
        pii_json = json.dumps(pii_detection["pii_fields"], indent=2)
        prompt = f"""Suggest the appropriate data owner/steward for a REAL table.

Table: {table_name}
Sensitivity: {sensitivity['level']}
PII Found:
{pii_json}

Consider org structure:
- confidential → data-governance-team (legal/compliance)
- restricted → data-steward (privacy expertise)
- internal → data-team (standard)
- public → data-team

Respond in JSON:
{{
  "owner": "email@company.com",
  "reasoning": "why",
  "alternatives": [...]
}}"""

        response = await self._call_llm(prompt)
        try:
            return json.loads(response)
        except:
            return {"owner": "data-team@company.com", "reasoning": "Unable to determine", "alternatives": []}

    async def _generate_metadata(self, table_name: str, field_analysis: dict, pii_detection: dict, sensitivity: dict) -> dict:
        """Generate comprehensive metadata for the table."""
        fields_with_types = []
        for field_info in field_analysis.get("fields", []):
            field_name = field_info.get("name", "unknown")
            # Check if this field is PII
            pii_info = next((pii for pii in pii_detection.get("pii_fields", []) if pii.get("name") == field_name), None)

            fields_with_types.append({
                "name": field_name,
                "type": field_info.get("detected_as", "unknown"),
                "confidence": field_info.get("confidence", 0),
                "reason": field_info.get("reason", ""),
                "is_pii": pii_info is not None,
                "pii_type": pii_info.get("pii_type") if pii_info else None,
                "risk_level": pii_info.get("risk") if pii_info else None,
            })

        return {
            "table": table_name,
            "sensitivity": sensitivity.get("level", "internal"),
            "confidence": sensitivity.get("confidence", 0),
            "total_fields": len(fields_with_types),
            "pii_fields": len([f for f in fields_with_types if f["is_pii"]]),
            "fields": fields_with_types,
            "generated_at": "2026-05-26",
        }

    def _apply_policy(self, sensitivity: str) -> dict:
        """Apply governance policy based on sensitivity."""
        return {
            "name": sensitivity,
            "retention": self.compliance_policies.get(sensitivity, self.compliance_policies["internal"])["retention"],
            "access_level": self.compliance_policies.get(sensitivity, self.compliance_policies["internal"])["access_level"],
        }

    async def _call_llm(self, prompt: str, system_prompt: str = None) -> str:
        """Call Ollama/Cloudera AI with full transparency."""
        import httpx

        try:
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{LLM_BASE_URL}/chat/completions",
                    json={
                        "model": LLM_MODEL,
                        "messages": messages,
                        "temperature": 0.3,
                        "max_tokens": 1000,
                    },
                    headers={"Authorization": f"Bearer {LLM_API_KEY}"}
                )
                response.raise_for_status()
                data = response.json()
                content = data["choices"][0]["message"]["content"]

                logger.info(f"\n[LLM CALL]")
                logger.info(f"  System: {system_prompt[:100] if system_prompt else 'None'}...")
                logger.info(f"  User: {prompt[:100]}...")
                logger.info(f"  Response: {content[:100]}...")

                return content
        except Exception as e:
            logger.warning(f"LLM call failed: {e}")
            return "{}"


# Orchestrator node (legacy compatibility)
from langchain_core.messages import AIMessage
from agents.state import AgentState


def metadata_curator_node(state: AgentState) -> dict:
    return {
        "messages": [AIMessage(content="Metadata Curator initialized")],
        "active_agent": "metadata_curator",
        "sse_events": [{"type": "stub", "agent": "metadata_curator", "content": "Metadata Curator operational"}],
        "next": "end",
    }
