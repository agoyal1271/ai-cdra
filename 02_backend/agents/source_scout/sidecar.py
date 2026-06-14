"""
ScoutSidecar — handles all reasoning so the main agent handles only execution.

Responsibilities:
  - Detect which modules (PII, cost, freshness, pipeline, dataquality) are active
  - Decide whether an asset passes the goal's filters (name pattern, time window)
  - Assemble the dynamic LLM system prompt from active module snippets
  - Parse the LLM response back into a structured dict

The main agent instantiates one sidecar per run and delegates every
"thinking" task to it. No filtering or prompt logic lives in agent.py.
"""
import base64
import difflib
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

from agents.source_scout.modules import MODULE_MAP, Module

logger = logging.getLogger(__name__)

# ── Semantic words that signal a content/column query, NOT a table-name pattern ──
_SEMANTIC_WORDS = {
    "pii", "person", "people", "personal", "sensitive", "private",
    "email", "phone", "ssn", "address", "name", "names", "data",
    "column", "columns", "field", "fields", "record", "records",
    "information", "content", "value", "values", "quality", "anomaly",
    "cost", "fresh", "freshness", "drift", "customer",
}

_GENERIC_DISCOVERY = {
    "discover all", "find all", "list all", "show all", "scan all", "get all",
    "discover everything", "find everything", "show everything", "all data",
    "all sources", "all tables", "all topics", "all data sources",
}
_STOP_WORDS = {
    "a", "an", "the", "some", "any", "all", "this", "that",
    "with", "for", "and", "or", "but", "after", "before",
    "about", "by", "from", "to", "in", "on", "at", "of", "as",
}

# Phrases that signal a content qualifier even when the goal starts with "all X"
# e.g. "all topics which has location data" is specific, not generic
_CONTENT_QUALIFIER_RE = re.compile(
    r'\b(?:which\s+has|that\s+has|which\s+contains?|that\s+contains?|'
    r'having\s+|containing\s+)\b'
)

# Semantic field families — maps goal keywords to the field name substrings that
# prove the data is present. Used for deterministic pre-LLM filtering.
# A schema passes if ANY field name contains ANY substring in the family set.
_SEMANTIC_FIELD_FAMILIES: dict[str, set[str]] = {
    "geolocation": {"lat", "lon", "lng", "latitude", "longitude",
                    "geohash", "geo_hash", "coordinates", "coordinate",
                    "gps", "location", "position", "altitude", "elevation", "accuracy"},
    "address":     {"address", "street", "city", "zip", "postal", "postcode",
                    "country", "state", "region", "district", "suburb"},
    "email":       {"email", "e_mail", "mail_address"},
    "phone":       {"phone", "mobile", "telephone", "cell", "msisdn", "contact_no"},
    "financial":   {"amount", "price", "cost", "revenue", "payment", "transaction",
                    "charge", "fee", "balance", "invoice", "billing"},
    "identity":    {"ssn", "social_security", "national_id", "passport", "dob",
                    "date_of_birth", "birth_date", "member_id", "patient_id"},
}

# Maps goal keyword substrings → field family key
_GOAL_KEYWORD_TO_FAMILY: list[tuple[str, str]] = [
    ("geolocation", "geolocation"),
    ("geo location", "geolocation"),
    ("location data", "geolocation"),
    ("location field", "geolocation"),
    ("coordinates", "geolocation"),
    ("latitude", "geolocation"),
    ("longitude", "geolocation"),
    ("gps data", "geolocation"),
    ("address", "address"),
    ("email", "email"),
    ("phone", "phone"),
    ("financial", "financial"),
    ("payment", "financial"),
    ("invoice", "financial"),
    ("identity", "identity"),
    ("ssn", "identity"),
    ("date of birth", "identity"),
]

# ── Base prompt and schema every LLM call starts from ────────────────────────
_BASE_SCHEMA: dict = {
    "asset_name": "string — exact name of the asset",
    "summary": "one sentence: what data this source contains",
    "recommended_pipeline": "NiFi | Flink SQL | Kafka Connect | Spark Streaming | Spark Batch",
    "reasoning": "why this pipeline suits this source type and schema",
}
_BASE_SNIPPET = (
    "You are a Cloudera data platform expert.\n"
    "Analyse the data source described below and produce a pipeline recommendation.\n"
    "Respond ONLY with compact JSON — no markdown, no explanation outside the JSON."
)


def _jwt_exp(token: str) -> Optional[float]:
    """Decodes the JWT payload and returns the 'exp' claim as a Unix timestamp, or None."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        payload_b64 = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        return float(payload["exp"]) if "exp" in payload else None
    except Exception:
        return None


def _get_sso_jwt(username: str, password: str, knox_url: str) -> str:
    """
    Step 1 of Knox two-step auth: authenticate to KnoxSSO with Basic auth and
    return the hadoop-jwt cookie. The cdp-datashare-access topology uses JWTProvider
    so the managed token endpoint only accepts Bearer auth — Basic Auth returns 401.
    """
    gateway_base = knox_url.split("/gateway/")[0] + "/gateway"
    knoxsso_url = gateway_base + "/knoxsso/api/v1/websso"
    resp = requests.get(
        knoxsso_url,
        params={"originalUrl": knox_url},
        auth=(username, password),
        timeout=10,
        verify=False,
        allow_redirects=False,
    )
    jwt = resp.cookies.get("hadoop-jwt")
    if not jwt:
        set_cookie = resp.headers.get("Set-Cookie", "")
        if "hadoop-jwt=" in set_cookie:
            jwt = set_cookie.split("hadoop-jwt=")[1].split(";")[0].strip()
    if not jwt:
        raise RuntimeError(
            f"KnoxSSO did not return hadoop-jwt. Status: {resp.status_code}. "
            "Check credentials and Knox SSO URL."
        )
    return jwt


def get_valid_knox_token() -> str:
    """
    Returns a valid Knox JWT, refreshing it automatically when it is expired
    or within KNOX_TOKEN_REFRESH_BUFFER_SECS of expiry.

    Uses Knox two-step auth:
      1. GET {gateway}/knoxsso/api/v1/websso with Basic auth → hadoop-jwt cookie
      2. GET {KNOX_LOGIN_URL}/knoxtoken/api/v1/token with Bearer <hadoop-jwt> → managed token

    On success: updates os.environ["KNOX_JWT"] and returns the new token.
    On failure: logs a warning and returns the existing token so callers can handle 401.
    """
    import os
    import config
    current = os.getenv("KNOX_JWT", "")
    buffer  = config.KNOX_TOKEN_REFRESH_BUFFER_SECS

    if current:
        exp = _jwt_exp(current)
        if exp is not None and exp - time.time() > buffer:
            return current
        # exp is None or token is within refresh buffer — fall through to refresh

    login_url = config.KNOX_LOGIN_URL
    username  = config.KNOX_USERNAME
    password  = config.KNOX_PASSWORD

    if not login_url or not username:
        logger.warning(
            "[sidecar] Knox JWT expired but KNOX_LOGIN_URL/KNOX_USERNAME not configured — "
            "cannot refresh. Set these env vars to enable automatic token refresh."
        )
        return current

    try:
        # Step 1: get short-lived SSO JWT via KnoxSSO (Basic auth)
        logger.info("[sidecar] Refreshing Knox JWT — step 1: KnoxSSO")
        sso_jwt = _get_sso_jwt(username, password, login_url)

        # Step 2: exchange SSO JWT for a long-lived managed Knox token
        endpoint = f"{login_url.rstrip('/')}/knoxtoken/api/v1/token"
        logger.info(f"[sidecar] Refreshing Knox JWT — step 2: managed token from {endpoint}")
        resp = requests.get(
            endpoint,
            headers={"Authorization": f"Bearer {sso_jwt}"},
            timeout=10,
            verify=False,
        )
        resp.raise_for_status()
        data = resp.json()
        new_token = data.get("access_token") or data.get("token")
        if not new_token:
            raise ValueError(f"Unexpected Knox token response: {data}")
        os.environ["KNOX_JWT"] = new_token
        config.KNOX_JWT = new_token
        logger.info("[sidecar] Knox JWT refreshed successfully")
        return new_token
    except Exception as e:
        logger.warning(f"[sidecar] Knox token refresh failed: {e}")
        return current


class ScoutSidecar:
    """
    Instantiated once per Source Scout run with the user's goal.
    All 'thinking' tasks are delegated here; agent.py only calls execute/emit.
    """

    def __init__(self, goal: str) -> None:
        self.goal = goal
        self._g = goal.lower()

        # Pre-compute once so every call is O(1)
        self._active_modules: list[tuple[str, Module]] = self._detect_modules()
        self._metadata_intent: bool = self._detect_metadata_intent()
        self._metadata_filters: dict[str, str] = self._build_metadata_filters() if self._metadata_intent else {}
        self._name_filter, self._name_filter_desc = self._build_name_filter()
        self._time_cutoff_ms: Optional[int] = self._build_time_cutoff()

        logger.debug(
            f"[sidecar] goal={goal!r} | "
            f"modules={[n for n, _ in self._active_modules]} | "
            f"metadata_intent={self._metadata_intent} filters={self._metadata_filters or 'none'} | "
            f"name_filter={self._name_filter_desc!r} | "
            f"time_cutoff_ms={self._time_cutoff_ms}"
        )

    # ── Module detection ──────────────────────────────────────────────────────

    def get_active_modules(self) -> list[tuple[str, Module]]:
        return self._active_modules

    def get_active_module_names(self) -> list[str]:
        return [name for name, _ in self._active_modules]

    def _detect_modules(self) -> list[tuple[str, Module]]:
        active = [(name, mod) for name, mod in MODULE_MAP.items()
                  if any(kw in self._g for kw in mod.keywords)]
        logger.debug(f"[sidecar] active modules: {[n for n, _ in active] or 'none (base only)'}")
        return active

    # ── Intent detection ──────────────────────────────────────────────────────

    def _detect_metadata_intent(self) -> bool:
        """
        Detect if the goal is asking about metadata properties (storage, format, catalog, location).
        Returns True if metadata keywords or patterns are present, False otherwise.

        Storage categories recognized:
          - s3: AWS S3 (s3://)
          - s3a: S3-compatible (MinIO, DigitalOcean, Wasabi, Ozone S3-layer, etc.)
          - ozone: Apache Ozone native (ofs://, o3fs://)
          - azure: Microsoft Azure (adls://, abfs://, wasb://)
          - gcs: Google Cloud Storage (gs://)
          - hdfs: Hadoop Distributed File System
          - cloud: generic cloud provider

        Examples:
          - "show tables with backend as ozone" → True
          - "tables stored in s3a" → True
          - "iceberg tables in hdfs" → True
        """
        # Direct metadata keywords (storage types, properties, etc.)
        metadata_keywords = {
            "storage", "backend", "stored", "format", "catalog", "location",
            # Specific storage backends
            "s3", "s3a", "ozone", "hdfs", "azure", "gcs", "cloud",
            # Azure variations
            "adls", "adl", "abfs", "wasb",
            # Formats and systems
            "parquet", "iceberg", "hms", "rest catalog",
        }
        if any(kw in self._g for kw in metadata_keywords):
            return True

        # URI scheme patterns
        uri_schemes = {"ofs://", "o3fs://", "s3://", "s3a://", "s3n://", "gs://", "gcs://", "adl://", "adls://", "abfs://", "wasb://", "hdfs://"}
        if any(scheme in self._g for scheme in uri_schemes):
            return True

        return False

    def get_metadata_intent_and_filters(self) -> tuple[bool, dict[str, str]]:
        """
        Return (has_metadata_intent, filters_dict).
        Caller uses this to decide if metadata filtering should apply.
        """
        return self._metadata_intent, self._metadata_filters

    def _build_metadata_filters(self) -> dict[str, str]:
        """
        Extract metadata filter expectations from the goal using language patterns.
        Returns dict with keys like storage, format, catalog, location if detected.
        Example: "show tables with backend as ozone" → {"storage": "ozone"}
        """
        try:
            from tools.intent_extractor import extract_metadata_filters_from_goal
            filters = extract_metadata_filters_from_goal(self.goal)
            if filters:
                logger.debug(f"[sidecar] metadata filters extracted: {filters}")
            return filters
        except Exception as e:
            logger.debug(f"[sidecar] metadata filter extraction failed: {e}")
            return {}

    # ── Asset filtering ───────────────────────────────────────────────────────

    def should_include_asset(self, asset: dict) -> tuple[bool, str]:
        """
        Single entry point for the agent to check whether an asset passes
        name/time filters (not metadata — agent.py decides metadata routing).
        Returns (include: bool, reason: str).
        """
        name = asset.get("name", "")

        # 1. Name pattern filter (explicit structural patterns)
        if self._name_filter is not None:
            if not self._name_filter(name):
                return False, f"skipped — does not match filter: {self._name_filter_desc}"

        # 2. Time filter (Iceberg tables only — checks first snapshot timestamp)
        if self._time_cutoff_ms is not None and asset.get("asset_type") == "iceberg_table":
            snap_ms = asset.get("metadata", {}).get("first_snapshot_ms")
            if snap_ms is not None and snap_ms < self._time_cutoff_ms:
                return False, f"skipped — first snapshot {snap_ms} is before cutoff {self._time_cutoff_ms}"

        return True, "included"

    def get_filter_summary(self) -> str:
        """Human-readable summary of active filters for the agent log."""
        parts = []
        if self._name_filter_desc:
            parts.append(f"name filter: {self._name_filter_desc}")
        if self._time_cutoff_ms:
            cutoff_dt = datetime.fromtimestamp(self._time_cutoff_ms / 1000, tz=timezone.utc)
            parts.append(f"time filter: created after {cutoff_dt.strftime('%Y-%m-%d %H:%M UTC')}")
        return " | ".join(parts) if parts else "no filters active"

    def has_active_modules(self) -> bool:
        """True when at least one reasoning module matched the goal."""
        return bool(self._active_modules)

    def is_goal_specific(self) -> bool:
        """
        True when the goal has specific intent but matched NO modules and NO name filter.
        These goals need an LLM intent classifier to decide asset relevance.

        "all topics which has location data" is specific even though it contains "all topics" —
        the content qualifier ("which has") signals a filter requirement, not a generic list.
        """
        if self._active_modules or self._name_filter is not None:
            return False
        # Content qualifiers ("which has X", "that contains X") override generic-discovery phrases
        if _CONTENT_QUALIFIER_RE.search(self._g):
            return True
        return (
            not any(phrase in self._g for phrase in _GENERIC_DISCOVERY)
            and len(self._g.split()) > 3
        )

    def build_intent_classifier_prompt(
        self, asset_name: str, schema_info: dict
    ) -> tuple[str, list[str]]:
        """
        Smart-strict relevance classifier for goals that matched no module keywords.
        Instead of a boolean "is it relevant?", asks the LLM for a confidence score (0-100)
        backed by reasoning. The caller filters at a threshold (e.g., >60%).
        Allows implicit/derived data IF the reasoning is sound, avoiding false negatives
        from over-strict logic while maintaining rigor.

        NOW: Also checks metadata properties (storage, format, catalog) in addition to schema fields.
        This enables matching of queries like "tables stored in ozone" or "parquet format tables".

        Returns the same (prompt, module_names) shape as build_system_prompt.
        """
        from tools.intent_extractor import build_intent_context_for_llm, extract_metadata_filters_from_goal

        scanned_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        # Build complete context including metadata
        asset_context = build_intent_context_for_llm(self.goal, asset_name, schema_info)

        # Extract what the user is looking for in terms of metadata
        metadata_filters = extract_metadata_filters_from_goal(self.goal)

        prompt = (
            "You are a smart data relevance classifier. Your job is to assess how well "
            "a data asset matches the user's goal — considering BOTH schema fields AND metadata properties.\n\n"
            "MATCH TYPES:\n"
            "A) METADATA MATCH: User asks for tables with specific properties (storage, format, catalog, location).\n"
            "   Example: user asks 'stored in ozone' → check if asset.metadata.storage == 'ozone' → score 90+\n"
            "   Example: user asks 'parquet format' → check if asset.metadata.format == 'parquet' → score 90+\n\n"
            "B) SCHEMA FIELD MATCH: User asks for specific column/field names or data types.\n"
            "   Example: user asks for 'email fields' → check if schema has 'email' column → score 85-100\n"
            "   Example: user asks for 'customer data' → check if schema has customer-like fields → score 65-84\n\n"
            "SCORING RULES:\n"
            "- 90-100: Metadata properties OR schema fields EXACTLY match the goal.\n"
            "- 75-89: Metadata or fields STRONGLY match the goal with clear semantic alignment.\n"
            "- 50-74: PARTIAL match — some metadata/fields match, others don't, or inference needed.\n"
            "- 0-49: NO match — metadata and fields don't align with goal.\n\n"
            "CRITICAL RULES:\n"
            "- If user asks for metadata (storage, format, catalog), prioritize checking metadata properties FIRST.\n"
            "- If metadata property matches, score 85+. If it doesn't match, score 0-40 unless fields compensate.\n"
            "- Only score 65+ for field matches if field names semantically align with the goal.\n"
            "- Do NOT invent connections. If user asks for 'ozone storage' and asset.storage='s3', score 0-39.\n"
            "- Synonyms are OK (price/cost/revenue for 'dollar amount'), but mismatches are NOT.\n\n"
        )

        # Add metadata expectations if found
        if metadata_filters:
            prompt += "USER'S METADATA EXPECTATIONS:\n"
            for key, value in metadata_filters.items():
                prompt += f"  - {key}: {value}\n"
            prompt += "\n"

        prompt += (
            f"{asset_context}\n\n"
            "INSTRUCTIONS:\n"
            "1. Check if the asset's METADATA properties match the user's expectations first.\n"
            "2. Then check if the schema FIELDS match.\n"
            "3. Combine both checks to determine final relevance score.\n"
            "4. Do not hallucinate — only score 65+ if there's a clear match in metadata OR fields.\n\n"
            "Respond ONLY with compact JSON:\n"
            "{\n"
            '  "asset_name": "<name>",\n'
            '  "summary": "<one sentence: what data this asset contains and where it\'s stored>",\n'
            '  "recommended_pipeline": "Spark Batch|Flink|NiFi",\n'
            '  "reasoning": "<2 sentences: which metadata/fields match or don\'t match the goal>",\n'
            '  "confidence_score": 75,\n'
            '  "relevance_reason": "<cite specific metadata properties or field names that match, or explain why none match>"\n'
            "}"
        )
        return prompt, ["intent_classifier"]

    def _build_name_filter(self):
        """
        Returns (filter_fn, description) for explicit table-name patterns only.
        Semantic goals ('contains PII', 'has person names') return (None, None).
        """
        patterns = [
            (r'start(?:s|ing)?\s+with\s+["\']?(\w+)["\']?',
             lambda m, n: n.split(".")[-1].lower().startswith(m.group(1).lower()),
             lambda m: f"name starts with '{m.group(1)}'"),

            (r'begin(?:s|ning)?\s+with\s+["\']?(\w+)["\']?',
             lambda m, n: n.split(".")[-1].lower().startswith(m.group(1).lower()),
             lambda m: f"name starts with '{m.group(1)}'"),

            (r'end(?:s|ing)?\s+with\s+["\']?(\w+)["\']?',
             lambda m, n: n.split(".")[-1].lower().endswith(m.group(1).lower()),
             lambda m: f"name ends with '{m.group(1)}'"),

            (r'(?:table\s+)?named?\s+["\']?(\w+)["\']?',
             lambda m, n: n.split(".")[-1].lower() == m.group(1).lower(),
             lambda m: f"name is '{m.group(1)}'"),

            (r'(?:table\s+)?called\s+["\']?(\w+)["\']?',
             lambda m, n: n.split(".")[-1].lower() == m.group(1).lower(),
             lambda m: f"name is '{m.group(1)}'"),
        ]

        for pattern, check_fn, desc_fn in patterns:
            m = re.search(pattern, self._g)
            if not m:
                continue
            captured = m.group(1).lower()
            if captured in _SEMANTIC_WORDS or captured in _STOP_WORDS:
                logger.debug(
                    f"[sidecar] name pattern captured '{captured}' — semantic word, "
                    f"no code filter applied, LLM will reason over columns"
                )
                return None, None
            return (lambda name, _m=m, _c=check_fn: _c(_m, name)), desc_fn(m)

        return None, None

    def _build_time_cutoff(self) -> Optional[int]:
        now = datetime.now(timezone.utc)
        g = self._g
        m = re.search(r'last\s+(\d+)\s+hour', g)
        if m: return int((now - timedelta(hours=int(m.group(1)))).timestamp() * 1000)
        m = re.search(r'last\s+(\d+)\s+day', g)
        if m: return int((now - timedelta(days=int(m.group(1)))).timestamp() * 1000)
        m = re.search(r'last\s+(\d+)\s+minute', g)
        if m: return int((now - timedelta(minutes=int(m.group(1)))).timestamp() * 1000)
        if 'today' in g:
            return int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        if 'last week' in g:  return int((now - timedelta(days=7)).timestamp() * 1000)
        if 'last month' in g: return int((now - timedelta(days=30)).timestamp() * 1000)
        return None

    # ── Column verification (deterministic pre-LLM filter) ───────────────────

    def check_column_match(self, schema_info: dict) -> tuple[bool, str]:
        """
        Deterministic check: if the goal explicitly names specific columns/fields,
        verify that at least one of them exists in the schema.

        Returns (passes: bool, reason: str).
        - (True,  "")              → no column constraint in goal, skip check
        - (True,  "column(s) ... found in schema")  → at least one match
        - (False, "column(s) ... not found ...")     → none match, caller should reject asset
        """
        # Extract field names from schema_info
        raw_fields = schema_info.get("fields", [])
        field_names_lower = set()
        for f in raw_fields:
            fname = f.get("name") if isinstance(f, dict) else None
            if fname:
                field_names_lower.add(fname.lower())

        # Find column names explicitly mentioned in the goal
        patterns = [
            r'(?:column|field|attribute)\s+["\']?(\w+)["\']?',
            r'(?:has|with|contains?|having)\s+["\']?(\w+)["\']?\s+(?:column|field)',
        ]
        mentioned: set[str] = set()
        for pattern in patterns:
            for m in re.finditer(pattern, self._g):
                word = m.group(1).lower()
                if word not in _STOP_WORDS and word not in _SEMANTIC_WORDS:
                    mentioned.add(word)

        # No column names found in goal → no constraint, pass through
        if not mentioned:
            return True, ""

        # Mentioned columns but schema has no fields → can't verify, pass through
        if not field_names_lower:
            return True, ""

        # Exact match
        matched = mentioned & field_names_lower
        if matched:
            return True, f"column(s) {sorted(matched)} found in schema"

        # Fuzzy match — catch typos like "invice_id" matching "invoice_id"
        field_list = sorted(field_names_lower)
        fuzzy_pairs: list[tuple[str, str]] = []
        for col in mentioned:
            close = difflib.get_close_matches(col, field_list, n=1, cutoff=0.8)
            if close:
                fuzzy_pairs.append((col, close[0]))

        if fuzzy_pairs:
            corrections = ", ".join(f"'{q}'→'{m}'" for q, m in fuzzy_pairs)
            return True, f"fuzzy column match: {corrections}"

        # Mentioned columns exist but none match schema fields even fuzzily
        return (
            False,
            f"column(s) {sorted(mentioned)} not found in schema fields {sorted(field_names_lower)}",
        )

    def check_semantic_field_match(self, schema_info: dict) -> tuple[bool, str]:
        """
        Deterministic check: if the goal maps to a known semantic field family
        (geolocation, email, phone, etc.), verify the schema actually has those fields.

        Returns (passes: bool, reason: str).
        - (True,  "")                        → no semantic family detected, skip check
        - (True,  "geo fields found: lat, lon") → family matched in schema
        - (False, "geo fields not found ...")   → family required but absent → reject
        """
        raw_fields = schema_info.get("fields", [])
        field_names_lower = {
            f.get("name", "").lower() for f in raw_fields if isinstance(f, dict) and f.get("name")
        }

        # Find which family the goal requires
        detected_family: Optional[str] = None
        for keyword, family in _GOAL_KEYWORD_TO_FAMILY:
            if keyword in self._g:
                detected_family = family
                break

        if not detected_family:
            return True, ""  # no semantic constraint — let LLM decide

        if not field_names_lower:
            return True, ""  # no schema info — can't verify, pass through

        required_substrings = _SEMANTIC_FIELD_FAMILIES[detected_family]
        matched = [
            fname for fname in field_names_lower
            if any(sub in fname for sub in required_substrings)
        ]

        if matched:
            return True, f"{detected_family} fields found: {', '.join(sorted(matched))}"

        return (
            False,
            f"goal requires {detected_family} data but schema has none of "
            f"{sorted(required_substrings)[:6]}... — fields are: {sorted(field_names_lower)[:8]}",
        )

    # ── Prompt assembly ───────────────────────────────────────────────────────

    def build_system_prompt(
        self,
        asset_type: str,
        asset_name: str,
        schema_info: dict,
    ) -> tuple[str, list[str]]:
        """
        Stitches BASE_SNIPPET + active module snippets + merged JSON schema
        into a single system prompt. Returns (prompt, active_module_names).
        """
        merged_schema = dict(_BASE_SCHEMA)
        for _, mod in self._active_modules:
            merged_schema.update(mod.json_fields)

        # When modules are active the LLM must declare goal relevance.
        # Use real boolean + explicit instruction so the model doesn't return a string.
        if self._active_modules:
            merged_schema["matches_criteria"] = False   # REQUIRED: true if relevant, false if not
            merged_schema["relevance_reason"] = "one sentence explaining the relevance decision"

        parts = [_BASE_SNIPPET]
        if self._active_modules:
            parts.append(f"Goal context from user: {self.goal}")
        for _, mod in self._active_modules:
            parts.append(mod.prompt_snippet)

        relevance_instruction = (
            "\nIMPORTANT: You MUST set \"matches_criteria\" to the JSON boolean true or false "
            "(not a string). Set it to false if the asset does NOT contain data relevant to "
            "the user's goal. The caller will discard any asset where matches_criteria is false.\n"
            if self._active_modules else ""
        )

        scanned_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        parts.append(
            f"Data freshness: LIVE — catalog scanned at {scanned_at}. "
            f"This is real production data, NOT mock or sample data.\n"
            f"Asset type  : {asset_type}\n"
            f"Asset name  : {asset_name}\n"
            f"Schema/meta : {json.dumps(schema_info, default=str)}\n"
            f"{relevance_instruction}\n"
            f"Respond ONLY with JSON matching this schema "
            f"(include all module-specific fields shown):\n"
            f"{json.dumps(merged_schema, indent=2)}"
        )

        prompt = "\n\n".join(parts)
        names  = self.get_active_module_names()
        logger.debug(
            f"[sidecar] prompt built for {asset_name!r} | "
            f"{len(prompt)} chars | modules={names}"
        )
        return prompt, names

    # ── Response parsing ──────────────────────────────────────────────────────

    def parse_llm_response(self, raw: str) -> dict:
        """Extracts and parses the JSON object from a raw LLM response."""
        clean = raw.strip()
        if "```" in clean:
            clean = clean.split("```", 1)[1].split("```")[0].lstrip("json").strip()
        s, e = clean.find("{"), clean.rfind("}") + 1
        if s != -1 and e > s:
            clean = clean[s:e]
        try:
            return json.loads(clean)
        except Exception:
            logger.warning(f"[sidecar] JSON parse failed: {raw[:120]!r}")
            return {
                "asset_name": "",
                "summary": raw[:200],
                "recommended_pipeline": "Spark Batch",
                "reasoning": "Could not parse LLM response as JSON.",
            }
