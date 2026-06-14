"""
Confluent / Cloudera Schema Registry client with optional Kerberos (SPNEGO) auth.

Config env vars:
  SCHEMA_REGISTRY_URL        — e.g. https://schema-registry.corp.example.com
  SCHEMA_REGISTRY_AUTH_TYPE  — NONE | KERBEROS | BASIC (default NONE)
  SCHEMA_REGISTRY_KEYTAB     — path to keytab file (for KERBEROS)
  SCHEMA_REGISTRY_PRINCIPAL  — Kerberos principal, e.g. kafka/host@REALM
  SCHEMA_REGISTRY_USER       — username (for BASIC)
  SCHEMA_REGISTRY_PASSWORD   — password (for BASIC)

  
  
Kerberos auth flow:
  1. If KRB5CCNAME env var points to a valid credential cache → reuse it.
  2. If SCHEMA_REGISTRY_KEYTAB is set → kinit using the keytab before first request.
  3. On 401 response → force re-kinit and retry ONCE.
  4. If re-auth fails → log warning and return graceful mock schema.

Avro decoding (Confluent wire format):
  Frame: [0x00][schema_id: 4 bytes big-endian][avro payload]
  Requires: fastavro (optional — degrades gracefully if not installed)
"""
import json
import logging
import os
import struct
import subprocess
import time
from functools import lru_cache
from typing import Any
import config
import requests

logger = logging.getLogger(__name__)

# Example env config (set real values in .env, not here):
# SCHEMA_REGISTRY_URL="http://<host>:8443/gateway/cdp-proxy-api/schema-registry"
# SCHEMA_REGISTRY_TYPE="cloudera"
# SCHEMA_REGISTRY_USER="<user>"
# SCHEMA_REGISTRY_PASSWORD="<password>"
# SCHEMA_REGISTRY_AUTH_TYPE="BASIC"

# ── Config ────────────────────────────────────────────────────────────────────
SR_URL        = config.SCHEMA_REGISTRY_URL
SR_AUTH_TYPE  = "BASIC"
SR_KEYTAB     = os.getenv("SCHEMA_REGISTRY_KEYTAB", "")
SR_PRINCIPAL  = os.getenv("SCHEMA_REGISTRY_PRINCIPAL", "")
SR_USER       = config.KNOX_USERNAME
SR_PASSWORD   = config.KNOX_PASSWORD

_last_kinit: float = 0.0
_KINIT_VALID_SECS = 3000   # re-kinit after 50 min (ticket lifetime – buffer)

_MOCK_AVRO_SCHEMA = {
    "type": "record",
    "name": "MockEvent",
    "fields": [
        {"name": "id",      "type": "string"},
        {"name": "ts",      "type": "long"},
        {"name": "payload", "type": "string"},
    ],
}


# ── Kerberos helpers ──────────────────────────────────────────────────────────

def _kinit(force: bool = False) -> bool:
    """
    Runs kinit using the configured keytab.
    Skipped if KRB5CCNAME points to an existing credential cache (managed externally).
    Returns True if a ticket is now available.
    """
    global _last_kinit

    # If the environment already manages the ticket, don't kinit ourselves
    if os.getenv("KRB5CCNAME") and not force:
        logger.debug("[schema_registry] KRB5CCNAME set — reusing existing ticket cache")
        return True

    if not SR_KEYTAB or not SR_PRINCIPAL:
        return False

    if not force and time.time() - _last_kinit < _KINIT_VALID_SECS:
        return True   # still valid

    try:
        result = subprocess.run(
            ["kinit", "-kt", SR_KEYTAB, SR_PRINCIPAL],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            _last_kinit = time.time()
            logger.info(f"[schema_registry] kinit OK: {SR_PRINCIPAL!r}")
            return True
        logger.warning(f"[schema_registry] kinit failed ({result.returncode}): {result.stderr.strip()}")
        return False
    except FileNotFoundError:
        logger.warning("[schema_registry] 'kinit' not found in PATH — Kerberos unavailable")
        return False
    except subprocess.TimeoutExpired:
        logger.warning("[schema_registry] kinit timed out")
        return False


def _build_auth():
    """Returns a requests auth object (or None) based on SR_AUTH_TYPE."""
    if SR_AUTH_TYPE == "KERBEROS":
        try:
            from requests_kerberos import HTTPKerberosAuth, OPTIONAL
            return HTTPKerberosAuth(mutual_authentication=OPTIONAL)
        except ImportError:
            logger.warning(
                "[schema_registry] requests-kerberos not installed. "
                "Install it: pip install requests-kerberos. Falling back to no auth."
            )
            return None
    if SR_AUTH_TYPE == "BASIC" and SR_USER:
        return (SR_USER, SR_PASSWORD)
    return None


# ── HTTP layer ────────────────────────────────────────────────────────────────

def _get(path: str) -> Any:
    """
    Authenticated GET against the Schema Registry (Confluent wire format).
    Retries once on 401 (re-kinits before retry).
    Raises requests.HTTPError for non-recoverable errors.
    """
    return _get_json(path, accept="application/vnd.schemaregistry.v1+json")


def _get_json(path: str, accept: str = "application/json") -> Any:
    """
    Generic authenticated GET. Used by the Cloudera SR indexer (application/json)
    and by _get() for Confluent compatibility.
    """
    if not SR_URL:
        raise RuntimeError("SCHEMA_REGISTRY_URL is not configured")

    _kinit()
    auth = _build_auth()

    def _attempt() -> requests.Response:
        return requests.get(
            f"{SR_URL.rstrip('/')}{path}",
            auth=auth,
            timeout=30,   # aggregated endpoint can be slow on first call
            headers={"Accept": accept},
            verify=True,   # set REQUESTS_CA_BUNDLE env var to override CA in Kerberized envs
        )

    resp = _attempt()

    if resp.status_code == 401:
        logger.warning("[schema_registry] 401 Unauthorized — re-authenticating")
        global _last_kinit
        _last_kinit = 0.0   # force re-kinit
        if _kinit(force=True):
            auth = _build_auth()   # rebuild after re-kinit
            resp = _attempt()
        if resp.status_code == 401:
            logger.warning("[schema_registry] re-auth failed — returning mock schema")
            raise requests.HTTPError("401 after re-auth", response=resp)

    resp.raise_for_status()
    return resp.json()


# ── Public API ────────────────────────────────────────────────────────────────

@lru_cache(maxsize=512)
def fetch_schema_by_id(schema_id: int) -> dict:
    """Fetches an Avro/JSON schema by its numeric ID. Result is LRU-cached in process memory."""
    if not SR_URL:
        logger.debug(f"[schema_registry] no URL — mock schema for id={schema_id}")
        return _MOCK_AVRO_SCHEMA
    try:
        data = _get(f"/schemas/ids/{schema_id}")
        raw = data.get("schema", "{}")
        return json.loads(raw) if isinstance(raw, str) else raw
    except Exception as e:
        logger.warning(f"[schema_registry] fetch_schema_by_id({schema_id}) failed: {e}")
        return _MOCK_AVRO_SCHEMA


def fetch_topic_schema(topic: str, is_key: bool = False) -> dict:
    """Fetches the latest schema for a Kafka topic subject (topic-value or topic-key)."""
    suffix  = "-key" if is_key else "-value"
    subject = f"{topic}{suffix}"

    if not SR_URL:
        logger.debug(f"[schema_registry] no URL — mock schema for {subject!r}")
        return _MOCK_AVRO_SCHEMA
    try:
        data   = _get(f"/subjects/{subject}/versions/latest")
        raw    = data.get("schema", "{}")
        schema = json.loads(raw) if isinstance(raw, str) else raw
        logger.debug(f"[schema_registry] fetched schema for {subject!r} id={data.get('id')}")
        return schema
    except Exception as e:
        logger.warning(f"[schema_registry] fetch_topic_schema({subject!r}) failed: {e}")
        return _MOCK_AVRO_SCHEMA


def list_subjects() -> list[str]:
    """Lists all subjects in the Schema Registry. Falls back to plausible mock list."""
    if not SR_URL:
        return ["customer-events-value", "iot-telemetry-value", "payment-transactions-value"]
    try:
        return _get("/subjects")
    except Exception as e:
        logger.warning(f"[schema_registry] list_subjects failed: {e}")
        return []


def decode_avro_message(raw_bytes: bytes) -> dict[str, Any]:
    """
    Decodes a Confluent-framed Avro message.
    Frame: [0x00][4-byte schema_id big-endian][avro payload]
    Requires fastavro; degrades gracefully if not installed.
    """
    if not raw_bytes or len(raw_bytes) < 5 or raw_bytes[0] != 0x00:
        return {"_raw": raw_bytes[:64].hex() if raw_bytes else ""}

    schema_id  = struct.unpack(">I", raw_bytes[1:5])[0]
    avro_bytes = raw_bytes[5:]

    try:
        import io
        import fastavro
        schema = fetch_schema_by_id(schema_id)
        parsed = fastavro.parse_schema(schema)
        return dict(fastavro.schemaless_reader(io.BytesIO(avro_bytes), parsed))
    except ImportError:
        logger.debug("[schema_registry] fastavro not installed — returning schema_id only")
        return {"schema_id": schema_id, "_raw": avro_bytes[:32].hex()}
    except Exception as e:
        logger.warning(f"[schema_registry] avro decode failed (id={schema_id}): {e}")
        return {"schema_id": schema_id, "_error": str(e)}
