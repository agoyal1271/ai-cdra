import os
from dotenv import load_dotenv

load_dotenv(override=True)

# LLM (OpenAI-compatible: Ollama locally, Cloudera AI Inference in prod)
LLM_BASE_URL = os.getenv("CLOUDERA_AI_URL", "http://localhost:11434/v1")
LLM_MODEL = os.getenv("CLOUDERA_AI_MODEL", "qwen2.5:14b")
LLM_API_KEY = os.getenv("CLOUDERA_AI_KEY", "ollama")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")
KAFKA_SSL_CA_LOCATION = os.getenv("KAFKA_SSL_CA_LOCATION", "")
KAFKA_KERBEROS_SERVICE_NAME = os.getenv("KAFKA_KERBEROS_SERVICE_NAME", "kafka")
KAFKA_KERBEROS_PRINCIPAL = os.getenv("KAFKA_KERBEROS_PRINCIPAL", "")
KAFKA_KERBEROS_KEYTAB = os.getenv("KAFKA_KERBEROS_KEYTAB", "")

# Schema Registry
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://cdp-utility.cdp.local:8443/gateway/cdp-proxy-api/schema-registry")
SCHEMA_REGISTRY_TYPE = os.getenv("SCHEMA_REGISTRY_TYPE", "cloudera")  # cloudera | confluent
SCHEMA_REGISTRY_AUTH_TYPE = os.getenv("SCHEMA_REGISTRY_AUTH_TYPE", "NONE")  # NONE | KERBEROS | BASIC
SCHEMA_REGISTRY_KEYTAB = os.getenv("SCHEMA_REGISTRY_KEYTAB", "")
SCHEMA_REGISTRY_PRINCIPAL = os.getenv("SCHEMA_REGISTRY_PRINCIPAL", "") 

# Iceberg
ICEBERG_CATALOG_TYPE = os.getenv("ICEBERG_CATALOG_TYPE", "hadoop")
ICEBERG_CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "/Users/archit/iceberg-warehouse")

# Knox
KNOX_LOGIN_URL = os.getenv("KNOX_LOGIN_URL", "")
KNOX_USERNAME = os.getenv("KNOX_USERNAME", "")
KNOX_PASSWORD = os.getenv("KNOX_PASSWORD", "")
KNOX_TOKEN_REFRESH_BUFFER_SECS = int(os.getenv("KNOX_TOKEN_REFRESH_BUFFER_SECS", "300"))

# Ranger (Apache Ranger for access control)
RANGER_URL = os.getenv("RANGER_URL", "http://localhost:6080")
RANGER_USERNAME = os.getenv("RANGER_USERNAME", "admin")
RANGER_PASSWORD = os.getenv("RANGER_PASSWORD", "admin")

SCHEMA_REGISTRY_USER = KNOX_USERNAME
SCHEMA_REGISTRY_PASSWORD = KNOX_PASSWORD



# KNOX_JWT — intentionally NOT a cached module-level constant.
# The token is refreshed at runtime by get_valid_knox_token() in sidecar.py,
# which must write back via os.environ["KNOX_JWT"] = new_token.
# Always call get_knox_jwt() so callers receive the current value.
KNOX_JWT = ""  # sentinel only — do not use directly for auth


def get_knox_jwt() -> str:
    """Return the current Knox JWT from the environment.

    Reading from os.getenv here (rather than a module-level constant) means
    any token refresh that writes back to os.environ is picked up immediately
    without restarting the process.
    """
    return os.getenv("KNOX_JWT", "")


# Apache Ozone (S3-compatible)
OZONE_ENDPOINT = os.getenv("OZONE_ENDPOINT", "http://localhost:9878")
OZONE_ACCESS_KEY = os.getenv("OZONE_ACCESS_KEY", "")
OZONE_SECRET_KEY = os.getenv("OZONE_SECRET_KEY", "")

# HDFS (WebHDFS REST API)
HDFS_WEBHDFS_URL = os.getenv("HDFS_WEBHDFS_URL", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "hdfs")

# Flink REST API
FLINK_REST_URL = os.getenv("FLINK_REST_URL", "http://localhost:8081")

# App
APP_PORT = int(os.getenv("CDSW_APP_PORT", "8000"))
FRONTEND_PORT = 5173



# PostgreSQL — replaces SQLite for caching
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "cloudera_ai")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Qdrant — replaces Chroma for vector embeddings
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")

# Cache TTLs
ICEBERG_CACHE_TTL_SECONDS = int(os.getenv("ICEBERG_CACHE_TTL_SECONDS", "300"))
SCHEMA_REGISTRY_CACHE_TTL_SECONDS = int(os.getenv("SCHEMA_REGISTRY_CACHE_TTL_SECONDS", "3600"))





def kafka_client_config() -> dict:
    """Builds confluent-kafka config dict from env vars."""
    cfg = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "security.protocol": KAFKA_SECURITY_PROTOCOL,
    }
    if KAFKA_SASL_MECHANISM:
        cfg["sasl.mechanism"] = KAFKA_SASL_MECHANISM
    if KAFKA_USERNAME:
        cfg["sasl.username"] = KAFKA_USERNAME
    if KAFKA_PASSWORD:
        cfg["sasl.password"] = KAFKA_PASSWORD
    if KAFKA_SSL_CA_LOCATION:
        cfg["ssl.ca.location"] = KAFKA_SSL_CA_LOCATION

    if KAFKA_SASL_MECHANISM == "GSSAPI":
        cfg["sasl.kerberos.service.name"] = KAFKA_KERBEROS_SERVICE_NAME
        if KAFKA_KERBEROS_PRINCIPAL:
            cfg["sasl.kerberos.principal"] = KAFKA_KERBEROS_PRINCIPAL
        if KAFKA_KERBEROS_KEYTAB:
            cfg["sasl.kerberos.keytab"] = KAFKA_KERBEROS_KEYTAB
        else:
            # No keytab — rely on existing TGT from `kinit`; override the default
            # kinit.cmd which references %{sasl.kerberos.keytab} and would crash.
            cfg["sasl.kerberos.kinit.cmd"] = "kinit -R"

    return cfg
