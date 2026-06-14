"""
Centralized error handling for Cloudera AI Agents.

All endpoints should raise these errors, which are caught by the global
exception handler in app.py and converted to consistent JSON responses.
"""
from typing import Any, Optional


class AgentError(Exception):
    """Base error for all agent-related exceptions."""

    def __init__(
        self,
        error_code: str,
        message: str,
        details: Optional[dict[str, Any]] = None,
        status_code: int = 400,
    ):
        """
        Args:
            error_code: Machine-readable error code (e.g., 'INVALID_REQUEST')
            message: Human-readable error message
            details: Additional error context (field-specific errors, etc.)
            status_code: HTTP status code (default: 400 Bad Request)
        """
        self.error_code = error_code
        self.message = message
        self.details = details or {}
        self.status_code = status_code
        super().__init__(self.message)

    def to_dict(self) -> dict:
        """Convert to JSON response dict."""
        return {
            "error": {
                "code": self.error_code,
                "message": self.message,
                "details": self.details,
            }
        }


class ValidationError(AgentError):
    """Request validation failed (invalid schema, missing required fields)."""

    def __init__(self, message: str, details: Optional[dict] = None):
        super().__init__(
            error_code="VALIDATION_ERROR",
            message=message,
            details=details,
            status_code=400,
        )


class ConfigurationError(AgentError):
    """Missing or misconfigured environment variables."""

    def __init__(self, message: str, missing_vars: Optional[list[str]] = None):
        details = {"missing_vars": missing_vars} if missing_vars else {}
        super().__init__(
            error_code="CONFIGURATION_ERROR",
            message=message,
            details=details,
            status_code=500,
        )


class ResourceNotFoundError(AgentError):
    """Asset, catalog, or data source not found."""

    def __init__(self, resource_type: str, resource_id: str):
        super().__init__(
            error_code="NOT_FOUND",
            message=f"{resource_type} '{resource_id}' not found",
            details={"resource_type": resource_type, "resource_id": resource_id},
            status_code=404,
        )


class ExternalServiceError(AgentError):
    """External service (Kafka, Iceberg catalog, etc.) is unavailable."""

    def __init__(self, service: str, original_error: Optional[str] = None):
        details = {"service": service}
        if original_error:
            details["original_error"] = original_error
        super().__init__(
            error_code="SERVICE_UNAVAILABLE",
            message=f"{service} service is unavailable or returned an error",
            details=details,
            status_code=503,
        )


class TimeoutError(AgentError):
    """Operation timed out."""

    def __init__(self, operation: str, timeout_seconds: float):
        super().__init__(
            error_code="TIMEOUT",
            message=f"{operation} timed out after {timeout_seconds}s",
            details={"operation": operation, "timeout_seconds": timeout_seconds},
            status_code=504,
        )


class ConflictError(AgentError):
    """Resource already exists or state conflict."""

    def __init__(self, message: str, details: Optional[dict] = None):
        super().__init__(
            error_code="CONFLICT",
            message=message,
            details=details,
            status_code=409,
        )
