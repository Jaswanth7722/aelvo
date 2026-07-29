"""Error semantics normalization across providers."""

from __future__ import annotations

from typing import Any, Optional


class ProviderError(Exception):
    """Base exception for provider errors."""

    def __init__(self, message: str, provider_id: str, status_code: Optional[int] = None, original_error: Optional[Exception] = None):
        self.provider_id = provider_id
        self.status_code = status_code
        self.original_error = original_error
        super().__init__(message)


class AuthenticationError(ProviderError):
    """Authentication failed (invalid API key, expired token)."""
    pass


class RateLimitError(ProviderError):
    """Rate limit exceeded."""
    pass


class QuotaExceededError(ProviderError):
    """Quota or billing limit exceeded."""
    pass


class ContextLengthError(ProviderError):
    """Input exceeds model's context window."""
    pass


class TimeoutError_(ProviderError):
    """Request timed out."""
    pass


class ServiceUnavailableError(ProviderError):
    """Provider service is unavailable."""
    pass


class InvalidRequestError(ProviderError):
    """Invalid request parameters."""
    pass


class ModelNotAvailableError(ProviderError):
    """Requested model is not available."""
    pass


class ContentFilterError(ProviderError):
    """Response blocked by content filter."""
    pass


class ErrorAdapter:
    """Normalizes error semantics across providers into canonical error types."""

    ERROR_MAP: dict[str, dict[str, Any]] = {
        "openai": {
            "401": "AuthenticationError",
            "403": "AuthenticationError",
            "429": "RateLimitError",
            "500": "ServiceUnavailableError",
            "502": "ServiceUnavailableError",
            "503": "ServiceUnavailableError",
            "400": "InvalidRequestError",
            "context_length_exceeded": "ContextLengthError",
            "insufficient_quota": "QuotaExceededError",
            "content_filter": "ContentFilterError",
            "timeout": "TimeoutError_",
        },
        "anthropic": {
            "401": "AuthenticationError",
            "403": "AuthenticationError",
            "429": "RateLimitError",
            "529": "ServiceUnavailableError",
            "overloaded": "ServiceUnavailableError",
            "too_many_tokens": "ContextLengthError",
        },
        "google": {
            "401": "AuthenticationError",
            "403": "AuthenticationError",
            "429": "RateLimitError",
            "500": "ServiceUnavailableError",
            "503": "ServiceUnavailableError",
            "SAFETY": "ContentFilterError",
            "BLOCKED": "ContentFilterError",
            "MAX_TOKENS": "ContextLengthError",
        },
    }

    @classmethod
    def normalize_error(
        cls,
        error: Exception,
        provider_id: str = "openai",
        status_code: Optional[int] = None,
    ) -> ProviderError:
        """Convert a provider-specific error to a canonical error type."""
        error_str = str(error).lower()
        provider_map = cls.ERROR_MAP.get(provider_id, cls.ERROR_MAP["openai"])

        # Check by status code
        if status_code and str(status_code) in provider_map:
            error_type = provider_map[str(status_code)]
            return cls._create_error(error_type, str(error), provider_id, status_code, error)

        # Check by error message patterns
        for pattern, error_type in provider_map.items():
            if isinstance(pattern, str) and pattern in error_str:
                return cls._create_error(error_type, str(error), provider_id, status_code, error)

        return ProviderError(str(error), provider_id, status_code, error)

    @classmethod
    def _create_error(cls, error_type: str, message: str, provider_id: str, status_code: Optional[int], original: Exception) -> ProviderError:
        error_classes = {
            "AuthenticationError": AuthenticationError,
            "RateLimitError": RateLimitError,
            "QuotaExceededError": QuotaExceededError,
            "ContextLengthError": ContextLengthError,
            "TimeoutError_": TimeoutError_,
            "ServiceUnavailableError": ServiceUnavailableError,
            "InvalidRequestError": InvalidRequestError,
            "ModelNotAvailableError": ModelNotAvailableError,
            "ContentFilterError": ContentFilterError,
        }
        cls_error = error_classes.get(error_type, ProviderError)
        return cls_error(message, provider_id, status_code, original)
