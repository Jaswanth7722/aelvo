# api_key.py - API Key Authentication
"""API key authentication handler with validation, rotation, and environment resolution."""

from __future__ import annotations

import os
import re
import time
import logging
from typing import Dict, List, Optional, Tuple

from auth.types import (
    AuthConfig,
    AuthMethod,
    Credential,
    CredentialType,
    ProviderConfig,
)
from auth.cred_storage import CredentialStore

log = logging.getLogger("aelvo.auth.api_key")


class APIKeyValidator:
    """Validates API keys against patterns and provider requirements."""

    # Common API key patterns for validation
    KNOWN_PATTERNS: Dict[str, str] = {
        "sk-proj-": r"^sk-proj-[A-Za-z0-9]{20,}$",           # OpenAI project
        "sk-ant-": r"^sk-ant-[A-Za-z0-9]{32,}$",              # Anthropic
        "gsk_": r"^gsk_[A-Za-z0-9]{20,}$",                    # Groq
        "AIza": r"^AIza[0-9A-Za-z\-_]{35}$",                   # Google
        "xai-": r"^xai-[A-Za-z0-9]{20,}$",                    # xAI
        "sk-": r"^sk-[A-Za-z0-9]{20,}$",                      # Generic OpenAI-style
    }

    @classmethod
    def validate_format(cls, api_key: str, provider: Optional[str] = None) -> Tuple[bool, str]:
        """Validate API key format. Returns (is_valid, reason)."""
        if not api_key or not api_key.strip():
            return False, "API key is empty"

        if len(api_key) < 10:
            return False, f"API key too short ({len(api_key)} chars, expected >= 10)"

        # Provider-specific validation
        if provider:
            config = _get_provider_key_pattern(provider)
            if config:
                pattern = config
                if not re.match(pattern, api_key):
                    return False, f"API key format does not match expected pattern for {provider}"

        # Generic pattern checks
        for prefix, pattern in cls.KNOWN_PATTERNS.items():
            if api_key.startswith(prefix):
                if not re.match(pattern, api_key):
                    return False, f"API key has {prefix} prefix but does not match expected format"
                break

        # Check for suspicious patterns
        suspicious = [
            "your-api-key", "sk-your", "sk-here", "api-key-here",
            "placeholder", "test-key", "dummy-key",
        ]
        if any(term in api_key.lower() for term in suspicious):
            return False, "API key appears to be a placeholder value"

        return True, "API key format is valid"

    @classmethod
    def mask_key(cls, api_key: str) -> str:
        """Mask an API key for logging, showing only first 4 and last 4 chars."""
        if not api_key or len(api_key) < 12:
            return "***masked***"
        return f"{api_key[:4]}...{api_key[-4:]}"


class APIKeyAuth:
    """API key authentication handler."""

    def __init__(self, cred_store: Optional[CredentialStore] = None):
        self.cred_store = cred_store or CredentialStore()

    def resolve_from_env(self, provider_config: ProviderConfig) -> Optional[str]:
        """Resolve API key from environment variables."""
        auth = provider_config.auth
        if not auth.env_var:
            return None

        key = os.environ.get(auth.env_var, "")
        if not key and auth.env_var_fallback:
            key = os.environ.get(auth.env_var_fallback, "")

        if key:
            valid, reason = APIKeyValidator.validate_format(key, provider_config.name.lower())
            if not valid:
                log.warning(f"API key for {provider_config.name} failed validation: {reason}")
            return key

        return None

    def resolve_from_store(self, provider_key: str) -> Optional[Credential]:
        """Resolve API key from credential store."""
        return self.cred_store.get_for_provider(
            provider=provider_key,
            credential_type=CredentialType.API_KEY,
        )

    def resolve(self, provider_config: ProviderConfig, use_store: bool = True) -> Optional[str]:
        """Resolve API key from any available source. Returns the key or None."""
        # 1. Try credential store first (if allowed)
        if use_store:
            cred = self.resolve_from_store(provider_config.name.lower())
            if cred:
                log.info(f"Resolved API key from credential store for {provider_config.name}")
                return cred.value

        # 2. Try environment
        key = self.resolve_from_env(provider_config)
        if key:
            log.info(f"Resolved API key from environment for {provider_config.name}")
            return key

        log.warning(f"No API key found for {provider_config.name}")
        return None

    def store(self, provider_key: str, api_key: str, label: str = "") -> bool:
        """Store an API key in the credential store."""
        valid, reason = APIKeyValidator.validate_format(api_key, provider_key)
        if not valid:
            log.error(f"Cannot store invalid API key for {provider_key}: {reason}")
            return False

        credential = Credential(
            provider=provider_key,
            credential_type=CredentialType.API_KEY,
            value=api_key,
            label=label or f"{provider_key} API Key",
        )
        return self.cred_store.store(credential)

    def validate(self, api_key: str, provider_key: str) -> Tuple[bool, str]:
        """Validate an API key format."""
        return APIKeyValidator.validate_format(api_key, provider_key)


def _get_provider_key_pattern(provider: str) -> Optional[str]:
    """Get the expected API key pattern for a provider."""
    patterns = {
        "openai": r"^sk-[A-Za-z0-9]{20,}$",
        "anthropic": r"^sk-ant-[A-Za-z0-9]{32,}$",
        "google": r"^AIza[0-9A-Za-z\-_]{35}$",
        "groq": r"^gsk_[A-Za-z0-9]{20,}$",
        "mistral": r"^[A-Za-z0-9]{32,}$",
        "deepseek": r"^[A-Za-z0-9]{32,}$",
        "xai": r"^xai-[A-Za-z0-9]{20,}$",
        "together": r"^[A-Za-z0-9]{32,}$",
        "cohere": r"^[A-Za-z0-9]{40,}$",
        "perplexity": r"^pplx-[A-Za-z0-9]{20,}$",
        "fireworks": r"^[A-Za-z0-9]{32,}$",
        "openrouter": r"^sk-or-v1-[A-Za-z0-9]{20,}$",
    }
    return patterns.get(provider.lower())
