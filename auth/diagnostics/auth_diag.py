"""Authentication diagnostics — validate auth configuration for providers."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class AuthDiagnosticResult:
    """Result of an authentication diagnostic check."""

    provider_id: str
    has_api_key_env: bool = False
    has_api_key_registered: bool = False
    has_oauth_configured: bool = False
    has_browser_flow: bool = False
    has_session: bool = False
    is_valid: bool = False
    issues: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)
    environment_variables: list[str] = field(default_factory=list)


class AuthDiagnostics:
    """Validates authentication configuration for providers."""

    PROVIDER_ENV_VARS: dict[str, list[str]] = {
        "openai": ["OPENAI_API_KEY"],
        "anthropic": ["ANTHROPIC_API_KEY"],
        "google": ["GOOGLE_API_KEY", "GEMINI_API_KEY"],
        "groq": ["GROQ_API_KEY"],
        "mistral": ["MISTRAL_API_KEY"],
        "deepseek": ["DEEPSEEK_API_KEY"],
        "cohere": ["COHERE_API_KEY"],
        "xai": ["XAI_API_KEY"],
        "together": ["TOGETHER_API_KEY"],
        "fireworks": ["FIREWORKS_API_KEY"],
        "perplexity": ["PERPLEXITY_API_KEY"],
        "openrouter": ["OPENROUTER_API_KEY"],
        "huggingface": ["HUGGINGFACE_API_KEY", "HF_API_KEY"],
        "azure": ["AZURE_OPENAI_API_KEY", "AZURE_OPENAI_ENDPOINT"],
        "bedrock": ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"],
        "vertex": ["GOOGLE_APPLICATION_CREDENTIALS", "VERTEX_AI_PROJECT_ID"],
    }

    def __init__(self) -> None:
        self._registered_keys: set[str] = set()

    def register_key(self, provider_id: str) -> None:
        self._registered_keys.add(provider_id)

    async def diagnose(self, provider_id: str) -> AuthDiagnosticResult:
        """Run auth diagnostics for a provider."""
        result = AuthDiagnosticResult(provider_id=provider_id)
        env_vars = self.PROVIDER_ENV_VARS.get(provider_id, [f"{provider_id.upper()}_API_KEY"])

        # Check environment variables
        result.environment_variables = env_vars
        for var in env_vars:
            if os.environ.get(var):
                result.has_api_key_env = True
                break

        # Check registered keys
        result.has_api_key_registered = provider_id in self._registered_keys

        # Check for missing configuration
        if not result.has_api_key_env and not result.has_api_key_registered:
            result.issues.append(f"No API key found for {provider_id}")
            result.recommendations.append(
                f"Set the {env_vars[0]} environment variable "
                f"or call register_api_key('{provider_id}', 'your-key')"
            )

        # Provider-specific checks
        if provider_id == "azure":
            if not os.environ.get("AZURE_OPENAI_ENDPOINT"):
                result.issues.append("AZURE_OPENAI_ENDPOINT not set")
                result.recommendations.append("Set AZURE_OPENAI_ENDPOINT to your Azure endpoint")

        if provider_id == "bedrock":
            if not os.environ.get("AWS_REGION"):
                result.issues.append("AWS_REGION not set")
                result.recommendations.append("Set AWS_REGION (e.g., us-east-1)")

        if provider_id == "vertex":
            if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
                result.issues.append("GOOGLE_APPLICATION_CREDENTIALS not set")
                result.recommendations.append("Set GOOGLE_APPLICATION_CREDENTIALS to your service account JSON path")

        if provider_id in ("ollama", "lm_studio", "vllm", "llamacpp"):
            result.recommendations.append(
                f"Local runtime ({provider_id}) does not require API keys. "
                f"Ensure the server is running."
            )

        result.is_valid = result.has_api_key_env or result.has_api_key_registered or provider_id in ("ollama", "lm_studio", "vllm", "llamacpp")

        return result

    async def diagnose_all(self) -> dict[str, AuthDiagnosticResult]:
        results = {}
        for pid in self.PROVIDER_ENV_VARS:
            results[pid] = await self.diagnose(pid)
        return results

    def summary(self, results: dict[str, AuthDiagnosticResult]) -> list[dict[str, Any]]:
        return [
            {
                "provider": pid,
                "configured": "✅" if r.is_valid else "❌",
                "env_var": "✅" if r.has_api_key_env else "❌",
                "registered": "✅" if r.has_api_key_registered else "❌",
                "issues": "; ".join(r.issues[:2]),
            }
            for pid, r in results.items()
        ]
