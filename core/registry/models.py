# models.py - Unified Pattern-Based Model Registry for AELVO Agentic OS
"""
This module provides a streamlined, zero-maintenance registry for AI models.
By using a provider-first architecture with dynamic fallbacks, it supports
virtually all models released across 11+ top-tier companies.

Each provider carries a curated catalog of its current top models (the
``models`` field: ``ModelManifest`` entries with real API IDs, context
windows and abilities) plus a ``default_model``. The catalog is what the
CLI's ``/provider`` → ``/model`` pickers offer, so users only ever pick
models that actually exist on the provider's API.
"""

from enum import Enum
from typing import Dict, List, Optional
from pydantic import BaseModel
import os

# ============================================================================
# ENUMS & TYPES
# ============================================================================

class ChatStyle(str, Enum):
    OPENAI = "openai"       # Header-based system prompts
    ANTHROPIC = "anthropic" # System as top-level param
    GOOGLE = "google"       # Gemini-specific structure

class SDKType(str, Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GOOGLE = "google"

class ModelAbility(str, Enum):
    TOOL_CALLING = "tool_calling"
    VISION = "vision"
    STRICT_JSON = "strict_json"
    LONG_CONTEXT = "long_context"
    FAST_INFERENCE = "fast_inference"
    REASONING = "reasoning"

class ModelManifest(BaseModel):
    """Metadata for a specific LLM version."""
    id: str
    context_window: int = 128000
    abilities: List[ModelAbility] = [ModelAbility.TOOL_CALLING]

class ProviderConfig(BaseModel):
    """Enterprise configuration for an LLM Provider."""
    name: str
    env_key: str
    base_url: Optional[str] = None
    sdk: SDKType
    style: ChatStyle
    default_model: str
    models: List[ModelManifest] = []  # Curated top-model catalog (default + the rest)

    @property
    def special_cases(self) -> List[ModelManifest]:
        """Legacy alias: the non-default models in the catalog.

        Kept for code that predates the full ``models`` catalog (e.g.
        ``cli.providers.provider_models``); the canonical list is ``models``.
        """
        return [m for m in self.models if m.id != self.default_model]

# ============================================================================
# UNIFIED REGISTRY — 11+ Providers (top ~10 current models each)
# ============================================================================
# Model IDs are the providers' real API strings. Defaults are the current
# flagship for agentic/coding work; ability hints drive the CLI picker rows.

MODEL_REGISTRY: Dict[str, ProviderConfig] = {
    "openai": ProviderConfig(
        name="OpenAI", env_key="OPENAI_API_KEY",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="gpt-5",
        models=[
            ModelManifest(id="gpt-5", context_window=400000,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING]),
            ModelManifest(id="gpt-5-mini", context_window=400000,
                          abilities=[ModelAbility.REASONING, ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING]),
            ModelManifest(id="gpt-5-nano", context_window=400000,
                          abilities=[ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING]),
            ModelManifest(id="o3", context_window=200000,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING]),
            ModelManifest(id="o3-mini", context_window=200000,
                          abilities=[ModelAbility.REASONING, ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING]),
            ModelManifest(id="o4-mini", context_window=200000,
                          abilities=[ModelAbility.REASONING, ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING]),
            ModelManifest(id="gpt-4.1", context_window=1047576,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.VISION, ModelAbility.LONG_CONTEXT]),
            ModelManifest(id="gpt-4.1-mini", context_window=1047576,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE, ModelAbility.LONG_CONTEXT]),
            ModelManifest(id="gpt-4o", context_window=128000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.VISION]),
            ModelManifest(id="gpt-4o-mini", context_window=128000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE]),
        ],
    ),
    "anthropic": ProviderConfig(
        name="Anthropic", env_key="ANTHROPIC_API_KEY",
        sdk=SDKType.ANTHROPIC, style=ChatStyle.ANTHROPIC,
        default_model="claude-sonnet-4-20250514",
        models=[
            ModelManifest(id="claude-sonnet-4-20250514", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING]),
            ModelManifest(id="claude-opus-4-1-20250805", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING]),
            ModelManifest(id="claude-3-7-sonnet-20250219", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING]),
            ModelManifest(id="claude-haiku-4-5-20251001", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE]),
            ModelManifest(id="claude-3-5-sonnet-20241022", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING]),
            ModelManifest(id="claude-3-5-haiku-20241022", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE]),
        ],
    ),
    "google": ProviderConfig(
        name="Google Gemini", env_key="GOOGLE_API_KEY",
        sdk=SDKType.GOOGLE, style=ChatStyle.GOOGLE,
        default_model="gemini-2.5-pro",
        models=[
            ModelManifest(id="gemini-2.5-pro", context_window=2000000,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT]),
            ModelManifest(id="gemini-2.5-flash", context_window=1000000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE, ModelAbility.LONG_CONTEXT]),
            ModelManifest(id="gemini-2.5-flash-lite", context_window=1000000,
                          abilities=[ModelAbility.FAST_INFERENCE, ModelAbility.LONG_CONTEXT]),
            ModelManifest(id="gemini-2.0-flash", context_window=1000000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.VISION, ModelAbility.FAST_INFERENCE]),
            ModelManifest(id="gemini-1.5-pro", context_window=2000000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT]),
        ],
    ),
    "groq": ProviderConfig(
        name="Groq", env_key="GROQ_API_KEY", base_url="https://api.groq.com/openai/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="llama-3.3-70b-versatile",
        models=[
            ModelManifest(id="llama-3.3-70b-versatile", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING]),
            ModelManifest(id="llama-3.1-70b-versatile", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING]),
            ModelManifest(id="llama-3.1-8b-instant", context_window=131072,
                          abilities=[ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING]),
            ModelManifest(id="deepseek-r1-distill-llama-70b", context_window=131072,
                          abilities=[ModelAbility.REASONING]),
            ModelManifest(id="qwen-2.5-72b-instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING]),
            ModelManifest(id="openai-gpt-oss-120b", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING]),
            ModelManifest(id="gemma2-9b-it", context_window=8192,
                          abilities=[ModelAbility.FAST_INFERENCE]),
            ModelManifest(id="mixtral-8x7b-32768", context_window=32768,
                          abilities=[ModelAbility.FAST_INFERENCE]),
        ],
    ),
    "mistral": ProviderConfig(
        name="Mistral AI", env_key="MISTRAL_API_KEY", base_url="https://api.mistral.ai/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="mistral-large-latest",
        models=[
            ModelManifest(id="mistral-large-latest", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING]),
            ModelManifest(id="mistral-small-latest", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE]),
            ModelManifest(id="codestral-latest", context_window=262144,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT]),
            ModelManifest(id="pixtral-large-latest", context_window=131072,
                          abilities=[ModelAbility.VISION, ModelAbility.TOOL_CALLING]),
            ModelManifest(id="ministral-8b-latest", context_window=131072,
                          abilities=[ModelAbility.FAST_INFERENCE]),
        ],
    ),
    "cohere": ProviderConfig(
        name="Cohere", env_key="COHERE_API_KEY", base_url="https://api.cohere.ai/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="command-a-03-2025",
        models=[
            ModelManifest(id="command-a-03-2025", context_window=262144,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT]),
            ModelManifest(id="command-r7b-12-2024", context_window=131072,
                          abilities=[ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING]),
            ModelManifest(id="command-r-plus-08-2024", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING]),
            ModelManifest(id="command-r-08-2024", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING]),
            ModelManifest(id="command-r-03-2024", context_window=131072,
                          abilities=[ModelAbility.FAST_INFERENCE]),
        ],
    ),
    "deepseek": ProviderConfig(
        name="DeepSeek", env_key="DEEPSEEK_API_KEY", base_url="https://api.deepseek.com/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="deepseek-chat",
        models=[
            ModelManifest(id="deepseek-chat", context_window=65536,
                          abilities=[ModelAbility.TOOL_CALLING]),
            ModelManifest(id="deepseek-reasoner", context_window=65536,
                          abilities=[ModelAbility.REASONING]),
        ],
    ),
    "moonshot": ProviderConfig(
        name="Moonshot AI (Kimi)", env_key="MOONSHOT_API_KEY", base_url="https://api.moonshot.ai/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="kimi-k2.5",
        models=[
            ModelManifest(id="kimi-k2.5", context_window=262144,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT]),
            ModelManifest(id="kimi-k2", context_window=262144,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT]),
            ModelManifest(id="kimi-k2-turbo-preview", context_window=262144,
                          abilities=[ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING]),
            ModelManifest(id="moonshot-v1-128k", context_window=131072,
                          abilities=[ModelAbility.LONG_CONTEXT]),
            ModelManifest(id="moonshot-v1-32k", context_window=32768,
                          abilities=[ModelAbility.FAST_INFERENCE]),
        ],
    ),
    "nvidia": ProviderConfig(
        name="NVIDIA NIM", env_key="NVIDIA_API_KEY",
        base_url="https://integrate.api.nvidia.com/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="nvidia/nemotron-3-super-120b-a12b",
        models=[
            ModelManifest(id="nvidia/nemotron-3-super-120b-a12b", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING]),
            ModelManifest(id="meta/llama-3.1-nemotron-70b-instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING]),
            ModelManifest(id="nvidia/llama-3.3-nemotron-super-49b-v1", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING]),
            ModelManifest(id="nvidia/nemotron-4-340b-instruct", context_window=32768,
                          abilities=[ModelAbility.REASONING]),
            ModelManifest(id="meta/llama-3.3-70b-instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING]),
            ModelManifest(id="meta/llama-3.1-70b-instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING]),
        ],
    ),
    "together": ProviderConfig(
        name="Together AI", env_key="TOGETHER_API_KEY", base_url="https://api.together.xyz/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="meta-llama/Llama-4-Maverick-17B-128E-Instruct",
        models=[
            ModelManifest(id="meta-llama/Llama-4-Maverick-17B-128E-Instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING]),
            ModelManifest(id="meta-llama/Llama-4-Scout-17B-16E-Instruct", context_window=1048576,
                          abilities=[ModelAbility.LONG_CONTEXT, ModelAbility.FAST_INFERENCE]),
            ModelManifest(id="meta-llama/Llama-3.3-70B-Instruct-Turbo", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE]),
            ModelManifest(id="Qwen/Qwen2.5-72B-Instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING]),
            ModelManifest(id="deepseek-ai/DeepSeek-R1", context_window=65536,
                          abilities=[ModelAbility.REASONING]),
            ModelManifest(id="meta-llama/Llama-3.1-405B-Instruct-Turbo", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING]),
            ModelManifest(id="mistralai/Mixtral-8x22B-Instruct-v0.1", context_window=65536,
                          abilities=[ModelAbility.FAST_INFERENCE]),
        ],
    ),
    "openrouter": ProviderConfig(
        name="OpenRouter", env_key="OPENROUTER_API_KEY", base_url="https://openrouter.ai/api/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="anthropic/claude-3.7-sonnet",
        models=[
            ModelManifest(id="anthropic/claude-3.7-sonnet", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING]),
            ModelManifest(id="anthropic/claude-sonnet-4-20250514", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING]),
            ModelManifest(id="openai/gpt-5", context_window=400000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING]),
            ModelManifest(id="openai/gpt-4o", context_window=128000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.VISION]),
            ModelManifest(id="google/gemini-2.5-pro", context_window=2000000,
                          abilities=[ModelAbility.REASONING, ModelAbility.LONG_CONTEXT]),
            ModelManifest(id="deepseek/deepseek-chat", context_window=65536,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE]),
            ModelManifest(id="meta-llama/llama-3.3-70b-instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING]),
            ModelManifest(id="qwen/qwen3-235b-a22b", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING]),
            ModelManifest(id="moonshotai/kimi-k2", context_window=262144,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT]),
            ModelManifest(id="x-ai/grok-4", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING]),
        ],
    ),
}

# ============================================================================
# DYNAMIC KERNEL ACCESSORS — Supporting "Tomorrow's Models Today"
# ============================================================================

def get_provider_config(provider_key: str) -> Optional[ProviderConfig]:
    """Retrieves validated config for a provider (e.g., 'nvidia' or 'google')."""
    return MODEL_REGISTRY.get(provider_key.lower())

def get_model_manifest(provider_key: str, model_id: str) -> ModelManifest:
    """
    Retrieves the manifest for any model ID.
    If the model is in the provider's catalog (a special case / top model), it
    returns precise metadata. Otherwise it returns a dynamic 'General
    Configuration' that supports ANY model released by that provider today or
    tomorrow.
    """
    provider = get_provider_config(provider_key)
    if not provider:
        return ModelManifest(id=model_id)  # Absolute Fallback

    # 1. Look up the curated catalog (covers the default model too).
    for m in provider.models:
        if m.id == model_id:
            return m

    # 2. General Pattern Fallback (Tomorrow-Proof)
    # Automatically inherits TOOL_CALLING and a standard 128k window.
    return ModelManifest(id=model_id)

def get_model_abilities(provider_key: str, model_id: str) -> List[ModelAbility]:
    """Check what any model can do."""
    return get_model_manifest(provider_key, model_id).abilities

def get_context_window(provider_key: str, model_id: str) -> int:
    """Predict the token limit for any model."""
    return get_model_manifest(provider_key, model_id).context_window

def get_api_key(provider_key: str) -> str:
    """Safe retrieval of the key from the environment."""
    provider = get_provider_config(provider_key)
    return os.environ.get(provider.env_key, "") if provider else ""

def list_all_providers() -> List[str]:
    """Returns keys for all integrated providers."""
    return list(MODEL_REGISTRY.keys())
