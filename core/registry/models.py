# models.py - Unified Pattern-Based Model Registry for AELVO Agentic OS
"""
This module provides a streamlined, zero-maintenance registry for AI models.
By using a provider-first architecture with dynamic fallbacks, it supports
virtually all models released across 11+ top-tier companies.

Each provider carries a curated catalog of its current top models (the
``models`` field: ``ModelManifest`` entries with real API IDs, context
windows, abilities and a cost tier) plus a ``default_model``. The catalog is
what the CLI's ``/provider`` → ``/model`` pickers offer, so users only ever
pick models that actually exist on the provider's API.

Cost tiers are curated for the catalog; models that come from a provider's
*live* model list (not in the catalog) get their context window and a
price-derived tier from the runtime registry (``auth.config``) when the
provider publishes per-token pricing.
"""

from enum import Enum
from typing import Dict, List, Optional, Tuple
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

class CostTier(str, Enum):
    """Rough per-token cost band shown as $ / $$ / $$$ in the picker.

    BUDGET:    combined in+out < $3 per 1M tokens
    STANDARD:  $3 – $20
    PREMIUM:   ≥ $20
    """
    BUDGET = "budget"
    STANDARD = "standard"
    PREMIUM = "premium"

class ModelManifest(BaseModel):
    """Metadata for a specific LLM version."""
    id: str
    context_window: int = 128000
    abilities: List[ModelAbility] = [ModelAbility.TOOL_CALLING]
    cost_tier: CostTier = CostTier.STANDARD

class ProviderConfig(BaseModel):
    """Enterprise configuration for an LLM Provider."""
    name: str
    env_key: str
    base_url: Optional[str] = None
    sdk: SDKType
    style: ChatStyle
    default_model: str
    models: List[ModelManifest] = []  # Curated top-model catalog (default + the rest)
    local: bool = False  # Local runtime (ollama/lm_studio/vllm/llama.cpp): no API key

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
# Cost tiers mirror the providers' published per-1M-token pricing.

MODEL_REGISTRY: Dict[str, ProviderConfig] = {
    "openai": ProviderConfig(
        name="OpenAI", env_key="OPENAI_API_KEY",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="gpt-5",
        models=[
            ModelManifest(id="gpt-5", context_window=400000,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="gpt-5-mini", context_window=400000,
                          abilities=[ModelAbility.REASONING, ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="gpt-oss-120b", context_window=131072,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="o3", context_window=200000,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="o3-mini", context_window=200000,
                          abilities=[ModelAbility.REASONING, ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="o4-mini", context_window=200000,
                          abilities=[ModelAbility.REASONING, ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="gpt-4.1", context_window=1047576,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.VISION, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="gpt-4.1-mini", context_window=1047576,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="gpt-4o", context_window=128000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.VISION],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="gpt-4o-mini", context_window=128000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.BUDGET),
        ],
    ),
    "anthropic": ProviderConfig(
        name="Anthropic", env_key="ANTHROPIC_API_KEY",
        sdk=SDKType.ANTHROPIC, style=ChatStyle.ANTHROPIC,
        default_model="claude-sonnet-5",
        models=[
            ModelManifest(id="claude-sonnet-5", context_window=1000000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING, ModelAbility.VISION, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="claude-opus-5", context_window=1000000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING, ModelAbility.VISION, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.PREMIUM),
            ModelManifest(id="claude-opus-4-7", context_window=1000000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING, ModelAbility.VISION],
                          cost_tier=CostTier.PREMIUM),
            ModelManifest(id="claude-opus-4-8", context_window=1000000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING, ModelAbility.VISION],
                          cost_tier=CostTier.PREMIUM),
            ModelManifest(id="claude-sonnet-4-6", context_window=1000000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING, ModelAbility.VISION],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="claude-sonnet-4-20250514", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="claude-opus-4-1-20250805", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING],
                          cost_tier=CostTier.PREMIUM),
            ModelManifest(id="claude-haiku-4-5-20251001", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="claude-3-7-sonnet-20250219", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="claude-3-5-haiku-20241022", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.STANDARD),
        ],
    ),
    "google": ProviderConfig(
        name="Google Gemini", env_key="GOOGLE_API_KEY",
        sdk=SDKType.GOOGLE, style=ChatStyle.GOOGLE,
        default_model="gemini-2.5-pro",
        models=[
            ModelManifest(id="gemini-3.1-pro-preview", context_window=1048576,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.PREMIUM),
            ModelManifest(id="gemini-3.5-flash", context_window=1048576,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="gemini-3.5-flash-lite", context_window=1048576,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="gemini-2.5-pro", context_window=2000000,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="gemini-2.5-flash", context_window=1000000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="gemini-2.5-flash-lite", context_window=1000000,
                          abilities=[ModelAbility.FAST_INFERENCE, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="gemini-2.0-flash", context_window=1000000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.VISION, ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="gemini-1.5-pro", context_window=2000000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.STANDARD),
        ],
    ),
    "groq": ProviderConfig(
        name="Groq", env_key="GROQ_API_KEY", base_url="https://api.groq.com/openai/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="llama-3.3-70b-versatile",
        models=[
            ModelManifest(id="llama-3.3-70b-versatile", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="llama-3.1-70b-versatile", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="llama-3.1-8b-instant", context_window=131072,
                          abilities=[ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="llama-4-scout-17b-16e-instruct", context_window=1048576,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="llama-4-maverick-17b-128e-instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="deepseek-r1-distill-llama-70b", context_window=131072,
                          abilities=[ModelAbility.REASONING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="qwen-2.5-72b-instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="openai-gpt-oss-120b", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="gemma2-9b-it", context_window=8192,
                          abilities=[ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="mixtral-8x7b-32768", context_window=32768,
                          abilities=[ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.BUDGET),
        ],
    ),
    "mistral": ProviderConfig(
        name="Mistral AI", env_key="MISTRAL_API_KEY", base_url="https://api.mistral.ai/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="mistral-large-latest",
        models=[
            ModelManifest(id="mistral-large-latest", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="mistral-small-latest", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="codestral-latest", context_window=262144,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="pixtral-large-latest", context_window=131072,
                          abilities=[ModelAbility.VISION, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="ministral-8b-latest", context_window=131072,
                          abilities=[ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.BUDGET),
        ],
    ),
    "cohere": ProviderConfig(
        name="Cohere", env_key="COHERE_API_KEY", base_url="https://api.cohere.ai/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="command-a-03-2025",
        models=[
            ModelManifest(id="command-a-03-2025", context_window=262144,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="command-a-pro", context_window=262144,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="command-r7b-12-2024", context_window=131072,
                          abilities=[ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="command-r-plus-08-2024", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="command-r-08-2024", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
        ],
    ),
    "deepseek": ProviderConfig(
        name="DeepSeek", env_key="DEEPSEEK_API_KEY", base_url="https://api.deepseek.com/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="deepseek-chat",
        models=[
            # deepseek-chat is DeepSeek-V3, deepseek-reasoner is DeepSeek-R1 —
            # those are the platform's only two callable API IDs.
            ModelManifest(id="deepseek-chat", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="deepseek-reasoner", context_window=131072,
                          abilities=[ModelAbility.REASONING],
                          cost_tier=CostTier.BUDGET),
        ],
    ),
    "moonshot": ProviderConfig(
        name="Moonshot AI (Kimi)", env_key="MOONSHOT_API_KEY", base_url="https://api.moonshot.ai/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="kimi-k2.5",
        models=[
            ModelManifest(id="kimi-k2.5", context_window=262144,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="kimi-k2", context_window=262144,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="kimi-k2-turbo-preview", context_window=262144,
                          abilities=[ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="kimi-k2.5-thinking", context_window=262144,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="moonshot-v1-128k", context_window=131072,
                          abilities=[ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="moonshot-v1-32k", context_window=32768,
                          abilities=[ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.BUDGET),
        ],
    ),
    "nvidia": ProviderConfig(
        name="NVIDIA NIM", env_key="NVIDIA_API_KEY",
        base_url="https://integrate.api.nvidia.com/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="nvidia/nemotron-3-super-120b-a12b",
        models=[
            ModelManifest(id="nvidia/nemotron-3-super-120b-a12b", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="nvidia/nemotron-3-super-49b-v1", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="meta/llama-3.1-nemotron-70b-instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="nvidia/nemotron-4-340b-instruct", context_window=32768,
                          abilities=[ModelAbility.REASONING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="meta/llama-3.3-70b-instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="meta/llama-3.1-70b-instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
        ],
    ),
    "together": ProviderConfig(
        name="Together AI", env_key="TOGETHER_API_KEY", base_url="https://api.together.xyz/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="meta-llama/Llama-4-Maverick-17B-128E-Instruct",
        models=[
            ModelManifest(id="meta-llama/Llama-4-Maverick-17B-128E-Instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="meta-llama/Llama-4-Scout-17B-16E-Instruct", context_window=1048576,
                          abilities=[ModelAbility.LONG_CONTEXT, ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="Qwen/Qwen3-235B-A22B", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="Qwen/Qwen2.5-72B-Instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="deepseek-ai/DeepSeek-V3", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="deepseek-ai/DeepSeek-R1", context_window=131072,
                          abilities=[ModelAbility.REASONING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="meta-llama/Llama-3.3-70B-Instruct-Turbo", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="meta-llama/Llama-3.1-405B-Instruct-Turbo", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
        ],
    ),
    "openrouter": ProviderConfig(
        name="OpenRouter", env_key="OPENROUTER_API_KEY", base_url="https://openrouter.ai/api/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="anthropic/claude-3.7-sonnet",
        models=[
            ModelManifest(id="anthropic/claude-3.7-sonnet", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="anthropic/claude-sonnet-4-20250514", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="openai/gpt-5", context_window=400000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="openai/gpt-4o", context_window=128000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.VISION],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="google/gemini-2.5-pro", context_window=2000000,
                          abilities=[ModelAbility.REASONING, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="google/gemini-3.5-flash", context_window=1048576,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="deepseek/deepseek-chat", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="meta-llama/llama-3.3-70b-instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="qwen/qwen3-235b-a22b", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="moonshotai/kimi-k2", context_window=262144,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="x-ai/grok-4", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING],
                          cost_tier=CostTier.STANDARD),
        ],
    ),
    "xai": ProviderConfig(
        name="xAI (Grok)", env_key="XAI_API_KEY",
        base_url="https://api.x.ai/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="grok-4",
        models=[
            ModelManifest(id="grok-4", context_window=131072,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="grok-4.5", context_window=131072,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="grok-4-mini", context_window=131072,
                          abilities=[ModelAbility.REASONING, ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="grok-3", context_window=131072,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="grok-3-mini", context_window=131072,
                          abilities=[ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="grok-3-fast", context_window=131072,
                          abilities=[ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
        ],
    ),
    "fireworks": ProviderConfig(
        name="Fireworks AI", env_key="FIREWORKS_API_KEY",
        base_url="https://api.fireworks.ai/inference/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="accounts/fireworks/models/llama-v3p3-70b-instruct",
        models=[
            ModelManifest(id="accounts/fireworks/models/llama-v3p3-70b-instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="accounts/fireworks/models/deepseek-v3", context_window=131072,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="accounts/fireworks/models/deepseek-r1", context_window=131072,
                          abilities=[ModelAbility.REASONING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="accounts/fireworks/models/qwen3-235b-a22b", context_window=131072,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="accounts/fireworks/models/llama-v3p1-405b-instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="accounts/fireworks/models/gemma3-27b-it", context_window=8192,
                          abilities=[ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
        ],
    ),
    "perplexity": ProviderConfig(
        name="Perplexity", env_key="PERPLEXITY_API_KEY",
        base_url="https://api.perplexity.ai",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="sonar-pro",
        models=[
            ModelManifest(id="sonar-pro", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="sonar", context_window=127000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="sonar-reasoning-pro", context_window=200000,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="sonar-reasoning", context_window=127000,
                          abilities=[ModelAbility.REASONING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="sonar-deep-research", context_window=200000,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.PREMIUM),
        ],
    ),
    "huggingface": ProviderConfig(
        name="Hugging Face", env_key="HUGGINGFACE_API_KEY",
        base_url="https://api-inference.huggingface.co/models",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="meta-llama/Llama-3.3-70B-Instruct",
        models=[
            ModelManifest(id="meta-llama/Llama-3.3-70B-Instruct", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="meta-llama/Llama-3.1-405B-Instruct", context_window=131072,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.PREMIUM),
            ModelManifest(id="Qwen/Qwen3-235B-A22B", context_window=131072,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="zai-org/GLM-5.1", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="deepseek-ai/DeepSeek-V3", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="mistralai/Mistral-7B-Instruct-v0.3", context_window=32768,
                          abilities=[ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.BUDGET),
        ],
    ),
    "ollama": ProviderConfig(
        name="Ollama (Local)", env_key="OLLAMA_BASE_URL",
        base_url="http://localhost:11434/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="llama3.2",
        local=True,
        models=[
            ModelManifest(id="llama3.2", context_window=32768,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="llama3.1", context_window=32768,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="llama3.3", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="qwen2.5-coder", context_window=32768,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="qwen2.5", context_window=32768,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="qwen3", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="gemma3", context_window=131072,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.VISION, ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="mistral", context_window=32768,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="codellama", context_window=16384,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="deepseek-r1", context_window=65536,
                          abilities=[ModelAbility.REASONING],
                          cost_tier=CostTier.BUDGET),
        ],
    ),
    "lm_studio": ProviderConfig(
        name="LM Studio (Local)", env_key="LM_STUDIO_BASE_URL",
        base_url="http://localhost:1234/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="local-model",
        local=True,
        models=[
            ModelManifest(id="local-model", context_window=65536,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
        ],
    ),
    "vllm": ProviderConfig(
        name="vLLM (Local)", env_key="VLLM_BASE_URL",
        base_url="http://localhost:8000/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="local-model",
        local=True,
        models=[
            ModelManifest(id="local-model", context_window=32768,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
        ],
    ),
    "llama_cpp": ProviderConfig(
        name="llama.cpp (Local)", env_key="LLAMA_CPP_BASE_URL",
        base_url="http://localhost:8080/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="local-model",
        local=True,
        models=[
            ModelManifest(id="local-model", context_window=4096,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
        ],
    ),
    "azure": ProviderConfig(
        name="Azure OpenAI", env_key="AZURE_OPENAI_API_KEY",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="gpt-5",
        models=[
            ModelManifest(id="gpt-5", context_window=400000,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="gpt-5-mini", context_window=400000,
                          abilities=[ModelAbility.REASONING, ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="gpt-4o", context_window=128000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.VISION],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="gpt-4o-mini", context_window=128000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="gpt-4.1", context_window=1047576,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="gpt-4.1-mini", context_window=1047576,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.BUDGET),
        ],
    ),
    "bedrock": ProviderConfig(
        name="AWS Bedrock", env_key="AWS_ACCESS_KEY_ID",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI,
        default_model="anthropic.claude-sonnet-4-20250514",
        models=[
            ModelManifest(id="anthropic.claude-sonnet-4-20250514", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="anthropic.claude-opus-4-7", context_window=1000000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING],
                          cost_tier=CostTier.PREMIUM),
            ModelManifest(id="anthropic.claude-sonnet-4-6", context_window=1000000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.REASONING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="anthropic.claude-haiku-4-5-20251001-v1:0", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="anthropic.claude-3-5-sonnet-20241022-v2", context_window=200000,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="meta.llama3-70b-instruct-v1", context_window=8192,
                          abilities=[ModelAbility.TOOL_CALLING],
                          cost_tier=CostTier.BUDGET),
        ],
    ),
    "vertex": ProviderConfig(
        name="Google Vertex AI", env_key="GOOGLE_APPLICATION_CREDENTIALS",
        sdk=SDKType.GOOGLE, style=ChatStyle.GOOGLE,
        default_model="gemini-2.5-pro",
        models=[
            ModelManifest(id="gemini-3.1-pro-preview", context_window=1048576,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.PREMIUM),
            ModelManifest(id="gemini-2.5-pro", context_window=1000000,
                          abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.STANDARD),
            ModelManifest(id="gemini-2.5-flash", context_window=1000000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="gemini-2.5-flash-lite", context_window=1000000,
                          abilities=[ModelAbility.FAST_INFERENCE, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.BUDGET),
            ModelManifest(id="gemini-2.0-flash", context_window=1000000,
                          abilities=[ModelAbility.TOOL_CALLING, ModelAbility.FAST_INFERENCE, ModelAbility.LONG_CONTEXT],
                          cost_tier=CostTier.BUDGET),
        ],
    ),
}

# ============================================================================
# RUNTIME PRICING — real per-1M-token prices for live-only models
# ============================================================================
# Lazily built once from auth.config's PROVIDER_REGISTRY (which publishes
# ``pricing_per_1m_input/output`` for ~60 models). Models in the curated
# catalog above keep their curated tier; live-only ids fall back to this.

_RUNTIME_MODELS: Optional[Dict[str, Tuple[int, float, float]]] = None
# {model_id: (context_window, price_in_1m, price_out_1m)}


def _runtime_models() -> Dict[str, Tuple[int, float, float]]:
    global _RUNTIME_MODELS
    if _RUNTIME_MODELS is None:
        table: Dict[str, Tuple[int, float, float]] = {}
        try:
            from auth.config import PROVIDER_REGISTRY

            for _pkey, cfg in PROVIDER_REGISTRY.items():
                for m in getattr(cfg, "models", None) or []:
                    p_in = getattr(m, "pricing_per_1m_input", None)
                    p_out = getattr(m, "pricing_per_1m_output", None)
                    if p_in is None or p_out is None:
                        continue
                    try:
                        table[m.id] = (
                            int(getattr(m, "context_window", 0) or 0),
                            float(p_in),
                            float(p_out),
                        )
                    except (TypeError, ValueError):
                        continue
        except Exception:
            table = {}
        _RUNTIME_MODELS = table
    return _RUNTIME_MODELS


def _tier_from_price(price_in_1m: float, price_out_1m: float) -> CostTier:
    combined = price_in_1m + price_out_1m
    if combined < 3.0:
        return CostTier.BUDGET
    if combined >= 20.0:
        return CostTier.PREMIUM
    return CostTier.STANDARD

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
    returns precise metadata. Live-only models (from the provider's own model
    list) are enriched with the runtime registry's context window and a
    price-derived cost tier when available. Anything else gets a dynamic
    'General Configuration' that supports ANY model released by that provider
    today or tomorrow.
    """
    provider = get_provider_config(provider_key)
    if not provider:
        return ModelManifest(id=model_id)  # Absolute Fallback

    # 1. Look up the curated catalog (covers the default model too).
    for m in provider.models:
        if m.id == model_id:
            return m

    # 2. Runtime enrichment for live-only models: real context + cost tier.
    rt = _runtime_models().get(model_id)
    if rt is not None and rt[0]:
        return ModelManifest(
            id=model_id,
            context_window=rt[0],
            cost_tier=_tier_from_price(rt[1], rt[2]),
        )

    # 3. General Pattern Fallback (Tomorrow-Proof)
    # Automatically inherits TOOL_CALLING and a standard 128k window.
    return ModelManifest(id=model_id)

def get_model_abilities(provider_key: str, model_id: str) -> List[ModelAbility]:
    """Check what any model can do."""
    return get_model_manifest(provider_key, model_id).abilities

def get_context_window(provider_key: str, model_id: str) -> int:
    """Predict the token limit for any model."""
    return get_model_manifest(provider_key, model_id).context_window

def get_cost_tier(provider_key: str, model_id: str) -> CostTier:
    """Cost tier for any model: curated catalog, then runtime pricing, else standard."""
    return get_model_manifest(provider_key, model_id).cost_tier

def format_context_window(window: int) -> str:
    """Human-friendly token count: 131072 → '128k', 2000000 → '2M'.

    Computes both a decimal (×1000) and binary (×1024) rendering and picks
    whichever lands closest to a round number — 131072 → '128k' (binary) while
    128000 → '128k' (decimal). Decimal wins on ties. Windows below 1k render
    as raw digits (512 → '512'); missing, zero or negative windows → '?'.
    """
    if not window or window < 0:
        return "?"
    if window < 1000:
        return str(int(window))
    candidates = []  # (closeness-to-round, is_binary, label)
    for value, _base, is_binary in (
        (window / 1000, 1000, False),
        (window / 1024, 1024, True),
    ):
        if value >= 1000:  # millions, in the scaled unit (1023k ≈ 1M)
            value = value / 1024 if is_binary else value / 1000
            label = f"{value:.1f}".rstrip("0").rstrip(".") + "M"
        else:
            label = f"{value:.1f}".rstrip("0").rstrip(".") + "k"
        candidates.append((abs(value - round(value)), is_binary, label))
    candidates.sort()
    return candidates[0][2]

def get_api_key(provider_key: str) -> str:
    """Safe retrieval of the key from the environment."""
    provider = get_provider_config(provider_key)
    return os.environ.get(provider.env_key, "") if provider else ""

def list_all_providers() -> List[str]:
    """Returns keys for all integrated providers."""
    return list(MODEL_REGISTRY.keys())
