# models.py - Unified Pattern-Based Model Registry for AELVO Agentic OS
"""
This module provides a streamlined, zero-maintenance registry for AI models.
By using a provider-first architecture with dynamic fallbacks, it supports 
virtually all models released across 11+ top-tier companies.
"""

from enum import Enum
from typing import Dict, List, Optional, Any
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
    special_cases: List[ModelManifest] = [] # Only for models with unique limits (e.g., Gemini 1M)

# ============================================================================
# UNIFIED REGISTRY — 11+ Providers (All Models Supported Dynamically)
# ============================================================================

MODEL_REGISTRY: Dict[str, ProviderConfig] = {
    "nvidia": ProviderConfig(
        name="NVIDIA NIM", env_key="NVIDIA_API_KEY", 
        base_url="https://integrate.api.nvidia.com/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI, 
        default_model="nvidia/nemotron-3-super-120b-a12b"
    ),
    "openai": ProviderConfig(
        name="OpenAI", env_key="OPENAI_API_KEY", 
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI, 
        default_model="gpt-4o",
        special_cases=[
            ModelManifest(id="o1-preview", abilities=[ModelAbility.REASONING, ModelAbility.TOOL_CALLING]),
            ModelManifest(id="gpt-4", context_window=8192)
        ]
    ),
    "anthropic": ProviderConfig(
        name="Anthropic", env_key="ANTHROPIC_API_KEY",
        sdk=SDKType.ANTHROPIC, style=ChatStyle.ANTHROPIC,
        default_model="claude-3-5-sonnet-20241022",
        special_cases=[ModelManifest(id="claude-3-5-sonnet-20241022", context_window=200000)]
    ),
    "google": ProviderConfig(
        name="Google Gemini", env_key="GOOGLE_API_KEY",
        sdk=SDKType.GOOGLE, style=ChatStyle.GOOGLE,
        default_model="gemini-1.5-pro-latest",
        special_cases=[
            ModelManifest(id="gemini-1.5-pro-latest", context_window=1000000, abilities=[ModelAbility.LONG_CONTEXT, ModelAbility.TOOL_CALLING]),
            ModelManifest(id="gemini-1.5-flash-latest", context_window=1000000, abilities=[ModelAbility.FAST_INFERENCE, ModelAbility.TOOL_CALLING])
        ]
    ),
    "groq": ProviderConfig(
        name="Groq", env_key="GROQ_API_KEY", base_url="https://api.groq.com/openai/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI, default_model="llama-3.3-70b-versatile"
    ),
    "mistral": ProviderConfig(
        name="Mistral AI", env_key="MISTRAL_API_KEY", base_url="https://api.mistral.ai/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI, default_model="mistral-large-latest"
    ),
    "together": ProviderConfig(
        name="Together AI", env_key="TOGETHER_API_KEY", base_url="https://api.together.xyz/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI, default_model="meta-llama/Llama-3.3-70B-Instruct-Turbo"
    ),
    "moonshot": ProviderConfig(
        name="Moonshot AI (Kimi)", env_key="MOONSHOT_API_KEY", base_url="https://api.moonshot.ai/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI, default_model="kimi-k2.5"
    ),
    "deepseek": ProviderConfig(
        name="DeepSeek", env_key="DEEPSEEK_API_KEY", base_url="https://api.deepseek.com/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI, default_model="deepseek-chat"
    ),
    "openrouter": ProviderConfig(
        name="OpenRouter", env_key="OPENROUTER_API_KEY", base_url="https://openrouter.ai/api/v1",
        sdk=SDKType.OPENAI, style=ChatStyle.OPENAI, default_model="anthropic/claude-3.5-sonnet"
    ),
    "cohere": ProviderConfig(
       name="Cohere", env_key="COHERE_API_KEY", base_url="https://api.cohere.ai/v1",
       sdk=SDKType.OPENAI, style=ChatStyle.OPENAI, default_model="command-r-plus"
    )
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
    If the model is a 'Special Case' (e.g. Gemini 1M), it returns precise metadata.
    Otherwise, it returns a dynamic 'General Configuration' that supports ANY 
    model released by that provider today or tomorrow.
    """
    provider = get_provider_config(provider_key)
    if not provider:
        return ModelManifest(id=model_id) # Absolute Fallback

    # 1. Check for 'Special Case' (The Holy Grails)
    for m in provider.special_cases:
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
