# config.py - Provider Configuration Registry
"""
Full provider configuration registry supporting 20+ providers across
foundation models, coding runtimes, embedding providers, and image/multimodal providers.

Every provider is registered with its auth configuration, capabilities, 
pricing, rate limits, and supported models.
"""

from typing import Dict, List, Optional
from auth.types import (
    AuthMethod,
    AuthConfig,
    Capability,
    CredentialType,
    ModelFamily,
    ModelConfig,
    ProviderConfig,
    ProviderKind,
)


# ============================================================================
# AUTH CONFIGURATIONS
# ============================================================================

AUTH_API_KEY = AuthConfig(
    method=AuthMethod.API_KEY,
    credential_type=CredentialType.API_KEY,
    header_template="Bearer {key}",
    supports_refresh=False,
)

AUTH_API_KEY_BASIC = AuthConfig(
    method=AuthMethod.API_KEY,
    credential_type=CredentialType.API_KEY,
    header_template="{key}",
    supports_refresh=False,
)

AUTH_OAUTH_DEVICE = AuthConfig(
    method=AuthMethod.OAUTH_DEVICE,
    credential_type=CredentialType.OAUTH_TOKEN,
    supports_refresh=True,
    oauth_scopes=["openid", "profile", "email"],
)

AUTH_LOCAL_TRUST = AuthConfig(
    method=AuthMethod.LOCAL_TRUST,
    credential_type=CredentialType.API_KEY,
    default_value="local-trust-mode",
    header_template="",
)

AUTH_AZURE_IDENTITY = AuthConfig(
    method=AuthMethod.AZURE_IDENTITY,
    credential_type=CredentialType.AZURE_TENANT,
    header_template="api-key {key}",
    supports_refresh=True,
)

AUTH_AWS_SIGNATURE = AuthConfig(
    method=AuthMethod.AWS_SIGNATURE,
    credential_type=CredentialType.AWS_ACCESS_KEY,
    supports_refresh=True,
)

AUTH_GCP_SERVICE_ACCOUNT = AuthConfig(
    method=AuthMethod.GCP_SERVICE_ACCOUNT,
    credential_type=CredentialType.GCP_CREDENTIALS,
    supports_refresh=True,
)

AUTH_HUGGINGFACE_TOKEN = AuthConfig(
    method=AuthMethod.HUGGINGFACE_TOKEN,
    credential_type=CredentialType.BEARER_TOKEN,
    header_template="Bearer {token}",
    supports_refresh=False,
)

AUTH_BEARER = AuthConfig(
    method=AuthMethod.BEARER_TOKEN,
    credential_type=CredentialType.BEARER_TOKEN,
    header_template="Bearer {token}",
    supports_refresh=False,
)


# ============================================================================
# FOUNDATION MODEL PROVIDERS
# ============================================================================

PROVIDER_REGISTRY: Dict[str, ProviderConfig] = {
    # ------------------------------------------------------------------
    # OpenAI
    # ------------------------------------------------------------------
    "openai": ProviderConfig(
        name="OpenAI",
        kind=ProviderKind.FOUNDATION,
        base_url="https://api.openai.com/v1",
        auth=AUTH_API_KEY.model_copy(update={"env_var": "OPENAI_API_KEY"}),
        default_model="gpt-4o",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.PARALLEL_TOOL_CALLING, Capability.STRUCTURED_OUTPUT,
            Capability.VISION, Capability.FUNCTION_CALLING, Capability.JSON_MODE,
            Capability.SYSTEM_MESSAGE, Capability.MULTI_TURN, Capability.STOP_SEQUENCE,
            Capability.TEMPERATURE, Capability.TOP_P, Capability.MAX_TOKENS,
            Capability.FREQUENCY_PENALTY, Capability.PRESENCE_PENALTY,
            Capability.SEED, Capability.RESPONSE_FORMAT, Capability.LOGPROBS,
        },
        models=[
            ModelConfig(id="gpt-4o", provider="openai", family=ModelFamily.MULTIMODAL, context_window=128000,
                       capabilities={Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING, Capability.VISION,
                                    Capability.STRUCTURED_OUTPUT, Capability.FUNCTION_CALLING},
                       pricing_per_1m_input=2.50, pricing_per_1m_output=10.00),
            ModelConfig(id="gpt-4o-mini", provider="openai", family=ModelFamily.INSTRUCTION, context_window=128000,
                       pricing_per_1m_input=0.15, pricing_per_1m_output=0.60),
            ModelConfig(id="gpt-4-turbo", provider="openai", family=ModelFamily.INSTRUCTION, context_window=128000,
                       pricing_per_1m_input=10.00, pricing_per_1m_output=30.00),
            ModelConfig(id="gpt-4", provider="openai", family=ModelFamily.INSTRUCTION, context_window=8192,
                       pricing_per_1m_input=30.00, pricing_per_1m_output=60.00),
            ModelConfig(id="o1", provider="openai", family=ModelFamily.REASONING, context_window=200000,
                       capabilities={Capability.REASONING, Capability.TOOL_CALLING},
                       pricing_per_1m_input=15.00, pricing_per_1m_output=60.00),
            ModelConfig(id="o3-mini", provider="openai", family=ModelFamily.REASONING, context_window=200000,
                       capabilities={Capability.REASONING, Capability.TOOL_CALLING},
                       pricing_per_1m_input=1.10, pricing_per_1m_output=4.40),
            ModelConfig(id="o4-mini", provider="openai", family=ModelFamily.REASONING, context_window=200000,
                       capabilities={Capability.REASONING, Capability.TOOL_CALLING},
                       pricing_per_1m_input=1.10, pricing_per_1m_output=4.40),
            ModelConfig(id="text-embedding-3-small", provider="openai", family=ModelFamily.EMBEDDING,
                       capabilities={Capability.EMBEDDING},
                       pricing_per_1m_input=0.02, pricing_per_1m_output=0.00),
            ModelConfig(id="text-embedding-3-large", provider="openai", family=ModelFamily.EMBEDDING,
                       capabilities={Capability.EMBEDDING},
                       pricing_per_1m_input=0.13, pricing_per_1m_output=0.00),
        ],
        supported_models=["gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-4", "o1", "o3-mini", "o4-mini",
                         "text-embedding-3-small", "text-embedding-3-large"],
        streaming_supported=True,
        tool_calling_supported=True,
        vision_supported=True,
        structured_output_supported=True,
        reasoning_supported=True,
        rate_limit_rpm=500,
        rate_limit_tpm=200000,
        sdk_type="openai",
        pricing_per_1m_input=2.50,
        pricing_per_1m_output=10.00,
    ),

    # ------------------------------------------------------------------
    # Anthropic
    # ------------------------------------------------------------------
    "anthropic": ProviderConfig(
        name="Anthropic",
        kind=ProviderKind.FOUNDATION,
        base_url="https://api.anthropic.com/v1",
        auth=AUTH_API_KEY.model_copy(update={
            "env_var": "ANTHROPIC_API_KEY",
            "header_template": "x-api-key {key}",
        }),
        default_model="claude-sonnet-4-20250514",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.PARALLEL_TOOL_CALLING, Capability.VISION,
            Capability.SYSTEM_MESSAGE, Capability.MULTI_TURN, Capability.STOP_SEQUENCE,
            Capability.TEMPERATURE, Capability.TOP_P, Capability.MAX_TOKENS,
            Capability.STRUCTURED_OUTPUT, Capability.REASONING,
        },
        models=[
            ModelConfig(id="claude-sonnet-4-20250514", provider="anthropic", family=ModelFamily.MULTIMODAL,
                       context_window=200000,
                       capabilities={Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING, Capability.VISION,
                                    Capability.STRUCTURED_OUTPUT, Capability.REASONING},
                       pricing_per_1m_input=3.00, pricing_per_1m_output=15.00),
            ModelConfig(id="claude-3-5-sonnet-20241022", provider="anthropic", family=ModelFamily.MULTIMODAL,
                       context_window=200000,
                       pricing_per_1m_input=3.00, pricing_per_1m_output=15.00),
            ModelConfig(id="claude-3-5-haiku-20241022", provider="anthropic", family=ModelFamily.FAST_INFERENCE,
                       context_window=200000,
                       pricing_per_1m_input=0.80, pricing_per_1m_output=4.00),
            ModelConfig(id="claude-3-opus-20240229", provider="anthropic", family=ModelFamily.REASONING,
                       context_window=200000,
                       pricing_per_1m_input=15.00, pricing_per_1m_output=75.00),
            ModelConfig(id="claude-3-haiku-20240307", provider="anthropic", family=ModelFamily.FAST_INFERENCE,
                       context_window=200000,
                       pricing_per_1m_input=0.25, pricing_per_1m_output=1.25),
        ],
        supported_models=["claude-sonnet-4-20250514", "claude-3-5-sonnet-20241022", "claude-3-5-haiku-20241022",
                         "claude-3-opus-20240229", "claude-3-haiku-20240307"],
        streaming_supported=True,
        tool_calling_supported=True,
        vision_supported=True,
        structured_output_supported=True,
        reasoning_supported=True,
        rate_limit_rpm=400,
        rate_limit_tpm=160000,
        sdk_type="anthropic",
        pricing_per_1m_input=3.00,
        pricing_per_1m_output=15.00,
    ),

    # ------------------------------------------------------------------
    # Google Gemini
    # ------------------------------------------------------------------
    "google": ProviderConfig(
        name="Google Gemini",
        kind=ProviderKind.FOUNDATION,
        base_url="https://generativelanguage.googleapis.com/v1beta",
        auth=AUTH_API_KEY.model_copy(update={
            "env_var": "GOOGLE_API_KEY",
            "header_template": "Bearer {key}",
        }),
        default_model="gemini-2.5-pro-exp-03-25",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.VISION, Capability.SYSTEM_MESSAGE, Capability.MULTI_TURN,
            Capability.STOP_SEQUENCE, Capability.TEMPERATURE, Capability.TOP_P,
            Capability.MAX_TOKENS, Capability.LONG_CONTEXT, Capability.JSON_MODE,
            Capability.AUDIO,
        },
        models=[
            ModelConfig(id="gemini-2.5-pro-exp-03-25", provider="google", family=ModelFamily.REASONING,
                       context_window=1000000,
                       capabilities={Capability.LONG_CONTEXT, Capability.TOOL_CALLING, Capability.VISION, Capability.AUDIO},
                       pricing_per_1m_input=1.25, pricing_per_1m_output=10.00),
            ModelConfig(id="gemini-2.5-flash-preview-04-17", provider="google", family=ModelFamily.FAST_INFERENCE,
                       context_window=1000000,
                       pricing_per_1m_input=0.15, pricing_per_1m_output=0.60),
            ModelConfig(id="gemini-2.0-flash", provider="google", family=ModelFamily.FAST_INFERENCE,
                       context_window=1000000,
                       pricing_per_1m_input=0.10, pricing_per_1m_output=0.40),
            ModelConfig(id="gemini-1.5-pro", provider="google", family=ModelFamily.MULTIMODAL,
                       context_window=1000000,
                       pricing_per_1m_input=1.25, pricing_per_1m_output=10.00),
            ModelConfig(id="gemini-1.5-flash", provider="google", family=ModelFamily.FAST_INFERENCE,
                       context_window=1000000,
                       pricing_per_1m_input=0.075, pricing_per_1m_output=0.30),
            ModelConfig(id="text-embedding-004", provider="google", family=ModelFamily.EMBEDDING,
                       capabilities={Capability.EMBEDDING}),
        ],
        supported_models=["gemini-2.5-pro-exp-03-25", "gemini-2.5-flash-preview-04-17", "gemini-2.0-flash",
                         "gemini-1.5-pro", "gemini-1.5-flash", "text-embedding-004"],
        streaming_supported=True,
        tool_calling_supported=True,
        vision_supported=True,
        context_window_multiplier=1.0,
        rate_limit_rpm=360,
        sdk_type="google",
        pricing_per_1m_input=1.25,
        pricing_per_1m_output=10.00,
    ),

    # ------------------------------------------------------------------
    # Groq
    # ------------------------------------------------------------------
    "groq": ProviderConfig(
        name="Groq",
        kind=ProviderKind.FOUNDATION,
        base_url="https://api.groq.com/openai/v1",
        auth=AUTH_API_KEY.model_copy(update={"env_var": "GROQ_API_KEY"}),
        default_model="llama-3.3-70b-versatile",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.JSON_MODE, Capability.SYSTEM_MESSAGE, Capability.MULTI_TURN,
            Capability.TEMPERATURE, Capability.TOP_P, Capability.MAX_TOKENS,
            Capability.STOP_SEQUENCE,
        },
        models=[
            ModelConfig(id="llama-3.3-70b-versatile", provider="groq", family=ModelFamily.INSTRUCTION,
                       context_window=32768, pricing_per_1m_input=0.59, pricing_per_1m_output=0.79),
            ModelConfig(id="llama-3.1-8b-instant", provider="groq", family=ModelFamily.FAST_INFERENCE,
                       context_window=32768, pricing_per_1m_input=0.05, pricing_per_1m_output=0.08),
            ModelConfig(id="mixtral-8x7b-32768", provider="groq", family=ModelFamily.INSTRUCTION,
                       context_window=32768, pricing_per_1m_input=0.24, pricing_per_1m_output=0.24),
            ModelConfig(id="gemma2-9b-it", provider="groq", family=ModelFamily.INSTRUCTION,
                       context_window=8192, pricing_per_1m_input=0.20, pricing_per_1m_output=0.20),
        ],
        streaming_supported=True,
        tool_calling_supported=True,
        rate_limit_rpm=30,
        sdk_type="openai",
        pricing_per_1m_input=0.59,
        pricing_per_1m_output=0.79,
    ),

    # ------------------------------------------------------------------
    # Mistral AI
    # ------------------------------------------------------------------
    "mistral": ProviderConfig(
        name="Mistral AI",
        kind=ProviderKind.FOUNDATION,
        base_url="https://api.mistral.ai/v1",
        auth=AUTH_API_KEY.model_copy(update={"env_var": "MISTRAL_API_KEY"}),
        default_model="mistral-large-2411",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.FUNCTION_CALLING, Capability.JSON_MODE,
            Capability.SYSTEM_MESSAGE, Capability.MULTI_TURN,
            Capability.TEMPERATURE, Capability.TOP_P, Capability.MAX_TOKENS,
            Capability.STOP_SEQUENCE, Capability.VISION,
        },
        models=[
            ModelConfig(id="mistral-large-2411", provider="mistral", family=ModelFamily.INSTRUCTION,
                       context_window=128000,
                       pricing_per_1m_input=2.00, pricing_per_1m_output=6.00),
            ModelConfig(id="mistral-small-2501", provider="mistral", family=ModelFamily.FAST_INFERENCE,
                       context_window=32768, pricing_per_1m_input=1.00, pricing_per_1m_output=3.00),
            ModelConfig(id="codestral-2501", provider="mistral", family=ModelFamily.CODING,
                       context_window=256000,
                       pricing_per_1m_input=1.00, pricing_per_1m_output=3.00),
            ModelConfig(id="mistral-embed", provider="mistral", family=ModelFamily.EMBEDDING,
                       capabilities={Capability.EMBEDDING},
                       pricing_per_1m_input=0.10, pricing_per_1m_output=0.00),
        ],
        streaming_supported=True,
        tool_calling_supported=True,
        vision_supported=True,
        sdk_type="openai",
        pricing_per_1m_input=2.00,
        pricing_per_1m_output=6.00,
    ),

    # ------------------------------------------------------------------
    # Cohere
    # ------------------------------------------------------------------
    "cohere": ProviderConfig(
        name="Cohere",
        kind=ProviderKind.FOUNDATION,
        base_url="https://api.cohere.ai/v1",
        auth=AUTH_BEARER.model_copy(update={"env_var": "COHERE_API_KEY"}),
        default_model="command-r-plus-08-2024",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.FUNCTION_CALLING, Capability.RERANKING, Capability.EMBEDDING,
            Capability.SYSTEM_MESSAGE, Capability.MULTI_TURN,
            Capability.TEMPERATURE, Capability.TOP_P, Capability.MAX_TOKENS,
        },
        models=[
            ModelConfig(id="command-r-plus-08-2024", provider="cohere", family=ModelFamily.INSTRUCTION,
                       context_window=128000,
                       pricing_per_1m_input=3.00, pricing_per_1m_output=15.00),
            ModelConfig(id="command-r-08-2024", provider="cohere", family=ModelFamily.INSTRUCTION,
                       context_window=128000,
                       pricing_per_1m_input=0.50, pricing_per_1m_output=1.50),
            ModelConfig(id="embed-english-v3.0", provider="cohere", family=ModelFamily.EMBEDDING,
                       capabilities={Capability.EMBEDDING}),
            ModelConfig(id="rerank-english-v3.0", provider="cohere", family=ModelFamily.RERANKER,
                       capabilities={Capability.RERANKING}),
        ],
        streaming_supported=True,
        tool_calling_supported=True,
        sdk_type="openai",
        pricing_per_1m_input=3.00,
        pricing_per_1m_output=15.00,
    ),

    # ------------------------------------------------------------------
    # xAI (Grok)
    # ------------------------------------------------------------------
    "xai": ProviderConfig(
        name="xAI",
        kind=ProviderKind.FOUNDATION,
        base_url="https://api.x.ai/v1",
        auth=AUTH_API_KEY.model_copy(update={"env_var": "XAI_API_KEY"}),
        default_model="grok-2-1212",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.FUNCTION_CALLING, Capability.VISION,
            Capability.SYSTEM_MESSAGE, Capability.MULTI_TURN,
            Capability.TEMPERATURE, Capability.TOP_P, Capability.MAX_TOKENS,
        },
        models=[
            ModelConfig(id="grok-2-1212", provider="xai", family=ModelFamily.INSTRUCTION,
                       context_window=131072,
                       pricing_per_1m_input=2.00, pricing_per_1m_output=10.00),
            ModelConfig(id="grok-2-vision-1212", provider="xai", family=ModelFamily.MULTIMODAL,
                       context_window=32768,
                       capabilities={Capability.VISION},
                       pricing_per_1m_input=2.00, pricing_per_1m_output=10.00),
        ],
        streaming_supported=True,
        tool_calling_supported=True,
        vision_supported=True,
        sdk_type="openai",
        pricing_per_1m_input=2.00,
        pricing_per_1m_output=10.00,
    ),

    # ------------------------------------------------------------------
    # DeepSeek
    # ------------------------------------------------------------------
    "deepseek": ProviderConfig(
        name="DeepSeek",
        kind=ProviderKind.FOUNDATION,
        base_url="https://api.deepseek.com/v1",
        auth=AUTH_API_KEY.model_copy(update={"env_var": "DEEPSEEK_API_KEY"}),
        default_model="deepseek-chat",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.FUNCTION_CALLING, Capability.JSON_MODE,
            Capability.SYSTEM_MESSAGE, Capability.MULTI_TURN,
            Capability.TEMPERATURE, Capability.TOP_P, Capability.MAX_TOKENS,
        },
        models=[
            ModelConfig(id="deepseek-chat", provider="deepseek", family=ModelFamily.INSTRUCTION,
                       context_window=65536,
                       pricing_per_1m_input=0.14, pricing_per_1m_output=0.28),
            ModelConfig(id="deepseek-reasoner", provider="deepseek", family=ModelFamily.REASONING,
                       context_window=65536,
                       capabilities={Capability.REASONING},
                       pricing_per_1m_input=0.55, pricing_per_1m_output=2.19),
        ],
        streaming_supported=True,
        tool_calling_supported=True,
        reasoning_supported=True,
        sdk_type="openai",
        pricing_per_1m_input=0.14,
        pricing_per_1m_output=0.28,
    ),

    # ------------------------------------------------------------------
    # Together AI
    # ------------------------------------------------------------------
    "together": ProviderConfig(
        name="Together AI",
        kind=ProviderKind.FOUNDATION,
        base_url="https://api.together.xyz/v1",
        auth=AUTH_API_KEY.model_copy(update={"env_var": "TOGETHER_API_KEY"}),
        default_model="meta-llama/Llama-4-Maverick-17B-128E-Instruct",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.JSON_MODE, Capability.SYSTEM_MESSAGE, Capability.MULTI_TURN,
            Capability.TEMPERATURE, Capability.TOP_P, Capability.MAX_TOKENS,
            Capability.IMAGE_GENERATION, Capability.VISION,
        },
        models=[
            ModelConfig(id="meta-llama/Llama-4-Maverick-17B-128E-Instruct", provider="together",
                       family=ModelFamily.INSTRUCTION, context_window=131072,
                       pricing_per_1m_input=0.20, pricing_per_1m_output=0.20),
            ModelConfig(id="meta-llama/Llama-3.3-70B-Instruct-Turbo", provider="together",
                       family=ModelFamily.INSTRUCTION, context_window=131072,
                       pricing_per_1m_input=0.88, pricing_per_1m_output=0.88),
            ModelConfig(id="mistralai/Mixtral-8x22B-Instruct-v0.1", provider="together",
                       family=ModelFamily.INSTRUCTION, context_window=65536,
                       pricing_per_1m_input=0.90, pricing_per_1m_output=0.90),
        ],
        streaming_supported=True,
        tool_calling_supported=True,
        vision_supported=True,
        sdk_type="openai",
        pricing_per_1m_input=0.20,
        pricing_per_1m_output=0.20,
    ),

    # ------------------------------------------------------------------
    # Fireworks AI
    # ------------------------------------------------------------------
    "fireworks": ProviderConfig(
        name="Fireworks AI",
        kind=ProviderKind.FOUNDATION,
        base_url="https://api.fireworks.ai/inference/v1",
        auth=AUTH_API_KEY.model_copy(update={"env_var": "FIREWORKS_API_KEY"}),
        default_model="accounts/fireworks/models/llama-v3p3-70b-instruct",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.FUNCTION_CALLING, Capability.JSON_MODE,
            Capability.SYSTEM_MESSAGE, Capability.MULTI_TURN,
            Capability.TEMPERATURE, Capability.TOP_P, Capability.MAX_TOKENS,
        },
        models=[
            ModelConfig(id="accounts/fireworks/models/llama-v3p3-70b-instruct", provider="fireworks",
                       family=ModelFamily.INSTRUCTION, context_window=32768,
                       pricing_per_1m_input=0.20, pricing_per_1m_output=0.20),
            ModelConfig(id="accounts/fireworks/models/deepseek-r1", provider="fireworks",
                       family=ModelFamily.REASONING, context_window=65536,
                       pricing_per_1m_input=2.00, pricing_per_1m_output=8.00),
        ],
        streaming_supported=True,
        tool_calling_supported=True,
        sdk_type="openai",
        pricing_per_1m_input=0.20,
        pricing_per_1m_output=0.20,
    ),

    # ------------------------------------------------------------------
    # Perplexity
    # ------------------------------------------------------------------
    "perplexity": ProviderConfig(
        name="Perplexity",
        kind=ProviderKind.FOUNDATION,
        base_url="https://api.perplexity.ai",
        auth=AUTH_API_KEY.model_copy(update={"env_var": "PERPLEXITY_API_KEY"}),
        default_model="sonar-pro",
        capabilities={
            Capability.CHAT, Capability.STREAMING,
            Capability.SYSTEM_MESSAGE, Capability.MULTI_TURN,
            Capability.TEMPERATURE, Capability.TOP_P, Capability.MAX_TOKENS,
        },
        models=[
            ModelConfig(id="sonar-pro", provider="perplexity", family=ModelFamily.INSTRUCTION,
                       context_window=127000, pricing_per_1m_input=1.00, pricing_per_1m_output=1.00),
            ModelConfig(id="sonar", provider="perplexity", family=ModelFamily.FAST_INFERENCE,
                       context_window=127000, pricing_per_1m_input=0.20, pricing_per_1m_output=0.20),
        ],
        streaming_supported=True,
        sdk_type="openai",
        pricing_per_1m_input=1.00,
        pricing_per_1m_output=1.00,
    ),

    # ------------------------------------------------------------------
    # OpenRouter
    # ------------------------------------------------------------------
    "openrouter": ProviderConfig(
        name="OpenRouter",
        kind=ProviderKind.GATEWAY,
        base_url="https://openrouter.ai/api/v1",
        auth=AUTH_API_KEY.model_copy(update={"env_var": "OPENROUTER_API_KEY"}),
        default_model="anthropic/claude-sonnet-4-20250514",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.FUNCTION_CALLING, Capability.VISION,
            Capability.SYSTEM_MESSAGE, Capability.MULTI_TURN,
            Capability.TEMPERATURE, Capability.TOP_P, Capability.MAX_TOKENS,
            Capability.JSON_MODE,
        },
        models=[
            ModelConfig(id="anthropic/claude-sonnet-4-20250514", provider="openrouter",
                       family=ModelFamily.INSTRUCTION, context_window=200000),
            ModelConfig(id="openai/gpt-4o", provider="openrouter",
                       family=ModelFamily.INSTRUCTION, context_window=128000),
            ModelConfig(id="google/gemini-2.5-pro-exp-03-25", provider="openrouter",
                       family=ModelFamily.REASONING, context_window=1000000),
        ],
        streaming_supported=True,
        tool_calling_supported=True,
        vision_supported=True,
        rate_limit_rpm=60,
        sdk_type="openai",
    ),

    # ------------------------------------------------------------------
    # HuggingFace Inference
    # ------------------------------------------------------------------
    "huggingface": ProviderConfig(
        name="HuggingFace Inference",
        kind=ProviderKind.FOUNDATION,
        base_url="https://api-inference.huggingface.co/models",
        auth=AUTH_HUGGINGFACE_TOKEN.model_copy(update={"env_var": "HUGGINGFACE_API_KEY"}),
        default_model="microsoft/Phi-3-mini-4k-instruct",
        capabilities={
            Capability.CHAT, Capability.STREAMING,
            Capability.MULTI_TURN, Capability.TEMPERATURE, Capability.TOP_P,
            Capability.MAX_TOKENS, Capability.VISION,
        },
        models=[
            ModelConfig(id="meta-llama/Llama-3.3-70B-Instruct", provider="huggingface",
                       family=ModelFamily.INSTRUCTION, context_window=128000),
            ModelConfig(id="microsoft/Phi-3-mini-4k-instruct", provider="huggingface",
                       family=ModelFamily.INSTRUCTION, context_window=4096),
            ModelConfig(id="mistralai/Mistral-7B-Instruct-v0.3", provider="huggingface",
                       family=ModelFamily.INSTRUCTION, context_window=32768),
        ],
        streaming_supported=True,
        vision_supported=True,
        sdk_type="openai",
    ),

    # ------------------------------------------------------------------
    # Ollama (Local)
    # ------------------------------------------------------------------
    "ollama": ProviderConfig(
        name="Ollama",
        kind=ProviderKind.LOCAL,
        base_url="http://localhost:11434/api",
        auth=AUTH_LOCAL_TRUST,
        default_model="llama3.2",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.VISION, Capability.MULTI_TURN,
            Capability.TEMPERATURE, Capability.TOP_P, Capability.MAX_TOKENS,
            Capability.EMBEDDING,
        },
        models=[
            ModelConfig(id="llama3.2", provider="ollama", family=ModelFamily.LOCAL_GGUF,
                       context_window=32768, is_local=True),
            ModelConfig(id="llama3.1", provider="ollama", family=ModelFamily.LOCAL_GGUF,
                       context_window=32768, is_local=True),
            ModelConfig(id="mistral", provider="ollama", family=ModelFamily.LOCAL_GGUF,
                       context_window=32768, is_local=True),
            ModelConfig(id="codellama", provider="ollama", family=ModelFamily.CODING,
                       context_window=16384, is_local=True, is_coding_specialized=True),
            ModelConfig(id="llava", provider="ollama", family=ModelFamily.MULTIMODAL,
                       context_window=4096, is_local=True, is_multimodal=True),
            ModelConfig(id="nomic-embed-text", provider="ollama", family=ModelFamily.EMBEDDING,
                       capabilities={Capability.EMBEDDING}, is_local=True),
        ],
        streaming_supported=True,
        tool_calling_supported=True,
        vision_supported=True,
        local=True,
        sdk_type="openai",
    ),

    # ------------------------------------------------------------------
    # LM Studio (Local)
    # ------------------------------------------------------------------
    "lm_studio": ProviderConfig(
        name="LM Studio",
        kind=ProviderKind.LOCAL,
        base_url="http://localhost:1234/v1",
        auth=AUTH_LOCAL_TRUST,
        default_model="local-model",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.MULTI_TURN, Capability.TEMPERATURE, Capability.TOP_P,
            Capability.MAX_TOKENS, Capability.EMBEDDING,
        },
        models=[
            ModelConfig(id="local-model", provider="lm_studio", family=ModelFamily.LOCAL_GGUF,
                       context_window=32768, is_local=True),
        ],
        streaming_supported=True,
        tool_calling_supported=True,
        local=True,
        sdk_type="openai",
    ),

    # ------------------------------------------------------------------
    # vLLM (Local)
    # ------------------------------------------------------------------
    "vllm": ProviderConfig(
        name="vLLM",
        kind=ProviderKind.LOCAL,
        base_url="http://localhost:8000/v1",
        auth=AUTH_LOCAL_TRUST,
        default_model="local-model",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.FUNCTION_CALLING, Capability.JSON_MODE,
            Capability.MULTI_TURN, Capability.TEMPERATURE, Capability.TOP_P,
            Capability.MAX_TOKENS, Capability.SEED,
        },
        models=[
            ModelConfig(id="local-model", provider="vllm", family=ModelFamily.LOCAL_GGUF,
                       context_window=32768, is_local=True),
        ],
        streaming_supported=True,
        tool_calling_supported=True,
        local=True,
        sdk_type="openai",
    ),

    # ------------------------------------------------------------------
    # llama.cpp (Local)
    # ------------------------------------------------------------------
    "llama_cpp": ProviderConfig(
        name="llama.cpp",
        kind=ProviderKind.LOCAL,
        base_url="http://localhost:8080/v1",
        auth=AUTH_LOCAL_TRUST,
        default_model="local-model",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.MULTI_TURN, Capability.TEMPERATURE, Capability.TOP_P,
            Capability.MAX_TOKENS,
        },
        models=[
            ModelConfig(id="local-model", provider="llama_cpp", family=ModelFamily.LOCAL_GGUF,
                       context_window=4096, is_local=True),
        ],
        streaming_supported=True,
        tool_calling_supported=True,
        local=True,
        sdk_type="openai",
    ),

    # ------------------------------------------------------------------
    # Azure OpenAI
    # ------------------------------------------------------------------
    "azure": ProviderConfig(
        name="Azure OpenAI",
        kind=ProviderKind.FOUNDATION,
        auth=AUTH_AZURE_IDENTITY.model_copy(update={
            "env_var": "AZURE_OPENAI_API_KEY",
            "env_var_fallback": "AZURE_OPENAI_API_VERSION",
        }),
        default_model="gpt-4o",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.PARALLEL_TOOL_CALLING, Capability.STRUCTURED_OUTPUT,
            Capability.VISION, Capability.FUNCTION_CALLING, Capability.JSON_MODE,
            Capability.SYSTEM_MESSAGE, Capability.MULTI_TURN,
            Capability.TEMPERATURE, Capability.TOP_P, Capability.MAX_TOKENS,
        },
        models=[
            ModelConfig(id="gpt-4o", provider="azure", family=ModelFamily.MULTIMODAL, context_window=128000,
                       capabilities={Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING, Capability.VISION,
                                    Capability.STRUCTURED_OUTPUT, Capability.FUNCTION_CALLING}),
            ModelConfig(id="gpt-4o-mini", provider="azure", family=ModelFamily.INSTRUCTION, context_window=128000),
            ModelConfig(id="gpt-4", provider="azure", family=ModelFamily.INSTRUCTION, context_window=8192),
        ],
        streaming_supported=True,
        tool_calling_supported=True,
        vision_supported=True,
        structured_output_supported=True,
        sdk_type="openai",
    ),

    # ------------------------------------------------------------------
    # AWS Bedrock
    # ------------------------------------------------------------------
    "bedrock": ProviderConfig(
        name="AWS Bedrock",
        kind=ProviderKind.FOUNDATION,
        auth=AUTH_AWS_SIGNATURE,
        default_model="anthropic.claude-sonnet-4-20250514",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.VISION, Capability.MULTI_TURN,
            Capability.TEMPERATURE, Capability.TOP_P, Capability.MAX_TOKENS,
        },
        models=[
            ModelConfig(id="anthropic.claude-sonnet-4-20250514", provider="bedrock",
                       family=ModelFamily.MULTIMODAL, context_window=200000),
            ModelConfig(id="anthropic.claude-3-5-sonnet-20241022-v2", provider="bedrock",
                       family=ModelFamily.MULTIMODAL, context_window=200000),
            ModelConfig(id="meta.llama3-70b-instruct-v1", provider="bedrock",
                       family=ModelFamily.INSTRUCTION, context_window=8192),
            ModelConfig(id="amazon.titan-text-premier-v1", provider="bedrock",
                       family=ModelFamily.INSTRUCTION, context_window=4096),
        ],
        streaming_supported=True,
        tool_calling_supported=True,
        vision_supported=True,
        sdk_type="openai",
    ),

    # ------------------------------------------------------------------
    # Vertex AI
    # ------------------------------------------------------------------
    "vertex": ProviderConfig(
        name="Vertex AI",
        kind=ProviderKind.FOUNDATION,
        api_version="v1",
        auth=AUTH_GCP_SERVICE_ACCOUNT,
        default_model="gemini-2.5-pro-exp-03-25",
        capabilities={
            Capability.CHAT, Capability.STREAMING, Capability.TOOL_CALLING,
            Capability.VISION, Capability.LONG_CONTEXT, Capability.MULTI_TURN,
            Capability.TEMPERATURE, Capability.TOP_P, Capability.MAX_TOKENS,
            Capability.AUDIO,
        },
        models=[
            ModelConfig(id="gemini-2.5-pro-exp-03-25", provider="vertex",
                       family=ModelFamily.REASONING, context_window=1000000,
                       capabilities={Capability.LONG_CONTEXT, Capability.TOOL_CALLING, Capability.VISION, Capability.AUDIO}),
            ModelConfig(id="gemini-2.0-flash", provider="vertex",
                       family=ModelFamily.FAST_INFERENCE, context_window=1000000),
            ModelConfig(id="claude-3-5-sonnet-v2@20241022", provider="vertex",
                       family=ModelFamily.MULTIMODAL, context_window=200000),
        ],
        streaming_supported=True,
        tool_calling_supported=True,
        vision_supported=True,
        sdk_type="google",
    ),
}


# ============================================================================
# FLATTENED MODEL REGISTRY (for fast lookup)
# ============================================================================

MODEL_REGISTRY: Dict[str, ModelConfig] = {}
for provider_key, provider_config in PROVIDER_REGISTRY.items():
    for model in provider_config.models:
        model.provider = provider_key  # ensure provider is set
        MODEL_REGISTRY[model.id] = model


# ============================================================================
# LOOKUP HELPERS
# ============================================================================

def get_provider(provider_key: str) -> Optional[ProviderConfig]:
    """Get provider config by key."""
    return PROVIDER_REGISTRY.get(provider_key.lower())


def get_model(model_id: str) -> Optional[ModelConfig]:
    """Get model config by model ID."""
    return MODEL_REGISTRY.get(model_id)


def get_models_by_provider(provider_key: str) -> List[ModelConfig]:
    """Get all models for a provider."""
    provider = get_provider(provider_key)
    if not provider:
        return []
    return provider.models


def get_models_by_family(family: ModelFamily) -> List[ModelConfig]:
    """Get all models of a given family."""
    return [m for m in MODEL_REGISTRY.values() if m.family == family]


def get_providers_by_capability(capability: Capability) -> List[str]:
    """Get all provider keys that support a given capability."""
    return [
        key for key, config in PROVIDER_REGISTRY.items()
        if capability in config.capabilities
    ]


def get_models_by_capability(capability: Capability) -> List[ModelConfig]:
    """Get all models with a given capability."""
    return [m for m in MODEL_REGISTRY.values() if capability in m.capabilities]


def list_all_providers() -> List[str]:
    """List all registered provider keys."""
    return sorted(PROVIDER_REGISTRY.keys())


def list_all_models() -> List[str]:
    """List all registered model IDs."""
    return sorted(MODEL_REGISTRY.keys())


def get_local_providers() -> Dict[str, ProviderConfig]:
    """Get all local providers."""
    return {k: v for k, v in PROVIDER_REGISTRY.items() if v.local}


def get_cloud_providers() -> Dict[str, ProviderConfig]:
    """Get all cloud-based providers."""
    return {k: v for k, v in PROVIDER_REGISTRY.items() if not v.local}


# ============================================================================
# PROVIDER CATEGORIES
# ============================================================================

FOUNDATION_PROVIDERS = [
    "openai", "anthropic", "google", "groq", "mistral", "cohere",
    "xai", "deepseek", "together", "fireworks", "perplexity",
]

GATEWAY_PROVIDERS = ["openrouter"]

LOCAL_PROVIDERS = ["ollama", "lm_studio", "vllm", "llama_cpp"]

CLOUD_PLATFORM_PROVIDERS = ["azure", "bedrock", "vertex"]

HUGGINGFACE_PROVIDERS = ["huggingface"]

ALL_PROVIDERS = list(PROVIDER_REGISTRY.keys())
