# types.py - Canonical Types for AELVO Provider Runtime Ecosystem
"""
Canonical data models for the entire provider runtime ecosystem.

Every provider, every adapter, every runtime component uses these types.
No provider-specific formats leak above the adapter layer.
"""

from __future__ import annotations

import abc
import time
import enum
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    TYPE_CHECKING,
    Union,
)
from pydantic import BaseModel, Field, field_validator, model_validator


# ============================================================================
# ENUMS
# ============================================================================

class ProviderKind(str, enum.Enum):
    """Classification of provider types."""
    FOUNDATION = "foundation"
    CODING_RUNTIME = "coding_runtime"
    EMBEDDING = "embedding"
    IMAGE = "image"
    MULTIMODAL = "multimodal"
    RERANKER = "reranker"
    LOCAL = "local"
    GATEWAY = "gateway"
    CLOUD = "cloud"


class ModelFamily(str, enum.Enum):
    """Canonical model family classification."""
    # Reasoning
    REASONING = "reasoning"
    # General instruction-following
    INSTRUCTION = "instruction"
    # Tool-calling specialized
    TOOL_CALLING = "tool_calling"
    # Coding specialized
    CODING = "coding"
    # Multimodal (vision + text)
    MULTIMODAL = "multimodal"
    # Embedding
    EMBEDDING = "embedding"
    # Reranker
    RERANKER = "reranker"
    # Image generation
    IMAGE_GENERATION = "image_generation"
    # Audio
    AUDIO = "audio"
    # Local GGUF models
    LOCAL_GGUF = "local_gguf"
    # Speculative decoding
    SPECULATIVE = "speculative"
    # Long-context specialized
    LONG_CONTEXT = "long_context"
    # Fast inference specialized
    FAST_INFERENCE = "fast_inference"
    # Streaming-first
    STREAMING = "streaming"
    # Provider-specific families
    # Anthropic
    CLAUDE_3 = "claude_3"
    CLAUDE_3_5 = "claude_3_5"
    CLAUDE_4 = "claude_4"
    # Google
    GEMINI_2 = "gemini_2"
    GEMINI_1_5 = "gemini_1_5"
    GEMINI_1 = "gemini_1"
    # OpenAI
    GPT4 = "gpt4"
    GPT4O = "gpt4o"
    GPT4O_MINI = "gpt4o_mini"
    GPT4_TURBO = "gpt4_turbo"
    O1 = "o1"
    O3 = "o3"
    # Meta
    LLAMA = "llama"
    # Alibaba
    QWEN = "qwen"
    # Mistral
    MISTRAL = "mistral"
    # Google
    GEMMA = "gemma"
    # Mistral
    MIXTRAL = "mixtral"
    # DeepSeek
    DEEPSEEK = "deepseek"
    # Fireworks AI
    FIREWORKS = "fireworks"
    # Cohere
    COMMAND_R = "command_r"
    # xAI
    GROK = "grok"
    # Perplexity
    PERPLEXITY = "perplexity"
    # Local / catch-all
    LOCAL = "local"
    ANY = "any"


class AuthMethod(str, enum.Enum):
    """Supported authentication methods."""
    API_KEY = "api_key"
    OAUTH_DEVICE = "oauth_device"
    OAUTH_CLIENT = "oauth_client"
    BROWSER_SESSION = "browser_session"
    PROVIDER_TOKEN = "provider_token"
    LOCAL_TRUST = "local_trust"
    AZURE_IDENTITY = "azure_identity"
    AWS_SIGNATURE = "aws_signature"
    GCP_SERVICE_ACCOUNT = "gcp_service_account"
    HUGGINGFACE_TOKEN = "huggingface_token"
    BEARER_TOKEN = "bearer_token"
    BASIC_AUTH = "basic_auth"
    CUSTOM_HEADER = "custom_header"


class Capability(str, enum.Enum):
    """Provider/model capabilities."""
    CHAT = "chat"
    COMPLETION = "completion"
    STREAMING = "streaming"
    TOOL_CALLING = "tool_calling"
    PARALLEL_TOOL_CALLING = "parallel_tool_calling"
    STRUCTURED_OUTPUT = "structured_output"
    VISION = "vision"
    AUDIO = "audio"
    IMAGE_GENERATION = "image_generation"
    EMBEDDING = "embedding"
    RERANKING = "reranking"
    FUNCTION_CALLING = "function_calling"
    JSON_MODE = "json_mode"
    REASONING = "reasoning"
    LONG_CONTEXT = "long_context"
    SYSTEM_MESSAGE = "system_message"
    MULTI_TURN = "multi_turn"
    LOGPROBS = "logprobs"
    SEED = "seed"
    STOP_SEQUENCE = "stop_sequence"
    FREQUENCY_PENALTY = "frequency_penalty"
    PRESENCE_PENALTY = "presence_penalty"
    TOP_P = "top_p"
    TEMPERATURE = "temperature"
    MAX_TOKENS = "max_tokens"
    RESPONSE_FORMAT = "response_format"
    # Extended capabilities used by provider implementations
    TEXT_GENERATION = "text_generation"
    CHAT_COMPLETION = "chat_completion"
    SYSTEM_PROMPTS = "system_prompts"
    TOP_K = "top_k"
    MULTIMODAL = "multimodal"
    EMBEDDINGS = "embeddings"
    STOP_SEQUENCES = "stop_sequences"
    SEED_CONTROL = "seed_control"
    PARALLEL_TOOL_CALLS = "parallel_tool_calls"
    MULTIPLE_FUNCTIONS = "multiple_functions"
    LOCAL = "local"
    AUDIO_TRANSCRIPTION = "audio_transcription"
    OBJECT_DETECTION = "object_detection"
    TEXT_CLASSIFICATION = "text_classification"
    TOKEN_CLASSIFICATION = "token_classification"
    QUESTION_ANSWERING = "question_answering"
    SUMMARIZATION = "summarization"
    TRANSLATION = "translation"


class FinishReason(str, enum.Enum):
    """Canonical finish reasons (normalized across all providers)."""
    STOP = "stop"
    LENGTH = "length"
    TOOL_CALLS = "tool_calls"
    CONTENT_FILTER = "content_filter"
    ERROR = "error"
    CANCELLED = "cancelled"
    MAX_TOKENS = "max_tokens"
    RECITATION = "recitation"
    UNKNOWN = "unknown"
    NULL = "null"


class StreamEventType(str, enum.Enum):
    """Canonical streaming event types."""
    CHUNK = "chunk"
    TOOL_CALL_BEGIN = "tool_call_begin"
    TOOL_CALL_CHUNK = "tool_call_chunk"
    TOOL_CALL_END = "tool_call_end"
    REASONING_CHUNK = "reasoning_chunk"
    USAGE = "usage"
    ERROR = "error"
    DONE = "done"
    METADATA = "metadata"


class MessageRole(str, enum.Enum):
    """Canonical message roles."""
    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"
    TOOL = "tool"
    FUNCTION = "function"


class ContentType(str, enum.Enum):
    """Content types for multimodal messages."""
    TEXT = "text"
    IMAGE_URL = "image_url"
    IMAGE_BASE64 = "image_base64"
    AUDIO_URL = "audio_url"
    AUDIO_BASE64 = "audio_base64"
    VIDEO_URL = "video_url"
    FILE_URL = "file_url"
    TOOL_RESULT = "tool_result"
    THINKING = "thinking"


class ProviderStatus(str, enum.Enum):
    """Runtime status of a provider."""
    HEALTHY = "healthy"
    ACTIVE = "active"
    DEGRADED = "degraded"
    RATE_LIMITED = "rate_limited"
    ERROR = "error"
    UNAUTHORIZED = "unauthorized"
    DOWN = "down"
    UNKNOWN = "unknown"


class RoutingStrategy(str, enum.Enum):
    """Provider routing strategies."""
    PRIMARY = "primary"
    FALLBACK = "fallback"
    LATENCY_OPTIMAL = "latency_optimal"
    COST_OPTIMAL = "cost_optimal"
    CAPABILITY_REQUIRED = "capability_required"
    LOCAL_PREFERRED = "local_preferred"
    ROUND_ROBIN = "round_robin"
    WEIGHTED = "weighted"
    MANUAL = "manual"


class CredentialType(str, enum.Enum):
    """Types of stored credentials."""
    API_KEY = "api_key"
    OAUTH_TOKEN = "oauth_token"
    REFRESH_TOKEN = "refresh_token"
    SESSION_TOKEN = "session_token"
    SERVICE_ACCOUNT = "service_account"
    AWS_ACCESS_KEY = "aws_access_key"
    AZURE_TENANT = "azure_tenant"
    GCP_CREDENTIALS = "gcp_credentials"
    CUSTOM_HEADER = "custom_header"
    BASIC_AUTH = "basic_auth"
    BEARER_TOKEN = "bearer_token"


# ============================================================================
# AUTH MODELS
# ============================================================================

class Credential(BaseModel):
    """A stored credential with metadata."""
    id: str = Field(default_factory=lambda: f"cred_{int(time.time())}_{id(object())}")
    provider: str
    credential_type: CredentialType
    value: str = Field(..., repr=False)  # Never log this!
    label: str = ""
    expires_at: Optional[float] = None
    created_at: float = Field(default_factory=time.time)
    last_used_at: Optional[float] = None
    usage_count: int = 0
    is_valid: bool = True
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("value")
    @classmethod
    def validate_non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Credential value must not be empty")
        return v


class AuthConfig(BaseModel):
    """Authentication configuration for a provider."""
    method: AuthMethod
    credential_type: CredentialType
    env_var: Optional[str] = None
    env_var_fallback: Optional[str] = None
    default_value: Optional[str] = None
    header_template: Optional[str] = None  # e.g. "Bearer {token}"
    oauth_scopes: List[str] = Field(default_factory=list)
    oauth_device_endpoint: Optional[str] = None
    oauth_token_endpoint: Optional[str] = None
    supports_refresh: bool = False
    credential_validation: Optional[str] = None  # regex pattern for validation

    @field_validator("header_template")
    @classmethod
    def validate_template(cls, v: Optional[str]) -> Optional[str]:
        if v and "{token}" not in v and "{key}" not in v:
            raise ValueError("header_template must contain {token} or {key}")
        return v


# ============================================================================
# PROVIDER CONFIGURATION
# ============================================================================

class ProviderConfig(BaseModel):
    """Full configuration for a provider."""
    # Identity (used by provider implementations)
    provider_id: str = ""
    name: str = ""
    kind: ProviderKind = ProviderKind.FOUNDATION
    # Auth (used by provider implementations)
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    api_version: Optional[str] = None
    auth: AuthConfig = Field(default_factory=lambda: AuthConfig(method=AuthMethod.API_KEY, credential_type=CredentialType.API_KEY))
    # Runtime settings (used by provider implementations)
    timeout_seconds: float = 60.0
    max_retries: int = 3
    organization: Optional[str] = None
    extra_headers: Dict[str, str] = Field(default_factory=dict)
    # Model config
    default_model: str = ""
    models: List[ModelConfig] = Field(default_factory=list)
    supported_models: List[str] = Field(default_factory=list)
    capabilities: Set[Capability] = Field(default_factory=set)
    max_context_length: int = 128000
    max_output_tokens: int = 4096
    pricing_per_1m_input: Optional[float] = None
    pricing_per_1m_output: Optional[float] = None
    rate_limit_rpm: Optional[int] = None
    rate_limit_tpm: Optional[int] = None
    rate_limit_rpd: Optional[int] = None
    streaming_supported: bool = True
    tool_calling_supported: bool = True
    vision_supported: bool = False
    structured_output_supported: bool = False
    reasoning_supported: bool = False
    context_window_multiplier: float = 1.0
    local: bool = False
    requires_remote_ollama: bool = False
    sdk_type: Optional[str] = None  # "openai", "anthropic", "google"
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ModelConfig(BaseModel):
    """Configuration for a specific model version."""
    id: str
    provider: str
    family: ModelFamily
    context_window: int = 128000
    max_output_tokens: int = 4096
    capabilities: Set[Capability] = Field(default_factory=set)
    pricing_per_1m_input: Optional[float] = None
    pricing_per_1m_output: Optional[float] = None
    is_reasoning_model: bool = False
    is_multimodal: bool = False
    is_coding_specialized: bool = False
    is_local: bool = False
    release_date: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ============================================================================
# CANONICAL MESSAGE MODELS
# ============================================================================

class MessageContent(BaseModel):
    """A piece of content within a message (supports multimodal)."""
    type: ContentType = ContentType.TEXT
    text: Optional[str] = None
    image_url: Optional[str] = None
    image_data: Optional[str] = None  # base64
    audio_url: Optional[str] = None
    audio_data: Optional[str] = None  # base64
    file_url: Optional[str] = None
    tool_call_id: Optional[str] = None
    tool_name: Optional[str] = None
    thinking: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ChatMessage(BaseModel):
    """A canonical chat message normalized across all providers."""
    role: MessageRole
    content: Union[str, List[MessageContent]] = ""
    name: Optional[str] = None
    tool_calls: List[ToolCall] = Field(default_factory=list)
    tool_call_id: Optional[str] = None
    tool_name: Optional[str] = None
    thinking: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ToolDefinition(BaseModel):
    """A tool/function definition in canonical form."""
    name: str
    description: str = ""
    parameters: Dict[str, Any] = Field(default_factory=dict)  # JSON Schema
    strict: bool = False
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ToolCall(BaseModel):
    """A tool call in canonical form."""
    id: str
    type: str = "function"
    function: Dict[str, Any] = Field(default_factory=dict)
    index: Optional[int] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ============================================================================
# CANONICAL REQUEST/RESPONSE
# ============================================================================

class CanonicalRequest(BaseModel):
    """Canonical request — every provider adapter converts TO this."""
    model: str
    provider: Optional[str] = None
    messages: List[ChatMessage] = Field(default_factory=list)
    tools: List[ToolDefinition] = Field(default_factory=list)
    tool_choice: Optional[Union[str, Dict[str, Any]]] = None
    max_tokens: Optional[int] = None
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    stop: Optional[Union[str, List[str]]] = None
    frequency_penalty: Optional[float] = None
    presence_penalty: Optional[float] = None
    seed: Optional[int] = None
    stream: bool = False
    response_format: Optional[Dict[str, Any]] = None  # JSON Schema for structured output
    reasoning_effort: Optional[str] = None  # "low", "medium", "high"
    metadata: Dict[str, Any] = Field(default_factory=dict)
    request_id: Optional[str] = None
    timestamp: float = Field(default_factory=time.time)


class CanonicalResponse(BaseModel):
    """Canonical response — every provider adapter converts FROM their format."""
    id: str
    model: str
    provider: str
    messages: List[ChatMessage] = Field(default_factory=list)
    content: str = ""
    finish_reason: FinishReason = FinishReason.STOP
    tool_calls: List[ToolCall] = Field(default_factory=list)
    usage: Optional[Usage] = None
    thinking: Optional[str] = None
    latency_ms: Optional[float] = None
    cached: bool = False
    metadata: Dict[str, Any] = Field(default_factory=dict)
    timestamp: float = Field(default_factory=time.time)


class Usage(BaseModel):
    """Canonical usage tracking."""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    reasoning_tokens: Optional[int] = None
    cached_prompt_tokens: Optional[int] = None
    cost_usd: Optional[float] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode='after')
    def _compute_total_tokens(self) -> 'Usage':
        if not self.total_tokens and (self.prompt_tokens or self.completion_tokens):
            self.total_tokens = self.prompt_tokens + self.completion_tokens
        return self


# ============================================================================
# STREAMING
# ============================================================================

class StreamEvent(BaseModel):
    """Canonical streaming event."""
    type: StreamEventType
    content: Optional[str] = None
    tool_call: Optional[ToolCall] = None
    reasoning: Optional[str] = None
    usage: Optional[Usage] = None
    finish_reason: Optional[FinishReason] = None
    error: Optional[str] = None
    index: int = 0
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ============================================================================
# ROUTING & HEALTH
# ============================================================================

class HealthStatus(BaseModel):
    """Current health status of a provider."""
    provider: str
    status: ProviderStatus = ProviderStatus.UNKNOWN
    latency_p50_ms: Optional[float] = None
    latency_p95_ms: Optional[float] = None
    error_rate_1h: Optional[float] = None
    rate_limit_remaining: Optional[int] = None
    rate_limit_reset_at: Optional[float] = None
    quota_remaining: Optional[int] = None
    auth_valid: bool = True
    last_check_at: float = Field(default_factory=time.time)
    consecutive_failures: int = 0
    total_requests: int = 0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class RoutingDecision(BaseModel):
    """Record of a routing decision."""
    strategy: RoutingStrategy
    selected_provider: str
    selected_model: str
    candidates: List[Dict[str, Any]] = Field(default_factory=list)
    reason: str = ""
    fallback_chain: List[str] = Field(default_factory=list)
    latency_estimate_ms: Optional[float] = None
    cost_estimate_usd: Optional[float] = None
    timestamp: float = Field(default_factory=time.time)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ProviderError(BaseModel):
    """Canonical provider error."""
    provider: str
    model: str
    error_type: str = "unknown"  # "auth", "rate_limit", "timeout", "server_error", "invalid_request", etc.
    message: str = ""
    status_code: Optional[int] = None
    retryable: bool = False
    suggestion: Optional[str] = None
    timestamp: float = Field(default_factory=time.time)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ProviderInfo(BaseModel):
    """Full info about a provider including runtime state."""
    # Identity fields (used by provider implementations)
    provider_id: str = ""
    name: str = ""
    provider_type: ProviderKind = ProviderKind.FOUNDATION
    description: str = ""
    docs_url: str = ""
    website: str = ""
    # Runtime state
    config: ProviderConfig = Field(default_factory=ProviderConfig)
    health: Optional[HealthStatus] = None
    models: List[ModelConfig] = Field(default_factory=list)
    credential_status: bool = False
    routing_decision: Optional[RoutingDecision] = None
    last_error: Optional[ProviderError] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ============================================================================
# EVENT MODELS (for replayable provider events)
# ============================================================================

# ============================================================================
# BACKWARD-COMPATIBLE ALIASES
# ============================================================================

# Alias Capability -> CapabilityFlag for runtime modules
CapabilityFlag = Capability

# Alias Usage -> TokenUsage for runtime modules
TokenUsage = Usage

# Alias for ProviderCapabilities used by runtime modules
class ProviderCapabilities(BaseModel):
    """Capabilities associated with a provider (runtime alias)."""
    provider_id: str = ""
    capabilities: set[Capability] = Field(default_factory=set)
    model_families: set[ModelFamily] = Field(default_factory=set)
    max_context_length: Optional[int] = None
    max_output_tokens: Optional[int] = None
    supports_streaming: bool = False
    supports_tool_calling: bool = False
    supports_multimodal: bool = False
    supports_structured_output: bool = False
    is_local: bool = False


class ProviderHealth(BaseModel):
    """Health status of a provider (runtime alias)."""
    provider_id: str
    status: ProviderStatus = ProviderStatus.UNKNOWN
    last_check: float = 0.0
    last_latency_ms: float = 0.0
    last_error: Optional[str] = None
    consecutive_failures: int = 0
    is_degraded: bool = False


class ModelInfo(BaseModel):
    """Model information (runtime alias)."""
    model_id: str
    family: ModelFamily = ModelFamily.INSTRUCTION
    provider_id: str = ""
    context_length: int = 128000
    capabilities: set[Capability] = Field(default_factory=set)
    pricing_per_1m_input: Optional[float] = None
    pricing_per_1m_output: Optional[float] = None
    is_reasoning_model: bool = False
    is_multimodal: bool = False
    is_coding_specialized: bool = False
    is_local: bool = False


class ModelCapability(BaseModel):
    """Capability information for a model (runtime alias)."""
    model_id: str = ""
    capability: Capability = Capability.CHAT
    supported: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)

class AuthCredentials(BaseModel):
    """Credentials returned from an auth flow."""
    provider_id: str
    api_key: Optional[str] = Field(default=None, repr=False)
    access_token: Optional[str] = Field(default=None, repr=False)
    refresh_token: Optional[str] = Field(default=None, repr=False)
    id_token: Optional[str] = Field(default=None, repr=False)
    session_token: Optional[str] = Field(default=None, repr=False)
    expires_at: Optional[float] = None
    scopes: str = ""
    token_type: str = "bearer"
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode='after')
    def _require_at_least_one_credential(self) -> 'AuthCredentials':
        if not any([self.api_key, self.access_token, self.refresh_token,
                     self.id_token, self.session_token]):
            raise ValueError(
                "At least one credential (api_key, access_token, refresh_token, "
                "id_token, or session_token) must be provided"
            )
        return self


class ProviderAuthStatus(BaseModel):
    """Authentication status for a provider."""
    provider_id: str
    authenticated: bool = False
    reason: str = ""
    expires_at: Optional[float] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


ProviderType = ProviderKind


class ProviderEvent(BaseModel):
    """A replayable provider event."""
    id: str = Field(default_factory=lambda: f"evt_{int(time.time())}_{id(object())}")
    type: str  # "request", "response", "error", "auth_refresh", "fallback", etc.
    provider: str
    model: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
    timestamp: float = Field(default_factory=time.time)
    session_id: Optional[str] = None
    request_id: Optional[str] = None
    duration_ms: Optional[float] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ============================================================================
# ADAPTER INTERFACES
# ============================================================================

class ProviderAdapter(abc.ABC):
    """Abstract interface for provider-specific adapters."""

    @abc.abstractmethod
    def to_canonical_request(self, request: CanonicalRequest, config: ProviderConfig) -> Any:
        ...

    @abc.abstractmethod
    def from_canonical_response(self, response: Any, provider: str, model: str) -> CanonicalResponse:
        ...

    @abc.abstractmethod
    def stream_to_canonical(self, chunks: AsyncIterator[Any], provider: str, model: str) -> AsyncIterator[StreamEvent]:
        ...

    @abc.abstractmethod
    def from_canonical_error(self, error: Exception, provider: str, model: str) -> ProviderError:
        ...
