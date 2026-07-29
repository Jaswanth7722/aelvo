"""Conversion adapters for provider message/streaming/tool-call normalization."""

from .messages import MessageAdapter
from .streaming import StreamingAdapter
from .tool_calls import ToolCallAdapter
from .structured_output import StructuredOutputAdapter
from .errors import ErrorAdapter, AuthenticationError, RateLimitError

__all__ = [
    "MessageAdapter",
    "StreamingAdapter",
    "ToolCallAdapter",
    "StructuredOutputAdapter",
    "ErrorAdapter",
    "AuthenticationError",
    "RateLimitError",
]
