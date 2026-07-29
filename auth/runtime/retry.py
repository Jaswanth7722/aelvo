"""Retry/Backoff Engine — configurable retry with exponential backoff."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Awaitable, Callable, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RetryableErrorType(Enum):
    """Types of errors that may be retried."""
    TRANSIENT = auto()
    RATE_LIMIT = auto()
    TIMEOUT = auto()
    SERVICE_UNAVAILABLE = auto()
    SERVER_ERROR = auto()
    GATEWAY_ERROR = auto()
    AUTH_EXPIRED = auto()


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    jitter_factor: float = 0.1
    exponential_base: float = 2.0
    retryable_errors: list[RetryableErrorType] = field(
        default_factory=lambda: [
            RetryableErrorType.TRANSIENT,
            RetryableErrorType.RATE_LIMIT,
            RetryableErrorType.TIMEOUT,
            RetryableErrorType.SERVICE_UNAVAILABLE,
            RetryableErrorType.SERVER_ERROR,
            RetryableErrorType.GATEWAY_ERROR,
        ]
    )
    retryable_status_codes: list[int] = field(
        default_factory=lambda: [429, 500, 502, 503, 504]
    )


@dataclass
class RetryAttempt:
    """Record of a single retry attempt."""

    attempt: int
    delay: float
    error: str
    timestamp: float = field(default_factory=time.time)


@dataclass
class RetryResult:
    """Result of a retry operation."""

    success: bool
    attempts: int
    total_delay: float
    attempts_history: list[RetryAttempt] = field(default_factory=list)
    result: Any = None
    last_error: Optional[str] = None


class RetryEngine:
    """Configurable retry engine with exponential backoff and jitter.

    Supports:
    - Exponential backoff with configurable base
    - Random jitter to avoid thundering herd
    - Status-code-based retry decisions
    - Error-type-based retry decisions
    - Attempt tracking and reporting
    """

    def __init__(
        self, config: Optional[RetryConfig] = None
    ) -> None:
        self.config = config or RetryConfig()

    def should_retry(
        self,
        error: Exception,
        attempt: int,
        status_code: Optional[int] = None,
    ) -> bool:
        """Determine if an error should trigger a retry."""
        if attempt >= self.config.max_attempts:
            return False

        error_str = str(error).lower()

        # Check by status code
        if status_code and status_code in self.config.retryable_status_codes:
            return True

        # Check by error type patterns
        if status_code == 429:
            return RetryableErrorType.RATE_LIMIT in self.config.retryable_errors
        if status_code == 503:
            return RetryableErrorType.SERVICE_UNAVAILABLE in self.config.retryable_errors

        # Check by error message patterns
        retryable_patterns = [
            "timeout",
            "timed out",
            "temporarily",
            "try again",
            "rate limit",
            "too many",
            "unavailable",
            "server error",
            "bad gateway",
            "service unavailable",
            "connection",
            "reset",
            "refused",
            "broken",
            "throttl",
            "overloaded",
            "retry",
        ]

        for pattern in retryable_patterns:
            if pattern in error_str:
                return True

        return False

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for a given attempt number."""
        delay = self.config.base_delay * (
            self.config.exponential_base ** (attempt - 1)
        )
        delay = min(delay, self.config.max_delay)

        # Add jitter
        jitter = random.uniform(
            -self.config.jitter_factor * delay,
            self.config.jitter_factor * delay,
        )
        return max(0.0, delay + jitter)

    async def execute(
        self,
        operation: Callable[..., Awaitable[T]],
        *args: Any,
        status_code_provider: Optional[Callable[[], Optional[int]]] = None,
        **kwargs: Any,
    ) -> RetryResult:
        """Execute an operation with retry logic.

        Args:
            operation: Async callable to execute.
            *args: Positional args for the operation.
            status_code_provider: Optional callable that returns the
                HTTP status code of the last error.
            **kwargs: Keyword args for the operation.

        Returns:
            RetryResult with success/failure and attempt history.
        """
        last_error: Optional[str] = None
        attempts_history: list[RetryAttempt] = []
        total_delay = 0.0

        for attempt in range(1, self.config.max_attempts + 1):
            try:
                result = await operation(*args, **kwargs)
                return RetryResult(
                    success=True,
                    attempts=attempt,
                    total_delay=total_delay,
                    attempts_history=attempts_history,
                    result=result,
                )
            except Exception as e:
                last_error = str(e)
                status_code = (
                    status_code_provider() if status_code_provider else None
                )

                if not self.should_retry(e, attempt, status_code):
                    logger.debug(
                        "Non-retryable error on attempt %d: %s",
                        attempt,
                        e,
                    )
                    return RetryResult(
                        success=False,
                        attempts=attempt,
                        total_delay=total_delay,
                        attempts_history=attempts_history,
                        last_error=last_error,
                    )

                delay = self.calculate_delay(attempt)
                total_delay += delay

                attempts_history.append(
                    RetryAttempt(
                        attempt=attempt,
                        delay=delay,
                        error=str(e),
                    )
                )

                logger.info(
                    "Retry attempt %d/%d for %s in %.1fs (error: %s)",
                    attempt,
                    self.config.max_attempts,
                    getattr(operation, "__name__", "operation"),
                    delay,
                    e,
                )

                await asyncio.sleep(delay)

        return RetryResult(
            success=False,
            attempts=self.config.max_attempts,
            total_delay=total_delay,
            attempts_history=attempts_history,
            last_error=last_error,
        )

    async def execute_with_fallback(
        self,
        primary: Callable[..., Awaitable[T]],
        fallback: Callable[..., Awaitable[T]],
        *args: Any,
        **kwargs: Any,
    ) -> RetryResult:
        """Execute with retry on primary, then fallback on failure."""
        result = await self.execute(primary, *args, **kwargs)

        if result.success:
            return result

        logger.info(
            "Primary failed after %d attempts, trying fallback",
            result.attempts,
        )

        try:
            fallback_result = await fallback(*args, **kwargs)
            return RetryResult(
                success=True,
                attempts=result.attempts + 1,
                total_delay=result.total_delay,
                attempts_history=result.attempts_history,
                result=fallback_result,
            )
        except Exception as e:
            return RetryResult(
                success=False,
                attempts=result.attempts + 1,
                total_delay=result.total_delay,
                attempts_history=result.attempts_history,
                last_error=f"Primary: {result.last_error}, Fallback: {e}",
            )

    def summary(self, result: RetryResult) -> dict[str, Any]:
        return {
            "success": result.success,
            "attempts": result.attempts,
            "total_delay": round(result.total_delay, 2),
            "last_error": result.last_error,
            "history": [
                {
                    "attempt": a.attempt,
                    "delay": round(a.delay, 2),
                    "error": a.error[:100],
                }
                for a in result.attempts_history
            ],
        }
