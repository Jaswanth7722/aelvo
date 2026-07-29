"""core/execution/tool_registry.py — Advanced Tool Execution Registry

Phase 14: Structured tool registry with metadata, lifecycle management,
result caching, retry policies, and dependency resolution between tools.

Key components:
  - ToolSpec: Metadata and configuration for a registered tool
  - ToolResult: Structured execution result with caching support
  - ToolExecutionRegistry: Manages tool registration, execution, caching, retry
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

log = logging.getLogger("aelvo.core.execution.tool_registry")


class RetryPolicy(str, Enum):
    """Retry policy for tool execution."""
    NO_RETRY = "no_retry"
    RETRY_ON_FAILURE = "retry_on_failure"
    RETRY_ON_TIMEOUT = "retry_on_timeout"
    RETRY_ON_CONDITION = "retry_on_condition"
    ALWAYS_RETRY = "always_retry"


class ToolCategory(str, Enum):
    """Category of tool for organizational purposes."""
    FILE_OPERATION = "file_operation"
    CODE_ANALYSIS = "code_analysis"
    COMMAND_EXECUTION = "command_execution"
    MEMORY_OPERATION = "memory_operation"
    RESEARCH = "research"
    SECURITY = "security"
    UTILITY = "utility"


class ExecutionStrategy(str, Enum):
    """How the tool should be executed."""
    DIRECT = "direct"              # Execute immediately
    QUEUED = "queued"              # Queue for sequential execution
    PARALLEL = "parallel"          # Can run in parallel
    CONDITIONAL = "conditional"    # Only if condition is met


# ============================================================================
# Data Classes
# ============================================================================


@dataclass
class ToolSpec:
    """Metadata and configuration for a registered tool.

    Attributes:
        name: Unique tool name (e.g., 'read_file', 'bash_exec').
        category: Tool category.
        description: Human-readable description.
        timeout: Default timeout in seconds.
        retry_policy: Retry policy on failure.
        max_retries: Maximum number of retries.
        retry_delay: Delay between retries in seconds.
        cache_ttl: Time-to-live for cached results (0 = no caching).
        strategy: Execution strategy.
        required_capabilities: Set of capabilities this tool requires.
        tags: Tags for categorization and querying.
    """

    name: str
    category: ToolCategory = ToolCategory.UTILITY
    description: str = ""
    timeout: float = 30.0
    retry_policy: RetryPolicy = RetryPolicy.NO_RETRY
    max_retries: int = 2
    retry_delay: float = 1.0
    cache_ttl: float = 0.0
    strategy: ExecutionStrategy = ExecutionStrategy.DIRECT
    required_capabilities: Set[str] = field(default_factory=set)
    tags: List[str] = field(default_factory=list)


@dataclass
class ToolResult:
    """Structured execution result with caching support.

    Attributes:
        tool_name: The tool that was executed.
        status: 'success' or 'error'.
        output: Primary output text.
        error: Error message if status is 'error'.
        duration_ms: Execution duration in milliseconds.
        cached: Whether this result was served from cache.
        retry_attempt: Which retry attempt this was (0 = first try).
        metadata: Additional result metadata.
        timestamp: When the result was produced.
    """

    tool_name: str
    status: str = "success"
    output: str = ""
    error: str = ""
    duration_ms: float = 0.0
    cached: bool = False
    retry_attempt: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    @property
    def is_success(self) -> bool:
        return self.status == "success"

    @property
    def is_error(self) -> bool:
        return self.status == "error"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tool_name": self.tool_name,
            "status": self.status,
            "output": self.output[:500],
            "error": self.error[:200],
            "duration_ms": round(self.duration_ms, 2),
            "cached": self.cached,
            "retry_attempt": self.retry_attempt,
            "timestamp": self.timestamp,
        }


@dataclass
class CacheEntry:
    """A cached tool result with TTL tracking."""

    result: ToolResult
    created_at: float = field(default_factory=time.time)
    access_count: int = 0

    def is_expired(self, ttl: float) -> bool:
        return (time.time() - self.created_at) > ttl


# ============================================================================
# ToolExecutionRegistry
# ============================================================================


class ToolExecutionRegistry:
    """Structured tool registry with lifecycle management, caching, and retry.

    Manages tool registration, execution dispatch, result caching, and
    automatic retry with configurable policies. Tools can be queried
    by category, tags, or capabilities.
    """

    def __init__(self):
        self._tools: Dict[str, ToolSpec] = {}
        self._handlers: Dict[str, Callable[..., Any]] = {}
        self._results: Dict[str, ToolResult] = {}
        self._cache: Dict[str, CacheEntry] = {}
        self._execution_history: List[ToolResult] = []
        self._max_history: int = 1000

    # ── Registration ────────────────────────────────────────────────

    def register(
        self,
        spec: ToolSpec,
        handler: Callable[..., Any],
    ) -> ToolSpec:
        """Register a tool with its specification and handler.

        Args:
            spec: ToolSpec with metadata and configuration.
            handler: Async or sync callable that executes the tool.

        Returns:
            The registered ToolSpec.
        """
        name = spec.name
        self._tools[name] = spec
        self._handlers[name] = handler
        log.info("Registered tool '%s' (category=%s, timeout=%ds)", name, spec.category.value, spec.timeout)
        return spec

    def unregister(self, tool_name: str) -> bool:
        """Remove a registered tool."""
        self._tools.pop(tool_name, None)
        self._handlers.pop(tool_name, None)
        self._cache.pop(tool_name, None)
        return True

    def get_spec(self, tool_name: str) -> Optional[ToolSpec]:
        """Get the ToolSpec for a registered tool."""
        return self._tools.get(tool_name)

    def get_handler(self, tool_name: str) -> Optional[Callable[..., Any]]:
        """Get the handler for a registered tool."""
        return self._handlers.get(tool_name)

    def is_registered(self, tool_name: str) -> bool:
        """Check if a tool is registered."""
        return tool_name in self._tools

    def list_tools(
        self,
        category: Optional[ToolCategory] = None,
        tags: Optional[List[str]] = None,
    ) -> List[ToolSpec]:
        """List registered tools, optionally filtered.

        Args:
            category: Filter by category.
            tags: Filter by tags (tool must have ALL specified tags).

        Returns:
            List of matching ToolSpecs.
        """
        tools = list(self._tools.values())
        if category:
            tools = [t for t in tools if t.category == category]
        if tags:
            tools = [
                t for t in tools
                if all(tag in t.tags for tag in tags)
            ]
        return sorted(tools, key=lambda t: t.name)

    def count(self) -> int:
        """Get the number of registered tools."""
        return len(self._tools)

    # ── Execution ───────────────────────────────────────────────────

    async def execute(
        self,
        tool_name: str,
        args: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
        bypass_cache: bool = False,
    ) -> ToolResult:
        """Execute a tool with automatic retry and caching.

        Args:
            tool_name: Name of the tool to execute.
            args: Arguments to pass to the tool handler.
            timeout: Override the default timeout.
            bypass_cache: Skip cached results.

        Returns:
            ToolResult with execution details.
        """
        spec = self._tools.get(tool_name)
        if spec is None:
            return ToolResult(
                tool_name=tool_name,
                status="error",
                error=f"Unknown tool: {tool_name}",
            )

        handler = self._handlers.get(tool_name)
        if handler is None:
            return ToolResult(
                tool_name=tool_name,
                status="error",
                error=f"No handler registered for tool: {tool_name}",
            )

        actual_args = args or {}

        # Check cache
        if not bypass_cache and spec.cache_ttl > 0:
            cached = self._check_cache(tool_name, actual_args)
            if cached is not None:
                return cached

        # Execute with retries
        effective_timeout = timeout or spec.timeout
        last_error = ""

        for attempt in range(spec.max_retries + 1):
            result = await self._execute_once(
                tool_name, handler, actual_args,
                effective_timeout, attempt,
            )

            if result.is_success:
                # Cache the result
                if spec.cache_ttl > 0:
                    self._store_cache(tool_name, actual_args, result)

                self._record_history(result)
                return result

            last_error = result.error

            # Check retry policy
            if attempt < spec.max_retries and self._should_retry(spec, result):
                delay = spec.retry_delay * (attempt + 1)  # Linear backoff
                log.info(
                    "Retrying tool '%s' (attempt %d/%d) after %.1fs: %s",
                    tool_name, attempt + 1, spec.max_retries, delay, last_error,
                )
                await asyncio.sleep(delay)
            else:
                break

        # All retries exhausted
        result.retry_attempt = spec.max_retries
        result.error = f"Execution failed after {spec.max_retries + 1} attempt(s): {last_error}"
        self._record_history(result)
        return result

    async def execute_batch(
        self,
        tool_calls: List[Tuple[str, Dict[str, Any]]],
        timeout: Optional[float] = None,
    ) -> List[ToolResult]:
        """Execute multiple tools, respecting their execution strategy.

        DIRECT/CONDITIONAL tools run sequentially in order.
        PARALLEL tools are gathered concurrently.
        QUEUED tools run sequentially in their own queue.

        Args:
            tool_calls: List of (tool_name, args) tuples.
            timeout: Override default timeout for all tools.

        Returns:
            List of ToolResults in the same order as input.
        """
        results: List[ToolResult] = []

        # Separate by strategy
        direct_tools: List[Tuple[str, Dict[str, Any]]] = []
        parallel_tools: List[Tuple[str, Dict[str, Any], int]] = []
        queued_tools: List[Tuple[str, Dict[str, Any]]] = []

        for i, (name, args) in enumerate(tool_calls):
            spec = self._tools.get(name)
            strategy = spec.strategy if spec else ExecutionStrategy.DIRECT
            if strategy == ExecutionStrategy.PARALLEL:
                parallel_tools.append((name, args, i))
            elif strategy == ExecutionStrategy.QUEUED:
                queued_tools.append((name, args))
            else:
                direct_tools.append((name, args))

        # Execute parallel tools concurrently
        if parallel_tools:
            parallel_results = await asyncio.gather(*[
                self.execute(name, args, timeout=timeout)
                for name, args, _ in parallel_tools
            ])

        # Execute direct tools sequentially
        for name, args in direct_tools:
            result = await self.execute(name, args, timeout=timeout)
            results.append(result)

        # Execute queued tools sequentially
        for name, args in queued_tools:
            result = await self.execute(name, args, timeout=timeout)
            results.append(result)

        # Merge parallel results back in original order
        if parallel_tools:
            parallel_by_index = {
                idx: result
                for (name, args, idx), result in zip(parallel_tools, parallel_results)
            }
            for i in range(len(tool_calls)):
                if i in parallel_by_index:
                    results.insert(i, parallel_by_index[i])

        return results

    async def _execute_once(
        self,
        tool_name: str,
        handler: Callable[..., Any],
        args: Dict[str, Any],
        timeout: float,
        attempt: int,
    ) -> ToolResult:
        """Execute a tool once with timeout, handling both sync and async."""
        start = time.perf_counter()
        try:
            result = await asyncio.wait_for(
                self._call_handler(handler, args),
                timeout=timeout,
            )
            elapsed = (time.perf_counter() - start) * 1000

            # Handle different return types
            if isinstance(result, ToolResult):
                result.duration_ms = elapsed
                result.retry_attempt = attempt
                return result

            if isinstance(result, dict):
                status = result.get("status", "success")
                if status == "error":
                    return ToolResult(
                        tool_name=tool_name,
                        status="error",
                        error=result.get("logs", result.get("error", "Unknown error")),
                        output=result.get("logs", ""),
                        duration_ms=elapsed,
                        retry_attempt=attempt,
                        metadata=result.get("executed", {}),
                    )
                return ToolResult(
                    tool_name=tool_name,
                    status="success",
                    output=result.get("logs", result.get("output", str(result))),
                    duration_ms=elapsed,
                    retry_attempt=attempt,
                    metadata=result.get("executed", {}),
                )

            # Handle string or other types
            str_result = str(result)
            return ToolResult(
                tool_name=tool_name,
                status="success" if str_result else "error",
                output=str_result,
                duration_ms=elapsed,
                retry_attempt=attempt,
            )

        except asyncio.TimeoutError:
            elapsed = (time.perf_counter() - start) * 1000
            return ToolResult(
                tool_name=tool_name,
                status="error",
                error=f"Tool timed out after {timeout}s",
                duration_ms=elapsed,
                retry_attempt=attempt,
                metadata={"timeout": timeout},
            )
        except Exception as e:
            elapsed = (time.perf_counter() - start) * 1000
            return ToolResult(
                tool_name=tool_name,
                status="error",
                error=str(e),
                duration_ms=elapsed,
                retry_attempt=attempt,
            )

    async def _call_handler(
        self, handler: Callable[..., Any], args: Dict[str, Any]
    ) -> Any:
        """Call the handler, supporting both sync and async functions."""
        if asyncio.iscoroutinefunction(handler):
            return await handler(**args)
        return handler(**args)

    def _should_retry(self, spec: ToolSpec, result: ToolResult) -> bool:
        """Check if a retry should be attempted based on policy."""
        if spec.retry_policy == RetryPolicy.NO_RETRY:
            return False
        if spec.retry_policy == RetryPolicy.ALWAYS_RETRY:
            return True
        if spec.retry_policy == RetryPolicy.RETRY_ON_FAILURE:
            return result.status == "error"
        if spec.retry_policy == RetryPolicy.RETRY_ON_TIMEOUT:
            return "timed out" in result.error
        if spec.retry_policy == RetryPolicy.RETRY_ON_CONDITION:
            # Check condition based on error content (default: retry on transient errors)
            transient_signals = [
                "timeout", "connection", "rate limit", "too many",
                "resource temporarily", "try again", "temporary",
            ]
            return any(s in result.error.lower() for s in transient_signals)
        return False

    # ── Caching ─────────────────────────────────────────────────────

    def _cache_key(self, tool_name: str, args: Dict[str, Any]) -> str:
        """Generate a deterministic cache key from tool name and args."""
        sorted_args = sorted(args.items())
        raw = f"{tool_name}:{sorted_args}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _check_cache(self, tool_name: str, args: Dict[str, Any]) -> Optional[ToolResult]:
        """Check the cache for a valid entry."""
        key = self._cache_key(tool_name, args)
        entry = self._cache.get(key)
        if entry is None:
            return None

        spec = self._tools.get(tool_name)
        spec_ttl = spec.cache_ttl if spec else 0

        if entry.is_expired(spec_ttl):
            self._cache.pop(key, None)
            return None

        entry.access_count += 1
        cached_result = entry.result
        cached_result.cached = True
        return cached_result

    def _store_cache(self, tool_name: str, args: Dict[str, Any], result: ToolResult) -> None:
        """Store a result in the cache."""
        key = self._cache_key(tool_name, args)
        self._cache[key] = CacheEntry(result=result)

    def invalidate_cache(self, tool_name: Optional[str] = None) -> int:
        """Invalidate cache entries.

        Args:
            tool_name: If provided, only invalidate for this tool.
                      If None, invalidate entire cache.

        Returns:
            Number of invalidated entries.
        """
        if tool_name is None:
            count = len(self._cache)
            self._cache.clear()
            return count

        keys_to_remove = [
            key for key, entry in self._cache.items()
            if entry.result.tool_name == tool_name
        ]
        for key in keys_to_remove:
            self._cache.pop(key, None)
        return len(keys_to_remove)

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            "total_entries": len(self._cache),
            "by_tool": self._cache_by_tool(),
        }

    def _cache_by_tool(self) -> Dict[str, int]:
        """Count cached entries by tool name."""
        counts: Dict[str, int] = {}
        for entry in self._cache.values():
            name = entry.result.tool_name
            counts[name] = counts.get(name, 0) + 1
        return counts

    # ── History ─────────────────────────────────────────────────────

    def _record_history(self, result: ToolResult) -> None:
        """Record a tool execution result in history."""
        self._execution_history.append(result)
        if len(self._execution_history) > self._max_history:
            self._execution_history = self._execution_history[-self._max_history:]

    def get_history(
        self,
        tool_name: Optional[str] = None,
        limit: int = 50,
    ) -> List[ToolResult]:
        """Get execution history, optionally filtered by tool."""
        if tool_name:
            return [
                r for r in self._execution_history[-limit:]
                if r.tool_name == tool_name
            ]
        return self._execution_history[-limit:]

    def get_statistics(self, tool_name: Optional[str] = None) -> Dict[str, Any]:
        """Get execution statistics.

        Args:
            tool_name: If provided, only stats for this tool.

        Returns:
            Dict with success_rate, total, avg_duration, etc.
        """
        history = self._execution_history
        if tool_name:
            history = [r for r in history if r.tool_name == tool_name]

        if not history:
            return {"total": 0, "success_rate": 0, "avg_duration_ms": 0}

        success_count = sum(1 for r in history if r.is_success)
        avg_duration = sum(r.duration_ms for r in history) / len(history)

        return {
            "total": len(history),
            "success_count": success_count,
            "success_rate": round(success_count / len(history), 4),
            "avg_duration_ms": round(avg_duration, 2),
            "cached_count": sum(1 for r in history if r.cached),
        }

    # ── Snapshot ────────────────────────────────────────────────────

    def snapshot(self) -> Dict[str, Any]:
        """Get a snapshot of the registry state."""
        by_category: Dict[str, int] = {}
        for spec in self._tools.values():
            cat = spec.category.value
            by_category[cat] = by_category.get(cat, 0) + 1

        return {
            "registered_tools": self.count(),
            "by_category": by_category,
            "cache_entries": len(self._cache),
            "execution_history": len(self._execution_history),
            "statistics": self.get_statistics(),
        }

    def to_terminal_display(self) -> str:
        """Human-readable terminal display."""
        snap = self.snapshot()
        lines = [
            "  ── TOOL EXECUTION REGISTRY ──",
            f"  Tools: {snap['registered_tools']} registered",
            f"  Cache: {snap['cache_entries']} entries",
            f"  Executions: {snap['execution_history']} total",
        ]

        if snap['statistics']['total'] > 0:
            stats = snap['statistics']
            lines.append(f"  Success rate: {stats['success_rate']:.1%}  "
                         f"Avg duration: {stats['avg_duration_ms']:.0f}ms")

        if snap['by_category']:
            lines.append("")
            lines.append("  By Category:")
            for cat, count in sorted(snap['by_category'].items()):
                lines.append(f"    {cat}: {count}")

        lines.append("  ── ── ── ── ── ── ── ── ── ── ──")
        return "\n".join(lines)
