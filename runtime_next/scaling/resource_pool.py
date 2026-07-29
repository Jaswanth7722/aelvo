# runtime_next/scaling/resource_pool.py
# Phase 12: Async resource pooling for expensive connections and external services
#
# Provides:
# - Generic AsyncResourcePool with acquire/release lifecycle
# - Connection pooling for DB clients, ChromaDB, API clients
# - Concurrency limits (max connections per pool)
# - Idle timeout and automatic cleanup
# - Pool health monitoring and stats
# - Context manager support (async with)

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Set
from enum import Enum

log = logging.getLogger("aelvo.runtime.scaling.resource_pool")


class PoolState(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    DEGRADED = "degraded"
    EXHAUSTED = "exhausted"


class PooledResource:
    """A wrapper around a resource with metadata tracking."""

    def __init__(self, resource: Any, pool_id: str):
        self.resource = resource
        self.pool_id = pool_id
        self.created_at = time.time()
        self.last_used_at = time.time()
        self.acquired_count = 0
        self.failed_count = 0
        self.is_valid = True

    @property
    def age_seconds(self) -> float:
        return time.time() - self.created_at

    @property
    def idle_seconds(self) -> float:
        return time.time() - self.last_used_at

    def mark_used(self) -> None:
        self.last_used_at = time.time()
        self.acquired_count += 1

    def mark_failed(self) -> None:
        self.failed_count += 1
        self.is_valid = False


class ResourcePool:
    """Async resource pool with acquire/release lifecycle.

    Manages a pool of reusable resources (DB connections, API clients, etc.)
    with configurable concurrency limits, idle timeout, and health tracking.

    Usage:
        pool = ResourcePool(creator=lambda: create_client(), max_size=5)
        async with pool.acquire() as client:
            await client.query(...)

    Or manually:
        client = await pool.acquire()
        try:
            await client.query(...)
        finally:
            await pool.release(client)
    """

    def __init__(
        self,
        creator: Callable[[], Any],
        max_size: int = 5,
        min_size: int = 0,
        idle_timeout_seconds: float = 300.0,
        max_acquire_retries: int = 3,
        name: str = "default",
        validator: Optional[Callable[[Any], bool]] = None,
        destroyer: Optional[Callable[[Any], None]] = None,
    ):
        self._creator = creator
        self._max_size = max_size
        self._min_size = min_size
        self._idle_timeout = idle_timeout_seconds
        self._max_acquire_retries = max_acquire_retries
        self._name = name
        self._validator = validator or (lambda r: True)
        self._destroyer = destroyer

        self._pool: List[PooledResource] = []
        self._acquired: Set[int] = set()
        self._lock = asyncio.Lock()
        self._cond = asyncio.Condition(lock=self._lock)
        self._state = PoolState.OPEN
        self._total_created = 0
        self._total_acquired = 0
        self._total_failures = 0
        self._total_timeouts = 0
        self._cleanup_task: Optional[asyncio.Task] = None

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the pool and pre-create minimum resources."""
        async with self._lock:
            self._state = PoolState.OPEN
        # Pre-create min_size resources
        for _ in range(self._min_size):
            await self._create_resource()
        # Start idle cleanup loop
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        log.info(
            "ResourcePool '%s' started: min=%d max=%d",
            self._name, self._min_size, self._max_size,
        )

    async def stop(self, force: bool = False) -> None:
        """Stop the pool and destroy all resources."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except (asyncio.CancelledError, RuntimeError):
                pass
            self._cleanup_task = None

        async with self._lock:
            self._state = PoolState.CLOSED
            # Destroy all pooled resources
            for pr in self._pool:
                self._destroy_resource(pr.resource)
            self._pool.clear()
            self._cond.notify_all()

        log.info(
            "ResourcePool '%s' stopped: %d resources destroyed",
            self._name, self._total_created,
        )

    # ── Acquire / Release ────────────────────────────────────────────────────

    async def acquire(self, timeout_seconds: Optional[float] = None) -> Any:
        """Acquire a resource from the pool.

        Args:
            timeout_seconds: Max time to wait for a resource (None = wait forever).

        Returns:
            A pooled resource.

        Raises:
            TimeoutError: If no resource becomes available within timeout.
            RuntimeError: If pool is closed or exhausted.
        """
        deadline = None
        if timeout_seconds is not None:
            deadline = time.time() + timeout_seconds

        for attempt in range(self._max_acquire_retries):
            async with self._cond:
                if self._state == PoolState.CLOSED:
                    raise RuntimeError(
                        f"Pool '{self._name}' is closed"
                    )

                # Clean stale resources
                await self._evict_stale()

                # Try to find an available resource
                for pr in self._pool:
                    if id(pr) not in self._acquired and self._validator(pr.resource):
                        self._acquired.add(id(pr))
                        pr.mark_used()
                        self._total_acquired += 1
                        return pr.resource

                # If pool isn't full, create a new resource
                if len(self._pool) < self._max_size:
                    # Release the condition lock while creating
                    self._cond.release()
                    try:
                        pr = await self._create_resource()
                    finally:
                        await self._cond.acquire()

                    # Double-check: another coroutine may have acquired our resource
                    # while the lock was released for creation.
                    if pr and id(pr) not in self._acquired and pr in self._pool:
                        # Our resource is still available — acquire it for our caller.
                        # If the pool exceeded max_size during creation, the cleanup
                        # loop will evict excess resources when they're released.
                        self._acquired.add(id(pr))
                        pr.mark_used()
                        self._total_acquired += 1
                        return pr.resource
                    elif pr and pr in self._pool:
                        # Another coroutine already acquired it — remove extra
                        self._pool.remove(pr)
                        self._destroy_resource(pr.resource)
                    # Fall through to wait

                # Pool is full and all resources are acquired — wait
                if deadline is not None:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        self._total_timeouts += 1
                        raise TimeoutError(
                            f"Pool '{self._name}' — no resource available after "
                            f"{timeout_seconds}s (acquired={len(self._acquired)}, "
                            f"pool={len(self._pool)}, max={self._max_size})"
                        )
                    try:
                        await asyncio.wait_for(
                            self._cond.wait(), timeout=remaining
                        )
                    except asyncio.TimeoutError:
                        self._total_timeouts += 1
                        raise TimeoutError(
                            f"Pool '{self._name}' — acquire timed out after "
                            f"{timeout_seconds}s"
                        )
                else:
                    await self._cond.wait()

        raise RuntimeError(
            f"Pool '{self._name}' — failed to acquire after "
            f"{self._max_acquire_retries} retries"
        )

    async def release(self, resource: Any, mark_failed: bool = False) -> None:
        """Return a resource to the pool.

        Args:
            resource: The resource to release.
            mark_failed: If True, mark the resource as failed (will be destroyed).
        """
        async with self._cond:
            for pr in self._pool:
                if pr.resource is resource:
                    if mark_failed:
                        pr.mark_failed()
                        self._total_failures += 1
                    # Remove from acquired set
                    self._acquired.discard(id(pr))
                    # Notify waiters
                    self._cond.notify(1)
                    return

            # Resource not found in pool — destroy if we have a destroyer
            if self._destroyer:
                try:
                    self._destroyer(resource)
                except Exception as e:
                    log.warning("Failed to destroy unknown resource: %s", e)

    # ── Context Manager ─────────────────────────────────────────────────────

    async def __aenter__(self) -> "ResourcePool":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    class _AcquireContext:
        """Async context manager for a single acquire/release cycle."""

        def __init__(self, pool: "ResourcePool", timeout: Optional[float]):
            self._pool = pool
            self._timeout = timeout
            self._resource = None

        async def __aenter__(self) -> Any:
            self._resource = await self._pool.acquire(self._timeout)
            return self._resource

        async def __aexit__(self, exc_type: Any, *args: Any) -> None:
            if self._resource is not None:
                await self._pool.release(
                    self._resource, mark_failed=(exc_type is not None)
                )

    def acquire_context(self, timeout: Optional[float] = None) -> _AcquireContext:
        """Get a context manager for acquire/release.

        Usage:
            async with pool.acquire_context() as client:
                await client.query(...)
        """
        return self._AcquireContext(self, timeout)

    # ── Stats / Health ──────────────────────────────────────────────────────

    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
        time.time()
        return {
            "name": self._name,
            "state": self._state.value,
            "pool_size": len(self._pool),
            "acquired_count": len(self._acquired),
            "available_count": len(self._pool) - len(self._acquired),
            "max_size": self._max_size,
            "min_size": self._min_size,
            "total_created": self._total_created,
            "total_acquired": self._total_acquired,
            "total_failures": self._total_failures,
            "total_timeouts": self._total_timeouts,
            "idle_timeout_seconds": self._idle_timeout,
        }

    def get_state(self) -> PoolState:
        return self._state

    @property
    def is_healthy(self) -> bool:
        """Check if pool is in a healthy state."""
        if self._state == PoolState.CLOSED:
            return False
        # Check for too many failures
        if self._total_created > 0:
            failure_rate = self._total_failures / self._total_created
            if failure_rate > 0.5:
                return False
        return True

    @property
    def available_count(self) -> int:
        return len(self._pool) - len(self._acquired)

    # ── Internal ─────────────────────────────────────────────────────────────

    async def _create_resource(self) -> Optional[PooledResource]:
        """Create a new resource and add it to the pool."""
        try:
            loop = asyncio.get_running_loop()
            # Run creator in executor if it's synchronous
            if asyncio.iscoroutinefunction(self._creator):
                resource = await self._creator()
            else:
                resource = await loop.run_in_executor(None, self._creator)

            pr = PooledResource(resource, f"{self._name}_{self._total_created}")
            self._pool.append(pr)
            self._total_created += 1
            log.debug("Pool '%s': created resource #%d", self._name, self._total_created)
            return pr
        except Exception as e:
            log.error("Pool '%s': failed to create resource: %s", self._name, e)
            return None

    def _destroy_resource(self, resource: Any) -> None:
        """Destroy a resource using the destroyer callback."""
        if self._destroyer:
            try:
                self._destroyer(resource)
            except Exception as e:
                log.warning("Pool '%s': destroyer failed: %s", self._name, e)

    async def _evict_stale(self) -> None:
        """Evict stale (idle timeout exceeded) and invalid resources."""
        now = time.time()
        stale = []
        for pr in self._pool:
            if id(pr) in self._acquired:
                continue
            if not pr.is_valid:
                stale.append(pr)
            elif self._idle_timeout > 0 and (now - pr.last_used_at) > self._idle_timeout:
                stale.append(pr)

        for pr in stale:
            self._pool.remove(pr)
            self._destroy_resource(pr.resource)
            log.debug(
                "Pool '%s': evicted stale resource (age=%.1fs, idle=%.1fs)",
                self._name, pr.age_seconds, pr.idle_seconds,
            )

    async def _cleanup_loop(self) -> None:
        """Periodically evict stale resources."""
        try:
            while True:
                await asyncio.sleep(60)  # Check every 60s
                async with self._lock:
                    await self._evict_stale()
                    # Update state based on availability
                    if self._state == PoolState.OPEN:
                        available = len(self._pool) - len(self._acquired)
                        if available == 0 and len(self._pool) >= self._max_size:
                            self._state = PoolState.EXHAUSTED
                        elif self._total_failures > self._total_created * 0.5:
                            self._state = PoolState.DEGRADED
                        else:
                            self._state = PoolState.OPEN
        except asyncio.CancelledError:
            pass


class ConnectionPool(ResourcePool):
    """Specialized resource pool for database connections.

    Adds connection-specific tracking: query count, error tracking per connection.
    """

    def __init__(
        self,
        creator: Callable[[], Any],
        max_size: int = 10,
        min_size: int = 1,
        idle_timeout_seconds: float = 600.0,
        name: str = "connection",
        validator: Optional[Callable[[Any], bool]] = None,
        destroyer: Optional[Callable[[Any], None]] = None,
    ):
        super().__init__(
            creator=creator,
            max_size=max_size,
            min_size=min_size,
            idle_timeout_seconds=idle_timeout_seconds,
            name=name,
            validator=validator,
            destroyer=destroyer,
        )
        self._conn_stats: Dict[int, Dict[str, Any]] = {}

    async def acquire(self, timeout_seconds: Optional[float] = None) -> Any:
        conn = await super().acquire(timeout_seconds)
        conn_id = id(conn)
        if conn_id not in self._conn_stats:
            self._conn_stats[conn_id] = {
                "queries": 0,
                "errors": 0,
                "acquired_at": time.time(),
            }
        return conn

    async def release(self, resource: Any, mark_failed: bool = False) -> None:
        conn_id = id(resource)
        if mark_failed and conn_id in self._conn_stats:
            self._conn_stats[conn_id]["errors"] += 1
        await super().release(resource, mark_failed)

    def record_query(self, resource: Any) -> None:
        conn_id = id(resource)
        if conn_id in self._conn_stats:
            self._conn_stats[conn_id]["queries"] += 1

    def get_connection_stats(self) -> Dict[str, Any]:
        """Get per-connection statistics."""
        total_queries = sum(s["queries"] for s in self._conn_stats.values())
        total_errors = sum(s["errors"] for s in self._conn_stats.values())
        active = sum(
            1 for pr in self._pool if id(pr.resource) in self._acquired
        )
        return {
            "total_connections": len(self._pool),
            "active_connections": active,
            "total_queries": total_queries,
            "total_errors": total_errors,
            "per_connection": dict(self._conn_stats),
        }


class ResourcePoolManager:
    """Manages multiple named resource pools.

    Provides centralized lifecycle management and health monitoring
    for all pools in the system.
    """

    def __init__(self):
        self._pools: Dict[str, ResourcePool] = {}
        self._lock = asyncio.Lock()

    def register_pool(self, name: str, pool: ResourcePool) -> None:
        """Register a named pool."""
        self._pools[name] = pool

    async def start_all(self) -> None:
        """Start all registered pools."""
        for name, pool in self._pools.items():
            try:
                await pool.start()
                log.info("Started pool '%s'", name)
            except Exception as e:
                log.error("Failed to start pool '%s': %s", name, e)

    async def stop_all(self) -> None:
        """Stop all registered pools."""
        for name, pool in self._pools.items():
            try:
                await pool.stop()
                log.info("Stopped pool '%s'", name)
            except Exception as e:
                log.error("Failed to stop pool '%s': %s", name, e)

    def get_pool(self, name: str) -> Optional[ResourcePool]:
        """Get a named pool."""
        return self._pools.get(name)

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get stats for all pools."""
        return {
            name: pool.get_stats()
            for name, pool in self._pools.items()
        }

    def get_health_report(self) -> Dict[str, Any]:
        """Get a health report across all pools."""
        total_pools = len(self._pools)
        healthy = sum(1 for p in self._pools.values() if p.is_healthy)
        total_created = sum(p.get_stats()["total_created"] for p in self._pools.values())
        total_acquired = sum(p.get_stats()["total_acquired"] for p in self._pools.values())
        total_failures = sum(p.get_stats()["total_failures"] for p in self._pools.values())

        return {
            "total_pools": total_pools,
            "healthy_pools": healthy,
            "degraded_pools": total_pools - healthy,
            "total_resources_created": total_created,
            "total_acquired": total_acquired,
            "total_failures": total_failures,
            "overall_health": (
                "healthy" if healthy == total_pools and total_failures == 0
                else "degraded" if healthy > 0
                else "offline"
            ),
        }
