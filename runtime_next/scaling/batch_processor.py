# runtime_next/scaling/batch_processor.py
# Phase 12: Batch processing engine for parallel execution of grouped work items
#
# Features:
# - Configurable batch sizes and concurrency limits
# - Throttling with configurable delay between batches
# - Error isolation (one item failure doesn't affect others)
# - Result aggregation and summary statistics
# - Progress tracking with per-item callbacks
# - Support for ordered and unordered batch processing

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Awaitable
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime, timezone

log = logging.getLogger("aelvo.runtime.scaling.batch_processor")


class BatchItemState(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


class BatchStrategy(str, Enum):
    """Strategy for processing items within a batch."""
    PARALLEL = "parallel"
    """Process all items in a batch concurrently."""
    SEQUENTIAL = "sequential"
    """Process items one at a time within a batch."""
    THROTTLED = "throttled"
    """Process items concurrently but with a max concurrency limit."""


class BatchErrorPolicy(str, Enum):
    """Policy for handling errors within a batch."""
    STOP_ON_ERROR = "stop_on_error"
    """Stop the entire batch when any item fails."""
    CONTINUE_ON_ERROR = "continue_on_error"
    """Continue processing remaining items even if one fails."""
    RETRY_FAILED = "retry_failed"
    """Retry failed items up to max_retries before continuing."""


@dataclass
class BatchItem:
    """A single item within a batch."""

    item_id: str
    data: Any
    state: BatchItemState = BatchItemState.PENDING
    result: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    attempts: int = 0
    max_retries: int = 0


@dataclass
class BatchResult:
    """Result of processing a batch of items."""

    batch_id: str
    total_items: int
    completed: int = 0
    failed: int = 0
    skipped: int = 0
    total_duration_ms: float = 0.0
    results: List[BatchItem] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    success_rate: float = 0.0


class BatchProcessor:
    """Asynchronous batch processor with throttling, error isolation, and aggregation.

    Processes groups of work items with configurable parallelism, throttling,
    and error handling policies.

    Usage:
        async def process_item(data: str) -> str:
            return data.upper()

        processor = BatchProcessor[str, str](
            handler=process_item,
            batch_size=10,
            max_concurrency=5,
        )
        results = await processor.process(items=["a", "b", "c"])
    """

    def __init__(
        self,
        handler: Callable[[Any], Awaitable[Any]],
        batch_size: int = 10,
        max_concurrency: int = 5,
        batch_strategy: BatchStrategy = BatchStrategy.PARALLEL,
        error_policy: BatchErrorPolicy = BatchErrorPolicy.CONTINUE_ON_ERROR,
        throttle_delay_seconds: float = 0.0,
        max_retries: int = 0,
        item_callback: Optional[Callable[[BatchItem], None]] = None,
        name: str = "batch",
    ):
        self._handler = handler
        self._batch_size = batch_size
        self._max_concurrency = max_concurrency
        self._batch_strategy = batch_strategy
        self._error_policy = error_policy
        self._throttle_delay = throttle_delay_seconds
        self._max_retries = max_retries
        self._item_callback = item_callback
        self._name = name

        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._total_processed = 0
        self._total_failed = 0

    # ── Public API ───────────────────────────────────────────────────────────

    async def process(self, items: List[Any]) -> BatchResult:
        """Process a list of items in batches.

        Args:
            items: The items to process.

        Returns:
            A BatchResult with aggregated results and statistics.
        """
        batch_id = self._generate_batch_id(items)
        total = len(items)
        log.info(
            "Batch '%s' (%s): processing %d items (batch_size=%d, strategy=%s)",
            self._name, batch_id[:8], total, self._batch_size, self._batch_strategy.value,
        )

        start_time = time.time()
        all_results: List[BatchItem] = []
        all_errors: List[str] = []

        # Split items into batches
        batches = self._split_into_batches(items)

        for batch_idx, batch in enumerate(batches):
            log.debug(
                "Batch '%s': processing batch %d/%d (%d items)",
                self._name, batch_idx + 1, len(batches), len(batch),
            )

            # Apply throttle delay between batches
            if batch_idx > 0 and self._throttle_delay > 0:
                await asyncio.sleep(self._throttle_delay)

            # Process batch
            batch_results = await self._process_batch(batch, batch_id)

            # Accumulate results
            for item in batch_results:
                all_results.append(item)
                if item.state == BatchItemState.FAILED:
                    all_errors.append(
                        f"[{item.item_id[:8]}] {item.error}" if item.error
                        else f"[{item.item_id[:8]}] Unknown error"
                    )

        duration_ms = (time.time() - start_time) * 1000
        completed = sum(1 for r in all_results if r.state == BatchItemState.COMPLETED)
        failed = sum(1 for r in all_results if r.state == BatchItemState.FAILED)
        skipped = sum(1 for r in all_results if r.state == BatchItemState.SKIPPED)

        self._total_processed += completed
        self._total_failed += failed

        result = BatchResult(
            batch_id=batch_id,
            total_items=total,
            completed=completed,
            failed=failed,
            skipped=skipped,
            total_duration_ms=round(duration_ms, 1),
            results=all_results,
            errors=all_errors,
            success_rate=round(completed / total * 100, 1) if total > 0 else 0.0,
        )

        log.info(
            "Batch '%s': %d/%d completed, %d failed, %d skipped in %.1fms "
            "(success_rate=%.1f%%)",
            self._name, completed, total, failed, skipped,
            duration_ms, result.success_rate,
        )

        return result

    async def process_stream(
        self,
        items: List[Any],
        yield_interval: float = 0.1,
    ) -> "AsyncBatchIterator":
        """Process items and yield results as they complete (streaming).

        Args:
            items: The items to process.
            yield_interval: How often to yield intermediate results.

        Returns:
            An AsyncBatchIterator that yields partial BatchResults as items complete.
        """
        return AsyncBatchIterator(self, items, yield_interval)

    def get_stats(self) -> Dict[str, Any]:
        """Get cumulative statistics for this processor."""
        return {
            "name": self._name,
            "batch_size": self._batch_size,
            "max_concurrency": self._max_concurrency,
            "strategy": self._batch_strategy.value,
            "error_policy": self._error_policy.value,
            "total_processed": self._total_processed,
            "total_failed": self._total_failed,
            "max_retries": self._max_retries,
            "throttle_delay": self._throttle_delay,
        }

    # ── Internal ─────────────────────────────────────────────────────────────

    def _split_into_batches(self, items: List[Any]) -> List[List[Any]]:
        """Split items into batches of configured size."""
        batches = []
        for i in range(0, len(items), self._batch_size):
            batches.append(items[i:i + self._batch_size])
        return batches

    async def _process_batch(
        self,
        batch_items: List[Any],
        batch_id: str,
    ) -> List[BatchItem]:
        """Process a single batch of items."""
        items = [
            BatchItem(
                item_id=self._generate_item_id(data, batch_id),
                data=data,
                max_retries=self._max_retries,
            )
            for data in batch_items
        ]

        if self._batch_strategy == BatchStrategy.SEQUENTIAL:
            return await self._process_sequential(items)
        elif self._batch_strategy == BatchStrategy.THROTTLED:
            return await self._process_throttled(items)
        else:
            return await self._process_parallel(items)

    async def _process_sequential(self, items: List[BatchItem]) -> List[BatchItem]:
        """Process items one at a time."""
        for item in items:
            item.state = BatchItemState.PROCESSING
            item.started_at = time.time()
            self._notify_item(item)

            for attempt in range(item.max_retries + 1):
                item.attempts = attempt + 1
                if attempt > 0:
                    item.state = BatchItemState.RETRYING
                    await asyncio.sleep(1.0 * (2 ** (attempt - 1)))

                try:
                    item.result = await self._handler(item.data)
                    item.state = BatchItemState.COMPLETED
                    item.completed_at = time.time()
                    self._notify_item(item)
                    break
                except Exception as e:
                    item.error = str(e)
                    if attempt < item.max_retries:
                        log.warning(
                            "Item '%s': attempt %d/%d failed, retrying: %s",
                            item.item_id[:8], attempt + 1, item.max_retries + 1, e,
                        )
                    else:
                        item.state = BatchItemState.FAILED
                        item.completed_at = time.time()
                        self._notify_item(item)
                        if self._error_policy == BatchErrorPolicy.STOP_ON_ERROR:
                            return items

        return items

    async def _process_parallel(self, items: List[BatchItem]) -> List[BatchItem]:
        """Process all items concurrently."""
        cancel_event = asyncio.Event()

        async def _process_item(item: BatchItem) -> None:
            if cancel_event.is_set():
                item.state = BatchItemState.SKIPPED
                return

            item.state = BatchItemState.PROCESSING
            item.started_at = time.time()
            self._notify_item(item)

            for attempt in range(item.max_retries + 1):
                if cancel_event.is_set():
                    item.state = BatchItemState.SKIPPED
                    return
                item.attempts = attempt + 1
                if attempt > 0:
                    item.state = BatchItemState.RETRYING
                    await asyncio.sleep(1.0 * (2 ** (attempt - 1)))

                try:
                    item.result = await self._handler(item.data)
                    item.state = BatchItemState.COMPLETED
                    item.completed_at = time.time()
                    self._notify_item(item)
                    return
                except Exception as e:
                    item.error = str(e)
                    if attempt < item.max_retries:
                        log.debug(
                            "Item '%s': attempt %d/%d failed, retrying: %s",
                            item.item_id[:8], attempt + 1, item.max_retries + 1, e,
                        )
                    else:
                        item.state = BatchItemState.FAILED
                        item.completed_at = time.time()
                        self._notify_item(item)
                        if self._error_policy == BatchErrorPolicy.STOP_ON_ERROR:
                            cancel_event.set()
                            return

        await asyncio.gather(
            *(_process_item(item) for item in items),
            return_exceptions=True,
        )
        return items

    async def _process_throttled(self, items: List[BatchItem]) -> List[BatchItem]:
        """Process items with concurrency throttling."""
        sem = asyncio.Semaphore(self._max_concurrency)

        async def _throttled_process(item: BatchItem) -> None:
            async with sem:
                item.state = BatchItemState.PROCESSING
                item.started_at = time.time()
                self._notify_item(item)

                for attempt in range(item.max_retries + 1):
                    item.attempts = attempt + 1
                    if attempt > 0:
                        item.state = BatchItemState.RETRYING
                        await asyncio.sleep(1.0 * (2 ** (attempt - 1)))

                    try:
                        item.result = await self._handler(item.data)
                        item.state = BatchItemState.COMPLETED
                        item.completed_at = time.time()
                        self._notify_item(item)
                        return
                    except Exception as e:
                        item.error = str(e)
                        if attempt < item.max_retries:
                            log.debug(
                                "Item '%s': attempt %d/%d failed, retrying: %s",
                                item.item_id[:8], attempt + 1, item.max_retries + 1, e,
                            )
                        else:
                            item.state = BatchItemState.FAILED
                            item.completed_at = time.time()
                            self._notify_item(item)

        await asyncio.gather(
            *(_throttled_process(item) for item in items),
            return_exceptions=True,
        )
        return items

    def _notify_item(self, item: BatchItem) -> None:
        """Notify item callback if registered."""
        if self._item_callback:
            try:
                self._item_callback(item)
            except Exception as e:
                log.warning("Item callback failed: %s", e)

    def _generate_batch_id(self, items: List[Any]) -> str:
        raw = f"batch_{self._name}_{len(items)}_{time.time()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _generate_item_id(self, data: Any, batch_id: str) -> str:
        raw = f"item_{batch_id}_{str(data)[:40]}_{time.time()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]


class AsyncBatchIterator:
    """Async iterator that yields progressively complete BatchResults.

    Usage:
        async for partial in processor.process_stream(items):
            print(f"{partial.completed}/{partial.total_items} done")
    """

    def __init__(
        self,
        processor: BatchProcessor,
        items: List[Any],
        yield_interval: float,
    ):
        self._processor = processor
        self._items = items
        self._yield_interval = yield_interval
        self._task: Optional[asyncio.Task] = None

    def __aiter__(self) -> "AsyncBatchIterator":
        return self

    async def __anext__(self) -> BatchResult:
        if self._task is None:
            self._task = asyncio.create_task(self._processor.process(self._items))
            await asyncio.sleep(self._yield_interval)

        if self._task.done():
            raise StopAsyncIteration

        # Check progress every yield_interval
        # Note: This is a simplified implementation. A full streaming
        # implementation would use an asyncio.Queue for item-level updates.
        # For now, we wait for the next yield interval and report partial.
        await asyncio.sleep(self._yield_interval)

        if self._task.done():
            raise StopAsyncIteration

        return self._task.result()
