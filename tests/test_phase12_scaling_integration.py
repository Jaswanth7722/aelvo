"""Phase 12 — Scaling Integration Tests.

Covers:
- ResourcePool: acquire/release lifecycle, concurrency limits, idle timeout, error handling, ResourcePoolManager
- AsyncPipeline: stage execution, parallel branches, dependency ordering, pause/resume/cancel, retry, progress
- BatchProcessor: batch splitting, parallel/sequential/throttled strategies, error policies, retry, streaming
"""

import asyncio
import time
import logging

import pytest

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger("test_scaling")


# ═══════════════════════════════════════════════════════════════════════════════
# ResourcePool Tests
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_acquire_release_basic():
    """Basic acquire and release cycle."""
    from runtime_next.scaling.resource_pool import ResourcePool

    counter = {"created": 0, "destroyed": 0}

    def creator():
        counter["created"] += 1
        return {"id": counter["created"]}

    def destroyer(r):
        counter["destroyed"] += 1

    pool = ResourcePool(
        creator=creator,
        max_size=3,
        destroyer=destroyer,
        name="test_basic",
    )
    await pool.start()

    resource = await pool.acquire()
    assert resource is not None
    assert resource["id"] == 1
    assert pool.get_stats()["acquired_count"] == 1
    assert pool.available_count == 0

    await pool.release(resource)
    assert pool.available_count == 1

    await pool.stop()
    assert counter["destroyed"] == 1


@pytest.mark.asyncio
async def test_max_concurrent_acquires():
    """Pool enforces max_size limit."""
    from runtime_next.scaling.resource_pool import ResourcePool

    acquired_resources = []

    def creator():
        return {"id": len(acquired_resources) + 1}

    pool = ResourcePool(creator=creator, max_size=2, name="test_max")
    await pool.start()

    r1 = await pool.acquire()
    r2 = await pool.acquire()
    acquired_resources.extend([r1, r2])

    assert pool.available_count == 0

    # Release one, then acquire should work
    await pool.release(r1)
    r3 = await pool.acquire()
    assert r3 is not None

    await pool.release(r2)
    await pool.release(r3)
    await pool.stop()


@pytest.mark.asyncio
async def test_acquire_timeout():
    """Acquire raises TimeoutError when pool is exhausted and timeout elapses."""
    from runtime_next.scaling.resource_pool import ResourcePool

    def creator():
        return {"id": 1}

    pool = ResourcePool(creator=creator, max_size=1, name="test_timeout")
    await pool.start()

    r1 = await pool.acquire()
    with pytest.raises(TimeoutError):
        await pool.acquire(timeout_seconds=0.1)

    await pool.release(r1)
    await pool.stop()


@pytest.mark.asyncio
async def test_release_mark_failed():
    """Releasing with mark_failed=True marks resource as invalid."""
    from runtime_next.scaling.resource_pool import ResourcePool

    destroyed = []

    def creator():
        return {"id": len(destroyed) + 1}

    def destroyer(r):
        destroyed.append(r)

    pool = ResourcePool(creator=creator, max_size=2, destroyer=destroyer, name="test_failed")
    await pool.start()

    r1 = await pool.acquire()
    await pool.release(r1, mark_failed=True)

    stats = pool.get_stats()
    assert stats["total_failures"] == 1

    await pool.stop()


@pytest.mark.asyncio
async def test_context_manager():
    """Async context manager acquire_context works."""
    from runtime_next.scaling.resource_pool import ResourcePool

    def creator():
        return {"id": 1}

    pool = ResourcePool(creator=creator, max_size=2, name="test_ctx")
    await pool.start()

    async with pool.acquire_context() as resource:
        assert resource is not None
        assert resource["id"] == 1
        assert pool.available_count == 0

    assert pool.available_count == 1

    await pool.stop()


@pytest.mark.asyncio
async def test_pool_stats():
    """Pool stats reflect correct state."""
    from runtime_next.scaling.resource_pool import ResourcePool

    def creator():
        return {"id": 1}

    pool = ResourcePool(creator=creator, max_size=3, name="test_stats")
    await pool.start()

    r1 = await pool.acquire()
    r2 = await pool.acquire()

    stats = pool.get_stats()
    assert stats["name"] == "test_stats"
    assert stats["pool_size"] == 2  # Created on demand
    assert stats["acquired_count"] == 2
    assert stats["available_count"] == 0
    assert stats["max_size"] == 3
    assert stats["total_acquired"] == 2

    await pool.release(r1)
    await pool.release(r2)
    await pool.stop()


@pytest.mark.asyncio
async def test_is_healthy():
    """Pool health reflects state."""
    from runtime_next.scaling.resource_pool import ResourcePool

    def creator():
        return {"id": 1}

    pool = ResourcePool(creator=creator, max_size=2, name="test_health")
    await pool.start()

    assert pool.is_healthy is True

    await pool.stop()
    # After stop, closed pool is not healthy
    # (is_healthy checks state != CLOSED)
    assert pool.is_healthy is False


@pytest.mark.asyncio
async def test_connection_pool_basic():
    """ConnectionPool extends ResourcePool with connection tracking."""
    from runtime_next.scaling.resource_pool import ConnectionPool

    def creator():
        return {"conn": "db"}

    pool = ConnectionPool(creator=creator, max_size=2, name="test_conn")
    await pool.start()

    conn = await pool.acquire()
    assert conn is not None

    pool.record_query(conn)
    pool.record_query(conn)

    stats = pool.get_connection_stats()
    assert stats["total_queries"] == 2  # Two queries on one connection
    assert stats["total_connections"] == 1

    await pool.release(conn)
    await pool.stop()


@pytest.mark.asyncio
async def test_resource_pool_manager():
    """ResourcePoolManager manages multiple pools."""
    from runtime_next.scaling.resource_pool import ResourcePool, ResourcePoolManager

    def creator_a():
        return {"type": "a"}

    def creator_b():
        return {"type": "b"}

    manager = ResourcePoolManager()
    pool_a = ResourcePool(creator=creator_a, max_size=2, name="pool_a")
    pool_b = ResourcePool(creator=creator_b, max_size=3, name="pool_b")
    manager.register_pool("a", pool_a)
    manager.register_pool("b", pool_b)

    assert manager.get_pool("a") is pool_a
    assert manager.get_pool("b") is pool_b
    assert manager.get_pool("nonexistent") is None

    await manager.start_all()

    # Verify pools are running
    stats = manager.get_all_stats()
    assert "a" in stats
    assert "b" in stats

    health = manager.get_health_report()
    assert health["total_pools"] == 2
    assert health["healthy_pools"] == 2
    assert health["overall_health"] == "healthy"

    await manager.stop_all()


# ═══════════════════════════════════════════════════════════════════════════════
# AsyncPipeline Tests
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_pipeline_single_stage():
    """Single stage executes successfully."""
    from runtime_next.scaling.async_pipeline import (
        AsyncPipeline, PipelineStage, PipelineTask, StageState,
    )

    results = []

    async def task_a():
        results.append("a")
        return "A"

    pipeline = AsyncPipeline(name="test_single")
    stage = PipelineStage(
        stage_id="s1",
        name="Stage 1",
        tasks=[PipelineTask(task_id="t1", name="Task A", coro=task_a)],
        parallel=False,
    )
    pipeline.add_stage(stage)

    outcomes = await pipeline.run()

    assert len(results) == 1
    assert results[0] == "a"
    assert outcomes["s1"].state == StageState.COMPLETED
    assert pipeline.state.value == "completed"


@pytest.mark.asyncio
async def test_pipeline_multiple_stages_sequential():
    """Multiple sequential stages execute in order."""
    from runtime_next.scaling.async_pipeline import (
        AsyncPipeline, PipelineStage, PipelineTask, StageState,
    )

    order = []

    async def task_a():
        order.append("a")

    async def task_b():
        order.append("b")

    async def task_c():
        order.append("c")

    pipeline = AsyncPipeline(name="test_multi")
    pipeline.add_stage(PipelineStage(
        stage_id="s1", name="A",
        tasks=[PipelineTask(task_id="t1", name="A", coro=task_a)],
    ))
    pipeline.add_stage(PipelineStage(
        stage_id="s2", name="B",
        tasks=[PipelineTask(task_id="t2", name="B", coro=task_b)],
        dependencies=["s1"],
    ))
    pipeline.add_stage(PipelineStage(
        stage_id="s3", name="C",
        tasks=[PipelineTask(task_id="t3", name="C", coro=task_c)],
        dependencies=["s2"],
    ))

    await pipeline.run()

    assert order == ["a", "b", "c"]
    assert pipeline.get_result("s3").state == StageState.COMPLETED


@pytest.mark.asyncio
async def test_pipeline_parallel_stage():
    """Tasks within a parallel stage execute concurrently."""
    from runtime_next.scaling.async_pipeline import (
        AsyncPipeline, PipelineStage, PipelineTask, StageState,
    )

    order = []
    lock = asyncio.Lock()

    async def slow_task(name: str, delay: float):
        await asyncio.sleep(delay)
        async with lock:
            order.append(name)
        return name

    pipeline = AsyncPipeline(name="test_parallel")

    pipeline.add_stage(PipelineStage(
        stage_id="s1",
        name="Parallel",
        tasks=[
            PipelineTask(task_id="t1", name="Fast", coro=lambda: slow_task("fast", 0.05)),
            PipelineTask(task_id="t2", name="Slow", coro=lambda: slow_task("slow", 0.1)),
            PipelineTask(task_id="t3", name="Medium", coro=lambda: slow_task("medium", 0.075)),
        ],
        parallel=True,
        max_concurrency=3,
    ))

    outcomes = await pipeline.run()

    assert outcomes["s1"].state == StageState.COMPLETED
    # All tasks completed (order doesn't matter for parallel)
    assert "fast" in order
    assert "slow" in order
    assert "medium" in order


@pytest.mark.asyncio
async def test_pipeline_dependency_blocked():
    """Stage is blocked when dependencies are not met."""
    from runtime_next.scaling.async_pipeline import (
        AsyncPipeline, PipelineStage, PipelineTask, StageState,
    )

    async def task_a():
        return "A"

    pipeline = AsyncPipeline(name="test_blocked")
    pipeline.add_stage(PipelineStage(
        stage_id="s1", name="S1",
        tasks=[PipelineTask(task_id="t1", name="A", coro=task_a)],
    ))
    pipeline.add_stage(PipelineStage(
        stage_id="s2", name="S2",
        tasks=[PipelineTask(task_id="t2", name="B", coro=task_a)],
        dependencies=["nonexistent"],
    ))

    outcomes = await pipeline.run()

    assert outcomes["s1"].state == StageState.COMPLETED
    assert outcomes["s2"].state == StageState.BLOCKED


@pytest.mark.asyncio
async def test_pipeline_skip_on_failure():
    """Stage is skipped when dependency fails and skip_on_failure=True."""
    from runtime_next.scaling.async_pipeline import (
        AsyncPipeline, PipelineStage, PipelineTask, StageState,
    )

    async def failing_task():
        raise ValueError("Intentional failure")

    async def good_task():
        return "OK"

    pipeline = AsyncPipeline(name="test_skip")
    pipeline.add_stage(PipelineStage(
        stage_id="s1", name="Fail",
        tasks=[PipelineTask(task_id="t1", name="Fail", coro=failing_task)],
    ))
    pipeline.add_stage(PipelineStage(
        stage_id="s2", name="Skip",
        tasks=[PipelineTask(task_id="t2", name="Good", coro=good_task)],
        dependencies=["s1"],
        skip_on_failure=True,
    ))

    outcomes = await pipeline.run()

    assert outcomes["s1"].state == StageState.FAILED
    assert outcomes["s2"].state == StageState.SKIPPED
    assert pipeline.state.value == "failed"


@pytest.mark.asyncio
async def test_pipeline_pause_resume():
    """Pipeline can be paused and resumed."""
    from runtime_next.scaling.async_pipeline import (
        AsyncPipeline, PipelineStage, PipelineTask,
    )

    order = []

    async def fast_task():
        order.append("fast")

    async def slow_task():
        await asyncio.sleep(0.2)
        order.append("slow")

    pipeline = AsyncPipeline(name="test_pause")
    pipeline.add_stage(PipelineStage(
        stage_id="s1", name="Fast",
        tasks=[PipelineTask(task_id="t1", name="Fast", coro=fast_task)],
    ))
    pipeline.add_stage(PipelineStage(
        stage_id="s2", name="Slow",
        tasks=[PipelineTask(task_id="t2", name="Slow", coro=slow_task)],
        dependencies=["s1"],
    ))

    # Start pipeline in background
    async def run_with_pause():
        await pipeline.run()

    task = asyncio.create_task(run_with_pause())
    await asyncio.sleep(0.05)

    # Pause
    await pipeline.pause()
    assert "fast" in order  # First stage completed
    assert "slow" not in order  # Second stage paused

    # Resume
    await pipeline.resume()
    await task

    assert "slow" in order


@pytest.mark.asyncio
async def test_pipeline_cancel():
    """Pipeline can be cancelled."""
    from runtime_next.scaling.async_pipeline import (
        AsyncPipeline, PipelineStage, PipelineTask,
    )

    async def slow_task():
        await asyncio.sleep(5.0)  # Will be cancelled
        return "done"

    pipeline = AsyncPipeline(name="test_cancel")
    pipeline.add_stage(PipelineStage(
        stage_id="s1", name="Slow",
        tasks=[PipelineTask(task_id="t1", name="Slow", coro=slow_task)],
    ))

    async def run_and_cancel():
        await pipeline.run()

    asyncio.create_task(run_and_cancel())
    await asyncio.sleep(0.05)

    await pipeline.cancel()
    await asyncio.sleep(0.1)

    assert pipeline.state.value in ("cancelled", "failed")


@pytest.mark.asyncio
async def test_pipeline_progress():
    """Progress callback is invoked."""
    from runtime_next.scaling.async_pipeline import (
        AsyncPipeline, PipelineStage, PipelineTask,
    )

    progress_updates = []

    def on_progress(progress):
        progress_updates.append(progress)

    async def task_a():
        return "A"

    async def task_b():
        return "B"

    pipeline = AsyncPipeline(name="test_progress", progress_callback=on_progress)
    pipeline.add_stage(PipelineStage(
        stage_id="s1", name="A",
        tasks=[PipelineTask(task_id="t1", name="A", coro=task_a)],
    ))
    pipeline.add_stage(PipelineStage(
        stage_id="s2", name="B",
        tasks=[PipelineTask(task_id="t2", name="B", coro=task_b)],
        dependencies=["s1"],
    ))

    await pipeline.run()

    assert len(progress_updates) >= 1
    assert progress_updates[-1]["state"] == "completed"
    assert progress_updates[-1]["completed_stages"] == 2


@pytest.mark.asyncio
async def test_pipeline_builder():
    """PipelineBuilder constructs pipelines correctly."""
    from runtime_next.scaling.async_pipeline import PipelineBuilder, StageState

    order = []

    async def task_a():
        order.append("a")

    async def task_b():
        order.append("b")

    pipeline = (
        PipelineBuilder(name="test_builder")
        .with_max_concurrency(5)
        .add_stage("s1", "Stage 1")
        .add_task("Task A", task_a)
        .add_stage("s2", "Stage 2", dependencies=["s1"])
        .add_task("Task B", task_b)
        .build()
    )

    outcomes = await pipeline.run()

    assert order == ["a", "b"]
    assert outcomes["s1"].state == StageState.COMPLETED
    assert outcomes["s2"].state == StageState.COMPLETED


# ═══════════════════════════════════════════════════════════════════════════════
# BatchProcessor Tests
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_batch_basic():
    """Basic batch processing."""
    from runtime_next.scaling.batch_processor import BatchProcessor

    async def upper(data: str) -> str:
        return data.upper()

    processor = BatchProcessor(handler=upper, batch_size=10, name="test_basic")
    result = await processor.process(["a", "b", "c"])

    assert result.total_items == 3
    assert result.completed == 3
    assert result.failed == 0
    assert result.success_rate == 100.0
    assert result.results[0].result == "A"
    assert result.results[1].result == "B"
    assert result.results[2].result == "C"


@pytest.mark.asyncio
async def test_batch_sequential_strategy():
    """Sequential strategy processes items one at a time."""
    from runtime_next.scaling.batch_processor import (
        BatchProcessor, BatchStrategy,
    )

    order = []

    async def slow_upper(data: str) -> str:
        await asyncio.sleep(0.05)
        order.append(data)
        return data.upper()

    processor = BatchProcessor(
        handler=slow_upper,
        batch_size=5,
        batch_strategy=BatchStrategy.SEQUENTIAL,
        name="test_seq",
    )
    result = await processor.process(["x", "y", "z"])

    assert result.completed == 3
    assert order == ["x", "y", "z"]  # Sequential order
    assert result.results[0].result == "X"


@pytest.mark.asyncio
async def test_batch_throttled_strategy():
    """Throttled strategy limits concurrency."""
    from runtime_next.scaling.batch_processor import (
        BatchProcessor, BatchStrategy,
    )

    concurrent = {"max": 0, "current": 0}
    lock = asyncio.Lock()

    async def tracked_task(data: str) -> str:
        async with lock:
            concurrent["current"] += 1
            concurrent["max"] = max(concurrent["max"], concurrent["current"])

        await asyncio.sleep(0.1)

        async with lock:
            concurrent["current"] -= 1

        return data

    processor = BatchProcessor(
        handler=tracked_task,
        batch_size=10,
        max_concurrency=2,
        batch_strategy=BatchStrategy.THROTTLED,
        name="test_throttle",
    )
    result = await processor.process(["a", "b", "c", "d"])

    assert result.completed == 4
    # Max concurrency should not exceed our limit
    assert concurrent["max"] <= 2


@pytest.mark.asyncio
async def test_batch_continue_on_error():
    """CONTINUE_ON_ERROR policy processes remaining items after a failure."""
    from runtime_next.scaling.batch_processor import (
        BatchProcessor, BatchErrorPolicy, BatchItemState,
    )

    async def fragile(data: int) -> int:
        if data == 2:
            raise ValueError("Intentional failure for 2")
        return data * 2

    processor = BatchProcessor(
        handler=fragile,
        batch_size=5,
        error_policy=BatchErrorPolicy.CONTINUE_ON_ERROR,
        name="test_continue",
    )
    result = await processor.process([1, 2, 3, 4])

    assert result.total_items == 4
    assert result.completed == 3
    assert result.failed == 1
    assert result.results[0].result == 2
    assert result.results[1].state == BatchItemState.FAILED
    assert result.results[2].result == 6


@pytest.mark.asyncio
async def test_batch_stop_on_error():
    """STOP_ON_ERROR policy stops at first failure."""
    from runtime_next.scaling.batch_processor import (
        BatchProcessor, BatchStrategy, BatchErrorPolicy,
    )

    async def fragile(data: int) -> int:
        if data == 2:
            raise ValueError("Intentional failure for 2")
        await asyncio.sleep(0.02)
        return data * 2

    processor = BatchProcessor(
        handler=fragile,
        batch_size=5,
        error_policy=BatchErrorPolicy.STOP_ON_ERROR,
        batch_strategy=BatchStrategy.SEQUENTIAL,  # Sequential to control order
        name="test_stop",
    )
    result = await processor.process([1, 2, 3, 4])

    assert result.total_items == 4
    assert result.completed == 1  # Only first succeeded
    assert result.failed >= 1  # At least one failed


@pytest.mark.asyncio
async def test_batch_retry():
    """Retry mechanism works."""
    from runtime_next.scaling.batch_processor import BatchProcessor

    attempts = {"count": 0}

    async def flaky(data: str) -> str:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ValueError("Not ready yet")
        return "success"

    processor = BatchProcessor(
        handler=flaky,
        batch_size=1,
        max_retries=3,
        name="test_retry",
    )
    result = await processor.process(["task"])

    assert result.completed == 1
    assert result.results[0].result == "success"
    # Should have attempted 3 times (first 2 fail, 3rd succeeds)


@pytest.mark.asyncio
async def test_batch_throttle_delay():
    """Throttle delay between batches works."""
    from runtime_next.scaling.batch_processor import BatchProcessor

    async def identity(data: int) -> int:
        return data

    processor = BatchProcessor(
        handler=identity,
        batch_size=2,  # 2 items per batch
        throttle_delay_seconds=0.1,
        name="test_delay",
    )
    start = time.time()
    result = await processor.process([1, 2, 3, 4, 5, 6])  # 3 batches
    duration = time.time() - start

    # Should have had 2 delays (between batch 1->2, batch 2->3)
    assert duration >= 0.18  # Some fudge for execution time
    assert result.completed == 6


@pytest.mark.asyncio
async def test_batch_item_callback():
    """Item callback is invoked for each item."""
    from runtime_next.scaling.batch_processor import BatchProcessor, BatchItem

    callback_items = []

    def on_item(item: BatchItem):
        callback_items.append(item.item_id[:8])

    async def upper(data: str) -> str:
        return data.upper()

    processor = BatchProcessor(
        handler=upper,
        batch_size=3,
        item_callback=on_item,
        name="test_cb",
    )
    await processor.process(["a", "b"])

    # Callback fires on both start and completion (2 calls per item)
    assert len(callback_items) == 4


@pytest.mark.asyncio
async def test_batch_empty():
    """Empty item list returns empty result."""
    from runtime_next.scaling.batch_processor import BatchProcessor

    async def upper(data: str) -> str:
        return data.upper()

    processor = BatchProcessor(handler=upper, name="test_empty")
    result = await processor.process([])

    assert result.total_items == 0
    assert result.completed == 0
    assert result.success_rate == 0.0


@pytest.mark.asyncio
async def test_batch_stats():
    """BatchProcessor returns correct cumulative stats."""
    from runtime_next.scaling.batch_processor import BatchProcessor, BatchStrategy, BatchErrorPolicy

    async def upper(data: str) -> str:
        return data.upper()

    processor = BatchProcessor(
        handler=upper,
        batch_size=5,
        max_concurrency=3,
        batch_strategy=BatchStrategy.PARALLEL,
        error_policy=BatchErrorPolicy.CONTINUE_ON_ERROR,
        name="test_stats",
    )

    stats = processor.get_stats()
    assert stats["name"] == "test_stats"
    assert stats["batch_size"] == 5
    assert stats["max_concurrency"] == 3
    assert stats["strategy"] == "parallel"
    assert stats["error_policy"] == "continue_on_error"


# ═══════════════════════════════════════════════════════════════════════════════
# Integration Tests
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_integration_pipeline_with_batch():
    """Pipeline stages can use BatchProcessor for parallel item processing."""
    from runtime_next.scaling.async_pipeline import (
        AsyncPipeline, PipelineStage, PipelineTask, StageState,
    )
    from runtime_next.scaling.batch_processor import BatchProcessor

    async def process_items(items: list) -> list:
        async def handler(item: str) -> str:
            await asyncio.sleep(0.02)
            return item.upper()

        bp = BatchProcessor(handler=handler, batch_size=2, name="inner")
        result = await bp.process(items)
        return [r.result for r in result.results]

    pipeline = AsyncPipeline(name="test_integration")
    pipeline.add_stage(PipelineStage(
        stage_id="s1", name="Process",
        tasks=[PipelineTask(task_id="t1", name="Process", coro=lambda: process_items(["a", "b", "c"]))],
    ))
    pipeline.add_stage(PipelineStage(
        stage_id="s2", name="Verify",
        tasks=[PipelineTask(task_id="t2", name="Verify", coro=lambda: "done")],
        dependencies=["s1"],
    ))

    outcomes = await pipeline.run()

    assert outcomes["s1"].state == StageState.COMPLETED
    assert outcomes["s2"].state == StageState.COMPLETED


@pytest.mark.asyncio
async def test_integration_pool_with_pipeline():
    """ResourcePool can provide resources used within a pipeline stage."""
    from runtime_next.scaling.resource_pool import ResourcePool
    from runtime_next.scaling.async_pipeline import (
        AsyncPipeline, PipelineStage, PipelineTask,
    )

    pool_results = []

    def creator():
        return {"conn": "db"}

    pool = ResourcePool(creator=creator, max_size=2, name="integ_pool")
    await pool.start()

    async def use_pool():
        async with pool.acquire_context() as conn:
            pool_results.append(conn)

    pipeline = AsyncPipeline(name="test_integ_pool")
    pipeline.add_stage(PipelineStage(
        stage_id="s1", name="Pool",
        tasks=[PipelineTask(task_id="t1", name="Use", coro=use_pool)],
    ))

    await pipeline.run()

    assert len(pool_results) == 1
    assert pool_results[0]["conn"] == "db"

    await pool.stop()
