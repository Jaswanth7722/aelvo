# runtime_next/scaling/async_pipeline.py
# Phase 12: Async pipeline execution with parallel branches,
# concurrency control, stage management, and progress tracking.
#
# Pipelines are composed of stages that may execute sequentially or in
# parallel. Each stage can be a single task or a batch of parallel tasks.
# Progress is tracked across all stages with callbacks for real-time
# monitoring.

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar, Union
from enum import Enum
from datetime import datetime, timezone
from dataclasses import dataclass, field

log = logging.getLogger("aelvo.runtime.scaling.async_pipeline")


class StageState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    BLOCKED = "blocked"


class PipelineState(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StagePriority(int, Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class StageResult:
    """Result of a single pipeline stage execution."""

    stage_id: str
    state: StageState
    output: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    attempts: int = 1


@dataclass
class PipelineTask:
    """A single task within a pipeline stage."""

    task_id: str
    name: str
    coro: Any  # The async callable or coroutine
    priority: StagePriority = StagePriority.NORMAL
    timeout_seconds: Optional[float] = None
    retries: int = 0
    max_retries: int = 0


@dataclass
class PipelineStage:
    """A stage in the pipeline execution flow."""

    stage_id: str
    name: str
    tasks: List[PipelineTask]
    parallel: bool = False  # If True, tasks run concurrently within this stage
    max_concurrency: int = 0  # 0 = unlimited within stage
    dependencies: List[str] = field(default_factory=list)
    priority: StagePriority = StagePriority.NORMAL
    state: StageState = StageState.PENDING
    skip_on_failure: bool = False
    timeout_seconds: Optional[float] = None
    result: Optional[StageResult] = None


class AsyncPipeline:
    """Async pipeline executor with sequential and parallel stage execution.

    Features:
    - Stage-based execution with dependency ordering
    - Parallel branch execution within stages
    - Concurrency control (max_concurrency per stage)
    - Stage-level and per-task timeouts
    - Retry logic per task
    - Progress tracking with callbacks
    - Pause/resume/cancel lifecycle
    """

    def __init__(
        self,
        name: str = "default",
        max_concurrency: int = 5,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        self._name = name
        self._stages: Dict[str, PipelineStage] = {}
        self._stage_order: List[str] = []
        self._max_concurrency = max_concurrency
        self._progress_callback = progress_callback
        self._state = PipelineState.IDLE
        self._results: Dict[str, StageResult] = {}
        self._lock = asyncio.Lock()
        self._cancel_event = asyncio.Event()
        self._pause_event = asyncio.Event()
        self._pause_event.set()  # Not paused initially
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._current_run_task: Optional[asyncio.Task] = None

    # ── Stage Management ─────────────────────────────────────────────────────

    def add_stage(self, stage: PipelineStage) -> None:
        """Add a stage to the pipeline."""
        self._stages[stage.stage_id] = stage
        self._stage_order.append(stage.stage_id)

    def add_task(
        self,
        stage_id: str,
        task: PipelineTask,
    ) -> None:
        """Add a task to an existing stage."""
        if stage_id in self._stages:
            self._stages[stage_id].tasks.append(task)

    def insert_stage(
        self,
        after_stage_id: str,
        stage: PipelineStage,
    ) -> bool:
        """Insert a stage after another stage in the execution order."""
        if after_stage_id not in self._stages:
            return False
        self._stages[stage.stage_id] = stage
        idx = self._stage_order.index(after_stage_id) + 1
        self._stage_order.insert(idx, stage.stage_id)
        return True

    # ── Execution ────────────────────────────────────────────────────────────

    async def run(self) -> Dict[str, StageResult]:
        """Run the pipeline through all stages.

        Returns:
            Dict mapping stage_id -> StageResult for all executed stages.
        """
        if self._state == PipelineState.RUNNING:
            raise RuntimeError(f"Pipeline '{self._name}' is already running")

        self._state = PipelineState.RUNNING
        self._cancel_event.clear()
        self._pause_event.set()
        self._semaphore = asyncio.Semaphore(self._max_concurrency)
        self._current_run_task = asyncio.current_task()

        total_stages = len(self._stage_order)
        log.info(
            "Pipeline '%s': starting with %d stages, max_concurrency=%d",
            self._name, total_stages, self._max_concurrency,
        )

        completed_stages: Set[str] = set()

        try:
            for stage_id in self._stage_order:
                # Check cancellation
                if self._cancel_event.is_set():
                    self._state = PipelineState.CANCELLED
                    log.warning("Pipeline '%s': cancelled", self._name)
                    break

                # Wait if paused
                await self._pause_event.wait()

                stage = self._stages[stage_id]

                # Check dependencies
                deps_met = all(d in completed_stages for d in stage.dependencies)
                if not deps_met:
                    log.warning(
                        "Stage '%s': dependencies not met: %s",
                        stage_id, stage.dependencies,
                    )
                    stage.state = StageState.BLOCKED
                    self._results[stage_id] = StageResult(
                        stage_id=stage_id,
                        state=StageState.BLOCKED,
                        error=f"Dependencies not met: {stage.dependencies}",
                    )
                    self._notify_progress()
                    continue

                # Resolve dynamic dependencies: if skip_on_failure and any
                # dependency failed, skip this stage
                if stage.skip_on_failure:
                    dep_failed = any(
                        self._results.get(d, StageResult(stage_id=d, state=StageState.PENDING)).state
                        == StageState.FAILED
                        for d in stage.dependencies
                        if d in self._results
                    )
                    if dep_failed:
                        stage.state = StageState.SKIPPED
                        self._results[stage_id] = StageResult(
                            stage_id=stage_id,
                            state=StageState.SKIPPED,
                            output="Skipped due to dependency failure",
                        )
                        self._notify_progress()
                        continue

                # Execute stage
                result = await self._execute_stage(stage)
                self._results[stage_id] = result
                completed_stages.add(stage_id)

                if result.state == StageState.FAILED:
                    log.error(
                        "Pipeline '%s': stage '%s' failed: %s",
                        self._name, stage_id, result.error,
                    )
                    # Decide whether to continue
                    if not stage.skip_on_failure:
                        self._state = PipelineState.FAILED
                        # Mark remaining stages as skipped so they appear in results
                        for sid in self._stage_order:
                            if sid not in self._results:
                                s = self._stages[sid]
                                s.state = StageState.SKIPPED
                                self._results[sid] = StageResult(
                                    stage_id=sid,
                                    state=StageState.SKIPPED,
                                    output="Skipped due to upstream stage failure",
                                )
                        break

                self._notify_progress()

        except asyncio.CancelledError:
            self._state = PipelineState.CANCELLED
            log.warning("Pipeline '%s': cancelled via CancelledError", self._name)
        except Exception as e:
            self._state = PipelineState.FAILED
            log.error("Pipeline '%s': unexpected error: %s", self._name, e)
        finally:
            if self._state == PipelineState.RUNNING:
                self._state = PipelineState.COMPLETED

            completed = sum(
                1 for r in self._results.values() if r.state == StageState.COMPLETED
            )
            failed = sum(
                1 for r in self._results.values() if r.state == StageState.FAILED
            )
            log.info(
                "Pipeline '%s': %s — %d/%d stages, %d failed",
                self._name, self._state.value, completed, total_stages, failed,
            )

            # Final progress notification with finalized state
            self._notify_progress()

        return dict(self._results)

    async def _execute_stage(self, stage: PipelineStage) -> StageResult:
        """Execute a single pipeline stage."""
        stage.state = StageState.RUNNING
        started_at = time.time()
        log.info("Stage '%s': executing %d tasks (parallel=%s)", stage.name, len(stage.tasks), stage.parallel)

        if not stage.tasks:
            result = StageResult(
                stage_id=stage.stage_id,
                state=StageState.COMPLETED,
                started_at=started_at,
                completed_at=time.time(),
            )
            stage.state = StageState.COMPLETED
            return result

        max_retries = max(t.max_retries for t in stage.tasks) if stage.tasks else 0

        for attempt in range(max_retries + 1):
            self._semaphore = self._semaphore or asyncio.Semaphore(self._max_concurrency)

            if stage.parallel:
                result = await self._execute_parallel(stage, attempt)
            else:
                result = await self._execute_sequential(stage, attempt)

            if result.state == StageState.COMPLETED or attempt >= max_retries:
                stage.state = result.state
                return result

            # Retry
            delay = 1.0 * (2 ** attempt)
            log.warning("Stage '%s': retrying (attempt %d/%d) in %.1fs", stage.name, attempt + 1, max_retries, delay)
            await asyncio.sleep(delay)

        # Should not reach here
        return StageResult(
            stage_id=stage.stage_id,
            state=StageState.FAILED,
            error="All retries exhausted",
        )

    async def _execute_sequential(self, stage: PipelineStage, attempt: int) -> StageResult:
        """Execute stage tasks sequentially."""
        outputs = []
        errors = []

        for task in stage.tasks:
            if self._cancel_event.is_set():
                return StageResult(
                    stage_id=stage.stage_id,
                    state=StageState.FAILED,
                    error="Cancelled",
                )

            await self._pause_event.wait()
            try:
                result = await self._run_task(task, attempt)
                outputs.append(result)
            except Exception as e:
                errors.append(str(e))
                if stage.skip_on_failure:
                    return StageResult(
                        stage_id=stage.stage_id,
                        state=StageState.SKIPPED,
                        error=str(e),
                    )
                return StageResult(
                    stage_id=stage.stage_id,
                    state=StageState.FAILED,
                    error=str(e),
                    started_at=time.time(),
                    completed_at=time.time(),
                    attempts=attempt + 1,
                )

        return StageResult(
            stage_id=stage.stage_id,
            state=StageState.COMPLETED,
            output=outputs,
            started_at=time.time() - 1,
            completed_at=time.time(),
            attempts=attempt + 1,
        )

    async def _execute_parallel(self, stage: PipelineStage, attempt: int) -> StageResult:
        """Execute stage tasks in parallel with concurrency control."""
        sem = asyncio.Semaphore(stage.max_concurrency or self._max_concurrency)

        async def _run_wrapped(task: PipelineTask, sem: asyncio.Semaphore) -> Any:
            await self._pause_event.wait()
            async with sem:
                return await self._run_task(task, attempt)

        try:
            results = await asyncio.gather(
                *(_run_wrapped(t, sem) for t in stage.tasks),
                return_exceptions=True,
            )
        except Exception as e:
            return StageResult(
                stage_id=stage.stage_id,
                state=StageState.FAILED,
                error=str(e),
            )

        outputs = []
        errors = []
        for r in results:
            if isinstance(r, Exception):
                errors.append(str(r))
            else:
                outputs.append(r)

        if errors and not stage.skip_on_failure:
            return StageResult(
                stage_id=stage.stage_id,
                state=StageState.FAILED,
                error="; ".join(errors[:3]),
                output=outputs,
                started_at=time.time() - 1,
                completed_at=time.time(),
                attempts=attempt + 1,
            )

        return StageResult(
            stage_id=stage.stage_id,
            state=StageState.COMPLETED,
            output=outputs,
            started_at=time.time() - 1,
            completed_at=time.time(),
            attempts=attempt + 1,
        )

    async def _run_task(self, task: PipelineTask, attempt: int) -> Any:
        """Run a single task with optional timeout."""
        wrapped = task.coro
        if asyncio.iscoroutine(wrapped):
            coro = wrapped
        elif asyncio.iscoroutinefunction(wrapped):
            coro = wrapped()
        elif callable(wrapped):
            # Sync callable that may return an awaitable
            result = wrapped()
            if asyncio.iscoroutine(result) or hasattr(result, '__await__'):
                coro = result
            else:
                return result
        else:
            raise TypeError(f"Task '{task.name}' coro is not callable or awaitable")

        if task.timeout_seconds:
            try:
                return await asyncio.wait_for(coro, timeout=task.timeout_seconds)
            except asyncio.TimeoutError:
                raise TimeoutError(f"Task '{task.name}' timed out after {task.timeout_seconds}s")
        else:
            return await coro

    # ── Lifecycle Control ────────────────────────────────────────────────────

    async def pause(self) -> None:
        """Pause pipeline execution."""
        self._pause_event.clear()
        log.info("Pipeline '%s': paused", self._name)

    async def resume(self) -> None:
        """Resume pipeline execution."""
        self._pause_event.set()
        log.info("Pipeline '%s': resumed", self._name)

    async def cancel(self) -> None:
        """Cancel pipeline execution."""
        self._cancel_event.set()
        # Cancel the running task if known — this propagates CancelledError
        # through the execution chain, interrupting any running tasks.
        if self._current_run_task and not self._current_run_task.done():
            self._current_run_task.cancel()
        log.info("Pipeline '%s': cancelled", self._name)

    # ── Results / Progress ──────────────────────────────────────────────────

    @property
    def state(self) -> PipelineState:
        return self._state

    def get_result(self, stage_id: str) -> Optional[StageResult]:
        """Get the result for a specific stage."""
        return self._results.get(stage_id)

    def get_results(self) -> Dict[str, StageResult]:
        """Get all stage results."""
        return dict(self._results)

    def get_progress(self) -> Dict[str, Any]:
        """Get a progress report for the pipeline."""
        total = len(self._stage_order)
        completed = sum(
            1 for r in self._results.values()
            if r.state in (StageState.COMPLETED, StageState.SKIPPED)
        )
        failed = sum(
            1 for r in self._results.values()
            if r.state == StageState.FAILED
        )
        running = sum(
            1 for s in self._stages.values()
            if s.state == StageState.RUNNING
        )

        return {
            "pipeline": self._name,
            "state": self._state.value,
            "total_stages": total,
            "completed_stages": completed,
            "failed_stages": failed,
            "running_stages": running,
            "progress_pct": round(completed / total * 100, 1) if total > 0 else 0.0,
            "stages": [
                {
                    "id": sid,
                    "name": self._stages[sid].name,
                    "state": self._stages[sid].state.value,
                    "tasks": len(self._stages[sid].tasks),
                    "parallel": self._stages[sid].parallel,
                }
                for sid in self._stage_order
            ],
        }

    def _notify_progress(self) -> None:
        """Notify progress callback if registered."""
        if self._progress_callback:
            try:
                self._progress_callback(self.get_progress())
            except Exception as e:
                log.warning("Progress callback failed: %s", e)

    def _generate_id(self, prefix: str, content: str) -> str:
        raw = f"{prefix}_{content}_{time.time()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]


class PipelineBuilder:
    """Builder for constructing AsyncPipeline instances with a fluent API."""

    def __init__(self, name: str = "pipeline"):
        self._pipeline = AsyncPipeline(name=name)
        self._current_stage: Optional[PipelineStage] = None

    def with_max_concurrency(self, n: int) -> "PipelineBuilder":
        self._pipeline._max_concurrency = n
        return self

    def with_progress_callback(
        self, callback: Callable[[Dict[str, Any]], None]
    ) -> "PipelineBuilder":
        self._pipeline._progress_callback = callback
        return self

    def add_stage(
        self,
        stage_id: str,
        name: str,
        parallel: bool = False,
        max_concurrency: int = 0,
        dependencies: Optional[List[str]] = None,
        skip_on_failure: bool = False,
    ) -> "PipelineBuilder":
        """Add a new stage to the pipeline."""
        stage = PipelineStage(
            stage_id=stage_id,
            name=name,
            tasks=[],
            parallel=parallel,
            max_concurrency=max_concurrency,
            dependencies=dependencies or [],
            skip_on_failure=skip_on_failure,
        )
        self._pipeline.add_stage(stage)
        self._current_stage = stage
        return self

    def add_task(
        self,
        name: str,
        coro: Any,
        priority: StagePriority = StagePriority.NORMAL,
        timeout_seconds: Optional[float] = None,
        max_retries: int = 0,
    ) -> "PipelineBuilder":
        """Add a task to the current stage."""
        if self._current_stage is None:
            raise RuntimeError("No stage added yet — call add_stage first")
        task_id = hashlib.sha256(f"{name}_{time.time()}".encode()).hexdigest()[:12]
        task = PipelineTask(
            task_id=task_id,
            name=name,
            coro=coro,
            priority=priority,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )
        self._current_stage.tasks.append(task)
        return self

    def build(self) -> AsyncPipeline:
        """Build and return the pipeline."""
        return self._pipeline
