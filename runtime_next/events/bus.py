import asyncio
import json
import logging
from typing import Dict, List, Callable, Awaitable, Optional
from pathlib import Path

from ..models.events import BaseEvent, EventType

log = logging.getLogger("aelvo.runtime.events")


class EventBus:
    """Async typed event bus with replayable logging and subscriber management."""

    def __init__(self, log_path: Optional[str] = None):
        self._subscribers: Dict[EventType, List[Callable[[BaseEvent], Awaitable[None]]]] = {
            t: [] for t in EventType
        }
        self._global_subscribers: List[Callable[[BaseEvent], Awaitable[None]]] = []
        self._queue: asyncio.Queue = asyncio.Queue()
        self._log_path = Path(log_path) if log_path else None
        self._is_running = False
        self._process_task: Optional[asyncio.Task] = None
        self._replayed_count = 0

    async def start(self):
        self._is_running = True
        self._process_task = asyncio.create_task(self._process_events())
        log.info("EventBus started")

    async def stop(self):
        self._is_running = False
        await self._queue.put(None)
        if self._process_task:
            try:
                await asyncio.wait_for(self._process_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError) as _ex:
                log.warning("Silenced exception: %s", _ex)
        log.info("EventBus stopped")

    def subscribe(self, event_type: EventType, callback: Callable[[BaseEvent], Awaitable[None]]):
        self._subscribers.setdefault(event_type, []).append(callback)

    def subscribe_all(self, callback: Callable[[BaseEvent], Awaitable[None]]):
        self._global_subscribers.append(callback)

    async def publish(self, event: BaseEvent):
        await self._queue.put(event)
        if self._log_path:
            self._log_event(event)

    def _log_event(self, event: BaseEvent):
        try:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._log_path, "a", encoding="utf-8") as f:
                f.write(event.model_dump_json() + "\n")
        except Exception as e:
            log.error(f"Failed to log event: {e}")

    async def _process_events(self):
        while self._is_running or not self._queue.empty():
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                if event is None:
                    self._queue.task_done()
                    break

                for callback in self._global_subscribers:
                    try:
                        await callback(event)
                    except Exception as e:
                        log.error(f"Global subscriber error: {e}")

                for callback in self._subscribers.get(event.type, []):
                    try:
                        await callback(event)
                    except Exception as e:
                        log.error(f"Subscriber error ({event.type}): {e}")

                self._queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                log.error(f"EventBus internal error: {e}")

    async def replay(self, log_file: str, callback: Callable[[BaseEvent], Awaitable[None]]):
        path = Path(log_file)
        if not path.exists():
            log.warning(f"Replay file not found: {log_file}")
            return

        from ..models.events import (
            NodeTransitionEvent, CapabilityEvent, RecoveryEvent, GraphEvent,
            ArchitectDecisionEvent, ModeSelectionEvent,
            TaskBoardTransitionEvent, ConsensusEvent,
            BlackboardPublicationEvent,
            FindingConsumedEvent, ChallengeRaisedEvent,
            ReportGeneratedEvent, ExecutionStartedEvent,
            ExecutionCompletedEvent,
        )

        type_map = {
            EventType.NODE_TRANSITION: NodeTransitionEvent,
            EventType.CAPABILITY_CHANGED: CapabilityEvent,
            EventType.RECOVERY_INITIATED: RecoveryEvent,
            EventType.GRAPH_STARTED: GraphEvent,
            EventType.GRAPH_COMPLETED: GraphEvent,
            EventType.ARCHITECT_DECISION: ArchitectDecisionEvent,
            EventType.MODE_SELECTED: ModeSelectionEvent,
            EventType.TASK_BOARD_TRANSITION: TaskBoardTransitionEvent,
            EventType.CONSENSUS_FORMED: ConsensusEvent,
            EventType.BLACKBOARD_PUBLICATION: BlackboardPublicationEvent,
            EventType.FINDING_CONSUMED: FindingConsumedEvent,
            EventType.CHALLENGE_RAISED: ChallengeRaisedEvent,
            EventType.REPORT_GENERATED: ReportGeneratedEvent,
            EventType.EXECUTION_STARTED: ExecutionStartedEvent,
            EventType.EXECUTION_COMPLETED: ExecutionCompletedEvent,
        }

        count = 0
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    etype_str = data.get("type", "")
                    try:
                        etype = EventType(etype_str)
                    except ValueError:
                        etype = EventType.LOG_MESSAGE
                    model_cls = type_map.get(etype, BaseEvent)
                    event = model_cls(**data)
                    await callback(event)
                    count += 1
                except Exception as e:
                    log.error(f"Replay error: {e}")
        self._replayed_count = count
        log.info(f"Replayed {count} events from {log_file}")

    @property
    def replayed_count(self) -> int:
        return self._replayed_count

    @property
    def event_count(self) -> int:
        return self._queue.qsize()
