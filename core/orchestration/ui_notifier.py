import logging
import asyncio
from typing import Optional

try:
    from ui.events import create_task_event, create_specialist_event, EventType as UIEventType
    UI_AVAILABLE = True
except ImportError:
    UI_AVAILABLE = False

from runtime_next.models.events import BaseEvent, EventType as RuntimeEventType

log = logging.getLogger("aelvo.ui_notifier")

class UINotifier:
    """Handles UI notification events for the Orchestrator."""

    def __init__(self, orchestrator):
        self.orchestrator = orchestrator

    def notify_task_created(self, task_id: str, task_name: str, specialist: str):
        if self.orchestrator.ui_panel:
            self.orchestrator.ui_panel.on_task_created(task_id, task_name, specialist)
        
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.orchestrator.runtime_bus.publish(
                BaseEvent(id=task_id, type=RuntimeEventType.LOG_MESSAGE, payload={"msg": f"Task created: {task_name}", "specialist": specialist})
            ))
            if self.orchestrator.event_bus:
                loop.create_task(self.orchestrator.event_bus.publish(
                    create_task_event(UIEventType.TASK_CREATED, task_id, task_name, specialist, "pending")
                ))
        except RuntimeError:
            log.debug("No event loop running, skipping UI task creation notification")

    def notify_task_completed(self, task_id: str, success: bool):
        if self.orchestrator.ui_panel:
            self.orchestrator.ui_panel.on_task_completed(task_id, success)

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.orchestrator.runtime_bus.publish(
                BaseEvent(id=f"comp_{task_id}", type=RuntimeEventType.LOG_MESSAGE, payload={"msg": f"Task completed: {task_id}", "success": success})
            ))
            if self.orchestrator.event_bus:
                event_type = UIEventType.TASK_COMPLETED if success else UIEventType.TASK_FAILED
                loop.create_task(self.orchestrator.event_bus.publish(
                    create_task_event(event_type, task_id, "task", "specialist", "completed" if success else "failed")
                ))
        except RuntimeError:
            log.debug("No event loop running, skipping UI task completion notification")

    def notify_specialist_activated(self, specialist: str, score: float = 0.0, action: str = ""):
        try:
            loop = asyncio.get_running_loop()
            if self.orchestrator.event_bus:
                loop.create_task(self.orchestrator.event_bus.publish(
                    create_specialist_event(UIEventType.SPECIALIST_ACTIVATED, specialist, action, {"score": score})
                ))
            loop.create_task(self.orchestrator.runtime_bus.publish(
                BaseEvent(id=f"sa_{specialist}", type=RuntimeEventType.LOG_MESSAGE,
                          payload={"msg": f"Specialist activated: {specialist} (score: {score:.2f})", "specialist": specialist})
            ))
        except RuntimeError:
            log.debug("No event loop running, skipping specialist activation notification")

    def notify_specialist_thinking(self, specialist: str, action: str = ""):
        try:
            loop = asyncio.get_running_loop()
            if self.orchestrator.event_bus:
                loop.create_task(self.orchestrator.event_bus.publish(
                    create_specialist_event(UIEventType.SPECIALIST_THINKING, specialist, action)
                ))
            loop.create_task(self.orchestrator.runtime_bus.publish(
                BaseEvent(id=f"st_{specialist}", type=RuntimeEventType.LOG_MESSAGE,
                          payload={"msg": f"Specialist thinking: {specialist}", "specialist": specialist})
            ))
        except RuntimeError:
            log.debug("No event loop running, skipping specialist thinking notification")

    def notify_specialist_completed(self, specialist: str, summary: str = "", success: bool = True):
        try:
            loop = asyncio.get_running_loop()
            if self.orchestrator.event_bus:
                loop.create_task(self.orchestrator.event_bus.publish(
                    create_specialist_event(
                        UIEventType.SPECIALIST_ACTION if success else UIEventType.SPECIALIST_DEACTIVATED,
                        specialist, summary[:100],
                        {"success": success}
                    )
                ))
        except RuntimeError:
            log.debug("No event loop running, skipping specialist completion notification")

    def notify_tool_started(self, tool_name: str, cmd: str):
        try:
            loop = asyncio.get_running_loop()
            if self.orchestrator.event_bus:
                loop.create_task(self.orchestrator.event_bus.publish(
                    create_specialist_event(UIEventType.SPECIALIST_ACTION, "TERMINUS", f"Running tool {tool_name}")
                ))
        except RuntimeError as _ex:
            log.warning("Silenced exception: %s", _ex)

    def notify_tool_completed(self, tool_name: str, cmd: str, status: str, exit_code: Optional[int] = None):
        try:
            loop = asyncio.get_running_loop()
            if self.orchestrator.event_bus:
                loop.create_task(self.orchestrator.event_bus.publish(
                    create_specialist_event(UIEventType.SPECIALIST_ACTION, "TERMINUS", f"Completed tool {tool_name} with status {status}")
                ))
        except RuntimeError as _ex:
            log.warning("Silenced exception: %s", _ex)
