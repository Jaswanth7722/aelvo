import logging
import asyncio
from typing import Optional

# The Python TUI was removed — events stream to the web via the
# orchestrator's runtime EventBus instead of ui.events.

from runtime_next.models.events import BaseEvent, EventType as RuntimeEventType

log = logging.getLogger("aelvo.ui_notifier")

class UINotifier:
    """Handles UI notification events for the Orchestrator.

    The Python TUI was removed — notifications are published to the
    orchestrator's runtime EventBus, which the web bridge streams to
    the browser in real time.
    """

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
        except RuntimeError:
            log.debug("No event loop running, skipping UI task completion notification")

    def notify_specialist_activated(self, specialist: str, score: float = 0.0, action: str = ""):
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.orchestrator.runtime_bus.publish(
                BaseEvent(id=f"sa_{specialist}", type=RuntimeEventType.LOG_MESSAGE,
                          payload={"msg": f"Specialist activated: {specialist} (score: {score:.2f})", "specialist": specialist})
            ))
        except RuntimeError:
            log.debug("No event loop running, skipping specialist activation notification")

    def notify_specialist_thinking(self, specialist: str, action: str = ""):
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.orchestrator.runtime_bus.publish(
                BaseEvent(id=f"st_{specialist}", type=RuntimeEventType.LOG_MESSAGE,
                          payload={"msg": f"Specialist thinking: {specialist}", "specialist": specialist})
            ))
        except RuntimeError:
            log.debug("No event loop running, skipping specialist thinking notification")

    def notify_specialist_completed(self, specialist: str, summary: str = "", success: bool = True):
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.orchestrator.runtime_bus.publish(
                BaseEvent(id=f"sc_{specialist}", type=RuntimeEventType.LOG_MESSAGE,
                          payload={"msg": f"Specialist completed: {specialist} — {summary[:100]}", "specialist": specialist, "success": success})
            ))
        except RuntimeError:
            log.debug("No event loop running, skipping specialist completion notification")

    def notify_tool_started(self, tool_name: str, cmd: str):
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.orchestrator.runtime_bus.publish(
                BaseEvent(id=f"tool_start_{tool_name}", type=RuntimeEventType.LOG_MESSAGE,
                          payload={"msg": f"Running tool {tool_name}", "specialist": "TERMINUS", "tool": tool_name, "status": "running"})
            ))
        except RuntimeError:
            log.debug("No event loop running, skipping tool started notification")

    def notify_tool_completed(self, tool_name: str, cmd: str, status: str, exit_code: Optional[int] = None):
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.orchestrator.runtime_bus.publish(
                BaseEvent(id=f"tool_done_{tool_name}", type=RuntimeEventType.LOG_MESSAGE,
                          payload={"msg": f"Completed tool {tool_name} with status {status}", "specialist": "TERMINUS", "tool": tool_name, "status": status, "exit_code": exit_code})
            ))
        except RuntimeError:
            log.debug("No event loop running, skipping tool completed notification")
