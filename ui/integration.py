"""
TUI Integration Adapter
=======================
Connects AELVO's core orchestrator loop to the Textual TUI.
"""

import io
import json
import logging
import os
import traceback
from contextlib import redirect_stderr, redirect_stdout
from typing import Callable, Optional

from core.provider_runtime import format_provider_table
from ui.events import EventType, get_event_bus
from ui.events.event_factory import (
    create_collaboration_event,
    create_memory_event,
    create_safety_event,
    create_specialist_event,
    create_system_event,
    create_task_event,
    create_tool_event,
    create_verification_event,
)

log = logging.getLogger("aelvo.ui.integration")


HELP_TEXT = """AELVO commands

#help                         Show this command list
#mcp                          Show MCP command help
#mcp list                     List registered MCP servers
#mcp health                   Show MCP health summary
#status                       Runtime monitor summary
#providers                    List configured model providers
#checkpoint <name>            Save current state
#restore <snapshot_id>        Restore a checkpoint
#lock <target> <value>        Lock an anchor constraint
#update_anchor <target> <v>   Stage an anchor update
#confirm                      Apply staged update

Type natural language for normal AELVO tasks.
"""


class TUISession:
    def __init__(self):
        self.bus = get_event_bus()

    async def emit_system(self, message: str) -> None:
        await self.bus.publish(create_system_event(EventType.SYSTEM_STARTUP, message))

    async def emit_task(
        self,
        etype: EventType,
        task_id: str,
        name: str,
        specialist: str,
        status: str,
        progress: float = 0.0,
    ) -> None:
        await self.bus.publish(create_task_event(etype, task_id, name, specialist, status, progress))

    async def emit_specialist(self, etype: EventType, name: str, action: str) -> None:
        await self.bus.publish(create_specialist_event(etype, name, action))

    async def emit_tool(
        self,
        etype: EventType,
        tool: str,
        cmd: str,
        status: str = "running",
        exit_code: int = None,
        duration: float = None,
    ) -> None:
        await self.bus.publish(create_tool_event(etype, tool, cmd, status, exit_code=exit_code, duration=duration))

    async def emit_memory(
        self,
        etype: EventType,
        mem_type: str,
        query: str = "",
        count: int = 0,
        score: float = 0.0,
    ) -> None:
        await self.bus.publish(create_memory_event(etype, mem_type, query, count, score))

    async def emit_verification(
        self,
        etype: EventType,
        vtype: str,
        target: str,
        status: str,
        confidence: float = 0.0,
    ) -> None:
        await self.bus.publish(create_verification_event(etype, vtype, target, status, confidence))

    async def emit_safety(
        self,
        etype: EventType,
        action: str,
        risk: str,
        reason: str,
        requires_approval: bool = False,
    ) -> None:
        await self.bus.publish(create_safety_event(etype, action, risk, reason, requires_approval))

    async def emit_collaboration(
        self,
        etype: EventType,
        specialist: str,
        action: str,
        details: Optional[dict] = None,
    ) -> None:
        await self.bus.publish(create_collaboration_event(etype, specialist, action, details))

    async def emit_collaboration_finding(
        self,
        specialist: str,
        summary: str,
        entry_type: str = "finding",
        confidence: float = 0.0,
    ) -> None:
        await self.bus.publish(create_collaboration_event(
            EventType.COLLABORATION_FINDING,
            specialist,
            summary,
            {"entry_type": entry_type, "confidence": confidence},
        ))

    async def emit_collaboration_consumed(
        self,
        consumer: str,
        entry_id: str,
        entry_owner: str,
        entry_type: str = "finding",
    ) -> None:
        await self.bus.publish(create_collaboration_event(
            EventType.COLLABORATION_CONSUMED,
            consumer,
            f"Consumed {entry_type} from {entry_owner}",
            {"entry_id": entry_id, "entry_owner": entry_owner, "entry_type": entry_type},
        ))

    async def emit_collaboration_challenge(self, challenger: str, entry_id: str, reason: str) -> None:
        await self.bus.publish(create_collaboration_event(
            EventType.COLLABORATION_CHALLENGE,
            challenger,
            f"Challenge raised: {reason}",
            {"entry_id": entry_id, "reason": reason},
        ))

    async def emit_collaboration_consensus(
        self,
        topic: str,
        outcome: str,
        confidence: float,
        participants: list,
    ) -> None:
        await self.bus.publish(create_collaboration_event(
            EventType.COLLABORATION_CONSENSUS,
            "CONSENSUS",
            outcome,
            {"topic": topic, "confidence": confidence, "participants": participants},
        ))

    async def emit_collaboration_decision(
        self,
        specialist: str,
        outcome: str,
        reason: str,
        target_id: str = "",
    ) -> None:
        await self.bus.publish(create_collaboration_event(
            EventType.COLLABORATION_DECISION,
            specialist,
            f"Decision: {outcome}",
            {"outcome": outcome, "reason": reason, "target_id": target_id},
        ))

    async def emit_collaboration_execution_start(
        self,
        task_id: str,
        command: str,
        specialist: str = "TERMINUS",
    ) -> None:
        await self.bus.publish(create_collaboration_event(
            EventType.COLLABORATION_EXECUTION_START,
            specialist,
            f"Executing: {command[:50]}",
            {"task_id": task_id, "command": command},
        ))

    async def emit_collaboration_execution_end(
        self,
        task_id: str,
        exit_code: int,
        specialist: str = "TERMINUS",
    ) -> None:
        status = "success" if exit_code == 0 else "failed"
        await self.bus.publish(create_collaboration_event(
            EventType.COLLABORATION_EXECUTION_END,
            specialist,
            f"Execution {status} (exit={exit_code})",
            {"task_id": task_id, "exit_code": exit_code, "status": status},
        ))

    async def emit_collaboration_report(
        self,
        report_id: str,
        title: str,
        evidence_count: int = 0,
        challenge_count: int = 0,
    ) -> None:
        await self.bus.publish(create_collaboration_event(
            EventType.COLLABORATION_REPORT,
            "HERALD",
            f"Report: {title[:40]}",
            {"report_id": report_id, "evidence_count": evidence_count, "challenge_count": challenge_count},
        ))


class ProcessUserResult:
    def __init__(self, answer: str = "", status: str = "success"):
        self.answer = answer
        self.status = status
        self.tools_used = []
        self.files_touched = []


class SessionTracker:
    """Small TUI-local interaction tracker persisted to the workspace DB."""

    def __init__(self):
        self.user_query = ""
        self.tools_used = []
        self.files_touched = []
        self.final_answer = ""
        self.status = "success"

    def record_tool(self, tool_name: str, args: dict, outcome_status: str):
        self.tools_used.append(tool_name)
        if "path" in args and args["path"] not in self.files_touched:
            self.files_touched.append(args["path"])
        if "url" in args:
            url = args["url"][:80]
            if url not in self.files_touched:
                self.files_touched.append(url)
        if outcome_status in ("error", "failed", "rejected"):
            self.status = "partial"

    def record_answer(self, answer: str):
        self.final_answer = answer[:500]

    def save(self, db_path: str):
        if not self.user_query:
            return
        try:
            import sqlite3
            from datetime import datetime

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with sqlite3.connect(db_path) as db:
                db.execute("""
                    CREATE TABLE IF NOT EXISTS sessions (
                        session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        user_query TEXT,
                        tools_used TEXT,
                        files_touched TEXT,
                        final_answer TEXT,
                        status TEXT DEFAULT 'success'
                    )
                """)
                db.execute(
                    """
                    INSERT INTO sessions
                        (timestamp, user_query, tools_used, files_touched, final_answer, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        timestamp,
                        self.user_query[:200],
                        ", ".join(self.tools_used) if self.tools_used else "respond",
                        ", ".join(self.files_touched) if self.files_touched else "",
                        self.final_answer,
                        self.status,
                    ),
                )
        except Exception as exc:
            log.debug("Session save failed: %s", exc)


class _TUILoggingRedirect:
    """Route stream logging to a file while Textual owns the terminal."""

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.root = logging.getLogger()
        self.original_handlers: list[logging.Handler] = []
        self.file_handler: logging.Handler | None = None

    def __enter__(self):
        os.makedirs(os.path.join(self.base_dir, ".aelvo_runtime"), exist_ok=True)
        log_path = os.path.join(self.base_dir, ".aelvo_runtime", "tui.log")
        self.original_handlers = list(self.root.handlers)

        retained = [
            handler
            for handler in self.root.handlers
            if not isinstance(handler, logging.StreamHandler) or isinstance(handler, logging.FileHandler)
        ]
        self.file_handler = logging.FileHandler(log_path, encoding="utf-8")
        self.file_handler.setFormatter(logging.Formatter(
            "%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
        ))
        self.root.handlers = retained + [self.file_handler]
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.file_handler:
            self.file_handler.close()
        self.root.handlers = self.original_handlers
        return False


def _capture_sync(call):
    stdout = io.StringIO()
    stderr = io.StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        result = call()
    return result, stdout.getvalue().strip(), stderr.getvalue().strip()


async def _capture_async(call):
    stdout = io.StringIO()
    stderr = io.StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        result = await call()
    return result, stdout.getvalue().strip(), stderr.getvalue().strip()


def _format_command_result(title: str, result: dict | None, captured: str = "", errors: str = "") -> str:
    result = result or {}
    status = str(result.get("status", "OK")).upper()
    msg = result.get("msg") or result.get("message") or result.get("error") or ""

    lines = [title, f"status: {status.lower()}"]
    if msg:
        lines.append(str(msg))
    if captured:
        lines.extend(["", captured])
    if errors:
        lines.extend(["", "stderr:", errors])

    extra = {
        key: value
        for key, value in result.items()
        if key not in {"status", "msg", "message", "error"} and value not in (None, "", [], {})
    }
    if extra and not captured:
        lines.extend(["", json.dumps(extra, indent=2, default=str)])
    return "\n".join(lines)


async def process_user_input_tui(
    user_input: str,
    agent,
    orchestrator,
    memory_engine,
    aelvo_kernel,
    session_tracker,
    db_path: str,
    tui_session: TUISession,
    mcp_cli=None,
    runtime_cli=None,
    provider_runtime=None,
    stream_callback: Optional[Callable[[str], None]] = None,
) -> ProcessUserResult:
    """Process a single user input in TUI mode, emitting events throughout."""
    result = ProcessUserResult()

    if not user_input:
        return result

    if user_input.lower() in ("exit", "quit", "q"):
        result.status = "exit"
        return result

    session_tracker.user_query = user_input

    if user_input.startswith("#"):
        await _process_kernel_command(
            user_input,
            result,
            aelvo_kernel,
            session_tracker,
            db_path,
            mcp_cli=mcp_cli,
            runtime_cli=runtime_cli,
            provider_runtime=provider_runtime,
        )
        return result

    log.info("Routing through Orchestrator: %s...", user_input[:60])
    await tui_session.emit_task(EventType.TASK_CREATED, "main", user_input[:50], "HERMES", "pending")

    try:
        turn_result = await orchestrator.execute_turn(
            agent,
            user_input,
            session_tracker=session_tracker,
            tui_session=tui_session,
            stream_callback=stream_callback,
            db_path=db_path,
        )
        result.answer = turn_result["output"]
        result.tools_used = turn_result.get("specialists_active", [])
    except Exception as exc:
        log.error("Orchestrator error: %s", exc)
        log.debug(traceback.format_exc())
        result.status = "error"
        result.answer = f"Error: {exc}"
        await tui_session.emit_task(EventType.TASK_FAILED, "main", user_input[:50], "HERMES", "failed")
        await tui_session.emit_system(f"ERROR: {str(exc)[:100]}")

    session_tracker.save(db_path)
    return result


async def _process_kernel_command(
    user_input: str,
    result: ProcessUserResult,
    aelvo_kernel,
    session_tracker,
    db_path: str,
    mcp_cli=None,
    runtime_cli=None,
    provider_runtime=None,
) -> None:
    lower = user_input.lower().strip()
    result.status = "kernel"

    if lower == "#help":
        result.answer = HELP_TEXT
        session_tracker.record_answer(result.answer[:300])
        session_tracker.save(db_path)
        return

    if lower.startswith("#mcp"):
        if not mcp_cli:
            result.answer = "MCP subsystem is not initialized."
        else:
            command = "#mcp help" if lower == "#mcp" else user_input
            mcp_result, stdout, stderr = await _capture_async(lambda: mcp_cli.execute(command))
            result.answer = _format_command_result("MCP", mcp_result, stdout, stderr)
            session_tracker.record_tool(
                "mcp",
                {"command": command[:80]},
                mcp_result.get("status", "success").lower(),
            )
        session_tracker.record_answer(result.answer[:300])
        session_tracker.save(db_path)
        return

    if lower.startswith("#status"):
        if not runtime_cli:
            result.answer = "Runtime monitor is not initialized."
        else:
            status_result, stdout, stderr = _capture_sync(lambda: runtime_cli.execute(user_input))
            result.answer = _format_command_result("STATUS", status_result, stdout, stderr)
            session_tracker.record_tool(
                "status",
                {"command": user_input[:80]},
                status_result.get("status", "success").lower(),
            )
        session_tracker.record_answer(result.answer[:300])
        session_tracker.save(db_path)
        return

    if lower.startswith("#providers"):
        if provider_runtime:
            result.answer = "PROVIDERS\n\n" + format_provider_table(provider_runtime)
        else:
            result.answer = "Provider runtime is not initialized."
        session_tracker.record_answer(result.answer[:300])
        session_tracker.save(db_path)
        return

    if lower.startswith("#doctor") or lower.startswith("#diagnostics") or lower.startswith("#diag"):
        result.answer = "Diagnostics commands are available in CLI mode. Use #providers for TUI provider status."
        session_tracker.record_answer(result.answer[:300])
        session_tracker.save(db_path)
        return

    kernel_result, stdout, stderr = _capture_sync(lambda: aelvo_kernel.parse_and_execute(user_input))
    result.answer = _format_command_result("KERNEL", kernel_result, stdout, stderr)
    session_tracker.record_tool(
        "kernel",
        {"command": user_input[:80]},
        kernel_result.get("status", "success").lower(),
    )
    session_tracker.record_answer(result.answer[:300])
    session_tracker.save(db_path)


async def run_tui(
    agent,
    orchestrator,
    memory_engine,
    aelvo_kernel,
    db_path: str,
    mcp_cli=None,
    runtime_cli=None,
    provider_runtime=None,
) -> None:
    """Launch the AELVO TUI dashboard instead of the raw REPL."""
    from ui.app import AelvoTUI
    from ui.core.bridge import RuntimeToUIBridge

    tui_session = TUISession()
    session_tracker = SessionTracker()

    ui_event_bus = get_event_bus()
    runtime_bridge = RuntimeToUIBridge(
        runtime_bus=orchestrator.runtime_bus,
        ui_event_bus=ui_event_bus,
    )
    await runtime_bridge.start()
    log.info("RuntimeToUIBridge started for TUI session")

    async def on_user_input(text: str, stream_callback: Optional[Callable] = None):
        user_result = await process_user_input_tui(
            text,
            agent,
            orchestrator,
            memory_engine,
            aelvo_kernel,
            session_tracker,
            db_path,
            tui_session,
            mcp_cli=mcp_cli,
            runtime_cli=runtime_cli,
            provider_runtime=provider_runtime,
            stream_callback=stream_callback,
        )
        if user_result.status == "exit":
            quit()
        return user_result

    app = AelvoTUI(user_callback=on_user_input)
    try:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with _TUILoggingRedirect(base_dir):
            await app.run_async()
    finally:
        await runtime_bridge.stop()
        log.info("RuntimeToUIBridge stopped")
