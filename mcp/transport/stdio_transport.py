"""StdioTransport — manages subprocess-based MCP server communication.

Handles subprocess lifecycle (spawn, supervise, teardown), stdin/stdout
message passing, stderr capture for diagnostics, and unexpected exit detection.
"""

from __future__ import annotations

import asyncio
import json
import logging
import shlex
from typing import Any, AsyncIterator, Dict, List, Optional

from .base_transport import BaseTransport, MCPMessage

log = logging.getLogger("aelvo.mcp.transport.stdio")


class StdioTransport(BaseTransport):
    """Transport that communicates with an MCP server via subprocess stdio.

    The server process is spawned on connect() and its stdin/stdout
    are used for JSON-RPC message exchange. Stderr is captured for diagnostics.
    """

    def __init__(
        self,
        command: List[str],
        env: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
    ):
        self._command = command
        self._env = env
        self._cwd = cwd
        self._process: Optional[asyncio.subprocess.Process] = None
        self._connected = False
        self._read_buffer: asyncio.Queue = asyncio.Queue()
        self._reader_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._stderr_lines: List[str] = []

    async def connect(self) -> None:
        """Spawn the subprocess and prepare I/O streams."""
        if self._connected:
            return

        log.info("StdioTransport: spawning %s", " ".join(self._command))

        try:
            self._process = await asyncio.create_subprocess_exec(
                *self._command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=self._env,
                cwd=self._cwd,
            )
        except FileNotFoundError as e:
            raise MCPConnectionError(f"Command not found: {self._command[0]}") from e
        except Exception as e:
            raise MCPConnectionError(f"Failed to spawn process: {e}") from e

        self._connected = True
        self._reader_task = asyncio.create_task(self._read_stdout())
        self._stderr_task = asyncio.create_task(self._read_stderr())

        log.info("StdioTransport: connected (pid=%s)", self._process.pid)

    async def disconnect(self) -> None:
        """Gracefully terminate the subprocess."""
        if not self._connected:
            return

        log.info("StdioTransport: disconnecting")

        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            self._reader_task = None

        if self._stderr_task:
            self._stderr_task.cancel()
            try:
                await self._stderr_task
            except asyncio.CancelledError:
                pass
            self._stderr_task = None

        if self._process and self._process.stdin:
            self._process.stdin.close()

        if self._process:
            try:
                await asyncio.wait_for(self._process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                log.warning("StdioTransport: process did not exit gracefully, killing")
                self._process.kill()
                await self._process.wait()

        self._connected = False
        self._process = None
        log.info("StdioTransport: disconnected")

    async def send(self, message: MCPMessage) -> None:
        """Send a JSON-RPC message via stdin."""
        if not self._connected or not self._process or not self._process.stdin:
            raise MCPConnectionError("Not connected")

        data = message.model_dump(exclude_none=True)
        line = json.dumps(data) + "\n"
        self._process.stdin.write(line.encode("utf-8"))
        await self._process.stdin.drain()

    async def receive(self) -> AsyncIterator[MCPMessage]:
        """Yield parsed JSON-RPC messages from stdout."""
        while self._connected or not self._read_buffer.empty():
            try:
                message = await asyncio.wait_for(self._read_buffer.get(), timeout=1.0)
                yield message
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

    @property
    def is_connected(self) -> bool:
        if not self._connected or not self._process:
            return False
        return self._process.returncode is None

    @property
    def stderr_lines(self) -> List[str]:
        return list(self._stderr_lines)

    @property
    def pid(self) -> Optional[int]:
        return self._process.pid if self._process else None

    # ------------------------------------------------------------------
    # Internal I/O
    # ------------------------------------------------------------------

    async def _read_stdout(self) -> None:
        """Read and parse JSON-RPC messages from stdout."""
        if not self._process or not self._process.stdout:
            return

        try:
            async for raw_line in self._process.stdout:
                line = raw_line.decode("utf-8").strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    message = MCPMessage(**data)
                    await self._read_buffer.put(message)
                except json.JSONDecodeError as e:
                    log.warning("StdioTransport: invalid JSON from stdout: %s", e)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error("StdioTransport: stdout read error: %s", e)

    async def _read_stderr(self) -> None:
        """Capture stderr output for diagnostics."""
        if not self._process or not self._process.stderr:
            return

        try:
            async for raw_line in self._process.stderr:
                line = raw_line.decode("utf-8").strip()
                if line:
                    self._stderr_lines.append(line)
                    log.debug("StdioTransport [stderr]: %s", line)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.debug("StdioTransport: stderr read error: %s", e)


class MCPConnectionError(Exception):
    """Error establishing or maintaining an MCP transport connection."""
    pass
