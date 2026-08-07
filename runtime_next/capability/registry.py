import asyncio
import os
import shutil
import subprocess
import logging
import tempfile
from typing import Dict, List, Set, Optional, Any, Tuple
from pathlib import Path
from datetime import datetime, timezone

from ..models.capability import CapabilitySnapshot, ToolStatus, EnvironmentHealth, GitState
from ..events.bus import EventBus
from ..models.events import CapabilityEvent

log = logging.getLogger("aelvo.runtime.capability")


class CapabilityRegistry:
    """Ground truth registry for environment capabilities with real OS verification."""

    def __init__(self, workspace_root: str, event_bus: EventBus):
        self.workspace_root = Path(workspace_root).resolve()
        self.event_bus = event_bus
        self._last_snapshot: Optional[CapabilitySnapshot] = None
        self._monitor_task: Optional[asyncio.Task] = None
        self._is_running = False
        self._specialist_cache: List[str] = []
        self._tool_allowlist: List[str] = []

    def set_tool_allowlist(self, tools: List[str]):
        self._tool_allowlist = list(tools)

    async def start_monitoring(self):
        self._is_running = True
        await self.refresh()
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        log.info("Capability monitoring started")

    async def stop_monitoring(self):
        self._is_running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except (asyncio.CancelledError, RuntimeError) as _ex:
                log.warning("Silenced exception: %s", _ex)
        log.info("Capability monitoring stopped")

    async def _monitor_loop(self):
        watcher = None
        try:
            from watchfiles import awatch
            watcher = awatch(str(self.workspace_root))
            async for changes in watcher:
                if not self._is_running:
                    break
                log.info(f"Workspace change detected ({len(changes)} files)")
                await self.refresh()
        except ImportError:
            log.warning("watchfiles not available, periodic refresh only")
            while self._is_running:
                await asyncio.sleep(30)
                await self.refresh()
        except asyncio.CancelledError as _ex:
            # Close the watchfiles thread-watcher deterministically and with a
            # bound: if it is left open, asyncio.run()'s shutdown_asyncgens()
            # awaits its aclose() and a hung watcher thread would stall exit.
            if watcher is not None:
                try:
                    await asyncio.wait_for(watcher.aclose(), timeout=2.0)
                except Exception:
                    pass
            log.warning("Silenced exception: %s", _ex)

    async def refresh(self) -> CapabilitySnapshot:
        Path(os.getcwd()).resolve()

        specialists = self._detect_specialists()
        readable, writable = self._check_files()

        tools_to_check = self._tool_allowlist or ["python", "git", "ruff", "mypy", "pytest", "node", "npm", "tsc", "eslint", "cargo", "go"]
        tools = await self._check_tools(tools_to_check)

        # Run the (sync subprocess) git probe off the loop so it can never
        # block event-loop progress or leak a Proactor child watcher.
        git_state = await asyncio.to_thread(self._check_git)
        mem, disk = self._check_resources()
        permissions = self._check_permissions()
        health = self._classify_health(tools, disk, permissions)

        snapshot = CapabilitySnapshot(
            workspace_path=str(self.workspace_root),
            readable_files=readable,
            writable_files=writable,
            tools=tools,
            git=git_state,
            memory_usage_mb=mem,
            disk_free_gb=disk,
            permissions=permissions,
            health=health,
            metadata={"available_specialists": specialists}
        )

        if self._last_snapshot:
            diff = self.diff(self._last_snapshot, snapshot)
            if diff:
                event = CapabilityEvent(
                    id=f"cap_diff_{int(datetime.now(timezone.utc).timestamp() * 1000)}",
                    diff=diff
                )
                await self.event_bus.publish(event)

        self._last_snapshot = snapshot
        return snapshot

    def _detect_specialists(self) -> List[str]:
        try:
            from specialists import SPECIALIST_REGISTRY
            self._specialist_cache = list(SPECIALIST_REGISTRY.keys())
        except ImportError as _ex:
            log.warning("Silenced exception: %s", _ex)
        return self._specialist_cache

    def _check_files(self) -> Tuple[Set[str], Set[str]]:
        readable: Set[str] = set()
        writable: Set[str] = set()
        ignored = {".git", "__pycache__", "node_modules", ".venv", "venv", "chroma_db", "backups", ".mypy_cache", ".pytest_cache", ".ruff_cache"}
        try:
            for root, dirs, files in os.walk(self.workspace_root):
                dirs[:] = [d for d in dirs if d not in ignored]
                for f in files:
                    full_path = os.path.join(root, f)
                    rel_path = os.path.relpath(full_path, self.workspace_root)
                    if os.access(full_path, os.R_OK):
                        readable.add(rel_path)
                    if os.access(full_path, os.W_OK):
                        writable.add(rel_path)
        except Exception as e:
            log.error(f"File walk error: {e}")
        return readable, writable

    async def _check_tools(self, tool_names: List[str]) -> Dict[str, Dict[str, Any]]:
        """Probe tool availability via ``--version``.

        Probes run on the default executor with a hard ``subprocess.run``
        timeout instead of ``asyncio.create_subprocess_exec``: the Proactor
        event loop's subprocess transports can hang forever on cancellation
        (a stuck child process leaves ``_connect_pipes`` pending and stalls
        ``asyncio.run``'s ``_cancel_all_tasks`` teardown). A thread-based
        probe is bounded by a real timeout and always finishes.
        """
        import subprocess as _sp

        def _probe(name: str, path: str) -> Dict[str, Any]:
            try:
                proc = _sp.run(
                    [path, "--version"],
                    capture_output=True,
                    text=True,
                    timeout=5.0,
                )
                output = (proc.stdout or proc.stderr or "").strip()
                if proc.returncode == 0:
                    version = output.splitlines()[0] if output else "unknown"
                    return {"status": ToolStatus.AVAILABLE.value, "version": version, "path": path}
                return {
                    "status": ToolStatus.BROKEN.value,
                    "version": None,
                    "path": path,
                    "error": output[:200],
                }
            except _sp.TimeoutExpired:
                return {"status": ToolStatus.BROKEN.value, "version": None, "path": path, "error": "timeout"}
            except Exception as e:
                return {"status": ToolStatus.BROKEN.value, "version": None, "path": path, "error": str(e)[:200]}

        results: Dict[str, Dict[str, Any]] = {}
        for name in tool_names:
            path = shutil.which(name)
            if not path:
                results[name] = {"status": ToolStatus.MISSING.value, "version": None, "path": None}
                continue
            results[name] = await asyncio.to_thread(_probe, name, path)
        return results

    def _check_git(self) -> Optional[GitState]:
        if not shutil.which("git"):
            return None
        try:
            subprocess.check_output(["git", "rev-parse", "--is-inside-work-tree"], cwd=self.workspace_root, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError:
            return None

        def run_git(args: List[str]) -> str:
            try:
                return subprocess.check_output(["git"] + args, cwd=self.workspace_root, stderr=subprocess.DEVNULL).decode().strip()
            except subprocess.CalledProcessError:
                return ""

        branch = run_git(["rev-parse", "--abbrev-ref", "HEAD"])
        status_raw = run_git(["status", "--porcelain"])
        is_dirty = len(status_raw) > 0
        uncommitted = len([l for l in status_raw.splitlines() if l]) if status_raw else 0

        conflicts = False
        if status_raw:
            conflicts = any(l.startswith(("UU", "AA", "DD", "AU", "UA", "DU", "UD")) for l in status_raw.splitlines())

        stash_out = run_git(["stash", "list"])
        stash_count = len([l for l in stash_out.splitlines() if l]) if stash_out else 0

        remotes = run_git(["remote"])
        remote_configured = len(remotes) > 0

        commits_raw = run_git(["log", "-n", "5", "--oneline"])
        commits = [c for c in commits_raw.splitlines() if c] if commits_raw else []

        return GitState(
            branch=branch or "unknown",
            is_dirty=is_dirty,
            uncommitted_count=uncommitted,
            has_conflicts=conflicts,
            stash_count=stash_count,
            remote_configured=remote_configured,
            last_commits=commits
        )

    def _check_resources(self) -> Tuple[float, float]:
        mem = 0.0
        disk = 0.0
        try:
            import psutil
            mem = psutil.Process().memory_info().rss / (1024 * 1024)
            disk = shutil.disk_usage(self.workspace_root).free / (1024 * 1024 * 1024)
        except ImportError:
            try:
                disk = shutil.disk_usage(self.workspace_root).free / (1024 * 1024 * 1024)
            except Exception as _ex: log.warning("Silenced exception: %s", _ex)
        return mem, disk

    def _check_permissions(self) -> Dict[str, Any]:
        perms: Dict[str, Any] = {
            "can_write_workspace": os.access(self.workspace_root, os.W_OK),
            "can_read_workspace": os.access(self.workspace_root, os.R_OK),
        }
        try:
            fd, tmp_path = tempfile.mkstemp(prefix="aelvo_perm_test_", suffix=".tmp")
            try:
                os.write(fd, b"test")
                perms["can_write_temp"] = True
            finally:
                os.close(fd)
                os.unlink(tmp_path)
        except Exception:
            perms["can_write_temp"] = False
        return perms

    def _classify_health(self, tools: Dict, disk_gb: float, permissions: Dict) -> EnvironmentHealth:
        if not permissions.get("can_write_workspace", False):
            return EnvironmentHealth.RESTRICTED
        if disk_gb < 0.1:
            return EnvironmentHealth.DEGRADED
        python_status = tools.get("python", {}).get("status", ToolStatus.MISSING.value)
        git_status = tools.get("git", {}).get("status", ToolStatus.MISSING.value)
        if python_status != ToolStatus.AVAILABLE.value and git_status != ToolStatus.AVAILABLE.value:
            return EnvironmentHealth.OFFLINE
        if python_status != ToolStatus.AVAILABLE.value and disk_gb < 1.0:
            return EnvironmentHealth.DEGRADED
        return EnvironmentHealth.FULLY_OPERATIONAL

    def diff(self, old: CapabilitySnapshot, new: CapabilitySnapshot) -> Dict[str, Any]:
        changes: Dict[str, Any] = {}
        if old.health != new.health:
            changes["health"] = {"from": old.health.value, "to": new.health.value}
        for tool, status in new.tools.items():
            old_status = old.tools.get(tool, {})
            if old_status.get("status") != status.get("status"):
                changes.setdefault("tools", {})[tool] = {"from": old_status.get("status"), "to": status.get("status")}
        added = new.readable_files - old.readable_files
        removed = old.readable_files - new.readable_files
        if added:
            changes["files_added"] = min(len(added), 100)
        if removed:
            changes["files_removed"] = min(len(removed), 100)
        if abs(new.disk_free_gb - old.disk_free_gb) > 0.5:
            changes["disk_free_gb"] = {"from": round(old.disk_free_gb, 2), "to": round(new.disk_free_gb, 2)}
        return changes

    def to_prompt_injection(self) -> str:
        if not self._last_snapshot:
            return "[CAPABILITY SNAPSHOT] Not available."
        s = self._last_snapshot
        lines = [
            "[CAPABILITY SNAPSHOT]",
            f"HEALTH: {s.health.value}",
            f"WORKSPACE: {s.workspace_path}",
            f"DISK_FREE_GB: {s.disk_free_gb:.1f}",
            f"MEMORY_USAGE_MB: {s.memory_usage_mb:.0f}",
            "TOOLS:"
        ]
        for name, info in s.tools.items():
            if info.get("status") == ToolStatus.AVAILABLE.value:
                version = info.get("version", "?")
                lines.append(f"  {name} ({version})")
            else:
                lines.append(f"  {name}: {info.get('status', 'unknown')}")

        if s.git:
            g = s.git
            lines.append(f"GIT: branch={g.branch} dirty={g.is_dirty} uncommitted={g.uncommitted_count} conflicts={g.has_conflicts}")

        if s.metadata.get("available_specialists"):
            lines.append(f"SPECIALISTS: {', '.join(s.metadata['available_specialists'])}")

        return "\n".join(lines)

    @property
    def last_snapshot(self) -> Optional[CapabilitySnapshot]:
        return self._last_snapshot
