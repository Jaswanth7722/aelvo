"""Tests for deterministic background-task shutdown (the /exit hang fix).

``asyncio.run`` cancels every pending task at teardown and *waits* for it — if
a background health monitor, subprocess transport or event-bus task refuses to
finalize, the process hangs (10s–4min stalls after ``/exit``, worst after a
live ``/models`` fetch). These tests cover the ``shutdown_background_tasks``
helper and the components it relies on.
"""

import asyncio

from auth.monitoring.health import HealthCheckPolicy, HealthMonitor
from runtime_next.capability.registry import CapabilityRegistry
from runtime_next.events.bus import EventBus
from runtime_next.models.capability import ToolStatus


# ── main.shutdown_background_tasks ──────────────────────────────────────────

async def _pending_task():
    return await asyncio.sleep(3600)


class _FakeProviderRuntime:
    """Records whether stop_monitoring was awaited."""

    def __init__(self):
        self.stopped = False

    async def stop_monitoring(self):
        self.stopped = True


class _FakeRegistry:
    def __init__(self):
        self.stopped = False

    async def stop_monitoring(self):
        self.stopped = True


class _FakeBus:
    def __init__(self):
        self.stopped = False

    async def stop(self):
        self.stopped = True


class _FakeExecutor:
    def __init__(self):
        self.shutdown_calls = []

    def shutdown(self, wait=False):
        self.shutdown_calls.append(wait)


class _FakeMemoryEngine:
    def __init__(self):
        self._executor = _FakeExecutor()


class _FakeOrchestrator:
    def __init__(self):
        self._bus_task = None
        self._registry_task = None
        self.runtime_registry = _FakeRegistry()
        self.runtime_bus = _FakeBus()


def test_shutdown_background_tasks_stops_everything():
    from main import shutdown_background_tasks

    pr = _FakeProviderRuntime()
    orch = _FakeOrchestrator()
    me = _FakeMemoryEngine()

    asyncio.run(shutdown_background_tasks(orchestrator=orch, provider_runtime=pr, memory_engine=me))

    assert pr.stopped is True
    assert orch.runtime_registry.stopped is True
    assert orch.runtime_bus.stopped is True
    assert me._executor.shutdown_calls == [False]


def test_shutdown_background_tasks_cancels_pending_orchestrator_tasks():
    from main import shutdown_background_tasks

    async def _scenario():
        orch = _FakeOrchestrator()
        orch._bus_task = asyncio.create_task(_pending_task())
        await asyncio.sleep(0)  # let the task start
        await shutdown_background_tasks(orchestrator=orch)
        await asyncio.sleep(0)  # let the loop finalize the cancelled task
        assert orch._bus_task.cancelled()

    asyncio.run(_scenario())


def test_shutdown_background_tasks_never_raises_on_failures():
    from main import shutdown_background_tasks

    class _Exploding:
        async def stop_monitoring(self):
            raise RuntimeError("boom")

        async def stop(self):
            raise RuntimeError("boom")

    class _ExplodingOrchestrator:
        runtime_registry = _Exploding()
        runtime_bus = _Exploding()
        _bus_task = _Exploding()  # no .done() -> guarded by getattr+done()

    class _ExplodingEngine:
        @property
        def _executor(self):
            raise RuntimeError("boom")

    # Must not raise even though every component explodes.
    asyncio.run(
        shutdown_background_tasks(
            orchestrator=_ExplodingOrchestrator(),
            provider_runtime=_Exploding(),
            memory_engine=_ExplodingEngine(),
            timeout=1.0,
        )
    )


# ── HealthMonitor.stop ───────────────────────────────────────────────────────

def test_health_monitor_stop_cancels_check_loops():
    async def _fake_check():
        return True

    async def _scenario():
        hm = HealthMonitor()
        hm.register_policy(
            HealthCheckPolicy(
                provider_id="fake",
                check_interval=60.0,
                enabled=True,
                check_fn=_fake_check,
            )
        )
        await hm.start()
        assert hm.summary()["is_running"] is True
        assert hm._check_tasks
        await hm.stop()
        assert hm.summary()["is_running"] is False
        assert hm._check_tasks == {}
        return True

    assert asyncio.run(_scenario())


# ── HealthCheckRunner.connectivity_check ────────────────────────────────────

def test_connectivity_check_returns_bool_without_hanging():
    """The probe runs on a worker thread (sync httpx) — an unreachable port
    must resolve to False quickly and never leak a Proactor socket connect."""
    from auth.diagnostics.health_checks import HealthCheckRunner

    async def _run():
        runner = HealthCheckRunner()
        ok = await runner.connectivity_check("http://127.0.0.1:1/", timeout=3.0)
        return ok

    assert asyncio.run(_run()) is False


# ── CapabilityRegistry._check_tools (thread-based probes) ───────────────────

def test_check_tools_thread_based(tmp_path):
    """_check_tools must return status dicts (MISSING for unknown tools,
    AVAILABLE for real ones) and complete quickly — no Proactor subprocess
    transport that could hang teardown."""

    async def _run():
        reg = CapabilityRegistry(workspace_root=str(tmp_path), event_bus=EventBus())
        res = await reg._check_tools(["definitely_not_a_real_tool_xyz_12345"])
        assert res["definitely_not_a_real_tool_xyz_12345"]["status"] == ToolStatus.MISSING.value
        py = await reg._check_tools(["python"])
        assert py["python"]["status"] == ToolStatus.AVAILABLE.value
        assert py["python"]["version"]
        return True

    assert asyncio.run(_run())


# ── EventBus.stop stays bounded ─────────────────────────────────────────────

def test_event_bus_start_stop_roundtrip():
    async def _scenario():
        bus = EventBus()
        await bus.start()
        await bus.stop()
        assert bus._is_running is False
        return True

    assert asyncio.run(_scenario())
