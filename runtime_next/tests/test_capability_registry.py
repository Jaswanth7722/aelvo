import logging
import shutil
import tempfile
from pathlib import Path

import pytest

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("test_capability")


@pytest.fixture
def test_dir():
    d = Path(tempfile.mkdtemp(prefix="aelvo_cap_test_"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def registry(test_dir):
    from runtime_next.events.bus import EventBus
    from runtime_next.capability.registry import CapabilityRegistry
    bus = EventBus()
    reg = CapabilityRegistry(workspace_root=str(test_dir), event_bus=bus)
    return reg, bus


@pytest.mark.asyncio
async def test_refresh_workspace_path(registry, test_dir):
    reg, bus = registry
    snapshot = await reg.refresh()
    assert snapshot.workspace_path == str(test_dir.resolve())


@pytest.mark.asyncio
async def test_file_discovery(registry, test_dir):
    reg, bus = registry
    test_file = test_dir / "test.txt"
    test_file.write_text("hello")
    snapshot = await reg.refresh()
    assert "test.txt" in snapshot.readable_files
    assert "test.txt" in snapshot.writable_files


@pytest.mark.asyncio
async def test_tool_detection(registry):
    reg, bus = registry
    reg.set_tool_allowlist(["python", "git"])
    snapshot = await reg.refresh()
    assert "python" in snapshot.tools
    assert snapshot.tools["python"]["status"] == "available"


@pytest.mark.asyncio
async def test_git_state_non_repo(registry, test_dir):
    reg, bus = registry
    snapshot = await reg.refresh()
    assert snapshot.git is None


@pytest.mark.asyncio
async def test_permissions(registry):
    reg, bus = registry
    snapshot = await reg.refresh()
    assert snapshot.permissions.get("can_write_workspace") is True
    assert snapshot.permissions.get("can_read_workspace") is True


@pytest.mark.asyncio
async def test_diff_detects_changes(registry, test_dir):
    reg, bus = registry
    s1 = await reg.refresh()
    test_file = test_dir / "new_file.py"
    test_file.write_text("x = 1")
    s2 = await reg.refresh()
    diff = reg.diff(s1, s2)
    assert "files_added" in diff


@pytest.mark.asyncio
async def test_prompt_injection_format(registry):
    reg, bus = registry
    await reg.refresh()
    prompt = reg.to_prompt_injection()
    assert "[CAPABILITY SNAPSHOT]" in prompt
    assert "HEALTH:" in prompt
    assert "WORKSPACE:" in prompt
    assert "TOOLS:" in prompt
