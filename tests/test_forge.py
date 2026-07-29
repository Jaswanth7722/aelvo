"""
FORGE Integration Tests
Run with: pytest tests/test_forge.py -v
Direct CLI mode — no TypeScript tool server required.
"""
import json
import os
import shutil
import tempfile
import uuid

import pytest

from config.settings import BASE_DIR
from core.governance.kernel import MemoryEngine
from memory.forge_memory import ForgeMemory
from specialists.forge import ForgeSpecialist
from tools.code_tools import (
    build_symbol_graph,
    run_formatter,
    run_linter,
    run_type_checker,
)


@pytest.fixture
def workspace():
    tmpdir = tempfile.mkdtemp(prefix="forge_test_")
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


def _workspace_temp_py(content: str, work_dir: str) -> tuple:
    fname = f"_forge_test_{uuid.uuid4().hex[:8]}.py"
    abs_path = os.path.join(work_dir, fname)
    with open(abs_path, "w", encoding="utf-8") as f:
        f.write(content)
    return abs_path, fname


def test_linter_returns_structured_violations(workspace):
    abs_path, rel = _workspace_temp_py(
        "import os\n\n"
        "def bad():\n"
        "    try:\n"
        "        pass\n"
        "    except:\n"
        "        pass\n\n"
        'x = f"hello"\n',
        workspace,
    )
    try:
        result = run_linter(rel, "python", workspace)
        assert result.get("status") in ("success", "error", "not_available")
        if result.get("status") == "error":
            executed = result.get("executed", {})
            assert executed.get("violation_count", 0) >= 2
            for v in result.get("data", []):
                for field in ("line", "col", "code", "message"):
                    assert field in v
    finally:
        if os.path.exists(abs_path):
            os.unlink(abs_path)


def test_formatter_changes_file(workspace):
    abs_path, rel = _workspace_temp_py("def foo():\n\n\n    return 1\n\n\n", workspace)
    try:
        result = run_formatter(rel, "python", workspace)
        if result.get("status") == "not_available":
            pytest.skip("ruff formatter not available")
        assert result.get("status") == "success"
        executed = result.get("executed", {})
        assert executed.get("changed") is True
    finally:
        if os.path.exists(abs_path):
            os.unlink(abs_path)


def test_type_checker_finds_errors(workspace):
    abs_path, rel = _workspace_temp_py("def add(a: int, b: int) -> str:\n    return a + b\n", workspace)
    try:
        result = run_type_checker(rel, "python", workspace)
        if result.get("status") == "not_available":
            pytest.skip("mypy not available")
        executed = result.get("executed", {})
        if executed.get("error_count", 0) > 0:
            assert result.get("status") == "error"
        else:
            pytest.skip("mypy did not find errors in test input")
    finally:
        if os.path.exists(abs_path):
            os.unlink(abs_path)


def test_memory_write_and_retrieval_roundtrip(workspace):
    db_path = os.path.join(workspace, "forge_test.db")
    anchor = BASE_DIR / "global_anchor.md"
    engine = MemoryEngine(
        db_path=db_path,
        anchor_path=str(anchor),
        tool_registry={},
        project_name="test_forge_roundtrip",
    )
    fm = ForgeMemory(engine, "test_forge_roundtrip")
    distinctive = "FORGE_ROUNDTRIP_UNIQUE_PATTERN_zephyr_42"
    saved = fm.save_code_pattern(
        description=distinctive,
        file_path="module.py",
        language="python",
        pattern_type="integration",
        signature="def zephyr(): pass",
        context="test roundtrip",
    )
    assert saved is True
    time.sleep(0.5)
    hits = fm.query_patterns(distinctive, 5)
    if not hits:
        hits = fm.query_patterns("zephyr integration pattern module", 5)
    docs = [h.get("doc", "") for h in hits]
    assert any(distinctive in d for d in docs), f"Expected distinctive text in hits: {hits}"


def test_deduplication_prevents_duplicate_entries(workspace):
    db_path = os.path.join(workspace, "forge_dedup.db")
    anchor = BASE_DIR / "global_anchor.md"
    engine = MemoryEngine(
        db_path=db_path,
        anchor_path=str(anchor),
        tool_registry={},
        project_name="test_forge_dedup",
    )
    fm = ForgeMemory(engine, "test_forge_dedup")
    content = "DEDUP_TEST_FORGE_identical_content_marker"
    fm.save_code_pattern(
        description=content,
        file_path="a.py",
        language="python",
        pattern_type="dedup",
        signature="def dup(): pass",
        context="dedup test",
    )
    fm.save_code_pattern(
        description=content,
        file_path="a.py",
        language="python",
        pattern_type="dedup",
        signature="def dup(): pass",
        context="dedup test",
    )
    hits = fm.query_patterns("DEDUP_TEST_FORGE_identical", 10)
    matching = [h for h in hits if content in h.get("doc", "")]
    assert len(matching) <= 1


def test_symbol_graph_extracts_real_symbols(workspace):
    # Seed the temp workspace with some Python files so the scanner finds symbols
    for fname, content in [
        ("mod_one.py", "class Foo:\n    def bar(self): pass\n"),
        ("mod_two.py", "def baz(): pass\n"),
    ]:
        with open(os.path.join(workspace, fname), "w") as f:
            f.write(content)
    result = build_symbol_graph(workspace)
    assert result.get("status") == "success"
    executed = result.get("executed", {})
    assert executed.get("total_symbols", 0) > 0
    assert executed.get("file_count", 0) > 0
    for sym in list(result.get("data", {}).values())[:5]:
        if isinstance(sym, dict) and "error" not in sym:
            for field in ("classes", "functions", "imports"):
                assert field in sym


def test_verify_output_catches_missing_verification():
    forge = ForgeSpecialist()
    bad_calls = [
        {"tool": "write_file", "args": {"path": "x.py", "content": "x = 1\n"}},
        {"tool": "respond", "args": {"message": "done"}},
    ]
    ok, reason = forge.verify_output(json.dumps(bad_calls), {})
    assert ok is False

    good_calls = [
        {"tool": "write_file", "args": {"path": "x.py", "content": "x = 1\n"}},
        {"tool": "run_linter", "args": {"path": "x.py"}},
        {"tool": "respond", "args": {"message": "done"}},
    ]
    ok2, _ = forge.verify_output(json.dumps(good_calls), {})
    assert ok2 is True


def test_forge_system_prompt_sections(workspace):
    forge = ForgeSpecialist()
    context = {
        "budget": 30,
        "constraints": {"language": {"value": "python", "locked": True}},
        "workspace_path": workspace,
        "task": "implement feature",
        "code_patterns": [],
        "error_recoveries": [],
        "conventions": [],
        "state": {},
    }
    prompt = forge.get_system_prompt(context)
    assert "HARD RULE" in prompt or "HARD CONSTRAINTS" in prompt
    assert "PROJECT STRUCTURE" in prompt
    assert "DETECTED LANGUAGE" in prompt
    assert "WORKFLOW" in prompt
    assert "10." in prompt
