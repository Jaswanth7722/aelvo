"""Smoke test for AELVO — verifies the main entry point boots and core components instantiate."""

import os
import shutil
import sys
import tempfile
import pytest


@pytest.fixture
def temp_workspace():
    tmpdir = tempfile.mkdtemp()
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


def test_main_imports():
    """main.py can be imported without ImportError."""
    import main
    assert hasattr(main, "main")


def test_core_components_import():
    """Core subsystems import without error."""
    from core.execution import AelvoKernel
    assert AelvoKernel is not None


def test_memory_engine_instantiation(temp_workspace):
    """MemoryEngine can be created with a temp workspace."""
    from core.governance.kernel import MemoryEngine

    db_path = os.path.join(temp_workspace, "test.db")

    engine = MemoryEngine(db_path, temp_workspace, tool_registry={}, project_name="test_project")
    assert engine.project_name == "test_project"


def test_specialist_import():
    """All 7 specialists import without error."""
    from specialists.forge import ForgeSpecialist
    from specialists.terminus import TerminusSpecialist
    from specialists.herald import HeraldSpecialist
    from specialists.hermes import HermesSpecialist
    from specialists.sentinel import SentinelSpecialist
    from specialists.architect import ArchitectSpecialist
    from specialists.oracle import OracleSpecialist

    for cls in [ForgeSpecialist, TerminusSpecialist, HeraldSpecialist,
                HermesSpecialist, SentinelSpecialist, ArchitectSpecialist,
                OracleSpecialist]:
        instance = cls()
        assert instance is not None


def test_forge_specialist_activation():
    """ForgeSpecialist can compute an activation score for a coding task."""
    from specialists.forge import ForgeSpecialist
    f = ForgeSpecialist()
    score = f.compute_activation_score("fix the bug in auth_handler.py", {})
    assert isinstance(score, (int, float))
    assert score >= 0


def test_hermes_specialist_calibrate():
    """HermesSpecialist can calibrate a response."""
    from specialists.hermes import HermesSpecialist
    h = HermesSpecialist()
    result = h.calibrate_response("Hello world", {})
    assert isinstance(result, str)
    assert len(result) > 0


def test_validate_script_runs():
    """validate.py (if present) executes without error."""
    import subprocess
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script = os.path.join(root, "validate.py")
    if not os.path.exists(script):
        pytest.skip("validate.py not present in repo root")
    result = subprocess.run(
        [sys.executable, script],
        capture_output=True, text=True, timeout=30,
        cwd=root,
    )
    assert result.returncode == 0, f"validate.py failed:\n{result.stderr}"


def test_provider_runtime_import():
    """ProviderRuntime imports and has expected methods."""
    from core.provider_runtime import init_provider_runtime
    assert callable(init_provider_runtime)
