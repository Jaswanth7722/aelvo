"""
End-to-End Integration Tests for #doctor and #diagnostics CLI Commands

Exercises every diagnostic method wired into ProviderRuntime to verify
they work correctly end-to-end.

Run with:
    python -m pytest tests/test_diagnostics_integration.py -v
    python -m pytest tests/test_diagnostics_integration.py -v -k "doctor or auth or capability"  # run subset
"""

import asyncio
import pytest
from core.provider_runtime import ProviderRuntime, init_provider_runtime
from auth.diagnostics.doctor import DoctorReport
from auth.diagnostics.auth_diag import AuthDiagnosticResult
from auth.diagnostics.capability_inspector import CapabilityReport
from auth.diagnostics.comparison_reports import ComparisonReport, ComparisonMetric


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def runtime():
    """Build a ProviderRuntime with all diagnostic components wired up.

    Constructs the runtime manually (instead of calling init_provider_runtime())
    to avoid the pre-existing register_model -> register_batch mismatch in that
    function. This test focuses on verifying that the diagnostic helper methods
    work correctly end-to-end.

    Uses a sync fixture to avoid pytest-asyncio module-scope compatibility issues.
    """
    from auth.config import PROVIDER_REGISTRY
    from auth.cred_storage import CredentialStore
    from auth.runtime.registry import ProviderRegistry
    from auth.runtime.health import ProviderHealthRuntime
    from auth.runtime.usage import UsageTracker
    from auth.runtime.fallback import FallbackRouter
    from auth.runtime.diagnostics import RuntimeDiagnostics
    from auth.runtime.capability import CapabilityRegistry
    from auth.runtime.model_registry import ModelRegistry
    from auth.monitoring.health import HealthMonitor
    from auth.monitoring.metrics import MetricsCollector
    from auth.monitoring.degradation import DegradationDetector
    from auth.types import (
        CapabilityFlag, ModelCapability, ProviderCapabilities,
        ProviderConfig, ProviderInfo, ProviderKind, ProviderStatus,
    )
    import tempfile, os

    # Build minimal runtime
    cred_store = CredentialStore(db_path=os.path.join(tempfile.gettempdir(), "test_creds.db"))
    registry = ProviderRegistry()
    model_registry = ModelRegistry()
    health = ProviderHealthRuntime()
    usage = UsageTracker()
    capability_registry = CapabilityRegistry()

    # Register a few providers for testing
    # Note: model registration is skipped for these integration tests since
    # the diagnostic methods work with or without models registered.
    provider_configs: dict[str, ProviderConfig] = dict(PROVIDER_REGISTRY)
    for provider_key, config in list(provider_configs.items())[:5]:
        info = ProviderInfo(
            provider_id=provider_key,
            name=config.name,
            provider_type=ProviderKind.FOUNDATION if not config.local else ProviderKind.LOCAL,
            version="1.0.0",
        )
        caps = ProviderCapabilities(
            capabilities={CapabilityFlag[cap.name] for cap in config.capabilities
                          if cap.name in CapabilityFlag.__members__},
            model_families=set(),
        )
        registry.register(
            info=info, config=config, capabilities=caps, client=None,
        )
        capability_registry.register_provider_capabilities(provider_key, caps)

    fallback = FallbackRouter(health_runtime=health)
    diagnostics = RuntimeDiagnostics(
        registry=registry, health=health, usage=usage, capability=capability_registry,
    )

    rt = ProviderRuntime(
        registry=registry,
        model_registry=model_registry,
        health=health,
        usage=usage,
        fallback=fallback,
        diagnostics=diagnostics,
        credential_store=cred_store,
        capability_registry=capability_registry,
        provider_configs=provider_configs,
        health_monitor=HealthMonitor(),
        metrics_collector=MetricsCollector(),
        degradation_detector=DegradationDetector(),
    )
    yield rt


# ── #doctor: ProviderDoctor Tests ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_doctor_get_doctor_returns_instance(runtime: ProviderRuntime):
    """get_doctor() should return a ProviderDoctor wired to the runtime."""
    doctor = runtime.get_doctor()
    from auth.diagnostics.doctor import ProviderDoctor
    assert isinstance(doctor, ProviderDoctor)


@pytest.mark.asyncio
async def test_doctor_scan_single_provider(runtime: ProviderRuntime):
    """doctor_scan('openai') should return a DoctorReport for that provider."""
    reports = await runtime.doctor_scan("openai")
    assert isinstance(reports, dict)
    assert "openai" in reports
    report = reports["openai"]
    assert isinstance(report, DoctorReport)
    assert report.provider_id == "openai"
    assert report.overall_health in ("healthy", "degraded", "unhealthy")
    assert hasattr(report, "connectivity")
    assert hasattr(report, "auth_status")
    assert hasattr(report, "latency_grade")
    assert hasattr(report, "uptime_grade")
    assert isinstance(report.recommendations, list)
    assert isinstance(report.issues, list)
    assert isinstance(report.capabilities, list)
    assert isinstance(report.models, list)


@pytest.mark.asyncio
async def test_doctor_scan_unknown_provider(runtime: ProviderRuntime):
    """doctor_scan('nonexistent') should gracefully handle unknown providers."""
    reports = await runtime.doctor_scan("nonexistent_provider")
    assert isinstance(reports, dict)
    assert "nonexistent_provider" in reports
    report = reports["nonexistent_provider"]
    assert isinstance(report, DoctorReport)
    # Unknown providers should have connectivity=False
    assert report.connectivity is False
    assert report.overall_health in ("degraded", "unhealthy")


@pytest.mark.asyncio
async def test_doctor_scan_all_providers(runtime: ProviderRuntime):
    """doctor_scan() with no args should return reports for ALL registered providers."""
    reports = await runtime.doctor_scan()
    assert isinstance(reports, dict)
    # Should have at least the providers we registered
    assert len(reports) >= 1
    # Every value should be a DoctorReport
    for pid, report in reports.items():
        assert isinstance(report, DoctorReport), f"{pid} report is not DoctorReport"
        assert report.provider_id == pid


@pytest.mark.asyncio
async def test_doctor_scan_report_has_expected_fields(runtime: ProviderRuntime):
    """Each DoctorReport should contain actionable metrics."""
    reports = await runtime.doctor_scan()
    for pid, report in reports.items():
        assert "avg_latency_ms" in report.metrics, f"{pid} missing avg_latency_ms"
        assert "uptime_1h" in report.metrics, f"{pid} missing uptime_1h"
        assert isinstance(report.issues, list)
        assert isinstance(report.recommendations, list)
        if report.issues:
            # If there are issues, there should be at least one recommendation
            assert len(report.recommendations) >= 1, f"{pid} has issues but no recommendations"


# ── #doctor: Diagnostics Summary Tests ────────────────────────────────────────


@pytest.mark.asyncio
async def test_diagnostics_summary_returns_all_providers(runtime: ProviderRuntime):
    """diagnostics_summary() should return an entry for every registered provider."""
    summary = runtime.diagnostics_summary()
    assert isinstance(summary, dict)
    for pid in runtime.provider_configs:
        assert pid in summary, f"{pid} missing from diagnostics_summary"


@pytest.mark.asyncio
async def test_diagnostics_summary_has_expected_keys(runtime: ProviderRuntime):
    """Each provider entry should have all diagnostic fields."""
    summary = runtime.diagnostics_summary()
    for pid, data in summary.items():
        assert "has_credentials" in data
        assert "is_active" in data
        assert "health_status" in data
        assert "latency_ms" in data
        assert "uptime_1h" in data
        assert "error_rate_pct" in data
        assert "models_count" in data


@pytest.mark.asyncio
async def test_diagnostics_summary_values_are_typed_correctly(runtime: ProviderRuntime):
    """Type checks on summary fields."""
    summary = runtime.diagnostics_summary()
    for pid, data in summary.items():
        assert isinstance(data["has_credentials"], bool)
        assert isinstance(data["is_active"], bool)
        assert isinstance(data["health_status"], str)
        assert isinstance(data["latency_ms"], (int, float))
        assert isinstance(data["uptime_1h"], (int, float))
        assert isinstance(data["error_rate_pct"], (int, float))
        assert isinstance(data["models_count"], int)


# ── #diagnostics auth: AuthDiagnostics Tests ──────────────────────────────────


@pytest.mark.asyncio
async def test_auth_diagnostics_get_auth_diagnostics_returns_instance(runtime: ProviderRuntime):
    """get_auth_diagnostics() should return an AuthDiagnostics instance."""
    auth = runtime.get_auth_diagnostics()
    from auth.diagnostics.auth_diag import AuthDiagnostics
    assert isinstance(auth, AuthDiagnostics)


@pytest.mark.asyncio
async def test_auth_diagnostics_single_provider(runtime: ProviderRuntime):
    """auth.diagnose('openai') should return a properly structured result."""
    auth = runtime.get_auth_diagnostics()
    result = await auth.diagnose("openai")
    assert isinstance(result, AuthDiagnosticResult)
    assert result.provider_id == "openai"
    assert hasattr(result, "has_api_key_env")
    assert hasattr(result, "has_api_key_registered")
    assert hasattr(result, "is_valid")
    assert isinstance(result.issues, list)
    assert isinstance(result.recommendations, list)
    assert isinstance(result.environment_variables, list)


@pytest.mark.asyncio
async def test_auth_diagnostics_all_providers(runtime: ProviderRuntime):
    """auth.diagnose_all() should return results for all known providers."""
    auth = runtime.get_auth_diagnostics()
    results = await auth.diagnose_all()
    assert isinstance(results, dict)
    # Should include major providers
    for expected in ("openai", "anthropic", "google", "groq"):
        assert expected in results, f"{expected} missing from auth diagnose_all"


@pytest.mark.asyncio
async def test_auth_diagnostics_local_providers_no_key_required(runtime: ProviderRuntime):
    """Local providers (ollama, lm_studio, etc.) should be valid even without env keys."""
    auth = runtime.get_auth_diagnostics()
    for local_pid in ("ollama", "lm_studio", "vllm", "llamacpp"):
        result = await auth.diagnose(local_pid)
        assert result.is_valid, f"{local_pid} should be valid (local provider)"
        assert any("does not require API keys" in rec for rec in result.recommendations), \
            f"{local_pid} should mention no API key required"


@pytest.mark.asyncio
async def test_auth_diagnostics_summary_table(runtime: ProviderRuntime):
    """auth.summary() should return a list of dicts with expected keys."""
    auth = runtime.get_auth_diagnostics()
    results = await auth.diagnose_all()
    table = auth.summary(results)
    assert isinstance(table, list)
    assert len(table) > 0
    for row in table:
        assert "provider" in row
        assert "configured" in row
        assert "env_var" in row
        assert "registered" in row
        assert "issues" in row
        # Check emoji-based status values
        assert row["configured"] in ("✅", "❌")
        assert row["env_var"] in ("✅", "❌")


# ── #diagnostics capabilities: CapabilityInspector Tests ──────────────────────


@pytest.mark.asyncio
async def test_capability_inspector_get_instance(runtime: ProviderRuntime):
    """get_capability_inspector() should return a CapabilityInspector."""
    inspector = runtime.get_capability_inspector()
    from auth.diagnostics.capability_inspector import CapabilityInspector
    assert isinstance(inspector, CapabilityInspector)


@pytest.mark.asyncio
async def test_capability_inspector_single_provider(runtime: ProviderRuntime):
    """inspector.inspect('openai') should return a CapabilityReport."""
    inspector = runtime.get_capability_inspector()
    report = inspector.inspect("openai")
    assert isinstance(report, CapabilityReport)
    assert report.provider_id == "openai"
    assert isinstance(report.capabilities, list)
    assert isinstance(report.missing_capabilities, list)
    assert isinstance(report.models_count, int)
    assert isinstance(report.supports_streaming, bool)
    assert isinstance(report.supports_tool_calling, bool)
    assert isinstance(report.supports_multimodal, bool)
    assert isinstance(report.is_local, bool)


@pytest.mark.asyncio
async def test_capability_inspector_matrix(runtime: ProviderRuntime):
    """capability_matrix() should return a dict with provider -> capabilities mapping."""
    inspector = runtime.get_capability_inspector()
    matrix = inspector.capability_matrix()
    assert isinstance(matrix, dict)
    # Each value should be a list of capability strings
    for pid, caps in matrix.items():
        assert isinstance(caps, (list, set)), f"{pid} capabilities is not a list/set"
        if isinstance(caps, list):
            for c in caps:
                assert isinstance(c, str), f"{pid} capability {c} is not a string"


@pytest.mark.asyncio
async def test_capability_find_providers_for_task(runtime: ProviderRuntime):
    """find_providers_for_task() should return providers matching the task type."""
    inspector = runtime.get_capability_inspector()
    # 'chat' should return multiple providers
    chat_providers = inspector.find_providers_for_task("chat")
    assert isinstance(chat_providers, list)
    # 'streaming' should have overlapping results with chat
    streaming_providers = inspector.find_providers_for_task("streaming")
    assert isinstance(streaming_providers, list)
    # 'local' should find at least the local providers
    local_providers = inspector.find_providers_for_task("local")
    assert isinstance(local_providers, list)


@pytest.mark.asyncio
async def test_capability_inspector_unknown_task(runtime: ProviderRuntime):
    """An unknown task type should return all providers."""
    inspector = runtime.get_capability_inspector()
    providers = inspector.find_providers_for_task("nonexistent_task")
    assert isinstance(providers, list)
    # Unknown task = empty required_caps → returns all providers
    assert len(providers) >= 1


@pytest.mark.asyncio
async def test_capability_compare_providers(runtime: ProviderRuntime):
    """compare_providers() should return reports for each provider."""
    inspector = runtime.get_capability_inspector()
    reports = inspector.compare_providers(["openai", "anthropic"])
    assert isinstance(reports, dict)
    assert "openai" in reports
    assert "anthropic" in reports
    for pid, report in reports.items():
        assert isinstance(report, CapabilityReport)


# ── #diagnostics compare: ComparisonReportGenerator Tests ─────────────────────


@pytest.mark.asyncio
async def test_comparison_generator_get_instance(runtime: ProviderRuntime):
    """get_comparison_generator() should return a ComparisonReportGenerator."""
    generator = runtime.get_comparison_generator()
    from auth.diagnostics.comparison_reports import ComparisonReportGenerator
    assert isinstance(generator, ComparisonReportGenerator)


@pytest.mark.asyncio
async def test_comparison_two_providers(runtime: ProviderRuntime):
    """compare(['openai', 'anthropic']) should return a structured report."""
    generator = runtime.get_comparison_generator()
    report = generator.compare(["openai", "anthropic"])
    assert isinstance(report, ComparisonReport)
    assert "openai" in report.providers
    assert "anthropic" in report.providers
    assert len(report.metrics) >= 1
    for metric in report.metrics:
        assert isinstance(metric, ComparisonMetric)
        assert metric.name
        assert "openai" in metric.values
        assert "anthropic" in metric.values
        assert isinstance(metric.unit, str)


@pytest.mark.asyncio
async def test_comparison_to_table(runtime: ProviderRuntime):
    """to_table() should convert report to a list of dicts."""
    generator = runtime.get_comparison_generator()
    report = generator.compare(["openai", "anthropic"])
    table = generator.to_table(report)
    assert isinstance(table, list)
    for row in table:
        assert "metric" in row
        assert "openai" in row
        assert "anthropic" in row


@pytest.mark.asyncio
async def test_comparison_winner_selected(runtime: ProviderRuntime):
    """compare() should select a winner when comparing providers."""
    generator = runtime.get_comparison_generator()
    report = generator.compare(["openai", "anthropic", "google"])
    assert isinstance(report.winner, str)
    assert report.winner in ("openai", "anthropic", "google")
    assert len(report.summary) > 0


# ── #diagnostics health: HealthCheckRunner Tests ──────────────────────────────


@pytest.mark.asyncio
async def test_health_check_runner_get_instance(runtime: ProviderRuntime):
    """get_health_check_runner() should return a HealthCheckRunner."""
    runner = runtime.get_health_check_runner()
    from auth.diagnostics.health_checks import HealthCheckRunner
    assert isinstance(runner, HealthCheckRunner)


@pytest.mark.asyncio
async def test_health_check_runner_register_and_run(runtime: ProviderRuntime):
    """Runner should allow registering and running custom checks."""
    runner = runtime.get_health_check_runner()

    # Register a custom check
    async def always_passes():
        return True

    async def always_fails():
        raise RuntimeError("Simulated failure")

    runner.register_check("pass_check", always_passes)
    runner.register_check("fail_check", always_fails)

    # Run the passing check
    pass_result = await runner.run_check("pass_check")
    assert pass_result.passed is True
    assert pass_result.name == "pass_check"
    assert pass_result.duration_ms >= 0

    # Run the failing check
    fail_result = await runner.run_check("fail_check")
    assert fail_result.passed is False
    assert "Simulated failure" in fail_result.error


@pytest.mark.asyncio
async def test_health_check_runner_run_all(runtime: ProviderRuntime):
    """run_all() should run registered checks and aggregate results."""
    runner = runtime.get_health_check_runner()

    async def check_a():
        return True

    async def check_b():
        return True

    runner.register_check("check_a", check_a)
    runner.register_check("check_b", check_b)

    results = await runner.run_all()
    assert isinstance(results, list)
    assert len(results) >= 2
    for r in results:
        assert isinstance(r.passed, bool)
        assert r.duration_ms >= 0


@pytest.mark.asyncio
async def test_health_check_runner_unknown_check(runtime: ProviderRuntime):
    """Running an unregistered check should return a failed result."""
    runner = runtime.get_health_check_runner()
    result = await runner.run_check("nonexistent_check")
    assert result.passed is False
    assert "No check registered" in result.error


# ── Cross-module: CLI Simulated Output Format Tests ───────────────────────────


@pytest.mark.asyncio
async def test_cli_doctor_scan_output_format(runtime: ProviderRuntime):
    """Simulate the output format the CLI would produce for #doctor scan."""
    reports = await runtime.doctor_scan()
    lines = []
    for pid, report in reports.items():
        lines.append(f"  ── {pid} ──")
        lines.append(f"  Health:      {report.overall_health}")
        lines.append(f"  Connectivity: {'✅' if report.connectivity else '❌'}")
        lines.append(f"  Auth:         {'✅' if report.auth_status else '❌'}")
        lines.append(f"  Latency:      {report.latency_grade}")
        lines.append(f"  Uptime:       {report.uptime_grade}")
        if report.issues:
            lines.extend(f"     - {i}" for i in report.issues)
        if report.recommendations:
            lines.extend(f"     - {r}" for r in report.recommendations[:2])
    output = "\n".join(lines)
    # Verify the format is what CLI would print
    assert len(output) > 0
    assert any("✅" in line or "❌" in line for line in lines)


@pytest.mark.asyncio
async def test_cli_diagnostics_auth_table_format(runtime: ProviderRuntime):
    """Simulate the #diagnostics auth table format."""
    auth = runtime.get_auth_diagnostics()
    results = await auth.diagnose_all()
    table = auth.summary(results)
    lines = []
    for row in table[:5]:  # First 5 rows
        lines.append(f"  {row['provider']:20s} | {row['configured']:8s} | {row['env_var']:8s}")
    output = "\n".join(lines)
    assert len(output) > 0


@pytest.mark.asyncio
async def test_cli_diagnostics_comparison_table_format(runtime: ProviderRuntime):
    """Simulate the #diagnostics compare table format."""
    generator = runtime.get_comparison_generator()
    providers = list(runtime.provider_configs.keys())[:3]
    if len(providers) >= 2:
        report = generator.compare(providers)
        table = generator.to_table(report)
        lines = []
        for row in table:
            line = f"  {row['metric']:25s}"
            for pid in providers:
                line += f" | {pid}: {row.get(pid, 'N/A')}"
            lines.append(line)
        lines.append(f"  🏆 Winner: {report.winner}")
        output = "\n".join(lines)
        assert len(output) > 0


# ── Edge Cases ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_diagnostics_summary_with_no_credentials(runtime: ProviderRuntime):
    """diagnostics_summary should correctly report credential status."""
    summary = runtime.diagnostics_summary()
    for pid, data in summary.items():
        # has_credentials should always be a bool
        assert isinstance(data["has_credentials"], bool)
        # is_active should match registry status
        if runtime.registry.has_provider(pid):
            expected_active = runtime.registry.get(pid).is_active
            assert data["is_active"] == expected_active, f"{pid} active mismatch"


@pytest.mark.asyncio
async def test_capability_inspector_consistency(runtime: ProviderRuntime):
    """Capability reports should be self-consistent."""
    inspector = runtime.get_capability_inspector()
    for pid in list(runtime.provider_configs.keys())[:5]:
        report = inspector.inspect(pid)
        # If it supports streaming, it should have streaming in capabilities
        if report.supports_streaming:
            assert any("STREAMING" in c for c in report.capabilities), \
                f"{pid} has supports_streaming=True but STREAMING not in capabilities"


@pytest.mark.asyncio
async def test_comparison_metric_consistency(runtime: ProviderRuntime):
    """Comparison metrics should have consistent values across all providers."""
    generator = runtime.get_comparison_generator()
    providers = list(runtime.provider_configs.keys())[:3]
    if len(providers) >= 2:
        report = generator.compare(providers)
        for metric in report.metrics:
            # Every provider in the comparison should have a value
            for pid in providers:
                assert pid in metric.values, f"{pid} missing from metric '{metric.name}'"
                # Values should be numeric (int or float)
                assert isinstance(metric.values[pid], (int, float)), \
                    f"{pid} value for '{metric.name}' is not numeric"


@pytest.mark.asyncio
async def test_health_check_runner_dns_check(runtime: ProviderRuntime):
    """dns_check() should resolve known hostnames."""
    runner = runtime.get_health_check_runner()
    result = await runner.dns_check("api.openai.com")
    assert isinstance(result, bool)
    # DNS should work for real hostnames (unless offline)
    # We don't assert True because CI might be offline, but it should not crash
