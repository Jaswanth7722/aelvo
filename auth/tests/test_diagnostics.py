"""Tests for diagnostics subsystem."""

import pytest
from auth.diagnostics.doctor import ProviderDoctor, DoctorReport
from auth.diagnostics.auth_diag import AuthDiagnostics, AuthDiagnosticResult
from auth.diagnostics.health_checks import HealthCheckRunner
from auth.diagnostics.capability_inspector import CapabilityInspector, CapabilityReport
from auth.diagnostics.comparison_reports import ComparisonReportGenerator, ComparisonReport
from auth.runtime.registry import ProviderRegistry
from auth.runtime.model_registry import ModelRegistry
from auth.runtime.health import ProviderHealthRuntime
from auth.runtime.usage import UsageTracker
from auth.runtime.capability import CapabilityRegistry


class TestProviderDoctor:
    @pytest.fixture
    def doctor(self):
        registry = ProviderRegistry()
        health = ProviderHealthRuntime()
        usage = UsageTracker()
        capability = CapabilityRegistry()
        return ProviderDoctor(registry, health, usage, capability)

    @pytest.mark.asyncio
    async def test_diagnose_unknown_provider(self, doctor):
        """Doctor should handle unknown providers gracefully."""
        report = await doctor.diagnose("nonexistent_provider")
        assert isinstance(report, DoctorReport)
        assert not report.connectivity

    @pytest.mark.asyncio
    async def test_diagnose_returns_report(self, doctor):
        """Diagnose should return a properly structured report."""
        report = await doctor.diagnose("openai")
        assert report.provider_id == "openai"
        assert hasattr(report, "overall_health")
        assert hasattr(report, "recommendations")
        assert hasattr(report, "issues")

    @pytest.mark.asyncio
    async def test_full_scan(self, doctor):
        """Running a full scan should return a dict of reports."""
        reports = await doctor.run_full_scan()
        assert isinstance(reports, dict)


class TestAuthDiagnostics:
    @pytest.fixture
    def auth_diag(self):
        return AuthDiagnostics()

    @pytest.mark.asyncio
    async def test_diagnose_provider(self, auth_diag):
        """Auth diagnostics should return a result for known providers."""
        result = await auth_diag.diagnose("openai")
        assert isinstance(result, AuthDiagnosticResult)
        assert result.provider_id == "openai"

    @pytest.mark.asyncio
    async def test_diagnose_returns_issues(self, auth_diag):
        """Should identify missing credentials."""
        result = await auth_diag.diagnose("openai")
        assert hasattr(result, "issues")
        assert hasattr(result, "recommendations")
        assert isinstance(result.issues, list)

    @pytest.mark.asyncio
    async def test_diagnose_all(self, auth_diag):
        """Diagnosing all providers should return a dict."""
        results = await auth_diag.diagnose_all()
        assert isinstance(results, dict)
        assert "openai" in results
        assert "anthropic" in results


class TestHealthCheckRunner:
    @pytest.fixture
    def runner(self):
        return HealthCheckRunner()

    def test_register_check(self, runner):
        """Should allow registering health checks."""
        async def dummy_check():
            return True
        runner.register_check("test", dummy_check)
        assert "test" in runner._checks

    def test_empty_runner(self, runner):
        """Running an empty runner should return empty list."""
        import asyncio
        results = asyncio.run(runner.run_all())
        assert isinstance(results, list)


class TestCapabilityInspector:
    @pytest.fixture
    def inspector(self):
        cap_registry = CapabilityRegistry()
        model_registry = ModelRegistry()
        return CapabilityInspector(cap_registry, model_registry)

    def test_inspect_empty_provider(self, inspector):
        """Inspecting an unknown provider should return empty report."""
        report = inspector.inspect("openai")
        assert isinstance(report, CapabilityReport)
        assert report.provider_id == "openai"

    def test_capability_matrix(self, inspector):
        """Capability matrix should return a dict."""
        matrix = inspector.capability_matrix()
        assert isinstance(matrix, dict)


class TestComparisonReportGenerator:
    @pytest.fixture
    def generator(self):
        health = ProviderHealthRuntime()
        usage = UsageTracker()
        capability = CapabilityRegistry()
        return ComparisonReportGenerator(health, usage, capability)

    def test_compare_empty(self, generator):
        """Comparing empty list should return report."""
        report = generator.compare([])
        assert isinstance(report, ComparisonReport)
        assert report.providers == []

    def test_to_table(self, generator):
        """to_table should convert report to table format."""
        report = generator.compare([])
        table = generator.to_table(report)
        assert isinstance(table, list)
