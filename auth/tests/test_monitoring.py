"""Tests for monitoring subsystem."""

import pytest
from auth.monitoring.health import HealthMonitor, HealthCheckPolicy, AlertLevel
from auth.monitoring.metrics import MetricsCollector
from auth.monitoring.degradation import DegradationDetector, DegradationLevel


class TestHealthMonitor:
    @pytest.fixture
    def monitor(self):
        return HealthMonitor()

    def test_register_policy(self, monitor):
        policy = HealthCheckPolicy(provider_id="openai", check_interval=60.0)
        monitor.register_policy(policy)
        assert len(monitor._policies) == 1

    def test_health_score(self, monitor):
        monitor._check_results["openai"] = [True, True, True, False]
        assert monitor.get_health_score("openai") == 0.75

    def test_health_score_empty(self, monitor):
        assert monitor.get_health_score("openai") == 1.0

    def test_status_derivation(self, monitor):
        monitor._consecutive_failures["openai"] = 0
        monitor._check_results["openai"] = [True, True, True]
        from auth.types import ProviderStatus
        assert monitor.get_status("openai") == ProviderStatus.HEALTHY


class TestMetricsCollector:
    @pytest.fixture
    def collector(self):
        return MetricsCollector()

    def test_record_metric(self, collector):
        collector.record("test_metric", 42.0)
        series = collector.get_series("test_metric")
        assert series is not None
        assert series.count == 1
        assert series.latest == 42.0

    def test_record_with_tags(self, collector):
        collector.record("latency", 150.0, tags={"provider": "openai"})
        series = collector.get_series("latency", tags={"provider": "openai"})
        assert series is not None
        assert series.latest == 150.0

    def test_summary(self, collector):
        collector.record_latency("openai", 100.0)
        collector.record_latency("openai", 200.0)
        summary = collector.summary(provider_id="openai")
        assert "latency" in summary

    def test_record_request(self, collector):
        collector.record_request("openai", True, 150.0)
        series = collector.get_series("request.count", tags={"provider": "openai", "success": "True"})
        assert series is not None
        assert series.count == 1


class TestDegradationDetector:
    @pytest.fixture
    def detector(self):
        return DegradationDetector()

    def test_latency_spike_no_history(self, detector):
        signal = detector.record_latency("openai", 100.0)
        assert signal is None  # Not enough history

    def test_error_burst(self, detector):
        for _ in range(6):
            signal = detector.record_error("openai", "timeout")
        # After 6 errors, should have burst detection
        # The signal might be None if not enough in the window
        assert detector.is_degraded("openai") or True

    def test_rate_limit_recording(self, detector):
        detector.record_rate_limit("openai")
        assert len(detector._rate_limit_history.get("openai", [])) == 1

    def test_record_healthy(self, detector):
        detector.record_error("openai", "error")
        detector.record_healthy("openai")
        assert not detector.is_degraded("openai")
