"""provider_runtime.py — AELVO Provider Runtime Bootstrap

Initializes the full auth provider runtime ecosystem and makes it
available to the orchestrator, specialists, and CLI commands.

Integrates:
  - CredentialStore (encrypted credential persistence)
  - ProviderRegistry (20+ provider configs)
  - ProviderHealthRuntime (health tracking, latency, uptime)
  - UsageTracker (token usage and cost tracking)
  - FallbackRouter (provider fallback with circuit breaker)
  - RuntimeDiagnostics (diagnose, compare, health summary)
  - SDK client factory (create provider-specific clients)

Usage:
    runtime = await init_provider_runtime()
    client = runtime.create_client("openai", api_key="sk-...")
"""

from __future__ import annotations

import os
import sys
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from auth.config import PROVIDER_REGISTRY, MODEL_REGISTRY, get_provider, get_model
from auth.cred_storage import CredentialStore
from auth.runtime.registry import ProviderRegistry
from auth.runtime.health import ProviderHealthRuntime
from auth.runtime.usage import UsageTracker
from auth.runtime.fallback import FallbackRouter
from auth.runtime.diagnostics import RuntimeDiagnostics
from auth.runtime.capability import CapabilityRegistry
from auth.runtime.model_registry import ModelRegistry
from auth.adapters.messages import MessageAdapter
from auth.adapters.streaming import StreamingAdapter
from auth.adapters.tool_calls import ToolCallAdapter
from auth.adapters.structured_output import StructuredOutputAdapter
from auth.monitoring.health import HealthMonitor, HealthCheckPolicy, AlertLevel
from auth.monitoring.metrics import MetricsCollector
from auth.monitoring.degradation import DegradationDetector
from auth.diagnostics.doctor import ProviderDoctor
from auth.diagnostics.auth_diag import AuthDiagnostics
from auth.diagnostics.capability_inspector import CapabilityInspector
from auth.diagnostics.comparison_reports import ComparisonReportGenerator
from auth.diagnostics.health_checks import HealthCheckRunner
from auth.types import (
    AuthMethod,
    Capability,
    Credential,
    CredentialType,
    HealthStatus,
    ProviderConfig,
    ProviderInfo,
    ProviderKind,
    ProviderStatus,
    Usage,
)

logger = logging.getLogger("aelvo.provider_runtime")

# Default paths
DEFAULT_RUNTIME_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".aelvo_runtime")
DEFAULT_VAULT_PATH = os.path.join(DEFAULT_RUNTIME_DIR, "credential_vault.db")


@dataclass
class ProviderRuntime:
    """Bundles all provider runtime components for AELVO integration."""

    registry: ProviderRegistry
    """Central registry of all provider implementations."""

    model_registry: ModelRegistry
    """Registry of all model configurations."""

    health: ProviderHealthRuntime
    """Health tracking, latency monitoring, uptime calculation."""

    usage: UsageTracker
    """Token usage and cost tracking."""

    fallback: FallbackRouter
    """Provider fallback routing with circuit breaker."""

    diagnostics: RuntimeDiagnostics
    """Comprehensive diagnostics and introspection."""

    credential_store: CredentialStore
    """Encrypted credential storage."""

    capability_registry: CapabilityRegistry
    """Provider capability registry."""

    provider_configs: dict[str, ProviderConfig] = field(default_factory=dict)
    """All registered provider configurations from auth.config."""

    # ── Monitoring Components ─────────────────────────────────────

    health_monitor: Optional[HealthMonitor] = None
    """Proactive health monitor — periodic checks and alerts."""

    metrics_collector: Optional[MetricsCollector] = None
    """Granular performance and reliability metrics collector."""

    degradation_detector: Optional[DegradationDetector] = None
    """Degradation pattern detector — latency spikes, error bursts, rate limit waves."""

    health_runner: Optional[HealthCheckRunner] = None
    """Health check runner — reusable connectivity and DNS checks for diagnostics & monitoring."""

    # ── Helpers ──────────────────────────────────────────────────

    def get_provider_config(self, provider_key: str) -> Optional[ProviderConfig]:
        """Get a provider's configuration by key."""
        return self.provider_configs.get(provider_key) or get_provider(provider_key)

    def list_providers(self) -> list[str]:
        """List all registered provider keys."""
        return sorted(self.provider_configs.keys())

    def list_models(self, provider_key: Optional[str] = None) -> list[str]:
        """List model IDs, optionally filtered by provider."""
        if provider_key:
            cfg = self.get_provider_config(provider_key)
            if not cfg:
                return []
            return [m.id for m in cfg.models]
        return sorted(MODEL_REGISTRY.keys())

    def get_active_providers(self) -> list[str]:
        """Get providers that are currently active and have credentials."""
        active = []
        for pid in self.registry.get_active_providers():
            entry = self.registry.get(pid)
            if entry and entry.is_active:
                active.append(pid)
        return active

    def has_credentials(self, provider_key: str) -> bool:
        """Check if credentials exist for a provider."""
        cfg = self.get_provider_config(provider_key)
        if not cfg:
            return False
        if cfg.local:
            return True  # Local providers don't need credentials
        try:
            cred = self.credential_store.get_for_provider(provider_key)
            return cred is not None
        except Exception:
            return os.environ.get(cfg.auth.env_var, "") != ""

    def is_provider_available(self, provider_key: str) -> bool:
        """Check if a provider is available (registered + has credentials + healthy)."""
        if not self.registry.has_provider(provider_key):
            return False
        if not self.has_credentials(provider_key):
            return False
        return self.health.is_available(provider_key)

    def create_client(self, provider_key: str, api_key: Optional[str] = None, **kwargs) -> Any:
        """Create an SDK client for a provider.

        Uses stored credentials if no api_key is provided.
        Returns None if the provider SDK is not installed.
        """
        cfg = self.get_provider_config(provider_key)
        if not cfg:
            raise ValueError(f"Unknown provider: {provider_key}")

        # Resolve API key
        if not api_key:
            if cfg.local:
                api_key = "local-trust-mode"
            else:
                try:
                    cred = self.credential_store.resolve(provider_key)
                    api_key = cred.secret if cred else os.environ.get(cfg.auth.env_var, "")
                except Exception:
                    api_key = os.environ.get(cfg.auth.env_var, "")

        if not api_key:
            raise ValueError(f"No API key available for provider: {provider_key}")

        base_url = kwargs.get("base_url") or cfg.base_url

        # Create provider-specific client
        sdk = cfg.sdk_type or "openai"
        if sdk == "openai":
            try:
                from openai import OpenAI as OpenAIClient
                return OpenAIClient(api_key=api_key, base_url=base_url)
            except ImportError:
                logger.warning("openai SDK not installed; cannot create client for %s", provider_key)
                return None
        elif sdk == "anthropic":
            try:
                from anthropic import Anthropic
                return Anthropic(api_key=api_key)
            except ImportError:
                logger.warning("anthropic SDK not installed; cannot create client for %s", provider_key)
                return None
        elif sdk == "google":
            try:
                import google.generativeai as genai
                genai.configure(api_key=api_key)
                return genai
            except ImportError:
                logger.warning("google-generativeai SDK not installed; cannot create client for %s", provider_key)
                return None
        else:
            logger.warning("Unknown SDK type '%s' for provider %s", sdk, provider_key)
            return None

    # ── Adapter Helpers ──────────────────────────────────────────

    def normalize_messages(
        self,
        messages: list[dict],
        target_provider: Optional[str] = None,
    ) -> list[dict]:
        """Convert canonical (OpenAI-format) messages to a target provider's format.

        Uses MessageAdapter to handle Anthropic tool blocks, Google parts,
        and any other provider-specific message format requirements.

        Args:
            messages: Messages in canonical (OpenAI) format.
            target_provider: Provider key (e.g. 'anthropic', 'google').
                Defaults to 'openai' if not specified.

        Returns:
            Messages converted to the target provider's format.
        """
        pid = target_provider or "openai"
        return MessageAdapter.from_canonical(messages, pid)

    def normalize_tool_calls(
        self,
        raw_calls: list[dict],
        source_provider: Optional[str] = None,
    ) -> list[dict]:
        """Normalize provider-specific tool calls to canonical format.

        Uses ToolCallAdapter to handle differences in how OpenAI, Anthropic,
        Google, and others represent tool calls.

        Args:
            raw_calls: Tool calls in provider-specific format.
            source_provider: Provider key (e.g. 'anthropic', 'google').
                Defaults to 'openai' if not specified.

        Returns:
            Tool calls normalized to canonical OpenAI format.
        """
        pid = source_provider or "openai"
        return ToolCallAdapter.normalize_tool_calls(raw_calls, pid)

    def format_tool_definition(
        self,
        tool: dict,
        target_provider: Optional[str] = None,
    ) -> dict:
        """Format a canonical tool definition for a specific provider.

        Anthropic uses 'name'/'description'/'input_schema' while OpenAI
        uses 'type'/'function'/'parameters'. This method handles the conversion.

        Args:
            tool: Tool definition in canonical format.
            target_provider: Provider key (e.g. 'anthropic').
                Defaults to 'openai' if not specified.

        Returns:
            Tool definition formatted for the target provider.
        """
        pid = target_provider or "openai"
        return ToolCallAdapter.format_tool_definition(tool, pid)

    def format_response_schema(
        self,
        schema: dict,
        target_provider: Optional[str] = None,
        strict: bool = True,
    ) -> dict:
        """Format a JSON schema as a response format for a specific provider.

        Args:
            schema: JSON schema dict with 'name', 'schema', optional 'description'.
            target_provider: Provider key (e.g. 'openai', 'anthropic', 'google').
                Defaults to 'openai' if not specified.
            strict: Whether to enforce strict schema adherence.

        Returns:
            Provider-specific response format configuration.
        """
        pid = target_provider or "openai"
        return StructuredOutputAdapter.format_response_format(schema, pid, strict)

    def validate_structured_response(
        self,
        response: str,
        schema: dict,
    ) -> tuple[bool, Optional[dict], Optional[str]]:
        """Validate a JSON response against a schema.

        Args:
            response: Raw response string (may contain markdown code blocks).
            schema: JSON schema dict to validate against.

        Returns:
            (is_valid, parsed_data, error_message)
        """
        return StructuredOutputAdapter.validate_response(response, schema)

    def parse_json_response(self, response: str) -> dict:
        """Parse a JSON response, handling markdown code block wrappers.

        Args:
            response: Raw response string, possibly wrapped in ```json ... ```.

        Returns:
            Parsed JSON dict.
        """
        return StructuredOutputAdapter.parse_json_mode(response)

    def get_stream_normalizer(self, provider_key: str) -> callable:
        """Get the chunk normalizer function for a provider's streaming format.

        Returns a callable that accepts a raw chunk from the provider's
        streaming API and returns a canonical stream event dict.
        """
        return StreamingAdapter.get_provider_normalizer(provider_key)

    # ── Diagnostics Methods ──────────────────────────────────────

    def get_doctor(self) -> ProviderDoctor:
        """Get the ProviderDoctor instance wired to this runtime.

        Provides comprehensive diagnostics: connectivity, auth status,
        latency grading, uptime grading, capability listing, and
        actionable recommendations.
        """
        return ProviderDoctor(
            registry=self.registry,
            health=self.health,
            usage=self.usage,
            capability=self.capability_registry,
        )

    def get_auth_diagnostics(self) -> AuthDiagnostics:
        """Get an AuthDiagnostics instance for validating provider auth configs.

        Checks environment variables, registered keys, and provider-specific
        requirements (e.g. AZURE_OPENAI_ENDPOINT, AWS_REGION).
        Returns a fresh instance each call for up-to-date env checks.
        """
        return AuthDiagnostics()

    def get_capability_inspector(self) -> CapabilityInspector:
        """Get a CapabilityInspector for inspecting provider capabilities.

        Supports: inspecting a single provider, generating a full capability
        matrix across all providers, finding providers by task type (chat,
        streaming, tool_calling, vision, etc.), and comparing providers.
        """
        return CapabilityInspector(
            capability_registry=self.capability_registry,
            model_registry=self.model_registry,
        )

    def get_comparison_generator(self) -> ComparisonReportGenerator:
        """Get a ComparisonReportGenerator for side-by-side provider comparisons.

        Compares providers across: latency, uptime, error rate, cost,
        and capability count. Returns a ranked report with a winner.
        """
        return ComparisonReportGenerator(
            health=self.health,
            usage=self.usage,
            capability=self.capability_registry,
        )

    def get_health_check_runner(self) -> HealthCheckRunner:
        """Get a HealthCheckRunner for running ad-hoc health checks.

        Returns the shared instance (with pre-registered per-provider checks
        from init_provider_runtime) if available, otherwise creates a fresh one.

        Supports registering custom checks, running connectivity checks,
        DNS checks, and aggregating results.
        """
        if self.health_runner is not None:
            return self.health_runner
        return HealthCheckRunner()

    async def doctor_scan(self, provider_id: Optional[str] = None) -> dict:
        """Run diagnostics on one or all providers.

        Args:
            provider_id: Specific provider to diagnose, or None for all.

        Returns:
            Dict mapping provider_id -> DoctorReport (serialized) for all providers,
            or a single report dict if a specific provider_id was given.
        """
        doctor = self.get_doctor()
        if provider_id:
            report = await doctor.diagnose(provider_id)
            return {provider_id: report}
        return await doctor.run_full_scan()

    def diagnostics_summary(self) -> dict[str, Any]:
        """Get a quick diagnostic overview of all providers.

        Returns auth configuration status and capability counts
        for all registered providers in a concise format.
        """
        result: dict[str, Any] = {}
        auth = self.get_auth_diagnostics()
        for pid in self.provider_configs:
            result[pid] = {
                "has_credentials": self.has_credentials(pid),
                "is_active": self.registry.has_provider(pid) and
                             self.registry.get(pid).is_active if self.registry.has_provider(pid) else False,
                "health_status": self.health.get_status(pid).name,
                "latency_ms": round(self.health.average_latency(pid), 1),
                "uptime_1h": round(self.health.uptime_percentage(pid, 60) * 100, 1),
                "error_rate_pct": round(self.health.error_rate(pid) * 100, 1),
                "models_count": len(self.list_models(pid)),
            }
        return result

    # ── Monitoring Lifecycle ────────────────────────────────────

    async def start_monitoring(self) -> None:
        """Start proactive health monitoring for all registered providers.

        Registers health check policies for each provider and starts
        the periodic health check loops. Also initializes metric
        collection and degradation detection.
        """
        if not self.health_monitor:
            return

        # Register a health check policy for each provider with credentials
        for pid, config in self.provider_configs.items():
            # Use HealthCheckRunner to create a provider-specific check function
            check_fn = None
            if self.health_runner and (config.base_url or not config.local):
                base_url = config.base_url or f"https://api.{pid}.com"
                check_fn = self.health_runner.create_provider_check_fn(pid, base_url=base_url)

            policy = HealthCheckPolicy(
                provider_id=pid,
                check_interval=120.0,  # Check every 2 minutes
                timeout=15.0,
                consecutive_failures_threshold=3,
                enabled=bool(config.base_url) or not config.local,
                check_fn=check_fn,
            )
            self.health_monitor.register_policy(policy)

        # Wire degradation detector into health monitor alerts
        if self.degradation_detector:
            def _on_alert(alert):
                if alert.level == AlertLevel.CRITICAL:
                    self.degradation_detector.record_error(
                        alert.provider_id, alert.message
                    )

            self.health_monitor.add_alert_handler(_on_alert)

        await self.health_monitor.start()
        logger.info(
            "Health monitoring started for %d providers",
            len(self.provider_configs),
        )

    async def stop_monitoring(self) -> None:
        """Stop proactive health monitoring."""
        if self.health_monitor:
            await self.health_monitor.stop()
            logger.info("Health monitoring stopped")

    # ── Monitoring Convenience Methods ─────────────────────────────

    def record_request(
        self,
        provider_id: str,
        success: bool,
        latency_ms: float = 0.0,
        model_id: str = "",
    ) -> None:
        """Record a provider request with metrics and degradation analysis.

        Routes data to:
        - MetricsCollector (latency, request counts, token usage as available)
        - DegradationDetector (latency spikes)
        - ProviderHealthRuntime (status tracking)
        """
        if self.metrics_collector:
            self.metrics_collector.record_request(
                provider_id, success, latency_ms
            )
            if latency_ms > 0:
                self.metrics_collector.record_latency(
                    provider_id, latency_ms, model_id
                )

        if self.degradation_detector and latency_ms > 0:
            self.degradation_detector.record_latency(provider_id, latency_ms)

        if success:
            self.health.record_success(provider_id, latency_ms)
        else:
            self.health.record_error(provider_id, "request_failure", latency_ms)

    def record_error(
        self,
        provider_id: str,
        error_type: str,
        latency_ms: float = 0.0,
    ) -> None:
        """Record a provider error with metrics and degradation analysis.

        Routes data to:
        - MetricsCollector (error counts)
        - DegradationDetector (error bursts)
        - ProviderHealthRuntime (error status)
        """
        if self.metrics_collector:
            self.metrics_collector.record_error(provider_id, error_type)

        if self.degradation_detector:
            self.degradation_detector.record_error(provider_id, error_type)

        self.health.record_error(provider_id, error_type, latency_ms)

    def record_rate_limit(self, provider_id: str) -> None:
        """Record a rate limit for degradation detection."""
        if self.degradation_detector:
            self.degradation_detector.record_rate_limit(provider_id)
        self.health.record_rate_limit(provider_id)

    def record_token_usage(
        self,
        provider_id: str,
        prompt_tokens: int,
        completion_tokens: int,
        model_id: str = "",
    ) -> None:
        """Record token usage metrics."""
        if self.metrics_collector:
            self.metrics_collector.record_token_usage(
                provider_id, prompt_tokens, completion_tokens, model_id
            )

    # ── Monitoring Queries ─────────────────────────────────────────

    def monitoring_summary(self) -> dict[str, Any]:
        """Get a comprehensive monitoring dashboard summary."""
        mon_summary = self.health_monitor.summary() if self.health_monitor else {}
        result: dict[str, Any] = {
            "health_monitor_running": mon_summary.get("is_running", False),
            "total_alerts": mon_summary.get("total_alerts", 0),
            "providers": {},
        }

        for pid in sorted(self.provider_configs):
            pdata: dict[str, Any] = {
                "status": self.health.get_status(pid).name,
                "health_score": (
                    self.health_monitor.get_health_score(pid)
                    if self.health_monitor else None
                ),
                "monitor_status": (
                    self.health_monitor.get_status(pid).name
                    if self.health_monitor else "N/A"
                ),
                "uptime_1h": f"{self.health.uptime_percentage(pid, 60):.1%}",
                "avg_latency": f"{self.health.average_latency(pid):.0f}ms",
                "error_rate": f"{self.health.error_rate(pid):.1%}",
                "is_degraded": (
                    self.degradation_detector.is_degraded(pid)
                    if self.degradation_detector else False
                ),
                "degradation_level": (
                    self.degradation_detector.get_state(pid).level.name
                    if self.degradation_detector
                    and self.degradation_detector.get_state(pid)
                    else "NONE"
                ),
            }

            # Add metric summaries if available
            if self.metrics_collector:
                msum = self.metrics_collector.summary(provider_id=pid)
                if msum:
                    pdata["metrics"] = msum

            result["providers"][pid] = pdata

        return result

    def summary(self) -> dict[str, Any]:
        """Get a human-readable summary of the runtime state."""
        s = {
            "providers_count": len(self.provider_configs),
            "models_count": len(MODEL_REGISTRY),
            "active_providers": self.get_active_providers(),
            "providers_with_creds": [
                pid for pid in self.provider_configs
                if self.has_credentials(pid)
            ],
            "local_providers": [
                pid for pid, cfg in self.provider_configs.items()
                if cfg.local
            ],
        }
        # Append monitoring status if available
        if self.health_monitor:
            mon = self.health_monitor.summary()
            s["monitoring"] = {
                "running": mon.get("is_running", False),
                "total_alerts": mon.get("total_alerts", 0),
                "critical_alerts": mon.get("critical_alerts", 0),
            }
        return s


async def init_provider_runtime(
    vault_path: Optional[str] = None,
    runtime_dir: Optional[str] = None,
) -> ProviderRuntime:
    """Initialize the full provider runtime ecosystem.

    Args:
        vault_path: Path to the credential vault SQLite database.
            Defaults to ``/.aelvo_runtime/credential_vault.db``.
        runtime_dir: Directory for runtime state files.
            Defaults to ``/.aelvo_runtime``.

    Returns:
        A fully initialized ProviderRuntime instance.
    """
    # Resolve paths
    if runtime_dir is None:
        runtime_dir = DEFAULT_RUNTIME_DIR
    if vault_path is None:
        vault_path = DEFAULT_VAULT_PATH

    os.makedirs(runtime_dir, exist_ok=True)

    # 1. Initialize credential store
    credential_store = CredentialStore(db_path=vault_path)
    logger.info("CredentialStore initialized at %s", vault_path)

    # 2. Initialize registries
    registry = ProviderRegistry()
    model_registry = ModelRegistry()

    # 3. Register all providers from auth.config
    provider_configs: dict[str, ProviderConfig] = dict(PROVIDER_REGISTRY)
    for provider_key, config in provider_configs.items():
        info = ProviderInfo(
            provider_id=provider_key,
            name=config.name,
            provider_type=ProviderKind.FOUNDATION if not config.local else ProviderKind.LOCAL,
            version="1.0.0",
        )
        # Build capabilities from provider config
        from auth.types import CapabilityFlag, ModelCapability, ProviderCapabilities, ProviderStatus
        capabilities = ProviderCapabilities(
            capabilities={CapabilityFlag[cap.name] for cap in config.capabilities if cap.name in CapabilityFlag.__members__},
            model_families=set(),
        )

        # Try to resolve credentials
        client = None
        if not config.local:
            try:
                cred = credential_store.get_for_provider(provider_key)
                if cred:
                    client = _try_create_sdk_client(provider_key, config, cred.value)
            except Exception as _ex: print("Silenced exception: %s", _ex)

        registry.register(
            info=info,
            config=config,
            capabilities=capabilities,
            client=client,
        )

        # Register models
        from auth.types import ModelInfo
        for model in config.models:
            minfo = ModelInfo(
                model_id=model.id,
                provider_id=provider_key,
                context_length=model.context_window or 4096,
            )
            model_registry.register(
                info=minfo,
                provider_ids=[provider_key],
            )

    logger.info("Registered %d providers and %d models", len(provider_configs), len(model_registry.list_models()))

    # 4. Initialize runtime components
    health = ProviderHealthRuntime()
    usage = UsageTracker()

    # 5. Initialize capability registry
    capability_registry = CapabilityRegistry()

    # 6. Initialize fallback router
    fallback = FallbackRouter(health_runtime=health)

    # 7. Initialize diagnostics
    diagnostics = RuntimeDiagnostics(
        registry=registry,
        health=health,
        usage=usage,
        capability=capability_registry,
    )

    # 8. Initialize monitoring components
    health_monitor = HealthMonitor()
    metrics_collector = MetricsCollector()
    degradation_detector = DegradationDetector()
    health_runner = HealthCheckRunner()

    # Register provider-specific health checks for each provider
    for provider_key, config in provider_configs.items():
        base_url = config.base_url  # may be None; register_provider_check handles fallback internally
        health_runner.register_provider_check(provider_key, base_url=base_url)

    logger.info(
        "Monitoring components initialized (HealthMonitor, MetricsCollector, DegradationDetector, HealthCheckRunner)"
    )

    runtime = ProviderRuntime(
        registry=registry,
        model_registry=model_registry,
        health=health,
        usage=usage,
        fallback=fallback,
        diagnostics=diagnostics,
        credential_store=credential_store,
        capability_registry=capability_registry,
        provider_configs=provider_configs,
        health_monitor=health_monitor,
        metrics_collector=metrics_collector,
        degradation_detector=degradation_detector,
        health_runner=health_runner,
    )

    # 9. Start proactive health monitoring
    await runtime.start_monitoring()
    logger.info(
        "Health monitoring active for %d providers",
        len(provider_configs),
    )

    logger.debug("Provider runtime fully initialized")
    return runtime


def _try_create_sdk_client(
    provider_key: str,
    config: ProviderConfig,
    api_key: str,
) -> Any:
    """Try to create an SDK client, returning None if SDK not installed."""
    try:
        sdk = config.sdk_type or "openai"
        base_url = config.base_url
        if sdk == "openai":
            from openai import OpenAI
            return OpenAI(api_key=api_key, base_url=base_url)
        elif sdk == "anthropic":
            from anthropic import Anthropic
            return Anthropic(api_key=api_key)
        elif sdk == "google":
            import google.generativeai as genai
            genai.configure(api_key=api_key)
            return genai
    except ImportError:
        pass
    return None


# ── Quick summary for CLI display ─────────────────────────────

def format_comparison_table(report: Any) -> str:
    """Format a comparison report as a rich-styled table string.

    Uses rich.table.Table to generate a nicely formatted, aligned table
    with color-coded headers and proper column spacing.

    Args:
        report: A ComparisonReport object with .providers, .metrics,
            .winner, and .summary attributes.

    Returns:
        A string containing the rendered table with winner annotation.
    """
    from rich.table import Table
    from rich.console import Console
    from rich import box

    table = Table(
        title="Provider Comparison Report",
        title_style="bold cyan",
        box=box.ROUNDED,
        border_style="bright_blue",
        header_style="bold white on bright_blue",
        show_lines=True,
        padding=(0, 2),
    )

    # Add columns: Metric + one per provider
    table.add_column("Metric", style="cyan", no_wrap=True)
    for pid in report.providers:
        table.add_column(pid, justify="right", style="bright_green")

    # Track best values per metric for highlighting
    best_values: dict[str, tuple[float, str]] = {}
    for metric in report.metrics:
        numeric_vals = {}
        for pid, val in metric.values.items():
            if isinstance(val, (int, float)):
                numeric_vals[pid] = val
        if numeric_vals:
            if metric.higher_is_better:
                best_pid = max(numeric_vals, key=numeric_vals.get)
            else:
                best_pid = min(numeric_vals, key=numeric_vals.get)
            best_values[metric.name] = (numeric_vals[best_pid], best_pid)

    # Add rows with winner highlighting
    for metric in report.metrics:
        metric_name = f"{metric.name} ({metric.unit})" if metric.unit else metric.name
        row = [metric_name]
        for pid in report.providers:
            val = metric.values.get(pid, "N/A")
            # Highlight best value
            best_info = best_values.get(metric.name)
            if best_info and isinstance(val, (int, float)) and val == best_info[0] and pid == best_info[1]:
                row.append(f"[bold yellow]* {val}[/bold yellow]")
            elif isinstance(val, float):
                row.append(f"{val:.1f}")
            else:
                row.append(str(val))
        table.add_row(*row)

    # Winner footer
    if report.winner:
        table.add_section()
        winner_text = f">> Winner: [bold yellow]{report.winner}[/bold yellow]"
        cols = len(report.providers) + 1
        table.add_row(
            winner_text,
            *([""] * (cols - 1)),
            style="bold white on bright_blue",
            end_section=True,
        )

    if report.summary:
        cols = len(report.providers) + 1
        table.add_row(
            report.summary,
            *([""] * (cols - 1)),
            style="dim white",
        )

    # Render to string
    console = Console(width=120, force_terminal=True, color_system="truecolor")
    with console.capture() as capture:
        console.print(table)
    return capture.get()


def format_provider_table(runtime: ProviderRuntime) -> str:
    """Format a markdown-style table of all providers with status."""
    lines = ["| Provider | Status | Credentials | Models | SDK |", "|----------|--------|-------------|--------|-----|"]
    for pid in sorted(runtime.provider_configs):
        config = runtime.provider_configs[pid]
        status = runtime.health.get_status(pid).name if runtime.registry.has_provider(pid) else "UNREGISTERED"
        has_creds = "✅" if runtime.has_credentials(pid) else "❌"
        model_count = len(config.models)
        sdk = config.sdk_type or "?"
        lines.append(f"| {pid} | {status} | {has_creds} | {model_count} | {sdk} |")
    return "\n".join(lines)
