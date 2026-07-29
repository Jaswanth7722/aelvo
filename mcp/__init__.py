"""
MCP Platform — Model Context Protocol Subsystem for AELVO Omega.

This package provides a production-grade MCP infrastructure that integrates
with the RuntimePipeline, EventBus, ExecutionGraph, and all seven specialists.
"""

from .registry.server_registry import ServerRegistry
from .registry.trust_manager import TrustLevel, TrustManager
from .registry.health_tracker import HealthState, HealthTracker
from .client.connection_manager import ConnectionManager
from .discovery.discovery_engine import DiscoveryEngine
from .capability.capability_engine import CapabilityEngine
from .execution.execution_engine import MCPExecutionEngine
from .governance.governance_layer import MCPGovernanceLayer
from .verification.verification_pipeline import MCPVerificationPipeline
from .recovery.recovery_engine import MCPRecoveryEngine
from .memory.mcp_memory_store import MCPMemoryStore
from .events.event_publisher import MCPEventPublisher

__all__ = [
    "ServerRegistry",
    "TrustLevel",
    "TrustManager",
    "HealthState",
    "HealthTracker",
    "ConnectionManager",
    "DiscoveryEngine",
    "CapabilityEngine",
    "MCPExecutionEngine",
    "MCPGovernanceLayer",
    "MCPVerificationPipeline",
    "MCPRecoveryEngine",
    "MCPMemoryStore",
    "MCPEventPublisher",
]
