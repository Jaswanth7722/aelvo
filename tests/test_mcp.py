"""Unit tests for the MCP Platform Subsystem in AELVO Omega."""

from __future__ import annotations

import asyncio
import os
import tempfile
import pytest
from typing import AsyncIterator, List

# --- Subsystem Imports ---
from mcp.registry.models import (
    MCPServerConfig, MCPServerRecord, TransportType, TrustLevel, HealthState,
    CapabilityProfile, ToolDefinition
)
from mcp.registry.server_registry import ServerRegistry
from mcp.registry.trust_manager import TrustManager
from mcp.registry.health_tracker import HealthTracker
from mcp.transport.base_transport import BaseTransport, MCPMessage
from mcp.client.connection_manager import ConnectionManager
from mcp.discovery.discovery_engine import DiscoveryEngine
from mcp.events.event_publisher import MCPEventPublisher
from mcp.capability.capability_engine import CapabilityEngine
from mcp.governance.governance_layer import MCPGovernanceLayer
from mcp.verification.verification_pipeline import MCPVerificationPipeline
from mcp.recovery.recovery_engine import MCPRecoveryEngine
from mcp.memory import MCPMemoryStore, ReliabilityTracker, SpecialistPreference, RoutingIntelligence
from mcp.execution.execution_engine import MCPExecutionEngine
from mcp.execution.execution_request import MCPExecutionRequest
from mcp.execution.execution_result import MCPExecutionResult
from mcp.execution.mcp_cli import MCPCommandLineInterface
from mcp.integrations.execution_graph.mcp_nodes import MCPToolNode
from mcp.integrations.execution_graph.node_factory import MCPNodeFactory
from mcp.integrations.specialists import (
    HermesMCPInterface
)
import logging

log = logging.getLogger(__name__)


# ============================================================================
# MOCKS
# ============================================================================

class MockTransport(BaseTransport):
    """Mock transport that simulates standard MCP responses."""

    def __init__(self, should_fail: bool = False):
        self._connected = False
        self._should_fail = should_fail
        self._received_messages: List[MCPMessage] = []
        self._send_queue: asyncio.Queue = asyncio.Queue()

    async def connect(self) -> None:
        if self._should_fail:
            from mcp.transport.stdio_transport import MCPConnectionError
            raise MCPConnectionError("Simulated connection failure")
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False

    async def send(self, message: MCPMessage) -> None:
        self._received_messages.append(message)
        # Simulate response
        if "call" in message.method:
            tool_name = message.params.get("name")
            resp = MCPMessage(
                id=message.id,
                result={"content": [{"type": "text", "text": f"Result from {tool_name}"}]}
            )
            await self._send_queue.put(resp)

    async def receive(self) -> AsyncIterator[MCPMessage]:
        while self._connected:
            try:
                msg = await self._send_queue.get()
                yield msg
            except asyncio.CancelledError:
                break

    @property
    def is_connected(self) -> bool:
        return self._connected

# ============================================================================
# TESTS
# ============================================================================

@pytest.fixture
def temp_db():
    fd, path = tempfile.mkstemp()
    yield path
    os.close(fd)
    try:
        os.remove(path)
    except OSError as _ex:
        log.warning("Silenced exception: %s", _ex)

@pytest.fixture
def mcp_platform(temp_db):
    from mcp.registry.registry_store import RegistryStore
    store = RegistryStore(db_path=temp_db)
    registry = ServerRegistry(store=store)
    
    trust_manager = TrustManager()
    health_tracker = HealthTracker(registry)
    event_publisher = MCPEventPublisher()
    connection_manager = ConnectionManager(event_publisher=event_publisher)
    discovery_engine = DiscoveryEngine(registry, event_publisher=event_publisher)
    capability_engine = CapabilityEngine(registry)
    governance = MCPGovernanceLayer(registry)
    verification = MCPVerificationPipeline(event_publisher=event_publisher)
    memory = MCPMemoryStore(db_path=temp_db)
    recovery = MCPRecoveryEngine(
        registry=registry,
        connection_manager=connection_manager,
        capability_engine=capability_engine,
        health_tracker=health_tracker,
        trust_manager=trust_manager,
        event_publisher=event_publisher
    )
    execution = MCPExecutionEngine(
        registry=registry,
        connection_manager=connection_manager,
        capability_engine=capability_engine,
        governance=governance,
        verification=verification,
        recovery=recovery,
        memory=memory,
        event_publisher=event_publisher,
        health_tracker=health_tracker
    )
    
    components = {
        "registry": registry,
        "connection_manager": connection_manager,
        "discovery_engine": discovery_engine,
        "capability_engine": capability_engine,
        "execution_engine": execution,
        "governance": governance,
        "verification": verification,
        "recovery": recovery,
        "memory": memory,
        "event_publisher": event_publisher,
        "health_tracker": health_tracker,
        "trust_manager": trust_manager
    }
    return components

def test_registry_crud(mcp_platform):
    registry = mcp_platform["registry"]
    config = MCPServerConfig(
        id="test-server",
        name="Test Server",
        transport_type=TransportType.STDIO,
        connection_config={"command": ["echo", "1"]},
        trust_level=TrustLevel.VERIFIED
    )
    
    # Register
    record = registry.register(config)
    assert record.id == "test-server"
    assert record.trust_level == TrustLevel.VERIFIED
    assert record.enabled is True
    
    # Query
    assert len(registry.list_servers()) == 1
    assert registry.get("test-server") is not None
    
    # Update Trust
    registry.update_trust("test-server", TrustLevel.TRUSTED)
    assert registry.get("test-server").trust_level == TrustLevel.TRUSTED
    
    # Disable
    registry.disable("test-server")
    assert registry.get("test-server").enabled is False
    
    # Unregister
    assert registry.unregister("test-server") is True
    assert len(registry.list_servers()) == 0

@pytest.mark.asyncio
async def test_connection_management(mcp_platform):
    connection_manager = mcp_platform["connection_manager"]
    config = MCPServerConfig(
        id="mock-server",
        name="Mock Server",
        transport_type=TransportType.STDIO,
        connection_config={}
    )
    
    # Inject mock transport creation in factory
    from mcp.transport.transport_factory import TransportFactory
    old_create = TransportFactory.create
    TransportFactory.create = lambda cfg: MockTransport()
    
    try:
        connected = await connection_manager.connect(config)
        assert connected is True
        assert connection_manager.is_connected("mock-server") is True
        
        disconnected = await connection_manager.disconnect("mock-server")
        assert disconnected is True
        assert connection_manager.is_connected("mock-server") is False
    finally:
        TransportFactory.create = old_create

@pytest.mark.asyncio
async def test_capability_catalog(mcp_platform):
    registry = mcp_platform["registry"]
    capability_engine = mcp_platform["capability_engine"]
    
    config = MCPServerConfig(
        id="capabilities-server",
        name="Capabilities Server",
        transport_type=TransportType.STDIO,
        connection_config={}
    )
    registry.register(config)
    
    profile = CapabilityProfile(
        server_id="capabilities-server",
        tools=[ToolDefinition(name="test_tool", description="A test tool", input_schema={})]
    )
    
    await capability_engine.refresh_capabilities("capabilities-server", profile)
    assert capability_engine.get_profile("capabilities-server") is not None
    
    servers_with_tool = await capability_engine.find_tool("test_tool")
    assert len(servers_with_tool) == 1
    assert servers_with_tool[0][0].id == "capabilities-server"

@pytest.mark.asyncio
async def test_governance_checks(mcp_platform):
    registry = mcp_platform["registry"]
    governance = mcp_platform["governance"]
    
    config = MCPServerConfig(
        id="gov-server",
        name="Gov Server",
        transport_type=TransportType.STDIO,
        connection_config={},
        trust_level=TrustLevel.SANDBOXED
    )
    registry.register(config)
    
    # Request demanding verified trust on a sandboxed server
    request = MCPExecutionRequest(
        request_id="req-1",
        specialist_id="FORGE",
        server_id="gov-server",
        tool_name="write_file",
        trust_requirement=TrustLevel.VERIFIED
    )
    
    # Should block due to insufficient trust
    result = await governance.check(request)
    assert result.allowed is False
    assert "trust level" in result.reason.lower()

@pytest.mark.asyncio
async def test_verification_pipeline(mcp_platform):
    verification = mcp_platform["verification"]
    
    class FakeResponse:
        def __init__(self, result=None, error=None):
            self.result = result
            self.error = error
        def model_dump(self):
            return {"result": self.result}
            
    response = FakeResponse(result={"content": [{"type": "text", "text": "Valid output"}]})
    
    results = await verification.verify(
        request_id="req-2",
        server_id="test-server",
        tool_name="read_file",
        response=response
    )
    
    assert all(r.passed for r in results)

@pytest.mark.asyncio
async def test_reliability_scoring_and_routing(mcp_platform):
    memory = mcp_platform["memory"]
    registry = mcp_platform["registry"]
    mcp_platform["capability_engine"]
    
    s1 = MCPServerRecord(id="server-1", name="S1", transport_type=TransportType.STDIO, enabled=True, health_state=HealthState.HEALTHY)
    s2 = MCPServerRecord(id="server-2", name="S2", transport_type=TransportType.STDIO, enabled=True, health_state=HealthState.HEALTHY)
    registry.register(s1)
    registry.register(s2)
    
    # Store successful executions for server-1, failure for server-2
    ReliabilityTracker(memory)
    pref = SpecialistPreference(memory)
    
    r1 = MCPExecutionResult(
        request_id="req-s1", specialist_id="ORACLE", server_id="server-1",
        tool_name="search", success=True, duration_ms=200, verification_passed=True
    )
    r2 = MCPExecutionResult(
        request_id="req-s2", specialist_id="ORACLE", server_id="server-2",
        tool_name="search", success=False, duration_ms=2000, verification_passed=False
    )
    
    await memory.store_result(r1)
    await memory.store_result(r2)
    await pref.record_routing_outcome("ORACLE", "server-1", "search", True)
    await pref.record_routing_outcome("ORACLE", "server-2", "search", False)
    
    routing = RoutingIntelligence(memory)
    best = await routing.select_server([s1, s2], "ORACLE", "search")
    assert best is not None
    assert best.id == "server-1"

def test_node_factory():
    node = MCPNodeFactory.create_tool_node(
        node_id="node-1",
        server_id="test-server",
        tool_name="test_tool",
        arguments_template={"path": "{{upstream}}"}
    )
    assert isinstance(node, MCPToolNode)
    assert node.server_id == "test-server"
    assert node.tool_name == "test_tool"

@pytest.mark.asyncio
async def test_specialist_contracts(mcp_platform):
    execution = mcp_platform["execution_engine"]
    registry = mcp_platform["registry"]
    governance = mcp_platform["governance"]
    
    # Setup HERMES interface
    hermes_mcp = HermesMCPInterface(execution, registry)
    
    # HERMES turn call budget limit test
    hermes_mcp.reset_turn()
    
    # Mock transport connection
    from mcp.transport.transport_factory import TransportFactory
    old_create = TransportFactory.create
    TransportFactory.create = lambda cfg: MockTransport()
    
    try:
        config = MCPServerConfig(id="user-db", name="User DB", transport_type=TransportType.STDIO, connection_config={})
        registry.register(config)
        registry.update_health("user-db", HealthState.HEALTHY)
        
        # Enable all in allowlist
        governance._allowlist.clear()
        
        res1 = await hermes_mcp.execute_user_context_query("user-db", "read_profile", {})
        res2 = await hermes_mcp.execute_user_context_query("user-db", "read_preferences", {})
        res3 = await hermes_mcp.execute_user_context_query("user-db", "read_history", {})
        
        assert res1.success is True
        assert res2.success is True
        assert res3.success is False
        assert "turn limit" in res3.error
    finally:
        TransportFactory.create = old_create

@pytest.mark.asyncio
async def test_cli_help(mcp_platform):
    cli = MCPCommandLineInterface(mcp_platform)
    res = await cli.execute("#mcp help")
    assert res["status"] == "SUCCESS"

# ============================================================================
# PATH EXECUTABLE DISCOVERY - SECURITY REGRESSION TESTS
# ============================================================================
# Regression coverage for the HIGH-severity fix: `mcp-*` / `*.mcp` executables
# on PATH must NOT be auto-registered by default (an attacker who can write to
# a PATH directory would otherwise get arbitrary code execution through MCP).
# When opt-in discovery is enabled, candidates must be registered as
# QUARANTINED (inspection only, no execution) until the user promotes them.


def _make_path_executable(tmp_path, name):
    """Create a fake mcp-* executable inside a temp PATH dir."""
    exe = tmp_path / name
    exe.write_text("#!/bin/sh\necho fake-mcp\n", encoding="utf-8")
    exe.chmod(0o755)


@pytest.mark.asyncio
async def test_path_executables_not_registered_by_default(mcp_platform, tmp_path, monkeypatch):
    """Regression: PATH mcp-* executables are NOT auto-registered by default."""
    from mcp.discovery.filesystem_discovery import FilesystemDiscovery

    registry = mcp_platform["registry"]
    _make_path_executable(tmp_path, "mcp-fake-server")
    _make_path_executable(tmp_path, "other-tool.mcp")
    monkeypatch.setenv("PATH", str(tmp_path))

    discovery = FilesystemDiscovery(registry, scan_paths=[], discover_path_executables=False)
    discovered = await discovery.discover()

    # No fs_* server may be discovered or registered without explicit opt-in.
    assert not any(d.startswith("fs_") for d in discovered)
    assert all(not r.id.startswith("fs_") for r in registry.list_servers())
    assert registry.get("fs_mcp-fake-server") is None
    assert registry.get("fs_other-tool.mcp") is None


@pytest.mark.asyncio
async def test_path_executables_quarantined_when_opted_in(mcp_platform, tmp_path, monkeypatch):
    """Regression: opt-in PATH discovery registers candidates as QUARANTINED."""
    from mcp.discovery.filesystem_discovery import FilesystemDiscovery

    registry = mcp_platform["registry"]
    _make_path_executable(tmp_path, "mcp-fake-server")
    monkeypatch.setenv("PATH", str(tmp_path))

    discovery = FilesystemDiscovery(registry, scan_paths=[], discover_path_executables=True)
    discovered = await discovery.discover()

    assert "fs_mcp-fake-server" in discovered
    record = registry.get("fs_mcp-fake-server")
    assert record is not None
    # Registered for inspection only - QUARANTINED blocks execution until the
    # user explicitly promotes the trust level.
    assert record.trust_level == TrustLevel.QUARANTINED
    assert record.enabled is True
    assert "unvetted" in record.tags


def test_discovery_engine_path_executable_flag_wiring(mcp_platform):
    """Regression: DiscoveryEngine defaults PATH discovery off and forwards opt-in."""
    from mcp.discovery.discovery_engine import DiscoveryEngine
    from mcp.events.event_schemas import DiscoverySource

    default_engine = DiscoveryEngine(mcp_platform["registry"])
    assert default_engine.get_source(DiscoverySource.FILESYSTEM).discover_path_executables is False

    optin_engine = DiscoveryEngine(mcp_platform["registry"], discover_path_executables=True)
    assert optin_engine.get_source(DiscoverySource.FILESYSTEM).discover_path_executables is True



@pytest.mark.asyncio
async def test_quarantined_server_promotion_enables_execution(mcp_platform, tmp_path, monkeypatch):
    """Regression: a QUARANTINED PATH-discovered server is blocked from execution
    until the user promotes it (update_trust); afterwards it executes normally.
    """
    from mcp.discovery.filesystem_discovery import FilesystemDiscovery
    from mcp.transport.transport_factory import TransportFactory

    registry = mcp_platform["registry"]
    governance = mcp_platform["governance"]
    execution = mcp_platform["execution_engine"]

    # Discover a PATH executable with opt-in -> registered QUARANTINED.
    _make_path_executable(tmp_path, "mcp-promotable-server")
    monkeypatch.setenv("PATH", str(tmp_path))
    discovery = FilesystemDiscovery(registry, scan_paths=[], discover_path_executables=True)
    discovered = await discovery.discover()
    server_id = "fs_mcp-promotable-server"
    assert server_id in discovered
    assert registry.get(server_id).trust_level == TrustLevel.QUARANTINED

    # Governance must block execution while the server is QUARANTINED.
    governance._allowlist.clear()
    request = MCPExecutionRequest(
        request_id="req-promote-1",
        specialist_id="FORGE",
        server_id=server_id,
        tool_name="read_file",
        trust_requirement=TrustLevel.SANDBOXED,
    )
    blocked = await governance.check(request)
    assert blocked.allowed is False
    assert blocked.trust_level_ok is False

    # The user promotes the server (explicit user action).
    assert registry.update_trust(server_id, TrustLevel.VERIFIED) is True
    assert registry.get(server_id).trust_level == TrustLevel.VERIFIED

    # Governance now allows the same request.
    allowed = await governance.check(request)
    assert allowed.allowed is True
    assert allowed.trust_level_ok is True

    # And it executes normally through the governed pipeline.
    old_create = TransportFactory.create
    TransportFactory.create = lambda cfg: MockTransport()
    try:
        result = await execution.execute(request)
        assert result.success is True
        assert result.governance_passed is True
        assert result.trust_level_at_execution == "verified"
    finally:
        TransportFactory.create = old_create



@pytest.mark.asyncio
async def test_quarantined_path_server_blocked_from_execution(mcp_platform, tmp_path, monkeypatch):
    """Regression: a QUARANTINED PATH-discovered server is blocked end-to-end
    by the governance/trust layer - execution must not reach the transport.
    """
    from mcp.discovery.filesystem_discovery import FilesystemDiscovery
    from mcp.transport.transport_factory import TransportFactory

    registry = mcp_platform["registry"]
    governance = mcp_platform["governance"]
    execution = mcp_platform["execution_engine"]

    # Discover a PATH executable with opt-in -> registered QUARANTINED.
    _make_path_executable(tmp_path, "mcp-blocked-server")
    monkeypatch.setenv("PATH", str(tmp_path))
    discovery = FilesystemDiscovery(registry, scan_paths=[], discover_path_executables=True)
    discovered = await discovery.discover()
    server_id = "fs_mcp-blocked-server"
    assert server_id in discovered
    record = registry.get(server_id)
    assert record is not None
    assert record.trust_level == TrustLevel.QUARANTINED

    # Open the allowlist so the trust level is the only gate under test.
    governance._allowlist.clear()

    request = MCPExecutionRequest(
        request_id="req-quarantine-1",
        specialist_id="FORGE",
        server_id=server_id,
        tool_name="read_file",
        trust_requirement=TrustLevel.SANDBOXED,
    )

    # Even with a working transport available, execution must be refused:
    # QUARANTINED is inspection-only, below the SANDBOXED requirement.
    old_create = TransportFactory.create
    TransportFactory.create = lambda cfg: MockTransport()
    try:
        result = await execution.execute(request)
    finally:
        TransportFactory.create = old_create

    assert result.success is False
    assert result.governance_passed is False
    assert "Governance blocked" in result.error
    assert "trust level" in result.error

    # Direct governance check agrees: the trust gate itself denies.
    blocked = await governance.check(request)
    assert blocked.allowed is False
    assert blocked.trust_level_ok is False

