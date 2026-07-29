"""MCP CLI Command Handler — implements all #mcp commands for the AELVO interactive shell."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List
from ..registry.models import TrustLevel, MCPServerConfig, HealthState
from ..events.event_schemas import DiscoverySource

log = logging.getLogger("aelvo.mcp.cli")


class MCPCommandLineInterface:
    """Handles parsing and execution of all #mcp CLI commands."""

    def __init__(self, platform_components: Dict[str, Any]):
        self.registry = platform_components["registry"]
        self.connection_manager = platform_components["connection_manager"]
        self.discovery_engine = platform_components["discovery_engine"]
        self.capability_engine = platform_components["capability_engine"]
        self.execution_engine = platform_components["execution_engine"]
        self.governance = platform_components["governance"]
        self.verification = platform_components["verification"]
        self.recovery = platform_components["recovery"]
        self.memory = platform_components["memory"]
        self.event_publisher = platform_components["event_publisher"]
        self.health_tracker = platform_components["health_tracker"]

    async def execute(self, command_line: str) -> Dict[str, Any]:
        """Parse and run the command."""
        parts = command_line.strip().split()
        if len(parts) < 2:
            return {
                "status": "REJECTED",
                "msg": "Usage: #mcp <command> [args]. Type '#mcp help' or check commands.",
            }

        cmd = parts[1].lower()
        args = parts[2:]

        try:
            if cmd == "list":
                return await self._cmd_list(args)
            elif cmd == "connect":
                return await self._cmd_connect(args)
            elif cmd == "disconnect":
                return await self._cmd_disconnect(args)
            elif cmd == "health":
                return await self._cmd_health(args)
            elif cmd == "capabilities":
                return await self._cmd_capabilities(args)
            elif cmd == "inspect":
                return await self._cmd_inspect(args)
            elif cmd == "trust":
                return await self._cmd_trust(args)
            elif cmd == "diagnostics":
                return await self._cmd_diagnostics(args)
            elif cmd == "discover":
                return await self._cmd_discover(args)
            elif cmd == "register":
                return await self._cmd_register(args)
            elif cmd == "unregister":
                return await self._cmd_unregister(args)
            elif cmd == "enable":
                return await self._cmd_enable(args)
            elif cmd == "disable":
                return await self._cmd_disable(args)
            elif cmd == "reliability":
                return await self._cmd_reliability(args)
            elif cmd == "events":
                return await self._cmd_events(args)
            elif cmd == "help":
                return self._cmd_help()
            else:
                return {"status": "REJECTED", "msg": f"Unknown #mcp subcommand: {cmd}"}
        except Exception as e:
            log.exception("Error executing MCP command %s", command_line)
            return {"status": "REJECTED", "error": str(e)}

    async def _cmd_list(self, args: List[str]) -> Dict[str, Any]:
        filter_trust = None
        for arg in args:
            if arg.startswith("--filter") and "trust=" in arg:
                filter_trust = arg.split("trust=")[1].lower()

        servers = self.registry.list_servers()
        if filter_trust:
            servers = [s for s in servers if s.trust_level.value == filter_trust]

        print("\n[MCP] Registered Servers:")
        header = f"  {'ID':20s} | {'Trust':10s} | {'Health':12s} | {'Enabled':7s} | {'Transport'}"
        sep = f"  {'-'*20}-+-{'-'*10}-+-{'-'*12}-+-{'-'*7}-+-{'-'*9}"
        print(header)
        print(sep)
        for s in servers:
            connected = self.connection_manager.is_connected(s.id)
            health_str = s.health_state.value
            if connected and health_str == "unknown":
                health_str = "healthy"
            print(
                f"  {s.id:20s} | {s.trust_level.value:10s} | {health_str:12s} | {'yes' if s.enabled else 'no':7s} | {s.transport_type.value}"
            )
        print()
        return {"status": "SUCCESS", "msg": f"Listed {len(servers)} servers"}

    async def _cmd_connect(self, args: List[str]) -> Dict[str, Any]:
        if not args:
            return {"status": "REJECTED", "msg": "Usage: #mcp connect <server_id>"}
        server_id = args[0]
        record = self.registry.get(server_id)
        if not record:
            return {"status": "REJECTED", "msg": f"Server '{server_id}' not found"}

        config = MCPServerConfig(
            id=record.id,
            name=record.name,
            transport_type=record.transport_type,
            connection_config=record.connection_config,
            trust_level=record.trust_level,
            enabled=record.enabled,
        )
        success = await self.connection_manager.connect(config)
        if success:
            self.registry.update_health(server_id, HealthState.HEALTHY)
            return {"status": "SUCCESS", "msg": f"Connected to server '{server_id}'"}
        else:
            return {"status": "REJECTED", "msg": f"Failed to connect to server '{server_id}'"}

    async def _cmd_disconnect(self, args: List[str]) -> Dict[str, Any]:
        if not args:
            return {"status": "REJECTED", "msg": "Usage: #mcp disconnect <server_id>"}
        server_id = args[0]
        success = await self.connection_manager.disconnect(server_id)
        if success:
            self.registry.update_health(server_id, HealthState.UNKNOWN)
            return {"status": "SUCCESS", "msg": f"Disconnected server '{server_id}'"}
        return {"status": "REJECTED", "msg": f"Server '{server_id}' was not connected"}

    async def _cmd_health(self, args: List[str]) -> Dict[str, Any]:
        if args:
            server_id = args[0]
            record = self.registry.get(server_id)
            if not record:
                return {"status": "REJECTED", "msg": f"Server '{server_id}' not found"}
            print(f"\n[MCP] Server Health: {server_id}")
            print(f"  Status:  {record.health_state.value.upper()}")
            print(f"  Enabled: {'Yes' if record.enabled else 'No'}")
            print(f"  Seen:    {record.last_seen or 'Never'}")
            print()
            return {"status": "SUCCESS", "msg": f"Health of '{server_id}' is {record.health_state.value}"}

        print("\n[MCP] Health Summary:")
        for s in self.registry.list_servers():
            print(f"  - {s.id:20s}: {s.health_state.value.upper()}")
        print()
        return {"status": "SUCCESS"}

    async def _cmd_capabilities(self, args: List[str]) -> Dict[str, Any]:
        if args:
            server_id = args[0]
            record = self.registry.get(server_id)
            if not record:
                return {"status": "REJECTED", "msg": f"Server '{server_id}' not found"}
            print(f"\n[MCP] Capabilities for {server_id}:")
            profile = record.capabilities
            print(f"  Tools ({len(profile.tools)}):")
            for t in profile.tools:
                print(f"    - {t.name}: {t.description}")
            print(f"  Prompts ({len(profile.prompts)}):")
            for p in profile.prompts:
                print(f"    - {p.name}: {p.description}")
            print()
            return {"status": "SUCCESS"}

        print("\n[MCP] All Capabilities:")
        tools = self.capability_engine.get_tool_catalog().list_all()
        for tname, servers in tools.items():
            print(f"  - Tool '{tname}' supported by: {', '.join(servers)}")
        print()
        return {"status": "SUCCESS"}

    async def _cmd_inspect(self, args: List[str]) -> Dict[str, Any]:
        if not args:
            return {"status": "REJECTED", "msg": "Usage: #mcp inspect <server_id>"}
        server_id = args[0]
        record = self.registry.get(server_id)
        if not record:
            return {"status": "REJECTED", "msg": f"Server '{server_id}' not found"}
        print(f"\n[MCP] Inspecting {server_id}:")
        print(json.dumps(record.model_dump(), default=str, indent=2))
        print()
        return {"status": "SUCCESS"}

    async def _cmd_trust(self, args: List[str]) -> Dict[str, Any]:
        if len(args) < 2:
            return {"status": "REJECTED", "msg": "Usage: #mcp trust <server_id> <level>"}
        server_id, level_str = args[0], args[1].lower()
        record = self.registry.get(server_id)
        if not record:
            return {"status": "REJECTED", "msg": f"Server '{server_id}' not found"}

        try:
            target_level = TrustLevel(level_str)
        except ValueError:
            return {"status": "REJECTED", "msg": f"Invalid trust level: {level_str}"}

        success = self.registry.update_trust(server_id, target_level)
        if success:
            return {"status": "SUCCESS", "msg": f"Trust level for '{server_id}' set to {level_str}"}
        return {"status": "REJECTED", "msg": "Failed to update trust level"}

    async def _cmd_diagnostics(self, args: List[str]) -> Dict[str, Any]:
        print("\n[MCP] Diagnostics Report:")
        print(f"  Connected Servers: {len(self.connection_manager.list_connected())}")
        print(f"  Audit Logs:        {len(self.governance._audit_logger.get_records())}")
        gaps = await self.capability_engine.get_capability_gaps()
        print(f"  Capability Gaps:   {len(gaps)}")
        for gap in gaps:
            print(f"    - Task type: {gap.get('task_type')}, Tool: {gap.get('tool_name')}")
        print()
        return {"status": "SUCCESS"}

    async def _cmd_discover(self, args: List[str]) -> Dict[str, Any]:
        print("\n[MCP] Scanning discovery sources...")
        results = await self.discovery_engine.discover_all()
        total = sum(len(v) for v in results.values())
        print(f"  Discovered {total} servers.")
        for src, servers in results.items():
            print(f"    - {src}: {servers}")
        print()
        return {"status": "SUCCESS", "msg": f"Discovered {total} servers"}

    async def _cmd_register(self, args: List[str]) -> Dict[str, Any]:
        if not args:
            return {"status": "REJECTED", "msg": "Usage: #mcp register <config_path>"}
        config_path = args[0]
        if not os.path.exists(config_path):
            return {"status": "REJECTED", "msg": f"File not found: {config_path}"}

        source = self.discovery_engine.get_source(DiscoverySource.CONFIG)
        if source:
            try:
                with open(config_path, "r") as f:
                    if config_path.endswith(".json"):
                        data = json.load(f)
                    else:
                        import yaml
                        data = yaml.safe_load(f)

                servers = data.get("servers", [])
                registered = []
                for s in servers:
                    sid = source._register_from_config(s)
                    if sid:
                        registered.append(sid)
                return {"status": "SUCCESS", "msg": f"Registered {len(registered)} servers: {registered}"}
            except Exception as e:
                return {"status": "REJECTED", "msg": f"Error registering: {e}"}
        return {"status": "REJECTED", "msg": "Config discovery source not available"}

    async def _cmd_unregister(self, args: List[str]) -> Dict[str, Any]:
        if not args:
            return {"status": "REJECTED", "msg": "Usage: #mcp unregister <server_id>"}
        server_id = args[0]
        success = self.registry.unregister(server_id)
        if success:
            return {"status": "SUCCESS", "msg": f"Unregistered server '{server_id}'"}
        return {"status": "REJECTED", "msg": f"Server '{server_id}' not found in registry"}

    async def _cmd_enable(self, args: List[str]) -> Dict[str, Any]:
        if not args:
            return {"status": "REJECTED", "msg": "Usage: #mcp enable <server_id>"}
        server_id = args[0]
        success = self.registry.enable(server_id)
        if success:
            return {"status": "SUCCESS", "msg": f"Enabled server '{server_id}'"}
        return {"status": "REJECTED", "msg": f"Server '{server_id}' not found"}

    async def _cmd_disable(self, args: List[str]) -> Dict[str, Any]:
        if not args:
            return {"status": "REJECTED", "msg": "Usage: #mcp disable <server_id>"}
        server_id = args[0]
        success = self.registry.disable(server_id)
        if success:
            await self.connection_manager.disconnect(server_id)
            return {"status": "SUCCESS", "msg": f"Disabled and disconnected server '{server_id}'"}
        return {"status": "REJECTED", "msg": f"Server '{server_id}' not found"}

    async def _cmd_reliability(self, args: List[str]) -> Dict[str, Any]:
        print("\n[MCP] Server Reliability Scores:")
        tracker = self.memory.reliability_tracker
        for s in self.registry.list_servers():
            score = await tracker.get_reliability_score(s.id)
            print(f"  - {s.id:20s}: {score:.4f}")
        print()
        return {"status": "SUCCESS"}

    async def _cmd_events(self, args: List[str]) -> Dict[str, Any]:
        tail = 10
        for arg in args:
            if arg.startswith("--tail"):
                try:
                    parts = arg.split("=")
                    if len(parts) > 1:
                        tail = int(parts[1])
                    else:
                        tail = int(args[args.index(arg) + 1])
                except Exception:
                    pass

        # Stub events for tailing
        print(f"\n[MCP] Tailing recent events (cap={tail}):")
        print("  No recent event log stream active.")
        print()
        return {"status": "SUCCESS"}

    def _cmd_help(self) -> Dict[str, Any]:
        print("\n" + "=" * 60)
        print("  AELVO MCP Subsystem — CLI Commands")
        print("=" * 60)
        print("  #mcp list                         — List all registered servers")
        print("  #mcp list --filter trust=TRUSTED  — Filter servers by trust level")
        print("  #mcp connect <server_id>          — Connect a registered server")
        print("  #mcp disconnect <server_id>       — Disconnect a server")
        print("  #mcp health                       — Health summary for all servers")
        print("  #mcp health <server_id>           — Detailed health for one server")
        print("  #mcp capabilities                 — List all available tools")
        print("  #mcp capabilities <server_id>     — Tools for a specific server")
        print("  #mcp inspect <server_id>          — Full server metadata dump")
        print("  #mcp trust <server_id> <level>    — Set server trust level")
        print("  #mcp diagnostics                  — Full platform diagnostics")
        print("  #mcp discover                     — Trigger discovery scan")
        print("  #mcp register <config_path>       — Register from config file")
        print("  #mcp unregister <server_id>       — Remove server from registry")
        print("  #mcp enable <server_id>           — Enable a disabled server")
        print("  #mcp disable <server_id>          — Disable without removing")
        print("  #mcp reliability                  — Show reliability scores")
        print("  #mcp events [--tail N]            — Tail recent MCP events")
        print("=" * 60 + "\n")
        return {"status": "SUCCESS"}
