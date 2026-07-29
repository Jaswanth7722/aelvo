"""MCP Repository Lens — indexes and analyzes MCP metadata as part of Repository Intelligence."""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List

log = logging.getLogger("aelvo.mcp.repo_intelligence")


class MCPRepositoryLens:
    """Scans and indexes the repository to extract MCP architecture context.

    Discovers:
    - Configuration files (mcp_servers.yaml, etc.)
    - Local server definitions
    - Tool dependency patterns in the codebase
    """

    def __init__(self, workspace_path: str):
        self.workspace_path = workspace_path

    async def analyze(self) -> Dict[str, Any]:
        """Index the repository for MCP context."""
        configs = self._scan_config_files()
        deps = self._scan_tool_dependencies()

        return {
            "server_definitions": self._extract_definitions(configs),
            "tool_dependencies": deps,
            "capability_requirements": self._extract_requirements(deps),
            "mcp_configuration_files": configs,
            "dependency_graph": self._build_dependency_graph(deps),
        }

    def _scan_config_files(self) -> List[str]:
        """Find configuration files in the workspace matching mcp pattern."""
        files = []
        for root, _, fs in os.walk(self.workspace_path):
            if ".git" in root or ".venv" in root or ".mypy_cache" in root:
                continue
            for f in fs:
                if f in ("mcp.json", "mcp.yaml", "mcp_servers.yaml", "mcp_servers.json"):
                    files.append(os.path.join(root, f))
        return files

    def _scan_tool_dependencies(self) -> List[Dict[str, Any]]:
        """Scan code files for tool executions or dependencies."""
        dependencies = []
        # Regex to match mcp call or tool execute calls in codebase
        # E.g. execute_tool("server", "tool") or client.call_tool("tool")
        call_pattern = re.compile(
            r"(?:execute|call)_tool\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]"
        )

        for root, _, fs in os.walk(self.workspace_path):
            if ".git" in root or ".venv" in root or ".mypy_cache" in root or "mcp" in root:
                continue
            for f in fs:
                if not f.endswith((".py", ".ts", ".js")):
                    continue
                path = os.path.join(root, f)
                try:
                    with open(path, "r", encoding="utf-8", errors="ignore") as file:
                        content = file.read()
                        for match in call_pattern.finditer(content):
                            dependencies.append(
                                {
                                    "file": os.path.relpath(path, self.workspace_path),
                                    "server_id": match.group(1),
                                    "tool_name": match.group(2),
                                }
                            )
                except Exception as e:
                    log.debug("Failed to read %s for tool analysis: %s", path, e)
        return dependencies

    def _extract_definitions(self, config_files: List[str]) -> List[Dict[str, Any]]:
        """Extract server definitions from configuration files."""
        definitions = []
        for path in config_files:
            try:
                if path.endswith(".json"):
                    with open(path, "r") as f:
                        data = json.load(f)
                        servers = data.get("servers", [])
                        definitions.extend(servers)
                elif path.endswith((".yaml", ".yml")):
                    import yaml
                    with open(path, "r") as f:
                        data = yaml.safe_load(f)
                        if data and "servers" in data:
                            definitions.extend(data["servers"])
            except Exception as e:
                log.debug("Failed to extract definitions from %s: %s", path, e)
        return definitions

    def _extract_requirements(self, dependencies: List[Dict[str, Any]]) -> List[str]:
        """Extract unique capability/tool requirements from dependencies."""
        return sorted(list(set(dep["tool_name"] for dep in dependencies)))

    def _build_dependency_graph(self, dependencies: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Build a dictionary mapping file path -> List of tools called."""
        graph = {}
        for dep in dependencies:
            f = dep["file"]
            tool = f"{dep['server_id']}/{dep['tool_name']}"
            if f not in graph:
                graph[f] = []
            if tool not in graph[f]:
                graph[f].append(tool)
        return graph
