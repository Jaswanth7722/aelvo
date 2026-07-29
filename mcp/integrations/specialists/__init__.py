"""Specialists integrations for the MCP Subsystem."""

from .hermes_mcp import HermesMCPInterface
from .architect_mcp import ArchitectMCPInterface, MCPCapabilityAcquisitionStep
from .oracle_mcp import OracleMCPInterface
from .forge_mcp import ForgeMCPInterface
from .sentinel_mcp import SentinelMCPInterface
from .terminus_mcp import TerminusMCPInterface
from .herald_mcp import HeraldMCPInterface

__all__ = [
    "HermesMCPInterface",
    "ArchitectMCPInterface",
    "MCPCapabilityAcquisitionStep",
    "OracleMCPInterface",
    "ForgeMCPInterface",
    "SentinelMCPInterface",
    "TerminusMCPInterface",
    "HeraldMCPInterface",
]
