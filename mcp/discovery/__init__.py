"""MCP Discovery — populate the registry from multiple sources with minimal user intervention."""
from .discovery_engine import DiscoveryEngine
from .filesystem_discovery import FilesystemDiscovery
from .config_discovery import ConfigDiscovery
from .runtime_discovery import RuntimeDiscovery
from .manual_registration import ManualRegistration

__all__ = [
    "DiscoveryEngine",
    "FilesystemDiscovery",
    "ConfigDiscovery",
    "RuntimeDiscovery",
    "ManualRegistration",
]
