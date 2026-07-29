from enum import Enum
from typing import Dict, List, Optional, Set, Any
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime


class ToolStatus(str, Enum):
    AVAILABLE = "available"
    MISSING = "missing"
    BROKEN = "broken"


class EnvironmentHealth(str, Enum):
    FULLY_OPERATIONAL = "fully_operational"
    DEGRADED = "degraded"
    RESTRICTED = "restricted"
    OFFLINE = "offline"


class GitState(BaseModel):
    branch: str
    is_dirty: bool
    uncommitted_count: int
    has_conflicts: bool
    stash_count: int
    remote_configured: bool
    last_commits: List[str] = Field(default_factory=list)


class CapabilitySnapshot(BaseModel):
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    workspace_path: str
    readable_files: Set[str] = Field(default_factory=set)
    writable_files: Set[str] = Field(default_factory=set)
    tools: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    git: Optional[GitState] = None
    memory_usage_mb: float = 0.0
    disk_free_gb: float = 0.0
    permissions: Dict[str, Any] = Field(default_factory=dict)
    health: EnvironmentHealth = EnvironmentHealth.FULLY_OPERATIONAL
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)
