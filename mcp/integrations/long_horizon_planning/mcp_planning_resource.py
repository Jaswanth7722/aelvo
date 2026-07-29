"""MCP planning resource models for long-horizon planning."""

from __future__ import annotations

from typing import List
from pydantic import BaseModel


class PlanningConcept(BaseModel):
    """Base class for planning concepts."""

    pass


class MCPCapabilityRequirement(PlanningConcept):
    """A plan step requires a capability that may not exist yet."""

    required_capability: str
    priority: str = "normal"
    blocking: bool = True


class MCPCapabilityAcquisitionStrategy(PlanningConcept):
    """How to acquire a missing capability."""

    missing_capability: str
    strategies: List[str]
    # e.g.: find_existing_server | build_new_server | request_human_to_provision
