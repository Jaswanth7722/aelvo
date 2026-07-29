"""MCPExecutionRequest — typed execution request with governance and routing context."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict
from pydantic import BaseModel, Field

from ..events.event_schemas import ExecutionPriority
from ..registry.models import TrustLevel


class MCPExecutionRequest(BaseModel):
    """A fully specified request to execute an MCP tool.

    Contains all context needed for governance checks, routing,
    execution, and verification — no external lookups required.
    """
    request_id: str
    specialist_id: str               # Which specialist is requesting
    server_id: str                   # Target server
    tool_name: str                   # Tool to execute
    arguments: Dict[str, Any] = Field(default_factory=dict)  # Pre-validated arguments
    timeout_ms: int = 30000          # Hard timeout
    trust_requirement: TrustLevel = TrustLevel.SANDBOXED  # Minimum trust level
    priority: ExecutionPriority = ExecutionPriority.NORMAL
    metadata: Dict[str, Any] = Field(default_factory=dict)  # Tracing, correlation IDs
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str = ""

    def model_post_init(self, __context: Any) -> None:
        if not self.created_by and self.specialist_id:
            self.created_by = self.specialist_id
