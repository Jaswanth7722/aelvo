"""MCPExecutionResult — typed execution result with verification and recovery metadata."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class MCPExecutionResult(BaseModel):
    """Result of an MCP tool execution through the governed pipeline.

    Contains the raw output, verification results, timing, and recovery
    metadata — everything needed for memory persistence and specialist response.
    """
    request_id: str
    specialist_id: str
    server_id: str
    tool_name: str
    success: bool
    output: Any = None
    error: Optional[str] = None
    duration_ms: int = 0
    verification_passed: bool = False
    verification_results: List[Dict[str, Any]] = Field(default_factory=list)
    governance_passed: bool = False
    governance_details: Dict[str, Any] = Field(default_factory=dict)
    recovery_attempted: bool = False
    recovery_successful: bool = False
    trust_level_at_execution: str = ""
    completed_at: datetime = Field(default_factory=datetime.utcnow)
    raw_response: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
