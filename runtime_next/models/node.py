from __future__ import annotations
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime, timezone
from .plan import NodeState, NodeType, Criticality, RetryPolicy, OutputContract


class DangerClassification(str, Enum):
    SAFE = "safe"
    REVERSIBLE = "reversible"
    DESTRUCTIVE = "destructive"


class NodeDefinition(BaseModel):
    id: str = Field(..., description="Unique ID derived from content hash")
    description: str = ""
    node_type: NodeType = NodeType.TOOL_CALL
    criticality: Criticality = Criticality.IMPORTANT
    specialist: str = ""
    tool_name: str = ""
    tools: List[str] = Field(default_factory=list)
    args: Dict[str, Any] = Field(default_factory=dict)
    state: NodeState = NodeState.PENDING
    dependencies: List[str] = Field(default_factory=list)
    dependents: List[str] = Field(default_factory=list)
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    output_contract: OutputContract = Field(default_factory=OutputContract)
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    retry_budget: int = 3
    retry_count: int = 0
    steps_consumed: int = 0
    estimated_steps: int = 1
    danger: DangerClassification = DangerClassification.SAFE
    files: List[str] = Field(default_factory=list)
    history: List[Dict[str, Any]] = Field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def add_history(self, from_state: NodeState, to_state: NodeState, reason: str = ""):
        entry = {
            "from": from_state.value if isinstance(from_state, NodeState) else from_state,
            "to": to_state.value if isinstance(to_state, NodeState) else to_state,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "reason": reason
        }
        self.history.append(entry)
        self.updated_at = datetime.now(timezone.utc)

    def can_retry(self) -> bool:
        return self.retry_count < self.retry_budget

    def next_backoff(self) -> float:
        return self.retry_policy.compute_delay(self.retry_count)
