from __future__ import annotations
from enum import Enum
from typing import Dict, List, Optional, Any, Set
from pydantic import BaseModel, Field
from datetime import datetime, timezone


class NodeType(str, Enum):
    MEMORY_QUERY = "memory_query"
    TOOL_CALL = "tool_call"
    SPECIALIST_CALL = "specialist_call"
    VERIFICATION = "verification"
    DECISION = "decision"
    SYNTHESIS = "synthesis"


class Criticality(str, Enum):
    CRITICAL = "critical"
    IMPORTANT = "important"
    OPTIONAL = "optional"


class NodeState(str, Enum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    BLOCKED = "blocked"
    RETRYING = "retrying"


class RetryDelayStrategy(str, Enum):
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"


class EdgeConditionType(str, Enum):
    UNCONDITIONAL = "unconditional"
    OUTPUT_MATCHES = "output_matches"
    FIELD_COMPARISON = "field_comparison"
    STATUS_IS = "status_is"


class OutputContract(BaseModel):
    fields: Dict[str, str] = Field(default_factory=dict, description="field_name -> type_string")
    required_fields: List[str] = Field(default_factory=list)

    def validate_output(self, output: Dict[str, Any]) -> tuple[bool, str]:
        for field in self.required_fields:
            if field not in output:
                return False, f"Missing required field: {field}"
            if output[field] is None:
                return False, f"Required field {field} is None"
        return True, ""


class RetryPolicy(BaseModel):
    max_retries: int = 2
    delay_strategy: RetryDelayStrategy = RetryDelayStrategy.EXPONENTIAL
    base_delay_seconds: float = 1.0
    retryable_failure_types: List[str] = Field(default_factory=lambda: ["timeout", "rate_limit", "lock_contention"])

    def compute_delay(self, attempt: int) -> float:
        if self.delay_strategy == RetryDelayStrategy.FIXED:
            return self.base_delay_seconds
        elif self.delay_strategy == RetryDelayStrategy.LINEAR:
            return self.base_delay_seconds * (attempt + 1)
        return min(60.0, self.base_delay_seconds * (2 ** attempt))

    def is_retryable(self, failure_reason: str) -> bool:
        r = failure_reason.lower()
        # Normalize: "rate limit" should match "rate_limit"
        r_normalized = r.replace(" ", "_")
        for ftype in self.retryable_failure_types:
            if ftype in r or ftype in r_normalized:
                return True
        return False


class ArgumentRef(BaseModel):
    """Reference to an upstream node's output field using path syntax: node_002.output.file_path"""
    node_id: str = ""
    field_path: str = ""

    @classmethod
    def parse(cls, ref: str) -> Optional[ArgumentRef]:
        parts = ref.split(".", 2)
        if len(parts) >= 3 and parts[1] == "output":
            return cls(node_id=parts[0], field_path=parts[2])
        return None


class EdgeCondition(BaseModel):
    condition_type: EdgeConditionType = EdgeConditionType.UNCONDITIONAL
    field_path: str = ""
    operator: str = ""  # >, <, ==, !=, in, contains
    value: Any = None

    def evaluate(self, node_output: Dict[str, Any]) -> bool:
        if self.condition_type == EdgeConditionType.UNCONDITIONAL:
            return True
        if self.condition_type == EdgeConditionType.STATUS_IS:
            actual = node_output.get("status", "")
            return actual == self.value
        value = self._resolve_field(node_output, self.field_path)
        if self.condition_type == EdgeConditionType.FIELD_COMPARISON:
            return self._compare(value, self.operator, self.value)
        if self.condition_type == EdgeConditionType.OUTPUT_MATCHES:
            return value == self.value
        return True

    def _resolve_field(self, data: Dict[str, Any], path: str) -> Any:
        return _resolve_field_path(data, path)

    def _compare(self, left: Any, op: str, right: Any) -> bool:
        try:
            if op == "==": return left == right
            if op == "!=": return left != right
            if op == ">": return float(left) > float(right)
            if op == "<": return float(left) < float(right)
            if op == ">=": return float(left) >= float(right)
            if op == "<=": return float(left) <= float(right)
            if op == "in": return left in right if isinstance(right, (list, str)) else False
            if op == "contains": return right in left if isinstance(left, (list, str)) else False
        except (TypeError, ValueError):
            return False
        return False


def _resolve_field_path(data: Dict[str, Any], path: str) -> Any:
    """Shared field resolution — navigate a dot-separated path into a nested dict."""
    parts = path.split(".")
    current = data
    for part in parts:
        if isinstance(current, dict):
            if part in current:
                current = current[part]
            else:
                return None
        else:
            return None
    return current


class DataTransformer(BaseModel):
    mappings: Dict[str, str] = Field(default_factory=dict, description="target_arg -> source_field_path")

    def apply(self, source_output: Dict[str, Any]) -> Dict[str, Any]:
        result = {}
        for target_arg, source_path in self.mappings.items():
            value = _resolve_field_path(source_output, source_path)
            result[target_arg] = value
        return result


class ExecutionEdge(BaseModel):
    id: str
    source_node_id: str
    target_node_id: str
    condition: EdgeCondition = Field(default_factory=EdgeCondition)
    data_transformer: DataTransformer = Field(default_factory=DataTransformer)


class ExecutionNode(BaseModel):
    id: str
    description: str = ""
    node_type: NodeType = NodeType.TOOL_CALL
    criticality: Criticality = Criticality.IMPORTANT
    specialist: str = ""
    tool_name: str = ""
    args: Dict[str, Any] = Field(default_factory=dict)
    state: NodeState = NodeState.PENDING
    output_contract: OutputContract = Field(default_factory=OutputContract)
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    steps_consumed: int = 0
    estimated_steps: int = 1
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    history: List[Dict[str, Any]] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

    def add_history(self, event: str, detail: str = ""):
        self.history.append({
            "event": event,
            "detail": detail,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })


class ExecutionPlan(BaseModel):
    id: str
    task_description: str = ""
    nodes: Dict[str, ExecutionNode] = Field(default_factory=dict)
    edges: List[ExecutionEdge] = Field(default_factory=list)
    entry_node_id: str = ""
    exit_node_ids: List[str] = Field(default_factory=list)
    total_budget: int = 30
    context: Dict[str, Any] = Field(default_factory=dict)
    critical_path: List[str] = Field(default_factory=list)
    parallel_branches: List[List[str]] = Field(default_factory=list)
    pattern_source: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

    def add_node(self, node: ExecutionNode):
        self.nodes[node.id] = node

    def add_edge(self, edge: ExecutionEdge):
        self.edges.append(edge)

    def get_node(self, node_id: str) -> Optional[ExecutionNode]:
        return self.nodes.get(node_id)

    def get_outgoing_edges(self, node_id: str) -> List[ExecutionEdge]:
        return [e for e in self.edges if e.source_node_id == node_id]

    def get_incoming_edges(self, node_id: str) -> List[ExecutionEdge]:
        return [e for e in self.edges if e.target_node_id == node_id]

    def get_dependency_ids(self, node_id: str) -> List[str]:
        return [e.source_node_id for e in self.edges if e.target_node_id == node_id]

    def get_dependent_ids(self, node_id: str) -> List[str]:
        return [e.target_node_id for e in self.edges if e.source_node_id == node_id]

    def topological_sort(self) -> List[str]:
        in_degree: Dict[str, int] = {nid: len(self.get_dependency_ids(nid)) for nid in self.nodes}
        queue = [nid for nid, deg in in_degree.items() if deg == 0]
        result = []
        while queue:
            nid = queue.pop(0)
            result.append(nid)
            for dep_id in self.get_dependent_ids(nid):
                in_degree[dep_id] -= 1
                if in_degree[dep_id] == 0:
                    queue.append(dep_id)
        return result

    def get_ready_nodes(self, completed_ids: Set[str]) -> List[str]:
        ready = []
        for nid, node in self.nodes.items():
            if node.state != NodeState.PENDING:
                continue
            deps = self.get_dependency_ids(nid)
            if all(d in completed_ids for d in deps):
                ready.append(nid)
        return ready

    def calculate_critical_path(self) -> List[str]:
        sorted_nodes = self.topological_sort()
        longest_path: Dict[str, List[str]] = {}
        for nid in sorted_nodes:
            deps = self.get_dependency_ids(nid)
            if not deps:
                longest_path[nid] = [nid]
            else:
                best = max(deps, key=lambda d: len(longest_path.get(d, [])))
                longest_path[nid] = longest_path.get(best, []) + [nid]
        if not longest_path:
            return []
        return max(longest_path.values(), key=len)

    def to_terminal_display(self) -> str:
        lines = [
            f"╔══ EXECUTION PLAN: {self.id} ══╗",
            f"  Task: {self.task_description[:60]}",
            f"  Budget: {self.total_budget} steps",
            f"  Nodes: {len(self.nodes)}",
            f"  Critical path: {' -> '.join(self.critical_path)}" if self.critical_path else "",
            "",
            "  NODES:"
        ]
        for nid in self.topological_sort():
            node = self.nodes[nid]
            label = f"  [{node.node_type.value}] {nid}"
            if node.criticality == Criticality.CRITICAL:
                label += " ★"
            lines.append(label)
            lines.append(f"    → {node.description[:50]}")
            deps = self.get_dependency_ids(nid)
            if deps:
                lines.append(f"    depends: {', '.join(deps)}")
            if node.estimated_steps:
                lines.append(f"    est: {node.estimated_steps} steps")

        if self.parallel_branches:
            lines.append("")
            lines.append("  PARALLEL BRANCHES:")
            for i, branch in enumerate(self.parallel_branches):
                lines.append(f"    Branch {i+1}: {', '.join(branch)}")

        lines.append(f"╚══ {'═' * (len(self.id) + 22)}══╝")
        return "\n".join(lines)


class ExecutionPattern(BaseModel):
    id: str
    task_type_signature: str = ""
    graph_topology: Dict[str, Any] = Field(default_factory=dict)
    node_type_sequence: List[str] = Field(default_factory=list)
    timing_stats: Dict[str, Any] = Field(default_factory=dict)
    outcome_stats: Dict[str, Any] = Field(default_factory=dict)
    success_count: int = 0
    failure_count: int = 0
    total_steps_avg: float = 0.0
    importance: float = 0.7
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
    last_used: Optional[datetime] = None

    def similarity_to(self, task_signature: str) -> float:
        from difflib import SequenceMatcher
        return SequenceMatcher(None, self.task_type_signature.lower(), task_signature.lower()).ratio()
