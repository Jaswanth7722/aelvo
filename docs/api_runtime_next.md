# RuntimeNext API Documentation

> **Package:** `runtime_next`
> **Purpose:** Production-grade runtime subsystem for AELVO OMEGA — reliability, governance, scaling, monitoring, and security.

---

## Table of Contents

1. [Package Overview](#1-package-overview)
2. [`runtime_next.models` — Data Models](#2-runtimenextmodels--data-models)
   - [`models.plan` — Execution Plan, Node, Edge, Pattern](#21-modelsplan)
   - [`models.node` — NodeDefinition](#22-modelsnode)
   - [`models.events` — Event Types & Event Models](#23-modelsevents)
   - [`models.capability` — CapabilitySnapshot](#24-modelscapability)
3. [`runtime_next.engine` — Execution Engine](#3-runtimenextengine--execution-engine)
4. [`runtime_next.events` — Event Bus](#4-runtimenextevents--event-bus)
5. [`runtime_next.capability` — Capability Registry](#5-runtimenextcapability--capability-registry)
6. [`runtime_next.verification` — Verification & Self-Healing](#6-runtimenextverification--verification--self-healing)
   - [`types` — Verification Type System](#61-types)
   - [`pipeline` — Verification Pipeline](#62-pipeline)
   - [`classifier` — Failure Classifier](#63-classifier)
   - [`recovery` — Recovery Strategy Engine](#64-recovery)
   - [`retry_safety` — Retry Safety Engine](#65-retry_safety)
   - [`injector` — Recovery Node Injector](#66-injector)
   - [`consistency` — Runtime Consistency Validator](#67-consistency)
   - [`memory` — Learned Recovery Memory](#68-memory)
   - [`events` — Replayable Verification Events](#69-events)
   - [`governance` — Recovery Governance](#610-governance)
   - [`sandbox_verifier` — Sandbox Verifier](#611-sandbox_verifier)
   - [`code_verifier` — Code Verifier](#612-code_verifier)
   - [`graph_verifier` — Graph Consistency Verifier](#613-graph_verifier)
   - [`additional_verifiers` — Additional Verifiers](#614-additional_verifiers)
   - [`driven_recovery` — Verification-Driven Recovery Pipeline](#615-driven_recovery)
7. [`runtime_next.recovery` — Recovery Engines](#7-runtimenextrecovery--recovery-engines)
   - [`RecoveryEngine` — Central Coordinator](#71-recoveryengine)
   - [`ConsensusRecoveryEngine` — Consensus-Level Recovery](#72-consensusrecoveryengine)
   - [`SpecialistRecoveryEngine` — Specialist-Level Recovery](#73-specialistrecoveryengine)
   - [`TaskRecoveryEngine` — Task-Level Recovery](#74-taskrecoveryengine)
8. [`runtime_next.governance` — Governance & Policy](#8-runtimenextgovernance--governance--policy)
   - [`GovernancePolicyEngine` — Policy Engine](#81-governancepolicyengine)
   - [`RecoveryGovernanceHooks` — Governance Hooks](#82-recoverygovernancehooks)
   - [`create_default_policies` — Default Policies Factory](#83-create_default_policies)
9. [`runtime_next.monitoring` — Monitoring & Observability](#9-runtimenextmonitoring--monitoring--observability)
   - [`RuntimeMetricsCollector` — Metrics Collection](#91-runtimemetricscollector)
   - [`RuntimeHealthMonitor` — Health Monitoring](#92-runtimehealthmonitor)
   - [`AlertManager` — Alert Management](#93-alertmanager)
   - [`RuntimeDashboard` — Dashboard Snapshots](#94-runtimedashboard)
   - [`RuntimeCLI` — CLI Status Commands](#95-runtimecli)
10. [`runtime_next.security` — Security Hardening](#10-runtimenextsecurity--security-hardening)
    - [`RuntimeSecurityScanner` — Security Scanning](#101-runtimesecurityscanner)
    - [`PolicyAuditTrail` — Policy Audit Trail](#102-policyaudittrail)
    - [`SandboxIntegrityVerifier` — Sandbox Integrity](#103-sandboxintegrityverifier)
    - [`RuntimeSecurityOrchestrator` — Security Orchestrator](#104-runtimesecurityorchestrator)
11. [`runtime_next.scaling` — Scaling & Resource Management](#11-runtimenextscaling--scaling--resource-management)
    - [`ResourcePool` — Async Resource Pool](#111-resourcepool)
    - [`ConnectionPool` — Database Connection Pool](#112-connectionpool)
    - [`ResourcePoolManager` — Pool Manager](#113-resourcepoolmanager)
    - [`AsyncPipeline` — Async Pipeline Executor](#114-asyncpipeline)
    - [`PipelineBuilder` — Pipeline Builder](#115-pipelinebuilder)
    - [`BatchProcessor` — Batch Processing](#116-batchprocessor)
    - [`AsyncBatchIterator` — Streaming Batch Iterator](#117-asyncbatchiterator)
12. [`runtime_next.plan` — Planning & Architecture](#12-runtimenextplan--planning--architecture)
    - [`ArchitectPlan` — Plan Data Model](#121-architectplan)
    - [`PlanBuilder` — Execution Plan Builder](#122-planbuilder)
    - [`ArchitectOrchestrator` — Master Planning Intelligence](#123-architectorchestrator)
    - [`ArchitectIntelligenceCoordinator` — Strategic Domains](#124-architectintelligencecoordinator)
    - [`PlanCalibrationSystem` — Calibration & Learning](#125-plancalibrationsystem)
    - [`SubBudgetAllocator` — Budget Allocation](#126-subbudgetallocator)

---

## 1. Package Overview

```python
from runtime_next import (
    CapabilityRegistry,    # Environment capability registry
    EventBus,              # Async typed event bus
    RecoveryEngine,        # Central recovery coordinator
    models,                # Data models (plan, node, events, capability)
    scaling,               # Resource pools, pipelines, batch processing
    governance,            # Policy engine, recovery hooks
    monitoring,            # Metrics, health, alerts, dashboard, CLI
    security,              # Scanner, audit trail, integrity verifier
)
```

---

## 2. `runtime_next.models` — Data Models

### 2.1 `models.plan`

**File:** `runtime_next/models/plan.py`

#### Enums

| Enum | Values |
|------|--------|
| `NodeType` | `MEMORY_QUERY`, `TOOL_CALL`, `SPECIALIST_CALL`, `VERIFICATION`, `DECISION`, `SYNTHESIS` |
| `Criticality` | `CRITICAL`, `IMPORTANT`, `OPTIONAL` |
| `NodeState` | `PENDING`, `READY`, `RUNNING`, `COMPLETED`, `FAILED`, `SKIPPED`, `BLOCKED`, `RETRYING` |
| `RetryDelayStrategy` | `FIXED`, `LINEAR`, `EXPONENTIAL` |
| `EdgeConditionType` | `UNCONDITIONAL`, `OUTPUT_MATCHES`, `FIELD_COMPARISON`, `STATUS_IS` |

#### `OutputContract`

```python
@dataclass
class OutputContract:
    fields: Dict[str, str]                 # field_name -> type_string
    required_fields: List[str]

    def validate_output(self, output: Dict[str, Any]) -> Tuple[bool, str]
```

#### `RetryPolicy`

```python
@dataclass
class RetryPolicy:
    max_retries: int = 2
    delay_strategy: RetryDelayStrategy = RetryDelayStrategy.EXPONENTIAL
    base_delay_seconds: float = 1.0
    retryable_failure_types: List[str] = ["timeout", "rate_limit", "lock_contention"]

    def compute_delay(self, attempt: int) -> float
    def is_retryable(self, failure_reason: str) -> bool
```

#### `ArgumentRef`

```python
@dataclass
class ArgumentRef:
    node_id: str
    field_path: str

    @classmethod
    def parse(cls, ref: str) -> Optional[ArgumentRef]
```

#### `EdgeCondition`

```python
@dataclass
class EdgeCondition:
    condition_type: EdgeConditionType = EdgeConditionType.UNCONDITIONAL
    field_path: str = ""
    operator: str = ""            # >, <, ==, !=, in, contains
    value: Any = None

    def evaluate(self, node_output: Dict[str, Any]) -> bool
```

#### `DataTransformer`

```python
@dataclass
class DataTransformer:
    mappings: Dict[str, str]            # target_arg -> source_field_path

    def apply(self, source_output: Dict[str, Any]) -> Dict[str, Any]
```

#### `ExecutionEdge`

```python
@dataclass
class ExecutionEdge:
    id: str
    source_node_id: str
    target_node_id: str
    condition: EdgeCondition = EdgeCondition()
    data_transformer: DataTransformer = DataTransformer()
```

#### `ExecutionNode`

```python
@dataclass
class ExecutionNode:
    id: str
    description: str = ""
    node_type: NodeType = NodeType.TOOL_CALL
    criticality: Criticality = Criticality.IMPORTANT
    specialist: str = ""
    tool_name: str = ""
    args: Dict[str, Any] = {}
    state: NodeState = NodeState.PENDING
    output_contract: OutputContract = OutputContract()
    retry_policy: RetryPolicy = RetryPolicy()
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    steps_consumed: int = 0
    estimated_steps: int = 1
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    history: List[Dict[str, Any]] = []
    created_at: datetime = datetime.utcnow()

    def add_history(self, event: str, detail: str = "")
```

#### `ExecutionPlan`

```python
@dataclass
class ExecutionPlan:
    id: str
    task_description: str = ""
    nodes: Dict[str, ExecutionNode] = {}
    edges: List[ExecutionEdge] = []
    entry_node_id: str = ""
    exit_node_ids: List[str] = []
    total_budget: int = 30
    context: Dict[str, Any] = {}
    critical_path: List[str] = []
    parallel_branches: List[List[str]] = []
    pattern_source: Optional[str] = None
    created_at: datetime = datetime.utcnow()

    def add_node(self, node: ExecutionNode)
    def add_edge(self, edge: ExecutionEdge)
    def get_node(self, node_id: str) -> Optional[ExecutionNode]
    def get_outgoing_edges(self, node_id: str) -> List[ExecutionEdge]
    def get_incoming_edges(self, node_id: str) -> List[ExecutionEdge]
    def get_dependency_ids(self, node_id: str) -> List[str]
    def get_dependent_ids(self, node_id: str) -> List[str]
    def topological_sort(self) -> List[str]
    def get_ready_nodes(self, completed_ids: Set[str]) -> List[str]
    def calculate_critical_path(self) -> List[str]
    def to_terminal_display(self) -> str
```

#### `ExecutionPattern`

```python
@dataclass
class ExecutionPattern:
    id: str
    task_type_signature: str = ""
    graph_topology: Dict[str, Any] = {}
    node_type_sequence: List[str] = []
    timing_stats: Dict[str, Any] = {}
    outcome_stats: Dict[str, Any] = {}
    success_count: int = 0
    failure_count: int = 0
    total_steps_avg: float = 0.0
    importance: float = 0.7
    created_at: datetime = datetime.utcnow()
    last_used: Optional[datetime] = None

    def similarity_to(self, task_signature: str) -> float
```

---

### 2.2 `models.node`

**File:** `runtime_next/models/node.py`

#### `DangerClassification`

```python
class DangerClassification(str, Enum):
    SAFE = "safe"
    REVERSIBLE = "reversible"
    DESTRUCTIVE = "destructive"
```

#### `NodeDefinition`

```python
@dataclass
class NodeDefinition:
    id: str
    description: str = ""
    node_type: NodeType = NodeType.TOOL_CALL
    criticality: Criticality = Criticality.IMPORTANT
    specialist: str = ""
    tool_name: str = ""
    tools: List[str] = []
    args: Dict[str, Any] = {}
    state: NodeState = NodeState.PENDING
    dependencies: List[str] = []
    dependents: List[str] = []
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    output_contract: OutputContract = OutputContract()
    retry_policy: RetryPolicy = RetryPolicy()
    retry_budget: int = 3
    retry_count: int = 0
    steps_consumed: int = 0
    estimated_steps: int = 1
    danger: DangerClassification = DangerClassification.SAFE
    files: List[str] = []
    history: List[Dict[str, Any]] = []
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    created_at: datetime = ...
    updated_at: datetime = ...

    def add_history(self, from_state: NodeState, to_state: NodeState, reason: str = "")
    def can_retry(self) -> bool
    def next_backoff(self) -> float
```

---

### 2.3 `models.events`

**File:** `runtime_next/models/events.py`

#### `EventType`

```python
class EventType(str, Enum):
    CAPABILITY_CHANGED = "capability_changed"
    NODE_TRANSITION = "node_transition"
    NODE_FAILED = "node_failed"
    GRAPH_STARTED = "graph_started"
    GRAPH_COMPLETED = "graph_completed"
    PLAN_CREATED = "plan_created"
    RECOVERY_INITIATED = "recovery_initiated"
    RECOVERY_COMPLETED = "recovery_completed"
    BUDGET_WARNING = "budget_warning"
    HANDOFF_INITIATED = "handoff_initiated"
    LOG_MESSAGE = "log_message"
    TELEMETRY = "telemetry"
    VERIFICATION_STARTED = "verification_started"
    VERIFICATION_COMPLETED = "verification_completed"
    VERIFICATION_FAILED = "verification_failed"
    FAILURE_CLASSIFIED = "failure_classified"
    RECOVERY_INJECTED = "recovery_injected"
    RETRY_BLOCKED = "retry_blocked"
    GRAPH_ROLLBACK = "graph_rollback"
    REPLAY_DIVERGENCE = "replay_divergence"
    CONSISTENCY_CHECK = "consistency_check"
    PLAN_VALIDATED = "plan_validated"
    PLAN_FAILED = "plan_failed"
    ARCHITECT_DECISION = "architect_decision"
    MODE_SELECTED = "mode_selected"
    TASK_BOARD_TRANSITION = "task_board_transition"
    CONSENSUS_FORMED = "consensus_formed"
    BLACKBOARD_PUBLICATION = "blackboard_publication"
```

#### Event Models

| Class | Fields |
|-------|--------|
| `BaseEvent` | `id: str`, `type: EventType`, `timestamp: datetime`, `payload: Dict` |
| `NodeTransitionEvent` | Extends BaseEvent: `node_id`, `node_type`, `criticality`, `from_state`, `to_state`, `reason`, `steps_consumed` |
| `CapabilityEvent` | Extends BaseEvent: `diff: Dict` |
| `RecoveryEvent` | Extends BaseEvent: `node_id`, `classification`, `action`, `retry_count` |
| `GraphEvent` | `graph_id`, `node_count`, `completed_count`, `failed_count`, `skipped_count`, `total_steps` |
| `TelemetryEvent` | `plan_id`, `node_telemetry`, `total_steps_consumed`, `critical_path_completed` |
| `ArchitectPlanEvent` | `plan_id`, `plan_title`, `objective`, `phase_count`, `specialist_roles`, `risk_level`, `verification_count`, `self_review_score`, `failure_reason` |
| `ArchitectDecisionEvent` | `decision_id`, `outcome`, `target_type`, `target_id`, `reason`, `conditions`, `assigned_to`, `overridden_recommendation`, `override_rationale`, `replan_trigger`, `replan_scope` |
| `ModeSelectionEvent` | `mode`, `rationale`, `task_preview`, `risk_profile`, `complexity`, `has_explicit_prefix`, `triggers` |
| `TaskBoardTransitionEvent` | `task_id`, `task_type`, `from_status`, `to_status`, `specialist`, `reason`, `session_id` |
| `ConsensusEvent` | `consensus_id`, `target_id`, `recommendation`, `confidence`, `positions: Dict[str,str]`, `method` |
| `BlackboardPublicationEvent` | `specialist`, `entry_type`, `summary`, `tags`, `session_id` |

---

### 2.4 `models.capability`

**File:** `runtime_next/models/capability.py`

#### Enums

| Enum | Values |
|------|--------|
| `ToolStatus` | `AVAILABLE`, `MISSING`, `BROKEN` |
| `EnvironmentHealth` | `FULLY_OPERATIONAL`, `DEGRADED`, `RESTRICTED`, `OFFLINE` |

#### `GitState`

```python
@dataclass
class GitState:
    branch: str
    is_dirty: bool
    uncommitted_count: int
    has_conflicts: bool
    stash_count: int
    remote_configured: bool
    last_commits: List[str]
```

#### `CapabilitySnapshot`

```python
@dataclass
class CapabilitySnapshot:
    timestamp: datetime
    workspace_path: str
    readable_files: Set[str]
    writable_files: Set[str]
    tools: Dict[str, Dict[str, Any]]
    git: Optional[GitState]
    memory_usage_mb: float
    disk_free_gb: float
    permissions: Dict[str, Any]
    health: EnvironmentHealth
    metadata: Dict[str, Any]
```

---

## 3. `runtime_next.engine` — Execution Engine

**File:** `runtime_next/engine/engine.py`

### `ExecutionGraph`

```python
class ExecutionGraph:
    def __init__(self, bus=None, mutex=None, runner=None, recovery_engine=None)
    @property
    def event_bus(self)

    def add_node(self, node_def)
    def connect(self, from_id, to_id)
    def remove_node(self, node_id)
    def add_edge(self, from_id, to_id)

    def serialize(self, path)
    @classmethod
    def deserialize(cls, path, bus=None, mutex=None) -> ExecutionGraph

    def inject_node(self, node_def, dependencies=None)
    async def transition_node(self, node_id, state, reason="")
    async def start(self, context=None)
```

### `ExecutionEngine`

```python
class ExecutionEngine:
    def __init__(self, graph: ExecutionGraph, parallel: bool = False)
    async def execute(self, context=None)
```

**File:** `runtime_next/engine/file_mutex.py`

### `FileMutex`

```python
class FileMutex:
    async def acquire(self, paths: List[str])
    async def release(self, paths: List[str])
```

**File:** `runtime_next/engine/runner.py`

### `NodeRunner`

```python
class NodeRunner:
    def __init__(self, tool_executor=None)
    def register_handler(self, node_type: str, handler)
    async def run_node(self, node: ExecutionNode, context: Dict[str, Any]) -> Dict[str, Any]
    async def execute_tool(self, tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]
    def bind_to_engine(self, engine) -> Callable
```

---

## 4. `runtime_next.events` — Event Bus

**File:** `runtime_next/events/bus.py`

### `EventBus`

```python
class EventBus:
    def __init__(self, log_path: Optional[str] = None)

    async def start(self)
    async def stop(self)

    def subscribe(self, event_type: EventType, callback)
    def subscribe_all(self, callback)
    async def publish(self, event: BaseEvent)

    async def replay(self, log_file: str, callback)

    @property
    def replayed_count(self) -> int
    @property
    def event_count(self) -> int
```

---

## 5. `runtime_next.capability` — Capability Registry

**File:** `runtime_next/capability/registry.py`

### `CapabilityRegistry`

```python
class CapabilityRegistry:
    def __init__(self, workspace_root: str, event_bus: EventBus)

    def set_tool_allowlist(self, tools: List[str])
    async def start_monitoring(self)
    async def stop_monitoring(self)
    async def refresh(self) -> CapabilitySnapshot

    def diff(self, old: CapabilitySnapshot, new: CapabilitySnapshot) -> Dict[str, Any]
    def to_prompt_injection(self) -> str

    @property
    def last_snapshot(self) -> Optional[CapabilitySnapshot]
```

---

## 6. `runtime_next.verification` — Verification & Self-Healing

**Package:** `runtime_next/verification/__init__.py`

### 6.1 Types

**File:** `runtime_next/verification/types.py`

#### Enums

| Enum | Values |
|------|--------|
| `VerificationType` | `LINT`, `TYPECHECK`, `UNIT_TEST`, `INTEGRATION_TEST`, `SECURITY_SCAN`, `RUNTIME_VALIDATION`, `SANDBOX_VALIDATION`, `DEPENDENCY_VALIDATION`, `GRAPH_CONSISTENCY`, `SERIALIZATION_INTEGRITY`, `CAPABILITY_VALIDATION`, `ARCHITECTURE_VALIDATION`, `MUTEX_VALIDATION`, `REPLAY_CONSISTENCY` |
| `FailureClassification` | `SYNTAX_ERROR`, `DEPENDENCY_MISSING`, `PERMISSION_DENIED`, `ENVIRONMENT_FAILURE`, `TIMEOUT`, `VERIFICATION_FAILURE`, `GRAPH_INCONSISTENCY`, `SERIALIZATION_FAILURE`, `TOOL_FAILURE`, `STALE_RUNTIME_STATE`, `MUTEX_VIOLATION`, `REPLAY_DIVERGENCE`, `CAPABILITY_MISMATCH`, `ARCHITECTURE_VIOLATION`, `SANDBOX_ESCAPE`, `UNKNOWN_FAILURE` |
| `Severity` | `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `Confidence` | `CERTAIN`, `HIGH`, `MEDIUM`, `LOW`, `GUESS` |
| `Retryability` | `SAFE`, `CONDITIONAL`, `DANGEROUS`, `NEVER` |

#### Pydantic Models

| Model | Key Fields |
|-------|------------|
| `VerificationManifest` | `required: List[VerificationType]`, `optional`, `blocking`, `scope_override` |
| `VerificationScope` | `affected_files`, `affected_symbols`, `affected_tests`, `affected_architectural_boundaries`, `dependency_chain`, `is_minimal`, `provenance` |
| `VerificationResult` | `verification_id`, `node_id`, `verification_type`, `timestamp`, `duration_ms`, `success`, `confidence`, `severity`, `retryability`, `artifacts`, `diagnostics`, `affected_files`, `runtime_implications`, `provenance` |
| `ClassificationResult` | `primary: FailureClassification`, `confidence: Confidence`, `confidence_score`, `evidence`, `alternatives`, `raw_stderr`, `exit_code`, `graph_state_snapshot`, `capability_snapshot` |
| `RecoveryStrategy` | `id`, `name`, `failure_type`, `description`, `danger_level`, `max_retries`, `requires_user_approval`, `handler`, `metadata` |
| `RecoveryAction` | `id`, `strategy_id`, `node_id`, `failure_classification`, `action_type`, `description`, `params`, `injected_node_id`, `success`, `timestamp`, `duration_ms`, `result` |
| `RetryDecision` | `can_retry`, `reason`, `suggested_backoff`, `graph_consistent`, `capability_valid`, `mutation_safe`, `dependency_fresh`, `replay_divergence_risk`, `failure_stability`, `retry_count` |
| `GovernanceDecision` | `verdict`, `reason`, `confidence`, `danger_assessment`, `requires_user_intervention`, `suggested_message` |
| `ConsistencyResult` | `is_consistent`, `checks_performed`, `violations`, `graph_integrity`, `serialization_integrity`, `replay_consistency`, `mutex_correctness` |

#### Utility Functions

```python
def classify_exit_code(code: Optional[int]) -> Optional[FailureClassification]
```

#### Constants

```python
EXIT_CODE_CLASSIFICATION_MAP: Dict[int, Dict[str, Any]]
DEFAULT_RECOVERY_MAP: Dict[FailureClassification, str]
```

#### Exceptions

```python
class VerificationError(Exception)
class VerificationNotImplementedError(VerificationError)
```

---

### 6.2 Pipeline

**File:** `runtime_next/verification/pipeline.py`

### `VerificationPipeline`

```python
class VerificationPipeline:
    def __init__(self)

    def register_verifier(self, verification_type: VerificationType, handler)
    def on_event(self, callback)

    async def verify(
        self,
        node_id: str,
        manifest: VerificationManifest,
        scope: VerificationScope,
        context: Dict[str, Any],
    ) -> List[VerificationResult]

    @property
    def history(self) -> List[VerificationResult]
    def get_results_for_node(self, node_id: str) -> List[VerificationResult]
    def all_passed(self, node_id: str) -> bool
```

---

### 6.3 Classifier

**File:** `runtime_next/verification/classifier.py`

### `FailureClassifier`

```python
class FailureClassifier:
    def __init__(self)

    def register_pattern(self, pattern: str, classification: FailureClassification)

    async def classify(
        self,
        error_message: str = "",
        stderr: str = "",
        stdout: str = "",
        exit_code: Optional[int] = None,
        graph_state: Optional[Dict[str, Any]] = None,
        capability_state: Optional[Dict[str, Any]] = None,
        verification_results: Optional[List[Dict[str, Any]]] = None,
        execution_history: Optional[List[Dict[str, Any]]] = None,
    ) -> ClassificationResult

    @property
    def classification_history(self) -> List[ClassificationResult]
    def get_recent_classifications(self, n: int = 10) -> List[ClassificationResult]
```

---

### 6.4 Recovery

**File:** `runtime_next/verification/recovery.py`

### `RecoveryStrategyEngine`

```python
class RecoveryStrategyEngine:
    def __init__(self)

    def register_strategy(self, strategy: RecoveryStrategy)
    def register_executor(self, strategy_id: str, executor)

    async def execute_recovery(
        self,
        node_id: str,
        failure_type: FailureClassification,
        classification_result: Any,
        context: Dict[str, Any],
    ) -> Optional[RecoveryAction]

    def get_strategy(self, failure_type: FailureClassification) -> Optional[RecoveryStrategy]
    def get_recovery_history(self, node_id: Optional[str] = None) -> List[RecoveryAction]

    @property
    def recovery_count(self) -> int
    @property
    def strategies(self) -> Dict[FailureClassification, RecoveryStrategy]
```

Registers **14 default strategies** covering every `FailureClassification`, each with appropriate `max_retries` and `danger_level`.

---

### 6.5 Retry Safety

**File:** `runtime_next/verification/retry_safety.py`

### `RetrySafetyEngine`

```python
class RetrySafetyEngine:
    def __init__(self)

    async def evaluate(
        self,
        node_id: str,
        classification: FailureClassification,
        retryability: Retryability,
        graph_state: Optional[Dict[str, Any]] = None,
        capability_state: Optional[Dict[str, Any]] = None,
        serialization_state: Optional[Dict[str, Any]] = None,
        replay_state: Optional[Dict[str, Any]] = None,
        execution_history: Optional[List[Dict[str, Any]]] = None,
    ) -> RetryDecision

    def get_retry_count(self, node_id: str) -> int
    def get_decisions(self, node_id: Optional[str] = None) -> List[RetryDecision]
    def reset(self, node_id: Optional[str] = None)
```

Checks: graph consistency, capability validity, mutation safety, dependency freshness, replay divergence risk, failure stability, serialization integrity.

---

### 6.6 Injector

**File:** `runtime_next/verification/injector.py`

### `RecoveryNodeInjector`

```python
class RecoveryNodeInjector:
    def __init__(self)

    async def inject_recovery_node(
        self,
        action: RecoveryAction,
        strategy: RecoveryStrategy,
        graph: Any,
        context: Dict[str, Any],
    ) -> Optional[str]

    async def inject_rollback_node(
        self,
        plan_id: str,
        reason: str,
        checkpoint_path: str,
        nodes_affected: List[str],
        graph: Any,
    ) -> Optional[str]

    @property
    def injected_nodes(self) -> Dict[str, Dict[str, Any]]
    def get_injections_for_node(self, node_id: str) -> List[Dict[str, Any]]
    def clear(self)
```

---

### 6.7 Consistency

**File:** `runtime_next/verification/consistency.py`

### `RuntimeConsistencyValidator`

```python
class RuntimeConsistencyValidator:
    def __init__(self)

    async def validate_all(
        self,
        graph_state: Optional[Dict[str, Any]] = None,
        serialization_state: Optional[Dict[str, Any]] = None,
        event_log_state: Optional[Dict[str, Any]] = None,
        mutex_state: Optional[Dict[str, Any]] = None,
        capability_state: Optional[Dict[str, Any]] = None,
        replay_state: Optional[Dict[str, Any]] = None,
    ) -> ConsistencyResult

    def take_snapshot_hash(self, name: str, state: Dict[str, Any]) -> str
    def verify_snapshot_hash(self, name: str, state: Dict[str, Any]) -> bool

    @property
    def check_history(self) -> List[ConsistencyResult]
    def is_consistently_healthy(self, recent_checks: int = 5) -> bool
```

7 checks: graph integrity, serialization, replay consistency, mutex correctness, capability freshness, event ordering, dependency validity.

---

### 6.8 Memory

**File:** `runtime_next/verification/memory.py`

### `RecoveryMemoryEntry`

```python
class RecoveryMemoryEntry(BaseModel):
    id: str
    failure_type: FailureClassification
    recovery_strategy_id: str
    recovery_strategy_name: str
    success: bool
    node_context: str
    repo_context: str
    toolchain_context: Dict[str, Any]
    runtime_state_hash: str
    graph_conditions: Dict[str, Any]
    duration_ms: float
    timestamp: datetime
    metadata: Dict[str, Any]
```

### `LearnedRecoveryMemory`

```python
class LearnedRecoveryMemory:
    def __init__(self, storage_path: Optional[str] = None)

    async def record(self, action: RecoveryAction, strategy=None, success=None, context=None) -> RecoveryMemoryEntry

    async def find_similar_failures(self, failure_type, context=None, limit=5) -> List[Tuple[RecoveryMemoryEntry, float]]
    async def best_recovery_for(self, failure_type, context=None) -> Optional[Tuple[RecoveryMemoryEntry, float]]
    async def success_rate(self, failure_type=None, strategy_id=None) -> float
    async def strategy_ranking(self, failure_type) -> List[Tuple[str, float, int]]

    @property
    def entries(self) -> List[RecoveryMemoryEntry]
    @property
    def total_entries(self) -> int
    @property
    def overall_success_rate(self) -> float
    def get_entries_by_type(self, failure_type) -> List[RecoveryMemoryEntry]
    def clear(self)
```

Persistence to disk via JSON.

---

### 6.9 Events

**File:** `runtime_next/verification/events.py`

| Event | Key Fields |
|-------|------------|
| `VerificationStartedEvent` | `event_id`, `node_id`, `verification_type`, `scope`, `replay_id` |
| `VerificationCompletedEvent` | `event_id`, `node_id`, `verification_type`, `result: VerificationResult`, `duration_ms` |
| `VerificationFailedEvent` | `event_id`, `node_id`, `verification_type`, `result`, `classification`, `duration_ms` |
| `FailureClassifiedEvent` | `event_id`, `node_id`, `classification: ClassificationResult`, `raw_error` |
| `RecoveryInjectedEvent` | `event_id`, `node_id`, `injected_node_id`, `strategy_id`, `failure_classification` |
| `RetryBlockedEvent` | `event_id`, `node_id`, `classification`, `decision: RetryDecision`, `retry_attempt` |
| `GraphRollbackEvent` | `event_id`, `plan_id`, `checkpoint_path`, `reason`, `nodes_affected` |
| `ReplayDivergenceEvent` | `event_id`, `expected_hash`, `actual_hash`, `divergent_nodes` |

---

### 6.10 Governance

**File:** `runtime_next/verification/governance.py`

### `RecoveryGovernance`

```python
class RecoveryGovernance:
    def __init__(self)

    async def decide(
        self,
        failure_type: FailureClassification,
        strategy: RecoveryStrategy,
        action_type: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> GovernanceDecision

    def mark_approval_pending(self, decision_id: str)
    def approve(self, decision_id: str) -> bool
    def reject(self, decision_id: str) -> bool

    @property
    def pending_approvals(self) -> List[GovernanceDecision]
    @property
    def decisions(self) -> List[GovernanceDecision]
    @property
    def auto_recovery_count(self) -> int
    @property
    def intervention_count(self) -> int
```

Decision paths: `auto_recover`, `require_approval`, `abort`, `notify_user`. Unknown failures are never silently retried.

---

### 6.11 Sandbox Verifier

**File:** `runtime_next/verification/sandbox_verifier.py`

### `SandboxVerifier`

```python
class SandboxVerifier:
    def create_handler(self) -> Callable
    def verify(self, node_id, scope, context) -> VerificationResult
    # Context must contain 'sandbox_result' key

def register_sandbox_verifier(pipeline) -> SandboxVerifier
def classify_sandbox_error(error_type, error_detail) -> Dict[str, Any]
```

Maps sandbox error types (`sandbox_denied`, `timeout`, `resource_limit`, `workspace_escape`, etc.) to `FailureClassification`.

---

### 6.12 Code Verifier

**File:** `runtime_next/verification/code_verifier.py`

### `CodeVerifier`

```python
class CodeVerifier:
    def create_handler(self) -> Callable
    def verify(self, node_id, scope, context) -> VerificationResult  # AST parsing
```

### `TypeCheckVerifier`

```python
class TypeCheckVerifier:
    def create_handler(self) -> Callable
    def verify(self, node_id, scope, context) -> VerificationResult  # py_compile
```

---

### 6.13 Graph Verifier

**File:** `runtime_next/verification/graph_verifier.py`

### `GraphConsistencyVerifier`

```python
class GraphConsistencyVerifier:
    def create_handler(self) -> Callable
    def verify(self, node_id, scope, context) -> VerificationResult
    # Checks: edge target validity, node terminal state, failure rate
```

---

### 6.14 Additional Verifiers

**File:** `runtime_next/verification/additional_verifiers.py`

### `AdditionalVerifier`

```python
class AdditionalVerifier:
    def __init__(self, vtype: VerificationType)
    def create_handler(self) -> Callable
    async def verify(self, node_id, scope, context) -> VerificationResult
```

Handles: `UNIT_TEST`, `INTEGRATION_TEST`, `SECURITY_SCAN` (8 regex patterns), `DEPENDENCY_VALIDATION`, `SERIALIZATION_INTEGRITY`, `CAPABILITY_VALIDATION`, `ARCHITECTURE_VALIDATION`, `MUTEX_VALIDATION`, `REPLAY_CONSISTENCY`, `RUNTIME_VALIDATION`.

---

### 6.15 Driven Recovery

**File:** `runtime_next/verification/driven_recovery.py`

### `RecoveryPipelinePhase`

```python
class RecoveryPipelinePhase(str, Enum):
    INITIATED, CLASSIFYING, CLASSIFIED, GOVERNING, GOVERNED,
    ASSESSING_SAFETY, SAFETY_ASSESSED, RECOVERING, RECOVERED,
    REVERIFYING, REVERIFIED, RECORDING, RECORDED, EVOLVING,
    COMPLETED, FAILED, BLOCKED, ABORTED
```

### `RecoveryPipelineResult`

```python
@dataclass
class RecoveryPipelineResult:
    pipeline_id: str
    node_id: str
    status: RecoveryPipelinePhase
    started_at: float
    completed_at: float
    error_message: str
    classification: Optional[ClassificationResult]
    strategy: Optional[RecoveryStrategy]
    governance: Optional[GovernanceDecision]
    retry_decision: Optional[RetryDecision]
    recovery_action: Optional[RecoveryAction]
    pre_recovery_verifications: List[VerificationResult]
    post_recovery_verifications: List[VerificationResult]
    recovery_memory_entry_id: Optional[str]
    plan_evolution_notified: bool

    @property
    def duration_ms(self) -> float
    @property
    def recovery_success(self) -> Optional[bool]
    @property
    def reverify_passed(self) -> Optional[bool]
    @property
    def overall_success(self) -> bool
    @property
    def failure_type(self) -> Optional[str]
    def to_summary(self) -> str
    def format_report(self) -> Dict[str, Any]
```

### `RecoveryPipelineConfig`

```python
@dataclass
class RecoveryPipelineConfig:
    max_retries_per_failure: int = 3
    enable_reverify: bool = True
    enable_plan_evolution: bool = True
    reverify_types: List[VerificationType] = [LINT, TYPECHECK]
    track_success_rates: bool = True
```

### `VerificationDrivenRecoveryPipeline`

```python
class VerificationDrivenRecoveryPipeline:
    def __init__(self, classifier=None, recovery_strategies=None, retry_safety=None,
                 governance=None, injector=None, recovery_memory=None,
                 verification_pipeline=None, plan_evolution_engine=None, config=None)

    async def run(self, node_id, error_message="", stderr="", stdout="", exit_code=None,
                  graph_state=None, capability_state=None, verification_results=None,
                  execution_history=None, context=None) -> RecoveryPipelineResult

    @property
    def pipeline_history(self) -> List[RecoveryPipelineResult]
    def get_pipeline_count(self) -> int
    def get_success_rate(self, failure_type=None) -> float
    def get_success_rates_by_type(self) -> Dict[str, Dict[str, Any]]
    def get_recent_pipelines(self, n=10) -> List[RecoveryPipelineResult]
    def snapshot(self) -> Dict[str, Any]
```

8-phase lifecycle: Classify → Strategy → Governance → Safety → Recover → Re-verify → Record → Evolve

---

## 7. `runtime_next.recovery` — Recovery Engines

### 7.1 `RecoveryEngine`

**File:** `runtime_next/recovery/engine.py`

```python
class RecoveryEngine:
    def __init__(self, graph=None)

    # Toggle between legacy and new verification subsystem
    def use_legacy_recovery(self, enabled: bool = True)
    @property
    def use_new_subsystem(self) -> bool
    @property
    def graph(self)
    @graph.setter
    def graph(self, g)

    # Event handling
    async def on_event(self, event: BaseEvent)

    # Core failure handling
    async def handle_failure(self, node_id: str, reason: str)

    # Architect Plan Recovery Integration
    def inject_plan_strategies(self, plan_strategies: Any) -> int

    # Calibration Integration
    def link_calibration_system(self, calibration_system: Any)
    def sync_recovery_to_calibration(self) -> Dict[str, Any]

    @property
    def recovery_count(self) -> int
    def get_plan_strategies(self) -> Dict[str, Any]
```

**Internal Subsystems:**

| Component | Type | Purpose |
|-----------|------|---------|
| `classifier` | `FailureClassifier` | Failure classification |
| `recovery_strategies` | `RecoveryStrategyEngine` | Strategy management |
| `retry_safety` | `RetrySafetyEngine` | Retry safety evaluation |
| `injector` | `RecoveryNodeInjector` | Recovery node injection |
| `recovery_memory` | `LearnedRecoveryMemory` | Recovery history |
| `governance` | `RecoveryGovernance` | Autonomy boundary |
| `consensus_recovery` | `ConsensusRecoveryEngine` | Phase 11 consensus recovery |
| `specialist_recovery` | `SpecialistRecoveryEngine` | Phase 11 specialist recovery |
| `task_recovery` | `TaskRecoveryEngine` | Phase 11 task recovery |
| `governance_policy_engine` | `GovernancePolicyEngine` | Phase 13 policy engine |
| `governance_hooks` | `RecoveryGovernanceHooks` | Phase 13 governance hooks |
| `metrics_collector` | `RuntimeMetricsCollector` | Phase 14 metrics |
| `health_monitor` | `RuntimeHealthMonitor` | Phase 14 health |
| `alert_manager` | `AlertManager` | Phase 14 alerts |
| `dashboard` | `RuntimeDashboard` | Phase 14 dashboard |
| `security_scanner` | `RuntimeSecurityScanner` | Phase 15 security |
| `policy_audit_trail` | `PolicyAuditTrail` | Phase 15 audit |
| `sandbox_integrity` | `SandboxIntegrityVerifier` | Phase 15 integrity |
| `security_orchestrator` | `RuntimeSecurityOrchestrator` | Phase 15 security coordinator |
| `runtime_cli` | `RuntimeCLI` | Interactive monitoring CLI |

---

### 7.2 `ConsensusRecoveryEngine`

**File:** `runtime_next/recovery/consensus_recovery.py`

```python
class ConsensusRecoveryEngine:
    def __init__(self)
    def link_recovery_engine(self, engine: RecoveryEngine)
    def set_governance_hooks(self, hooks)
    async def resolve(self, consensus_id, failure_type, participants, context) -> ConsensusRecoveryAction
```

### 7.3 `SpecialistRecoveryEngine`

**File:** `runtime_next/recovery/specialist_recovery.py`

```python
class SpecialistRecoveryEngine:
    def __init__(self)
    def set_reassign_callback(self, callback)
    def set_governance_hooks(self, hooks)
    async def handle_specialist_failure(self, task_id, specialist, error, context) -> SpecialistRecoveryAction
```

### 7.4 `TaskRecoveryEngine`

**File:** `runtime_next/recovery/task_recovery.py`

```python
class TaskRecoveryEngine:
    def __init__(self)
    def set_governance_hooks(self, hooks)
    async def handle_task_failure(self, task_id, trigger, context) -> TaskRecoveryAction
```

---

## 8. `runtime_next.governance` — Governance & Policy

### 8.1 `GovernancePolicyEngine`

**File:** `runtime_next/governance/policy_engine.py`

#### Enums

| Enum | Values |
|------|--------|
| `PolicyEffect` | `ALLOW`, `DENY`, `REQUIRE_APPROVAL`, `LOG_ONLY` |
| `PolicyScope` | `ALL`, `CONSENSUS`, `SPECIALIST`, `TASK` |
| `PolicySeverity` | `INFO`, `WARNING`, `ERROR`, `CRITICAL` |

#### `PolicyRule`

```python
@dataclass
class PolicyRule:
    policy_id: str
    name: str
    description: str
    effect: PolicyEffect
    scope: PolicyScope = PolicyScope.ALL
    action_types: List[str] = []
    specialists: List[str] = []
    failure_types: List[str] = []
    consensus_types: List[str] = []
    task_triggers: List[str] = []
    priority: int = 0
    enabled: bool = True
    reason_template: str = ""
    metadata: Dict[str, Any] = {}

    def matches(self, scope, action_type, specialist=None, failure_type=None,
                consensus_type=None, task_trigger=None) -> bool
    def format_reason(self, **kwargs) -> str
```

#### `PolicyResult`

```python
@dataclass
class PolicyResult:
    policy_id: str
    policy_name: str
    effect: PolicyEffect
    reason: str
    severity: PolicySeverity = PolicySeverity.WARNING
    metadata: Dict[str, Any] = {}
```

#### `PolicyEvaluation`

```python
@dataclass
class PolicyEvaluation:
    overall_effect: PolicyEffect
    reason: str
    matching_rules: List[PolicyResult] = []
    evaluated_count: int = 0

    @property
    def is_allowed(self) -> bool
    @property
    def requires_approval(self) -> bool
    @property
    def is_denied(self) -> bool
```

#### `GovernancePolicyEngine`

```python
class GovernancePolicyEngine:
    def __init__(self)

    # Policy Management
    def add_policy(self, rule: PolicyRule)
    def remove_policy(self, policy_id: str) -> bool
    def enable_policy(self, policy_id: str) -> bool
    def disable_policy(self, policy_id: str) -> bool
    def get_policy(self, policy_id: str) -> Optional[PolicyRule]
    def get_policies(self, scope: Optional[PolicyScope] = None) -> List[PolicyRule]

    # Evaluation — most restrictive wins (DENY > REQUIRE_APPROVAL > LOG_ONLY > ALLOW)
    def evaluate(self, scope, action_type, specialist=None, failure_type=None,
                 consensus_type=None, task_trigger=None) -> PolicyEvaluation
    def evaluate_consensus_action(self, action_type, consensus_type, specialist=None) -> PolicyEvaluation
    def evaluate_specialist_action(self, action_type, specialist, failure_type=None) -> PolicyEvaluation
    def evaluate_task_action(self, action_type, task_trigger, specialist=None, failure_type=None) -> PolicyEvaluation

    # Approval Management
    def request_approval(self, evaluation: PolicyEvaluation, context: Dict[str, Any]) -> str
    def approve(self, token: str) -> bool
    def reject(self, token: str) -> bool

    # History / Stats
    def get_evaluation_history(self, limit: int = 50) -> List[PolicyEvaluation]
    def get_pending_approvals(self) -> Dict[str, Dict[str, Any]]
    def get_stats(self) -> Dict[str, Any]
```

---

### 8.2 `RecoveryGovernanceHooks`

**File:** `runtime_next/governance/recovery_hooks.py`

#### `HookResult`

```python
class HookResult(str, Enum):
    ALLOWED = "allowed"
    DENIED = "denied"
    APPROVAL_PENDING = "approval_pending"
```

#### `HookOutcome`

```python
@dataclass
class HookOutcome:
    result: HookResult
    reason: str
    policy_id: Optional[str] = None
    approval_token: Optional[str] = None
    evaluation: Optional[PolicyEvaluation] = None
    duration_ms: float = 0.0
```

#### `RecoveryGovernanceHooks`

```python
class RecoveryGovernanceHooks:
    def __init__(self, policy_engine: Optional[GovernancePolicyEngine] = None)

    def set_metrics_collector(self, collector)

    @property
    def policy_engine(self) -> GovernancePolicyEngine

    # Consensus Recovery
    def pre_consensus_recovery(self, consensus_id, action_type, consensus_type, context=None) -> HookOutcome
    def post_consensus_recovery(self, consensus_id, action_type, outcome_data)

    # Specialist Recovery
    def pre_specialist_recovery(self, task_id, action_type, specialist, context=None) -> HookOutcome
    def post_specialist_recovery(self, task_id, action_type, outcome_data)

    # Task Recovery
    def pre_task_recovery(self, task_id, action_type, task_trigger, context=None) -> HookOutcome
    def post_task_recovery(self, task_id, action_type, outcome_data)

    # Cross-level evaluation
    def evaluate_recovery_action(self, scope, action_type, entity_id, specialist=None, context=None) -> HookOutcome

    def get_hook_history(self, level=None, limit=100) -> List[Dict[str, Any]]
    def get_hook_stats(self) -> Dict[str, Any]
```

---

### 8.3 `create_default_policies`

```python
def create_default_policies() -> List[PolicyRule]
```

Returns 6 default policies:
- `gov_deny_destructive_consensus` — DENY escalate_to_user on consensus scope
- `gov_log_specialist_failover` — LOG_ONLY failover actions on specialist scope
- `gov_deny_abort_without_notification` — DENY silent task aborts
- `gov_log_consensus_escalation` — LOG_ONLY consensus escalations
- `gov_log_task_replan` — LOG_ONLY task replan events
- `gov_deny_specialist_escalation_sentinel` — DENY SENTINEL escalation

---

## 9. `runtime_next.monitoring` — Monitoring & Observability

### 9.1 `RuntimeMetricsCollector`

**File:** `runtime_next/monitoring/metrics.py`

#### `MetricType`

```python
class MetricType(str, Enum):
    RECOVERY = "recovery"
    GOVERNANCE = "governance"
    SCALING = "scaling"
    HEALTH = "health"
    SYSTEM = "system"
```

#### `MetricPoint`

```python
@dataclass
class MetricPoint:
    value: float
    tags: Dict[str, str] = {}
    timestamp: float = time.time()
```

#### `MetricSeries`

```python
class MetricSeries:
    def __init__(self, name: str, max_len: int = 1000)
    def record(self, value: float, tags=None)
    # Computed properties: count, latest, min, max, avg, sum
    def percentile(self, pct: float) -> Optional[float]
    def get_points(self) -> List[MetricPoint]
    def reset(self)
```

#### `RuntimeMetricsCollector`

```python
class RuntimeMetricsCollector:
    def __init__(self, max_series_len: int = 1000)

    def set_alert_manager(self, alert_manager)  # Auto-evaluates alert rules on every recording
    def record(self, metric_name: str, value: float = 1.0, tags=None)

    # Recovery Metrics
    def record_recovery_attempt(self, level, failure_type, success, duration_ms=0.0)
    def record_recovery_strategy(self, level, strategy)
    def record_consensus_action(self, action_type, consensus_type)
    def record_specialist_state_change(self, specialist, from_state, to_state)
    def record_task_recovery_trigger(self, trigger, action)
    def record_specialist_reassign(self, original, replacement)

    # Governance Metrics
    def record_governance_evaluation(self, scope, effect, policy_id=None)
    def record_governance_approval(self, approved, policy_id)
    def record_hook_execution(self, level, result, duration_ms=0.0)

    # Scaling Metrics
    def record_pool_utilization(self, pool_name, active, capacity)
    def record_pool_acquire_wait(self, pool_name, wait_ms)
    def record_pool_timeout(self, pool_name)
    def record_pipeline_stage(self, pipeline, stage_name, state, duration_ms=0.0)
    def record_batch_completed(self, batch_id, item_count, success_count, duration_ms)
    def record_batch_error(self, batch_id, error_type)

    # Rate Tracking
    def record_rate(self, metric_name)
    def get_rate(self, metric_name, window_seconds=60.0) -> float

    # Reporting
    def get_series(self, name, tags=None) -> Optional[MetricSeries]
    def summary(self, metric_type=None) -> Dict[str, Any]
    def get_counters(self) -> Dict[str, int]
    def recovery_summary(self) -> Dict[str, Any]
    def governance_summary(self) -> Dict[str, Any]
    def scaling_summary(self) -> Dict[str, Any]
    def reset(self)
```

---

### 9.2 `RuntimeHealthMonitor`

**File:** `runtime_next/monitoring/health.py`

#### `HealthStatus`

```python
class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"
```

#### `HealthCheckResult`

```python
@dataclass
class HealthCheckResult:
    healthy: bool
    message: str = ""
    metric_value: Optional[float] = None
    timestamp: float = time.time()
    details: Dict[str, Any] = {}

    def to_dict(self) -> Dict[str, Any]
```

#### `HealthCheckPolicy`

```python
@dataclass
class HealthCheckPolicy:
    subsystem: str
    check_id: str
    description: str = ""
    interval_seconds: float = 60.0
    failure_threshold: int = 3
    success_threshold: int = 2
    enabled: bool = True
    check_fn: Optional[Callable[[], HealthCheckResult]] = None

    @property
    def key(self) -> str
```

#### `RuntimeHealthMonitor`

```python
class RuntimeHealthMonitor:
    def __init__(self)
    def set_metrics_collector(self, collector)

    # Registration
    def register_check(self, policy: HealthCheckPolicy)
    def unregister_check(self, subsystem, check_id) -> bool
    def get_policies(self, subsystem=None) -> List[HealthCheckPolicy]

    # Execution
    def run_check(self, subsystem, check_id) -> Optional[HealthCheckResult]
    def run_all_checks(self, subsystems=None) -> Dict[str, List[HealthCheckResult]]

    # Status
    def get_check_status(self, subsystem, check_id) -> HealthStatus
    def get_subsystem_status(self, subsystem) -> HealthStatus
    def get_overall_status(self) -> HealthStatus

    # Reporting
    def get_recent_results(self, subsystem=None, limit=10) -> List[Tuple[str, HealthCheckResult]]
    def generate_health_report(self) -> Dict[str, Any]
    def reset(self)
```

---

### 9.3 `AlertManager`

**File:** `runtime_next/monitoring/alerting.py`

#### `AlertSeverity`

```python
class AlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"
```

#### `Alert`

```python
@dataclass
class Alert:
    alert_id: str
    title: str
    message: str
    severity: AlertSeverity
    subsystem: str
    source: str = ""
    timestamp: float = time.time()
    acknowledged: bool = False
    acknowledged_at: Optional[float] = None
    metadata: Dict[str, Any] = {}
    suppressed: bool = False

    def acknowledge(self)
    def to_dict(self) -> Dict[str, Any]
```

#### `AlertRule`

```python
@dataclass
class AlertRule:
    rule_id: str
    name: str
    description: str
    subsystem: str
    severity: AlertSeverity = AlertSeverity.WARNING
    metric_name: str = ""
    threshold_min: Optional[float] = None
    threshold_max: Optional[float] = None
    consecutive_count: int = 1
    enabled: bool = True
    cooldown_seconds: float = 300.0
    message_template: str = "{metric_name} is {value} (threshold: {threshold})"

    def matches(self, metric_name: str, value: float) -> bool
    def format_message(self, metric_name: str, value: float) -> str
```

#### `AlertManager`

```python
class AlertManager:
    def __init__(self)

    # Handlers
    def add_handler(self, handler: Callable[[Alert], None])
    def remove_handler(self, handler) -> bool

    # Rules
    def add_rule(self, rule: AlertRule)
    def remove_rule(self, rule_id: str) -> bool
    def get_rules(self, subsystem=None) -> List[AlertRule]

    # Creation
    def create_alert(self, title, message, severity, subsystem, source="",
                     metadata=None, dedup_key=None) -> Alert

    # Auto-evaluation
    def evaluate_metric(self, metric_name, value, subsystem=None) -> List[Alert]

    # Queries
    def get_alerts(self, subsystem=None, severity=None, limit=50, include_suppressed=False) -> List[Alert]
    def get_unacknowledged_alerts(self, subsystem=None) -> List[Alert]
    def acknowledge_alert(self, alert_id: str) -> bool
    def acknowledge_all(self, subsystem=None) -> int

    # Stats
    def get_stats(self) -> Dict[str, Any]
    def reset(self)
```

Deduplication via `dedup_key` with 5-minute cooldown.

---

### 9.4 `RuntimeDashboard`

**File:** `runtime_next/monitoring/dashboard.py`

#### `SubsystemHealth`

```python
@dataclass
class SubsystemHealth:
    name: str
    status: str
    checks_passing: int = 0
    checks_failing: int = 0
    total_checks: int = 0
    metrics_summary: Dict[str, Any] = {}
    active_alerts: int = 0
    description: str = ""
```

#### `DashboardSnapshot`

```python
@dataclass
class DashboardSnapshot:
    timestamp: float = time.time()
    overall_status: str = "unknown"
    subsystems: Dict[str, SubsystemHealth] = {}
    alerts_summary: Dict[str, Any] = {}
    metrics_highlights: Dict[str, Any] = {}
    generation_duration_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]
```

#### `RuntimeDashboard`

```python
class RuntimeDashboard:
    def __init__(self, metrics_collector=None, health_monitor=None, alert_manager=None)

    @property
    def metrics_collector(self) -> Optional[RuntimeMetricsCollector]
    @property
    def health_monitor(self) -> Optional[RuntimeHealthMonitor]
    @property
    def alert_manager(self) -> Optional[AlertManager]

    def set_metrics_collector(self, collector)
    def set_health_monitor(self, monitor)
    def set_alert_manager(self, manager)

    def generate_snapshot(self) -> DashboardSnapshot
    def generate_report(self) -> Dict[str, Any]
```

Combines metrics, health, and alerts into a unified snapshot. Monitored subsystems: `recovery`, `governance`, `scaling`.

---

### 9.5 `RuntimeCLI`

**File:** `runtime_next/monitoring/cli.py`

### `RuntimeCLI`

```python
class RuntimeCLI:
    def __init__(self, dashboard: RuntimeDashboard)
    def execute(self, command_line: str) -> Dict[str, Any]
```

**Commands:**

| Command | Description |
|---------|-------------|
| `#status dashboard` | Full runtime dashboard snapshot |
| `#status health` | Detailed health report per subsystem |
| `#status alerts` | Unacknowledged alerts |
| `#status alerts --all` | Alert history (last 50) |
| `#status alerts --severity=critical` | Filter by severity |
| `#status alerts --subsystem=recovery` | Filter by subsystem |
| `#status alerts --ack` | Acknowledge all alerts |

---

## 10. `runtime_next.security` — Security Hardening

### 10.1 `RuntimeSecurityScanner`

**File:** `runtime_next/security/scanner.py`

#### Enums

| Enum | Values |
|------|--------|
| `SecuritySeverity` | `INFO`, `LOW`, `MEDIUM`, `HIGH`, `CRITICAL` |
| `SecurityCategory` | `CREDENTIAL_LEAK`, `PATH_TRAVERSAL`, `COMMAND_INJECTION`, `SECRET_EXPOSURE`, `POLICY_VIOLATION`, `UNSAFE_COMMAND`, `SUSPICIOUS_PATTERN`, `SANDBOX_TAMPER`, `CONFIGURATION_ISSUE` |

#### `SecurityFinding`

```python
@dataclass
class SecurityFinding:
    finding_id: str
    category: SecurityCategory
    severity: SecuritySeverity
    title: str
    message: str
    location: str = ""
    line_number: Optional[int] = None
    snippet: str = ""
    recommendation: str = ""
    timestamp: float = time.time()
    source: str = ""
    metadata: Dict[str, Any] = {}

    def to_dict(self) -> Dict[str, Any]
```

#### `ScanResult`

```python
@dataclass
class ScanResult:
    scan_id: str
    timestamp: float = time.time()
    duration_ms: float = 0.0
    findings: List[SecurityFinding] = []
    targets_scanned: int = 0
    total_findings: int = 0
    critical_count: int = 0
    high_count: int = 0
    medium_count: int = 0
    low_count: int = 0
    info_count: int = 0
    scan_type: str = "full"

    @property
    def passed(self) -> bool  # True if 0 critical + 0 high
    def merge(self, other: ScanResult) -> ScanResult
    def to_dict(self) -> Dict[str, Any]
```

**Credential Patterns:** API keys (OpenAI `sk-*`, GitHub `ghp_*`/`gho_*`/`ghu_*`, Slack `xox*`, AWS `AKIA*`), JWTs, passwords, connection strings, private keys.

**Path Traversal Patterns:** `../../..`, URL-encoded traversal, sensitive system files (`/etc/passwd`), Windows system directory access.

**Command Injection:** Shell metacharacters, piped shell invocation, command substitution, piped remote execution, dynamic code execution functions.

**Secret Exposure:** API keys/tokens/secrets/passwords in output/logs.

**Dangerous Commands:** `rm -rf /`, `dd`, `format`, fork bombs, package install with disabled verification, remote downloads, git clone with embedded credentials.

#### `RuntimeSecurityScanner`

```python
class RuntimeSecurityScanner:
    def __init__(self)

    def scan_text(self, text, source="", scan_types=None) -> ScanResult
    def scan_file(self, file_path, content=None) -> Optional[ScanResult]
    def scan_plan(self, plan) -> ScanResult
    def scan_context(self, context, source="runtime_context") -> ScanResult
    def scan_all(self, text_targets=None, file_targets=None, plan=None, context=None) -> ScanResult

    def create_verifier_handler(self) -> Callable  # For VerificationPipeline SECURITY_SCAN

    def get_scan_history(self, limit=10) -> List[ScanResult]
    def get_latest_scan(self) -> Optional[ScanResult]
    def reset(self)
```

---

### 10.2 `PolicyAuditTrail`

**File:** `runtime_next/security/policy_audit.py`

#### Enums

| Enum | Values |
|------|--------|
| `AuditAction` | `POLICY_EVALUATION`, `APPROVAL_REQUESTED`, `APPROVAL_GRANTED`, `APPROVAL_REJECTED`, `GOVERNANCE_DECISION`, `RECOVERY_ACTION`, `SECURITY_SCAN`, `SECURITY_FINDING`, `INTEGRITY_CHECK`, `CONFIGURATION_CHANGE`, `POLICY_CHANGE`, `SYSTEM_EVENT` |
| `AuditDecision` | `ALLOWED`, `DENIED`, `APPROVAL_PENDING`, `APPROVED`, `REJECTED`, `LOG_ONLY`, `ESCALATED`, `BLOCKED` |

#### `AuditRecord`

```python
@dataclass
class AuditRecord:
    record_id: str
    timestamp: float = time.time()
    action: AuditAction
    decision: AuditDecision
    actor: str = ""
    subsystem: str = ""
    resource: str = ""
    reason: str = ""
    message: str = ""
    severity: str = "info"
    metadata: Dict[str, Any] = {}
    previous_hash: str = ""  # SHA-256 of previous record
    record_hash: str = ""    # SHA-256 of this record's content

    def compute_hash(self) -> str
    def verify(self, previous_hash: str) -> bool
    def to_dict(self) -> Dict[str, Any]
```

#### `AuditQuery`

```python
@dataclass
class AuditQuery:
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    actions: Optional[List[AuditAction]] = None
    decisions: Optional[List[AuditDecision]] = None
    actor: Optional[str] = None
    subsystem: Optional[str] = None
    resource: Optional[str] = None
    severity: Optional[str] = None
    limit: int = 100
    offset: int = 0
```

#### `PolicyAuditTrail`

```python
class PolicyAuditTrail:
    def __init__(self, max_records: int = 10000)

    # Recording
    def record(self, action, decision, actor="", subsystem="", resource="",
               reason="", message="", severity="info", metadata=None) -> AuditRecord

    # Convenience Methods
    def record_policy_evaluation(self, policy_id, policy_name, scope, decision, reason, metadata=None) -> AuditRecord
    def record_approval(self, request_id, decision, actor, reason="") -> AuditRecord
    def record_security_finding(self, finding_id, severity, category, message, metadata=None) -> AuditRecord
    def record_integrity_check(self, check_id, passed, details, metadata=None) -> AuditRecord

    # Querying
    def query(self, query: AuditQuery) -> List[AuditRecord]
    def get_records_by_subsystem(self, subsystem, limit=50) -> List[AuditRecord]
    def get_records_by_actor(self, actor, limit=50) -> List[AuditRecord]
    def get_recent(self, limit=50) -> List[AuditRecord]
    def get_by_resource(self, resource) -> List[AuditRecord]

    # Integrity Verification
    def verify_chain_integrity(self) -> bool
    def get_chain_status(self) -> Dict[str, Any]

    # Stats
    def get_stats(self) -> Dict[str, Any]
    def reset(self)

    # Governance Integration
    def wrap_governance_hooks(self, hooks)  # Idempotent — auto-records governance decisions
```

Hash-chain integrity: each record stores the SHA-256 hash of the previous record. `verify_chain_integrity()` detects any tampering.

---

### 10.3 `SandboxIntegrityVerifier`

**File:** `runtime_next/security/sandbox_integrity.py`

#### Enums

| Enum | Values |
|------|--------|
| `BinaryVerificationStatus` | `VERIFIED`, `MISMATCH`, `NOT_FOUND`, `UNKNOWN` |
| `AuditLogIntegrityStatus` | `INTACT`, `TAMPERED`, `EMPTY`, `UNKNOWN` |

#### `IntegrityCheckResult`

```python
@dataclass
class IntegrityCheckResult:
    check_id: str
    name: str
    passed: bool
    status: str = ""
    message: str = ""
    details: Dict[str, Any] = {}
    timestamp: float = time.time()

    def to_dict(self) -> Dict[str, Any]
    def to_health_check_result(self) -> HealthCheckResult
```

#### `SandboxIntegrityVerifier`

```python
class SandboxIntegrityVerifier:
    def __init__(self, sandbox_binary_path=None, expected_sha256=None, audit_log_path=None)

    def set_expected_hash(self, sha256: str)
    def add_known_hash(self, sha256: str)

    # Binary Integrity
    def verify_binary_integrity(self) -> IntegrityCheckResult

    # Audit Log Integrity
    def verify_audit_log_integrity(self, audit_records=None) -> IntegrityCheckResult

    # Process Health
    def check_process_health(self, process_name="sandbox_core", expected_count_range=(0, 5)) -> IntegrityCheckResult

    # Filesystem Isolation
    def check_filesystem_isolation(self, workspace_path=None) -> IntegrityCheckResult

    # Comprehensive
    def run_all_checks(self, workspace_path=None, audit_records=None) -> Dict[str, IntegrityCheckResult]
    def all_passed(self) -> bool
    def get_summary(self) -> Dict[str, Any]
    def get_last_result(self, check_name) -> Optional[IntegrityCheckResult]

    # Health Check Integration
    def create_health_check_fns(self) -> Dict[str, Any]
```

Supports Windows (`tasklist`) and Unix (`ps`) process checking.

---

### 10.4 `RuntimeSecurityOrchestrator`

**File:** `runtime_next/security/orchestrator.py`

#### `SecurityScanSchedule`

```python
@dataclass
class SecurityScanSchedule:
    enabled: bool = True
    interval_seconds: float = 300.0
    scan_types: List[str] = ["credential", "path_traversal", "command_injection", "secret_exposure", "dangerous_command"]
    auto_remediate: bool = False
    alert_on_findings: bool = True
    alert_threshold: str = "medium"
```

#### `SecurityPosture`

```python
@dataclass
class SecurityPosture:
    overall_status: str = "unknown"   # healthy, attention_needed, critical, unknown
    total_scans: int = 0
    total_findings: int = 0
    critical_findings: int = 0
    high_findings: int = 0
    medium_findings: int = 0
    low_findings: int = 0
    audit_records_count: int = 0
    audit_chain_valid: bool = True
    integrity_checks_passed: int = 0
    integrity_checks_failed: int = 0
    last_scan_time: float = 0.0
    last_integrity_check_time: float = 0.0
    recommendations: List[str] = []

    def to_dict(self) -> Dict[str, Any]
```

#### `RuntimeSecurityOrchestrator`

```python
class RuntimeSecurityOrchestrator:
    def __init__(self)

    # Linking Components
    def link_scanner(self, scanner: RuntimeSecurityScanner)
    def link_audit_trail(self, audit_trail: PolicyAuditTrail)
    def link_integrity_verifier(self, verifier: SandboxIntegrityVerifier)
    def set_alert_callback(self, callback)  # fn(title, message, severity)
    def set_scan_schedule(self, schedule: SecurityScanSchedule)

    # Scanning
    def run_security_scan(self, text_targets=None, file_targets=None, plan=None, context=None) -> Optional[ScanResult]

    # Integrity
    def run_integrity_checks(self, workspace_path=None) -> Dict[str, IntegrityCheckResult]

    # Audit
    def get_audit_trail(self) -> Optional[PolicyAuditTrail]
    def query_audit(self, query: AuditQuery) -> List[AuditRecord]
    def verify_audit_chain(self) -> bool

    # Posture
    def get_posture(self) -> SecurityPosture

    # Health Check Integration
    def create_health_check_fns(self) -> Dict[str, Any]

    def reset(self)
```

---

## 11. `runtime_next.scaling` — Scaling & Resource Management

### 11.1 `ResourcePool`

**File:** `runtime_next/scaling/resource_pool.py`

#### Enums

| Enum | Values |
|------|--------|
| `PoolState` | `OPEN`, `CLOSED`, `DEGRADED`, `EXHAUSTED` |

#### `PooledResource`

```python
class PooledResource:
    def __init__(self, resource: Any, pool_id: str)
    @property
    def age_seconds(self) -> float
    @property
    def idle_seconds(self) -> float
    def mark_used(self)
    def mark_failed(self)
```

#### `ResourcePool`

```python
class ResourcePool:
    def __init__(self, creator, max_size=5, min_size=0, idle_timeout_seconds=300.0,
                 max_acquire_retries=3, name="default", validator=None, destroyer=None)

    async def start(self)           # Pre-create min_size, start cleanup loop
    async def stop(self, force=False)

    async def acquire(self, timeout_seconds=None) -> Any
    async def release(self, resource, mark_failed=False)

    async def __aenter__(self) -> ResourcePool
    async def __aexit__(self, *args)
    def acquire_context(self, timeout=None) -> _AcquireContext

    def get_stats(self) -> Dict[str, Any]
    def get_state(self) -> PoolState
    @property
    def is_healthy(self) -> bool
    @property
    def available_count(self) -> int
```

Features: idle timeout, exponential backoff retries, stale resource eviction, state tracking (OPEN/CLOSED/DEGRADED/EXHAUSTED), context manager support.

---

### 11.2 `ConnectionPool`

```python
class ConnectionPool(ResourcePool):
    def __init__(self, creator, max_size=10, min_size=1, idle_timeout_seconds=600.0,
                 name="connection", validator=None, destroyer=None)

    async def acquire(self, timeout_seconds=None) -> Any
    async def release(self, resource, mark_failed=False)
    def record_query(self, resource)
    def get_connection_stats(self) -> Dict[str, Any]
```

---

### 11.3 `ResourcePoolManager`

```python
class ResourcePoolManager:
    def __init__(self)
    def register_pool(self, name: str, pool: ResourcePool)
    async def start_all(self)
    async def stop_all(self)
    def get_pool(self, name: str) -> Optional[ResourcePool]
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]
    def get_health_report(self) -> Dict[str, Any]
```

---

### 11.4 `AsyncPipeline`

**File:** `runtime_next/scaling/async_pipeline.py`

#### Enums

| Enum | Values |
|------|--------|
| `StageState` | `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `SKIPPED`, `BLOCKED` |
| `PipelineState` | `IDLE`, `RUNNING`, `PAUSED`, `COMPLETED`, `FAILED`, `CANCELLED` |
| `StagePriority` | `LOW = 0`, `NORMAL = 1`, `HIGH = 2`, `CRITICAL = 3` |

#### `StageResult`, `PipelineTask`, `PipelineStage`

```python
@dataclass
class StageResult:
    stage_id: str; state: StageState; output: Any; error: Optional[str]
    started_at: Optional[float]; completed_at: Optional[float]; attempts: int = 1

@dataclass
class PipelineTask:
    task_id: str; name: str; coro: Any         # The async callable or coroutine
    priority: StagePriority = StagePriority.NORMAL
    timeout_seconds: Optional[float]; retries: int = 0; max_retries: int = 0

@dataclass
class PipelineStage:
    stage_id: str; name: str; tasks: List[PipelineTask]
    parallel: bool = False; max_concurrency: int = 0
    dependencies: List[str] = []; priority: StagePriority = StagePriority.NORMAL
    state: StageState = StageState.PENDING; skip_on_failure: bool = False
    timeout_seconds: Optional[float]; result: Optional[StageResult] = None
```

#### `AsyncPipeline`

```python
class AsyncPipeline:
    def __init__(self, name="default", max_concurrency=5, progress_callback=None)

    def add_stage(self, stage: PipelineStage)
    def add_task(self, stage_id, task)
    def insert_stage(self, after_stage_id, stage) -> bool

    async def run(self) -> Dict[str, StageResult]

    async def pause(self)
    async def resume(self)
    async def cancel(self)

    @property
    def state(self) -> PipelineState
    def get_result(self, stage_id) -> Optional[StageResult]
    def get_results(self) -> Dict[str, StageResult]
    def get_progress(self) -> Dict[str, Any]
```

Features: dependency ordering, parallel branches, concurrency control, timeouts, retries, progress callbacks, pause/resume/cancel lifecycle.

---

### 11.5 `PipelineBuilder`

```python
class PipelineBuilder:
    def __init__(self, name="pipeline")

    def with_max_concurrency(self, n) -> PipelineBuilder
    def with_progress_callback(self, callback) -> PipelineBuilder
    def add_stage(self, stage_id, name, parallel=False, max_concurrency=0,
                  dependencies=None, skip_on_failure=False) -> PipelineBuilder
    def add_task(self, name, coro, priority=StagePriority.NORMAL,
                 timeout_seconds=None, max_retries=0) -> PipelineBuilder
    def build(self) -> AsyncPipeline
```

---

### 11.6 `BatchProcessor`

**File:** `runtime_next/scaling/batch_processor.py`

#### Enums

| Enum | Values |
|------|--------|
| `BatchItemState` | `PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`, `SKIPPED`, `RETRYING` |
| `BatchStrategy` | `PARALLEL`, `SEQUENTIAL`, `THROTTLED` |
| `BatchErrorPolicy` | `STOP_ON_ERROR`, `CONTINUE_ON_ERROR`, `RETRY_FAILED` |

#### `BatchItem`

```python
@dataclass
class BatchItem:
    item_id: str; data: Any
    state: BatchItemState = BatchItemState.PENDING
    result: Any = None; error: Optional[str] = None
    started_at: Optional[float]; completed_at: Optional[float]
    attempts: int = 0; max_retries: int = 0
```

#### `BatchResult`

```python
@dataclass
class BatchResult:
    batch_id: str; total_items: int
    completed: int = 0; failed: int = 0; skipped: int = 0
    total_duration_ms: float = 0.0
    results: List[BatchItem] = []
    errors: List[str] = []
    success_rate: float = 0.0
```

#### `BatchProcessor`

```python
class BatchProcessor:
    def __init__(self, handler, batch_size=10, max_concurrency=5,
                 batch_strategy=BatchStrategy.PARALLEL,
                 error_policy=BatchErrorPolicy.CONTINUE_ON_ERROR,
                 throttle_delay_seconds=0.0, max_retries=0,
                 item_callback=None, name="batch")

    async def process(self, items: List[Any]) -> BatchResult
    async def process_stream(self, items: List[Any], yield_interval=0.1) -> AsyncBatchIterator
    def get_stats(self) -> Dict[str, Any]
```

---

### 11.7 `AsyncBatchIterator`

```python
class AsyncBatchIterator:
    def __init__(self, processor, items, yield_interval)
    def __aiter__(self) -> AsyncBatchIterator
    async def __anext__(self) -> BatchResult
```

---

## 12. `runtime_next.plan` — Planning & Architecture

### 12.1 `ArchitectPlan`

**File:** `runtime_next/plan/architect_types.py`

#### Enums

| Enum | Values |
|------|--------|
| `RiskLevel` | `LOW`, `MEDIUM`, `HIGH`, `CRITICAL` |
| `BlastRadius` | `ISOLATED`, `LOCALIZED`, `WIDESPREAD`, `SYSTEMIC` |
| `RecoveryStrategyType` | `RETRY`, `ROLLBACK`, `SUBSTITUTE`, `ESCALATE`, `DECOMPOSE`, `ABORT` |
| `VerificationMethod` | `UNIT_TEST`, `INTEGRATION_TEST`, `TYPECHECK`, `LINT`, `SECURITY_SCAN`, `MANUAL_REVIEW`, `COMPARISON`, `ARCHITECTURE_CHECK` |
| `SpecialistRole` | `FORGE`, `SENTINEL`, `ORACLE`, `TERMINUS`, `HERALD`, `HERMES`, `ARCHITECT` |
| `PlanStatus` | `DRAFT`, `REVIEW_REQUIRED`, `VALIDATED`, `IN_PROGRESS`, `COMPLETED`, `FAILED`, `SUPERSEDED` |

#### Plan Section Models

| Section | Class | Key Fields |
|---------|-------|------------|
| 1. Objective | `ObjectiveSection` | `goal`, `success_criteria`, `hidden_constraints`, `ambiguities` |
| 2. Current Understanding | `CurrentUnderstandingSection` | `summary`, `relevant_modules`, `key_files`, `architectural_context` |
| 3. Impact Analysis | `ImpactAnalysisSection` | `blast_radius`, `affected_files`, `affected_modules`, `impacts: List[ImpactItem]` |
| 4. Risks | `RiskSection` | `risks: List[RiskItem]`, `overall_level` |
| 5. Execution Strategy | `ExecutionStrategySection` | `phases: List[ExecutionPhase]`, `dependency_edges`, `critical_path`, `parallelizable_phases` |
| 6. Specialist Assignments | `SpecialistAssignmentsSection` | `assignments: List[SpecialistAssignment]` |
| 7. Verification Plan | `VerificationPlanSection` | `checks: List[VerificationCheck]` |
| 8. Recovery Plan | `RecoveryPlanSection` | `failure_strategies: List[FailureModeStrategy]`, `rollback_points`, `general_approach` |
| 9. Completion Criteria | `CompletionCriteriaSection` | `criteria`, `verification_required`, `human_review_before_merge` |
| 10. Self-Review | `SelfReviewSection` | `is_coherent`, `is_minimal`, `is_executable`, `issues`, `score`, `verdict` |
| Omega 2. Context Analysis | `ContextAnalysisSection` | `explicit_goals`, `implicit_goals`, `hidden_requirements`, `unstated_constraints` |
| Omega 3. Repository Analysis | `RepositoryAnalysisSection` | `intelligence_status`, `architecture_layers`, `subsystem_ownership`, `hotspots` |
| Omega 4. Architectural Analysis | `ArchitecturalAnalysisSection` | `boundaries`, `subsystem_responsibilities`, `drift_indicators` |
| Omega 5. Dependency Analysis | `DependencyAnalysisSection` | `execution_dependencies`, `repository_dependencies`, `critical_dependencies` |
| Omega 6. Governance Analysis | `GovernanceAnalysisSection` | `protected_components`, `security_sensitive_systems`, `escalation_required` |
| Omega 7. Long-Term Impact | `LongTermImpactSection` | `maintenance_effects`, `scaling_effects`, `evolution_effects`, `recommendations` |
| Final Approval | `FinalApprovedPlanSection` | `approved`, `approval_status`, `blocking_reasons`, `conditions`, `confidence` |

#### `ArchitectPlan`

```python
class ArchitectPlan(BaseModel):
    id: str
    title: str = ""
    status: PlanStatus = PlanStatus.DRAFT
    created_at: datetime
    updated_at: datetime

    # 10 Core Sections
    objective: ObjectiveSection
    current_understanding: CurrentUnderstandingSection
    impact_analysis: ImpactAnalysisSection
    risks: RiskSection
    execution_strategy: ExecutionStrategySection
    specialist_assignments: SpecialistAssignmentsSection
    verification_plan: VerificationPlanSection
    recovery_plan: RecoveryPlanSection
    completion_criteria: CompletionCriteriaSection
    self_review: SelfReviewSection

    # 4 Omega Strategic Sections + Final Approval
    context_analysis: ContextAnalysisSection
    repository_analysis: RepositoryAnalysisSection
    architectural_analysis: ArchitecturalAnalysisSection
    dependency_analysis: DependencyAnalysisSection
    governance_analysis: GovernanceAnalysisSection
    long_term_impact: LongTermImpactSection
    final_approved_plan: FinalApprovedPlanSection

    source_goal_id: Optional[str]
    metadata: Dict[str, Any] = {}

    def validate_complete(self) -> List[str]
    def to_execution_plan(self) -> Dict[str, Any]
    def to_terminal_display(self) -> str
```

---

### 12.2 `PlanBuilder`

**File:** `runtime_next/plan/builder.py`

**Dependencies:** `cognition.types` (Goal, SubGoal, PlanStep, PlanPrecondition, PlanUncertainty, SpecialistRole)

### `PlanBuilder`

```python
class PlanBuilder:
    def __init__(self, patterns: Optional[List[ExecutionPattern]] = None)

    def build(self, task_description: str, context=None) -> ExecutionPlan
    def build_with_goal(self, goal: Goal, context=None) -> ExecutionPlan

    @classmethod
    def from_legacy_node(cls, node: LegacyNode, plan_id: str) -> ExecutionNode
```

Builds execution plans with hierarchical goal decomposition, precondition tracking, uncertainty levels, dependency analysis, criticality assignment, parallelism identification, budget allocation, and pattern matching.

---

### 12.3 `ArchitectOrchestrator`

**File:** `runtime_next/plan/architect.py`

### `ArchitectOrchestrator`

```python
class ArchitectOrchestrator:
    def __init__(self, repo_intelligence=None, forge_memory=None, event_bus=None)

    def create_plan(self, objective: str, context=None) -> ArchitectPlan
    def get_plan(self, plan_id: str) -> Optional[ArchitectPlan]
    def list_plans(self) -> List[str]
    def finalize(self, plan_id: str) -> bool
    def self_critique(self, plan: ArchitectPlan) -> List[str]
    def estimate_cost(self, plan: ArchitectPlan) -> Dict[str, Any]

    def persist_plan_to_memory(self, plan: ArchitectPlan)
    def link_strategic_memory(self, strategic_memory)

    def enrich_context_with_plan(self, plan: ArchitectPlan, context: Dict[str, Any]) -> Dict[str, Any]
    def build_plan_from_conversation(self, objective, repo_intel_output=None,
                                     memory_context=None, constraints=None) -> ArchitectPlan

    def get_calibration_summary(self) -> Dict[str, Any]
```

Uses `ArchitectIntelligenceBrain` with 13 intelligence engines for real reasoning:
objective analysis, repository intelligence, architectural reasoning, strategic selection, risk analysis, execution design, specialist assignment, verification design, recovery design, dependency analysis, long-horizon impact, governance analysis, iterative self-critique.

---

### 12.4 `ArchitectIntelligenceCoordinator`

**File:** `runtime_next/plan/intelligence.py`

#### StrategicOutput

```python
class StrategicOutput(BaseModel):
    # Output from the Architect Intelligence Brain — encapsulates all
    # strategic reasoning results from the 13 intelligence engines.
    context_analysis: Dict[str, Any]
    repository_analysis: Dict[str, Any]
    architectural_analysis: Dict[str, Any]
    execution_strategy: Dict[str, Any]
    specialist_assignments: Dict[str, Any]
    verification_plan: Dict[str, Any]
    recovery_plan: Dict[str, Any]
    risk_analysis: Dict[str, Any]
    dependency_analysis: Dict[str, Any]
    long_horizon_impact: Dict[str, Any]
    governance_requirements: Dict[str, Any]
    self_review: Dict[str, Any]
    coordination_decisions: List[str]
```

### Domain Intelligence Classes

| Class | Purpose |
|-------|---------|
| `ObjectiveIntelligence` | Understands explicit goals, implicit goals, hidden requirements, unstated constraints |
| `RepositoryIntelligence` | Consumes Repository Intelligence Omega as strategic evidence |
| `ArchitecturalIntelligence` | Reasons about boundaries, design intent, architectural drift |
| `DependencyIntelligence` | Builds execution/repository/specialist/verification/recovery dependencies |
| `GovernanceIntelligence` | Protects critical infrastructure, escalates dangerous changes |
| `LongHorizonIntelligence` | Reasons about maintenance, scaling, evolution, technical debt |
| `AutonomousCoordinationIntelligence` | Explains specialist participation, resolves assignment conflicts |
| `StrategicApprovalIntelligence` | Approves final plan after self-critique and governance review |

### `ArchitectIntelligenceCoordinator`

```python
class ArchitectIntelligenceCoordinator:
    def __init__(self, repo_intelligence=None)
    def preflight(self, objective: str, context: Dict[str, Any]) -> StrategicIntelligenceSnapshot
```

`StrategicIntelligenceSnapshot` combines: `context_analysis`, `repository_analysis`, `architectural_analysis`, `long_term_impact`.

---

### 12.5 `PlanCalibrationSystem`

**File:** `runtime_next/plan/calibration.py`

#### Enums

| Enum | Values |
|------|--------|
| `DeviationType` | `UNPLANNED_FAILURE`, `INCORRECT_RISK`, `INEFFICIENT_VERIFICATION`, `UNNECESSARY_SPECIALIST`, `INCORRECT_DEPENDENCY`, `STRATEGY_MISMATCH`, `OBJECTIVE_MISALIGNMENT` |

#### `LearningEntry`

```python
class LearningEntry(BaseModel):
    id: str
    deviation_type: DeviationType
    description: str
    plan_prediction: str
    actual_outcome: str
    severity: float = 0.5
    applicable_task_types: List[str] = []
    applicable_strategy_classes: List[str] = []
    recommendation: str = ""
    confidence: float = 0.5
    created_at: datetime
    times_applied: int = 0
    effectiveness_score: float = 0.5
```

#### `PlanOutcome`

```python
class PlanOutcome(BaseModel):
    plan_id: str
    objective: str
    task_type: str
    strategy_class: str
    planned_phases: int
    completed_phases: int
    planned_specialists: List[str]
    actual_specialists: List[str]
    planned_risks: int
    materialized_risks: int
    verification_checks_run: int
    verification_failures_caught: int
    verification_type_failures: Dict[str, int] = {}
    unplanned_failures: int
    total_duration_ms: float
    success: bool
    deviations: List[LearningEntry] = []
    timestamp: datetime
```

#### `CalibrationAdjustment`

```python
class CalibrationAdjustment(BaseModel):
    field: str
    old_value: Any
    new_value: Any
    reason: str
    confidence: float = 0.5
    learning_count: int = 1
```

#### `PlanCalibrationSystem`

```python
class PlanCalibrationSystem:
    def __init__(self, storage_path=None, save_interval=1)

    def record_outcome(self, plan_id, objective, task_type, strategy_class,
                       planned_phases, completed_phases, planned_specialists,
                       actual_specialists, planned_risks, materialized_risks,
                       verification_checks_run, verification_failures_caught,
                       verification_type_failures=None, unplanned_failures=0,
                       total_duration_ms=0.0, success=True) -> PlanOutcome

    def get_adjustments_for_task(self, task_type, strategy_class) -> List[CalibrationAdjustment]
    def get_task_type_success_rate(self, task_type) -> float
    def get_strategy_effectiveness(self, strategy_class) -> Dict[str, Any]
    def get_recent_learnings(self, limit=10) -> List[LearningEntry]
    def get_calibration_summary(self) -> Dict[str, Any]
    def snapshot(self) -> Dict[str, Any]

    def flush(self)
    def clear_persistence(self)
```

Automatically analyzes 5 deviation types per recorded outcome. Supports JSON persistence with `save_interval` for batching.

---

### 12.6 `SubBudgetAllocator`

**File:** `runtime_next/plan/allocator.py`

### `BudgetEnvelope`

```python
class BudgetEnvelope:
    def __init__(self, section_id: str, allocated: int, consumed: int = 0)
    @property
    def remaining(self) -> int
    @property
    def exhausted(self) -> bool
```

### `SubBudgetAllocator`

```python
class SubBudgetAllocator:
    def __init__(self, plan: ExecutionPlan)
    def allocate(self) -> Dict[str, BudgetEnvelope]
    def consume(self, section: str, steps: int) -> bool
    def can_dispatch(self, node_id: str) -> bool
    def skip_optional_budget(self) -> int
```

Three-priority budget allocation: critical path (guaranteed) → important branches → optional enrichment.

---

> **End of RuntimeNext API Documentation**
>
> Generated from source code. Coverage: 12 modules, 80+ classes, 60+ enums, 200+ public methods.
