# AELVO Current State

> Based on direct code analysis of the AELVO Omega repository.
> Date: June 2026

---

## 1. Actual Architecture

AELVO is a **hybrid sequential-orchestrated system** with 7 specialist agents coordinated by an internal orchestrator (`UnifiedOrchestrator`).

### Entry Point

`main.py` bootstraps:
1. `AelvoKernel` — state manager, command router (`commands.py`)
2. `AelvoFileSystem` — jailed file operations (`automation.py`)
3. `MemoryEngine` — hybrid SQLite + ChromaDB memory
4. `AelvoAgent` — universal LLM adapter (OpenAI, Anthropic, Google)
5. `Orchestrator` — turn execution coordinator
6. `CognitiveEngine` — planning, research, consensus, coordination
7. `SPECIALIST_REGISTRY` — dict of 7 `BaseSpecialist` instances

### Execution Flow

```
User input → Orchestrator.execute_turn()
  → HERMES (context analysis)
  → ARCHITECT (plan, mode selection)
  → [specialist loop: FORGE/ORACLE/SENTINEL/TERMINUS/HERALD]
  → HERALD (summary)
  → Response to user
```

### Mode B (Collaborative)

When complexity is high or risk is elevated, `ArchitectSpecialist.select_execution_mode()` returns `ExecutionMode.COLLABORATIVE`, which routes work through:
- `SharedTaskBoard` — task lifecycle with state machine
- `SpecialistCoordinationRuntime` — delegate to specialists
- `CognitiveBlackboard` — shared findings across specialists

---

## 2. Specialist Model

All 7 specialists extend `BaseSpecialist` (`specialists/base.py`):

| Specialist | Role | Activation | Input | Output |
|------------|------|------------|-------|--------|
| **HERMES** | User calibration | Keyword + trigger patterns | User task, conversation history | HermesContext (goals, risk, complexity) |
| **ARCHITECT** | Strategic planning | Planning keywords | Task + HermesContext | 14-section plan (ArchitectPlan) |
| **ORACLE** | Research | Research keywords | Question, context | Findings via blackboard |
| **FORGE** | Coding | Code keywords | Coding task, context | Code changes via tools |
| **SENTINEL** | Security | Security keywords | Implementation to review | Approval/rejection via blackboard |
| **TERMINUS** | DevOps | Terminal keywords | Command plan, context | Execution results |
| **HERALD** | Communication | Communication keywords | Summary request | Drafts, summaries |

### Key Methods
- `compute_activation_score(task, context)` → 0.0-1.0 confidence
- `get_system_prompt(context)` → dynamic prompt
- `execute(task, context)` → LLM call with system prompt
- `verify_output(output, context)` → validation
- `post_process(result, memory, history)` → memory persistence

### Current Task Discovery
Each specialist has `pickup_task(task_board, task_type, max_tasks)` which polls the `SharedTaskBoard` for pending/assigned tasks. Specialists CAN discover tasks but don't autonomousy decide to — the orchestrator creates tasks and assigns them.

---

## 3. Task Flow

### Current Flow (Orchestrator-Driven)

```
1. User submits task
2. Orchestrator.execute_turn() called
3. Orchestrator._route_mode_selection() chooses Mode A or B
4. If Mode A (Consolidated): Single LLM call handles everything
5. If Mode B (Collaborative):
   a. Architect creates plan via ArchitectOrchestrator
   b. Tasks created on SharedTaskBoard
   c. Tasks assigned to specialists by orchestrator
   d. Specialists execute via SpecialistCoordinationRuntime.delegate()
   e. Results collected, verified, returned
```

### Task State Machine (SharedTaskBoard)

```
PENDING → ASSIGNED → IN_PROGRESS → REVIEWING → COMPLETED
                                        ↓ FAILED → ASSIGNED (retry)
                                    BLOCKED → IN_PROGRESS (unblock)
```

### Current Routing

Specialist selection happens via:
1. `ArchitectSpecialist.select_execution_mode()` — chooses Mode A vs B
2. `SpecialistCoordinationRuntime.delegate()` — delegates to best specialist
3. Selection uses `compute_activation_score()` for capability matching

---

## 4. Memory Flow

```
AELVOAgent (LLM) ←→ MemoryEngine (SQLite + ChromaDB)
                         ↕
                   ForgeMemory (code patterns)
                         ↕
                   UserModelManager (preferences)
                         ↕
                   Blackboard (shared findings)
```

### Dual-Sync Pattern
Every memory write goes to both SQLite (system of record) and ChromaDB (vector search). On conflict, ChromaDB is rolled back if SQLite fails.

---

## 5. Verification Flow

```
Specialist output → verify_output()
  → SENTINEL: secret scanning, vulnerability detection
  → FORGE: tool-call ordering validation (writes before verification)
  → ARCHITECT: Mermaid syntax validation, plan completeness
  → HERMES: empty/TODO/FIXME check
  → ORACLE: citation requirement check
  → TERMINUS: destructive command block
```

Additionally, `runtime_next/verification/pipeline.py` provides structured verification.

---

## 6. Recovery Flow

```
DynamicReplanningEngine.evaluate(plan, trigger, context)
  → NODE_FAILURE, PLAN_STALLED, MANUAL triggers
  → Returns replan action (restructure, bypass, abort, retry)

ConsensusRecovery — multi-agent fault handling via consensus
SpecialistRecovery — specialist-specific recovery
TaskRecovery — task-level recovery
```

---

## 7. Event/UI Flow

```
Internal events:
  EventBus (asyncio-based)
    → UI panels (audit_log, task_board, specialist_panel)
    → Dashboard updates
    → Audit trail logging

Blackboard events:
  BlackboardPublicationEvent (on publish)
  FindingConsumedEvent (on consume)
  ChallengeRaisedEvent (on challenge)

Task Board events:
  NodeTransitionEvent (on state change)
  task_created, task_deleted events
```

---

## 8. Core Strengths

| Subsystem | Reason |
|-----------|--------|
| `specialists/` (7 agents) | Role-specialized agents with activation scoring |
| `cognition/blackboard.py` | Internal shared memory across specialists |
| `shared_task_board/board.py` | Internal task tracking with state machine |
| `memory/forge_memory.py` | Code pattern memory — internal optimization |
| `runtime_next/verification/` | Output validation pipeline |
| `runtime_next/recovery/` | Fault tolerance and recovery engine |
| `cognition/engine.py` | Core orchestration |
| `ui/` | Dashboard, audit trail |
| `repo_intelligence/` | Codebase analysis |
| `event_bus` | Internal event routing |

## 10. What to Remove/De-prioritize

| Item | Reason |
|------|--------|
| `AELVO_RAW_SKILL.md` | Damaged, unused |
| `collaboration_orchestrator.py` — `IntelligentRouter` | Overlaps with orchestrator routing |
