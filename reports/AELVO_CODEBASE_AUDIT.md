# AELVO Codebase Audit — "Powerful Coding Agent" Cleanup Map

> Goal: strip dead weight and instrument-only surfaces so AELVO becomes a lean,
> powerful coding agent. SQLite persistence is KEPT (memory/cache/vault are load-bearing).

## 1. What the boot path actually loads (KEEP — the live core)

`main.py` → `web/server.py` wires exactly this:

| Subsystem | Lines | Role | Verdict |
|---|---|---|---|
| `core/` (execution, filesystem, governance, orchestration, provider_runtime, startup, rag, registry) | ~17.6k | Kernel, file jail, MemoryEngine, Orchestrator, provider detection | ✅ **KEEP** |
| `specialists/` (7 agents + registry) | ~6.3k | The multi-agent brain: Hermes→Herald | ✅ **KEEP** |
| `shared_task_board/` | ~2.7k | Task lifecycle + collaboration orchestration (how agents coordinate) | ✅ **KEEP** |
| `cognition/` | ~6.5k | Blackboard, consensus, planning, coordination | ✅ **KEEP** |
| `memory/forge_memory.py` | ~0.8k | FORGE-scoped memory discipline | ✅ **KEEP** |
| `repo_intelligence/` | ~12.9k | Codebase understanding, impact analysis — core coding superpower | ✅ **KEEP** |
| `tools/` | ~1.3k | Extended tool registry | ✅ **KEEP** |
| `runtime_next/` (models, events, engine, verification, recovery, plan, monitoring, capability) | ~22.7k | Execution-graph engine the Orchestrator is built on | ✅ **KEEP** (see #3 for trims) |
| `mcp/` | ~9.2k | Model Context Protocol platform (booted in main.py) | 🟡 Review — heavy, optional |
| `auth/` | ~11k | Provider registry, adapters, credential vault, rate limiting | ✅ **KEEP** (see #3 for trims) |
| `planning/` | ~4.3k | Long-horizon planning integration | 🟡 Merge candidates inside |
| `web/` | — | React frontend + WS bridge | ✅ **KEEP** |

## 2. DEAD CODE — safe to remove today

| Path | Why dead | Size |
|---|---|---|
| `learning/agent_metrics.py` | `AgentMetricsTracker` is imported **nowhere** (0 refs) | ~350 lines |
| `auth/providers/*` (23 files) | Never imported anywhere; actual SDK routing lives in `main.py AelvoAgent` + `auth/adapters/`. `ProviderAuthOrchestrator` is only re-exported, never called | ~4k lines |
| `core/health/` + `core/monitoring/` | `SystemHealthMonitor` is only referenced by `core/monitoring/system_dashboard.py`, which **nobody imports**. Dead Phase-13 cluster — superseded by `runtime_next/monitoring/` | ~2k lines |
| `runtime_next/scaling/` (resource_pool, async_pipeline, batch_processor) | Only imported by tests (`test_phase12_scaling_integration.py`); not on the prod path | ~1.5k lines |
| `adversarial_tests/` | Standalone test dir, never imported | ~0.4k |
| Old `ui/` TUI | Already deleted — remaining `from ui.events import ...` are all inside `try/except ImportError` no-ops | — |

**Estimated dead weight: ~8–9k lines** — remove first, zero risk.

## 3. HEAVY subsystems — review before touching

| Subsystem | Verdict | Notes |
|---|---|---|
| `runtime_next/` (22.7k) | **KEEP core** — `events.bus`, `models`, `engine`, `verification`, `recovery`, `plan.architect`, `plan.calibration`, `capability.registry`, `monitoring` are all imported by `core/orchestration/orchestrator.py` | Trim only `scaling/` (dead). `security/` + `governance/` are used by `recovery/engine.py` — keep |
| `mcp/` (9.2k) | 🟡 **Merge/keep-minimal** — boots a full MCP server platform. If you never use MCP servers, this is ~9k lines of platform you don't need | Removing it requires editing `main.py` (9 imports + init block) |
| `auth/` (11k) | **KEEP** adapters + cred_storage + config + monitoring. `auth/providers/*` dead (remove) | `auth/auth/*`, `auth/runtime/*`, `auth/diagnostics/*` — used by `provider_runtime.py`; keep |

## 4. SQLite inventory — all KEPT (your choice)

| DB | Role | Keep? |
|---|---|---|
| `memory.db` (per project) | Long-term memory: state, retained facts, constraints, sessions | ✅ |
| `global_memory.db` | Project registry | ✅ |
| `llm_cache.db` | 2h LLM response cache (perf/cost) | ✅ |
| `credential_vault.db` | **Encrypted** API keys (Providers page) | ✅ |
| `repository_memory.db` / `repository_governance.db` | Repo intelligence | ✅ |
| `mcp_memory.db` | MCP registry | ✅ (if mcp kept) |
| ChromaDB collections | Semantic vector memory | ✅ |

## 5. Web UI — removable sections (instrumentation, not features)

Per your earlier choice: **remove Governance + System/Health pages**; slim to coding core.

| Page | Verdict | Backend impact |
|---|---|---|
| Chat | ✅ keep | — |
| Tasks | ✅ keep | — |
| Knowledge | ✅ keep | — |
| Agents | ✅ keep | — |
| Providers | ✅ keep | — |
| Dashboard | 🟡 optional (summary) | — |
| Governance (merged Consensus+Governance) | 🔴 **remove** | `GovernancePage`/`SystemPage` routes + components; the **backend consensus/decision logic stays** (it drives agent behavior) |
| System (merged Health+Monitoring) | 🔴 **remove** | Same — the dashboards are pure event viewers |
| Timeline, Admin | 🟡 remove or hide | — |

## 6. Recommended execution order

1. **Phase A (zero risk):** delete dead code (§2) → `learning/agent_metrics.py`, `auth/providers/*`, `core/health/*`, `core/monitoring/*`, `runtime_next/scaling/*`. Re-run full test suite.
2. **Phase B (UI):** remove Governance + System pages & routes from `web/`; keep backend logic.
3. **Phase C (optional):** make `mcp/` lazy/optional in `main.py` (boot only if configured) — cuts boot time & surface.
4. **Phase D (optional):** prune `runtime_next/plan/brain/*` only if tests prove unused by `architect.py`.

**Verdict: ~8–9k lines dead today (safe), ~11k more optional (mcp), and the UI can go to 6 core pages.**
