# Codebase Analysis Report

**Generated:** 2026-07-06
**Scope:** Full analysis of ~350 source files across all subsystems:
- `main.py`, `core/`, `specialists/`, `runtime_next/`, `auth/`, `mcp/`
- `cognition/`, `planning/`, `memory/`, `learning/`, `repo_intelligence/`
- `shared_task_board/`, `tools/`, `ui/`, `web/` (TypeScript/React), `sandbox_core/` (Rust)
- `tests/`, `adversarial_tests/`, `config/`, `docs/`

No files were excluded. Due to the scale (~350 files, ~100,000+ lines), some files received representative sampling rather than line-by-line coverage of every single line. Every module and subsystem was covered.

---

## Executive Summary

AELVO Omega is an ambitious, large-scale multi-agent AI engineering platform with sophisticated architecture spanning Python, Rust, and TypeScript. The codebase shows strong modular design, extensive testing, and comprehensive feature coverage. **The single biggest risk** is systemic silent error swallowing: `except Exception: pass` and `log.debug("Silenced exception: %s", _ex)` patterns appear in 150+ locations, meaning production failures (DB corruption, API crashes, memory desyncs) are silently invisible. **The single biggest quick win** is replacing these silent swallows with at minimum `log.warning()` level logging — no logic changes needed, just severity bumps. A secondary critical concern is LLM-prompt-injection-to-arbitrary-code-execution via `bash_exec`/`python_exec` in `main.py` that passes untrusted LLM output to `subprocess` with no sandboxing. The MCP subsystem has 6+ stub verifiers/strategies that always return "pass" or `True` — they are documented as real but do nothing. The Rust sandbox has a missing `libc` dependency breaking Unix builds and silent response failures that will hang the Python orchestrator indefinitely.

---

## Severity Legend
- **CRITICAL:** Could cause data loss, security breach, or production crash
- **HIGH:** Significant bug risk, maintainability hazard, or architectural flaw
- **MEDIUM:** Real but contained issue, or moderate tech debt
- **LOW:** Style/consistency/minor cleanup

---

## Findings by Severity

### CRITICAL

| # | File:Line | Description | Why it matters | Suggested fix |
|---|-----------|-------------|----------------|---------------|
| 1 | `main.py:670,674` | `bash_exec` and `python_exec` pass untrusted LLM output to `subprocess` with no command allowlist or sandboxing | Prompt injection on the LLM yields arbitrary code execution on the host | Sandbox all shell execution; use the Rust sandbox or a strict allowlist |
| 2 | `main.py:269-270,282-283,537,630-631` | Silent `except Exception: pass` swallows all errors in LLM cache, search, and memory reinforcement | Cache corruption, DB failures, and memory desyncs are invisible in production | Log at `log.warning()` minimum |
| 3 | `tools/git_tools.py:11-12,93,209` | `_run_git_cmd` passes caller-provided branch names directly into `subprocess.run` with no validation | A crafted branch name (`'; rm -rf /'`) in `base_branch`/`head_branch` enables shell argument injection | Validate branch names against `^[a-zA-Z0-9_/.-]+$` |
| 4 | `tools/code_tools.py:333-335` | `test_filter` from caller is appended to `pytest -k` command; pytest evaluates `-k` as Python expressions | Expression injection possible via `test_filter` | Sanitize or restrict to alphanumeric match patterns |
| 5 | `sandbox_core/Cargo.toml` | `libc` crate is NOT listed but `process.rs:149` uses `libc::setrlimit` in `#[cfg(not(windows))]` blocks | Unix build fails to compile entirely | Add `libc = "0.2"` to `Cargo.toml` |
| 6 | `sandbox_core/main.rs:251-252` | `respond_error`/`respond_success` silently discard JSON serialization failures | Python orchestrator hangs indefinitely waiting for a JSON response that was never written | Use `eprintln!` fallback or raw string write on serde failure |
| 7 | `sandbox_core/policy.rs:120-125` | `.expect()` on regex compilation — panics the binary if any hardcoded pattern is malformed | Trivially triggered production crash on regex compilation failure | Propagate as `Result` or guard with `cfg(debug_assertions)` |
| 8 | `auth/cred_storage.py:65` | `_machine_id()` falls back to `uuid.uuid4().hex` on Windows (no `os.uname`) | Each process restart generates a different machine identity, making encrypted credentials unrecoverable | Use a stable Windows identifier (e.g., `MachineGuid` from registry) |
| 9 | `auth/cred_storage.py:227` | `_get_test_value()` returns `None` on fresh DB; `_decrypt(None)` calls `.encode()` on `None` → `AttributeError` | First unlock always crashes on a fresh credential store | Guard against `None` before calling `_decrypt` |
| 10 | `auth/auth/oauth.py:125` | Refresh token stored in `credential.metadata` which is serialized as plain JSON in SQLite `metadata` column | Refresh tokens are stored in plaintext | Encrypt the entire metadata column |
| 11 | `mcp/registry/health_tracker.py:54-56` | `UnboundLocalError` when `success=False` AND `error_rate <= degrade_threshold` — `new_state` never assigned | Crash on specific error-rate health tracking path | Initialize `new_state` before the conditional |
| 12 | `mcp/discovery/filesystem_discovery.py:37-48` | Auto-registers every `mcp-*`/`*.mcp` executable from PATH directories | Any attacker who writes `mcp-malicious` to a PATH-writable dir gets code execution in MCP | Require user confirmation or signing for auto-registration |
| 13 | `mcp/verification/trust_verifier.py:30-37` | Direct dict key access `result["__meta"]["server_id"]` — `KeyError` if `__meta` lacks `server_id` | Crashes verification for any server without the metadata field | Use `.get("server_id")` with fallback |
| 14 | `specialists/forge.py:~751` | Undefined variable `context` in `post_process(self, result, memory_engine, conversation_history)` — no `context` parameter exists | **NameError** at runtime if that code path executes | Add `context` parameter or remove reference |
| 15 | `specialists/architect.py:1109-1110` | `context["plan"]` unchecked KeyError — `context` dict may lack `"plan"` key | Unhandled KeyError crashes the planner | Use `context.get("plan")` |
| 16 | `web/src/main.tsx` | No React error boundary wrapping `<App />` | Any render crash blows entire app to white screen | Wrap in `<ErrorBoundary>` |
| 17 | `web/src/components/ChatWorkspace.tsx:190-357` | Unhandled promise rejections in `processMessage` and `handleSend` | Silent failures when async operations throw | Add `.catch()` or try/catch to all async handlers |
| 18 | `web/src/components/ChatWorkspace.tsx:265-303` | `.push()` and direct index assignment on `const` arrays stored in `useCallback` | Mutation of React state violates immutability contract, causes stale closures | Use immutable updates (e.g., `newSteps = [...steps, ...]`) |
| 19 | `auth/diagnostics/doctor.py:68,107` | `auth_health.status.value > 0` compares string enum to int → TypeError; `entry.info.supported_models` — `supported_models` is a method, not attribute → AttributeError | `#doctor scan` command crashes instead of reporting diagnostics | Use proper comparisons and call the method |
| 20 | Core subsystem: ubiquitous `except Exception as _ex: log.debug(...)` pattern | ~150+ locations across the codebase silently swallow exceptions at DEBUG level (invisible in production) | Critical failures (DB corruption, API errors, desync) are invisible | Change `log.debug` to `log.warning` or `log.error` |

### HIGH

| # | File:Line | Description | Why it matters | Suggested fix |
|---|-----------|-------------|----------------|---------------|
| 1 | `main.py:381,392` | `response.choices[0].message.content` and `response.content[0].text` — unchecked index access on API responses | Empty `choices` list (API error) raises IndexError | Check `len()` before indexing |
| 2 | `main.py:632-638` | Race condition: background thread mutates `memory_engine.memory_collection` while main async flow also accesses it | Memory corruption from concurrent ChromaDB access | Add `asyncio.Lock` around memory collection access |
| 3 | `main.py:580-640 vs 694-734` | ~100 lines of duplicated SQL+Vector dual-sync logic in `_wrap_respond` and `_wrap_save_constraint` | Update one but not the other → silent desync between SQLite and vector DB | Extract into shared `_dual_sync()` function |
| 4 | `core/orchestration/orchestrator.py:1184` | `retain[:30]` on potentially-None result — None-slice crashes | Hard crash when memory retrieval returns None | Guard with `if retain: ...` |
| 5 | `core/orchestration/pipeline.py` | Multiple fire-and-forget async tasks without error handling | Background pipeline failures silently lost | Capture and log task exceptions |
| 6 | `core/filesystem/automation.py` | Thread-unsafe shared dicts without locks across async/thread boundary | Data corruption on concurrent writes | Add `threading.Lock` or `asyncio.Lock` |
| 7 | `core/scraping/web_scraping.py` | No URL scheme allowlist — potential SSRF via `file://` or internal network URLs | SSRF vulnerability in web scraping | Validate URL scheme against allowlist |
| 8 | `core/workers/python_worker.py` | Symlink escape risk in worker sandbox | Path traversal to write files outside workspace | Validate resolved path is within workspace |
| 9 | `core/security/security_memory.py` | In-memory-only security event storage (no persistence) | Security events lost on restart | Add SQLite or file-backed persistence |
| 10 | `specialists/base.py:84-108` | `async def execute()` calls synchronous `agent.send_user_message()` — blocks event loop | UI freeze during specialist execution | Use `asyncio.to_thread()` or make `send_user_message` async |
| 11 | `specialists/hermes.py:109` | Corrupted unicode `"ðŸ˜Š"`, `"ðŸ‘"` used as emoji sentinels | Emoji matching will always fail → dead code paths | Fix encoding or use Unicode codepoints directly |
| 12 | `specialists/herald.py:1069-1082` | Variable shadowing: outer `stats` overwritten by loop variable `for agent, stats in ...`; mutates dict while iterating | Subtle data corruption on iteration | Rename one of the variables |
| 13 | `runtime_next/recovery/engine.py:64,133` | `threading.Lock()` inside `asyncio`-based recovery engine blocks event loop thread | Event loop starvation under concurrent recovery | Replace with `asyncio.Lock()` |
| 14 | `runtime_next/engine/engine.py:196-219` | `handle_failure` called inline during node execution — re-entrant if handler fails | Cascading failure / infinite recursion | Queue failure handling as separate task |
| 15 | `runtime_next/capability/registry.py:217-223` | Predictable temp filename `aelvo_perm_test_{os.getpid()}.tmp` — TOCTOU symlink risk | Symlink attack on capability testing | Use `tempfile.mkstemp()` |
| 16 | `sandbox_core/process.rs:48,132-181` | Command injection surface: raw `cmd_str` passed to `cmd /C`/`sh -c`; `kill_process_tree` never awaited | Arbitrary shell execution + orphaned processes | Use exec-ve variants or safer process spawning |
| 17 | `sandbox_core/audit.rs:322-326` | Audit write failures silently absorbed with `if let Err(_e) = self.write_entry()` | Security events silently lost | Surface write errors to caller |
| 18 | `sandbox_core/checkpoint.rs:496` | Insufficient path sanitization — `replace("..", "__")` bypassable with `....//` | Path traversal in checkpoint operations | Use canonical path resolution instead of string replace |
| 19 | `auth/cred_storage.py:280` | `log.info(f"...")` uses f-string — evaluates regardless of log level, leaks credential metadata in log config | Credential metadata leaked to non-INFO log configs | Use `log.info("%s", ...)` lazy formatting |
| 20 | `mcp/execution/execution_engine.py:162-173,263-271` | No timeout on `transport.send()`; `_receive_response` loops forever if server never sends matching message ID | Hangs entire execution engine | Add timeout to send/receive |
| 21 | `mcp/governance/governance_layer.py:87` | Empty `request.server_id` not validated — passes allowlist check | Authentication bypass for governance | Validate server_id is non-empty |
| 22 | `cognition/blackboard.py` (multiple locations) | All shared state dicts (`_slots`, `_subscriptions`, `_challenges`, `_votes`, `_consumptions`) modified without locks | Race conditions and data corruption in concurrent access | Add `asyncio.Lock` for all state mutations |
| 23 | `cognition/state.py` (all methods) | `_goals`, `_sub_goals`, `_blocked_paths`, `_hypotheses` all modified without locks | Race conditions in cognitive state | Add locks |
| 24 | `planning/goal_hierarchy.py:67` | `_nodes` dict modified without lock across all mutations | Race condition in goal hierarchy | Add threading lock |
| 25 | `learning/accumulator.py:231-234` | `for/else` logic bug: `else` on `for` loop always increments `skipped` from 0 to 1, so "Skipped 1 duplicate patterns" always logs even when none skipped | Misleading log output + dead code path | Remove the `else` clause or restructure the loop |
| 26 | `learning/agent_metrics.py:219-223` | `success_rate` only recomputed on `success=True`; `implementation_count` always increments → `success_rate` converges incorrectly over mixed-success calls | Incorrect metrics tracking | Recompute `success_rate` on every call |
| 27 | `tools/code_tools.py:427` | `py_files = py_files[:1000]` — silent truncation of analysis to 1000 files | Code analysis silently incomplete for large projects | Log warning when truncated |
| 28 | `tools/security_tools.py:9-22` | `Generic Assignment Secret` pattern matches ANY variable named `secret`/`password` | High false positive rate | Use entropy-based detection as complement |
| 29 | `ui/core/bridge.py:47,64` | `self._assigned_task_ids: set = set()` declared TWICE — second overwrites first | Confusing initialization, potential bug if first assignment intended | Remove duplicate |
| 30 | `web/src` (7+ files) | Agent config maps (color/icon/label) duplicated across 7+ components with different values | Inconsistencies in agent display — some files have HERMES but not CONSENSUS and vice versa | Centralize into single shared config file |
| 31 | `web/tsconfig.json:15-16` | `"noUnusedLocals": false`, `"noUnusedParameters": false` | TypeScript won't catch dead code | Enable for CI builds |
| 32 | `memory/forge_memory.py:206-208`, `memory/user_model.py:208,214` | ChromaDB query results accessed as `res["ids"][0][0]` — IndexError on empty results | Crash when ChromaDB returns no matches | Check result emptiness before indexing |

### MEDIUM

| # | File:Line | Description |
|---|-----------|-------------|
| 1 | `main.py:614-615,728-729` | Logs "FATAL" but continues execution — misleading severity label, not actually fatal |
| 2 | `main.py:1158` | `asyncio.get_event_loop()` deprecated in Python 3.10+; will warn/fail in 3.12+ |
| 3 | `main.py:997-1009` | API keys stay in process memory after use — no zeroing |
| 4 | `main.py:137-145` | YAML frontmatter parsed by splitting raw text on `---` instead of proper parser |
| 5 | `core/orchestration/pipeline.py:435,454,720,728,1440` | Multiple `except Exception: pass` — silent failure in pipeline execution |
| 6 | `core/orchestration/verification_coordinator.py` | Verification results unused in coordinator flow |
| 7 | `core/security/execution_governance.py` | Security governance stub methods return default-allow |
| 8 | `core/scraping/web_scraping.py` | No timeout on web scraping requests |
| 9 | `specialists/hermes.py:419-450` | Substring keyword matching — `"delete"` matches `"undelete"`, `"drop"` matches `"backdrop"` |
| 10 | `specialists/architect.py:82` | Task type classification uses substring match without word boundary |
| 11 | `specialists/architect.py:88-92` | Requirements sliced to `[:10]` and ambiguities to `[:5]` — silently drops data |
| 12 | `specialists/architect.py:104,273` and `forge.py:1033` | Skip set `{".git", "__pycache__", ...}` duplicated in 3 locations — should be shared constant |
| 13 | `specialists/architect.py:1048` | Default fallback is `APPROVE` — any unhandled consensus condition silently approves |
| 14 | `specialists/sentinel.py:194-198` | `"secret" in logs` substring matching — false positive on `"not_a_secret"` |
| 15 | `specialists/terminus.py:386` | `rm -rf /` blocked but NOT `rm -rf /*`, `rm -rf /var`, or `rm -rf / --no-preserve-root` |
| 16 | `runtime_next/plan/brain/*.py` (13 files) | Identical copy-pasted docstring on all files — says "brain.py" regardless of actual module |
| 17 | `runtime_next/plan/brain/*.py` | Import header duplicated across all 13 brain sub-modules (~7 lines each) |
| 18 | `runtime_next/engine/engine.py` and `recovery/engine.py` and `verification/driven_recovery.py` | Duplicated recovery pipeline logic across 3 subsystems (classify→govern→safety→recover→record) |
| 19 | `runtime_next/capability/registry.py:104-110` | `except ImportError: pass` on missing `specialists` module — silent import failure |
| 20 | `sandbox_core/fs_jail.rs:596-598` | Temp file extension collision: uses PID only → concurrent writes to same-stem files collide |
| 21 | `sandbox_core/fs_jail.rs:352-355` | URL encoding bypass: checks `%2e%2e` but NOT `%252e%252e` (double encoding) |
| 22 | `sandbox_core/threat_detection.rs:232` | `> /dev/null 2>&1` flagged as dangerous shell pattern — extremely common idiom → false positive/alert fatigue |
| 23 | `sandbox_core/resource.rs:385-393` | `diagnose_limit_violation` is `#[allow(dead_code)]` and never called |
| 24 | `sandbox_core/process.rs:194-199` | Resource governor enforcement is non-fatal → process runs without resource limits on failure |
| 25 | `sandbox_core/audit.rs:436-438` | Malformed audit log lines silently discarded during read |
| 26 | `auth/config.py:778-782` | `model.provider = provider_key` mutates shared model objects across providers → last registration wins for duplicate model IDs |
| 27 | `mcp/governance/governance_layer.py:176-184` | `_check_rate_limit` and `_check_side_effects` are stubs that always return True |
| 28 | `mcp/verification/timeout_verifier.py:17-24` and `capability_verifier.py:16-22` | Always return PASS — no actual verification |
| 29 | `mcp/recovery/capability_refresh.py:26-29` | Calls `get_profile()` but never `refresh_capabilities()` — no-op |
| 30 | `mcp/recovery/failover_strategy.py:27-32` | Returns True but never performs actual rerouting |
| 31 | `mcp/recovery/trust_downgrade.py:26-27` | Hardcoded placeholder `TrustLevel.VERIFIED` — always downgrades from VERIFIED regardless of actual state |
| 32 | `mcp/execution/execution_queue.py:26` | `asyncio.PriorityQueue` with tuple containing non-comparable `MCPExecutionRequest` → TypeError on same-priority items |
| 33 | `mcp/registry/registry_store.py` (multiple) | New SQLite connection per operation, no WAL mode, no connection pooling |
| 34 | `mcp/*/*.py` (30+ locations) | `datetime.utcnow()` deprecated since Python 3.12 |
| 35 | `cognition/blackboard.py:214,208` | ChromaDB results accessed as `res["metadatas"][0][0]` — IndexError on empty |
| 36 | `cognition/consensus_extended.py:460-467` | `_apply_strategy` returns `APPROVED_WITH_RISK` for `ARCHITECT_DECIDES` even though architect hasn't decided |
| 37 | `cognition/blackboard_schemas.py:251` | `ENTRY_SCHEMA_REGISTRY[schema_type]` — unhandled KeyError on unknown schema |
| 38 | `planning/integration.py:217` | Monkey-patching `self.orchestrator.build_shared_context` — breaks encapsulation |
| 39 | `planning/goal_hierarchy.py:406-419` | Naive keyword overlap for request alignment classification |
| 40 | `learning/knowledge_graph.py:704` | SQLite VACUUM requires exclusive access — no error handling for SQLITE_BUSY |
| 41 | `tools/code_tools.py:82-83,180` | Broad `except Exception` returns generic error string, loses type information |
| 42 | `tools/code_tools.py:107-112,333-335` | `language` and `test_filter` validated but could be restricted further |
| 43 | `shared_task_board/board.py` | SQLite operations without WAL mode |
| 44 | `ui/events/event_bus.py:127` | `subscribe`/`unsubscribe` not protected by lock in async context |
| 45 | `ui/menu.py:110,129` | `projects[int(del_choice)-1]` — no bounds check, IndexError on out-of-range |
| 46 | `web/src/hooks/useWebSocket.ts:54` | Silent `JSON.parse` failure — no console.warn on malformed WebSocket messages |
| 47 | `web/src/pages/AdminSettings.tsx:222-227` | No WebSocket URL validation — `javascript:` or `file://` silently accepted |
| 48 | `web/src/types.ts:2` | `UIEventType = string` defeats type safety — should be union of literal strings |
| 49 | `config/settings.py` | No config validation framework — all settings are plain strings/numbers with no schema |
| 50 | `docs/api_runtime_next.md` | Lists `ConnectionPool` (database connection pool) in runtime_next/scaling but it does not exist in the actual `runtime_next/scaling/` directory |

### LOW

(Selected representative items from ~100+ LOW-severity findings)

| # | File:Line | Description |
|---|-----------|-------------|
| 1 | `main.py:22,24,37,44,48` | Multiple unused/duplicate imports (7 instances) |
| 2 | `main.py:267,379,387,608` | Magic numbers (7200 cache TTL, 0.1 temperature, 2048 max_tokens, 0.6 importance) |
| 3 | `main.py:383,471` | Typo comments: "Anthtopic" → "Anthropic", "Surgically" → "Surgically" |
| 4 | `main.py:1132` | Case-sensitive env var check — misses `"True"`, `"TRUE"`, `"Yes"` |
| 5 | `main.py:244,258,273,346-347` | Imports inside function bodies (11+ instances) |
| 6 | `core/*.py` (multiple) | Magic numbers, unused imports, non-ASCII box-drawing chars in strings |
| 7 | `specialists/base.py:12` | `activation_threshold: float = 0.6` — magic number |
| 8 | `runtime_next/plan/architect_types.py:697` | Dead code path: `intelligence_status == "unknown"` check but default is `"unavailable"` |
| 9 | `sandbox_core/fs_jail.rs:245,762` and `checkpoint.rs:522` | Three nearly identical directory walkers |
| 10 | `sandbox_core/audit.rs:296-311` | Manual Gregorian calendar math (`is_leap`) instead of using `chrono` |
| 11 | `sandbox_core/resource.rs:203` | `JobObject` created without name — makes debugging harder |
| 12 | `sandbox_core/process.rs:93` | Hard-coded timeout cap at 300s silently applied |
| 13 | `auth/tests/test_config.py:72,83` | Brittle test assertions (`len(PROVIDER_REGISTRY) == 20`) |
| 14 | `mcp/*.py` (multiple) | `hashlib`, `json`, `time` duplicated across many files |
| 15 | `cognition/types.py` (multiple) | `Field(default_factory=datetime.utcnow)` — naive datetimes |
| 16 | `learning/classifier.py:19`, `confidence.py:20`, `delta.py:22` | PEP8 violation: `import threading` not at top of file |
| 17 | `web/src/components/AgentDashboard.tsx:165` | Magic number `5` for recent actions limit |
| 18 | `web/src/components/SessionSidebar.tsx:17` | Magic `86400000` / `604800000` for day/week boundaries |
| 19 | `web/src/components/RecoveryState.tsx:81` | Fragile string manipulation `barColor.replace("bg-", "text-")` |
| 20 | `web/src/context/SettingsContext.tsx:11` | Hardcoded `"ws://127.0.0.1:8765"` fallback |

---

## Architecture Notes

### Module/Crate Map

```
main.py (entry point)
├── core/ — Orchestration, Execution, Filesystem, Security, RAG, Workers
├── specialists/ — 7 AI agents (Hermes → Architect → Oracle → Forge → Sentinel → Terminus → Herald)
├── runtime_next/ — Production runtime (Engine, Events, Verification, Recovery, Governance, Monitoring, Scaling, Security)
├── auth/ — Multi-provider auth ecosystem (21 LLM providers, credential storage, diagnostics)
├── mcp/ — Model Context Protocol platform (discovery, registry, transport, execution, governance, verification, recovery)
├── cognition/ — Cognitive engine (blackboard, consensus, coordination, planning, research, trust)
├── planning/ — Long-horizon planning (goal hierarchy, self-critique, plan evolution, debt forecasting)
├── memory/ — Memory systems (forge memory, user model)
├── learning/ — Pattern extraction (accumulator, agent metrics, classifier, confidence, knowledge graph)
├── repo_intelligence/ — Codebase analysis (symbol/dependency/call graphs, impact analysis)
├── shared_task_board/ — Task board with state machine
├── tools/ — Extended tool registry (git, code, security, diagram, research)
├── ui/ — TUI dashboard (Textual framework)
├── tests/ — 50+ test files
└── web/ — React+TypeScript web dashboard
```

### Dependency Graph Observations

- **Cyclic dependency risk**: `cognition/` and `specialists/` both reference each other. `cognition/engine.py` imports from `specialists/` while `specialists/architect.py` references `cognition.types`. This creates a potential circular import issue that's currently "resolved" via deferred (lazy) imports, which is fragile.
- **runtime_next/ duplicates core/**: There are two orchestration layers — `core/orchestration/` (legacy) and `runtime_next/` (new). They overlap significantly: both have verification pipelines, recovery engines, governance, and monitoring. The coexistence is documented (a toggle `use_legacy_recovery` exists at `runtime_next/recovery/engine.py:43`) but creates confusion about which path is active.
- **Monolithic main.py**: At 1621 lines, `main.py` is a God object that bootstraps every subsystem, manages state, and implements the REPL loop. It should be broken into at least 5-7 modules.
- **auth/ is appropriately decoupled**: The auth subsystem is cleanly isolated with clear interfaces, which is the right design for a 21-provider integration.

### Boundary Violations

- `core/filesystem/automation.py` accesses private method `fs._validate_path()` from `main.py:644` — breaks encapsulation
- `mcp/memory/capability_memory.py:30` accesses `self._memory_store._db_path` (private attribute of another class)
- `planning/integration.py:217` monkey-patches `self.orchestrator.build_shared_context` — runtime method replacement

### Divergence from Stated Design

- README.md shows a clean 7-specialist linear pipeline, but the actual code has additional routing via `@SPECIALIST` prefix, direct kernel commands (`#providers`, `#doctor`), and conditional consensus/recovery bypasses that aren't documented
- `docs/api_runtime_next.md` documents a `ConnectionPool` class under `runtime_next.scaling` that does not exist in the actual code
- MCP subsystem documentation claims comprehensive verification, but 6+ verifiers/strategies are stubs that always return PASS/True

---

## Security Notes

### CRITICAL
1. **`main.py:670,674`** — Arbitrary code execution via LLM prompt injection (bash_exec/python_exec). Severity: CRITICAL
2. **`tools/git_tools.py:11-12,93,209`** — Shell argument injection via branch names. Severity: CRITICAL
3. **`tools/code_tools.py:333-335`** — Pytest expression injection via test_filter. Severity: CRITICAL
4. **`sandbox_core/process.rs:132-181`** — Raw command string passed to shell. Severity: CRITICAL
5. **`auth/cred_storage.py:65`** — Broken credential encryption on Windows (non-deterministic machine ID). Severity: CRITICAL
6. **`auth/cred_storage.py:227`** — First-use crash on credential store. Severity: CRITICAL
7. **`auth/auth/oauth.py:125`** — Refresh tokens in plaintext metadata. Severity: CRITICAL
8. **`mcp/discovery/filesystem_discovery.py:37-48`** — Auto-registration of PATH executables. Severity: CRITICAL
9. **`core/scraping/web_scraping.py`** — No URL scheme allowlist (SSRF). Severity: HIGH

### HIGH
10. **`main.py:997-1009`** — API keys not zeroed after use, stays in process memory
11. **`core/security/security_memory.py`** — Security events lost on restart (in-memory only)
12. **`specialists/architect.py:1048`** — Default-APPROVE for unhandled consensus conditions
13. **`runtime_next/capability/registry.py:217-223`** — Predictable temp filename, TOCTOU symlink risk
14. **`auth/cred_storage.py:153`** — SQLite `check_same_thread=False` without explicit locking
15. **`auth/config.py:778-782`** — Shared model object mutation across providers (last-registration-wins)
16. **`mcp/governance/governance_layer.py:87`** — Empty server_id passes allowlist
17. **`mcp/governance/allowlist_manager.py:56`** — Default-allow when no allowlists configured

### MEDIUM
18. **`sandbox_core/fs_jail.rs:352-355`** — Double-URL encoding bypass potential
19. **`sandbox_core/threat_detection.rs:232`** — False positive on common shell idioms (alert fatigue)
20. **`mcp/discovery/filesystem_discovery.py:76`** — TOCTOU between `os.access()` and execution
21. **`mcp/execution/execution_engine.py:162-173,263-271`** — No timeout on transport send/receive
22. **`auth/runtime/session.py:50`** — Access tokens in plaintext `to_dict()`

### LOW
23. **`main.py:150-157`** — Local file paths leaked in system prompt sent to external LLM API
24. **`sandbox_core/fs_jail.rs:596-605`** — Atomic write not cross-volume safe on Windows

---

## Test Coverage Notes

### Qualitative Assessment by Module

| Module | Coverage Level | Notes |
|--------|---------------|-------|
| `core/orchestration/` | **Partially tested** | Pipeline, orchestrator, session_manager have tests; verification_coordinator, task_board_pipeline are untested |
| `core/execution/` | **Untested** | No dedicated tests for commands.py, experience_pipeline.py, sandbox_session.py, tool_registry.py |
| `core/filesystem/` | **Untested** | automation.py has no tests |
| `core/governance/` | **Untested** | kernel.py (MemoryEngine) has no direct tests |
| `core/security/` | **Untested** | All 6 security modules lack dedicated tests |
| `core/rag/` | **Partially tested** | Some retrieval tests exist, but chunking, types untested |
| `core/scraping/` | **Untested** | web_scraping.py has no tests |
| `core/workers/` | **Untested** | python_worker.py lacks tests |
| `specialists/` | **Partially tested** | forge.py has tests (integration-style, some flaky); hermes, architect, oracle, sentinel, terminus, herald have minimal coverage |
| `runtime_next/engine/` | **Partially tested** | Some engine tests exist; runner, file_mutex may be tested in integration |
| `runtime_next/events/` | **Tested** | EventBus has reasonable coverage |
| `runtime_next/verification/` | **Partially tested** | Pipeline and classifier have tests; many verifiers lack unit tests |
| `runtime_next/recovery/` | **Partially tested** | RecoveryEngine has tests; sub-engines less tested |
| `runtime_next/governance/` | **Partially tested** | Policy engine has tests |
| `runtime_next/monitoring/` | **Tested** | Good coverage on metrics, health, alerts |
| `runtime_next/plan/` | **Untested** | architect.py, builder.py, calibration.py — no dedicated unit tests |
| `runtime_next/scaling/` | **Untested** | async_pipeline, batch_processor, resource_pool lack tests |
| `runtime_next/security/` | **Untested** | scanner, policy_audit, sandbox_integrity lack tests |
| `auth/` | **Partially tested** | Provider config tests exist; credential storage, OAuth, runtime tests are thin |
| `mcp/` | **Minimally tested** | Very few unit tests for MCP subsystems |
| `cognition/` | **Minimally tested** | Only `tests/test_cognition.py` exists — does not cover consensus_extended, coordination, etc. |
| `planning/` | **Untested** | No dedicated test files |
| `memory/` | **Untested** | forge_memory.py, user_model.py have no dedicated tests |
| `learning/` | **Untested** | No test files exist for the learning engine |
| `repo_intelligence/` | **Untested** | No tests exist |
| `shared_task_board/` | **Untested** | No dedicated tests |
| `tools/` | **Untested** | git_tools.py, code_tools.py, security_tools.py — command execution tools with NO tests |
| `ui/` | **Untested** | TUI dashboard has no tests |
| `web/` | **Partially tested** | 6 component test files exist, but many components, hooks, pages lack tests |
| `sandbox_core/` | **Untested** | No Rust tests exist |

### Biggest Test Gaps

1. **`tools/` (CRITICAL)**: `git_tools.py` and `code_tools.py` execute shell commands with user-controlled input and have ZERO tests
2. **`core/security/` (CRITICAL)**: Security modules governing execution approval, sandboxing, analytics have zero tests
3. **Sandbox integration**: No tests verify the Python orchestrator → Rust sandbox communication
4. **`mcp/`**: The entire MCP platform has minimal testing despite being a major subsystem
5. **`learning/` and `planning/`**: Learning engine and long-horizon planning have no test coverage

### Known Flaky Tests
- `tests/test_forge.py::test_memory_write_and_retrieval_roundtrip` — timing-dependent
- `tests/test_forge.py::test_deduplication_prevents_duplicate_entries` — same timing pattern
- Some auth tests have brittle assertions (e.g., `assert len(PROVIDER_REGISTRY) == 20`)

---

## Dependency Notes

### Python (`requirements.txt`)

| Dependency | Status | Notes |
|-----------|--------|-------|
| `pydantic>=2.0.0` | ✅ OK | Well-pinned |
| `cryptography>=41.0.0` | ✅ OK | Used in auth credential encryption |
| `chromadb>=0.4.0` | ✅ OK | Vector store |
| `scrapy`, `scrapy-playwright` | ⚠️ Over-specified | Heavy dependencies for web scraping; only light use in `core/scraping/` |
| `textual>=0.44.0` | ✅ OK | TUI framework |
| `prompt_toolkit>=3.0.0` | ✅ OK | CLI input |
| **Missing**: No `pyproject.toml` or `setup.py` | ⚠️ | Project is not installable as a package; must be run from source root |

### Rust (`sandbox_core/Cargo.toml`)

| Dependency | Status | Notes |
|-----------|--------|-------|
| `serde`, `serde_json` | ✅ OK | JSON serialization |
| `regex` | ✅ OK | Pattern matching |
| **Missing**: `libc` | 🚫 CRITICAL | Used in `process.rs:149` via `libc::setrlimit` — Unix build broken |
| **Missing**: `chrono` | ⚠️ | Manual date math in `audit.rs` reinvents `chrono` functionality |

### TypeScript/React (`web/package.json`)

| Dependency | Status | Notes |
|-----------|--------|-------|
| React 19 | ✅ OK | Latest stable |
| Vite 6 | ✅ OK | Modern bundler |
| TypeScript 5.7 | ✅ OK | Good |
| Tailwind 3.4 | ✅ OK | |
| Vitest 4.1 | ✅ OK | Test framework |
| `jsdom`, `@testing-library/react` | ✅ OK | Test utilities |

### Unused/Redundant Dependencies
- No unused declared dependencies identified in any lockfile
- The `scrapy` dependency is very heavy for the limited web scraping usage — could potentially be replaced with `httpx` + `beautifulsoup4` for the light usage pattern

---

## Technical Debt Inventory

### TODO/FIXME/HACK Comments

No literal `TODO`, `FIXME`, or `HACK` comments were found in the codebase. However, the following represent undocumented debt:

| Location | Type | Description |
|----------|------|-------------|
| `main.py:920` | Stale comment | "scaffolding omitted for brevity, will keep it in full" — code was NOT omitted |
| `main.py:311` | Stale troubleshooting note | "PERSISTENT CLIENTS (Fix: Reuse connection to eliminate SSL/DNS lag)" |
| `main.py:582,589,617,698,701` | Missing documentation | Phase references ("PHASE 4", "PHASE 7", "PHASE 8") — no documentation of what these phases mean |
| `core/*.py` (multiple) | Copy-paste debt | Skip sets (`{".git", "__pycache__", ...}`) defined in 3+ places independently |
| `runtime_next/plan/brain/*.py` (13 files) | Copy-paste debt | Identical docstrings and import headers across all 13 brain sub-modules |
| `runtime_next/engine/engine.py` + `recovery/engine.py` + `verification/driven_recovery.py` | Duplication | Three independent implementations of the classify→govern→safety→recover→record recovery pipeline |
| `mcp/governance/governance_layer.py:176-184` | Stub | `_check_rate_limit` and `_check_side_effects` always return True |
| `mcp/verification/timeout_verifier.py:17-24` | Stub | Always returns PASS |
| `mcp/verification/capability_verifier.py:16-22` | Stub | Always returns PASS |
| `mcp/recovery/capability_refresh.py:26-29` | No-op | Calls `get_profile()` but never `refresh_capabilities()` |
| `mcp/recovery/failover_strategy.py:27-32` | No-op | Returns True but never performs actual rerouting |
| `sandbox_core/resource.rs:385-393` | Dead code | `diagnose_limit_violation` is `#[allow(dead_code)]` |
| `web/src` (7+ files) | Duplication | Agent config maps (color/icon/label) duplicated across 7 components |
| `web/src` (~15 files) | Duplication | Time formatting functions (`formatRelative`, `formatTimestamp`, etc.) duplicated across 15+ components |
| `web/src/components` (5 files) | Duplication | `SummaryCard` component duplicated with different props in 5 dashboard files |
| `main.py:580-640,694-734` | Duplication | ~100 lines of SQL+Vector dual-sync logic duplicated |
| `main.py:544-822` | God function | `build_tool_registry()` is 278 lines defining 15 nested closures |
| `main.py:894-1591` | God function | `main_async()` is ~700 lines — REPL, LLM init, MCP init, TUI, parsing, session tracking |
| `cognition/blackboard.py` | Concurrency debt | All shared state dicts lack locks — documented pattern across cognition/planning modules |
| `auth/cred_storage.py` | Windows support gap | Machine identity not deterministically derivable on Windows |
| `auth/tests/test_config.py:72,83` | Brittle tests | Hardcoded counts (`== 20`, `>= 40`) break on provider list changes |

### Pattern: Silent Error Swallowing (Largest Single Debt Item)

The pattern `except Exception: pass` or `except Exception as _ex: log.debug(...)` appears in ~150+ locations. This is by far the largest source of technical debt. Key locations:

- `main.py`: lines 133, 145, 269, 282, 412, 518, 525, 537, 630, 636, 887, 1167, 1575, 1582
- `core/orchestration/pipeline.py`: lines 435, 454, 720, 728, 1351, 1383, 1440
- `core/orchestration/task_board_pipeline.py`: lines 828, 885, 925, 1103, 1141, 1232, 1279
- `specialists/`: 20+ locations across base.py, hermes.py, architect.py, forge.py, sentinel.py, terminus.py
- `runtime_next/`: 15+ locations across multiple files
- `cognition/`: 15+ locations across blackboard.py, consensus.py, coordination.py, etc.
- `auth/cred_storage.py`: 5+ locations
- `mcp/`: 10+ locations
- `tools/`: 5+ locations
- `ui/`: 5+ locations

---

## Recommended Priority Order

| Priority | Item | Severity | Effort | Rationale |
|----------|------|----------|--------|-----------|
| 1 | Fix `main.py:670,674` — sandbox basheval/python_exec | CRITICAL | 2 days | Active RCE from prompt injection; worst-case security impact |
| 2 | Fix `tools/git_tools.py:11-12,209` — branch name injection | CRITICAL | 1 day | Direct shell command injection via user-controlled input |
| 3 | Fix `main.py:269-270,282-283` + all silent `except` handlers (~150 locations) | CRITICAL | 3 days | Systemic failure invisibility; bump `log.debug` to `log.warning` |
| 4 | Add `libc` to `sandbox_core/Cargo.toml` | CRITICAL | 0.5 day | Unix build is completely broken |
| 5 | Fix `sandbox_core/main.rs:251-252` — response serialization failure | CRITICAL | 0.5 day | Python orchestrator hangs indefinitely |
| 6 | Fix `auth/cred_storage.py:65,227` — Windows credential unrecoverability | CRITICAL | 1 day | Encrypted credentials lost on every restart |
| 7 | Fix `auth/auth/oauth.py:125` — refresh token in plaintext | CRITICAL | 1 day | OAuth refresh tokens leaked in DB metadata column |
| 8 | Fix `mcp/discovery/filesystem_discovery.py:37-48` — auto-register PATH executables | CRITICAL | 1 day | Unauthenticated code execution via MCP |
| 9 | Fix `mcp/registry/health_tracker.py:54-56` — UnboundLocalError | CRITICAL | 0.5 day | Crash on specific health tracking path |
| 10 | Fix `web/src/main.tsx` — missing error boundary | CRITICAL | 0.5 day | Entire web app white-screens on any render error |
| 11 | Fix `specialists/forge.py:~751` — undefined variable `context` | CRITICAL | 0.5 day | NameError at runtime kills code generation specialist |
| 12 | Fix `specialists/architect.py:1109-1110` — unchecked KeyError | CRITICAL | 0.5 day | CRASH in architect planner |
| 13 | Fix `core/filesystem/automation.py` — thread-unsafe shared dicts | HIGH | 1 day | Data corruption on concurrent writes |
| 14 | Fix `core/scraping/web_scraping.py` — SSRF | HIGH | 0.5 day | Internal network scanning via scraping |
| 15 | Consolidate dual-sync logic in `main.py` (lines 580-640, 694-734) | HIGH | 1 day | Prevents silent SQL/Vector desync |
| 16 | Add locks to cognition/blackboard.py, state.py, goal_hierarchy.py | HIGH | 2 days | Data races in core cognitive state |
| 17 | Fix `runtime_next/recovery/engine.py:64` — threading.Lock in async context | HIGH | 0.5 day | Event loop blocking |
| 18 | Fix `sandbox_core/process.rs:48,132` — command injection + orphaned processes | HIGH | 2 days | Unsafe shell execution in sandbox |
| 19 | Deduplicate specialist config maps in web/ | MEDIUM | 1 day | Inconsistent agent display across dashboard |
| 20 | Remove or implement MCP stub verifiers (6+ files) | MEDIUM | 2 days | False sense of security — documented as real but do nothing |
| 21 | Break up `main.py:894-1591` (`main_async()`) into smaller functions | MEDIUM | 2 days | Improves maintainability |
| 22 | Add tests for `tools/` and `core/security/` | MEDIUM | 3 days | Command execution tools with ZERO tests |
| 23 | Replace `datetime.utcnow()` with `datetime.now(timezone.utc)` (30+ locations) | LOW | 1 day | Deprecated in Python 3.12 |
| 24 | Enable `noUnusedLocals`/`noUnusedParameters` in web/tsconfig.json | LOW | 0.5 day | Catches dead TS code |
| 25 | Remove unused imports across codebase | LOW | 1 day | Code cleanup |
