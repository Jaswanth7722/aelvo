# AELVO Omega

**Automated Engineering & Logic-Verification OS**

AELVO is a web + terminal multi-agent engineering system that plans and executes complex software engineering tasks using **seven specialized AI agents** coordinated through a canonical pipeline with cross-cutting verification, recovery, and memory.

```
User → HERMES → ARCHITECT → ORACLE → FORGE → SENTINEL → TERMINUS → HERALD
         ↑                          Memory & Intelligence Layer                ↓
         ←──────────────────── Verification & Recovery ──────────────────────→
```

---

## Why AELVO Exists

Single-agent coding tools hit a wall on complex tasks. One agent cannot simultaneously:

- **Calibrate** to your communication style and expertise level
- **Plan** with repository-aware dependency analysis and risk assessment
- **Research** facts and verify claims against live sources
- **Generate** code with institutional pattern memory and error recovery
- **Secure** every change against vulnerabilities and credential leaks
- **Execute** DevOps commands safely with rollback planning
- **Report** results with strategic communication advisory

AELVO solves this with **seven specialized agents** that collaborate through a shared coordination layer with visible handoffs, role specialization, and state passing.

---

## Key Capabilities

| Capability | How AELVO Does It |
|---|---|
| **7 Specialized Agents** | HERMES, ARCHITECT, ORACLE, FORGE, SENTINEL, TERMINUS, HERALD |
| **Repository Intelligence** | Symbol graph, dependency graph, call graph, impact analysis |
| **Architect Intelligence** | 14-section strategic plans with verification and recovery design |
| **Verification Pipeline** | Lint, typecheck, security scan, graph consistency, sandbox validation |
| **Recovery Engine** | Failure classification, retry safety, governance, learned recovery memory |
| **Execution Graph** | DAG-based execution with node states, retry policies, output contracts |
| **Event System** | Typed async event bus with replayable logging |
| **Memory Systems** | Dual-sync (SQLite + ChromaDB vector), cross-specialist memory |
| **Learning Engine** | Pattern extraction from execution deltas, confidence calibration |
| **Web + Terminal UI** | Web dashboard with chat/Files/agent metrics, plus a CodeBuff-style terminal CLI |
| **Multi-Provider** | OpenAI, Anthropic, Google, Groq, Together, Mistral, NVIDIA, 20+ providers |
| **Long-Horizon Planning** | Session continuity, goal hierarchy, multi-session awareness |
| **Plan Calibration** | Track outcomes vs plans, adjust future strategies automatically |

---

## Quickstart

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/aelvo.git
cd aelvo

# Install Python dependencies
pip install -r requirements.txt

# Install the Rust sandbox (optional, for sandboxed execution)
cd sandbox_core && cargo build --release && cd ..
```

### Provider Setup

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your LLM provider API key:
#   LLM_PROVIDER=openai
#   API_KEY=sk-...
#   MODEL=gpt-4
#
# Or use NVIDIA:
#   LLM_PROVIDER=nvidia
#   NVIDIA_API_KEY=nvapi-...
#   MODEL=nvidia/nemotron-3-super-120b-a12b
```

### Launch

```bash
# Launch the interactive terminal CLI (default — CodeBuff / Claude Code style)
python main.py

# One-shot CLI — run a single prompt and exit
python main.py --ask "refactor the auth module to use async sessions"

# Launch the web dashboard instead
python main.py --web

# Launch with a specific provider and model
python main.py --provider openai --model gpt-4
```

---

## Terminal CLI (default — `python main.py`)

A dedicated interactive terminal agent in the spirit of CodeBuff / Claude
Code, reusing the exact same backend as the web dashboard. Type any natural
language task — Enter submits, `Esc+Enter` inserts a newline, and tool calls
(read/write/bash/scrape/memory) render live as the agent works.

```text
❯ refactor the authentication module to use async database sessions
  ✓ ✏️ write_file core/auth/session.py
  ✓ ⚙️ bash_exec python -m pytest tests/test_auth.py
  …
  **Done.** Migrated the session store to an async engine and verified it.
```

Force-route to specific specialists with `@SPECIALIST` prefixes:

```text
❯ @FORGE fix the race condition in worker.py
❯ @ORACLE research the latest changes to Python 3.13
❯ @ARCHITECT design a database schema for the new reporting feature
```

### Slash Commands

| Command | Description |
|---|---|
| `/help` | Show all commands |
| `/exit` · `/quit` | Exit the CLI |
| `/clear [history]` | Clear the screen; `/clear history` resets the conversation |
| `/workspace <dir>` · `/open` · `/cd` | Point the agent at a folder (re-jails its tools) |
| `/pwd` | Print the active workspace |
| `/status` | Provider, model, workspace + live agent metrics |
| `/projects` | List known workspaces |
| `/models` | List available models |
| `/retry` | Re-run the previous prompt |
| `/ask <prompt>` | Run a prompt without the agent loop |

---

## Web Dashboard (`python main.py --web`)

`python main.py --web` serves the web dashboard (HTTP + WebSocket bridge):
chat, a terminal-style **Files** page with an *Open as Workspace* action,
agent metrics, and provider setup from the browser. The terminal CLI is the
default interface; add `--no-browser` to run the server headless.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        AELVO OMEGA                               │
│                                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ HERMES    │→│ ARCHITECT│→│ ORACLE   │→│ FORGE    │        │
│  │ Calibrate │  │ Plan     │  │ Research │  │ Code     │        │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
│       ↓              ↓              ↓             ↓            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                     │
│  │ SENTINEL │→│ TERMINUS │→│ HERALD   │                     │
│  │ Security │  │ DevOps   │  │ Report   │                     │
│  └──────────┘  └──────────┘  └──────────┘                     │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │               Cross-Cutting Subsystems                    │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐  │  │
│  │  │ Runtime   │  │ Verify   │  │ Recovery │  │ Memory  │  │  │
│  │  │ Pipeline  │  │ Pipeline │  │ Engine   │  │ Systems │  │  │
│  │  └──────────┘  └──────────┘  └──────────┘  └─────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐    │
│  │ Repo     │  │ Architect│  │ Learning │  │ Provider     │    │
│  │ Intel    │  │ Brain    │  │ Engine   │  │ Runtime      │    │
│  └──────────┘  └──────────┘  └──────────┘  └──────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

### Key Files

| File | Purpose |
|---|---|
| `main.py` | Entry point, `AelvoAgent`, web/CLI boot |
| `cli/app.py` | Terminal CLI — REPL, live tool rendering, slash commands |
| `core/orchestration/orchestrator.py` | `Orchestrator` — central coordinator |
| `core/orchestration/pipeline.py` | `RuntimePipeline` — canonical execution pipeline |
| `specialists/*.py` | 7 specialist implementations |
| `runtime_next/engine/engine.py` | `ExecutionGraph` & `ExecutionEngine` |
| `runtime_next/events/bus.py` | Async typed `EventBus` with replay |
| `runtime_next/recovery/engine.py` | `RecoveryEngine` — failure classification & recovery |
| `runtime_next/verification/pipeline.py` | `VerificationPipeline` — plugin-based verifiers |
| `runtime_next/plan/architect.py` | `ArchitectOrchestrator` — strategic planning |
| `runtime_next/plan/brain.py` | 13-engine `ArchitectIntelligenceBrain` |
| `repo_intelligence/engine.py` | `RepoIntelligenceEngine` — symbol & dependency graphs |
| `learning/engine.py` | `PatternExtractionEngine` — execution pattern learning |
| `cognition/engine.py` | `CognitiveEngine` — goals, planning, research, consensus |
| `web/` | Web dashboard (React frontend + WebSocket bridge) |

---

## Example Workflow

```text
$ python main.py
AELVO — the automated engineering & logic-verification agent
────────────────────────────────────────────────────────────────
  project: default   provider: nvidia   model: nvidia/nemotron-3-super
  workspace: D:/aelvo/workspace/default

❯ Fix the race condition in the worker pool module

[Thinking] ⠋
✓ Pipeline completed: SUCCESS in 12.3s with 7 phases (1 LLM call)

  ── AELVO PIPELINE EXECUTION ──
  Phases: calibration → planning → research → implementation → security → execution → reporting
  Result: ✅ SUCCESS
  Duration: 12300ms
  Memory: Consolidated

  Verification:
    ✓ calibration: verification passed
    ✓ planning: verification passed
    ✓ implementation: verification passed
    ✓ security: verification passed
    ✓ execution: verification passed

[AELVO] Fixed the race condition in worker_pool.py by replacing the
shared mutable state with an asyncio.Queue and adding proper worker
lifecycle management. The fix was verified with type checks and tests.
```

---

## License

MIT — see LICENSE for details.
