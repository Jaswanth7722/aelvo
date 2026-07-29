# AELVO Omega

**Automated Engineering & Logic-Verification OS**

AELVO is a terminal-based multi-agent engineering system that plans and executes complex software engineering tasks using **seven specialized AI agents** coordinated through a canonical pipeline with cross-cutting verification, recovery, and memory.

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
| **TUI Dashboard** | Real-time panels for specialists, execution, tools, memory, verification, safety |
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
# Launch with TUI (default — recommended)
python main.py

# Launch with CLI (legacy REPL mode)
python main.py --cli

# Launch with a specific provider and model
python main.py --provider openai --model gpt-4
```

---

## CLI Usage

Once in the REPL, type any natural language task:

```text
YOU > Refactor the authentication module to use async database sessions
```

Or use `@SPECIALIST` prefix to force-route directly to a specialist:

```text
YOU > @FORGE fix the race condition in worker.py
YOU > @ORACLE research the latest changes to Python 3.13
YOU > @ARCHITECT design a database schema for the new reporting feature
```

### Available Commands

| Command | Description |
|---|---|
| `#help` | Show all available commands |
| `#providers list` | List all configured LLM providers |
| `#providers health` | Show provider health status, latency, error rates |
| `#providers models [name]` | List models for a provider |
| `#doctor scan` | Run full provider diagnostic scan |
| `#diagnostics auth` | Show auth configuration for all providers |
| `#diagnostics capabilities` | Show capability matrix across providers |
| `#diagnostics compare p1,p2` | Compare two providers side-by-side |
| `#lock <key> <value>` | Lock a constraint in the anchor |
| `#checkpoint <name>` | Save a system checkpoint |
| `exit` or `quit` | Shut down AELVO |

---

## TUI Usage

The Textual-based TUI dashboard (launched by default) provides real-time visibility into:

- **Specialist Activity** — which agents are active, thinking, or acting
- **Execution Graph** — task queue with status tracking
- **Tool Calls** — real-time tool execution stream
- **Memory Operations** — retrievals, storage, injections
- **Verification Results** — pass/fail with confidence scores
- **Safety Events** — security checks, risk classification, approvals
- **Timeline** — chronological event log across all subsystems

Shortcuts in TUI:

| Key | Action |
|---|---|
| `Ctrl+C` | Quit |
| `Ctrl+D` | Toggle dark mode |
| `Ctrl+L` | Clear timeline |
| `Ctrl+P` | Focus input |
| `Ctrl+O` | Focus output |

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
| `main.py` | Entry point, `AelvoAgent`, CLI/TUI loop |
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
| `ui/app.py` | `AelvoTUI` — Textual dashboard |

---

## Example Workflow

```text
$ python main.py
╔═══════════════════════════════════════════════════════════════╗
║                      AELVO OMEGA                             ║
║   Autonomous Engineering & Logic-Verification Operating System ║
╠═══════════════════════════════════════════════════════════════╣
║   Provider: nvidia  |  Model: nvidia/nemotron-3-super        ║
║   Project: default  |  Memory: dual-sync (SQLite + Chroma)   ║
╚═══════════════════════════════════════════════════════════════╝

YOU > Fix the race condition in the worker pool module

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

## Screenshots

> _AELVO TUI dashboard showing specialist activity, execution graph, tool calls, memory, verification, and safety panels side-by-side._

---

## License

MIT — see LICENSE for details.
