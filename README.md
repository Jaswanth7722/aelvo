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
| **Multi-Provider** | 22+ providers: OpenAI, Anthropic, Google, Groq, Mistral, Cohere, xAI, DeepSeek, Together, Fireworks, Perplexity, OpenRouter, Hugging Face, NVIDIA, Azure, Bedrock, Vertex + local (Ollama, LM Studio, vLLM, llama.cpp) |
| **Long-Horizon Planning** | Session continuity, goal hierarchy, multi-session awareness |
| **Plan Calibration** | Track outcomes vs plans, adjust future strategies automatically |

---

## Quickstart

### Install via npm (recommended)

```bash
# One-liner — installs the CLI, creates a Python venv, and installs deps
npm install -g aelvo

# Activate from ANY folder — the command is the activation (claude/codex style)
aelvo
```

`aelvo` opens the **current working directory**. `aelvo <folder>` opens any
folder. Per-folder state (memory, anchor, backups) lives in a hidden
`.aelvo/` directory inside the opened folder, so your project tree stays
clean and every folder gets its own isolated memory. Global state (credential
vault, global memory, logs) lives in `~/.aelvo/` (`AELVO_DATA_DIR` to
override). The Rust sandbox is optional — a pure-Python fallback provides the
same file tools + policy when it's absent.

### Install from source

```bash
git clone https://github.com/aelvolabs/aelvo.git
cd aelvo
pip install -r requirements.txt

# Optional: compile the Rust sandbox for sandboxed execution
cd sandbox_core && cargo build --release && cd ..
```

### Provider Setup

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your LLM provider API key:
#   LLM_PROVIDER=openai
#   API_KEY=sk-...
#   MODEL=gpt-5
#
# Or use NVIDIA:
#   LLM_PROVIDER=nvidia
#   NVIDIA_API_KEY=nvapi-...
#   MODEL=nvidia/nemotron-3-super-120b-a12b
```

### Launch

```bash
# Activate in the current folder (npm-installed or from source)
aelvo                 # opens the current directory
aelvo ./my-project    # opens any folder
aelvo "refactor the auth module"   # one-shot prompt in the current folder

# From source, these are equivalent:
python -m cli                         # opens the current directory
python -m cli ./my-project            # opens any folder
python -m cli -w ./my-project --provider openai --model gpt-5

# Full boot → CLI (runs the whole platform first)
python main.py

# Launch the web dashboard instead
python main.py --web
```

---

## Terminal CLI (`aelvo`, `python -m cli`)

A dedicated interactive terminal agent in the spirit of CodeBuff / Claude
Code, reusing the exact same backend as the web dashboard. `python -m cli`
boots a **lean** backend (kernel, filesystem, memory, orchestrator, provider
runtime) and skips the heavy optional subsystems the web boot runs (MCP
discovery, long-horizon planning, repo scans), so the prompt appears in a
couple of seconds. Type any natural language task — Enter submits,
`Esc+Enter` inserts a newline, and tool calls (read/write/bash/scrape/memory)
render live as the agent works.

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
| `/pwd` | Print the active folder |
| `/status` | Provider, model, folder + live agent metrics |
| `/provider [name] [key]` | Two-step interactive picker: choose a provider, then one of its models (or `/provider <name> [key]` directly); the API key is asked inline as part of selection — existing keys can be replaced/rotated right in the picker — and stored in the encrypted vault |
| `/model [name]` | Open an interactive picker to switch the active model (or `/model <name>` directly) |

There is no workspace registry or `/workspace` command — `aelvo` opens any
folder directly (the current directory by default, or `aelvo <folder>`), and
per-folder state lives in `.aelvo/` inside that folder. API keys are stored
in the universal AELVO space (`~/.aelvo`), never inside the opened folder.

**Local runtimes** (Ollama, LM Studio, vLLM, llama.cpp) are first-class
providers: pick them like any other — no API key needed. The model list is
fetched live from your local server (`localhost:11434`, `:1234`, `:8000`,
`:8080` respectively), with the curated catalog as fallback.
| `/log [lines]` | Tail the AELVO log file |
| `/version` | Show version and environment info |
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
$ cd ~/projects/my-app          # open any folder — no workspace setup needed
$ aelvo
AELVO
────────────────────────────────────────────────────────────────
  project: my-app   provider: nvidia   model: nvidia/nemotron-3-super
  folder: C:/Users/you/projects/my-app

type /help for commands · Esc+Enter for a newline · Ctrl+C to exit
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

AELVO never litters your project — all per-folder state lives in a hidden
`.aelvo/` directory inside the opened folder, so `git status` stays clean:

```text
$ ls -a ~/projects/my-app
.  ..  .aelvo  worker_pool.py

$ ls ~/projects/my-app/.aelvo
anchor.md    backups/    history    memory.db
```

---

## License

MIT — see LICENSE for details.
