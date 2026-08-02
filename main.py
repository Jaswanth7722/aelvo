#!/usr/bin/env python3
"""
main.py - AELVO System Entry Point
===================================
Wires the four pillars (Kernel, FileSystem, Scraper, Commands) to any
LLM API via a universal multi-provider adapter.

Supported Providers:
    NVIDIA, OpenAI, Anthropic, Groq, Together, Mistral, Google Gemini, OpenRouter, DeepSeek

Usage:
    1. Set your provider's API key in .env
    2. python main.py [optional_workspace_name]
    3. python main.py --config (to change provider)
"""

import os
import json
import logging
import asyncio
from core.rag import MemorySearcher
from datetime import timedelta
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    print("WARNING: python-dotenv not installed. Environment variables must be set manually.")
    print("Install with: pip install python-dotenv")

import time
import yaml
import sqlite3
import datetime
from typing import Optional

# --- AELVO Imports ---
from core.execution import AelvoKernel
from core.filesystem import AelvoFileSystem
from core.scraping import execute_heavy_crawl, execute_light_scrape
from core.governance import MemoryEngine
from core.provider_runtime import init_provider_runtime
from tools import build_extended_tool_registry
from core.orchestration import Orchestrator
from core.startup import select_project, detect_provider
from cognition import CognitiveEngine, CognitiveEngineConfig
from repo_intelligence import RepoIntelligenceEngine
from memory.forge_memory import ForgeMemory
from specialists import SPECIALIST_REGISTRY
from planning.integration import LongHorizonPlanningIntegration

# --- MCP Platform Imports ---
from mcp.registry.server_registry import ServerRegistry
from mcp.registry.trust_manager import TrustManager
from mcp.registry.health_tracker import HealthTracker
from mcp.client.connection_manager import ConnectionManager
from mcp.discovery.discovery_engine import DiscoveryEngine
from mcp.capability.capability_engine import CapabilityEngine
from mcp.governance.governance_layer import MCPGovernanceLayer
from mcp.verification.verification_pipeline import MCPVerificationPipeline
from mcp.recovery.recovery_engine import MCPRecoveryEngine
from mcp.memory import MCPMemoryStore
from mcp.events.event_publisher import MCPEventPublisher
from mcp.execution.execution_engine import MCPExecutionEngine
from mcp.execution.mcp_cli import MCPCommandLineInterface

# --- Runtime Monitoring CLI ---
from runtime_next.monitoring.cli import RuntimeCLI
from runtime_next.monitoring.dashboard import RuntimeDashboard

log = logging.getLogger(__name__)

# --- Global Metadata Paths ---
GLOBAL_DB_PATH = os.path.join(os.path.dirname(__file__), "global_memory.db")
GLOBAL_ANCHOR_PATH = os.path.join(os.path.dirname(__file__), "global_anchor.md")
WORKSPACE_BASE = os.path.join(os.path.dirname(__file__), "workspace")

# --- Default Fallbacks (will be updated by bootloader) ---
_ws_name = "default"
DB_PATH = os.path.join(WORKSPACE_BASE, _ws_name, "memory.db")
ANCHOR_PATH = os.path.join(WORKSPACE_BASE, _ws_name, "anchor.md")
WORKSPACE_PATH = os.path.join(WORKSPACE_BASE, _ws_name)
BACKUP_DIR = os.path.join(WORKSPACE_BASE, _ws_name, "backups")

def init_global_metadata():
    """Ensures the global database for tracking projects is ready."""
    try:
        with sqlite3.connect(GLOBAL_DB_PATH) as db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS projects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE,
                    description TEXT,
                    path TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_opened TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS user_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            # Scaffold global anchor if missing
            if not os.path.exists(GLOBAL_ANCHOR_PATH):
                with open(GLOBAL_ANCHOR_PATH, "w", encoding="utf-8") as f:
                    f.write("---\nmeta: AELVO Global Constraints\nversion: 1.0\n---\n# Global Rules\nAll projects inherit these root constraints.\n")
    except Exception as e:
        print(f"Global Init Error: {e}")

# ============================================================================
# SYSTEM PROMPT â€” Dynamically includes current date/time
# ============================================================================
def get_system_prompt(user_query=""):
    """Generate system prompt with live date, anchor constraints, and kernel state."""
    now = datetime.datetime.now()
    yesterday = now - timedelta(days=1)

    # --- KERNEL ANCHOR & STATE (The "Active" Consciousness) ---
    # We only inject LOCKED constraints and active state.
    state_info = "(empty)"
    anchor_info = "(none)"
    try:
        with sqlite3.connect(DB_PATH) as db:
            rows = db.execute("SELECT key, value FROM state ORDER BY key").fetchall()
        if rows:
            state_info = "\n".join([f"  {k}: {v}" for k, v in rows if not k.startswith("runtime:")])
    except Exception as _ex: log.warning("Silenced exception: %s", _ex)

    try:
        if os.path.exists(ANCHOR_PATH):
            with open(ANCHOR_PATH, 'r', encoding='utf-8') as f:
                raw = f.read()
                if raw.startswith('---'):
                    parts = raw.split('---', 2)
                    if len(parts) >= 3:
                        data = yaml.safe_load(parts[1])
                        if data and data.get("constraints"):
                            anchor_info = "\n".join([f"  {k}: {v.get('value')}" for k, v in data["constraints"].items()])
    except Exception as _ex: log.warning("Silenced exception: %s", _ex)

    # --- SECRETARY: Active Semantic Injection (DYNAMIC RAG ONLY) ---

    return """
You are AELVO, a deterministic AI agent operating inside a hardened execution environment on the user's local host machine (Windows OS).
NOTE: Do not confuse your local operating environment with the user's target project environments. While your tools are jailed to your local workspace, the USER is free to code, deploy, or move their ML projects to external platforms (e.g., Kaggle, AWS, cloud servers). You should fully assist them with code or logic meant for those platforms without claiming it's unsupported.
Your creator and authorized developer is defined in the anchor constraints below.
""" + f"""

**SYSTEM CONTEXT**:
- Execution Path: {os.path.abspath(os.path.dirname(__file__))}
- Workspace Jail: {os.path.abspath(WORKSPACE_PATH)}

**CURRENT DATE & TIME**: {now.strftime('%Y-%m-%d %H:%M')} (today)
**YESTERDAY**: {yesterday.strftime('%Y-%m-%d')}
**CURRENT YEAR**: {now.year}

IMPORTANT: Always use the current year ({now.year}) when constructing URLs or searching for recent events.

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
PERSISTENT ANCHOR (Hard Constraints)
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
ANCHOR CONSTRAINTS (from anchor.md):
{anchor_info}

KERNEL STATE:
{state_info}

You KNOW this information. Answer IMMEDIATELY from the above.**CRITICAL PROTOCOL**: Every tool-call must include a mandatory `"rationale"` field. 
Any action without a clear, one-sentence reasoning for *why* it is being taken will be REJECTED.

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
FORMAT 1: JSON TOOL CALL (REASONING MANDATED)
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
Output a JSON array for one or MORE related tool calls in one turn:
[
  {{
    "rationale": "<One sentence explaining WHY this step is necessary for the goal>",
    "tool": "<tool_name>", 
    "args": {{<arguments>}}
  }}
]

  search_memory â€” args: {{"query": "<keywords>"}} (Always search before guessing)
  save_constraint â€” args: {{"tag": "<tag>", "rule": "<fact>"}} (Reinforce critical project facts)
  read_file    â€” args: {{"path": "<relative_path>"}} (Read to understand file structure/symbols)
  read_file_range â€” args: {{"path": "<relative_path>", "start_line": 1, "end_line": 120}} (Bounded line read)
  write_file   â€” args: {{"path": "<path>", "content": "<text>"}} (Atomic write)
  edit_file    â€” args: {{"path": "<path>", "old_block": "<find>", "new_block": "<replace>"}} (Surgical edit)
  list_files   â€” args: {{"path": "<relative_dir>"}} (Map project structure)
  find_files   â€” args: {{"pattern": "*.py"}} (Find files by glob)
  search_code  â€” args: {{"query": "<literal text>"}} (Search source files)
  grep_file    â€” args: {{"path": "<file>", "pattern": "<regex>"}} (Search inside one file)
  project_tree â€” args: {{"max_depth": 2}} (Compact workspace tree)
  bash_exec    â€” args: {{"command": "<safe shell command>", "timeout": 30}} (Bounded shell execution)
  python_exec  â€” args: {{"script": "<path>"}} (Execute and analyze output)
  heavy_crawl  â€” args: {{"url": "<url>"}} (Deep research)
  light_scrape â€” args: {{"url": "<url>"}} (Fast info gathering)

TOOL RESPONSE CONTRACT: Every tool returns {{"status": "success"|"error", "logs": "...", "executed": {{...}}}}

**ITERATIVE DEBUGGING PROTOCOL**: 
If a tool returns an "error" status (especially `python_exec` or `edit_file`), you MUST NOT give up. 
Analyze the stack trace or logical violation, identify the exact root cause, and execute a correction in the next turn. 
Coding agents like you succeed through persistence and corrective reasoning.

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
FORMAT 2: # KERNEL COMMAND
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
Output a kernel command to manipulate system state:
  #lock <target> <value>          â€” Lock a constraint (e.g., #lock DEV_NAME Jaswanth)
  #update_anchor <target> <value> â€” Stage an anchor update
  #confirm                        â€” Apply staged update
  #checkpoint <snap_name>         â€” Save system snapshot
  #drop_state <state_key>         â€” Remove a state key

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
FORMAT 3: CONVERSATIONAL RESPONSE
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
If providing a final answer or conversational response:
{{"tool": "respond", "args": {{"message": "<your answer here>", "retain_memory": "<optional summary>"}}}}

RULES:
1. FORMAT: Always use JSON arrays for tool calls.
2. REASONING: The 'rationale' field is your Chain-of-Thought. Use it to prevent hallucinations.
3. JAILED: File paths are strictly relative to the workspace root.
4. WEB: Use web tools for any information beyond your training cutoff. Never guess dates/specs.
5. PERSISTENCE: If a task has multiple steps (read -> fix -> test), BATCH THEM into the JSON array for efficiency.
6. HONESTY: If a tool fails, report the failure and fix it. Do not hide errors.
7. For identity/state/context questions, answer from PERSISTENT MEMORY above.
8. If a task has multiple steps (read -> fix -> test), BATCH THEM into the JSON array for efficiency.
"""


class LLMCache:
    """SQLite-backed cache for LLM provider completions to optimize performance."""

    def __init__(self, db_path=None):
        import sqlite3
        if db_path is None:
            db_path = os.path.join(os.path.dirname(__file__), "llm_cache.db")
        self.db_path = db_path
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    key TEXT PRIMARY KEY,
                    response TEXT,
                    timestamp REAL
                )
            """)
            conn.commit()

    def get(self, key: str) -> Optional[str]:
        import sqlite3
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT response, timestamp FROM cache WHERE key = ?", (key,))
                row = cursor.fetchone()
                if row:
                    # Cache entries are valid for 2 hours
                    if time.time() - row[1] < 7200:
                        return row[0]
        except Exception as _ex:
            log.warning("Silenced exception: %s", _ex)
        return None

    def set(self, key: str, response: str):
        import sqlite3
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO cache (key, response, timestamp) VALUES (?, ?, ?)",
                    (key, response, time.time())
                )
                conn.commit()
        except Exception as _ex:
            log.warning("Silenced exception: %s", _ex)


# ============================================================================
# AelvoAgent — Universal LLM bridge (supports all providers)
# ============================================================================
class AelvoAgent:
    """
    Universal LLM adapter. Routes API calls to the correct SDK
    based on provider config. Uses auth.adapters for message
    normalization and tool call handling.

    Implements the interface expected by
    MemoryEngine.execute_turn(): get_next_action(context) and
    force_regenerate(feedback).
    """

    def __init__(self, api_key, model, provider_name, provider_config, provider_runtime=None):
        self.api_key = api_key
        self.model = model
        self._llm_cache = LLMCache()
        self.provider_name = provider_name
        self.config = provider_config
        self.sdk_type = getattr(provider_config, "sdk", None) if provider_config is not None else None
        self.provider_runtime = provider_runtime
        self.conversation_history = []
        self.last_context = None
        
        # PERSISTENT CLIENTS (Fix: Reuse connection to eliminate SSL/DNS lag)
        self.client = None
        if self.sdk_type == "openai":
            from openai import OpenAI
            self.client = OpenAI(api_key=self.api_key, base_url=getattr(self.config, 'base_url', None))
        elif self.sdk_type == "anthropic":
            from anthropic import Anthropic
            self.client = Anthropic(api_key=self.api_key)

    def _call_llm(self, messages):
        """Unified internal router for multi-provider support.

        Converts canonical (OpenAI-format) messages to the target
        provider's format using MessageAdapter before sending.
        """
        # SPEED OPTIMIZATION: Only generate/inject system prompt once per user interaction.
        current_hash = ""
        if hasattr(self, "last_context") and self.last_context and isinstance(self.last_context, dict):
            current_hash = self.last_context.get("anchor_hash", "")

        if not hasattr(self, "_cached_system_prompt") or \
           getattr(self, "_last_hash", "") != current_hash or \
           time.time() - getattr(self, "_cache_time", 0) > self.SYSTEM_PROMPT_CACHE_TTL:
            user_query = ""
            for m in reversed(messages):
                if m.get("role") == "user":
                    user_query = m.get("content", "")
                    break
            self._cached_system_prompt = get_system_prompt(user_query)
            self._last_hash = current_hash
            self._cache_time = time.time()

        system_prompt = self._cached_system_prompt

        # Hash inputs for caching lookup
        import hashlib
        import json
        cache_key = None
        try:
            cache_input = {
                "model": self.model,
                "system_prompt": system_prompt,
                "messages": messages
            }
            cache_key = hashlib.sha256(json.dumps(cache_input, sort_keys=True).encode("utf-8")).hexdigest()
            cached_resp = self._llm_cache.get(cache_key)
            if cached_resp is not None:
                log.info("LLM Cache Hit for model %s", self.model)
                return cached_resp
        except Exception as e:
            log.debug("LLM cache key generation failed: %s", e)

        # Normalize non-system messages for the target provider using MessageAdapter
        non_system_msgs = [m for m in messages if m.get("role") != "system"]
        if self.provider_runtime:
            provider_msgs = self.provider_runtime.normalize_messages(
                non_system_msgs, self.provider_name
            )
        else:
            provider_msgs = non_system_msgs

        response_text = ""
        if self.sdk_type == "openai":
            # OpenAI accepts system as a message
            all_msgs = [{"role": "system", "content": system_prompt}] + list(provider_msgs)
            response = self.client.chat.completions.create(
                model=self.model,
                messages=all_msgs,
                temperature=0.1
            )
            if response.choices:
                response_text = response.choices[0].message.content or ""
            else:
                log.error("OpenAI response contained no choices")

        elif self.sdk_type == "anthropic":
            # Anthtopic handles system separately
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2048,
                messages=provider_msgs,
                system=system_prompt,
                temperature=0.1
            )
            if response.content:
                response_text = response.content[0].text or ""
            else:
                log.error("Anthropic response contained no content blocks")

        elif self.sdk_type == "google":
            # provider_msgs already in Google format ({role, parts}) from MessageAdapter
            import google.generativeai as genai
            genai.configure(api_key=self.api_key)
            model = genai.GenerativeModel(self.model, system_instruction=system_prompt)
            response = model.generate_content(
                provider_msgs,
                generation_config=genai.types.GenerationConfig(temperature=0.1)
            )
            response_text = response.text

        else:
            raise ValueError(f"SDK '{self.sdk_type}' not implemented.")

        # Save to cache
        if cache_key and response_text:
            try:
                self._llm_cache.set(cache_key, response_text)
            except Exception as e:
                log.debug("Failed to write to LLM cache: %s", e)

        return response_text

    def _format_context_message(self, context):
        """Builds a technical system status injection for the LLM."""
        return f"""
[AELVO EXECUTOR â€” SYSTEM DATA]
LOCKED CONSTRAINTS: {json.dumps({k: v['value'] for k, v in context['constraints'].items() if v.get('locked')})}
CURRENT STATE: {json.dumps(context['state'])}
EPISODE HISTORY (last 10): {json.dumps(context['episodes'])}
"""

    def get_next_action(self, context: dict) -> str:
        """Called by MemoryEngine. Decide what to do."""
        self.last_context = context
        context_msg = self._format_context_message(context)

        if self.conversation_history:
            user_msg = f"{context_msg}\n\nBased on the above state and your previous results, decide your next action."
        else:
            user_msg = f"{context_msg}\n\nYou are now online. Analyze the system state and decide your first action."

        self.conversation_history.append({"role": "user", "content": user_msg})
        raw_output = self._call_llm(self.conversation_history)
        self.conversation_history.append({"role": "assistant", "content": raw_output})

        return self._extract_action(raw_output)

    def force_regenerate(self, feedback: str) -> str:
        """Called by MemoryEngine on constraint violations. Forces LLM to fix."""
        correction_msg = f"[AELVO EXECUTOR â€” VIOLATION DETECTED]\n{feedback}\n\nRegenerate your action NOW. Output ONLY valid JSON."
        self.conversation_history.append({"role": "user", "content": correction_msg})
        raw_output = self._call_llm(self.conversation_history)
        self.conversation_history.append({"role": "assistant", "content": raw_output})

        return self._extract_action(raw_output)

    SYSTEM_PROMPT_CACHE_TTL = 300  # Seconds before system prompt cache is invalidated
    MAX_CONVERSATION_HISTORY = 100  # Prevent unbounded growth

    def send_user_message(self, user_input: str) -> str:
        """Direct user message â†’ LLM. Returns raw action string."""
        self.conversation_history.append({"role": "user", "content": user_input})
        # Truncate oldest entries to prevent unbounded memory growth
        if len(self.conversation_history) > self.MAX_CONVERSATION_HISTORY * 2:
            self.conversation_history = self.conversation_history[-self.MAX_CONVERSATION_HISTORY:]
        raw_output = self._call_llm(self.conversation_history)
        self.conversation_history.append({"role": "assistant", "content": raw_output})

        return raw_output

    def feed_result(self, result: dict):
        """Feed tool execution result back into conversation history."""
        result_msg = f"[AELVO EXECUTOR â€” TOOL RESULT]\n```json\n{json.dumps(result, indent=2, default=str)}\n```"
        self.conversation_history.append({"role": "user", "content": result_msg})

    @staticmethod
    def _extract_action(raw_output: str) -> str:
        """Surgically extract tool logic from LLM output."""
        text = raw_output.strip()
        
        # 1. Direct Command
        if text.startswith("#"):
            return text
            
        # 2. Pattern Matching
        out_type, payload = parse_llm_output(text)
        if out_type == "tool_call":
            return json.dumps(payload)
            
        return text


# ============================================================================
# HELPER â€” Output Parsing & Routing
# ============================================================================
def parse_llm_output(text: str):
    """
    Parses LLM output into a typed payload.
    Supports single dict or a list of dicts (Batched Execution).
    Returns: (output_type, payload)
    types: "kernel_command", "tool_calls", "unknown"
    """
    text = text.strip()

    # 1. Check for # Kernel Command
    if text.startswith("#"):
        return ("kernel_command", [text])

    # 2. Check for JSON Tool Call(s) â€” Array or Object
    def normalize_calls(data):
        if isinstance(data, list): 
            return [x for x in data if isinstance(x, dict) and "tool" in x]
        if isinstance(data, dict) and "tool" in data:
            return [data]
        return None

    # First, try to find a code block
    try:
        if "```json" in text:
            block = text.split("```json")[1].split("```")[0].strip()
            parsed = json.loads(block, strict=False)
            norm = normalize_calls(parsed)
            if norm: return ("tool_calls", norm)
    except Exception as _ex: log.warning("Silenced exception: %s", _ex)

    # Try direct parse
    try:
        parsed = json.loads(text, strict=False)
        norm = normalize_calls(parsed)
        if norm: return ("tool_calls", norm)
    except Exception as _ex: log.warning("Silenced exception: %s", _ex)

    # Aggressive Search via JSONDecoder
    decoder = json.JSONDecoder(strict=False)
    for marker in ['[', '{']:
        start = text.find(marker)
        if start != -1:
            try:
                candidate = text[start:]
                parsed, index = decoder.raw_decode(candidate)
                norm = normalize_calls(parsed)
                if norm: return ("tool_calls", norm)
            except Exception as _ex:
                log.warning("Silenced exception: %s", _ex)
                continue

    return ("unknown", text)


# build_tool_registry â€” Maps tool names â†’ implementations
# ============================================================================
def build_tool_registry(fs: AelvoFileSystem, kernel: AelvoKernel, memory_engine: MemoryEngine):
    """Build the tool registry that MemoryEngine uses to dispatch tool calls.
    
    Each wrapper extracts ONLY the expected args â€” this makes the system
    resilient to LLMs sending extra kwargs like 'workspace', 'overwrite', etc.
    """
    def _wrap_read(path, **_ignored):
        result = fs.read_file(path)
        result.setdefault("logs", f"Read {path}")
        result.setdefault("executed", {})
        result["executed"]["path"] = path
        result["executed"]["workspace"] = f"./{_ws_name}"
        return result

    def _wrap_write(path, content, **_ignored):
        result = fs.write_atomic(path, content)
        result.setdefault("logs", f"Wrote {path}")
        result.setdefault("executed", {})
        result["executed"]["path"] = path
        result["executed"]["workspace"] = f"./{_ws_name}"
        return result

    def _wrap_edit(path, old_block, new_block, **_ignored):
        result = fs.edit_file_block(path, old_block, new_block)
        result.setdefault("logs", f"Edited {path}")
        result.setdefault("executed", {})
        result["executed"]["path"] = path
        result["executed"]["workspace"] = f"./{_ws_name}"
        return result

    def _wrap_heavy(url, **_ignored):
        return execute_heavy_crawl(url, kernel)

    def _wrap_light(url, **_ignored):
        return execute_light_scrape(url, kernel)

    def _wrap_respond(message="", retain_memory=None, **_ignored):
        if retain_memory:
            # PHASE 8: Conflict Resolution (Deduplication)
            with memory_engine.collection_guard() as coll:
                searcher = MemorySearcher(coll)
                if searcher.resolve_conflict(retain_memory, meta_type="fact"):
                    # Concept already exists; skip redundant insert to prevent bloat
                    return {"status": "success", "logs": f"Deduplicated: {message}", "executed": {"message": message, "memory_retained": False}}

            try:
                # PHASE 4 & 7: Atomic Dual-Sync + Adaptive Metadata
                from datetime import datetime
                import time
                import hashlib
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                m_id = hashlib.sha256(f"voluntary_{ts}_{(retain_memory or "")[:30]}".encode()).hexdigest()
                
                # 1. SQL System of Record
                with sqlite3.connect(DB_PATH) as db:
                    db.execute("INSERT INTO retained_memory (content) VALUES (?)", (retain_memory,))
                
                # 2. Vector Search Engine with Lifecycle Metadata
                with memory_engine.collection_guard() as coll:
                    coll.add(
                        ids=[m_id],
                        documents=[retain_memory],
                        metadatas=[{
                            "type": "voluntary",
                            "timestamp": ts,
                            "timestamp_unix": time.time(),
                            "importance": 0.6,    # Standard starting importance
                            "usage_count": 0,
                            "source": "respond"
                        }]
                    )
                log.info(f"âœ“ Voluntary memory atomized: {m_id}")
            except Exception as e:
                log.error(f"FATAL: Memory Desync on Respond: {e}")

        # PHASE 7: Feedback Loop (Reinforce used memories - run asynchronously to mitigate read-path writes)
        used_ids = getattr(memory_engine, "last_retrieved_ids", [])
        if used_ids:
            def _async_reinforce_memories(ids_to_update):
                for mid in ids_to_update:
                    try:
                        with memory_engine.collection_guard() as coll:
                            data = coll.get(ids=[mid], include=["metadatas"])
                            if not data or not data.get("metadatas"): continue
                            meta = data["metadatas"][0]
                            # Reward: Increase importance and usage count
                            meta["usage_count"] = int(meta.get("usage_count", 0)) + 1
                            meta["importance"] = min(1.0, float(meta.get("importance", 0.5)) + 0.05)
                            coll.update(ids=[mid], metadatas=[meta])
                    except Exception as _ex:
                        log.warning("Silenced exception: %s", _ex)
            
            try:
                memory_engine._executor.submit(_async_reinforce_memories, list(used_ids))
            except Exception as e:
                log.debug("Failed to submit async memory reinforcement: %s", e)
            # Reset feedback for next turn
            memory_engine.last_retrieved_ids = []

        return {"status": "success", "logs": message, "executed": {"message": message, "memory_retained": bool(retain_memory)}}

    def _wrap_hash(path, **_ignored):
        import hashlib
        safe_path = fs._validate_path(path)
        if not safe_path.is_file():
            return {"status": "error", "logs": f"File not found: {path}", "executed": {"path": path, "workspace": f"./{_ws_name}"}}
        with open(safe_path, 'rb') as f:
            sha256 = hashlib.sha256(f.read()).hexdigest()
        return {
            "status": "success",
            "logs": f"SHA-256 of {path}: {sha256}",
            "executed": {"path": path, "workspace": f"./{_ws_name}"},
            "data": {"hash": sha256, "algorithm": "sha256"}
        }

    def _wrap_list(path=".", **_ignored):
        safe_path = fs._validate_path(path)
        if not safe_path.is_dir():
            return {"status": "error", "logs": f"Not a directory: {path}", "executed": {"path": path, "workspace": f"./{_ws_name}"}}
        entries = []
        for item in sorted(safe_path.iterdir()):
            entries.append({"name": item.name, "type": "dir" if item.is_dir() else "file"})
        return {
            "status": "success",
            "logs": f"Listed {len(entries)} items in {path}",
            "executed": {"path": path, "workspace": f"./{_ws_name}"},
            "data": entries
        }

    def _wrap_python_exec(script, timeout=30, **_ignored):
        return fs.python_exec(script, timeout)

    def _wrap_bash_exec(command, timeout=30, **_ignored):
        return fs.bash_exec(command, timeout)

    def _wrap_read_range(path, start_line=1, end_line=120, **_ignored):
        return fs.read_file_range(path, int(start_line), int(end_line))

    def _wrap_grep_file(path, pattern, case_sensitive=False, max_matches=100, **_ignored):
        return fs.grep_file(path, pattern, bool(case_sensitive), int(max_matches))

    def _wrap_search_code(query, max_matches=100, **_ignored):
        return fs.search_code(query, int(max_matches))

    def _wrap_find_files(pattern="*", max_results=200, **_ignored):
        return fs.find_files(pattern, int(max_results))

    def _wrap_project_tree(max_depth=2, max_entries=300, **_ignored):
        return fs.project_tree(int(max_depth), int(max_entries))

    def _wrap_scaffold_website(project_dir=".", title="AELVO App", **_ignored):
        return fs.scaffold_website(project_dir, title)

    def _wrap_save_constraint(tag, rule, **_ignored):
        # PHASE 8: Conflict Resolution (Deduplication)
        content = f"{tag}: {rule}"
        with memory_engine.collection_guard() as coll:
            searcher = MemorySearcher(coll)
            if searcher.resolve_conflict(content, meta_type="fact"):
                return {"status": "success", "logs": f"Deduplicated constraint: {tag}", "executed": {"tag": tag}}

        try:
            # PHASE 4 & 7: Atomic Dual-Sync + Adaptive Metadata
            from datetime import datetime
            import time
            import hashlib
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            m_id = hashlib.sha256(f"semantic_{ts}_{tag}".encode()).hexdigest()

            # 1. SQL System of Record
            with sqlite3.connect(DB_PATH) as db:
                db.execute("INSERT INTO semantic_memory (tag, constraint_rule) VALUES (?, ?)", (tag, rule))
            
            # 2. Vector Search Engine with Lifecycle Metadata
            with memory_engine.collection_guard() as coll:
                coll.add(
                    ids=[m_id],
                    documents=[content],
                    metadatas=[{
                        "type": "semantic",
                        "tag": tag,
                        "timestamp": ts,
                        "timestamp_unix": time.time(),
                        "importance": 0.8,    # Constraints start with higher importance
                        "usage_count": 0
                    }]
                )
            log.info(f"âœ“ Semantic memory atomized: {m_id}")
            result = {"status": "success"}
        except Exception as e:
            log.error(f"FATAL: Memory Desync on Constraint: {e}")
            result = {"status": "error", "logs": str(e)}
            
        result.setdefault("logs", f"Saved constraint under tag: {tag}")
        result.setdefault("executed", {"tag": tag, "workspace": f"./{_ws_name}"})
        return result

    return {
        "read_file": {
            "fn": _wrap_read,
            "constraints_map": {},
            "required_constraints": []
        },
        "write_file": {
            "fn": _wrap_write,
            "constraints_map": {},
            "required_constraints": []
        },
        "edit_file": {
            "fn": _wrap_edit,
            "constraints_map": {},
            "required_constraints": []
        },
        "heavy_crawl": {
            "fn": _wrap_heavy,
            "constraints_map": {},
            "required_constraints": []
        },
        "light_scrape": {
            "fn": _wrap_light,
            "constraints_map": {},
            "required_constraints": []
        },
        "respond": {
            "fn": _wrap_respond,
            "constraints_map": {},
            "required_constraints": []
        },
        "hash_file": {
            "fn": _wrap_hash,
            "constraints_map": {},
            "required_constraints": []
        },
        "list_files": {
            "fn": _wrap_list,
            "constraints_map": {},
            "required_constraints": []
        },
        "save_constraint": {
            "fn": _wrap_save_constraint,
            "constraints_map": {},
            "required_constraints": []
        },
        "python_exec": {
            "fn": _wrap_python_exec,
            "constraints_map": {},
            "required_constraints": []
        },
        "bash_exec": {
            "fn": _wrap_bash_exec,
            "constraints_map": {},
            "required_constraints": []
        },
        "read_file_range": {
            "fn": _wrap_read_range,
            "constraints_map": {},
            "required_constraints": []
        },
        "grep_file": {
            "fn": _wrap_grep_file,
            "constraints_map": {},
            "required_constraints": []
        },
        "search_code": {
            "fn": _wrap_search_code,
            "constraints_map": {},
            "required_constraints": []
        },
        "find_files": {
            "fn": _wrap_find_files,
            "constraints_map": {},
            "required_constraints": []
        },
        "project_tree": {
            "fn": _wrap_project_tree,
            "constraints_map": {},
            "required_constraints": []
        },
        "scaffold_website": {
            "fn": _wrap_scaffold_website,
            "constraints_map": {},
            "required_constraints": []
        },
    }


# ============================================================================
# SESSION MEMORY â€” Condensed interaction records
# ============================================================================
class SessionTracker:
    """Tracks one user interaction: query â†’ tools â†’ answer. Saves to SQLite."""

    def __init__(self):
        self.user_query = ""
        self.tools_used = []     # ["light_scrape", "write_file"]
        self.files_touched = []  # ["intel/news.json", "intel/log.md"]
        self.final_answer = ""
        self.status = "success"

    def record_tool(self, tool_name: str, args: dict, outcome_status: str):
        self.tools_used.append(tool_name)
        # Extract file paths / URLs touched
        if "path" in args:
            path = args["path"]
            if path not in self.files_touched:
                self.files_touched.append(path)
        if "url" in args:
            url = args["url"][:80]  # Truncate long URLs
            if url not in self.files_touched:
                self.files_touched.append(url)
        if outcome_status == "error":
            self.status = "partial"

    def record_answer(self, answer: str):
        self.final_answer = answer[:500]  # Cap at 500 chars â€” no noise

    def save(self, db_path: str):
        """Persist condensed session to SQLite."""
        import sqlite3
        if not self.user_query:
            return
        try:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with sqlite3.connect(db_path) as db:
                # Ensure sessions table exists (defensive programming)
                db.execute("""
                    CREATE TABLE IF NOT EXISTS sessions (
                        session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        user_query TEXT,
                        tools_used TEXT,
                        files_touched TEXT,
                        final_answer TEXT,
                        status TEXT DEFAULT 'success'
                    )
                """)
                db.execute(
                    "INSERT INTO sessions (timestamp, user_query, tools_used, files_touched, final_answer, status) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        timestamp,
                        self.user_query[:200],        # Cap query
                        ", ".join(self.tools_used) if self.tools_used else "respond",
                        ", ".join(self.files_touched) if self.files_touched else "",
                        self.final_answer,
                        self.status,
                    )
                )
        except Exception as e:
            log.debug(f"Session save failed: {e}")


# ============================================================================
# MAIN LOOP
# ============================================================================
async def main_async():
    # ---- Interactive Project Selection ----
    global _ws_name, DB_PATH, ANCHOR_PATH, WORKSPACE_PATH, BACKUP_DIR
    _ws_name = select_project(os.environ.get("AELVO_PROJECT", "").strip() or None)

    # ---- Map Paths based on Selection ----
    WORKSPACE_PATH = os.path.join(WORKSPACE_BASE, _ws_name)
    DB_PATH = os.path.join(WORKSPACE_PATH, "memory.db")
    ANCHOR_PATH = os.path.join(WORKSPACE_PATH, "anchor.md")
    BACKUP_DIR = os.path.join(WORKSPACE_PATH, "backups")

    # Ensure project folder exists
    os.makedirs(WORKSPACE_PATH, exist_ok=True)

    # ---- Detect Provider & API Key ----
    try:
        from core.registry import MODEL_REGISTRY as OLD_MODEL_REGISTRY
        provider_name, provider_config, api_key, model = detect_provider(OLD_MODEL_REGISTRY)
    except Exception:
        provider_name, provider_config, api_key, model = None, None, None, None

    # ---- Initialize Core Components ----
    log.info(f"Booting AELVO... [Project: {_ws_name}]")
    if provider_name:
        log.info(f"Provider: {provider_name} | SDK: {provider_config.sdk} | Model: {model}")
    
    # ... (scaffolding omitted for brevity, will keep it in full)
    
    # 1. AelvoKernel (commands.py) â€” the #command router & state manager
    aelvo_kernel = AelvoKernel(
        db_path=DB_PATH,
        anchor_path=ANCHOR_PATH,
        backup_dir=BACKUP_DIR
    )
    log.info("âœ“ AelvoKernel initialized (commands, audit trail, state)")

    # 2. AelvoFileSystem (automation.py) â€” jailed, locked file operations
    os.makedirs(WORKSPACE_PATH, exist_ok=True)
    fs = AelvoFileSystem(
        base_path=WORKSPACE_PATH,
        kernel=aelvo_kernel
    )
    log.info(f"âœ“ AelvoFileSystem jailed to: {WORKSPACE_PATH}")

    # 3. MemoryEngine (kernel.py) â€” THE HYBRID ENGINE (SQLite + Vector)
    memory_engine = MemoryEngine(
        db_path=DB_PATH,
        anchor_path=ANCHOR_PATH,
        tool_registry={}, # Will populate in step 4
        project_name=_ws_name
    )
    log.info("âœ“ MemoryEngine initialized (hybrid: SQLite + Vector)")

    # 4. Tool Registry â€” maps tool names to implementations
    tool_registry = build_tool_registry(fs, aelvo_kernel, memory_engine)
    
    # 4.1 Vector RAG Integration â€” mathematical concept similarity engine
    searcher = MemorySearcher(chroma_collection=memory_engine.memory_collection)
    tool_registry["search_memory"] = {
        "fn": searcher.search,
        "required_constraints": [],
        "constraints_map": {}
    }
    
    # Merge newly-added tools
    extended_tools = build_extended_tool_registry(fs, aelvo_kernel, memory_engine)
    tool_registry.update(extended_tools)
    
    # Inject tool_registry into memory_engine
    memory_engine.tools = tool_registry

    # 4.5 Repository Intelligence Engine â€” codebase understanding, query, impact analysis
    repo_intel = None
    try:
        repo_intel = RepoIntelligenceEngine(workspace_root=WORKSPACE_PATH)
        await repo_intel.initialize(full_scan=False)
        log.debug("âœ“ RepoIntelligenceEngine initialized for %s", WORKSPACE_PATH)
    except Exception as e:
        log.warning(f"RepoIntelligenceEngine init skipped: {e}")


    # 4.6 Cognitive Engine â€” autonomous planning, research, consensus, coordination
    try:
        forge_memory = ForgeMemory(memory_engine, _ws_name)
        cognitive_engine = CognitiveEngine(
            config=CognitiveEngineConfig(),
            repo_intelligence=repo_intel,
            forge_memory=forge_memory,
            governance_kernel=memory_engine,
            specialist_registry=SPECIALIST_REGISTRY,
        )
        log.info("âœ“ CognitiveEngine initialized (planning, research, consensus)")
    except Exception as e:
        log.warning(f"CognitiveEngine init skipped: {e}")
        cognitive_engine = None

    # 5. Initialize Provider Runtime (full auth provider ecosystem)
    provider_runtime = None
    try:
        provider_runtime = await init_provider_runtime()
        # Store the detected API key in the credential store
        if provider_runtime and api_key and provider_name:
            try:
                from auth.types import Credential, CredentialType
                import uuid
                import time
                cred = Credential(
                    id=f"key_{provider_name}_{uuid.uuid4().hex[:8]}",
                    provider=provider_name,
                    credential_type=CredentialType.API_KEY,
                    value=api_key,
                    label=f"{provider_name} API key (from .env/wizard)",
                    created_at=time.time(),
                    is_valid=True,
                    metadata={"model": model or "", "source": "env_or_wizard"},
                )
                provider_runtime.credential_store.store(cred)
                log.debug(f"âœ“ Credential stored for {provider_name}")
            except Exception as e:
                log.warning(f"Failed to store credential for {provider_name}: {e}")
        log.debug(f"âœ“ Provider runtime initialized ({len(provider_runtime.provider_configs)} providers, {len(provider_runtime.model_registry.list_models())} models)")
    except Exception as e:
        log.warning(f"Provider runtime init skipped: {e}")

    # 6. AelvoAgent â€” connection context to the LLM (uses provider_runtime for adapters)
    # Only build the agent when a provider is configured; otherwise leave it
    # None so the web UI surfaces the missing-key error instead of crashing.
    if provider_config is not None:
        agent = AelvoAgent(
            api_key=api_key,
            model=model,
            provider_name=provider_name,
            provider_config=provider_config,
            provider_runtime=provider_runtime,
        )
        log.info(f"âœ“ Using provider: {provider_name} | Model: {model}")
    else:
        agent = None
        log.warning(
            "No LLM provider configured — booting with agent=None. "
            "Configure a provider in .env and restart."
        )

    # 7. Orchestrator Coordinator
    orchestrator = Orchestrator(
        memory_engine=memory_engine,
        kernel=aelvo_kernel,
        base_path=WORKSPACE_PATH,
        provider_runtime=provider_runtime
    )
    log.debug("âœ“ Orchestrator coordinate systems online")
    orchestrator.cognitive_engine = cognitive_engine

    # Wire runtime EventBus into CognitiveBlackboard so every publish emits a BlackboardPublicationEvent
    if cognitive_engine is not None and hasattr(cognitive_engine, 'blackboard'):
        cognitive_engine.blackboard.set_event_bus(orchestrator.runtime_bus)
        log.debug("âœ“ Runtime EventBus wired to CognitiveBlackboard for publication events")

    # 8. Long-Horizon Planning Integration (attaches at three seam points)
    lhp = None
    try:
        lhp = LongHorizonPlanningIntegration(
            memory_engine=memory_engine,
            orchestrator=orchestrator,
            workspace_path=WORKSPACE_PATH,
            project=_ws_name,
        )
        continuity_ctx = await lhp.start()
        if continuity_ctx:
            log.info(
                "âœ“ LHP session restored: %s",
                continuity_ctx.get("continuity", {}).get("resume_msg", ""),
            )
        else:
            log.info("âœ“ LHP initialized (first session or no prior boundary)")
    except Exception as exc:
        log.warning("Long-Horizon Planning init skipped: %s", exc)
        lhp = None

    # 9. MCP Subsystem Integration
    log.info("Booting MCP Platform Subsystem...")
    mcp_cli = None
    try:
        mcp_registry = ServerRegistry()
        await mcp_registry.load()
        mcp_trust_manager = TrustManager()
        mcp_health_tracker = HealthTracker(mcp_registry)
        mcp_event_publisher = MCPEventPublisher()
        mcp_connection_manager = ConnectionManager(event_publisher=mcp_event_publisher)
        mcp_discovery_engine = DiscoveryEngine(mcp_registry, event_publisher=mcp_event_publisher)
        mcp_capability_engine = CapabilityEngine(mcp_registry)
        mcp_governance = MCPGovernanceLayer(mcp_registry)
        mcp_verification = MCPVerificationPipeline(event_publisher=mcp_event_publisher)
        mcp_memory = MCPMemoryStore()
        mcp_recovery = MCPRecoveryEngine(
            registry=mcp_registry,
            connection_manager=mcp_connection_manager,
            capability_engine=mcp_capability_engine,
            health_tracker=mcp_health_tracker,
            trust_manager=mcp_trust_manager,
            event_publisher=mcp_event_publisher
        )
        mcp_execution = MCPExecutionEngine(
            registry=mcp_registry,
            connection_manager=mcp_connection_manager,
            capability_engine=mcp_capability_engine,
            governance=mcp_governance,
            verification=mcp_verification,
            recovery=mcp_recovery,
            memory=mcp_memory,
            event_publisher=mcp_event_publisher,
            health_tracker=mcp_health_tracker
        )
        mcp_platform = {
            "registry": mcp_registry,
            "connection_manager": mcp_connection_manager,
            "discovery_engine": mcp_discovery_engine,
            "capability_engine": mcp_capability_engine,
            "execution_engine": mcp_execution,
            "governance": mcp_governance,
            "verification": mcp_verification,
            "recovery": mcp_recovery,
            "memory": mcp_memory,
            "event_publisher": mcp_event_publisher,
            "health_tracker": mcp_health_tracker,
        }
        mcp_cli = MCPCommandLineInterface(mcp_platform)
        await mcp_discovery_engine.discover_all()
        log.info("âœ“ MCP Platform Subsystem initialized and scanned")
    except Exception as mcp_err:
        log.warning("MCP Subsystem failed to initialize: %s", mcp_err)

    # 9.5 Runtime Monitoring CLI — #status commands
    runtime_cli = None
    try:
        runtime_cli = RuntimeCLI(dashboard=RuntimeDashboard())
        log.info("RuntimeCLI initialized for #status commands")
    except Exception as status_err:
        log.warning("RuntimeCLI init skipped: %s", status_err)

    # ------------------------------------------------------------------------
    # BOOT LOGO
    # ------------------------------------------------------------------------
    log.info(
        "AELVO OMEGA ONLINE — provider=%s model=%s project=%s workspace=%s",
        provider_name, model, _ws_name, WORKSPACE_PATH,
    )

    # ------------------------------------------------------------------------
    # ------------------------------------------------------------------------
    # WEB MODE (default) — serve the dashboard + WebSocket bridge
    # ------------------------------------------------------------------------
    _web_host = os.environ.get("AELVO_HOST", "127.0.0.1")
    _web_http_port = int(os.environ.get("AELVO_HTTP_PORT", "8000"))
    _web_ws_port = int(os.environ.get("AELVO_WS_PORT", "8765"))
    _no_browser = os.environ.get("AELVO_NO_BROWSER", "").strip() in ("1", "true", "yes")

    log.info("Launching web dashboard...")
    from web.server import run_web
    await run_web(
        agent=agent,
        orchestrator=orchestrator,
        memory_engine=memory_engine,
        aelvo_kernel=aelvo_kernel,
        db_path=DB_PATH,
        host=_web_host,
        http_port=_web_http_port,
        ws_port=_web_ws_port,
        open_browser=not _no_browser,
        mcp_cli=mcp_cli,
        runtime_cli=runtime_cli,
        provider_runtime=provider_runtime,
    )
    log.info("Web session ended.")
    if lhp:
        try:
            await lhp.shutdown()
        except Exception as _ex:
            log.warning("Silenced exception: %s", _ex)
    aelvo_kernel.conn.close()
    memory_engine.db.close()
    return
def main():
    # ── Argument Parsing ──
    import argparse
    parser = argparse.ArgumentParser(
        prog="AELVO",
        description="AELVO is a web-based AI agent that plans and executes complex software engineering tasks using seven specialized sub-agents.",
        epilog="Example: python main.py --provider openai --model gpt-4"
    )
    parser.add_argument("--provider", type=str, default=None, help="Select LLM provider (e.g., openai, anthropic, groq)")
    parser.add_argument("--model", type=str, default=None, help="Override model selection (e.g., gpt-4, claude-3-opus)")
    parser.add_argument("--project", type=str, default=None, help="Select workspace (default: most recently used)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Web dashboard bind host")
    parser.add_argument("--port", type=int, default=8000, help="Web dashboard HTTP port")
    parser.add_argument("--ws-port", type=int, default=8765, help="WebSocket bridge port")
    parser.add_argument("--no-browser", action="store_true", help="Do not auto-open the browser")
    args, _ = parser.parse_known_args()
    # Store parsed args in env for main_async to pick up
    if args.provider:
        os.environ["LLM_PROVIDER"] = args.provider
        os.environ["AELVO_PROVIDER"] = args.provider
    if args.model:
        os.environ["LLM_MODEL"] = args.model
        os.environ["AELVO_MODEL"] = args.model
    if args.project:
        os.environ["AELVO_PROJECT"] = args.project
    if args.host:
        os.environ["AELVO_HOST"] = args.host
    if args.port:
        os.environ["AELVO_HTTP_PORT"] = str(args.port)
    if args.ws_port:
        os.environ["AELVO_WS_PORT"] = str(args.ws_port)
    if args.no_browser:
        os.environ["AELVO_NO_BROWSER"] = "1"

    try:
        asyncio.run(main_async())
    except KeyboardInterrupt as _ex:
        log.warning("Silenced exception: %s", _ex)

if __name__ == "__main__":
    main()
