#!/usr/bin/env python3
"""
main.py — AELVO System Entry Point
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
import sys
import json
import logging
from rag import MemorySearcher
try:
    import traceback
    from datetime import timedelta
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

import yaml
import sqlite3
import datetime
from datetime import timedelta

# --- AELVO Imports ---
from commands import AelvoKernel
from automation import AelvoFileSystem
from web_scraping import execute_heavy_crawl, execute_light_scrape
from kernel import MemoryEngine
from models import MODEL_REGISTRY

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
        db = sqlite3.connect(GLOBAL_DB_PATH)
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
        db.commit()
        db.close()
    except Exception as e:
        print(f"Global Init Error: {e}")

def select_project_interactive():
    """Interactive boot menu for project selection."""
    init_global_metadata()
    print("\n" + "=" * 60)
    print("  AELVO PROJECT MANAGER")
    print("=" * 60)
    
    db = sqlite3.connect(GLOBAL_DB_PATH)
    projects = db.execute("SELECT name, description, last_opened FROM projects ORDER BY last_opened DESC").fetchall()
    
    if projects:
        print("  RECENT PROJECTS:")
        for i, (name, desc, last) in enumerate(projects):
            print(f"    [{i+1}] {name.ljust(15)} | {last} | {desc or 'No info'}")
        print(f"    [N] Create New Project")
        print(f"    [D] Delete Project")
    else:
        print("    No existing projects found.")
        print(f"    [N] Create Your First Project")
    print("  [X] Exit")
    print("=" * 60)
    
    choice = input("Select an option: ").strip().upper()
    
    if choice == "N":
        name = input("Enter new project name: ").strip()
        if not name: return select_project_interactive()
        desc = input("Enter project description: ").strip()
        try:
            db.execute("INSERT INTO projects (name, description, path) VALUES (?, ?, ?)", 
                       (name, desc, os.path.join(WORKSPACE_BASE, name)))
            db.commit()
            target_name = name
        except sqlite3.IntegrityError:
            print(f"Project '{name}' already exists.")
            return select_project_interactive()
    elif choice == "D" and projects:
        del_choice = input("Enter project number to delete: ").strip()
        if del_choice.isdigit() and 1 <= int(del_choice) <= len(projects):
            del_name = projects[int(del_choice)-1][0]
            confirm = input(f"WARNING: This will permanently delete '{del_name}' and all its files. Type 'yes' to confirm: ").strip().lower()
            if confirm == "yes":
                import shutil
                # Remove from database registry
                db.execute("DELETE FROM projects WHERE name = ?", (del_name,))
                db.commit()
                # Purge from physical disk
                try:
                    shutil.rmtree(os.path.join(WORKSPACE_BASE, del_name), ignore_errors=True)
                    print(f"Project '{del_name}' completely wiped.")
                except Exception as e:
                    print(f"Error removing folder: {e}")
            else:
                print("Deletion cancelled.")
        return select_project_interactive()
    elif choice.isdigit() and 1 <= int(choice) <= len(projects):
        target_name = projects[int(choice)-1][0]
        db.execute("UPDATE projects SET last_opened = CURRENT_TIMESTAMP WHERE name = ?", (target_name,))
        db.commit()
    elif choice == "X":
        print("Exiting...")
        sys.exit(0)
    else:
        return select_project_interactive()
    
    db.close()
    return target_name

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [AELVO/%(levelname)s] - %(message)s"
)
log = logging.getLogger("aelvo")

# Suppress noisy third-party logs
for noisy in ["scrapy", "twisted", "playwright", "urllib3", "httpx", "httpcore",
              "filelock", "asyncio", "scrapy.core", "scrapy.utils", "scrapy.crawler",
              "scrapy.middleware", "scrapy.extensions", "scrapy.core.scraper",
              "scrapy.core.engine", "scrapy.downloadermiddlewares", "openai._base_client"]:
    logging.getLogger(noisy).setLevel(logging.CRITICAL)


# MODEL_REGISTRY is now loaded from models.py


def interactive_provider_setup():
    print("\n" + "=" * 60)
    print("  AELVO API CONFIGURATION SETUP")
    print("=" * 60)
    providers = list(MODEL_REGISTRY.keys())
    for i, name in enumerate(providers):
        print(f"  [{i+1}] {name.upper()}")
    print("  [0] Exit")
    print("=" * 60)
    choice = input("Select a provider to configure: ").strip()
    if choice.isdigit() and 1 <= int(choice) <= len(providers):
        p_name = providers[int(choice)-1]
        cfg = MODEL_REGISTRY[p_name]
        api_key = input(f"Enter API Key for {p_name.upper()} ({cfg.env_key}): ").strip()
        model_name = input(f"Enter Model Name (leave blank for '{cfg.default_model}'): ").strip()
        
        env_path = os.path.join(os.path.dirname(__file__), ".env")
        try:
            lines = []
            if os.path.exists(env_path):
                with open(env_path, 'r') as f:
                    lines = f.readlines()
            
            # Remove old keys out of .env to rewrite them cleanly
            lines = [l for l in lines if not l.startswith(cfg.env_key + "=") and not l.startswith("LLM_PROVIDER=") and not l.startswith("LLM_MODEL=")]
            
            lines.append(f"{cfg.env_key}={api_key}\n")
            lines.append(f"LLM_PROVIDER={p_name}\n")
            if model_name:
                lines.append(f"LLM_MODEL={model_name}\n")
            
            with open(env_path, 'w') as f:
                f.writelines(lines)
            
            print("\n  [✓] Configuration successfully saved to .env")
            os.environ[cfg.env_key] = api_key
            os.environ["LLM_PROVIDER"] = p_name
            if model_name:
                os.environ["LLM_MODEL"] = model_name
            elif "LLM_MODEL" in os.environ:
                del os.environ["LLM_MODEL"]
                
            return p_name, cfg, api_key, model_name or cfg.default_model
        except Exception as e:
            print(f"Failed to save .env: {e}")
            sys.exit(1)
    else:
        print("Setup aborted.")
        sys.exit(0)

def detect_provider():
    """
    Auto-detect provider from environment variables or trigger interactive setup.
    Pass '--config' when running python main.py to force interactive setup.
    """
    if '--config' in sys.argv:
        res = interactive_provider_setup()
        if res: return res

    explicit = os.environ.get("LLM_PROVIDER", "").strip().lower()
    model_override = os.environ.get("LLM_MODEL", "").strip()

    if explicit:
        if explicit not in MODEL_REGISTRY:
            print(f"Unknown LLM_PROVIDER='{explicit}'.")
            return interactive_provider_setup()
        cfg = MODEL_REGISTRY[explicit]
        key = os.environ.get(cfg.env_key, "")
        if not key:
            print(f"LLM_PROVIDER='{explicit}' but {cfg.env_key} is missing.")
            return interactive_provider_setup()
        model = model_override or cfg.default_model
        return explicit, cfg, key, model

    # Auto-detect: scan for the first available API key
    for name, cfg in MODEL_REGISTRY.items():
        key = os.environ.get(cfg.env_key, "")
        if key and key not in ("your-api-key-here", "your-anthropic-api-key-here", ""):
            model = model_override or cfg.default_model
            log.info(f"Auto-detected provider: {name} (found {cfg.env_key})")
            return name, cfg, key, model

    # Nothing found -> Interactive Prompt
    print("\n  [!] No API key found. Launching initial setup...")
    return interactive_provider_setup()

# ============================================================================
# SYSTEM PROMPT — Dynamically includes current date/time
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
        db = sqlite3.connect(DB_PATH)
        rows = db.execute("SELECT key, value FROM state ORDER BY key").fetchall()
        if rows:
            state_info = "\n".join([f"  {k}: {v}" for k, v in rows if not k.startswith("runtime:")])
        db.close()
    except: pass

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
    except: pass

    # --- SECRETARY: Active Semantic Injection (DYNAMIC RAG ONLY) ---

    return f"""
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

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PERSISTENT ANCHOR (Hard Constraints)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ANCHOR CONSTRAINTS (from anchor.md):
{anchor_info}

KERNEL STATE:
{state_info}

You KNOW this information. Answer IMMEDIATELY from the above.**CRITICAL PROTOCOL**: Every tool-call must include a mandatory `"rationale"` field. 
Any action without a clear, one-sentence reasoning for *why* it is being taken will be REJECTED.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FORMAT 1: JSON TOOL CALL (REASONING MANDATED)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Output a JSON array for one or MORE related tool calls in one turn:
[
  {{
    "rationale": "<One sentence explaining WHY this step is necessary for the goal>",
    "tool": "<tool_name>", 
    "args": {{<arguments>}}
  }}
]

  search_memory — args: {{"query": "<keywords>"}} (Always search before guessing)
  save_constraint — args: {{"tag": "<tag>", "rule": "<fact>"}} (Reinforce critical project facts)
  read_file    — args: {{"path": "<relative_path>"}} (Read to understand file structure/symbols)
  write_file   — args: {{"path": "<path>", "content": "<text>"}} (Atomic write)
  edit_file    — args: {{"path": "<path>", "old_block": "<find>", "new_block": "<replace>"}} (Surgical edit)
  list_files   — args: {{"path": "<relative_dir>"}} (Map project structure)
  python_exec  — args: {{"script": "<path>"}} (Execute and analyze output)
  heavy_crawl  — args: {{"url": "<url>"}} (Deep research)
  light_scrape — args: {{"url": "<url>"}} (Fast info gathering)

TOOL RESPONSE CONTRACT: Every tool returns {{"status": "success"|"error", "logs": "...", "executed": {{...}}}}

**ITERATIVE DEBUGGING PROTOCOL**: 
If a tool returns an "error" status (especially `python_exec` or `edit_file`), you MUST NOT give up. 
Analyze the stack trace or logical violation, identify the exact root cause, and execute a correction in the next turn. 
Coding agents like you succeed through persistence and corrective reasoning.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FORMAT 2: # KERNEL COMMAND
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Output a kernel command to manipulate system state:
  #lock <target> <value>          — Lock a constraint (e.g., #lock DEV_NAME Jaswanth)
  #update_anchor <target> <value> — Stage an anchor update
  #confirm                        — Apply staged update
  #checkpoint <snap_name>         — Save system snapshot
  #drop_state <state_key>         — Remove a state key

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FORMAT 3: CONVERSATIONAL RESPONSE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
If providing a final answer or conversational response:
{"tool": "respond", "args": {"message": "<your answer here>", "retain_memory": "<optional summary>"}}

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


# ============================================================================
# AelvoAgent — Universal LLM bridge (supports all providers)
# ============================================================================
class AelvoAgent:
    """
    Universal LLM adapter. Routes API calls to the correct SDK
    based on provider config. Implements the interface expected by
    MemoryEngine.execute_turn(): get_next_action(context) and
    force_regenerate(feedback).
    """

    def __init__(self, api_key, model, provider_name, provider_config):
        self.api_key = api_key
        self.model = model
        self.provider_name = provider_name
        self.config = provider_config
        self.sdk_type = provider_config.sdk
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
        """Unified internal router for multi-provider support."""
        # SPEED OPTIMIZATION: Only generate/inject system prompt once per user interaction.
        # This prevents redundant DB queries and massive token re-transmissions during loops.
        # FIX: Force refresh if anchor hash or state count has changed significantly (Signal Extraction)
        current_hash = ""
        if hasattr(self, "last_context") and self.last_context and isinstance(self.last_context, dict):
            current_hash = self.last_context.get("anchor_hash", "")

        if not hasattr(self, "_cached_system_prompt") or getattr(self, "_last_hash", "") != current_hash:
            user_query = ""
            for m in reversed(messages):
                if m.get("role") == "user":
                    user_query = m.get("content", "")
                    break
            self._cached_system_prompt = get_system_prompt(user_query)
            self._last_hash = current_hash
            # Ensure system prompt is always injected first
            messages = [m for m in messages if m.get("role") != "system"]
            messages.insert(0, {"role": "system", "content": self._cached_system_prompt})

        system_prompt = self._cached_system_prompt

        # --- NVIDIA / OPENAI / GROQ / TOGETHER / MISTRAL / OPENROUTER (OpenAI SDK) ---
        if self.sdk_type == "openai":
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0.1
            )
            return response.choices[0].message.content

        # --- ANTHROPIC (Native SDK) ---
        elif self.sdk_type == "anthropic":
            # Anthropic handles system separately, so we remove it from messages list
            chat_msgs = [m for m in messages if m["role"] != "system"]
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2048,
                messages=chat_msgs,
                system=system_prompt,
                temperature=0.1
            )
            return response.content[0].text

        # --- GOOGLE GEMINI (Native SDK) ---
        elif self.sdk_type == "google":
            import google.generativeai as genai
            genai.configure(api_key=self.api_key)
            model = genai.GenerativeModel(self.model, system_instruction=system_prompt)
            # Convert OpenAI format to Gemini format (user/model roles, remove system)
            contents = []
            for m in messages:
                if m["role"] == "system": continue
                role = "user" if m["role"] == "user" else "model"
                contents.append({"role": role, "parts": [m["content"]]})
            response = model.generate_content(
                contents,
                generation_config=genai.types.GenerationConfig(temperature=0.1)
            )
            return response.text

        else:
            raise ValueError(f"SDK '{self.sdk_type}' not implemented.")

    def _format_context_message(self, context):
        """Builds a technical system status injection for the LLM."""
        return f"""
[AELVO EXECUTOR — SYSTEM DATA]
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
        correction_msg = f"[AELVO EXECUTOR — VIOLATION DETECTED]\n{feedback}\n\nRegenerate your action NOW. Output ONLY valid JSON."
        self.conversation_history.append({"role": "user", "content": correction_msg})
        raw_output = self._call_llm(self.conversation_history)
        self.conversation_history.append({"role": "assistant", "content": raw_output})

        return self._extract_action(raw_output)

    def send_user_message(self, user_input: str) -> str:
        """Direct user message → LLM. Returns raw action string."""
        self.conversation_history.append({"role": "user", "content": user_input})
        raw_output = self._call_llm(self.conversation_history)
        self.conversation_history.append({"role": "assistant", "content": raw_output})

        return raw_output

    def feed_result(self, result: dict):
        """Feed tool execution result back into conversation history."""
        result_msg = f"[AELVO EXECUTOR — TOOL RESULT]\n```json\n{json.dumps(result, indent=2, default=str)}\n```"
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
# HELPER — Output Parsing & Routing
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

    # 2. Check for JSON Tool Call(s) — Array or Object
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
    except Exception: pass

    # Try direct parse
    try:
        parsed = json.loads(text, strict=False)
        norm = normalize_calls(parsed)
        if norm: return ("tool_calls", norm)
    except Exception: pass

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
            except Exception: continue

    return ("unknown", text)


# build_tool_registry — Maps tool names → implementations
# ============================================================================
def build_tool_registry(fs: AelvoFileSystem, kernel: AelvoKernel, memory_engine: MemoryEngine):
    """Build the tool registry that MemoryEngine uses to dispatch tool calls.
    
    Each wrapper extracts ONLY the expected args — this makes the system
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
            searcher = MemorySearcher(memory_engine.memory_collection)
            if searcher.resolve_conflict(retain_memory, meta_type="fact"):
                # Concept already exists; skip redundant insert to prevent bloat
                return {"status": "success", "logs": f"Deduplicated: {message}", "executed": {"message": message, "memory_retained": False}}

            try:
                # PHASE 4 & 7: Atomic Dual-Sync + Adaptive Metadata
                from datetime import datetime
                import time
                import hashlib
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                m_id = hashlib.md5(f"voluntary_{ts}_{retain_memory[:30]}".encode()).hexdigest()
                
                # 1. SQL System of Record
                db = sqlite3.connect(DB_PATH)
                db.execute("INSERT INTO retained_memory (content) VALUES (?)", (retain_memory,))
                db.commit()
                db.close()
                
                # 2. Vector Search Engine with Lifecycle Metadata
                memory_engine.memory_collection.add(
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
                log.info(f"✓ Voluntary memory atomized: {m_id}")
            except Exception as e:
                log.error(f"FATAL: Memory Desync on Respond: {e}")

        # PHASE 7: Feedback Loop (Reinforce used memories)
        used_ids = getattr(memory_engine, "last_retrieved_ids", [])
        if used_ids:
            for mid in used_ids:
                try:
                    data = memory_engine.memory_collection.get(ids=[mid], include=["metadatas"])
                    if not data["metadatas"]: continue
                    meta = data["metadatas"][0]
                    # Reward: Increase importance and usage count
                    meta["usage_count"] = int(meta.get("usage_count", 0)) + 1
                    meta["importance"] = min(1.0, float(meta.get("importance", 0.5)) + 0.05)
                    memory_engine.memory_collection.update(ids=[mid], metadatas=[meta])
                except Exception: pass
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

    def _wrap_save_constraint(tag, rule, **_ignored):
        # PHASE 8: Conflict Resolution (Deduplication)
        content = f"{tag}: {rule}"
        searcher = MemorySearcher(memory_engine.memory_collection)
        if searcher.resolve_conflict(content, meta_type="fact"):
            return {"status": "success", "logs": f"Deduplicated constraint: {tag}", "executed": {"tag": tag}}

        try:
            # PHASE 4 & 7: Atomic Dual-Sync + Adaptive Metadata
            from datetime import datetime
            import time
            import hashlib
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            m_id = hashlib.md5(f"semantic_{ts}_{tag}".encode()).hexdigest()

            # 1. SQL System of Record
            db = sqlite3.connect(DB_PATH)
            db.execute("INSERT INTO semantic_memory (tag, constraint_rule) VALUES (?, ?)", (tag, rule))
            db.commit()
            db.close()
            
            # 2. Vector Search Engine with Lifecycle Metadata
            memory_engine.memory_collection.add(
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
            log.info(f"✓ Semantic memory atomized: {m_id}")
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
    }


# ============================================================================
# SESSION MEMORY — Condensed interaction records
# ============================================================================
class SessionTracker:
    """Tracks one user interaction: query → tools → answer. Saves to SQLite."""

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
        self.final_answer = answer[:500]  # Cap at 500 chars — no noise

    def save(self, db_path: str):
        """Persist condensed session to SQLite."""
        import sqlite3
        if not self.user_query:
            return
        try:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            db = sqlite3.connect(db_path)
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
            db.commit()
            db.close()
        except Exception as e:
            log.debug(f"Session save failed: {e}")


# ============================================================================
# MAIN LOOP
# ============================================================================
# ============================================================================
# MAIN LOOP
# ============================================================================
def main():
    # ---- Interactive Project Selection ----
    global _ws_name, DB_PATH, ANCHOR_PATH, WORKSPACE_PATH, BACKUP_DIR
    _ws_name = select_project_interactive()

    # ---- Map Paths based on Selection ----
    WORKSPACE_PATH = os.path.join(WORKSPACE_BASE, _ws_name)
    DB_PATH = os.path.join(WORKSPACE_PATH, "memory.db")
    ANCHOR_PATH = os.path.join(WORKSPACE_PATH, "anchor.md")
    BACKUP_DIR = os.path.join(WORKSPACE_PATH, "backups")

    # Ensure project folder exists
    os.makedirs(WORKSPACE_PATH, exist_ok=True)

    # ---- Detect Provider & API Key ----
    provider_name, provider_config, api_key, model = detect_provider()

    # ---- Initialize Core Components ----
    log.info(f"Booting AELVO... [Project: {_ws_name}]")
    log.info(f"Provider: {provider_name} | SDK: {provider_config.sdk} | Model: {model}")
    
    # ---- Scaffold Anchor if missing ----
    if not os.path.exists(ANCHOR_PATH):
        default_anchor = f"""---
project: {_ws_name}
constraints:
  workspace:
    value: ./{_ws_name}
    locked: true
    applies_to: [write_file, read_file, edit_file]
  PROJECT_GOAL:
    value: { _ws_name } implementation
    locked: false
---
# AELVO Anchor Document ({_ws_name})
This file defines the system's locked constraints specifically for this project.
"""
        with open(ANCHOR_PATH, "w", encoding="utf-8") as f:
            f.write(default_anchor)

    # 1. AelvoKernel (commands.py) — the #command router & state manager
    aelvo_kernel = AelvoKernel(
        db_path=DB_PATH,
        anchor_path=ANCHOR_PATH,
        backup_dir=BACKUP_DIR
    )
    log.info("✓ AelvoKernel initialized (commands, audit trail, state)")

    # 2. AelvoFileSystem (automation.py) — jailed, locked file operations
    os.makedirs(WORKSPACE_PATH, exist_ok=True)
    fs = AelvoFileSystem(
        base_path=WORKSPACE_PATH,
        kernel=aelvo_kernel
    )
    log.info(f"✓ AelvoFileSystem jailed to: {WORKSPACE_PATH}")

    # 3. MemoryEngine (kernel.py) — THE HYBRID ENGINE (SQLite + Vector)
    memory_engine = MemoryEngine(
        db_path=DB_PATH,
        anchor_path=ANCHOR_PATH,
        tool_registry={}, # Will populate in step 4
        project_name=_ws_name
    )
    log.info(f"✓ MemoryEngine initialized (hybrid: SQLite + Vector)")

    # 4. Tool Registry — maps tool names to implementations
    tool_registry = build_tool_registry(fs, aelvo_kernel, memory_engine)
    
    # 4.1 Vector RAG Integration — mathematical concept similarity engine
    searcher = MemorySearcher(chroma_collection=memory_engine.memory_collection)
    tool_registry["search_memory"] = {
        "fn": searcher.search,
        "required_constraints": [],
        "constraints_map": {}
    }
    
    # Inject tool_registry into memory_engine
    memory_engine.tools = tool_registry

    # 5. AelvoAgent — connection context to the LLM
    agent = AelvoAgent(
        api_key=api_key,
        model=model,
        provider_name=provider_name,
        provider_config=provider_config
    )
    log.info(f"✓ AelvoAgent connected ({provider_name}: {model})")

    # ------------------------------------------------------------------------
    # BOOT LOGO
    # ------------------------------------------------------------------------
    print("\n" + "━" * 60)
    print("  ╔═══════════════════════════════════════╗")
    print("  ║         A E L V O   O N L I N E       ║")
    print("  ║           Deterministic Agent         ║")
    print("  ╚═══════════════════════════════════════╝")
    print("━" * 60)
    print(f"  Provider:  {provider_name.upper()}")
    print(f"  Model:     {model}")
    print(f"  SDK:       {provider_config.sdk}")
    print(f"  Database:  {DB_PATH}")
    print(f"  Anchor:    {ANCHOR_PATH}")
    print(f"  Workspace: {WORKSPACE_PATH}")
    print("━" * 60)
    print("  Commands:  Type naturally or use #commands")
    print("  Exit:      Type 'exit', 'quit', or Ctrl+C")
    print("━" * 60 + "\n")

    # ------------------------------------------------------------------------
    # THE REPL LOOP
    # ------------------------------------------------------------------------
    while True:
        try:
            user_input = input("\nYOU > ").strip()
            if not user_input:
                continue
            if user_input.lower() in ("exit", "quit", "q"):
                log.info("Shutting down AELVO...")
                break

            # Start session tracking for this interaction
            session = SessionTracker()
            session.user_query = user_input

            # ========================================
            # ROUTE 1: Direct #kernel commands
            # ========================================
            if user_input.startswith("#"):
                log.info(f"Routing kernel command: {user_input[:50]}...")
                result = aelvo_kernel.parse_and_execute(user_input)
                print(f"\n[KERNEL] {json.dumps(result, indent=2)}\n")
                # Note: We don't feed kernel commands directly back to agent
                # memory here yet; we only feed back results of loops.
                session.record_tool("kernel", {"command": user_input[:80]}, result.get("status", "SUCCESS").lower())
                session.record_answer(json.dumps(result)[:300])
                session.save(DB_PATH)
                continue

            # ========================================
            # ROUTE 2: Natural language → Claude → Executor
            # ========================================
            log.info(f"Sending to {provider_name}...")
            raw_output = agent.send_user_message(user_input)
            log.info(f"LLM output: {raw_output[:100]}...")

            # Parse and route
            output_type, payload = parse_llm_output(raw_output)

            if output_type == "kernel_command":
                for cmd in payload:
                    log.info(f"Executing kernel command: {cmd[:50]}...")
                    result = aelvo_kernel.parse_and_execute(cmd)
                    print(f"\n[KERNEL] {json.dumps(result, indent=2)}\n")
                    agent.feed_result({"type": "kernel_command", "result": result})

            elif output_type == "tool_calls":
                # ---- Phase 8: Hard Execution Budget ----
                MAX_STEPS = 30 
                current_batch = payload

                for step in range(MAX_STEPS):
                    batch_outcomes = []
                    
                    # Execute the current batch sequentially
                    for call in current_batch:
                        tool_name = call.get("tool", "")
                        tool_args = call.get("args", {})

                        if tool_name == "respond":
                            print(f"\n[AELVO] {tool_args.get('message', '')}\n")
                            # FIX 4: Atomic Session Save on Respond
                            session.record_answer(tool_args.get('message', ''))
                            session.save(DB_PATH)
                            batch_complete = True
                            break # Inner batch loop

                        log.info(f"[Step {step + 1}/{MAX_STEPS}] Executing '{tool_name}'...")
                        try:
                            # Standard execution through MemoryEngine
                            class TurnAgent:
                                def __init__(self, action_obj): self._action = json.dumps(action_obj)
                                def get_next_action(self, context): return self._action
                                def force_regenerate(self, f): return agent.force_regenerate(f)

                            turn_agent = TurnAgent(call)
                            outcome = memory_engine.execute_turn(turn_agent, context_tags=tool_name)
                            
                            # PHASE 7: Feedback collection (Signal Awareness)
                            # If this was a search tool, record which IDs were shown to the agent
                            if tool_name == "search_memory" and outcome.get("executed", {}).get("retrieved_ids"):
                                memory_engine.last_retrieved_ids = outcome["executed"]["retrieved_ids"]
                                log.info(f"⚡ Memory Signal Tracked: {len(outcome['executed']['retrieved_ids'])} hits recorded for reinforcement.")

                        except Exception as e:
                            outcome = {"status": "error", "logs": str(e), "executed": {}}

                        agent.feed_result(outcome)
                        session.record_tool(tool_name, tool_args, outcome.get("status", "error"))
                        
                        # Stop batch if a critical failure occurs
                        if outcome.get("status") == "error":
                            break
                    
                    # EXECUTION MONITOR: Check for batch completion or confusion
                    if locals().get('batch_complete'):
                        break # Outer step loop
                        
                    # Prompt for NEXT batch or completion
                    next_output = agent.send_user_message(
                        "Batch execution complete. If you need further tools, BATCH them into a JSON array for efficiency. "
                        "If you are finished, use the 'respond' tool with your final answer."
                    )
                    
                    # Parse next batch
                    n_type, n_payload = parse_llm_output(next_output)
                    if n_type == "tool_calls":
                        current_batch = n_payload
                        continue
                    else:
                        print(f"\n[AELVO] {next_output}\n")
                        break

            else:
                # Unknown output format (just display it)
                print(f"\n[AELVO] {raw_output}\n")
                session.record_answer(raw_output)

            # Persist session to SQLite
            session.save(DB_PATH)

        except KeyboardInterrupt:
            log.info("AELVO terminated by user (Ctrl+C).")
            break
        except Exception:
            log.error("Unhandled REPL error:")
            traceback.print_exc()

    log.info("Database connections closed.")
    aelvo_kernel.conn.close()
    memory_engine.db.close()
    print("\n  AELVO shutdown complete.\n")


if __name__ == "__main__":
    main()
