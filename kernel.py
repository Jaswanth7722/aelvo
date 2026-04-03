# govern code of aelvo
#kernal.py
import yaml
import sqlite3
import time
import json
import hashlib
import logging
import concurrent.futures
import re
import chromadb
from chromadb.config import Settings
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')

def extract_yaml_frontmatter(text):
    if not text.startswith('---'):
        raise ValueError("FATAL: No YAML frontmatter found in anchor.")
    parts = text.split('---')
    if len(parts) < 3:
        raise ValueError("FATAL: Malformed frontmatter in anchor.")
    return parts[1]

def validate_action(action_obj):
    if isinstance(action_obj, str):
        try:
            action_obj = json.loads(action_obj)
        except json.JSONDecodeError:
            raise ValueError("FATAL: Action must be valid JSON.")
    assert isinstance(action_obj, dict), "FATAL: Action must be a dictionary."
    assert "tool" in action_obj, "FATAL: Action missing 'tool' key."
    assert "args" in action_obj and isinstance(action_obj["args"], dict), "FATAL: Action 'args' invalid."
    return action_obj

class MemoryEngine:
    def __init__(self, db_path, anchor_path, tool_registry, project_name="default_project"):
        self.anchor_path = anchor_path
        # tool_registry: {"tool_name": {"fn": callable, "constraints_map": {}, "required_constraints": []}}
        self.tools = tool_registry  
        self.db = sqlite3.connect(db_path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL;")
        self.db.execute("PRAGMA synchronous=NORMAL;")
        
        # Phase 1: Project-Specific Chroma Isolation
        chroma_path = os.path.join(os.path.dirname(db_path), "chroma_db")
        self.chroma_client = chromadb.PersistentClient(path=chroma_path)
        # Prevent cross-project memory bleed (Signal Extraction)
        safe_proj_name = re.sub(r'[^a-zA-Z0-9_-]', '_', project_name)
        self.memory_collection = self.chroma_client.get_or_create_collection(
            name=f"aelvo_memory_{safe_proj_name}"
        )
        
        self.session_failures = 0
        self._init_db(project_name)

    def _init_db(self, project_name):
        with self.db:
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS metadata (
                    project_name TEXT, version INTEGER, anchor_hash TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE IF NOT EXISTS episodes (
                    episode_id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    action_taken TEXT, outcome TEXT, technical_reason TEXT, tags TEXT
                );
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    user_query TEXT,
                    tools_used TEXT,
                    files_touched TEXT,
                    final_answer TEXT,
                    status TEXT DEFAULT 'success'
                );
                CREATE TABLE IF NOT EXISTS semantic_memory (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tag TEXT,
                    constraint_rule TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS retained_memory (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    content TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TRIGGER IF NOT EXISTS update_meta_state AFTER INSERT ON state
                BEGIN UPDATE metadata SET last_updated = CURRENT_TIMESTAMP; END;
                CREATE TRIGGER IF NOT EXISTS update_meta_episodes AFTER INSERT ON episodes
                BEGIN UPDATE metadata SET last_updated = CURRENT_TIMESTAMP; END;
            """)
            if self.db.execute("SELECT COUNT(*) FROM metadata").fetchone()[0] == 0:
                self.db.execute("INSERT INTO metadata (project_name, version, anchor_hash) VALUES (?, 1, '')", (project_name,))

    def parse_anchor(self):
        with open(self.anchor_path, 'r') as f:
            raw_yaml = extract_yaml_frontmatter(f.read())
            data = yaml.safe_load(raw_yaml)
            
        constraints = data.get("constraints", {})
        # DETECT CONFLICTING CONSTRAINTS
        seen = {}
        for k, v in constraints.items():
            if k in seen and seen[k] != v["value"]:
                raise RuntimeError(f"FATAL: Conflicting constraint detected for {k}: {seen[k]} vs {v['value']}")
            seen[k] = v["value"]
            
        current_hash = hashlib.sha256(raw_yaml.encode()).hexdigest()
        stored_hash = self.db.execute("SELECT anchor_hash FROM metadata").fetchone()[0]
        if stored_hash and stored_hash != current_hash:
            with self.db: self.db.execute("UPDATE metadata SET anchor_hash = ?", (current_hash,))
            raise RuntimeError("FATAL: Anchor changed. Manual state resync required.")
        return constraints

    def sync_state(self, constraints):
        with self.db:
            # Prune stale runtime keys
            allowed_runtime = ['runtime:last_action', 'runtime:last_status', 'runtime:last_error']
            self.db.execute(f"DELETE FROM state WHERE key LIKE 'runtime:%' AND key NOT IN ({','.join(['?']*len(allowed_runtime))})", allowed_runtime)
            
            # Sync constraints to state
            for k, v in constraints.items():
                if v.get("locked"):
                    self.db.execute("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)", (f"constraint:{k}", v["value"]))

        current_state = {row[0]: row[1] for row in self.db.execute("SELECT key, value FROM state").fetchall()}
        return current_state

    def check_constraints(self, action_obj, constraints):
        violations = []
        tool_name = action_obj["tool"]
        args = action_obj["args"]
        tool_spec = self.tools.get(tool_name, {})
        tool_mapping = tool_spec.get("constraints_map", {})
        
        # REQUIREMENT 1: Ensure Anchor is complete for this tool
        required_for_tool = tool_spec.get("required_constraints", [])
        for rc in required_for_tool:
            if rc not in constraints:
                raise RuntimeError(f"FATAL: Anchor missing required constraint '{rc}' for tool '{tool_name}'")

        # REQUIREMENT 2: Validate intent vs Anchor
        for k, v in constraints.items():
            if v.get("locked") and tool_name in v.get("applies_to", []):
                arg_key = tool_mapping.get(k, k)
                if arg_key not in args:
                    violations.append(f"Missing explicit arg: '{arg_key}'")
                elif args.get(arg_key) != v["value"]:
                    violations.append(f"Constraint mismatch: {arg_key} must be {v['value']}")
        return violations

    def execute_turn(self, agent, context_tags=None):
        if self.session_failures >= 3:
            return {"status": "fallback", "logs": "Circuit breaker: Agent degraded."}

        # Phase 7: Memory Hygiene (Lifecycle Decay)
        self.decay_memory()

        constraints = self.parse_anchor()
        current_state = self.sync_state(constraints)
        
        recent_episodes = self.db.execute(
            "SELECT action_taken, outcome, technical_reason FROM episodes WHERE tags LIKE ? ORDER BY timestamp DESC LIMIT 10",
            (f'%{context_tags or ""}%',)
        ).fetchall()
        
        MAX_RETRIES = 2
        timeout_start = time.time()
        import datetime
        raw_action = agent.get_next_action(context={"constraints": constraints, "state": current_state, "episodes": recent_episodes})

        for i in range(MAX_RETRIES + 1):
            if time.time() - timeout_start > 120: raise RuntimeError("Timeout: Loop hang.")
            try:
                action_obj = validate_action(raw_action)
                if action_obj["tool"] not in self.tools: raise ValueError(f"Unknown tool: {action_obj['tool']}")
                violations = self.check_constraints(action_obj, constraints)
            except Exception as e: violations = [str(e)]
            
            if not violations: break
            if i == MAX_RETRIES:
                self.session_failures += 1
                raise RuntimeError(f"Hard stop: Violations: {violations}")

            modifier = "RETURN ONLY JSON." if i == 1 else "Fix violations."
            raw_action = agent.force_regenerate(f"Violations: {violations}\nRules:\n{yaml.dump(constraints)}\n{modifier}")

        with self.db:
            try:
                tool_spec = self.tools[action_obj["tool"]]
                tool_func = tool_spec["fn"]
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(tool_func, **action_obj["args"])
                    outcome = future.result(timeout=90)
                
                if not isinstance(outcome, dict): raise RuntimeError("Tool must return dict")
                for k in ["status", "logs", "executed"]:
                    if k not in outcome: raise RuntimeError(f"Tool missing key: {k}")
                if not isinstance(outcome["executed"], dict): raise RuntimeError("'executed' must be dict")
                
                executed = outcome["executed"]
                tool_map = tool_spec.get("constraints_map", {})
                for k, v in constraints.items():
                    if v.get("locked") and action_obj["tool"] in v.get("applies_to", []):
                        ark = tool_map.get(k, k)
                        if ark not in executed: raise RuntimeError(f"Tool failed to report '{ark}'")
                        if executed[ark] != v["value"]:
                            raise RuntimeError(f"Honesty Violation: Tool used {executed[ark]} not {v['value']}")
                
                self.session_failures = 0 
            except Exception as e:
                logging.error(f"Execution Error: {e}")
                outcome = {"status": "error", "error": str(e), "logs": "Crash/Violation", "executed": {}, "important": True}
                self.session_failures += 1

            self.db.execute("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)", ("runtime:last_status", outcome["status"]))
            if outcome.get("status") == "error":
                self.db.execute("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)", ("runtime:last_error", outcome.get("error")))

            slashed_outcome = json.loads(json.dumps(outcome))
            if "logs" in slashed_outcome and len(str(slashed_outcome["logs"])) > 1000:
                slashed_outcome["logs"] = f"[LOGS SLASHED: {len(str(slashed_outcome['logs']))} bytes]"
            
            clean_action = dict(action_obj)
            if "args" in clean_action and isinstance(clean_action["args"], dict):
                clean_action["args"] = dict(clean_action["args"])
                for heavy_key in ["content", "old_block", "new_block"]:
                    if heavy_key in clean_action["args"]:
                        size = len(str(clean_action["args"][heavy_key]))
                        clean_action["args"][heavy_key] = f"[TRUNCATED {size} BYTES]"

            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.db.execute("INSERT INTO episodes (timestamp, action_taken, outcome, technical_reason, tags) VALUES (?, ?, ?, ?, ?)",
                            (timestamp, json.dumps(clean_action), outcome["status"], json.dumps(slashed_outcome), context_tags or action_obj["tool"]))
            self.db.commit()

            if self.db.execute("SELECT COUNT(*) FROM episodes").fetchone()[0] >= 50:
                self._summarize_history(agent)
            
        return outcome

    def decay_memory(self):
        """Phase 7: Reduces importance of unused memories to ensure fresh project focus."""
        try:
            results = self.memory_collection.get(include=['metadatas', 'ids'])
            if not results['ids']: return
            u_ids, u_metas = [], []
            for meta, mid in zip(results['metadatas'], results['ids']):
                imp = float(meta.get('importance', 0.5)) * 0.98
                meta['importance'] = max(0.1, round(imp, 3))
                u_ids.append(mid); u_metas.append(meta)
            if u_ids: self.memory_collection.update(ids=u_ids, metadatas=u_metas)
        except Exception as e: logging.error(f"Decay Error: {e}")

    def _summarize_history(self, agent):
        """Phase 2 & 7: Compresses raw audits into high-signal mission logs."""
        try:
            rows = self.db.execute("SELECT action_taken, outcome FROM episodes ORDER BY timestamp ASC LIMIT 40").fetchall()
            if not rows: return
            digest_src = "Audit: " + "; ".join([f"{r[0][:50]}->{r[1]}" for r in rows])
            digest = agent.send_user_message(f"Summarize these logs into one paragraph of Lessons Learned:\\n\\n{digest_src}")
            
            import time, hashlib, datetime
            ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            m_id = hashlib.md5(f"digest_{ts}".encode()).hexdigest()
            
            self.db.execute("INSERT INTO retained_memory (content) VALUES (?)", (f"MISSION LOG: {digest}",))
            self.memory_collection.add(
                ids=[m_id], documents=[digest],
                metadatas=[{"type": "summary", "timestamp": ts, "timestamp_unix": time.time(), "importance": 0.7, "usage_count": 0}]
            )
            self.db.execute("DELETE FROM episodes WHERE episode_id IN (SELECT episode_id FROM episodes ORDER BY timestamp ASC LIMIT 40)")
            self.db.commit()
            logging.info("✓ Signal Extraction complete.")
        except Exception as e: logging.error(f"Summary Error: {e}")