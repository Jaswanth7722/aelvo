# govern code of aelvo
#kernal.py
import yaml
import sqlite3
import time
import json
import hashlib
import logging
import concurrent.futures
import threading
import contextlib
import re
import os
try:
    import chromadb
except ImportError:
    chromadb = None

# NOTE: Do NOT call logging.basicConfig() at import time here. It hijacks the
# root logger globally (every aelvo.* module then prints INFO to the console,
# flooding the terminal when main.py boots). Logging is configured explicitly
# by main.py (_configure_logging) — console quiet by default, INFO+ to a file.


class LocalMemoryCollection:
    """Small Chroma-compatible fallback used when chromadb is not installed."""

    def __init__(self):
        self._docs = {}
        self._metas = {}

    def add(self, ids, documents, metadatas=None):
        metadatas = metadatas or [{} for _ in ids]
        for mid, doc, meta in zip(ids, documents, metadatas):
            self._docs[mid] = doc
            self._metas[mid] = dict(meta)

    def update(self, ids, metadatas=None, documents=None):
        for idx, mid in enumerate(ids):
            if metadatas is not None and mid in self._metas:
                self._metas[mid] = dict(metadatas[idx])
            if documents is not None and mid in self._docs:
                self._docs[mid] = documents[idx]

    def delete(self, ids):
        for mid in ids:
            self._docs.pop(mid, None)
            self._metas.pop(mid, None)

    def get(self, ids=None, where=None, include=None, limit=None):
        selected = ids or list(self._docs.keys())
        out_ids, docs, metas = [], [], []
        for mid in selected:
            if mid not in self._docs:
                continue
            meta = self._metas.get(mid, {})
            if not self._matches_where(meta, where):
                continue
            out_ids.append(mid)
            docs.append(self._docs[mid])
            metas.append(meta)
            if limit and len(out_ids) >= limit:
                break
        return {"ids": out_ids, "documents": docs, "metadatas": metas}

    def query(self, query_texts, n_results=5, where=None, include=None):
        all_ids, all_docs, all_metas, all_distances = [], [], [], []
        for query in query_texts:
            scored = []
            for mid, doc in self._docs.items():
                meta = self._metas.get(mid, {})
                if not self._matches_where(meta, where):
                    continue
                similarity = self._similarity(query, doc)
                scored.append((1.0 - similarity, mid, doc, meta))
            scored.sort(key=lambda item: item[0])
            top = scored[:n_results]
            all_distances.append([item[0] for item in top])
            all_ids.append([item[1] for item in top])
            all_docs.append([item[2] for item in top])
            all_metas.append([item[3] for item in top])
        return {"ids": all_ids, "documents": all_docs, "metadatas": all_metas, "distances": all_distances}

    @staticmethod
    def _similarity(left, right):
        left_terms = set(re.findall(r"[a-zA-Z0-9_]+", str(left).lower()))
        right_terms = set(re.findall(r"[a-zA-Z0-9_]+", str(right).lower()))
        if not left_terms or not right_terms:
            return 0.0
        return len(left_terms & right_terms) / len(left_terms | right_terms)

    @staticmethod
    def _matches_where(meta, where):
        if not where:
            return True
        for key, expected in where.items():
            actual = meta.get(key)
            if isinstance(expected, dict) and "$in" in expected:
                if actual not in expected["$in"]:
                    return False
            elif actual != expected:
                return False
        return True


class LocalChromaClient:
    _collections = {}

    def get_or_create_collection(self, name):
        if name not in self._collections:
            self._collections[name] = LocalMemoryCollection()
        return self._collections[name]

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
        self.db_path = db_path
        self.anchor_path = anchor_path
        self.project_name = project_name
        self.tools = tool_registry  
        self.db = sqlite3.connect(db_path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL;")
        self.db.execute("PRAGMA synchronous=NORMAL;")
        
        # Phase 1: Project-Specific Chroma Isolation
        chroma_path = os.path.join(os.path.dirname(db_path), "chroma_db")
        self.chroma_client = chromadb.PersistentClient(path=chroma_path) if chromadb else LocalChromaClient()
        
        # Prevent cross-project memory bleed (Signal Extraction)
        safe_proj_name = re.sub(r'[^a-zA-Z0-9_-]', '_', project_name)
        self.memory_collection = self.chroma_client.get_or_create_collection(
            name=f"aelvo_memory_{safe_proj_name}"
        )
        
        self.session_failures = 0
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="aelvo_tool")
        # Serialize access to the shared Chroma collection. Tool calls execute on
        # ThreadPoolExecutor worker threads (kernel._execute_tool) while the event
        # loop path (orchestrator) touches the same collection, so a plain
        # asyncio.Lock would NOT protect against cross-thread races.
        self._collection_lock = threading.RLock()
        self._init_db(project_name)
        self.reconcile_databases()

    @contextlib.contextmanager
    def collection_guard(self):
        """Yield the Chroma memory collection with the engine lock held.

        All readers/writers (executor worker threads, event-loop orchestrator,
        main.py tool wrappers) must go through this guard so Chroma's collection
        is never mutated concurrently.
        """
        with self._collection_lock:
            yield self.memory_collection

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

    def reconcile_databases(self):
        """Compare SQLite retained_memory with ChromaDB contents and sync any missing entries."""
        try:
            # 1. Get all entries from SQLite retained_memory
            sqlite_entries = []
            cursor = self.db.cursor()
            cursor.execute("SELECT content FROM retained_memory")
            rows = cursor.fetchall()
            for r in rows:
                sqlite_entries.append(r[0])
            
            # 2. Get all entries from ChromaDB memory_collection
            with self.collection_guard() as coll:
                chroma_data = coll.get()
            chroma_docs = chroma_data.get("documents", []) if chroma_data else []
            
            sqlite_set = set(sqlite_entries)
            chroma_set = set(chroma_docs)
            
            # 3. Find missing in ChromaDB
            import hashlib
            import time
            from datetime import datetime
            for content in sqlite_set - chroma_set:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                m_id = hashlib.sha256(f"reconciled_{ts}_{content[:30]}".encode()).hexdigest()
                try:
                    with self.collection_guard() as coll:
                        coll.add(
                            ids=[m_id],
                            documents=[content],
                            metadatas=[{
                                "type": "voluntary",
                                "timestamp": ts,
                                "timestamp_unix": time.time(),
                                "importance": 0.6,
                                "usage_count": 0,
                                "source": "reconciliation"
                            }]
                        )
                    logging.debug(f"Reconciliation: Added memory to Chroma: {content[:40]}")
                except Exception as ce:
                    logging.error(f"Reconciliation error adding to Chroma: {ce}")
            
            # 4. Find missing in SQLite
            for content in chroma_set - sqlite_set:
                try:
                    with self.db:
                        self.db.execute("INSERT INTO retained_memory (content) VALUES (?)", (content,))
                    logging.debug(f"Reconciliation: Added memory to SQLite: {content[:40]}")
                except Exception as se:
                    logging.error(f"Reconciliation error adding to SQLite: {se}")
        except Exception as e:
            logging.error(f"Database reconciliation failed: {e}")

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
                
                future = self._executor.submit(tool_func, **action_obj["args"])
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
            with self.collection_guard() as coll:
                results = coll.get(include=['metadatas', 'ids'])
                if not results['ids']: return
                u_ids, u_metas = [], []
                for meta, mid in zip(results['metadatas'], results['ids']):
                    imp = float(meta.get('importance', 0.5)) * 0.98
                    meta['importance'] = max(0.1, round(imp, 3))
                    u_ids.append(mid); u_metas.append(meta)
                if u_ids: coll.update(ids=u_ids, metadatas=u_metas)
        except Exception as e: logging.error(f"Decay Error: {e}")

    def _summarize_history(self, agent):
        """Phase 2 & 7: Compresses raw audits into high-signal mission logs."""
        try:
            rows = self.db.execute("SELECT action_taken, outcome FROM episodes ORDER BY timestamp ASC LIMIT 40").fetchall()
            if not rows: return
            digest_src = "Audit: " + "; ".join([f"{r[0][:50]}->{r[1]}" for r in rows])
            digest = agent.send_user_message(f"Summarize these logs into one paragraph of Lessons Learned:\\n\\n{digest_src}")
            
            import time
            import hashlib
            import datetime
            ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            m_id = hashlib.sha256(f"digest_{ts}".encode()).hexdigest()
            
            self.db.execute("INSERT INTO retained_memory (content) VALUES (?)", (f"MISSION LOG: {digest}",))
            with self.collection_guard() as coll:
                coll.add(
                    ids=[m_id], documents=[digest],
                    metadatas=[{"type": "summary", "timestamp": ts, "timestamp_unix": time.time(), "importance": 0.7, "usage_count": 0}]
                )
            self.db.execute("DELETE FROM episodes WHERE episode_id IN (SELECT episode_id FROM episodes ORDER BY timestamp ASC LIMIT 40)")
            self.db.commit()
            logging.info("✓ Signal Extraction complete.")
        except Exception as e: logging.error(f"Summary Error: {e}")
