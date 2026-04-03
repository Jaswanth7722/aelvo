#commands.py
import json, sqlite3, yaml, hashlib, time, os, shutil, tempfile, threading
from filelock import FileLock # Required: pip install filelock

class AelvoKernel:
    COMMAND_SCHEMA = {
        "#lock": ["target", "value"],
        "#update_anchor": ["target", "value"],
        "#drop_state": ["target"],
        "#checkpoint": ["name"],
        "#restore": ["snapshot_id"],
        "#priority": ["tag", "level"],
        "#alias": ["short", "full"],
        "#confirm": []
    }
    
    STAGED_TTL = 300 

    def __init__(self, db_path, anchor_path, backup_dir="backups"):
        self.db_path = db_path
        self.anchor_path = anchor_path
        self.lock_path = anchor_path + ".lock"
        self.backup_dir = backup_dir
        
        self.staged_change = None
        self.staged_timestamp = 0
        
        # Persistent Connection + Thread Lock (Fix 4)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.db_lock = threading.Lock()
        
        with self.db_lock:
            self.conn.execute("PRAGMA journal_mode=WAL;") 
        
        os.makedirs(self.backup_dir, exist_ok=True)
        self._init_db()
        self._sync_state_with_anchor()

    def _init_db(self):
        with self.db_lock:
            with self.conn:
                self.conn.executescript("""
                    CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT);
                    CREATE TABLE IF NOT EXISTS kv_metadata (key TEXT PRIMARY KEY, value TEXT);
                    CREATE TABLE IF NOT EXISTS aliases (short TEXT PRIMARY KEY, full TEXT);
                    CREATE TABLE IF NOT EXISTS audit_trail (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        cmd_type TEXT, args TEXT, status TEXT, msg TEXT, anchor_hash TEXT
                    );
                    CREATE TABLE IF NOT EXISTS cmd_episodes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT, 
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        tags TEXT, priority INTEGER DEFAULT 5
                    );
                """)

    # --- ATOMICITY & THREAD SAFETY ---

    def _get_anchor_data(self):
        """Robust Format Validation & Parsing."""
        if not os.path.exists(self.anchor_path):
            raise FileNotFoundError("FATAL: Anchor file missing.")
        with open(self.anchor_path, 'r') as f:
            content = f.read()
            parts = content.split('---')
            if len(parts) < 3:
                raise RuntimeError("FATAL: Invalid anchor format. Delimiters missing.")
            raw_yaml = parts[1]
            data = yaml.safe_load(raw_yaml)
            actual_hash = hashlib.sha256(raw_yaml.encode()).hexdigest()
            return data, actual_hash, parts

    def _atomic_write_anchor(self, data):
        """Structure-Preserving Write with Internal Locking (Fix 3)."""
        with FileLock(self.lock_path): # Fix 3: Internal Lock
            _, _, parts = self._get_anchor_data()
            parts[1] = "\n" + yaml.dump(data, sort_keys=False)
            new_content = '---'.join(parts)
            
            fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(self.anchor_path))
            try:
                with os.fdopen(fd, 'w') as f: f.write(new_content)
                os.replace(temp_path, self.anchor_path)
            except Exception as e:
                if os.path.exists(temp_path): os.remove(temp_path)
                raise e

    def _sync_state_with_anchor(self):
        """Transactional Anchor-to-DB Sync."""
        data, current_hash, _ = self._get_anchor_data()
        constraints = data.get("constraints", {})
        with self.db_lock: # Fix 4: Thread safety
            with self.conn:
                for k, v in constraints.items():
                    if v.get("locked"):
                        self.conn.execute("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)", 
                                         (f"constraint:{k}", v["value"]))
                self.conn.execute("INSERT OR REPLACE INTO kv_metadata (key, value) VALUES (?, ?)", 
                                 ("anchor_hash", current_hash))

    def authorize_scrape(self, url: str) -> bool:
        """Check if a URL has already been scraped successfully. Returns True if re-scrape allowed."""
        # Phase 5: Re-scrape failure recovery (Operational Hardening)
        with self.db_lock:
            result = self.conn.execute(
                "SELECT COUNT(*) FROM audit_trail WHERE cmd_type IN (?, ?) AND args LIKE ? AND status = ?",
                ("heavy_crawl", "light_scrape", f'%{url}%', "SUCCESS")
            ).fetchone()
        return result[0] == 0

    # --- VALIDATION GATE ---

    def _validate(self, cmd, args):
        """Fix 1: Strict Equality Key Validation."""
        if cmd not in self.COMMAND_SCHEMA:
            raise ValueError(f"Unknown command: {cmd}")
        
        provided = set(args.keys())
        needed = set(self.COMMAND_SCHEMA[cmd])
        
        if provided != needed: # Fix 1: Rejects extra keys
            raise ValueError(f"Argument mismatch for {cmd}. Expected {needed}, got {provided}")

    def _validate_dependencies(self, target, value):
        """Constraint Consistency & Conflict Detection (Fix 5)."""
        # 1. Hardware/Software check
        if target == "model" and value == "nemotron":
            with self.db_lock:
                res = self.conn.execute("SELECT value FROM state WHERE key = ?", ("constraint:env",)).fetchone()
            if not res or res[0] != "kaggle_v1":
                raise ValueError("DEPENDENCY ERROR: Nemotron requires Kaggle.")
        
        # 2. Conflict Detection (Fix 5)
        data, _, _ = self._get_anchor_data()
        existing = data.get("constraints", {}).get(target)
        if existing and existing["value"] != value:
            # Not a failure, but a detected conflict for the audit trail
            return f"CONFLICT: Overwriting existing value '{existing['value']}'"
        return "OK"

    # --- COMMAND ENGINE ---

    def parse_and_execute(self, user_input):
        if not user_input.startswith("#"): return None
        
        if self.staged_change and (time.time() - self.staged_timestamp > self.STAGED_TTL):
            self.staged_change = None

        cmd, args = "UNKNOWN", {}
        try:
            parts = user_input.split(maxsplit=1)
            cmd = parts[0].strip().lower()
            
            if cmd in self.COMMAND_SCHEMA:
                schema_keys = self.COMMAND_SCHEMA[cmd]
                if len(schema_keys) > 0 and len(parts) > 1:
                    raw_args_str = parts[1].strip()
                    if raw_args_str.startswith("{"):
                        args = json.loads(raw_args_str)
                    else:
                        arg_parts = raw_args_str.split(maxsplit=len(schema_keys)-1)
                        if len(arg_parts) == len(schema_keys):
                            args = {k: v.strip('"\'') for k, v in zip(schema_keys, arg_parts)}
                        else:
                            raise ValueError(f"Expected arguments: {schema_keys}")

            # Resolve Alias
            if "value" in args:
                with self.db_lock:
                    res = self.conn.execute("SELECT full FROM aliases WHERE short = ?", (args["value"],)).fetchone()
                args["value"] = res[0] if res else args["value"]

            self._validate(cmd, args)
            dep_status = "OK"
            if "target" in args and "value" in args:
                dep_status = self._validate_dependencies(args["target"], args["value"])

            result = self._dispatch(cmd, args)
            self._log_audit(cmd, args, "SUCCESS", f"{result.get('msg', '')} | {dep_status}")
            return result
        except Exception as e:
            self._log_audit(cmd, args, "REJECTED", str(e))
            return {"status": "REJECTED", "error": str(e)}

    # --- EXECUTORS ---

    def _execute_lock(self, args):
        data, _, _ = self._get_anchor_data()
        if "constraints" not in data: data["constraints"] = {}
        data["constraints"][args["target"]] = {"value": args["value"], "locked": True}
        
        self._atomic_write_anchor(data)
        self._sync_state_with_anchor()
        return {"status": "SUCCESS", "msg": f"Target {args['target']} locked."}

    def _execute_restore(self, args):
        """Fix 2: Forensic Manifest Validation Before Restore."""
        sid = args["snapshot_id"]
        snap_path = os.path.join(self.backup_dir, sid)
        manifest_path = os.path.join(snap_path, "manifest.json")
        snap_anchor = os.path.join(snap_path, "anchor.md")

        if not os.path.exists(manifest_path): raise FileNotFoundError("Manifest missing.")
        
        # FIX 2: Validate snapshot integrity before swap
        with open(manifest_path, 'r') as f: manifest = json.load(f)
        with open(snap_anchor, 'r') as f:
            raw_yaml = f.read().split('---')[1]
            snap_hash = hashlib.sha256(raw_yaml.encode()).hexdigest()
        
        if snap_hash != manifest["hash"]:
            raise RuntimeError("CRITICAL: Snapshot corruption. Hash mismatch.")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_db = shutil.copy2(os.path.join(snap_path, "memory.db"), tmpdir)
            tmp_an = shutil.copy2(snap_anchor, tmpdir)
            os.replace(tmp_db, self.db_path)
            os.replace(tmp_an, self.anchor_path)
        
        self._sync_state_with_anchor()
        return {"status": "SUCCESS", "msg": f"Restored to {sid}."}

    def _log_audit(self, cmd, args, status, msg):
        with self.db_lock:
            with self.conn:
                res = self.conn.execute("SELECT value FROM kv_metadata WHERE key = 'anchor_hash'").fetchone()
                curr_hash = res[0] if res else "UNKNOWN"
                self.conn.execute("INSERT INTO audit_trail (cmd_type, args, status, msg, anchor_hash) VALUES (?,?,?,?,?)",
                                 (cmd, json.dumps(args), status, msg, curr_hash))

    def _dispatch(self, cmd, args):
        if cmd == "#lock": return self._execute_lock(args)
        if cmd == "#update_anchor":
            self.staged_change, self.staged_timestamp = args, time.time()
            return {"status": "STAGED", "msg": "Issue #confirm to apply."}
        if cmd == "#restore": return self._execute_restore(args)
        if cmd == "#checkpoint": return self._execute_checkpoint(args)
        if cmd == "#confirm": return self._execute_confirm(args)
        if cmd == "#priority": return self._execute_priority(args)
        if cmd == "#alias": return self._execute_alias(args)
        if cmd == "#drop_state": return self._execute_drop_state(args)
        return {"status": "SUCCESS"}

    def _execute_checkpoint(self, args):
        """Create a full snapshot backup of memory.db + anchor.md."""
        name = args["name"]
        snap_path = os.path.join(self.backup_dir, name)
        os.makedirs(snap_path, exist_ok=True)

        # Copy current state
        shutil.copy2(self.db_path, os.path.join(snap_path, "memory.db"))
        shutil.copy2(self.anchor_path, os.path.join(snap_path, "anchor.md"))

        # Create forensic manifest
        _, current_hash, _ = self._get_anchor_data()
        manifest = {
            "name": name,
            "hash": current_hash,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        with open(os.path.join(snap_path, "manifest.json"), 'w') as f:
            json.dump(manifest, f, indent=2)

        return {"status": "SUCCESS", "msg": f"Checkpoint '{name}' saved to {snap_path}."}

    def _execute_confirm(self, args):
        """Apply a staged #update_anchor change."""
        if not self.staged_change:
            return {"status": "REJECTED", "msg": "Nothing staged to confirm."}
        if time.time() - self.staged_timestamp > self.STAGED_TTL:
            self.staged_change = None
            return {"status": "REJECTED", "msg": "Staged change expired (TTL exceeded)."}

        data, _, _ = self._get_anchor_data()
        if "constraints" not in data:
            data["constraints"] = {}
        target = self.staged_change["target"]
        value = self.staged_change["value"]
        data["constraints"][target] = {"value": value, "locked": True}

        self._atomic_write_anchor(data)
        self._sync_state_with_anchor()
        self.staged_change = None
        return {"status": "SUCCESS", "msg": f"Confirmed: {target} = {value}"}

    def _execute_priority(self, args):
        """Tag an episode with a priority level."""
        tag = args["tag"]
        level = int(args["level"])
        with self.db_lock:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO cmd_episodes (tags, priority) VALUES (?, ?)",
                    (tag, level)
                )
        return {"status": "SUCCESS", "msg": f"Priority '{tag}' set to level {level}."}

    def _execute_alias(self, args):
        """Register a shorthand alias for a constraint value."""
        short = args["short"]
        full = args["full"]
        with self.db_lock:
            with self.conn:
                self.conn.execute(
                    "INSERT OR REPLACE INTO aliases (short, full) VALUES (?, ?)",
                    (short, full)
                )
        return {"status": "SUCCESS", "msg": f"Alias '{short}' → '{full}' registered."}

    def _execute_drop_state(self, args):
        """Remove a key from the state table."""
        target = args["target"]
        with self.db_lock:
            with self.conn:
                cursor = self.conn.execute("DELETE FROM state WHERE key = ?", (target,))
        if cursor.rowcount == 0:
            return {"status": "REJECTED", "msg": f"Key '{target}' not found in state."}
        return {"status": "SUCCESS", "msg": f"Dropped state key '{target}'."}

