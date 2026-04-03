#automation.py
import os
import json
import hashlib
import tempfile
import time
import difflib
import subprocess
from pathlib import Path
from filelock import FileLock, Timeout

class AelvoFileSystem:
    """
    The Final Hardened AELVO File System.
    Production-ready: Atomic, Locked, Truncated, and Permissioned.
    """
    def __init__(self, base_path: str, kernel):
        self.base_path = Path(base_path).resolve()
        self.kernel = kernel
        self.MAX_FILE_SIZE = 5 * 1024 * 1024  # 5MB
        self.MAX_DIFF_SIZE = 5000             # Fix 1: Protect DB from bloat
        self.LOCK_TIMEOUT = 5 
        
        if not self.base_path.exists():
            self.base_path.mkdir(parents=True, exist_ok=True)

    def _validate_path(self, user_path: str) -> Path:
        # Strip redundant workspace prefixes the LLM might add
        clean = user_path.replace("\\", "/").strip()
        for prefix in ["./workspace/", "workspace/", "./workspace", "workspace"]:
            if clean.startswith(prefix):
                clean = clean[len(prefix):]
                break
        # Default to "." if empty (list root)
        if not clean:
            clean = "."
        safe_path = (self.base_path / clean).resolve()
        if not str(safe_path).startswith(str(self.base_path)):
            raise PermissionError(f"AELVO SECURITY: Access denied for '{user_path}'.")
        return safe_path

    def chunk_text(self, text: str, chunk_size: int = 500, overlap: int = 50) -> list:
        """
        Phase 3: The Slicer. 
        Breaks text into overlapping blocks to preserve semantic context
        for vector embeddings.
        """
        chunks = []
        if not text: return []
        
        start = 0
        while start < len(text):
            end = start + chunk_size
            chunks.append(text[start:end])
            start += (chunk_size - overlap)
            
        return chunks

    def _is_printable(self, data: str) -> bool:
        """Fix 3: Heuristic tradeoff - blocks binary, might hit minified JS."""
        if not data: return True
        non_printable = sum(1 for char in data[:1000] if not (char.isprintable() or char.isspace()))
        return (non_printable / min(len(data), 1000)) < 0.10 

    def _log_diff(self, path: str, old_content: str, new_content: str):
        """Fix 1: Truncated diff logging."""
        diff = list(difflib.unified_diff(
            old_content.splitlines(), 
            new_content.splitlines(), 
            fromfile='before', tofile='after', n=0
        ))
        diff_text = "\n".join(diff) if diff else "NO_CHANGE"
        
        # Truncation logic
        if len(diff_text) > self.MAX_DIFF_SIZE:
            diff_text = diff_text[:self.MAX_DIFF_SIZE] + "\n\n...[DIFF TRUNCATED]..."

        with self.kernel.db_lock:
            with self.kernel.conn:
                self.kernel.conn.execute(
                    "INSERT INTO audit_trail (cmd_type, args, status, msg) VALUES (?, ?, ?, ?)",
                    (f"FS_DIFF", json.dumps({"path": str(path)}), "LOGGED", diff_text)
                )

    def _acquire_with_retry(self, lock: FileLock, attempts: int = 2):
        """Fix 4: Basic retry mechanism for lock contention."""
        for i in range(attempts):
            try:
                return lock.acquire(timeout=self.LOCK_TIMEOUT)
            except Timeout:
                if i == attempts - 1: raise
                time.sleep(1) # Grace period

    def read_file(self, path: str) -> dict:
        try:
            safe_path = self._validate_path(path)
            if not safe_path.is_file(): return {"status": "error", "logs": "File not found."}

            lock = FileLock(str(safe_path) + ".aelvo.lock")
            with self._acquire_with_retry(lock):
                for enc in ['utf-8', 'latin-1']:
                    try:
                        with open(safe_path, 'r', encoding=enc) as f:
                            content = f.read()
                        if not self._is_printable(content):
                            return {"status": "error", "logs": "Binary content detected."}
                        return {"status": "success", "data": content[:25000]}
                    except UnicodeDecodeError:
                        continue
            return {"status": "error", "logs": "Failed to decode file."}
        except Timeout:
            return {"status": "error", "logs": "Lock timeout: File is busy."}
        except Exception as e:
            return {"status": "error", "logs": str(e)}

    def write_atomic(self, path: str, content: str) -> dict:
        """Atomic Write with strict size enforcement."""
        if len(content) > self.MAX_FILE_SIZE:
            return {"status": "error", "logs": f"Size error: {len(content)} > {self.MAX_FILE_SIZE}"}

        try:
            safe_path = self._validate_path(path)
            safe_path.parent.mkdir(parents=True, exist_ok=True)

            lock = FileLock(str(safe_path) + ".aelvo.lock")
            with self._acquire_with_retry(lock):
                fd, temp_p = tempfile.mkstemp(dir=str(safe_path.parent))
                try:
                    with os.fdopen(fd, 'w', encoding='utf-8') as f:
                        f.write(content)
                    os.replace(temp_p, safe_path)
                except Exception as e:
                    if os.path.exists(temp_p): os.remove(temp_p)
                    raise e
            
            # Log the FS_WRITE action to the audit trail
            with self.kernel.db_lock:
                with self.kernel.conn:
                    self.kernel.conn.execute(
                        "INSERT INTO audit_trail (cmd_type, args, status, msg) VALUES (?, ?, ?, ?)",
                        ("FS_WRITE", json.dumps({"path": path}), "SUCCESS", f"Wrote {len(content)} bytes to {path}")
                    )
            
            return {"status": "success", "logs": f"Wrote {path}."}
        except Exception as e:
            return {"status": "error", "logs": str(e)}

    def edit_file_block(self, path: str, old_block: str, new_block: str) -> dict:
        """Fix 2: Pre-write size check to prevent massive file expansion."""
        try:
            safe_path = self._validate_path(path)
            lock = FileLock(str(safe_path) + ".aelvo.lock")
            
            with self._acquire_with_retry(lock):
                with open(safe_path, 'r', encoding='utf-8', errors='replace') as f:
                    old_content = f.read()
                
                if old_block not in old_content:
                    return {"status": "error", "logs": "Block not found."}

                new_content = old_content.replace(old_block, new_block, 1)
                
                # Fix 2: Enforcement before the atomic swap
                if len(new_content) > self.MAX_FILE_SIZE:
                    return {"status": "error", "logs": "Resulting file exceeds 5MB limit."}

                fd, temp_p = tempfile.mkstemp(dir=str(safe_path.parent))
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                os.replace(temp_p, safe_path)
            
            self._log_diff(path, old_content, new_content)
            return {"status": "success", "logs": f"Updated {path} and logged diff.", "executed": {"workspace": "./workspace", "path": path}}
        except Exception as e:
            return {"status": "error", "logs": str(e), "executed": {"workspace": "./workspace"}}

    def _check_script_safety(self, script_path: Path):
        """Phase 8: AST-Level Sandboxing. Detects obfuscated escape vectors."""
        import ast
        
        # PROHIBITION POLICY
        BLOCKED_IMPORTS = {"subprocess", "socket", "requests", "urllib", "threading", "multiprocessing", "shutil"}
        BLOCKED_FUNCTIONS = {"exec", "eval", "getattr", "setattr", "delattr", "hasattr", "system", "popen"}

        try:
            with open(script_path, 'r', encoding='utf-8', errors='replace') as f:
                tree = ast.parse(f.read())
            
            for node in ast.walk(tree):
                # 1. Block Direct Imports
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name in BLOCKED_IMPORTS:
                            raise PermissionError(f"AELVO SECURITY: Prohibited import '{alias.name}' detected.")
                
                # 2. Block 'from X import Y'
                elif isinstance(node, ast.ImportFrom):
                    if node.module and (node.module in BLOCKED_IMPORTS or any(mod in node.module for mod in BLOCKED_IMPORTS)):
                        raise PermissionError(f"AELVO SECURITY: Prohibited from-import '{node.module}' detected.")

                # 3. Block Prohibited Function Calls (even if aliased)
                elif isinstance(node, ast.Call):
                    func_name = ""
                    if isinstance(node.func, ast.Name):
                        func_name = node.func.id
                    elif isinstance(node.func, ast.Attribute):
                        func_name = node.func.attr
                    
                    if func_name in BLOCKED_FUNCTIONS:
                        raise PermissionError(f"AELVO SECURITY: Prohibited function call '{func_name}' detected.")
                    
                    # Block dynamic __import__
                    if func_name == "__import__":
                        raise PermissionError("AELVO SECURITY: Dynamic imports via __import__ are strictly prohibited.")

        except SyntaxError as e:
            raise PermissionError(f"AELVO SECURITY: Script has syntax errors and cannot be verified: {e}")
        except Exception as e:
            if isinstance(e, PermissionError): raise
            raise PermissionError(f"AELVO SECURITY: Security verification failed: {e}")

    def python_exec(self, script_path: str, timeout: int = 30):
        """Executes a python script within the jailed workspace."""
        target = self._validate_path(script_path)
        if not target.is_file():
            raise FileNotFoundError(f"Target is not a file: {script_path}")
            
        try:
            # Phase 3 Security Check
            self._check_script_safety(target)
            result = subprocess.run(
                ["python", str(target.absolute())],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=str(self.base_path)
            )
            return {
                "status": "success" if result.returncode == 0 else "error",
                "logs": result.stdout.strip() if result.returncode == 0 else result.stderr.strip(),
                "executed": {"script": script_path, "return_code": result.returncode, "workspace": "./workspace"}
            }
        except subprocess.TimeoutExpired:
            return {"status": "error", "logs": f"Script execution timed out after {timeout} seconds", "executed": {"script": script_path, "workspace": "./workspace"}}
        except Exception as e:
            return {"status": "error", "logs": str(e), "executed": {"script": script_path, "workspace": "./workspace"}}