#automation.py
import os
import json
import difflib
import subprocess
import threading
from pathlib import Path

try:
    from filelock import FileLock, Timeout
except ImportError:
    class Timeout(Exception):
        """Fallback timeout used when filelock is unavailable."""

    class FileLock:
        _locks: dict[str, threading.Lock] = {}

        def __init__(self, path: str):
            self.path = path
            self._lock = self._locks.setdefault(path, threading.Lock())
            self._acquired = False

        def acquire(self, timeout: float | int | None = None):
            acquired = self._lock.acquire(timeout=timeout if timeout is not None else -1)
            if not acquired:
                raise Timeout(f"Timed out acquiring lock: {self.path}")
            self._acquired = True
            return self

        def release(self):
            if self._acquired:
                self._lock.release()
                self._acquired = False

        def __enter__(self):
            return self.acquire()

        def __exit__(self, exc_type, exc, tb):
            self.release()
            return False

from config.settings import (
    DEFAULT_TOOL_TIMEOUT_SECONDS,
    MAX_FILE_SIZE_BYTES,
    LOCK_TIMEOUT_SECONDS,
    BASE_DIR,
)

class AelvoFileSystem:
    """
    The Final Hardened AELVO File System.
    Offloaded to the secure Rust Sandbox Core for production-grade jailing, jailing enforcement,
    and process isolation.
    """
    def __init__(self, base_path: str, kernel):
        self.base_path = Path(base_path).resolve()
        self.kernel = kernel
        self.MAX_FILE_SIZE = MAX_FILE_SIZE_BYTES
        self.MAX_DIFF_SIZE = 5000
        self.LOCK_TIMEOUT = LOCK_TIMEOUT_SECONDS
        
        if not self.base_path.exists():
            self.base_path.mkdir(parents=True, exist_ok=True)

    def _invoke_rust_sandbox(self, action: str, write_mode: bool, params: dict) -> dict:
        """Invoke the compiled Rust sandbox core binary with a JSON-RPC request."""
        # Locate the compiled Rust sandbox executable
        binary_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
            "sandbox_core", "target", "release", "sandbox_core.exe"
        )
        
        request_data = {
            "action": action,
            "workspace_root": str(self.base_path),
            "repo_root": str(BASE_DIR),
            "write_mode": write_mode,
            "params": params
        }
        
        try:
            result = subprocess.run(
                [binary_path],
                input=json.dumps(request_data),
                capture_output=True,
                text=True,
                timeout=params.get("timeout_seconds", DEFAULT_TOOL_TIMEOUT_SECONDS) + 5
            )
            
            if result.returncode == 0:
                response = json.loads(result.stdout.strip())
                return response
            else:
                return {
                    "success": False,
                    "status": "error",
                    "logs": f"Rust Sandbox execution error: {result.stderr.strip()}"
                }
        except subprocess.TimeoutExpired:
            return {
                "success": False,
                "status": "timeout",
                "logs": "Rust Sandbox timed out."
            }
        except Exception as e:
            return {
                "success": False,
                "status": "error",
                "logs": f"Rust Sandbox invocation failed: {e}"
            }

    def _validate_path(self, user_path: str, read_only: bool = False) -> Path:
        """Validate and resolve a path through the Rust Sandbox's path jailing.

        FAILS CLOSED: if the Rust sandbox is unavailable or returns an error,
        we raise a SecurityError rather than falling back to local resolution.
        """
        params = {"path": user_path}
        res = self._invoke_rust_sandbox("resolve_path", False, params)
        if res.get("success"):
            data = res.get("data", {}) or {}
            resolved = data.get("resolved_path")
            if resolved:
                if resolved.startswith("\\\\?\\"):
                    resolved = resolved[4:]
                return Path(resolved)

        # Fail closed: if sandbox binary is unavailable or validation fails,
        # we deny the operation rather than silently falling back.
        msg = res.get("logs", "Sandbox unavailable")
        raise PermissionError(f"Path validation denied by sandbox: {msg}")

    def chunk_text(self, text: str, chunk_size: int = 500, overlap: int = 50) -> list:
        chunks = []
        if not text: return []
        
        start = 0
        while start < len(text):
            end = start + chunk_size
            chunks.append(text[start:end])
            start += (chunk_size - overlap)
            
        return chunks

    def _is_printable(self, data: str) -> bool:
        if not data: return True
        non_printable = sum(1 for char in data[:1000] if not (char.isprintable() or char.isspace()))
        return (non_printable / min(len(data), 1000)) < 0.10 

    def _log_diff(self, path: str, old_content: str, new_content: str):
        diff = list(difflib.unified_diff(
            old_content.splitlines(), 
            new_content.splitlines(), 
            fromfile='before', tofile='after', n=0
        ))
        diff_text = "\n".join(diff) if diff else "NO_CHANGE"
        
        if len(diff_text) > self.MAX_DIFF_SIZE:
            diff_text = diff_text[:self.MAX_DIFF_SIZE] + "\n\n...[DIFF TRUNCATED]..."

        with self.kernel.db_lock:
            with self.kernel.conn:
                self.kernel.conn.execute(
                    "INSERT INTO audit_trail (cmd_type, args, status, msg) VALUES (?, ?, ?, ?)",
                    ("FS_DIFF", json.dumps({"path": str(path)}), "LOGGED", diff_text)
                )

    def read_file(self, path: str) -> dict:
        params = {"path": path}
        res = self._invoke_rust_sandbox("read_file", False, params)
        if res.get("success"):
            data = res.get("data", {}) or {}
            content = data.get("content", "")
            return {"status": "success", "data": content}
        return {"status": "error", "logs": res.get("logs", "Read failed.")}

    def read_file_range(self, path: str, start_line: int = 1, end_line: int = 120) -> dict:
        params = {"path": path, "start_line": start_line, "end_line": end_line}
        res = self._invoke_rust_sandbox("read_file_range", False, params)
        if res.get("success"):
            data = res.get("data", {}) or {}
            content = data.get("content", "")
            lines = content.split("\n") if content else []
            return {
                "status": "success",
                "logs": content,
                "executed": {"path": path, "start_line": start_line, "end_line": end_line},
                "data": lines
            }
        return {"status": "error", "logs": res.get("logs", "Read range failed."), "executed": {"path": path}}

    def write_atomic(self, path: str, content: str) -> dict:
        params = {"path": path, "content": content}
        res = self._invoke_rust_sandbox("write_atomic", True, params)
        if res.get("success"):
            return {"status": "success", "logs": f"Wrote {path}."}
        return {"status": "error", "logs": res.get("logs", "Write failed.")}

    def edit_file_block(self, path: str, old_block: str, new_block: str) -> dict:
        # Read old content first to log diff for learning loop
        old_res = self.read_file(path)
        old_content = old_res.get("data", "") if old_res.get("status") == "success" else ""
        
        params = {"path": path, "old_block": old_block, "new_block": new_block}
        res = self._invoke_rust_sandbox("edit_file_block", True, params)
        if res.get("success"):
            # Re-read new content to compute unified diff
            new_res = self.read_file(path)
            new_content = new_res.get("data", "") if new_res.get("status") == "success" else ""
            if old_content and new_content:
                self._log_diff(path, old_content, new_content)
                
            return {
                "status": "success",
                "logs": f"Updated {path} and logged diff.",
                "executed": {"workspace": "./workspace", "path": path}
            }
        return {"status": "error", "logs": res.get("logs", "Edit failed."), "executed": {"workspace": "./workspace"}}

    def grep_file(self, path: str, pattern: str, case_sensitive: bool = False, max_matches: int = 100) -> dict:
        """Search one file with a regular expression via the Rust sandbox."""
        params = {
            "path": path,
            "pattern": pattern,
            "case_sensitive": case_sensitive,
            "max_matches": max_matches,
        }
        res = self._invoke_rust_sandbox("grep_file", False, params)
        if res.get("success"):
            data = res.get("data", {}) or {}
            matches = data.get("matches", [])
            return {
                "status": "success",
                "logs": f"Found {len(matches)} match(es) in {path}.",
                "executed": {"path": path, "pattern": pattern, "match_count": len(matches)},
                "data": matches,
            }
        return {"status": "error", "logs": res.get("logs", "Grep failed."), "executed": {"path": path, "pattern": pattern}}

    def search_code(self, query: str, max_matches: int = 100) -> dict:
        """Search source-like files across the workspace via the Rust sandbox."""
        params = {
            "query": query,
            "max_matches": max_matches,
        }
        res = self._invoke_rust_sandbox("search_code", False, params)
        if res.get("success"):
            data = res.get("data", {}) or {}
            matches = data.get("matches", [])
            return {
                "status": "success",
                "logs": f"Found {len(matches)} match(es) for '{query}'.",
                "executed": {"query": query, "match_count": len(matches)},
                "data": matches,
            }
        return {"status": "error", "logs": res.get("logs", "Search failed."), "executed": {"query": query}}

    def find_files(self, pattern: str = "*", max_results: int = 200) -> dict:
        """Find files by glob pattern under the workspace jail via the Rust sandbox."""
        params = {
            "pattern": pattern,
            "max_results": max_results,
        }
        res = self._invoke_rust_sandbox("find_files", False, params)
        if res.get("success"):
            data = res.get("data", {}) or {}
            files = data.get("files", [])
            return {
                "status": "success",
                "logs": f"Found {len(files)} file(s) matching {pattern}.",
                "executed": {"pattern": pattern, "count": len(files)},
                "data": files,
            }
        return {"status": "error", "logs": res.get("logs", "Find failed."), "executed": {"pattern": pattern}}

    def project_tree(self, max_depth: int = 2, max_entries: int = 300) -> dict:
        """Return a compact project tree via the Rust sandbox."""
        params = {
            "max_depth": max_depth,
            "max_entries": max_entries,
        }
        res = self._invoke_rust_sandbox("project_tree", False, params)
        if res.get("success"):
            data = res.get("data", {}) or {}
            tree = data.get("tree", [])
            return {
                "status": "success",
                "logs": "\n".join(tree),
                "executed": {"max_depth": max_depth, "entries": len(tree)},
                "data": tree,
            }
        return {"status": "error", "logs": res.get("logs", "Tree failed."), "executed": {"max_depth": max_depth}}

    def bash_exec(self, command: str, timeout: int = DEFAULT_TOOL_TIMEOUT_SECONDS) -> dict:
        """Execute a bounded shell command in the secure Rust Sandbox."""
        params = {"command": command, "timeout_seconds": timeout}
        res = self._invoke_rust_sandbox("execute_command", True, params)
        
        if not res.get("success"):
            return {"status": "error", "logs": res.get("logs", "Blocked."), "executed": {"command": command}}
        
        # Parse data from the Rust sandbox response
        data = res.get("data") or {}
        stdout = data.get("stdout", "")
        stderr = data.get("stderr", "")
        exit_code = data.get("exit_code", -1)
        
        # Combine stdout and logs for backward compatibility
        combined_logs = stdout
        if stderr:
            combined_logs += "\n" + stderr
        
        return {
            "status": "success" if exit_code == 0 else "error",
            "logs": combined_logs,
            "stdout": stdout,
            "stderr": stderr,
            "executed": {
                "command": command,
                "return_code": exit_code,
                "workspace": str(self.base_path),
            }
        }

    def scaffold_website(self, project_dir: str = ".", title: str = "AELVO App") -> dict:
        """Create a small working static website scaffold inside the workspace jail."""
        try:
            files = {
                "index.html": (
                    "<!doctype html>\n<html lang=\"en\">\n<head>\n"
                    "  <meta charset=\"utf-8\">\n  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
                    f"  <title>{title}</title>\n  <link rel=\"stylesheet\" href=\"styles.css\">\n"
                    "</head>\n<body>\n  <main>\n"
                    f"    <h1>{title}</h1>\n    <p>AELVO static scaffold is online.</p>\n"
                    "  </main>\n  <script src=\"script.js\"></script>\n</body>\n</html>\n"
                ),
                "styles.css": "body { margin: 0; font-family: system-ui, sans-serif; background: #f7f7f2; color: #1f2933; }\nmain { max-width: 760px; margin: 12vh auto; padding: 24px; }\n",
                "script.js": "console.log('AELVO scaffold ready');\n",
            }
            written = []
            for name, content in files.items():
                rel_path = os.path.join(project_dir, name)
                res = self.write_atomic(rel_path, content)
                if res.get("status") == "success":
                    written.append(rel_path)
            return {
                "status": "success",
                "logs": f"Scaffolded {len(written)} website file(s).",
                "executed": {"project_dir": project_dir, "files": written},
            }
        except Exception as e:
            return {"status": "error", "logs": str(e), "executed": {"project_dir": project_dir}}

    def python_exec(self, script_path: str, timeout: int = 30):
        """Executes a python script within the secure Rust Sandbox."""
        try:
            safe_path = self._validate_path(script_path, read_only=False)
        except Exception as e:
            return {"status": "error", "logs": str(e), "executed": {"script": script_path, "workspace": "./workspace"}}
            
        rel_script = os.path.relpath(safe_path, self.base_path)
        params = {"command": f"python {rel_script}", "timeout_seconds": timeout}
        res = self._invoke_rust_sandbox("execute_command", True, params)
        
        # Exit code is now in data, not audit (Rust sandbox v2.0)
        data = res.get("data", {}) or {}
        exit_code = data.get("exit_code", -1)
        
        return {
            "status": "success" if exit_code == 0 else "error",
            "logs": res.get("logs", ""),
            "stdout": data.get("stdout", ""),
            "stderr": data.get("stderr", ""),
            "executed": {
                "script": script_path,
                "return_code": exit_code,
                "workspace": "./workspace"
            }
        }
