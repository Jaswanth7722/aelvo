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

    _LOCK_GUARD = threading.Lock()

    class FileLock:
        _locks: dict[str, threading.Lock] = {}

        def __init__(self, path: str):
            self.path = path
            with _LOCK_GUARD:
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

    def set_base_path(self, base_path: str) -> str:
        """Re-jail the filesystem to a new workspace root.

        All tool operations (read/write/edit/list/tree/bash/python) resolve
        against ``self.base_path`` on every call, so switching it here gives
        the agent direct folder/workspace access to the new directory — the
        same way CLI/web/desktop coding agents let you open a folder.

        Returns the resolved absolute path of the new root.
        """
        new_root = Path(base_path).resolve()
        if not new_root.exists():
            raise FileNotFoundError(f"Workspace folder does not exist: {new_root}")
        if not new_root.is_dir():
            raise NotADirectoryError(f"Not a folder: {new_root}")
        self.base_path = new_root
        return str(new_root)

    def _sandbox_available(self) -> bool:
        """True when the compiled Rust sandbox binary is present."""
        return os.path.exists(self._sandbox_binary_path())

    def _invoke_rust_sandbox(self, action: str, write_mode: bool, params: dict) -> dict:
        """Invoke the compiled Rust sandbox core binary with a JSON-RPC request.

        When the compiled binary is missing (fresh clone without a Rust
        toolchain, npm installs that don't ship the binary), every action
        transparently falls back to an equivalent pure-Python implementation
        (``_python_fallback``) that mirrors the Rust response shapes and
        enforces the same path jail + command policy, so the agent stays
        fully usable anywhere.
        """
        if not self._sandbox_available():
            return self._python_fallback(action, params)

        # Locate the compiled Rust sandbox executable
        binary_path = self._sandbox_binary_path()
        
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

    # ────────────────────────────────────────────────────────────────────────
    # Pure-Python sandbox fallback (used when the Rust binary is absent)
    # ────────────────────────────────────────────────────────────────────────

    # Mirror sandbox_core/src/policy.rs: only these command base names run.
    _PY_ALLOWED_COMMANDS = {
        "echo", "cat", "type", "dir", "more", "sort", "find", "findstr",
        "where", "which", "head", "tail", "wc", "tee",
        "ls", "pwd", "cd",
        "cp", "copy", "move", "ren", "mkdir", "rmdir",
        "touch", "chmod", "attrib",
        "python", "python3", "node", "npm", "npx", "pnpm", "yarn",
        "cargo", "rustc", "rustup", "gcc", "g++", "clang", "make",
        "cmake", "go", "deno", "bun", "pip", "pip3", "poetry",
        "git",
        "grep", "awk", "sed",
        "tar", "gzip", "gunzip", "zip", "unzip",
        "ping", "curl", "wget", "nslookup",
        "ps", "tasklist", "top", "htop",
        "cmd", "sh", "bash", "powershell",
        "xcopy", "robocopy", "rg", "ripgrep",
    }

    # Mirror sandbox_core/src/policy.rs blocked command patterns.
    _PY_BLOCKED_PATTERNS = (
        r"rm\s+-rf\s+/",
        r"rm\s+-rf\s+--no-preserve-root",
        r"mkfs\.",
        r"dd\s+if=",
        r":\(\)\s*\{",
        r">\s*/dev/sda",
        r"format\s+\w:\s*/q",
        r"del\s+/[fFsS].*\\\*\.\*",
        r"rd\s+/[sSqQ]\s+\\\\",
        r"shutdown\s+/[rsp]",
        r"taskkill\s+/[fF]\s+/[iI][mM]",
        r"reg\s+(delete|add)\s+",
        r"chmod\s+777\s+/",
        r"chown\s+-[Rr]\s+",
    )

    # Mirror sandbox_core/src/policy.rs shell-injection patterns.
    _PY_INJECTION_PATTERNS = (
        r"\$\(.*\)",
        r"`[^`]+`",
        r";\s*rm\s",
        r"\|\s*sh\b",
        r"\|\s*bash\b",
        r"\|\s*zsh\b",
        r">>\s*/etc/",
        r">\s*/etc/",
    )

    # Mirror sandbox_core/src/fs_jail.rs.
    _PY_SOURCE_EXTENSIONS = {
        "py", "ts", "tsx", "js", "jsx", "json", "md", "yaml", "yml",
        "toml", "rs", "go", "html", "css", "sql",
    }
    _PY_IGNORED_DIRS = {
        ".git", "__pycache__", "chroma_db", "backups", "node_modules",
        ".venv", "dist", "target", ".aelvo",
    }
    _PY_MAX_SEARCH_FILE_SIZE = 10 * 1024 * 1024

    def _python_fallback(self, action: str, params: dict) -> dict:
        """Pure-Python implementations of the sandbox actions.

        Returns the same response shapes the Rust binary produces so all
        callers are agnostic to which backend ran. Every file action routes
        through ``_validate_path_python`` (the existing jail mirror), and
        command execution enforces the same allow/block/injection policy.
        """
        try:
            if action in ("resolve_path",):
                resolved = self._validate_path_python(params.get("path", ""))
                return {
                    "success": True,
                    "data": {"resolved_path": str(resolved)},
                }
            if action == "read_file":
                safe = self._validate_path_python(params.get("path", ""))
                content = self._py_read_text(safe)
                return {"success": True, "data": {"content": content}}
            if action == "read_file_range":
                safe = self._validate_path_python(params.get("path", ""))
                start = max(1, int(params.get("start_line", 1)))
                end = max(start, int(params.get("end_line", 120)))
                lines = self._py_read_text(safe).splitlines()
                sliced = "\n".join(lines[start - 1:end])
                return {
                    "success": True,
                    "data": {
                        "content": sliced,
                        "start_line": start,
                        "end_line": min(end, len(lines)),
                    },
                }
            if action == "write_atomic":
                safe = self._validate_path_python(params.get("path", ""))
                safe.parent.mkdir(parents=True, exist_ok=True)
                content = params.get("content", "")
                tmp = safe.with_name(safe.name + ".aelvo_tmp")
                tmp.write_text(content, encoding="utf-8")
                tmp.replace(safe)
                return {"success": True, "logs": f"Wrote {safe.name}."}
            if action == "edit_file_block":
                safe = self._validate_path_python(params.get("path", ""))
                old = params.get("old_block", "")
                new = params.get("new_block", "")
                content = self._py_read_text(safe)
                if old not in content:
                    return {
                        "success": False,
                        "status": "error",
                        "logs": "Edit failed: old block not found in file.",
                    }
                content = content.replace(old, new, 1)
                tmp = safe.with_name(safe.name + ".aelvo_tmp")
                tmp.write_text(content, encoding="utf-8")
                tmp.replace(safe)
                return {
                    "success": True,
                    "logs": f"Updated {safe.name} and logged diff.",
                    "data": {"path": str(safe)},
                }
            if action == "delete_file":
                safe = self._validate_path_python(params.get("path", ""))
                if safe.is_file():
                    safe.unlink()
                return {"success": True, "logs": f"Deleted {safe.name}."}
            if action == "list_directory":
                safe = self._validate_path_python(params.get("path", "."))
                if not safe.is_dir():
                    return {"success": False, "status": "error", "logs": "Not a directory."}
                entries = sorted(
                    e.name for e in safe.iterdir() if not e.name.startswith(".aelvo")
                )
                return {
                    "success": True,
                    "data": {"path": str(safe), "entries": entries, "count": len(entries)},
                }
            if action == "grep_file":
                return self._py_grep(params)
            if action == "search_code":
                return self._py_search_code(params)
            if action == "find_files":
                return self._py_find_files(params)
            if action == "project_tree":
                return self._py_project_tree(params)
            if action in ("execute_command", "bash_exec"):
                return self._py_execute(params)
            return {
                "success": False,
                "status": "error",
                "logs": f"Unknown action '{action}'.",
            }
        except PermissionError as exc:
            return {
                "success": False,
                "status": "error",
                "logs": f"Path validation denied by sandbox: {exc}",
            }
        except Exception as exc:
            return {
                "success": False,
                "status": "error",
                "logs": f"Sandbox fallback error: {exc}",
            }

    def _py_read_text(self, safe: Path) -> str:
        """Read a text file with the 5 MB size cap (mirrors the Rust side)."""
        if safe.stat().st_size > self.MAX_FILE_SIZE:
            raise PermissionError(f"File exceeds size limit: {safe}")
        return safe.read_text(encoding="utf-8", errors="replace")

    def _py_grep(self, params: dict) -> dict:
        safe = self._validate_path_python(params.get("path", ""))
        if not safe.is_file():
            return {"success": False, "status": "error", "logs": "Not a file."}
        import re as _re

        pattern = params.get("pattern", "")
        case_sensitive = bool(params.get("case_sensitive", False))
        max_matches = int(params.get("max_matches", 100))
        flags = 0 if case_sensitive else _re.IGNORECASE
        try:
            regex = _re.compile(pattern, flags)
        except _re.error as exc:
            return {"success": False, "status": "error", "logs": f"Invalid regex: {exc}"}
        matches = []
        content = self._py_read_text(safe)
        for line_no, line in enumerate(content.splitlines(), start=1):
            if len(matches) >= max_matches:
                break
            if regex.search(line):
                matches.append({"line": line_no, "text": line})
        return {
            "success": True,
            "data": {
                "matches": matches,
                "match_count": len(matches),
                "path": str(safe),
            },
        }

    def _py_search_code(self, params: dict) -> dict:
        import re as _re

        query = params.get("query", "")
        max_matches = int(params.get("max_matches", 100))
        try:
            regex = _re.compile(_re.escape(query), _re.IGNORECASE)
        except _re.error:
            return {"success": False, "status": "error", "logs": "Invalid query."}
        matches = []
        root = self.base_path.resolve()
        for current, dirs, files in os.walk(root):
            dirs[:] = [
                d for d in dirs
                if d not in self._PY_IGNORED_DIRS and not d.startswith(".aelvo")
            ]
            for name in files:
                if len(matches) >= max_matches:
                    break
                ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
                if ext not in self._PY_SOURCE_EXTENSIONS:
                    continue
                path = Path(current) / name
                try:
                    if path.stat().st_size > self._PY_MAX_SEARCH_FILE_SIZE:
                        continue
                    content = path.read_text(encoding="utf-8", errors="replace")
                except Exception:
                    continue
                rel = os.path.relpath(path, root)
                for line_no, line in enumerate(content.splitlines(), start=1):
                    if len(matches) >= max_matches:
                        break
                    if regex.search(line):
                        matches.append({"file": rel, "line": line_no, "text": line})
        return {
            "success": True,
            "data": {"matches": matches, "match_count": len(matches), "query": query},
        }

    def _py_find_files(self, params: dict) -> dict:
        import fnmatch as _fnmatch

        pattern = params.get("pattern", "*")
        max_results = int(params.get("max_results", 200))
        root = self.base_path.resolve()
        files = []
        for current, dirs, names in os.walk(root):
            dirs[:] = [
                d for d in dirs
                if d not in self._PY_IGNORED_DIRS and not d.startswith(".aelvo")
            ]
            for name in names:
                if len(files) >= max_results:
                    break
                if _fnmatch.fnmatch(name, pattern) or _fnmatch.fnmatch(name, f"*{pattern}*"):
                    files.append(os.path.relpath(os.path.join(current, name), root))
        return {
            "success": True,
            "data": {"files": files, "count": len(files), "pattern": pattern},
        }

    def _py_project_tree(self, params: dict) -> dict:
        max_depth = int(params.get("max_depth", 2))
        max_entries = int(params.get("max_entries", 300))
        root = self.base_path.resolve()
        lines = []
        entry_count = 0

        def _walk(directory: Path, depth: int) -> None:
            nonlocal entry_count
            if entry_count >= max_entries:
                return
            try:
                entries = sorted(directory.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
            except Exception:
                return
            dirs = [e for e in entries if e.is_dir()]
            files = [e for e in entries if not e.is_dir()]
            indent = "  " * (depth + 1)
            for entry in dirs + files:
                if entry_count >= max_entries:
                    break
                if entry.name in self._PY_IGNORED_DIRS or entry.name.startswith(".aelvo"):
                    continue
                lines.append(f"{indent}{entry.name}/" if entry.is_dir() else f"{indent}{entry.name}")
                entry_count += 1
                if entry.is_dir() and depth + 1 < max_depth:
                    _walk(entry, depth + 1)

        if root.exists():
            lines.append(f"{root.name}/")
            _walk(root, 0)
        return {
            "success": True,
            "data": {"tree": lines, "entry_count": entry_count, "max_depth": max_depth},
        }

    def _py_execute(self, params: dict) -> dict:
        """Run a command with the Rust policy (allowlist + block + injection)."""
        import re as _re

        command = (params.get("command", "") or "").strip()
        if not command:
            return {"success": False, "status": "error", "logs": "Empty command."}
        lower = command.lower()
        for pat in self._PY_BLOCKED_PATTERNS:
            if _re.search(pat, command, _re.IGNORECASE):
                return {
                    "success": False,
                    "status": "error",
                    "logs": f"Command contains blocked pattern: {pat}",
                }
        for pat in self._PY_INJECTION_PATTERNS:
            if _re.search(pat, command, _re.IGNORECASE):
                return {
                    "success": False,
                    "status": "error",
                    "logs": f"Command contains injection pattern: {pat}",
                }
        base = command.split()[0].split("\\")[-1].split("/")[-1]
        if base not in self._PY_ALLOWED_COMMANDS:
            return {
                "success": False,
                "status": "error",
                "logs": f"Command '{base}' not in the allowed command list.",
            }
        timeout = float(params.get("timeout_seconds", DEFAULT_TOOL_TIMEOUT_SECONDS))
        try:
            result = subprocess.run(
                command,
                shell=True,
                cwd=str(self.base_path),
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            return {"success": False, "status": "timeout", "logs": "Command timed out."}
        except Exception as exc:
            return {"success": False, "status": "error", "logs": str(exc)}
        exit_code = result.returncode if result.returncode is not None else -1
        return {
            "success": True,
            "data": {
                "stdout": result.stdout or "",
                "stderr": result.stderr or "",
                "exit_code": exit_code,
            },
            "logs": f"Command completed (exit code: {exit_code}).",
        }

    def _validate_path(self, user_path: str, read_only: bool = False) -> Path:
        """Validate and resolve a path through the Rust Sandbox's path jailing.

        Security model:
        - If the Rust sandbox binary is present and answers, its decision is
          authoritative: policy denials (traversal, containment violations,
          unknown actions) fail CLOSED and raise PermissionError.
        - If the binary is MISSING entirely (fresh clone without a Rust
          toolchain), fall back to a pure-Python jail check that mirrors the
          Rust policy (reject ``..`` / URL-encoded traversal, null bytes, and
          anything outside the workspace jail). This keeps the agent usable
          on machines where the sandbox was never compiled.
        """
        if not os.path.exists(self._sandbox_binary_path()):
            return self._validate_path_python(user_path)

        params = {"path": user_path}
        res = self._invoke_rust_sandbox("resolve_path", False, params)
        if res.get("success"):
            data = res.get("data", {}) or {}
            resolved = data.get("resolved_path")
            if resolved:
                if resolved.startswith("\\\\?\\"):
                    resolved = resolved[4:]
                return Path(resolved)

        # Fail closed: if the sandbox binary is present but validation fails,
        # we deny the operation rather than silently falling back.
        msg = res.get("logs", "Sandbox unavailable")
        raise PermissionError(f"Path validation denied by sandbox: {msg}")

    def _sandbox_binary_path(self) -> str:
        """Absolute path of the compiled Rust sandbox executable."""
        return os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "sandbox_core", "target", "release", "sandbox_core.exe",
        )

    def _validate_path_python(self, user_path: str) -> Path:
        """Pure-Python path jail used only when the Rust sandbox is absent.

        Mirrors sandbox_core's policy for path validation:
        - reject empty/whitespace-only paths
        - reject ``..`` and URL-encoded traversal (``%2e%2e``, ``..%2f``, …)
        - reject null bytes
        - the resolved path must live inside the workspace jail
        """
        if not user_path or not user_path.strip():
            raise PermissionError(
                f"Path validation denied by sandbox: Path is empty: {user_path!r}"
            )
        lower = user_path.lower()
        if (
            ".." in lower
            or "%2e%2e" in lower
            or "..%2f" in lower
            or "..%5c" in lower
        ):
            raise PermissionError(
                f"Path validation denied by sandbox: Path traversal detected in {user_path!r}"
            )
        if "\0" in user_path:
            raise PermissionError(
                "Path validation denied by sandbox: Path contains null byte."
            )

        candidate = Path(user_path)
        if not candidate.is_absolute():
            candidate = self.base_path / candidate
        try:
            resolved = candidate.resolve()
        except Exception as exc:
            raise PermissionError(
                f"Path validation denied by sandbox: cannot resolve {user_path!r}: {exc}"
            )

        # Containment: the resolved path must stay inside the jail.
        jail = self.base_path.resolve()
        if not (
            resolved == jail or jail in resolved.parents
        ):
            raise PermissionError(
                f"Path validation denied by sandbox: {resolved} escapes the jail {jail}"
            )
        return resolved

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
        # Guard: reading a directory is almost always a model mistake (weak
        # local models call read_file on '.' instead of list_files). Fail
        # with a clear, actionable error instead of a confusing backend one.
        try:
            safe = self._validate_path_python(path)
        except PermissionError as exc:
            return {"status": "error", "logs": str(exc)}
        if safe.is_dir():
            return {
                "status": "error",
                "logs": (
                    f"'{path}' is a directory — use list_files to list its "
                    "contents, not read_file."
                ),
            }
        params = {"path": path}
        res = self._invoke_rust_sandbox("read_file", False, params)
        if res.get("success"):
            data = res.get("data", {}) or {}
            content = data.get("content", "")
            return {"status": "success", "data": content}
        return {"status": "error", "logs": res.get("logs", "Read failed.")}

    def read_file_range(self, path: str, start_line: int = 1, end_line: int = 120) -> dict:
        # Same directory guard as read_file: line ranges only make sense on
        # real files, and a weak model should get a pointer to list_files.
        try:
            safe = self._validate_path_python(path)
        except PermissionError as exc:
            return {"status": "error", "logs": str(exc), "executed": {"path": path}}
        if safe.is_dir():
            return {
                "status": "error",
                "logs": (
                    f"'{path}' is a directory — use list_files to list its "
                    "contents, not read_file_range."
                ),
                "executed": {"path": path},
            }
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
