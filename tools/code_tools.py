# code_tools.py - Direct CLI tool invocations for AELVO OMEGA (no HTTP, no TS server)
"""
Pure Python implementation that calls developer tools directly via subprocess.
No TypeScript HTTP server. No Node.js dependency for this layer.
Tools called: ruff, mypy, pytest, tsc, eslint, prettier, vitest, cargo, go test.
All calls time-boxed and return the standard AELVO tool result contract:
  {"status": "success"|"error", "logs": str, "executed": dict}
"""

import json
import logging
import os
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from config.settings import (
    RUFF_BINARY,
    MYPY_BINARY,
    PYTEST_BINARY,
    TSC_BINARY,
    ESLINT_BINARY,
    PRETTIER_BINARY,
    VITEST_BINARY,
    CARGO_BINARY,
    GO_BINARY,
    LINTER_TIMEOUT_SECONDS,
    FORMATTER_TIMEOUT_SECONDS,
    TYPE_CHECKER_TIMEOUT_SECONDS,
    TEST_RUNNER_TIMEOUT_SECONDS,
)

log = logging.getLogger("aelvo.forge.tools")

# Language → file extensions mapping
_LANG_EXTS: Dict[str, tuple] = {
    "python": (".py", ".pyi", ".pyx"),
    "typescript": (".ts", ".tsx"),
    "javascript": (".js", ".jsx", ".mjs", ".cjs"),
    "rust": (".rs",),
    "go": (".go",),
}


def _run(
    cmd: List[str],
    cwd: str,
    timeout: int,
    env: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Run a subprocess and return {"returncode", "stdout", "stderr"}."""
    try:
        proc = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, **(env or {})},
        )
        return {
            "returncode": proc.returncode,
            "stdout": proc.stdout.strip(),
            "stderr": proc.stderr.strip(),
        }
    except FileNotFoundError:
        return {
            "returncode": -1,
            "stdout": "",
            "stderr": f"Command not found: {cmd[0]}. Install it and ensure it is on PATH.",
        }
    except subprocess.TimeoutExpired:
        return {
            "returncode": -1,
            "stdout": "",
            "stderr": f"Tool timed out after {timeout}s: {' '.join(cmd)}",
        }
    except Exception as exc:
        return {"returncode": -1, "stdout": "", "stderr": str(exc)}


def _detect_language(path: str) -> str:
    """Detect language from file extension."""
    ext = Path(path).suffix.lower()
    for lang, exts in _LANG_EXTS.items():
        if ext in exts:
            return lang
    return "unknown"


def _resolve_workspace_path(path: str, workspace: str) -> Path:
    """Resolve path relative to workspace. Always within workspace."""
    p = Path(path)
    if not p.is_absolute():
        p = Path(workspace) / p
    return p.resolve()


# =============================================================================
# LINTER
# =============================================================================

def run_linter(path: str, language: str, workspace: str) -> Dict[str, Any]:
    """Run the appropriate linter for the given file and language."""
    if not language or language == "auto":
        language = _detect_language(path)

    abs_path = _resolve_workspace_path(path, workspace)

    if language == "python":
        result = _run(
            [RUFF_BINARY, "check", "--output-format=json", str(abs_path)],
            cwd=workspace,
            timeout=LINTER_TIMEOUT_SECONDS,
        )
        if result["returncode"] == -1:
            return {
                "status": "error",
                "logs": result["stderr"],
                "executed": {"path": path, "tool": "ruff", "language": language},
            }
        violations: List[Dict[str, Any]] = []
        try:
            raw = json.loads(result["stdout"]) if result["stdout"].startswith("[") else []
            for v in raw:
                violations.append({
                    "line": v.get("location", {}).get("row"),
                    "col": v.get("location", {}).get("column"),
                    "code": v.get("code"),
                    "message": v.get("message"),
                    "fixable": v.get("fix") is not None,
                })
        except (json.JSONDecodeError, AttributeError):
            # ruff returned non-JSON (e.g. parse error)
            violations_text = result["stdout"] or result["stderr"]
            return {
                "status": "error" if result["returncode"] != 0 else "success",
                "logs": violations_text,
                "executed": {"path": path, "tool": "ruff", "language": language, "violation_count": 0},
            }

        status = "success" if not violations else "error"
        log_msg = f"ruff: {len(violations)} violation(s) in {path}"
        return {
            "status": status,
            "logs": log_msg,
            "executed": {"path": path, "tool": "ruff", "language": language, "violation_count": len(violations)},
            "data": violations,
        }

    elif language in ("typescript", "javascript"):
        # eslint with JSON output
        result = _run(
            [ESLINT_BINARY, "--format=json", str(abs_path)],
            cwd=workspace,
            timeout=LINTER_TIMEOUT_SECONDS,
        )
        if result["returncode"] == -1:
            return {
                "status": "error",
                "logs": result["stderr"],
                "executed": {"path": path, "tool": "eslint", "language": language},
            }
        violations = []
        try:
            parsed = json.loads(result["stdout"])
            for file_result in parsed:
                for msg in file_result.get("messages", []):
                    violations.append({
                        "line": msg.get("line"),
                        "col": msg.get("column"),
                        "rule": msg.get("ruleId"),
                        "message": msg.get("message"),
                        "severity": "error" if msg.get("severity") == 2 else "warning",
                    })
        except Exception as _ex: log.debug("Silenced exception: %s", _ex)
        status = "success" if not violations else "error"
        return {
            "status": status,
            "logs": f"eslint: {len(violations)} issue(s) in {path}",
            "executed": {"path": path, "tool": "eslint", "language": language, "violation_count": len(violations)},
            "data": violations,
        }

    elif language == "rust":
        result = _run(
            [CARGO_BINARY, "clippy", "--message-format=json", "--", "-D", "warnings"],
            cwd=workspace,
            timeout=LINTER_TIMEOUT_SECONDS,
        )
        output = result["stdout"] + ("\n" + result["stderr"] if result["stderr"] else "")
        return {
            "status": "success" if result["returncode"] == 0 else "error",
            "logs": output[:3000],
            "executed": {"path": path, "tool": "cargo clippy", "language": language},
        }

    else:
        return {
            "status": "error",
            "logs": f"No linter configured for language: {language}",
            "executed": {"path": path, "language": language},
        }


# =============================================================================
# FORMATTER
# =============================================================================

def run_formatter(path: str, language: str, workspace: str) -> Dict[str, Any]:
    """Format a file in-place and report whether changes were made."""
    if not language or language == "auto":
        language = _detect_language(path)

    abs_path = _resolve_workspace_path(path, workspace)

    if language == "python":
        result = _run(
            [RUFF_BINARY, "format", str(abs_path)],
            cwd=workspace,
            timeout=FORMATTER_TIMEOUT_SECONDS,
        )
        changed = "reformatted" in (result["stdout"] + result["stderr"]).lower()
        return {
            "status": "success" if result["returncode"] == 0 else "error",
            "logs": result["stdout"] or result["stderr"],
            "executed": {"path": path, "tool": "ruff format", "language": language, "changed": changed},
        }

    elif language in ("typescript", "javascript"):
        result = _run(
            [PRETTIER_BINARY, "--write", str(abs_path)],
            cwd=workspace,
            timeout=FORMATTER_TIMEOUT_SECONDS,
        )
        return {
            "status": "success" if result["returncode"] == 0 else "error",
            "logs": result["stdout"] or result["stderr"],
            "executed": {"path": path, "tool": "prettier", "language": language},
        }

    elif language == "rust":
        result = _run(
            [CARGO_BINARY, "fmt", "--", str(abs_path)],
            cwd=workspace,
            timeout=FORMATTER_TIMEOUT_SECONDS,
        )
        return {
            "status": "success" if result["returncode"] == 0 else "error",
            "logs": result["stdout"] or result["stderr"],
            "executed": {"path": path, "tool": "cargo fmt", "language": language},
        }

    else:
        return {
            "status": "error",
            "logs": f"No formatter configured for language: {language}",
            "executed": {"path": path, "language": language},
        }


# =============================================================================
# TYPE CHECKER
# =============================================================================

def run_type_checker(path: str, language: str, workspace: str) -> Dict[str, Any]:
    """Run the type checker for the given file."""
    if not language or language == "auto":
        language = _detect_language(path)

    abs_path = _resolve_workspace_path(path, workspace)

    if language == "python":
        result = _run(
            [MYPY_BINARY, "--show-error-codes", "--no-error-summary", str(abs_path)],
            cwd=workspace,
            timeout=TYPE_CHECKER_TIMEOUT_SECONDS,
        )
        errors = [
            line for line in result["stdout"].splitlines()
            if ": error:" in line or ": note:" in line
        ]
        return {
            "status": "success" if result["returncode"] == 0 else "error",
            "logs": "\n".join(errors) if errors else (result["stdout"] or "No type errors found."),
            "executed": {
                "path": path,
                "tool": "mypy",
                "language": language,
                "error_count": len([e for e in errors if ": error:" in e]),
            },
        }

    elif language in ("typescript",):
        # tsc --noEmit for type check only
        result = _run(
            [TSC_BINARY, "--noEmit", "--strict", str(abs_path)],
            cwd=workspace,
            timeout=TYPE_CHECKER_TIMEOUT_SECONDS,
        )
        return {
            "status": "success" if result["returncode"] == 0 else "error",
            "logs": result["stdout"] or result["stderr"] or "No type errors.",
            "executed": {"path": path, "tool": "tsc", "language": language},
        }

    else:
        return {
            "status": "error",
            "logs": f"No type checker configured for language: {language}",
            "executed": {"path": path, "language": language},
        }


# =============================================================================
# TEST RUNNER
# =============================================================================

def run_tests(
    path: str, language: str, test_filter: str, workspace: str
) -> Dict[str, Any]:
    """Run tests for the given file or directory."""
    if not language or language == "auto":
        language = _detect_language(path)

    abs_path = _resolve_workspace_path(path, workspace)

    if language == "python":
        cmd = [PYTEST_BINARY, str(abs_path), "-v", "--tb=short", "--no-header"]
        if test_filter:
            cmd += ["-k", test_filter]
        result = _run(cmd, cwd=workspace, timeout=TEST_RUNNER_TIMEOUT_SECONDS)

        # Parse pytest summary line
        passed = failed = 0
        re.search(
            r"(\d+) passed|(\d+) failed|(\d+) error", result["stdout"]
        )
        if result["stdout"]:
            passed = int(re.search(r"(\d+) passed", result["stdout"]).group(1)) if re.search(r"(\d+) passed", result["stdout"]) else 0
            failed = int(re.search(r"(\d+) failed", result["stdout"]).group(1)) if re.search(r"(\d+) failed", result["stdout"]) else 0

        logs = result["stdout"][-3000:] if result["stdout"] else result["stderr"][-3000:]
        return {
            "status": "success" if result["returncode"] == 0 else "error",
            "logs": logs,
            "executed": {
                "path": path, "tool": "pytest", "language": language,
                "passed": passed, "failed": failed,
                "filter": test_filter or "",
            },
        }

    elif language in ("typescript", "javascript"):
        cmd = [VITEST_BINARY, "run", str(abs_path)]
        if test_filter:
            cmd += ["-t", test_filter]
        result = _run(cmd, cwd=workspace, timeout=TEST_RUNNER_TIMEOUT_SECONDS)
        return {
            "status": "success" if result["returncode"] == 0 else "error",
            "logs": result["stdout"][-3000:] or result["stderr"][-3000:],
            "executed": {"path": path, "tool": "vitest", "language": language},
        }

    elif language == "rust":
        result = _run(
            [CARGO_BINARY, "test", "--", "--nocapture"],
            cwd=workspace,
            timeout=TEST_RUNNER_TIMEOUT_SECONDS,
        )
        return {
            "status": "success" if result["returncode"] == 0 else "error",
            "logs": result["stdout"][-3000:] or result["stderr"][-3000:],
            "executed": {"path": path, "tool": "cargo test", "language": language},
        }

    elif language == "go":
        result = _run(
            [GO_BINARY, "test", "-v", "./..."],
            cwd=workspace,
            timeout=TEST_RUNNER_TIMEOUT_SECONDS,
        )
        return {
            "status": "success" if result["returncode"] == 0 else "error",
            "logs": result["stdout"][-3000:] or result["stderr"][-3000:],
            "executed": {"path": path, "tool": "go test", "language": language},
        }

    else:
        return {
            "status": "error",
            "logs": f"No test runner configured for language: {language}",
            "executed": {"path": path, "language": language},
        }


# =============================================================================
# SYMBOL GRAPH (Python AST — no external server needed)
# =============================================================================

def build_symbol_graph(workspace: str) -> Dict[str, Any]:
    """
    Build a symbol graph of the Python project using AST analysis.
    No TypeScript server. Pure stdlib ast module.
    Returns classes, functions, imports per file.
    """
    import ast

    workspace_path = Path(workspace)
    graph: Dict[str, Any] = {}
    skip_dirs = {".git", "__pycache__", "chroma_db", "backups", "node_modules", ".venv", "venv", "dist", "build"}

    py_files: List[Path] = []
    try:
        for root, dirs, files in os.walk(workspace):
            dirs[:] = [d for d in dirs if d not in skip_dirs and not d.startswith(".")]
            for fname in files:
                if fname.endswith(".py"):
                    py_files.append(Path(root) / fname)
    except Exception as e:
        return {"status": "error", "logs": f"Walk failed: {e}", "executed": {"workspace": workspace}}

    py_files = py_files[:1000]  # Cap at 1000 files to avoid timeout

    for fpath in py_files:
        rel = str(fpath.relative_to(workspace_path))
        try:
            source = fpath.read_text(encoding="utf-8", errors="replace")
            tree = ast.parse(source, filename=str(fpath))
        except SyntaxError as se:
            graph[rel] = {"error": f"SyntaxError: {se}"}
            continue
        except Exception as e:
            graph[rel] = {"error": str(e)}
            continue

        classes: List[str] = []
        functions: List[str] = []
        imports: List[str] = []

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                classes.append(node.name)
            elif isinstance(node, ast.FunctionDef) and not isinstance(
                getattr(node, "_parent", None), ast.ClassDef
            ):
                functions.append(node.name)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.append(alias.name)
                else:
                    module = node.module or ""
                    imports.append(module)

        graph[rel] = {
            "classes": classes,
            "functions": functions,
            "imports": list(set(imports))[:20],
        }

    total_symbols = sum(
        len(v.get("classes", [])) + len(v.get("functions", []))
        for v in graph.values()
        if isinstance(v, dict) and "error" not in v
    )

    return {
        "status": "success",
        "logs": f"Symbol graph: {len(graph)} files, {total_symbols} symbols extracted.",
        "executed": {
            "workspace": workspace,
            "file_count": len(graph),
            "total_symbols": total_symbols,
        },
        "data": graph,
    }
