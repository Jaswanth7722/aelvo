#!/usr/bin/env python3
"""JSONL worker for the TypeScript AELVO runtime.

The TypeScript side owns process communication and request correlation. This
worker owns Python-native specialist and tool execution. Every response is a
single JSON line with the same request id.
"""

from __future__ import annotations

import json
import os
import platform
import sys
import traceback
from pathlib import Path
from typing import Any, Callable, Dict

from specialists import SPECIALIST_REGISTRY, get_specialist
from tools.code_tools import (
    build_symbol_graph,
    run_formatter,
    run_linter,
    run_tests,
    run_type_checker,
)
from tools.diagram_tools import (
    generate_mermaid_flowchart,
    generate_mermaid_mindmap,
    validate_mermaid,
)
from tools.git_tools import (
    detect_merge_conflicts,
    generate_commit_message,
    generate_pr_description,
    get_git_state,
)
from tools.research_tools import build_wiki_entry, decompose_query, rank_source_credibility
from tools.security_tools import scan_for_secrets, scan_for_vulnerabilities, simulate_attack


PROJECT_ROOT = Path(__file__).resolve().parent


def _json(status: str, logs: str, executed: Dict[str, Any] | None = None, data: Any = None) -> Dict[str, Any]:
    result = {"status": status, "logs": logs, "executed": executed or {}}
    if data is not None:
        result["data"] = data
    return result


def _workspace(payload: Dict[str, Any]) -> Path:
    raw = payload.get("workspace") or str(PROJECT_ROOT)
    workspace = Path(raw).resolve()
    if not str(workspace).startswith(str(PROJECT_ROOT)):
        raise PermissionError(f"Workspace must stay under {PROJECT_ROOT}")
    return workspace


def _inspect_project(payload: Dict[str, Any]) -> Dict[str, Any]:
    workspace = _workspace(payload)
    ignored = {".git", "__pycache__", "node_modules", "chroma_db", "backups", ".venv", "dist"}
    ext_counts: Dict[str, int] = {}
    files = []
    for root, dirs, names in os.walk(workspace):
        dirs[:] = [d for d in dirs if d not in ignored]
        for name in names:
            path = Path(root) / name
            rel = path.relative_to(workspace).as_posix()
            files.append(rel)
            ext_counts[path.suffix.lower() or "<none>"] = ext_counts.get(path.suffix.lower() or "<none>", 0) + 1
            if len(files) >= int(payload.get("max_files", 400)):
                break
        if len(files) >= int(payload.get("max_files", 400)):
            break

    language_map = {
        ".py": "python",
        ".ts": "typescript",
        ".tsx": "typescript",
        ".js": "javascript",
        ".jsx": "javascript",
        ".rs": "rust",
        ".go": "go",
    }
    primary_ext = max(ext_counts, key=ext_counts.get) if ext_counts else ""
    primary_language = language_map.get(primary_ext, "unknown")
    return _json(
        "success",
        f"Inspected {len(files)} file(s). Primary language: {primary_language}.",
        {"workspace": str(workspace), "file_count": len(files)},
        {"files": files, "extension_counts": ext_counts, "primary_language": primary_language},
    )


def _classify(payload: Dict[str, Any]) -> Dict[str, Any]:
    task = str(payload.get("task") or "")
    if not task.strip():
        return _json("error", "Classification requires a non-empty task.", {"task": task})
    context = {"detected_language": payload.get("detected_language", ""), "memory_engine": None}
    scores = []
    for name, specialist in SPECIALIST_REGISTRY.items():
        score = specialist.compute_activation_score(task, context)
        scores.append({"name": name, "score": round(score, 3), "threshold": specialist.activation_threshold})
    active = [item["name"] for item in scores if item["score"] >= item["threshold"]]
    if not active:
        active = ["HERMES"]
    return _json("success", f"Activated specialists: {', '.join(active)}", {"task": task}, {"active": active, "scores": scores})


def _run_tool(payload: Dict[str, Any]) -> Dict[str, Any]:
    tool = str(payload.get("tool") or "")
    args = payload.get("args") or {}
    if not isinstance(args, dict):
        return _json("error", "Tool args must be an object.", {"tool": tool})
    workspace = _workspace(payload)

    registry: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {
        "project.inspect": lambda a: _inspect_project({**payload, **a}),
        "specialists.classify": lambda a: _classify(a),
        "security.scan_secrets": lambda a: scan_for_secrets(str(a.get("path", "")), str(workspace)),
        "security.scan_vulnerabilities": lambda a: scan_for_vulnerabilities(str(a.get("path", "")), str(workspace)),
        "security.simulate_attack": lambda a: simulate_attack(
            str(a.get("vulnerability_type", "")),
            str(a.get("path", "")),
            str(a.get("vulnerable_code", "")),
        ),
        "diagram.validate_mermaid": lambda a: _mermaid_result(str(a.get("diagram", ""))),
        "diagram.flowchart": lambda a: generate_mermaid_flowchart(a.get("components", []), a.get("connections", [])),
        "diagram.mindmap": lambda a: generate_mermaid_mindmap(a.get("structure", {})),
        "research.decompose": lambda a: decompose_query(str(a.get("query", ""))),
        "research.rank_source": lambda a: rank_source_credibility(str(a.get("url", ""))),
        "research.wiki": lambda a: build_wiki_entry(str(a.get("topic", "")), a.get("synthesis_data", []), a.get("contradictions")),
        "git.state": lambda a: get_git_state(str(workspace)),
        "git.commit_message": lambda a: generate_commit_message(str(workspace)),
        "git.merge_conflicts": lambda a: detect_merge_conflicts(str(workspace)),
        "git.pr_description": lambda a: generate_pr_description(str(a.get("base_branch", "main")), str(a.get("head_branch", "HEAD")), str(workspace)),
        "code.linter": lambda a: run_linter(str(a.get("path", "")), str(workspace), a.get("language")),
        "code.formatter": lambda a: run_formatter(str(a.get("path", "")), str(workspace), a.get("language")),
        "code.typecheck": lambda a: run_type_checker(str(a.get("path", "")), str(workspace), a.get("language")),
        "code.tests": lambda a: run_tests(str(a.get("target", "")), str(workspace), a.get("language")),
        "code.symbol_graph": lambda a: build_symbol_graph(str(workspace)),
    }
    handler = registry.get(tool)
    if handler is None:
        return _json("error", f"Unknown tool: {tool}", {"tool": tool, "available": sorted(registry)})
    return handler(args)


def _mermaid_result(diagram: str) -> Dict[str, Any]:
    ok, message = validate_mermaid(diagram)
    return _json("success" if ok else "error", message, {"diagram_length": len(diagram)})


def _handle(request: Dict[str, Any]) -> Dict[str, Any]:
    req_type = request.get("type")
    payload = request.get("payload") or {}
    if not isinstance(payload, dict):
        return _json("error", "Payload must be an object.", {"type": req_type})

    if req_type == "health.check":
        return _json(
            "success",
            "Python worker is online.",
            {"project_root": str(PROJECT_ROOT)},
            {
                "python": sys.version.split()[0],
                "platform": platform.platform(),
                "specialists": sorted(SPECIALIST_REGISTRY.keys()),
            },
        )
    if req_type == "project.inspect":
        return _inspect_project(payload)
    if req_type == "specialists.classify":
        return _classify(payload)
    if req_type == "tool.run":
        return _run_tool(payload)
    return _json("error", f"Unknown request type: {req_type}", {"type": req_type})


def _write_response(request_id: str, ok: bool, result: Dict[str, Any]) -> None:
    sys.stdout.write(json.dumps({"id": request_id, "ok": ok, "result": result}, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def main() -> int:
    for line in sys.stdin:
        if not line.strip():
            continue
        request_id = "unknown"
        try:
            request = json.loads(line)
            request_id = str(request.get("id") or "unknown")
            _write_response(request_id, True, _handle(request))
        except Exception as exc:
            _write_response(
                request_id,
                False,
                _json("error", f"{type(exc).__name__}: {exc}", {"traceback": traceback.format_exc(limit=8)}),
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
