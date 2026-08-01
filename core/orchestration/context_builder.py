"""context_builder.py — Shared context assembly for AELVO OMEGA."""
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List

from config.settings import CACHE_TREE_EXPIRY_SECONDS
from specialists import get_specialist

log = logging.getLogger("aelvo.context_builder")


class ContextBuilder:
    """Builds and caches shared execution context for all specialists.

    Responsibilities:
    - Cache and serve workspace tree snapshots
    - Assemble shared context with constraints, state, user profile, and cross-specialist memory
    - Inject cross-specialist memory (SENTINEL → FORGE, ARCHITECT → FORGE)
    """

    def __init__(self, memory_engine=None, base_path="", kernel=None,
                 runtime_registry=None, provider_runtime=None, user_manager=None):
        self.memory_engine = memory_engine
        self.base_path = base_path
        self.kernel = kernel
        self.runtime_registry = runtime_registry
        self.provider_runtime = provider_runtime
        self.user_manager = user_manager
        self.tree_cache = ""
        self.tree_cache_time = 0.0

    def get_workspace_tree(self) -> str:
        """Retrieves and caches a structural snapshot of the workspace directory."""
        now = time.time()
        if now - self.tree_cache_time < CACHE_TREE_EXPIRY_SECONDS and self.tree_cache:
            return self.tree_cache

        tree_lines = ["WORKSPACE STRUCTURE:"]
        try:
            for root, dirs, files in os.walk(self.base_path):
                dirs[:] = [d for d in dirs if d not in (
                    ".git", "__pycache__", "chroma_db", "backups",
                    "node_modules", ".venv", "venv",
                )]
                level = Path(root).relative_to(self.base_path)
                indent = "  " * len(level.parts)
                if root != str(self.base_path):
                    tree_lines.append(f"{indent}\N{open file folder} {os.path.basename(root)}/")
                sub_indent = "  " * (len(level.parts) + 1)
                for f in sorted(files):
                    if not f.endswith(".lock"):
                        tree_lines.append(f"{sub_indent}\N{page facing up} {f}")
        except Exception as e:
            tree_lines.append(f"  Error reading tree: {str(e)}")

        self.tree_cache = "\n".join(tree_lines[:100])
        self.tree_cache_time = now
        return self.tree_cache

    def build_shared_context(self, task: str, active_specialists: List[str]) -> Dict[str, Any]:
        """Synthesize constraints, tree, user profile, and cross-specialist memory."""
        project = getattr(self.memory_engine, "project_name", "") if self.memory_engine else ""

        # 1. Anchor constraints
        constraints: Dict[str, Any] = {}
        if self.memory_engine:
            try:
                constraints = self.memory_engine.parse_anchor() or {}
            except Exception as e:
                log.debug("Failed to parse anchor: %s", e)

        # 2. SQLite state
        state: Dict[str, str] = {}
        if self.memory_engine:
            try:
                rows = self.memory_engine.db.execute(
                    "SELECT key, value FROM state WHERE key NOT LIKE 'runtime:%'"
                ).fetchall()
                state = {r[0]: r[1] for r in rows}
            except Exception as e:
                log.debug("Failed to query state: %s", e)

        # 3. Capability prompt
        capability_prompt = ""
        if self.runtime_registry:
            capability_prompt = self.runtime_registry.to_prompt_injection()

        # 4. Provider runtime prompt
        if self.provider_runtime:
            provider_prompt = (
                "PROVIDER RUNTIME:\n"
                f"  Registered Providers: {len(self.provider_runtime.provider_configs)}\n"
                f"  Models Available: {len(self.provider_runtime.model_registry.list_models())}\n"
                f"  Providers with Credentials: "
                f"{len([p for p in self.provider_runtime.provider_configs if self.provider_runtime.has_credentials(p)])}\n"
                f"  Active Providers: {self.provider_runtime.get_active_providers()}\n"
            )
        else:
            provider_prompt = "PROVIDER RUNTIME: Not initialized\n"

        # 5. User profile
        user_profile_prompt = ""
        if self.user_manager:
            user_profile_prompt = self.user_manager.build_prompt_injection(project)

        # 6. Cross-specialist memory aggregation
        cross_memory: Dict[str, Any] = {}
        for name in active_specialists:
            spec = get_specialist(name)
            if not spec:
                continue
            try:
                mem_ctx = spec.build_memory_context(task, self.memory_engine) or {}
                for k, v in mem_ctx.items():
                    if k in cross_memory and isinstance(v, list) and isinstance(cross_memory[k], list):
                        seen = {json.dumps(x, sort_keys=True, default=str) for x in cross_memory[k]}
                        for item in v:
                            sig = json.dumps(item, sort_keys=True, default=str)
                            if sig not in seen:
                                cross_memory[k].append(item)
                                seen.add(sig)
                    else:
                        cross_memory[k] = v
            except NotImplementedError as _ex:
                log.warning("Silenced exception: %s", _ex)
            except Exception as e:
                log.warning("Memory context failed for %s: %s", name, e)

        # 7. SENTINEL → FORGE cross-injection
        sentinel_rules: List[Dict[str, Any]] = []
        if self.memory_engine and self.memory_engine.memory_collection:
            try:
                res = self.memory_engine.memory_collection.query(
                    query_texts=[task], n_results=5,
                    where={"type": "security_rule", "project": project},
                    include=["documents", "metadatas", "distances"],
                )
                if res.get("ids") and res["ids"][0]:
                    for doc, dist in zip(res["documents"][0], res["distances"][0]):
                        score = round(max(0.0, 1.0 - float(dist)), 3)
                        if score >= 0.15:
                            sentinel_rules.append({"doc": doc, "score": score})
            except Exception as e:
                log.debug("Failed to query security rules: %s", e)

        # 8. ARCHITECT ADRs → all specialists
        arch_decisions: List[Dict[str, Any]] = []
        if self.memory_engine and self.memory_engine.memory_collection:
            try:
                res = self.memory_engine.memory_collection.query(
                    query_texts=[task], n_results=5,
                    where={"type": "system_decision", "project": project},
                    include=["documents", "metadatas", "distances"],
                )
                if res.get("ids") and res["ids"][0]:
                    for doc, dist in zip(res["documents"][0], res["distances"][0]):
                        score = round(max(0.0, 1.0 - float(dist)), 3)
                        if score >= 0.15:
                            arch_decisions.append({"doc": doc, "score": score})
            except Exception as e:
                log.debug("Failed to query ADRs: %s", e)

        # Merge cross-injected data
        if sentinel_rules:
            existing = cross_memory.get("security_rules", [])
            seen = {json.dumps(x, sort_keys=True, default=str) for x in existing}
            for r in sentinel_rules:
                sig = json.dumps(r, sort_keys=True, default=str)
                if sig not in seen:
                    existing.append(r)
                    seen.add(sig)
            cross_memory["security_rules"] = existing

        if arch_decisions:
            existing = cross_memory.get("system_decisions", [])
            seen = {json.dumps(x, sort_keys=True, default=str) for x in existing}
            for r in arch_decisions:
                sig = json.dumps(r, sort_keys=True, default=str)
                if sig not in seen:
                    existing.append(r)
                    seen.add(sig)
            cross_memory["system_decisions"] = existing

        return {
            "project": project,
            "task": task,
            "budget": 30,
            "constraints": constraints,
            "state": state,
            "workspace_path": self.base_path,
            "tree_snapshot": self.get_workspace_tree(),
            "user_profile_prompt": user_profile_prompt,
            "capability_prompt": capability_prompt,
            "provider_prompt": provider_prompt,
            "env_prompt": "",
            "provider_runtime": self.provider_runtime,
            "cross_memory": cross_memory,
            "signals": {},
            "active_specialists": list(active_specialists),
            "security_rules": cross_memory.get("security_rules", []),
            "system_decisions": cross_memory.get("system_decisions", []),
            **cross_memory,
        }
