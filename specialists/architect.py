# architect.py - ARCHITECT Master Planning Intelligence for AELVO OMEGA
#
# ARCHITECT is the authoritative strategic brain of the system.
# It produces structured, dependency-aware, risk-assessed, verifiable,
# recoverable execution plans that every other specialist depends on.
#
# Integration:
#   - Uses ArchitectOrchestrator (runtime_next/plan/architect.py) for planning
#   - Uses ArchitectPlan types (runtime_next/plan/architect_types.py) for output
#   - Uses RepoIntelligenceEngine for repository-aware planning
#   - Uses ForgeMemory and CognitiveEngine for context-aware planning
#   - Uses self-critique to validate plans before finalizing
#   - Persists plans and ADRs to memory for future reference

import hashlib
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from specialists.base import BaseSpecialist
from tools.diagram_tools import validate_mermaid, generate_mermaid_mindmap

from cognition.architect_decision import (
    ArchitectDecision,
    ArchitectDecisionOutcome,
    ModeSelectionCriteria,
)

log = logging.getLogger("aelvo.specialists.architect")


# ===========================================================================
# Requirement Decomposition (existing, enhanced)
# ===========================================================================

_MUST_HINTS = ("must", "required", "mandatory", "critical", "shall")
_SHOULD_HINTS = ("should", "needs to", "expected to", "important")
_COULD_HINTS = ("could", "nice to have", "optional", "may", "would be nice")
_NFR_HINTS = (
    "performance", "latency", "throughput", "scalab", "availab", "uptime",
    "secur", "compliance", "gdpr", "pci", "audit", "observab", "cost",
    "reliab", "consistency", "concurren"
)


def decompose_requirements(task: str) -> Dict[str, List[str]]:
    """Decomposes a free-form requirement string into MoSCoW + non-functional buckets.

    Returns: {must, should, could, non_functional, ambiguities}
    """
    if not task or not isinstance(task, str):
        return {"must": [], "should": [], "could": [], "non_functional": [], "ambiguities": []}

    fragments: List[str] = []
    for piece in re.split(r"(?:\n|\r|\.\s|;|,\s+and\s+|,\s+but\s+|\u2022|\*\s+|-\s+|\d+\.\s+)", task):
        clean = piece.strip(" \t-*\u2022.")
        if clean and len(clean) > 4:
            fragments.append(clean)

    must: List[str] = []
    should: List[str] = []
    could: List[str] = []
    non_functional: List[str] = []
    ambiguities: List[str] = []

    for frag in fragments:
        low = frag.lower()
        is_nfr = any(h in low for h in _NFR_HINTS)
        if is_nfr:
            non_functional.append(frag)
            continue
        if any(h in low for h in _MUST_HINTS):
            must.append(frag)
        elif any(h in low for h in _SHOULD_HINTS):
            should.append(frag)
        elif any(h in low for h in _COULD_HINTS):
            could.append(frag)
        else:
            if low.endswith("?") or low.startswith(("what", "how", "should i", "can i")):
                ambiguities.append(frag)
            else:
                should.append(frag)

    return {
        "must": must[:10],
        "should": should[:10],
        "could": could[:10],
        "non_functional": non_functional[:10],
        "ambiguities": ambiguities[:5],
    }


# ===========================================================================
# Workspace Exploration (existing)
# ===========================================================================

def _walk_project_skeleton(workspace: str, max_dirs: int = 25, max_files_per_dir: int = 8) -> Dict[str, Any]:
    """Walks the workspace and returns a hierarchical structure suitable for mindmap rendering."""
    skeleton: Dict[str, Any] = {"_files": []}
    visited = 0
    skip = {".git", "__pycache__", "chroma_db", "backups", "node_modules", ".venv", "venv", "dist", "build"}

    try:
        for root, dirs, files in os.walk(workspace):
            dirs[:] = [d for d in dirs if d not in skip and not d.startswith(".")]
            rel = os.path.relpath(root, workspace)
            parts = [] if rel in (".", "") else rel.replace("\\", "/").split("/")

            node = skeleton
            for p in parts:
                node = node.setdefault(p, {"_files": []})

            for f in sorted(files)[:max_files_per_dir]:
                if f.startswith(".") or f.endswith((".pyc", ".lock")):
                    continue
                node["_files"].append(f)

            visited += 1
            if visited >= max_dirs:
                break
    except Exception as _ex: log.warning("Silenced exception: %s", _ex)

    return skeleton


def _skeleton_to_mindmap_dict(name: str, node: Dict[str, Any]) -> Dict[str, Any]:
    """Converts a directory skeleton into the {name, children} structure expected by generate_mermaid_mindmap."""
    children: List[Dict[str, Any]] = []
    for k, v in node.items():
        if k == "_files":
            for fname in v:
                children.append({"name": fname, "children": []})
        elif isinstance(v, dict):
            children.append(_skeleton_to_mindmap_dict(k, v))
    return {"name": name, "children": children}


def generate_project_mindmap(workspace: str, project_name: str = "project") -> str:
    """Walks the project tree and emits a validated Mermaid mindmap string."""
    skel = _walk_project_skeleton(workspace)
    structure = _skeleton_to_mindmap_dict(project_name, skel)
    res = generate_mermaid_mindmap(structure)
    if isinstance(res, dict):
        data = res.get("data") or {}
        if isinstance(data, dict) and "diagram" in data:
            return data["diagram"]
        if "logs" in res and isinstance(res["logs"], str) and "mindmap" in res["logs"].lower():
            return res["logs"]
    if isinstance(res, str):
        return res
    return "mindmap\n  root((project))"


# ===========================================================================
# ArchitectSpecialist
# ===========================================================================

class ArchitectSpecialist(BaseSpecialist):
    """ARCHITECT â€” the authoritative strategic planning intelligence for AELVO Omega.

    ARCHITECT does not write code. It plans, designs, and coordinates.
    It produces structured, dependency-aware, risk-assessed, verifiable,
    and recoverable execution plans that specialists execute.

    Capabilities:
    - Objective interpretation (goal â†’ clear strategic framing)
    - Repository-aware planning (repo intelligence â†’ impact analysis)
    - Hierarchical decomposition (goal â†’ phases â†’ tasks)
    - Specialist orchestration (who does what and why)
    - Verification planning (what must be validated and how)
    - Recovery planning (what happens when things fail)
    - Risk analysis (security, architecture, implementation, runtime, maintenance, coordination)
    - Cost analysis (complexity, surface area, effort, regressions)
    - Self-critique (review plan before finalizing)
    - Mermaid diagram generation and validation (legacy capability)
    - ADR persistence and recall (legacy capability)
    """

    name: str = "ARCHITECT"
    trigger_patterns: List[str] = [
        "architecture", "design", "system", "diagram", "mermaid", "flowchart",
        "mindmap", "adr", "capacity", "cost", "component", "interface",
        "protocol", "failure mode", "database schema", "openapi",
        "plan", "strategy", "roadmap", "milestone", "phase", "decomposition",
        "orchestrate", "coordinate", "specialist", "delegate",
    ]
    memory_types: List[str] = ["system_decision", "architecture_map", "architect_plan"]
    required_tools: List[str] = ["read_file", "write_file"]
    activation_threshold: float = 0.6

    def __init__(self):
        self._orchestrator = None

    def _get_orchestrator(self, context: Dict[str, Any]):
        """Lazy-init and cache the ArchitectOrchestrator with available intelligence."""
        if self._orchestrator is not None:
            return self._orchestrator

        from runtime_next.plan.architect import ArchitectOrchestrator

        repo_intel = context.get("repo_intelligence")
        forge_memory = context.get("forge_memory") or context.get("memory_engine")

        # Try to get an event bus for plan lifecycle events
        # The runtime_bus is injected into context by Orchestrator.build_shared_context()
        # Falls back to runtime_next events bus if available, else None
        event_bus = context.get("runtime_bus")

        # If repo_intel is a RepoIntelligenceEngine instance, pass it
        # Otherwise check if we can build a lightweight one
        ri = None
        if repo_intel is not None:
            ri = repo_intel
        else:
            # Try to get a repo intelligence engine from context
            engine = context.get("cognitive_engine")
            if engine:
                ri = getattr(engine, "_repo_intel", None)

        self._orchestrator = ArchitectOrchestrator(
            repo_intelligence=ri,
            forge_memory=forge_memory,
            event_bus=event_bus,
        )
        return self._orchestrator

    def compute_activation_score(self, task: str, context: Dict[str, Any]) -> float:
        score = super().compute_activation_score(task, context)
        clean_task = task.lower()

        # Boost for explicit planning signals
        if any(w in clean_task for w in [
            "database schema", "uml", "component design", "blueprint",
            "architectural", "high level design", "flow chart",
            "plan this", "decompose", "orchestrate", "strategy",
        ]):
            score += 0.35

        # Boost for multi-specialist coordination signals
        if any(w in clean_task for w in ["@forge", "@sentinel", "@terminus", "@oracle"]):
            score += 0.2

        return min(1.0, max(0.0, score))

    def get_system_prompt(self, context: Dict[str, Any]) -> str:
        """Dynamically compiles ARCHITECT system prompt with live decomposition,
        planning context, and architectural intelligence."""

        budget = context.get("budget", 30)
        task = context.get("task", "")

        # 1. Built-in plan (if one was created via create_architect_plan)
        plan_display = ""
        plan_sections = context.get("architect_plan_display", "")
        if plan_sections:
            plan_display = f"CURRENT PLAN:\n{plan_sections}\n"

        # 2. ADR recall
        decisions = context.get("system_decisions", [])
        decisions_str = ""
        if decisions:
            for d in decisions[:5]:
                decisions_str += f"  - ADR: {d.get('doc', '')[:200]}\n"
        else:
            decisions_str = "  - No prior architectural decisions in memory.\n"

        # 3. Workspace artifact survey
        workspace = context.get("workspace_path", ".")
        configs: List[str] = []
        skip = {".git", "__pycache__", "chroma_db", "backups", "node_modules", ".venv", "venv"}
        try:
            for root, dirs, files in os.walk(workspace):
                dirs[:] = [d for d in dirs if d not in skip and not d.startswith(".")]
                for f in files:
                    if f.lower() in (
                        "readme.md", "docker-compose.yml", "docker-compose.yaml",
                        "openapi.yaml", "openapi.json", "package.json", "pyproject.toml",
                        "requirements.txt", "tsconfig.json", "cargo.toml", "go.mod",
                    ):
                        configs.append(os.path.relpath(os.path.join(root, f), workspace))
        except Exception as _ex: log.warning("Silenced exception: %s", _ex)
        configs_str = ", ".join(configs[:8]) if configs else "None"

        # 4. Live decomposition
        decomposition = decompose_requirements(task)
        decomp_lines = []
        if decomposition["must"]:
            decomp_lines.append("  MUST:")
            for m in decomposition["must"]:
                decomp_lines.append(f"    - {m}")
        if decomposition["should"]:
            decomp_lines.append("  SHOULD:")
            for m in decomposition["should"]:
                decomp_lines.append(f"    - {m}")
        if decomposition["could"]:
            decomp_lines.append("  COULD:")
            for m in decomposition["could"]:
                decomp_lines.append(f"    - {m}")
        if decomposition["non_functional"]:
            decomp_lines.append("  NON-FUNCTIONAL:")
            for m in decomposition["non_functional"]:
                decomp_lines.append(f"    - {m}")
        if decomposition["ambiguities"]:
            decomp_lines.append("  AMBIGUITIES (RESOLVE BEFORE PLANNING):")
            for m in decomposition["ambiguities"]:
                decomp_lines.append(f"    - {m}")
        decomp_str = "\n".join(decomp_lines) if decomp_lines else "  - (no decomposable signals)"

        # 5. Constraints
        constraints = context.get("constraints", {})
        constraints_str = ""
        for k, v in constraints.items():
            if isinstance(v, dict):
                constraints_str += f"HARD RULE: {k} = {v.get('value')}\n"
            else:
                constraints_str += f"HARD RULE: {k} = {v}\n"

        # 6. Active specialists
        active_specialists = context.get("active_specialists", [])
        specialists_str = ", ".join(active_specialists) if active_specialists else "None"

        # 7. Security rules from cross-specialist injection
        security_rules = context.get("security_rules", [])
        security_str = ""
        if security_rules:
            for r in security_rules[:3]:
                security_str += f"  - {r.get('doc', '')[:150]}\n"

        # 8. Relevant files from repo intelligence
        relevant_files = context.get("relevant_files", [])
        files_str = ""
        if relevant_files:
            files_str = "\n".join(f"  - {f}" for f in relevant_files[:10])

        system_prompt = f"""You are ARCHITECT, the master planning intelligence for AELVO OMEGA.

You are NOT a general assistant. You do NOT write code. You plan, design, and coordinate.

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
ARCHITECTURE OUTPUT CONTRACT
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
Every plan you produce MUST include these 14 sections:

1. OBJECTIVE - What is being solved and what success looks like
2. CONTEXT ANALYSIS - Explicit goals, implicit goals, hidden requirements, and constraints
3. REPOSITORY ANALYSIS - Repository reality: ownership, dependencies, hotspots, and protected components
4. ARCHITECTURAL ANALYSIS - Boundaries, responsibilities, design intent, and architectural drift
5. DEPENDENCY ANALYSIS - Execution, repository, specialist, verification, and recovery dependencies
6. RISK ANALYSIS - What could go wrong technically, architecturally, or operationally
7. EXECUTION STRATEGY - The ordered strategy to solve the problem (phases with dependencies)
8. SPECIALIST ASSIGNMENTS - Which specialist should handle each part and why
9. VERIFICATION STRATEGY - How the result will be validated before execution completes
10. RECOVERY STRATEGY - What happens if something fails midway
11. COMPLETION CRITERIA - What must be true before the task is considered done
12. LONG-TERM IMPACT - Maintenance, scaling, evolution, and technical debt effects
13. SELF-CRITIQUE - A final check that the plan is coherent, minimal, and executable
14. FINAL APPROVED PLAN - Approval status, conditions, and any blocking governance reason

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
LIVE REQUIREMENT DECOMPOSITION
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
{decomp_str}

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
RECALLED ARCHITECTURAL DECISIONS (ADRs)
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
{decisions_str}

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
WORKSPACE ARCHITECTURAL ARTIFACTS
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
Active Configuration Files: {configs_str}
Relevant Files: {files_str if files_str else "(none identified)"}

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
PERSISTENT CONSTRAINTS & ANCHORS
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
{constraints_str if constraints_str else "(none)"}

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
SECURITY RULES (cross-injected from SENTINEL)
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
{security_str if security_str else "(none)"}

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
ACTIVE SPECIALISTS THIS TURN
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
{specialists_str}
BUDGET: {budget} steps remaining.

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
{plan_display}
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
ARCHITECT PLANNING PROTOCOLS
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”

1. OBJECTIVE INTERPRETATION
   - Understand the actual user goal before decomposing
   - Infer hidden constraints from context and memory
   - Separate requested outcome from implementation details
   - Detect ambiguity and surface it for resolution

2. REPOSITORY-AWARE PLANNING
   - Use available repo intelligence to understand structure
   - Identify affected modules, files, and subsystems
   - Estimate blast radius and coupling
   - Detect architectural risks

3. HIERARCHICAL DECOMPOSITION
   - Break work into phases with clear dependencies
   - Build a dependency-aware execution graph
   - Avoid shallow one-step task splitting
   - Preserve logical sequencing

4. SPECIALIST ORCHESTRATION
   - Decide which specialist should do what
   - Delegate only when the specialist adds value
   - Avoid redundant delegation
   - Define explicit handoff contracts

5. RISK ANALYSIS
   - Evaluate: security, architecture, implementation, runtime, maintenance, coordination
   - Assign likelihood Ã— impact scores
   - Define mitigations and contingencies

6. VERIFICATION PLANNING
   - Define what must be validated and how
   - Define success thresholds for each check
   - Distinguish blocking vs non-blocking checks

7. RECOVERY PLANNING
   - Predict likely failure modes per phase
   - Define fallback strategies (retry, rollback, substitute, escalate, decompose, abort)
   - Define rollback points

8. SELF-CRITIQUE
   - Review your plan before finalizing
   - Detect missing steps, circular reasoning, under-specified steps, over-complexity
   - Revise weak plans before release

DESIGN PRINCIPLES:
- Think globally before acting locally
- Minimize unnecessary delegation
- Maximize architectural clarity
- Prefer stable long-term decisions
- Preserve compatibility where possible
- Reduce coupling where possible
- Make plans deterministic and verifiable
- Surface uncertainty instead of hiding it
- Never write code directly
- Never produce vague plans
- Never ignore verification, rollback, blast radius, or security

DIAGRAM GENERATION (when explicitly requested):
- If diagrams are needed to communicate the plan, use Mermaid
- Always self-validate Mermaid syntax before emitting
- Use ADR format for architectural decisions
"""
        return system_prompt

    def build_memory_context(self, task: str, memory_engine) -> Dict[str, Any]:
        """Query system_decision and architecture_map collections.
        Also queries for prior architect plans stored in memory."""
        project = getattr(memory_engine, "project_name", "default")

        decisions: List[Dict[str, Any]] = []
        try:
            res = memory_engine.memory_collection.query(
                query_texts=[task],
                n_results=5,
                where={"$and": [{"type": "system_decision"}, {"project": project}]},
            )
            if res.get("ids") and res["ids"][0]:
                for doc, dist in zip(res["documents"][0], res["distances"][0]):
                    decisions.append({"doc": doc, "score": round(1.0 - dist, 3)})
        except Exception as _ex: log.warning("Silenced exception: %s", _ex)

        # Query for prior plans
        prior_plans: List[Dict[str, Any]] = []
        try:
            res = memory_engine.memory_collection.query(
                query_texts=[task],
                n_results=3,
                where={"$and": [{"type": "architect_plan"}, {"project": project}]},
            )
            if res.get("ids") and res["ids"][0]:
                for doc, dist in zip(res["documents"][0], res["distances"][0]):
                    prior_plans.append({"doc": doc[:200], "score": round(1.0 - dist, 3)})
        except Exception as _ex: log.warning("Silenced exception: %s", _ex)

        return {
            "system_decisions": decisions,
            "prior_architect_plans": prior_plans,
            "decomposition": decompose_requirements(task),
        }

    def post_process(self, result: str, memory_engine, conversation_history: List[Dict[str, str]]) -> str:
        """Mines the latest assistant turn for ADR markers + Mermaid blocks + plans and persists them."""
        project = getattr(memory_engine, "project_name", "default")
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        audits: List[str] = []

        from core.rag import MemorySearcher
        searcher = MemorySearcher(memory_engine.memory_collection)

        recent_assistant = " ".join(
            m.get("content", "") for m in conversation_history[-6:] if m.get("role") == "assistant"
        )
        text = (recent_assistant + "\n" + (result or "")).strip()
        low = text.lower()

        # 1. Persist plan markers as architect_plan vectors
        plan_headers = re.findall(r"â•”â•â• ARCHITECT PLAN|ARCHITECT PLAN:|plan_id:|# 1\. OBJECTIVE", text)
        if plan_headers:
            plan_section = text[:2000]
            m_id = hashlib.sha256(f"arch_plan_{project}_{time.time()}".encode()).hexdigest()
            meta = {
                "type": "architect_plan",
                "timestamp": timestamp,
                "timestamp_unix": time.time(),
                "importance": 0.85,
                "usage_count": 1,
                "project": project,
                "source_specialist": "architect",
            }
            try:
                memory_engine.memory_collection.add(
                    ids=[m_id], documents=[f"[ARCHITECT PLAN] {plan_section}"], metadatas=[meta]
                )
                audits.append("Persisted architect plan to memory.")
            except Exception as _ex: log.warning("Silenced exception: %s", _ex)

        # 2. Persist Mermaid artifacts as architecture_map vectors
        mermaid_blocks = re.findall(r"```mermaid\n([\s\S]+?)\n```", text)
        for blk in mermaid_blocks[:3]:
            ok, _ = validate_mermaid(blk)
            if not ok:
                continue
            kind = "flowchart" if "flowchart" in blk.lower() else ("mindmap" if "mindmap" in blk.lower() else "diagram")
            doc = f"[{kind}] {blk[:600]}"

            if searcher.resolve_conflict(doc, meta_type="architecture_map"):
                continue

            m_id = hashlib.sha256(f"arch_map_{kind}_{time.time()}_{hashlib.sha256(blk.encode()).hexdigest()}".encode()).hexdigest()
            meta = {
                "type": "architecture_map",
                "diagram_kind": kind,
                "timestamp": timestamp,
                "timestamp_unix": time.time(),
                "importance": 0.75,
                "usage_count": 1,
                "project": project,
                "source_specialist": "architect",
            }
            try:
                memory_engine.memory_collection.add(ids=[m_id], documents=[doc], metadatas=[meta])
            except Exception:
                continue
            try:
                with memory_engine.db:
                    memory_engine.db.execute(
                        "INSERT INTO retained_memory (content) VALUES (?)",
                        (f"[ARCHITECT:architecture_map|{project}] {kind} diagram",),
                    )
                audits.append(f"Persisted architecture_map ({kind}).")
            except Exception:
                try:
                    memory_engine.memory_collection.delete(ids=[m_id])
                except Exception as _ex: log.warning("Silenced exception: %s", _ex)

        # 3. Persist ADR-style decisions as system_decision vectors
        adr_matches = re.findall(
            r"(?:ADR[\s\-:]*\d*|Decision\s*:)\s*(.+?)(?:\n\n|\Z)",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        for adr in adr_matches[:2]:
            summary = f"ADR: {adr.strip()[:500]}"

            if searcher.resolve_conflict(summary, meta_type="system_decision"):
                continue

            m_id = hashlib.sha256(f"adr_{time.time()}_{hash(summary)}".encode()).hexdigest()
            meta = {
                "type": "system_decision",
                "timestamp": timestamp,
                "timestamp_unix": time.time(),
                "importance": 0.85,
                "usage_count": 1,
                "project": project,
                "source_specialist": "architect",
            }
            try:
                memory_engine.memory_collection.add(ids=[m_id], documents=[summary], metadatas=[meta])
            except Exception:
                continue
            try:
                with memory_engine.db:
                    memory_engine.db.execute(
                        "INSERT OR IGNORE INTO semantic_memory (tag, constraint_rule) VALUES (?, ?)",
                        ("system_decision", summary),
                    )
                    memory_engine.db.execute(
                        "INSERT INTO retained_memory (content) VALUES (?)",
                        (f"[ARCHITECT:system_decision|{project}] {summary[:800]}",),
                    )
                audits.append("Logged ADR system_decision.")
            except Exception:
                try:
                    memory_engine.memory_collection.delete(ids=[m_id])
                except Exception as _ex: log.warning("Silenced exception: %s", _ex)

        # 4. If diagrams referenced but none provided, log soft note
        if not mermaid_blocks and any(w in low for w in ["flowchart", "mindmap", "architecture diagram"]):
            audits.append("Diagram referenced but no validated Mermaid block emitted.")

        return f"[ARCHITECT AUDIT] Audits: {', '.join(audits) if audits else 'no architecture artifacts emitted this turn.'}"

    def verify_output(self, output: str, context: Dict[str, Any]) -> Tuple[bool, str]:
        """Runs the Mermaid validator on every diagram block contained in the response.
        Also validates that if the output claims to be a plan, it follows the Omega contract."""
        blocks = re.findall(r"```mermaid\n([\s\S]+?)\n```", output or "")
        for b in blocks:
            ok, err_msg = validate_mermaid(b)
            if not ok:
                return False, f"Proposed Mermaid diagram has syntax errors: {err_msg}"

        # Check for plan completeness if a plan was requested
        if "OBJECTIVE" in (output or "") and (
            "SELF-CRITIQUE" in (output or "") or "SELF-REVIEW" in (output or "")
        ):
            required_sections = [
                "OBJECTIVE", "CONTEXT ANALYSIS", "REPOSITORY ANALYSIS",
                "ARCHITECTURAL ANALYSIS", "DEPENDENCY ANALYSIS", "RISK ANALYSIS",
                "EXECUTION STRATEGY", "SPECIALIST ASSIGNMENTS",
                "VERIFICATION STRATEGY", "RECOVERY STRATEGY", "COMPLETION CRITERIA",
                "LONG-TERM IMPACT", "SELF-CRITIQUE", "FINAL APPROVED PLAN",
            ]
            missing = [s for s in required_sections if s not in (output or "")]
            if missing:
                return False, f"Plan is missing required sections: {', '.join(missing)}"

        return True, "Output validated successfully."

    # =========================================================================
    # ARCHITECT Plan Creation (Integration with ArchitectOrchestrator)
    # =========================================================================

    def create_architect_plan(
        self,
        objective: str,
        context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Create a structured ARCHITECT plan using the ArchitectOrchestrator.

        This is the primary planning entry point. It:
        1. Builds a plan with all 14 Omega strategic sections
        2. Self-critiques the plan
        3. Returns the plan as a serializable dict

        Args:
            objective: The goal or task description to plan for
            context: Shared context from the orchestrator

        Returns:
            Dict with plan_id, plan display, sections summary, and issues
        """
        orchestrator = self._get_orchestrator(context)
        if orchestrator is None:
            log.warning("ArchitectOrchestrator not available â€” falling back to manual planning")
            return None

        try:
            # Enrich context with additional data from the specialist context
            enriched_context = self._build_planning_context(objective, context)

            # Create the plan
            plan = orchestrator.create_plan(objective, enriched_context)

            # Self-critique
            issues = orchestrator.self_critique(plan)

            # Return structured result
            return {
                "plan_id": plan.id,
                "plan": plan,
                "plan_display": plan.to_terminal_display(),
                "sections": {
                    "objective": plan.objective.goal[:100],
                    "phases": len(plan.execution_strategy.phases),
                    "critical_path": plan.execution_strategy.critical_path,
                    "specialist_assignments": [
                        a.specialist.value for a in plan.specialist_assignments.assignments
                    ],
                    "risks": plan.risks.overall_level.value,
                    "verification_checks": len(plan.verification_plan.checks),
                    "self_review_score": plan.self_review.score,
                },
                "issues": issues,
                "cost_estimate": orchestrator.estimate_cost(plan),
            }
        except Exception as e:
            log.error("Architect plan creation failed: %s", e)
            return {
                "plan_id": None,
                "error": str(e),
                "issues": [f"Plan creation failed: {e}"],
            }

    def self_critique_plan(
        self,
        plan_data: Dict[str, Any],
    ) -> List[str]:
        """Critically review a plan and return issues.

        Can be called on any plan dict that has an 'objective' key.
        """
        plan = plan_data.get("plan")
        if plan is None:
            return ["No plan provided for critique"]

        orchestrator = self._get_orchestrator({})
        return orchestrator.self_critique(plan)

    def enrich_context_with_plan(
        self,
        plan_result: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Inject a completed plan into the shared context for downstream specialists."""
        plan = plan_result.get("plan")
        if plan is None:
            return context

        orchestrator = self._get_orchestrator(context)
        if orchestrator:
            context = orchestrator.enrich_context_with_plan(plan, context)

        return context

    def _build_planning_context(
        self,
        objective: str,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Build enriched context for the ArchitectOrchestrator."""
        planning_context = {
            "task": objective,
            "constraints": context.get("constraints", {}),
            "project": context.get("project", ""),
            "active_specialists": context.get("active_specialists", []),
            "tree_snapshot": context.get("tree_snapshot", ""),
            "repo_intelligence": context.get("repo_intelligence"),
        }

        # Add system decisions from memory
        dec = context.get("system_decisions", [])
        if dec:
            planning_context["system_decisions"] = dec

        # Add security rules
        sec = context.get("security_rules", [])
        if sec:
            planning_context["security_rules"] = sec

        # Add repo intelligence results
        ri = context.get("cross_memory", {}).get("repo_intel_results", [])
        if ri:
            planning_context["repo_intel_results"] = ri

        return planning_context

    # =====================================================================
    # Mode Selection  (Amendment 1)
    # =====================================================================

    def select_execution_mode(
        self,
        task: str = "",
        risk_profile: str = "low",
        complexity: int = 1,
        goals: Optional[List[str]] = None,
        constraints: Optional[Dict[str, Any]] = None,
        hermes_context: Any = None,
    ) -> Dict[str, Any]:
        """Evaluate the task and select Consolidated (Mode A) or Collaborative (Mode B).

        Uses ``ModeSelectionCriteria.from_hermes_context()`` to build
        the decision matrix defined in the risk register:

        - risk >= high -> Mode B
        - complexity > 4 -> Mode B
        - security concerns -> Mode B
        - requires consensus -> Mode B
        - affected_files >= 5 -> Mode B
        - goals >= 4 -> Mode B
        - Otherwise -> Mode A

        Args:
            task: The raw task description.
            risk_profile: Inferred risk level.
            complexity: Inferred complexity (1-10).
            goals: Decomposed goals from HermesContext.
            constraints: Extracted constraints.
            hermes_context: An optional HermesContext instance (preferred).

        Returns:
            Dict with ``mode`` (ExecutionMode), ``rationale`` (str),
            and ``criteria`` (ModeSelectionCriteria).
        """
        # If a full HermesContext is provided, extract fields from it
        if hermes_context is not None:
            task = task or getattr(hermes_context, "task", "")
            risk_profile = risk_profile or getattr(hermes_context, "risk_profile", "low")
            complexity = complexity or getattr(hermes_context, "complexity", 1)
            goals = goals or getattr(hermes_context, "goals", [])
            constraints = constraints or getattr(hermes_context, "constraints", {})

        criteria = ModeSelectionCriteria.from_hermes_context(
            task=task,
            risk_profile=risk_profile,
            complexity=complexity,
            goals=goals,
            constraints=constraints,
        )
        mode = criteria.select_mode()
        rationale = criteria.rationale()

        log.info(
            "Architect selected mode=%s for task (risk=%s, complexity=%d, goals=%d)",
            mode.value, risk_profile, complexity, len(goals or []),
        )

        return {
            "mode": mode,
            "rationale": rationale,
            "criteria": criteria,
        }

    # =====================================================================
    # Decision Authority  (Amendment 3)
    # =====================================================================

    def make_decision(
        self,
        outcome: ArchitectDecisionOutcome,
        target_type: str = "plan",
        target_id: str = "",
        reason: str = "",
        conditions: Optional[List[str]] = None,
        assigned_to: str = "",
        assigned_reason: str = "",
        overridden_recommendation: str = "",
        override_rationale: str = "",
        replan_trigger: str = "",
        replan_scope: str = "partial",
    ) -> ArchitectDecision:
        """Create an authoritative ArchitectDecision.

        This is the single point of creation for all Architect decisions.
        Every decision is logged and carries full provenance.

        Args:
            outcome: The decision outcome.
            target_type: What the decision applies to.
            target_id: ID of the target.
            reason: Human-readable justification.
            conditions: Optional conditions.
            assigned_to: Specialist to act on this decision.
            assigned_reason: Why assigned.
            overridden_recommendation: Original recommendation (for OVERRIDE).
            override_rationale: Why overridden (for OVERRIDE).
            replan_trigger: Trigger reason (for REPLAN).
            replan_scope: 'full' or 'partial' (for REPLAN).

        Returns:
            An immutable ArchitectDecision.
        """
        raw_id = f"arch_dec_{target_id}_{outcome.value}_{time.time()}"
        decision_id = hashlib.sha256(raw_id.encode()).hexdigest()[:16]

        decision = ArchitectDecision(
            decision_id=decision_id,
            outcome=outcome,
            target_type=target_type,
            target_id=target_id,
            reason=reason,
            conditions=conditions or [],
            assigned_to=assigned_to,
            assigned_reason=assigned_reason,
            overridden_recommendation=overridden_recommendation,
            override_rationale=override_rationale,
            replan_trigger=replan_trigger,
            replan_scope=replan_scope,
            decided_by="ARCHITECT",
        )

        log.info(
            "Architect decision: %s on %s %s: %s",
            outcome.value.upper(), target_type, target_id[:12], reason[:80],
        )
        return decision

    def review_consensus(
        self,
        consensus_recommendation: str,
        consensus_confidence: float = 0.5,
        consensus_id: str = "",
        positions: Optional[Dict[str, str]] = None,
        task: str = "",
        risk_profile: str = "low",
        complexity: int = 1,
        conditions: Optional[List[str]] = None,
    ) -> ArchitectDecision:
        """Review a consensus recommendation and return an ArchitectDecision.

        This implements Amendment 3: Consensus is advisory, Architect is
        authoritative. The Architect evaluates the consensus recommendation
        and decides whether to APPROVE, REJECT, ESCALATE, REPLAN, or OVERRIDE.

        The decision logic weighs:
        - Consensus confidence and position agreement
        - Task risk profile and complexity
        - Known conditions and constraints

        Args:
            consensus_recommendation: The advisory recommendation from consensus.
            consensus_confidence: Consensus confidence (0.0-1.0).
            consensus_id: The consensus event ID.
            positions: Specialist -> position mapping.
            task: The original task description.
            risk_profile: Task risk level.
            complexity: Task complexity.
            conditions: Conditions that must be satisfied.

        Returns:
            An ArchitectDecision with the review outcome.
        """
        positions = positions or {}
        conditions = conditions or []

        # Count positions for/against
        for_count = sum(1 for p in positions.values() if p.lower() in ("yes", "for", "approve"))
        against_count = sum(1 for p in positions.values() if p.lower() in ("no", "against", "reject"))
        total_positions = len(positions)

        # ── Decision Logic ────────────────────────────────────────

        # High risk + high complexity always gets Architect scrutiny
        if risk_profile in ("high", "critical") and complexity >= 6:
            if consensus_confidence >= 0.8 and for_count > against_count:
                return self.make_decision(
                    outcome=ArchitectDecisionOutcome.APPROVE,
                    target_type="consensus",
                    target_id=consensus_id,
                    reason=(
                        f"Approved after review: consensus confidence={consensus_confidence:.2f} "
                        f"with {for_count}/{total_positions} in favor, despite high risk"
                    ),
                    conditions=conditions,
                )
            return self.make_decision(
                outcome=ArchitectDecisionOutcome.REJECT,
                target_type="consensus",
                target_id=consensus_id,
                reason=(
                    f"Rejected: high-risk task (risk={risk_profile}, complexity={complexity}) "
                    f"with insufficient consensus confidence ({consensus_confidence:.2f})"
                ),
                assigned_to="FORGE",
                assigned_reason="Return for revision and resubmission",
            )

        # Strong consensus with high confidence -> approve
        if consensus_confidence >= 0.8 and for_count > total_positions / 2:
            return self.make_decision(
                outcome=ArchitectDecisionOutcome.APPROVE,
                target_type="consensus",
                target_id=consensus_id,
                reason=(
                    f"Approved: strong consensus (confidence={consensus_confidence:.2f}, "
                    f"{for_count}/{total_positions} in favor)"
                ),
                conditions=conditions,
            )

        # Low confidence but majority agrees -> approve with conditions
        if for_count > against_count and consensus_confidence >= 0.5:
            return self.make_decision(
                outcome=ArchitectDecisionOutcome.APPROVE,
                target_type="consensus",
                target_id=consensus_id,
                reason=(
                    f"Approved with conditions: majority in favor ({for_count}/{total_positions}) "
                    f"but confidence is {consensus_confidence:.2f}"
                ),
                conditions=list(conditions) + ["Re-evaluate after execution"],
            )

        # Split or low confidence -> escalate or replan
        if total_positions > 0 and for_count == against_count:
            if complexity >= 5:
                return self.make_decision(
                    outcome=ArchitectDecisionOutcome.REPLAN,
                    target_type="consensus",
                    target_id=consensus_id,
                    reason=f"Tied consensus ({for_count}/{against_count}) with complexity {complexity} — replanning",
                    replan_trigger="tied_consensus",
                    replan_scope="partial",
                )
            return self.make_decision(
                outcome=ArchitectDecisionOutcome.ESCALATE,
                target_type="consensus",
                target_id=consensus_id,
                reason=f"Tied consensus ({for_count}/{against_count}) — escalating to user",
            )

        # Majority against -> reject
        if against_count > for_count:
            return self.make_decision(
                outcome=ArchitectDecisionOutcome.REJECT,
                target_type="consensus",
                target_id=consensus_id,
                reason=f"Rejected: majority against ({against_count}/{total_positions})",
                assigned_to="FORGE",
                assigned_reason="Return for revision",
            )

        # Override: Architect disagrees with consensus
        if consensus_confidence < 0.4 and for_count > against_count:
            return self.make_decision(
                outcome=ArchitectDecisionOutcome.OVERRIDE,
                target_type="consensus",
                target_id=consensus_id,
                reason=f"Overriding low-confidence consensus (confidence={consensus_confidence:.2f})",
                overridden_recommendation=consensus_recommendation,
                override_rationale=(
                    f"Consensus confidence ({consensus_confidence:.2f}) is below 0.4 threshold. "
                    f"Architect determines action is not justified."
                ),
            )

        # Fallback: approve
        return self.make_decision(
            outcome=ArchitectDecisionOutcome.APPROVE,
            target_type="consensus",
            target_id=consensus_id,
            reason=f"Approved by default (confidence={consensus_confidence:.2f})",
            conditions=conditions,
        )

    def apply_decision(
        self,
        decision: ArchitectDecision,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Apply an ArchitectDecision and execute any side effects.

        Depending on the outcome, this may:
        - APPROVE: Return approval result (no side effect)
        - REJECT: Mark plan/task for revision
        - ESCALATE: Flag for user escalation
        - REPLAN: Trigger DynamicReplanningEngine
        - OVERRIDE: Record override in context

        Args:
            decision: The decision to apply.
            context: Optional shared context (may contain replan engine, etc.).

        Returns:
            Dict with ``applied`` (bool), ``result`` (str), and
            ``decision`` (ArchitectDecision).
        """
        context = context or {}
        result = ""

        if decision.outcome == ArchitectDecisionOutcome.APPROVE:
            result = f"Decision APPROVED: {decision.reason[:120]}"

        elif decision.outcome == ArchitectDecisionOutcome.REJECT:
            result = (
                f"Decision REJECTED: {decision.reason[:120]}"
                f"{' | Assigned to ' + decision.assigned_to if decision.assigned_to else ''}"
            )

        elif decision.outcome == ArchitectDecisionOutcome.ESCALATE:
            result = (
                f"Decision ESCALATED: {decision.reason[:120]}"
            )

        elif decision.outcome == ArchitectDecisionOutcome.REPLAN:
            from cognition.replan import ReplanTrigger
            replan_engine = context.get("replan_engine")
            plan = context.get("plan")
            if replan_engine is not None and plan is not None:
                trigger = ReplanTrigger.MANUAL
                replan_result = replan_engine.evaluate(
                    plan,
                    trigger=trigger,
                    context={
                        "manual_action": "restructure",
                        "manual_description": decision.replan_trigger or decision.reason,
                    },
                )
                if replan_result:
                    result = (
                        f"Decision REPLAN: {decision.reason[:100]} "
                        f"| Action: {replan_result.action.value}"
                    )
                else:
                    result = f"Decision REPLAN: {decision.reason[:100]} (replan engine returned no result)"
            else:
                result = (
                    f"Decision REPLAN: {decision.reason[:100]} "
                    f"(replan deferred: {'no engine' if not replan_engine else 'no plan'})"
                )
                return {
                    "applied": False,
                    "result": result,
                    "decision": decision,
                }

        elif decision.outcome == ArchitectDecisionOutcome.OVERRIDE:
            result = (
                f"Decision OVERRIDE: {decision.reason[:100]}"
                f" | Original: {decision.overridden_recommendation[:80]}"
            )

        log.info("Applied architect decision: %s", result[:120])
        return {
            "applied": True,
            "result": result,
            "decision": decision,
        }
