"""router.py — Task routing and classification for AELVO OMEGA."""
import re
import json
import logging
from typing import Any, Dict, List, Tuple

from config.settings import ACTIVATION_THRESHOLD_DEFAULT
from specialists import SPECIALIST_REGISTRY, get_specialist

log = logging.getLogger("aelvo.router")

_FORCE_ROUTE_RE = re.compile(r"^\s*((?:@[A-Z]+\s+)+)", re.IGNORECASE)

# Collaboration routing table:
# Maps evidence types to the specialists that should receive them.
# This is the canonical definition of the collaboration transport layer.
COLLABORATION_ROUTING_TABLE: Dict[str, List[str]] = {
    # ORACLE findings -> FORGE consumes, SENTINEL reviews
    "finding": ["FORGE", "SENTINEL"],
    # FORGE implementations -> SENTINEL reviews, ARCHITECT monitors
    "implementation": ["SENTINEL", "ARCHITECT"],
    # SENTINEL approvals -> FORGE proceeds, TERMINUS can execute
    "review_approve": ["FORGE", "TERMINUS"],
    # SENTINEL rejections -> FORGE revises, ARCHITECT decides
    "review_reject": ["FORGE", "ARCHITECT"],
    # SENTINEL challenges -> ARCHITECT arbitrates, CONSENSUS evaluates
    "challenge": ["ARCHITECT"],
    # Consensus recommendations -> ARCHITECT decides
    "consensus": ["ARCHITECT"],
    # ARCHITECT decisions -> TERMINUS executes, HERALD reports, FORGE informed
    "decision": ["TERMINUS", "HERALD", "FORGE"],
    # TERMINUS execution results -> HERALD reports, FORGE informed
    "execution_result": ["HERALD", "FORGE"],
    # HERALD reports -> user-facing (no specialist routing needed)
    "report": [],
}


class TaskRouter:
    """Routes tasks to appropriate specialists based on activation scoring and force-route syntax.

    Responsibilities:
    - Parse @SPECIALIST force-route prefixes
    - Compute activation scores for each specialist
    - Enforce deterministic execution priority ordering
    - Calibrate raw outputs through HERMES
    - **Collaboration Transport**: route_publication() maps evidence types
      to target specialists (Phase 2 of OMEGA directive)
    """

    def __init__(self, memory_engine=None):
        self.memory_engine = memory_engine

    def parse_force_route(self, task: str) -> Tuple[List[str], str]:
        """Detects @SPECIALIST prefixes and returns (forced_names, stripped_task)."""
        m = _FORCE_ROUTE_RE.match(task or "")
        if not m:
            return [], task
        prefix = m.group(1)
        forced: List[str] = []
        for token in prefix.split():
            name = token.lstrip("@").strip().upper()
            if name in SPECIALIST_REGISTRY and name not in forced:
                forced.append(name)
        stripped = task[m.end():].strip()
        return forced, (stripped or task)

    def route_publication(
        self,
        evidence_type: str,
        specialist: str,
        entry_id: str = "",
        content_preview: str = "",
    ) -> List[str]:
        """Route a blackboard publication to the appropriate specialists.

        Acts as the Collaboration Transport Layer. Based on the evidence
        type and originating specialist, this method determines which
        specialists should receive the publication.

        Args:
            evidence_type: Type of evidence (finding, implementation,
                review_approve, review_reject, challenge, consensus,
                decision, execution_result, report).
            specialist: The originating specialist name.
            entry_id: The blackboard entry ID (for logging).
            content_preview: Short content preview (for logging).

        Returns:
            List of specialist names that should receive this publication.
            Empty list if no routing is needed.
        """
        # Use the collaboration routing table
        targets = COLLABORATION_ROUTING_TABLE.get(evidence_type, [])

        # Don't route back to the originating specialist
        if specialist in targets:
            targets = [t for t in targets if t != specialist]

        if targets and entry_id:
            log.info(
                "[ROUTER] %s %s -> %s (entry=%s)",
                specialist, evidence_type, targets, entry_id[:8],
            )

        return targets

    def get_routing_table(self) -> Dict[str, List[str]]:
        """Return the current collaboration routing table."""
        return dict(COLLABORATION_ROUTING_TABLE)

    def get_routing_display(self) -> str:
        """Return a human-readable display of the routing table."""
        lines = ["Collaboration Routing Table:", ""]
        for evidence_type, targets in COLLABORATION_ROUTING_TABLE.items():
            if targets:
                lines.append(f"  {evidence_type.upper():20} -> {', '.join(targets)}")
            else:
                lines.append(f"  {evidence_type.upper():20} -> (terminal)")
        return "\n".join(lines)

    def classify_task(self, task: str) -> List[str]:
        """Calculates activation scores for registered specialists.

        Returns ordered list of specialist names whose scores exceed threshold.
        Falls back to ['HERMES'] if no specialist activates.
        """
        forced, _ = self.parse_force_route(task)
        if forced:
            return forced

        activated: List[Tuple[str, float]] = []
        for name, spec in SPECIALIST_REGISTRY.items():
            try:
                score = spec.compute_activation_score(task, {
                    "detected_language": "python",
                    "memory_engine": self.memory_engine,
                })
            except Exception as e:
                log.warning("Specialist '%s' activation score failed: %s", name, e)
                score = 0.0
            if score >= getattr(spec, "activation_threshold", ACTIVATION_THRESHOLD_DEFAULT):
                activated.append((name, score))

        activated.sort(key=lambda x: x[1], reverse=True)
        names = [x[0] for x in activated]
        return names if names else ["HERMES"]

    def resolve_execution_order(self, active_specialists: List[str]) -> List[str]:
        """Enforces deterministic execution priorities matching canonical architecture."""
        priority = ["HERMES", "ORACLE", "SENTINEL", "ARCHITECT",
                     "FORGE", "TERMINUS", "HERALD"]
        ordered: List[str] = []
        for name in priority:
            if name in active_specialists and name not in ordered:
                ordered.append(name)
        for name in active_specialists:
            if name not in ordered:
                ordered.append(name)
        return ordered

    def calibrate_raw_output(self, raw_output: str, signals: Dict[str, Any]) -> str:
        """Run respond messages through HERMES while preserving tool-call JSON."""
        hermes = get_specialist("HERMES")
        if not hermes or not hasattr(hermes, "calibrate_response"):
            return raw_output

        def calibrate_call(call: Dict[str, Any]) -> Dict[str, Any]:
            if call.get("tool") != "respond":
                return call
            args = call.get("args", {})
            if not isinstance(args, dict) or not isinstance(args.get("message"), str):
                return call
            updated = dict(call)
            updated_args = dict(args)
            updated_args["message"] = hermes.calibrate_response(updated_args["message"], signals)
            updated["args"] = updated_args
            return updated

        text = (raw_output or "").strip()
        candidates: List[Tuple[str, str]] = []
        if "```json" in text:
            try:
                block = text.split("```json", 1)[1].split("```", 1)[0].strip()
                candidates.append(("block", block))
            except (IndexError, ValueError):
                pass
        candidates.append(("direct", text))

        decoder = json.JSONDecoder(strict=False)
        for _mode, candidate in candidates:
            try:
                parsed = json.loads(candidate, strict=False)
            except (json.JSONDecodeError, ValueError):
                try:
                    indices = [idx for idx in [candidate.find("["), candidate.find("{")] if idx >= 0]
                    if not indices:
                        continue
                    start = min(indices)
                    parsed, _ = decoder.raw_decode(candidate[start:])
                except (json.JSONDecodeError, ValueError, IndexError):
                    continue

            if isinstance(parsed, list):
                calibrated = [calibrate_call(item) if isinstance(item, dict) else item
                              for item in parsed]
                return json.dumps(calibrated, ensure_ascii=False)
            if isinstance(parsed, dict):
                return json.dumps(calibrate_call(parsed), ensure_ascii=False)

        if isinstance(raw_output, str) and raw_output:
            return hermes.calibrate_response(raw_output, signals)
        return raw_output
