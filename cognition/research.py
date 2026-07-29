from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable

from cognition.types import (
    ResearchHypothesis, ResearchFinding, ResearchEvidence,
    HypothesisStatus,
)

log = logging.getLogger("aelvo.cognition.research")


class AutonomousResearchRuntime:
    """Autonomous Research Runtime.

    Forms hypotheses from goals and blackboard content, gathers evidence
    (via tools, specialists, memory), and produces findings with confidence
    measured from evidence chains â€” never from LLM self-assessment.
    """

    def __init__(self, tool_executor: Optional[Callable] = None):
        self._hypotheses: Dict[str, ResearchHypothesis] = {}
        self._findings: Dict[str, ResearchFinding] = {}
        self._tool_executor = tool_executor

    def propose_hypothesis(
        self,
        description: str,
        proposed_by: str = "system",
        tags: Optional[List[str]] = None,
    ) -> ResearchHypothesis:
        hyp_id = self._generate_id("hypothesis", description)
        hypothesis = ResearchHypothesis(
            id=hyp_id,
            description=description,
            proposed_by=proposed_by,
            tags=tags or [],
        )
        self._hypotheses[hyp_id] = hypothesis
        log.info("Proposed hypothesis %s: %s", hyp_id, description[:60])
        return hypothesis

    def add_evidence(
        self,
        hypothesis_id: str,
        description: str,
        source: str,
        content: str = "",
        relevance: float = 0.5,
        reliability: float = 0.5,
        supports: bool = True,
    ) -> Optional[ResearchEvidence]:
        hypothesis = self._hypotheses.get(hypothesis_id)
        if hypothesis is None:
            log.warning("Hypothesis %s not found", hypothesis_id)
            return None
        evidence = ResearchEvidence(
            id=self._generate_id("evidence", f"{hypothesis_id}_{description}"),
            description=description,
            source=source,
            relevance=max(0.0, min(1.0, relevance)),
            reliability=max(0.0, min(1.0, reliability)),
            content=content,
        )
        if supports:
            hypothesis.supporting_evidence.append(evidence)
        else:
            hypothesis.refuting_evidence.append(evidence)
        hypothesis.confidence = hypothesis.compute_confidence()
        hypothesis.status = self._infer_status(hypothesis)
        log.debug("Added %s evidence to %s (confidence now %.3f)",
                  "supporting" if supports else "refuting", hypothesis_id, hypothesis.confidence)
        return evidence

    def get_hypothesis(self, hypothesis_id: str) -> Optional[ResearchHypothesis]:
        return self._hypotheses.get(hypothesis_id)

    def get_active_hypotheses(self) -> List[ResearchHypothesis]:
        return [h for h in self._hypotheses.values()
                if h.status in (HypothesisStatus.PROPOSED, HypothesisStatus.INVESTIGATING)]

    def conclude_hypothesis(self, hypothesis_id: str) -> Optional[ResearchFinding]:
        hypothesis = self._hypotheses.get(hypothesis_id)
        if hypothesis is None:
            return None
        hypothesis.status = HypothesisStatus.SUPPORTED if hypothesis.confidence >= 0.6 else HypothesisStatus.REFUTED
        finding = ResearchFinding(
            id=self._generate_id("finding", hypothesis_id),
            hypothesis_id=hypothesis_id,
            description=hypothesis.description,
            conclusion=self._build_conclusion(hypothesis),
            confidence=hypothesis.confidence,
            evidence_summary=self._summarize_evidence(hypothesis),
        )
        self._findings[finding.id] = finding
        log.info("Concluded hypothesis %s: %s (confidence=%.3f)",
                 hypothesis_id, hypothesis.status.value, hypothesis.confidence)
        return finding

    def get_finding(self, finding_id: str) -> Optional[ResearchFinding]:
        return self._findings.get(finding_id)

    def find_findings(self, query: str, max_results: int = 5) -> List[ResearchFinding]:
        query_lower = query.lower()
        scored: List[tuple] = []
        for finding in self._findings.values():
            score = 0.0
            if query_lower in finding.description.lower():
                score += 0.5
            if query_lower in finding.conclusion.lower():
                score += 0.3
            score *= finding.confidence
            if score > 0:
                scored.append((score, finding))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [f for _, f in scored[:max_results]]

    def investigate(self, hypothesis_id: str, steps: int = 3) -> None:
        hypothesis = self._hypotheses.get(hypothesis_id)
        if hypothesis is None:
            return
        hypothesis.status = HypothesisStatus.INVESTIGATING
        log.info("Investigating hypothesis %s (%d steps)", hypothesis_id, steps)
        if self._tool_executor is not None:
            for step in range(steps):
                query = f"{hypothesis.description} step {step + 1}"
                try:
                    result = self._tool_executor("search", {"query": query, "max_results": 3})
                    if isinstance(result, list):
                        for item in result:
                            evidence = ResearchEvidence(
                                id=self._generate_id("evidence", f"{hypothesis_id}_{step}_{item.get('source', 'search')}"),
                                description=item.get("description", str(item)[:100]),
                                source=item.get("source", "search"),
                                relevance=item.get("relevance", 0.5),
                                reliability=item.get("reliability", 0.5),
                                content=item.get("content", str(item)),
                            )
                            self.add_evidence(hypothesis_id, evidence.description, evidence.source, evidence.relevance, evidence.reliability)
                except Exception as e:
                    log.warning("Research step %d failed: %s", step, e)
            hypothesis.status = HypothesisStatus.SUPPORTED if hypothesis.confidence >= 0.6 else HypothesisStatus.UNRESOLVED

    def search_knowledge(self, query: str, max_results: int = 5) -> List[ResearchEvidence]:
        if self._tool_executor is not None:
            try:
                result = self._tool_executor("search", {"query": query, "max_results": max_results})
                if isinstance(result, list):
                    return [ResearchEvidence(
                        id=self._generate_id("evidence", str(i)),
                        description=r.get("description", str(r)[:100]),
                        source=r.get("source", "search"),
                        relevance=r.get("relevance", 0.5),
                        reliability=r.get("reliability", 0.5),
                        content=r.get("content", str(r)),
                    ) for i, r in enumerate(result)]
            except Exception as e:
                log.warning("Knowledge search failed: %s", e)
        return []

    def snapshot(self) -> Dict[str, Any]:
        active = self.get_active_hypotheses()
        return {
            "total_hypotheses": len(self._hypotheses),
            "active_hypotheses": len(active),
            "total_findings": len(self._findings),
            "by_status": {
                status.value: len([h for h in self._hypotheses.values() if h.status == status])
                for status in HypothesisStatus
            },
        }

    def _infer_status(self, hypothesis: ResearchHypothesis) -> HypothesisStatus:
        if hypothesis.confidence >= 0.8:
            return HypothesisStatus.SUPPORTED
        elif hypothesis.confidence <= 0.2 and len(hypothesis.refuting_evidence) >= 2:
            return HypothesisStatus.REFUTED
        elif hypothesis.status == HypothesisStatus.INVESTIGATING:
            if len(hypothesis.supporting_evidence) + len(hypothesis.refuting_evidence) >= 3:
                if 0.3 < hypothesis.confidence < 0.7:
                    return HypothesisStatus.INCONCLUSIVE
            return HypothesisStatus.INVESTIGATING
        return HypothesisStatus.PROPOSED

    def _build_conclusion(self, hypothesis: ResearchHypothesis) -> str:
        if hypothesis.confidence >= 0.8:
            return f"Strongly supported: {hypothesis.description}"
        elif hypothesis.confidence >= 0.6:
            return f"Supported: {hypothesis.description}"
        elif hypothesis.confidence >= 0.4:
            return f"Inconclusive: {hypothesis.description}"
        elif hypothesis.confidence >= 0.2:
            return f"Weakly refuted: {hypothesis.description}"
        else:
            return f"Refuted: {hypothesis.description}"

    def _summarize_evidence(self, hypothesis: ResearchHypothesis) -> str:
        support_count = len(hypothesis.supporting_evidence)
        refute_count = len(hypothesis.refuting_evidence)
        return (
            f"{support_count} supporting, {refute_count} refuting evidence items. "
            f"Confidence={hypothesis.confidence:.3f}"
        )

    def _generate_id(self, prefix: str, content: str) -> str:
        raw = f"{prefix}_{content}_{datetime.now(timezone.utc).isoformat()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
