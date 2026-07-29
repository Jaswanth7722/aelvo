"""Layer 3 — Failure Classification Engine.

A failure without classification is useless runtime noise.

The classifier uses stderr/stdout, exit codes, runtime events, graph state,
capability registry, verification outputs, repository intelligence,
serialization state, and execution history.

Classification is probabilistic. Each classification includes confidence,
evidence, and alternative possibilities.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from .types import (
    FailureClassification,
    ClassificationResult,
    Confidence,
    Severity,
    Retryability,
    classify_exit_code,
    EXIT_CODE_CLASSIFICATION_MAP,
)

log = logging.getLogger("aelvo.runtime.verification.classifier")


class FailureClassifier:
    """Probabilistic failure classification engine.

    Analyzes multiple signals to produce a rich classification with
    confidence scores, evidence chains, and alternative possibilities.
    """

    def __init__(self):
        self._classification_history: List[ClassificationResult] = []
        self._custom_patterns: Dict[str, FailureClassification] = {}

    def register_pattern(
        self, pattern: str, classification: FailureClassification
    ):
        """Register a custom regex pattern for classification."""
        self._custom_patterns[pattern] = classification

    # ------------------------------------------------------------------
    # Main classification entry point
    # ------------------------------------------------------------------

    async def classify(
        self,
        error_message: str = "",
        stderr: str = "",
        stdout: str = "",
        exit_code: Optional[int] = None,
        graph_state: Optional[Dict[str, Any]] = None,
        capability_state: Optional[Dict[str, Any]] = None,
        verification_results: Optional[List[Dict[str, Any]]] = None,
        execution_history: Optional[List[Dict[str, Any]]] = None,
    ) -> ClassificationResult:
        """Classify a failure using all available signals.

        Every classification includes confidence, evidence, and
        alternative possibilities.
        """
        combined = self._combine_text(error_message, stderr, stdout)

        # Gather evidence from all signals
        evidence: Dict[str, Any] = {}
        scores: Dict[FailureClassification, float] = {}

        # 1. Analyze exit code
        exit_classification = classify_exit_code(exit_code)
        if exit_classification:
            scores[exit_classification] = scores.get(
                exit_classification, 0
            ) + self._exit_code_weight(exit_code)

        # 2. Pattern-based analysis on combined text
        text_scores = self._analyze_text(combined)
        for cls, score in text_scores.items():
            scores[cls] = scores.get(cls, 0) + score

        # 3. Custom registered patterns
        custom_scores = self._check_custom_patterns(combined)
        for cls, score in custom_scores.items():
            scores[cls] = scores.get(cls, 0) + score

        # 4. Graph state analysis
        if graph_state:
            graph_scores = self._analyze_graph_state(graph_state)
            for cls, score in graph_scores.items():
                scores[cls] = scores.get(cls, 0) + score

        # 5. Capability state analysis
        if capability_state:
            cap_scores = self._analyze_capability_state(capability_state)
            for cls, score in cap_scores.items():
                scores[cls] = scores.get(cls, 0) + score

        # 6. Verification result analysis
        if verification_results:
            ver_scores = self._analyze_verifications(verification_results)
            for cls, score in ver_scores.items():
                scores[cls] = scores.get(cls, 0) + score

        # Compute total confidence and determine primary
        if not scores:
            scores[FailureClassification.UNKNOWN_FAILURE] = 0.1

        total_score = sum(scores.values())
        normalized_scores = {
            cls: s / total_score if total_score > 0 else 0
            for cls, s in scores.items()
        }

        sorted_scores = sorted(
            normalized_scores.items(), key=lambda x: x[1], reverse=True
        )

        primary = sorted_scores[0][0]
        primary_score = sorted_scores[0][1]
        alternatives = [cls for cls, _ in sorted_scores[1:4]]
        alternative_scores = dict(sorted_scores[1:4])

        confidence = self._score_to_confidence(primary_score)

        # Build evidence
        evidence["text_analyzed"] = len(combined) > 0
        evidence["exit_code"] = exit_code
        evidence["graph_state_available"] = graph_state is not None
        evidence["capability_state_available"] = capability_state is not None
        evidence["verification_results_available"] = (
            verification_results is not None
        )

        result = ClassificationResult(
            primary=primary,
            confidence=confidence,
            confidence_score=round(primary_score, 4),
            evidence=evidence,
            alternatives=alternatives,
            alternative_scores={
                k: round(v, 4) for k, v in alternative_scores.items()
            },
            raw_stderr=stderr[:2000],
            raw_stdout=stdout[:2000],
            exit_code=exit_code,
            graph_state_snapshot=graph_state or {},
            capability_snapshot=capability_state or {},
        )

        self._classification_history.append(result)
        return result

    # ------------------------------------------------------------------
    # Signal analysis methods
    # ------------------------------------------------------------------

    def _combine_text(
        self,
        error_message: str,
        stderr: str,
        stdout: str,
    ) -> str:
        """Combine multiple text sources into a single string for analysis."""
        parts = []
        if error_message:
            parts.append(error_message)
        if stderr:
            parts.append(stderr)
        if stdout:
            parts.append(stdout)
        return "\n".join(parts)

    def _exit_code_weight(self, code: Optional[int]) -> float:
        """Weight for exit code based evidence. Uses shared mapping to avoid drift."""
        if code is None:
            return 0.0
        entry = EXIT_CODE_CLASSIFICATION_MAP.get(code)
        if entry:
            return entry["weight"]
        if code in (1, 2):
            return 0.3  # Generic error
        return 0.4

    def _analyze_text(
        self, text: str
    ) -> Dict[FailureClassification, float]:
        """Analyze combined text for failure patterns."""
        scores: Dict[FailureClassification, float] = {}
        text_lower = text.lower()

        # Syntax errors
        syntax_patterns = [
            r"(syntaxerror|syntax error|indentationerror|invalid syntax)",
            r"(unexpected token|unexpected indent|expected .* but found)",
            r"traceback \(most recent call last\)",
        ]
        for pattern in syntax_patterns:
            if re.search(pattern, text_lower):
                scores[FailureClassification.SYNTAX_ERROR] = (
                    scores.get(FailureClassification.SYNTAX_ERROR, 0) + 0.6
                )

        # Missing dependencies
        dep_patterns = [
            r"(modulenotfounderror|import error|no module named)",
            r"(cannot find module|not found|no such file)",
            r"(dependency missing|package not found|require failed)",
            r"(command not found|exec:.*: executable file not found)",
        ]
        for pattern in dep_patterns:
            if re.search(pattern, text_lower):
                scores[FailureClassification.DEPENDENCY_MISSING] = (
                    scores.get(
                        FailureClassification.DEPENDENCY_MISSING, 0
                    )
                    + 0.5
                )

        # Permission denied
        perm_patterns = [
            r"(permission denied|eacces|epipe|access denied)",
            r"(not writable|cannot create|forbidden)",
        ]
        for pattern in perm_patterns:
            if re.search(pattern, text_lower):
                scores[FailureClassification.PERMISSION_DENIED] = (
                    scores.get(FailureClassification.PERMISSION_DENIED, 0)
                    + 0.7
                )

        # Timeout
        timeout_patterns = [
            r"(timeout|timed out|time out)",
            r"(deadline exceeded|operation timed out)",
            r"(no response|hanging|unresponsive)",
        ]
        for pattern in timeout_patterns:
            if re.search(pattern, text_lower):
                scores[FailureClassification.TIMEOUT] = (
                    scores.get(FailureClassification.TIMEOUT, 0) + 0.5
                )

        # Verification failures
        ver_patterns = [
            r"(verification failed|verify|verification)",
            r"(validation error|constraint violation|anchor)",
            r"(test failed|assertion|assert)",
        ]
        for pattern in ver_patterns:
            if re.search(pattern, text_lower):
                scores[FailureClassification.VERIFICATION_FAILURE] = (
                    scores.get(
                        FailureClassification.VERIFICATION_FAILURE, 0
                    )
                    + 0.4
                )

        # Serialization failures
        ser_patterns = [
            r"(serialization|deserializ|json.*error|parse.*error)",
            r"(pickle|yaml.*error|unmarshal)",
        ]
        for pattern in ser_patterns:
            if re.search(pattern, text_lower):
                scores[FailureClassification.SERIALIZATION_FAILURE] = (
                    scores.get(
                        FailureClassification.SERIALIZATION_FAILURE, 0
                    )
                    + 0.6
                )

        # Environment failures
        env_patterns = [
            r"(out of memory|oom|memory.*exceed)",
            r"(disk full|no space left|disk quota)",
            r"(connection refused|connection reset|broken pipe)",
            r"(segmentation fault|segfault|bus error)",
        ]
        for pattern in env_patterns:
            if re.search(pattern, text_lower):
                scores[FailureClassification.ENVIRONMENT_FAILURE] = (
                    scores.get(
                        FailureClassification.ENVIRONMENT_FAILURE, 0
                    )
                    + 0.5
                )

        # Tool failures
        tool_patterns = [
            r"(tool.*fail|executor.*error|handler.*error)",
            r"(specialist.*fail|agent.*error)",
        ]
        for pattern in tool_patterns:
            if re.search(pattern, text_lower):
                scores[FailureClassification.TOOL_FAILURE] = (
                    scores.get(FailureClassification.TOOL_FAILURE, 0) + 0.4
                )

        # Mutex / lock contention
        mutex_patterns = [
            r"(lock.*contention|deadlock|mutex)",
            r"(resource busy|already locked|cannot acquire)",
        ]
        for pattern in mutex_patterns:
            if re.search(pattern, text_lower):
                scores[FailureClassification.MUTEX_VIOLATION] = (
                    scores.get(
                        FailureClassification.MUTEX_VIOLATION, 0
                    )
                    + 0.6
                )

        # Stale state indicators
        stale_patterns = [
            r"(stale|outdated|cache.*invalid)",
            r"(version mismatch|hash mismatch|checksum)",
        ]
        for pattern in stale_patterns:
            if re.search(pattern, text_lower):
                scores[FailureClassification.STALE_RUNTIME_STATE] = (
                    scores.get(
                        FailureClassification.STALE_RUNTIME_STATE, 0
                    )
                    + 0.5
                )

        # Architecture violations
        arch_patterns = [
            r"(circular dependency|cyclic import)",
            r"(architecture violation|layer violation|boundary)",
        ]
        for pattern in arch_patterns:
            if re.search(pattern, text_lower):
                scores[FailureClassification.ARCHITECTURE_VIOLATION] = (
                    scores.get(
                        FailureClassification.ARCHITECTURE_VIOLATION, 0
                    )
                    + 0.5
                )

        return scores

    def _check_custom_patterns(
        self, text: str
    ) -> Dict[FailureClassification, float]:
        """Check custom registered patterns."""
        scores: Dict[FailureClassification, float] = {}
        for pattern, classification in self._custom_patterns.items():
            if re.search(pattern, text, re.IGNORECASE):
                scores[classification] = scores.get(classification, 0) + 0.5
        return scores

    def _analyze_graph_state(
        self, graph_state: Dict[str, Any]
    ) -> Dict[FailureClassification, float]:
        """Analyze graph state for failure indicators."""
        scores: Dict[FailureClassification, float] = {}

        node_count = graph_state.get("node_count", 0)
        failed_count = graph_state.get("failed_count", 0)
        skipped_count = graph_state.get("skipped_count", 0)

        if failed_count > node_count * 0.5 and node_count > 0:
            scores[FailureClassification.GRAPH_INCONSISTENCY] = (
                scores.get(FailureClassification.GRAPH_INCONSISTENCY, 0)
                + 0.5
            )

        if skipped_count > node_count * 0.3 and node_count > 0:
            scores[FailureClassification.GRAPH_INCONSISTENCY] = (
                scores.get(FailureClassification.GRAPH_INCONSISTENCY, 0)
                + 0.3
            )

        return scores

    def _analyze_capability_state(
        self, capability_state: Dict[str, Any]
    ) -> Dict[FailureClassification, float]:
        """Analyze capability state for failure indicators."""
        scores: Dict[FailureClassification, float] = {}

        tools = capability_state.get("tools", {})
        health = capability_state.get("health", "")

        if health == "offline":
            scores[FailureClassification.ENVIRONMENT_FAILURE] = (
                scores.get(FailureClassification.ENVIRONMENT_FAILURE, 0)
                + 0.8
            )
        elif health == "restricted":
            scores[FailureClassification.PERMISSION_DENIED] = (
                scores.get(FailureClassification.PERMISSION_DENIED, 0)
                + 0.5
            )

        for tool_name, tool_info in tools.items():
            if tool_info.get("status") == "missing":
                scores[FailureClassification.DEPENDENCY_MISSING] = (
                    scores.get(
                        FailureClassification.DEPENDENCY_MISSING, 0
                    )
                    + 0.3
                )

        return scores

    def _analyze_verifications(
        self, verification_results: List[Dict[str, Any]]
    ) -> Dict[FailureClassification, float]:
        """Analyze verification results for failure indicators."""
        scores: Dict[FailureClassification, float] = {}

        for vresult in verification_results:
            if not vresult.get("success", False):
                scores[FailureClassification.VERIFICATION_FAILURE] = (
                    scores.get(
                        FailureClassification.VERIFICATION_FAILURE, 0
                    )
                    + 0.4
                )

            stale = vresult.get("stale_state_indicators", [])
            if stale:
                scores[FailureClassification.STALE_RUNTIME_STATE] = (
                    scores.get(
                        FailureClassification.STALE_RUNTIME_STATE, 0
                    )
                    + 0.3
                )

        return scores

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _score_to_confidence(self, score: float) -> Confidence:
        """Convert a numeric score to a Confidence enum."""
        if score >= 0.95:
            return Confidence.CERTAIN
        elif score >= 0.80:
            return Confidence.HIGH
        elif score >= 0.60:
            return Confidence.MEDIUM
        elif score >= 0.40:
            return Confidence.LOW
        else:
            return Confidence.GUESS

    @property
    def classification_history(self) -> List[ClassificationResult]:
        return list(self._classification_history)

    def get_recent_classifications(
        self, n: int = 10
    ) -> List[ClassificationResult]:
        return self._classification_history[-n:]
