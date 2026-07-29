from __future__ import annotations

import hashlib
import logging
import time
from typing import Any, Dict, List, Optional

from .types import (
    VerificationType,
    VerificationResult,
    VerificationScope,
    Confidence,
    Severity,
    Retryability,
)
from runtime_next.models.plan import NodeState

log = logging.getLogger("aelvo.runtime.verification.graph")


class GraphConsistencyVerifier:
    def create_handler(self):
        async def handler(
            node_id: str,
            scope: VerificationScope,
            context: Dict[str, Any],
        ) -> VerificationResult:
            return self.verify(node_id, scope, context)
        return handler

    def verify(
        self,
        node_id: str,
        scope: VerificationScope,
        context: Dict[str, Any],
    ) -> VerificationResult:
        start = time.monotonic()
        diagnostics: List[str] = []
        graph_data = context.get("graph_state", {})
        nodes = graph_data.get("nodes", {})
        edges = graph_data.get("edges", [])
        success = True

        node_ids = set(nodes.keys())
        for edge in edges:
            src = edge.get("source_node_id", edge.get("source", ""))
            tgt = edge.get("target_node_id", edge.get("target", ""))
            if src and src not in node_ids:
                diagnostics.append(f"Edge source '{src}' not found in nodes")
                success = False
            if tgt and tgt not in node_ids:
                diagnostics.append(f"Edge target '{tgt}' not found in nodes")
                success = False

        completed = sum(1 for n in nodes.values() if isinstance(n, dict) and n.get("state") in ("completed", NodeState.COMPLETED))
        failed = sum(1 for n in nodes.values() if isinstance(n, dict) and n.get("state") in ("failed", NodeState.FAILED))
        remaining = len(nodes) - completed - failed

        if remaining > 0 and not graph_data.get("is_executing", True):
            diagnostics.append(f"{remaining} nodes not in terminal state after execution completed")
            success = False

        if failed > len(nodes) // 2 and len(nodes) > 3:
            diagnostics.append(f"High failure rate: {failed}/{len(nodes)} nodes failed")
            success = False

        duration = (time.monotonic() - start) * 1000

        return VerificationResult(
            verification_id=hashlib.sha256(f"graph_{node_id}_{time.time()}".encode()).hexdigest()[:16],
            node_id=node_id,
            verification_type=VerificationType.GRAPH_CONSISTENCY,
            duration_ms=duration,
            success=success,
            confidence=Confidence.HIGH if success else Confidence.CERTAIN,
            severity=Severity.INFO if success else Severity.WARNING,
            retryability=Retryability.CONDITIONAL,
            diagnostics=diagnostics or ["Graph consistency check passed"],
            provenance="graph_verifier",
        )
