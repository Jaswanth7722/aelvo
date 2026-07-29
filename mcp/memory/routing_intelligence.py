"""RoutingIntelligence — memory-informed execution routing for MCP requests."""

from __future__ import annotations

import logging
from typing import List, Optional
from ..registry.models import MCPServerRecord
from .mcp_memory_store import MCPMemoryStore
from .reliability_tracker import ReliabilityTracker
from .specialist_preference import SpecialistPreference

log = logging.getLogger("aelvo.mcp.memory.routing")


class RoutingIntelligence:
    """Uses execution history, reliability scoring, and preferences to route requests."""

    def __init__(self, memory_store: MCPMemoryStore):
        self._memory_store = memory_store
        self._reliability_tracker = ReliabilityTracker(memory_store)
        self._specialist_preference = SpecialistPreference(memory_store)

    async def select_server(
        self,
        candidate_servers: List[MCPServerRecord],
        requesting_specialist: str,
        tool_type: str,
    ) -> Optional[MCPServerRecord]:
        """Select the highest-reliability server from candidates for a task.

        Combined score is:
        combined_score = reliability_score * 0.7 + preference_score * 0.3
        """
        if not candidate_servers:
            return None

        best_server: Optional[MCPServerRecord] = None
        best_score = -1.0

        for server in candidate_servers:
            reliability = await self._reliability_tracker.get_reliability_score(
                server.id, tool_name=tool_type
            )
            preference = await self._specialist_preference.get_preference(
                requesting_specialist, server.id, tool_name=tool_type
            )

            # Combined score weighting
            combined_score = (reliability * 0.7) + (preference * 0.3)

            log.debug(
                "RoutingIntelligence: server %s score = %s (rel=%s, pref=%s)",
                server.id,
                combined_score,
                reliability,
                preference,
            )

            if combined_score > best_score:
                best_score = combined_score
                best_server = server

        return best_server

    @property
    def reliability_tracker(self) -> ReliabilityTracker:
        return self._reliability_tracker

    @property
    def specialist_preference(self) -> SpecialistPreference:
        return self._specialist_preference
