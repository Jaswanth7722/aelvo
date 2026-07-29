"""PromptCatalog — central catalog of all MCP prompts across all registered servers."""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List, Tuple
from ..registry.models import PromptDefinition

log = logging.getLogger("aelvo.mcp.capability.prompts")


class PromptCatalog:
    """Registry of all MCP prompts across all servers."""

    def __init__(self):
        self._prompts: Dict[str, List[Tuple[str, PromptDefinition]]] = defaultdict(list)

    def register(self, server_id: str, prompt: PromptDefinition) -> None:
        existing = self._prompts.get(prompt.name, [])
        if any(sid == server_id for sid, _ in existing):
            return
        self._prompts[prompt.name].append((server_id, prompt))

    def unregister_server(self, server_id: str) -> int:
        count = 0
        for name in list(self._prompts.keys()):
            self._prompts[name] = [(sid, p) for sid, p in self._prompts[name] if sid != server_id]
            if not self._prompts[name]:
                del self._prompts[name]
            count += 1
        return count

    def find_prompt(self, name: str) -> List[Tuple[str, PromptDefinition]]:
        return list(self._prompts.get(name, []))

    def list_prompts(self) -> List[Dict[str, Any]]:
        return [
            {"name": name, "servers": [sid for sid, _ in entries], "count": len(entries)}
            for name, entries in sorted(self._prompts.items())
        ]
