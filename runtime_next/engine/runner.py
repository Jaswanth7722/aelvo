from __future__ import annotations
import logging
from typing import Dict, Any, Optional, Callable, Awaitable

from runtime_next.models.plan import ExecutionNode, NodeType

log = logging.getLogger("aelvo.graph.runner")


class NodeRunner:
    """Executes nodes by dispatching to the appropriate handler based on node type."""

    def __init__(self, tool_executor: Optional[Callable[[str, Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None):
        self._tool_executor = tool_executor
        self._handlers: Dict[str, Callable[[ExecutionNode, Dict[str, Any]], Awaitable[Dict[str, Any]]]] = {}

    def register_handler(self, node_type: str, handler: Callable[[ExecutionNode, Dict[str, Any]], Awaitable[Dict[str, Any]]]):
        self._handlers[node_type] = handler

    async def run_node(self, node: ExecutionNode, context: Dict[str, Any]) -> Dict[str, Any]:
        log.info(f"Runner: executing {node.id} ({node.node_type.value})")

        if node.node_type.value in self._handlers:
            return await self._handlers[node.node_type.value](node, context)

        if node.node_type == NodeType.TOOL_CALL and self._tool_executor:
            result = await self._tool_executor(node.tool_name, node.args)
            return result

        return {"status": "success", "output": f"Node {node.id} executed"}

    async def execute_tool(self, tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._tool_executor:
            return await self._tool_executor(tool_name, args)
        return {"status": "success", "output": f"Tool {tool_name} called"}

    def bind_to_engine(self, engine) -> Callable[[ExecutionNode], Awaitable[Dict[str, Any]]]:
        """Create a callable that the engine can use as its executor."""
        async def executor(node: ExecutionNode) -> Dict[str, Any]:
            return await self.run_node(node, {})
        return executor
