import asyncio
import logging
import time
from enum import Enum

from runtime_next.models.plan import NodeState

log = logging.getLogger("aelvo.runtime_next.engine")


class EngineState(Enum):
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    FAILED = "failed"
    COMPLETED = "completed"


MAX_RETRY_CYCLES = 3


def _topological_sort(nodes, edges):
    in_degree = {nid: 0 for nid in nodes}
    adjacency = {nid: [] for nid in nodes}
    for from_id, to_id in edges:
        if from_id in adjacency and to_id in in_degree:
            adjacency[from_id].append(to_id)
            in_degree[to_id] = in_degree.get(to_id, 0) + 1
    queue = [nid for nid, deg in in_degree.items() if deg == 0]
    sorted_nodes = []
    while queue:
        nid = queue.pop(0)
        sorted_nodes.append(nid)
        for neighbor in adjacency.get(nid, []):
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    remaining = set(nodes) - set(sorted_nodes)
    sorted_nodes.extend(remaining)
    return sorted_nodes


class ExecutionGraph:
    def __init__(self, bus=None, mutex=None, runner=None, recovery_engine=None):
        self.bus = bus
        self.mutex = mutex
        self.runner = runner
        self.recovery_engine = recovery_engine
        self.nodes = {}
        self.edges = []

    @property
    def event_bus(self):
        return self.bus

    def add_node(self, node_def):
        self.nodes[node_def.id] = node_def

    def connect(self, from_id, to_id):
        self.edges.append((from_id, to_id))

    def remove_node(self, node_id):
        if node_id in self.nodes:
            del self.nodes[node_id]
        self.edges = [(f, t) for f, t in self.edges if f != node_id and t != node_id]

    def add_edge(self, from_id, to_id):
        self.connect(from_id, to_id)

    def serialize(self, path):
        import json
        data = {
            "nodes": {nid: node_def.model_dump() if hasattr(node_def, "model_dump") else str(node_def) for nid, node_def in self.nodes.items()},
            "edges": list(self.edges),
        }
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    @classmethod
    def deserialize(cls, path, bus=None, mutex=None):
        import json
        from runtime_next.models.node import NodeDefinition
        graph = cls(bus=bus, mutex=mutex)
        with open(path) as f:
            data = json.load(f)
        for nid, node_data in data.get("nodes", {}).items():
            if isinstance(node_data, dict):
                try:
                    node = NodeDefinition(**node_data)
                    graph.nodes[node.id] = node
                except Exception as e:
                    log.warning("Failed to deserialize node %s: %s", nid, e)
            else:
                log.warning("Skipping non-dict node data for %s", nid)
        graph.edges = [(e[0], e[1]) if isinstance(e, list) else (e.get("source"), e.get("target")) for e in data.get("edges", [])]
        return graph

    def inject_node(self, node_def, dependencies=None):
        if node_def.id not in self.nodes:
            self.nodes[node_def.id] = node_def
            if dependencies:
                for dep in dependencies:
                    self.add_edge(dep, node_def.id)
            log.info("Injected recovery node %s", node_def.id)
            return node_def.id
        return None

    async def transition_node(self, node_id, state, reason=""):
        node = self.nodes.get(node_id)
        if node is None:
            log.warning("transition_node: node %s not found", node_id)
            return
        from_state = getattr(node, "state", None)
        from_str = from_state.value if isinstance(from_state, Enum) else str(from_state or "unknown")
        if hasattr(node, "state"):
            if isinstance(state, Enum):
                node.state = state
            elif hasattr(type(node.state), "__members__") and state in type(node.state).__members__:
                node.state = type(node.state)(state)
            else:
                node.state = state
        to_str = node.state.value if isinstance(node.state, Enum) else str(node.state)
        if hasattr(node, "add_history"):
            node.add_history(from_state, node.state, reason)
        log.info("Node %s: %s -> %s (%s)", node_id, from_str, to_str, reason[:80] if reason else "")
        if self.bus:
            from runtime_next.models.events import NodeTransitionEvent
            event = NodeTransitionEvent(
                id=f"trans_{node_id}_{int(time.time())}",
                node_id=node_id,
                from_state=from_str,
                to_state=to_str,
                reason=reason,
            )
            await self.bus.publish(event)

    async def start(self, context=None):
        engine = ExecutionEngine(self)
        await engine.execute(context)


class ExecutionEngine:
    def __init__(self, graph: ExecutionGraph, parallel: bool = False):
        self.graph = graph
        self.parallel = parallel
        self.state = EngineState.IDLE
        self._completed_ids = set()
        self._recovering = set()

    async def execute(self, context=None):
        self.state = EngineState.RUNNING
        order = _topological_sort(self.graph.nodes, self.graph.edges)
        log.info("Engine: executing %d nodes (%s)", len(order), ", ".join(order))

        await self._publish_graph_event("graph_started", order)

        for retry_cycle in range(MAX_RETRY_CYCLES + 1):
            if retry_cycle > 0:
                pending = [nid for nid, n in self.graph.nodes.items()
                           if hasattr(n, "state") and n.state in ("pending", NodeState.PENDING, NodeState.RETRYING)
                           and nid not in self._completed_ids]
                if not pending:
                    break
                log.info("Engine: retry cycle %d — re-dispatching %d pending nodes", retry_cycle, len(pending))
                order = _topological_sort(self.graph.nodes, self.graph.edges)

            for node_id in order:
                node = self.graph.nodes.get(node_id)
                if node is None:
                    log.warning("Engine: node %s not found, skipping", node_id)
                    continue
                node_state = getattr(node, "state", None)
                if node_state in (NodeState.COMPLETED, "completed") or node_id in self._completed_ids:
                    continue
                if node_state in (NodeState.SKIPPED, "skipped"):
                    continue
                if node_state in (NodeState.BLOCKED, "blocked"):
                    continue
                if node_state in (NodeState.FAILED, "failed") and retry_cycle == 0:
                    pass
                elif node_state in (NodeState.FAILED, "failed"):
                    continue

                await self._transition_node(node, NodeState.RUNNING)
                try:
                    await self._execute_node(node, context)
                    await self._transition_node(node, NodeState.COMPLETED)
                    self._completed_ids.add(node_id)
                except Exception as ex:
                    log.error("Engine: node %s failed: %s", node_id, ex)
                    node.error = str(ex)
                    await self._transition_node(node, NodeState.FAILED, str(ex))
                    # Wire RecoveryEngine into the main execution path
                    # The RecoveryEngine is subscribed to the event bus via subscribe_all.
                    # Directly call handle_failure with the graph's recovery_engine
                    # so recovery can proceed synchronously before we set state.
                    if self.graph.recovery_engine and node_id not in self._recovering:
                        self._recovering.add(node_id)
                        try:
                            await self.graph.recovery_engine.handle_failure(
                                node_id, str(ex)
                            )
                            # Check if recovery moved node to RETRYING or PENDING
                            recovered_node = self.graph.nodes.get(node_id)
                            if recovered_node:
                                recovered_state = getattr(recovered_node, 'state', None)
                                if recovered_state in (
                                    NodeState.RETRYING,
                                    NodeState.PENDING,
                                    'retrying', 'pending',
                                ):
                                    log.info(
                                        "Engine: recovery initiated for %s - "
                                        "will retry in next cycle", node_id
                                    )
                                    continue  # Don't set FAILED — retry cycle will pick it up
                        except Exception as rec_err:
                            log.error(
                                "Engine: recovery handler failed for %s: %s",
                                node_id, rec_err,
                            )
                        finally:
                            self._recovering.discard(node_id)
                    # Only set FAILED if recovery didn't handle it
                    self.state = EngineState.FAILED

        if self.state == EngineState.RUNNING:
            self.state = EngineState.COMPLETED
        elif self.state == EngineState.FAILED:
            remaining = sum(1 for nid, n in self.graph.nodes.items()
                           if hasattr(n, "state") and n.state in ("pending", NodeState.PENDING)
                           and nid not in self._completed_ids)
            if remaining == 0:
                self.state = EngineState.COMPLETED
                log.info("Engine: all nodes completed after retry cycles")

        log.info("Engine: execution %s (%d nodes)", self.state.value, len(order))
        await self._publish_graph_event("graph_completed", order)

    async def _transition_node(self, node, target_state, reason=""):
        if hasattr(node, "state"):
            from_state = node.state
            if isinstance(target_state, Enum):
                node.state = target_state
            elif hasattr(type(node.state), "__members__") and target_state in type(node.state).__members__:
                node.state = type(node.state)(target_state)
            else:
                node.state = target_state
            if hasattr(node, "add_history"):
                node.add_history(from_state, node.state, reason)
        if self.graph.bus:
            from runtime_next.models.events import NodeTransitionEvent
            from_str = from_state.value if isinstance(from_state, Enum) else str(from_state)
            to_str = node.state.value if isinstance(node.state, Enum) else str(node.state)
            event = NodeTransitionEvent(
                id=f"trans_{getattr(node, 'id', 'unknown')}_{int(time.time())}",
                node_id=getattr(node, "id", ""),
                from_state=from_str,
                to_state=to_str,
                reason=reason,
            )
            await self.graph.bus.publish(event)

    async def _publish_graph_event(self, event_type, order):
        if not self.graph.bus:
            return
        from runtime_next.models.events import GraphEvent, EventType as ET
        type_map = {"graph_started": ET.GRAPH_STARTED, "graph_completed": ET.GRAPH_COMPLETED}
        evt_type = type_map.get(event_type)
        if evt_type is None:
            return
        failed = sum(1 for nid, n in self.graph.nodes.items()
                     if hasattr(n, "state") and n.state in ("failed", NodeState.FAILED))
        completed = sum(1 for nid, n in self.graph.nodes.items()
                        if hasattr(n, "state") and n.state in ("completed", NodeState.COMPLETED))
        event = GraphEvent(
            id=f"graph_{int(time.time())}",
            type=evt_type,
            graph_id="",
            node_count=len(self.graph.nodes),
            completed_count=completed,
            failed_count=failed,
        )
        await self.graph.bus.publish(event)

    async def _execute_node(self, node, context):
        result = None
        max_attempts = max(1, getattr(node, "retry_budget", 1))

        for attempt in range(max_attempts):
            try:
                if hasattr(node, "run_func") and node.run_func:
                    if asyncio.iscoroutinefunction(node.run_func):
                        result = await node.run_func()
                    else:
                        result = node.run_func()
                elif self.graph.runner:
                    log.info("Engine: running node %s via runner (attempt %d/%d)", node.id, attempt + 1, max_attempts)
                    result = await self.graph.runner.run_node(node, context or {})
                else:
                    log.info("Engine: no runner for node %s, returning default", node.id)
                    result = {"status": "success", "output": f"Node {node.id} executed"}
                node.result = result
                return result
            except Exception as e:
                node.retry_count = attempt + 1
                if attempt < max_attempts - 1:
                    delay = getattr(node, "next_backoff", lambda: 1.0)()
                    log.warning("Engine: node %s failed (attempt %d/%d), retrying in %.1fs: %s", node.id, attempt + 1, max_attempts, delay, e)
                    await asyncio.sleep(delay)
                else:
                    raise
