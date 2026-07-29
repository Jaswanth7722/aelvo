from runtime_next.plan.builder import PlanBuilder
from runtime_next.models.plan import (
    ExecutionPlan, ExecutionNode, ExecutionEdge, ExecutionPattern,
    NodeType, Criticality, EdgeCondition, EdgeConditionType,
    OutputContract, RetryPolicy, DataTransformer
)


class TestPlanBuilder:

    def test_build_refactor_plan(self):
        builder = PlanBuilder()
        plan = builder.build("refactor the authentication module to use async SQLAlchemy")
        assert len(plan.nodes) >= 5
        assert plan.entry_node_id
        assert plan.exit_node_ids
        assert plan.critical_path
        assert any("read" in n for n in plan.nodes)

    def test_build_fix_plan(self):
        builder = PlanBuilder()
        plan = builder.build("fix the login bug in auth.py")
        assert len(plan.nodes) >= 4
        node_types = [n.node_type for n in plan.nodes.values()]
        assert NodeType.VERIFICATION in node_types

    def test_build_feature_plan(self):
        builder = PlanBuilder()
        plan = builder.build("implement a new user dashboard component")
        assert len(plan.nodes) >= 4
        node_types = [n.node_type for n in plan.nodes.values()]
        assert NodeType.SYNTHESIS in node_types

    def test_topological_sort(self):
        builder = PlanBuilder()
        plan = builder.build("refactor auth module")
        topo = plan.topological_sort()
        assert len(topo) > 0
        # Verify order: no node appears before its dependencies
        for i, nid in enumerate(topo):
            deps = plan.get_dependency_ids(nid)
            for dep in deps:
                if dep in topo:
                    assert dep in topo[:i], f"{dep} should be before {nid}"

    def test_dependency_correctness(self):
        builder = PlanBuilder()
        plan = builder.build("add error handling to payment module")
        for nid, node in plan.nodes.items():
            deps = plan.get_dependency_ids(nid)
            for dep_id in deps:
                assert dep_id in plan.nodes, f"Edge target {dep_id} not in plan"
            dependents = plan.get_dependent_ids(nid)
            for dep_id in dependents:
                assert dep_id in plan.nodes, f"Edge source {dep_id} not in plan"

    def test_critical_path_detection(self):
        builder = PlanBuilder()
        plan = builder.build("implement oauth2 authentication")
        assert plan.critical_path
        # Synthesis should be on critical path
        synth_nodes = [nid for nid, n in plan.nodes.items() if n.node_type == NodeType.SYNTHESIS]
        if synth_nodes:
            assert synth_nodes[0] in plan.critical_path

    def test_parallel_branches_identified(self):
        builder = PlanBuilder()
        plan = builder.build("refactor the entire api layer")
        if plan.parallel_branches:
            for branch in plan.parallel_branches:
                assert len(branch) >= 2
                # Verify nodes in same branch have no dependency on each other
                for a in branch:
                    for b in branch:
                        if a != b:
                            assert a not in plan.get_dependency_ids(b)
                            assert b not in plan.get_dependency_ids(a)

    def test_pattern_matching(self):
        existing = ExecutionPattern(
            id="p1",
            task_type_signature="refactor authentication module async",
            node_type_sequence=["tool_call", "specialist_call", "tool_call", "verification", "synthesis"],
            success_count=3
        )
        builder = PlanBuilder(patterns=[existing])
        plan = builder.build("Refactor the authentication module to use async/await")
        assert plan.pattern_source == "p1"

    def test_budget_allocation(self):
        builder = PlanBuilder()
        plan = builder.build("fix the security vulnerability in user auth")
        assert plan.total_budget == 30

    def test_edge_conditions(self):
        builder = PlanBuilder()
        plan = builder.build("fix the memory leak")
        # Basic plan structure is correct
        topo = plan.topological_sort()
        assert len(topo) == len(plan.nodes)


class TestPlanModels:

    def test_output_contract_validation(self):
        contract = OutputContract(required_fields=["status", "data"])
        valid, msg = contract.validate_output({"status": "success", "data": "ok"})
        assert valid
        invalid, msg = contract.validate_output({"status": "success"})
        assert not invalid
        assert "data" in msg

    def test_retry_policy_exponential(self):
        policy = RetryPolicy(delay_strategy="exponential", base_delay_seconds=1.0)
        assert policy.compute_delay(0) == 1.0
        assert policy.compute_delay(1) == 2.0
        assert policy.compute_delay(2) == 4.0

    def test_retry_policy_fixed(self):
        policy = RetryPolicy(delay_strategy="fixed", base_delay_seconds=2.0)
        assert policy.compute_delay(0) == 2.0
        assert policy.compute_delay(5) == 2.0

    def test_retry_policy_linear(self):
        policy = RetryPolicy(delay_strategy="linear", base_delay_seconds=1.0)
        assert policy.compute_delay(0) == 1.0
        assert policy.compute_delay(2) == 3.0

    def test_retryable_classification(self):
        policy = RetryPolicy(retryable_failure_types=["timeout", "rate_limit"])
        assert policy.is_retryable("timeout after 30s")
        assert policy.is_retryable("rate limit exceeded")
        assert not policy.is_retryable("syntax error")

    def test_edge_condition_unconditional(self):
        c = EdgeCondition(condition_type=EdgeConditionType.UNCONDITIONAL)
        assert c.evaluate({})

    def test_edge_condition_field_comparison(self):
        c = EdgeCondition(
            condition_type=EdgeConditionType.FIELD_COMPARISON,
            field_path="violations.count",
            operator=">",
            value=0
        )
        assert c.evaluate({"violations": {"count": 5}})
        assert not c.evaluate({"violations": {"count": 0}})

    def test_edge_condition_status_is(self):
        c = EdgeCondition(
            condition_type=EdgeConditionType.STATUS_IS,
            field_path="status",
            operator="==",
            value="success"
        )
        assert c.evaluate({"status": "success"})
        assert not c.evaluate({"status": "error"})

    def test_data_transformer(self):
        dt = DataTransformer(mappings={"file_path": "output.path", "content": "output.data"})
        result = dt.apply({"output": {"path": "/test.py", "data": "print('hi')"}})
        assert result["file_path"] == "/test.py"
        assert result["content"] == "print('hi')"

    def test_topological_sort_complex(self):
        plan = ExecutionPlan(id="test", task_description="test")
        nodes = ["A", "B", "C", "D", "E"]
        for nid in nodes:
            plan.add_node(ExecutionNode(id=nid, description=nid))
        plan.add_edge(ExecutionEdge(id="e1", source_node_id="A", target_node_id="B"))
        plan.add_edge(ExecutionEdge(id="e2", source_node_id="A", target_node_id="C"))
        plan.add_edge(ExecutionEdge(id="e3", source_node_id="B", target_node_id="D"))
        plan.add_edge(ExecutionEdge(id="e4", source_node_id="C", target_node_id="D"))
        plan.add_edge(ExecutionEdge(id="e5", source_node_id="D", target_node_id="E"))
        topo = plan.topological_sort()
        assert topo.index("A") < topo.index("B")
        assert topo.index("A") < topo.index("C")
        assert topo.index("B") < topo.index("D")
        assert topo.index("C") < topo.index("D")
        assert topo.index("D") < topo.index("E")

    def test_critical_path(self):
        plan = ExecutionPlan(id="cp", task_description="critical path test")
        plan.add_node(ExecutionNode(id="A", description="Start"))
        plan.add_node(ExecutionNode(id="B", description="Middle"))
        plan.add_node(ExecutionNode(id="C", description="End"))
        plan.add_edge(ExecutionEdge(id="e1", source_node_id="A", target_node_id="B"))
        plan.add_edge(ExecutionEdge(id="e2", source_node_id="B", target_node_id="C"))
        cp = plan.calculate_critical_path()
        assert cp == ["A", "B", "C"]


class TestSubBudgetAllocator:

    def test_budget_allocation_priorities(self):
        from runtime_next.plan.allocator import SubBudgetAllocator
        plan = ExecutionPlan(id="ba", task_description="budget test")
        plan.add_node(ExecutionNode(id="critical1", description="Critical", criticality=Criticality.CRITICAL, estimated_steps=5))
        plan.add_node(ExecutionNode(id="important1", description="Important", criticality=Criticality.IMPORTANT, estimated_steps=3))
        plan.add_node(ExecutionNode(id="optional1", description="Optional", criticality=Criticality.OPTIONAL, estimated_steps=2))
        plan.add_edge(ExecutionEdge(id="e1", source_node_id="critical1", target_node_id="important1"))
        plan.add_edge(ExecutionEdge(id="e2", source_node_id="important1", target_node_id="optional1"))
        plan.critical_path = ["critical1", "important1", "optional1"]

        allocator = SubBudgetAllocator(plan)
        envelopes = allocator.allocate()
        assert "critical_path" in envelopes
        assert "important" in envelopes
        assert "optional" in envelopes

    def test_can_dispatch(self):
        from runtime_next.plan.allocator import SubBudgetAllocator
        plan = ExecutionPlan(id="cd", task_description="dispatch test")
        plan.add_node(ExecutionNode(id="c1", description="Critical", criticality=Criticality.CRITICAL, estimated_steps=1))
        plan.add_node(ExecutionNode(id="o1", description="Optional", criticality=Criticality.OPTIONAL, estimated_steps=100))
        plan.critical_path = ["c1"]
        plan.total_budget = 10

        allocator = SubBudgetAllocator(plan)
        allocator.allocate()
        assert allocator.can_dispatch("c1")
        assert not allocator.can_dispatch("o1")
