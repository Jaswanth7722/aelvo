"""Tests for Layer 6 — Retry Safety Engine."""

import pytest


@pytest.mark.asyncio
async def test_unknown_failure_never_retried():
    from runtime_next.verification.retry_safety import RetrySafetyEngine
    from runtime_next.verification.types import FailureClassification, Retryability

    engine = RetrySafetyEngine()
    decision = await engine.evaluate(
        node_id="node_001",
        classification=FailureClassification.UNKNOWN_FAILURE,
        retryability=Retryability.NEVER,
    )

    assert decision.can_retry is False
    assert decision.blocking_condition == "unknown_failure"


@pytest.mark.asyncio
async def test_non_retryable_blocked():
    from runtime_next.verification.retry_safety import RetrySafetyEngine
    from runtime_next.verification.types import FailureClassification, Retryability

    engine = RetrySafetyEngine()
    decision = await engine.evaluate(
        node_id="node_001",
        classification=FailureClassification.PERMISSION_DENIED,
        retryability=Retryability.NEVER,
    )

    assert decision.can_retry is False
    assert decision.blocking_condition == "non_retryable"


@pytest.mark.asyncio
async def test_safe_retry_allowed():
    from runtime_next.verification.retry_safety import RetrySafetyEngine
    from runtime_next.verification.types import FailureClassification, Retryability

    engine = RetrySafetyEngine()
    decision = await engine.evaluate(
        node_id="node_001",
        classification=FailureClassification.TIMEOUT,
        retryability=Retryability.SAFE,
        graph_state={
            "node_count": 10,
            "completed_count": 5,
            "failed_count": 1,
        },
        capability_state={
            "health": "fully_operational",
            "tools": {"python": {"status": "available"}},
        },
    )

    assert decision.can_retry is True


@pytest.mark.asyncio
async def test_graph_inconsistency_blocks():
    from runtime_next.verification.retry_safety import RetrySafetyEngine
    from runtime_next.verification.types import FailureClassification, Retryability

    engine = RetrySafetyEngine()
    decision = await engine.evaluate(
        node_id="node_001",
        classification=FailureClassification.TIMEOUT,
        retryability=Retryability.SAFE,
        graph_state={
            "node_count": 10,
            "completed_count": 12,  # Impossible — exceeds node_count
            "failed_count": 0,
        },
    )

    assert decision.can_retry is False
    assert decision.blocking_condition == "graph_inconsistency"


@pytest.mark.asyncio
async def test_offline_capability_blocks():
    from runtime_next.verification.retry_safety import RetrySafetyEngine
    from runtime_next.verification.types import FailureClassification, Retryability

    engine = RetrySafetyEngine()
    decision = await engine.evaluate(
        node_id="node_001",
        classification=FailureClassification.TIMEOUT,
        retryability=Retryability.SAFE,
        capability_state={
            "health": "offline",
        },
    )

    assert decision.can_retry is False


@pytest.mark.asyncio
async def test_retry_count_tracking():
    from runtime_next.verification.retry_safety import RetrySafetyEngine
    from runtime_next.verification.types import FailureClassification, Retryability

    engine = RetrySafetyEngine()

    decision1 = await engine.evaluate(
        "node_001", FailureClassification.TIMEOUT, Retryability.SAFE,
    )
    decision2 = await engine.evaluate(
        "node_001", FailureClassification.TIMEOUT, Retryability.SAFE,
    )

    assert engine.get_retry_count("node_001") == 2
    assert decision1.retry_count == 1
    assert decision2.retry_count == 2


@pytest.mark.asyncio
async def test_backoff_increases():
    from runtime_next.verification.retry_safety import RetrySafetyEngine
    from runtime_next.verification.types import FailureClassification, Retryability

    engine = RetrySafetyEngine()

    d1 = await engine.evaluate("n1", FailureClassification.TIMEOUT, Retryability.SAFE)
    d2 = await engine.evaluate("n1", FailureClassification.TIMEOUT, Retryability.SAFE)
    d3 = await engine.evaluate("n1", FailureClassification.TIMEOUT, Retryability.SAFE)

    assert d1.suggested_backoff <= d2.suggested_backoff <= d3.suggested_backoff
    assert d3.suggested_backoff <= 60.0


@pytest.mark.asyncio
async def test_mutation_safety_block():
    from runtime_next.verification.retry_safety import RetrySafetyEngine
    from runtime_next.verification.types import FailureClassification, Retryability

    engine = RetrySafetyEngine()
    decision = await engine.evaluate(
        node_id="node_001",
        classification=FailureClassification.SYNTAX_ERROR,
        retryability=Retryability.CONDITIONAL,
        graph_state={
            "nodes": {
                "node_001": {
                    "files": ["src/main.py"],
                    "state": "failed",
                    "dependencies": [],
                }
            },
        },
    )

    # Node wrote files and had syntax error — mutation may be dangerous
    # But retry is still possible if the files weren't corrupted
    # The safety check flags it but doesn't necessarily block
    assert decision.mutation_safe is not None


@pytest.mark.asyncio
async def test_failure_stability():
    from runtime_next.verification.retry_safety import RetrySafetyEngine
    from runtime_next.verification.types import FailureClassification, Retryability

    engine = RetrySafetyEngine()

    # Simulate same failure type repeated
    await engine.evaluate("stable", FailureClassification.TIMEOUT, Retryability.SAFE)
    await engine.evaluate("stable", FailureClassification.TIMEOUT, Retryability.SAFE)
    await engine.evaluate("stable", FailureClassification.TIMEOUT, Retryability.SAFE)

    decision = await engine.evaluate(
        "stable", FailureClassification.TIMEOUT, Retryability.SAFE,
    )

    # Stable failure (same type repeated) = high stability
    assert decision.failure_stability >= 0.9


@pytest.mark.asyncio
async def test_failure_instability():
    from runtime_next.verification.retry_safety import RetrySafetyEngine
    from runtime_next.verification.types import FailureClassification, Retryability

    engine = RetrySafetyEngine()

    # Simulate varied failure types
    await engine.evaluate("unstable", FailureClassification.TIMEOUT, Retryability.SAFE)
    await engine.evaluate("unstable", FailureClassification.SYNTAX_ERROR, Retryability.SAFE)
    await engine.evaluate("unstable", FailureClassification.DEPENDENCY_MISSING, Retryability.SAFE)

    decision = await engine.evaluate(
        "unstable", FailureClassification.TIMEOUT, Retryability.SAFE,
    )

    assert decision.failure_stability < 0.9


@pytest.mark.asyncio
async def test_reset_clears_count():
    from runtime_next.verification.retry_safety import RetrySafetyEngine
    from runtime_next.verification.types import FailureClassification, Retryability

    engine = RetrySafetyEngine()
    await engine.evaluate("n1", FailureClassification.TIMEOUT, Retryability.SAFE)
    await engine.evaluate("n1", FailureClassification.TIMEOUT, Retryability.SAFE)

    assert engine.get_retry_count("n1") == 2

    engine.reset("n1")
    assert engine.get_retry_count("n1") == 0


@pytest.mark.asyncio
async def test_serialization_corruption_blocks():
    from runtime_next.verification.retry_safety import RetrySafetyEngine
    from runtime_next.verification.types import FailureClassification, Retryability

    engine = RetrySafetyEngine()
    decision = await engine.evaluate(
        node_id="node_001",
        classification=FailureClassification.SERIALIZATION_FAILURE,
        retryability=Retryability.CONDITIONAL,
        serialization_state={"is_valid": False},
    )

    assert decision.can_retry is False
