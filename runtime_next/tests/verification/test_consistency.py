"""Tests for Layer 7 — Runtime Consistency Validation."""

import pytest
from datetime import datetime, timezone


@pytest.mark.asyncio
async def test_validate_all_passes():
    from runtime_next.verification.consistency import (
        RuntimeConsistencyValidator,
    )

    validator = RuntimeConsistencyValidator()
    result = await validator.validate_all(
        graph_state={
            "nodes": {
                "n1": {
                    "state": "completed",
                    "end_time": "2024-01-01T00:00:00",
                    "dependencies": [],
                },
                "n2": {
                    "state": "pending",
                    "dependencies": ["n1"],
                },
            },
            "edges": [
                {"source_node_id": "n1", "target_node_id": "n2"},
            ],
        },
        capability_state={
            "health": "fully_operational",
            "timestamp": datetime.now(timezone.utc),
        },
    )

    assert result.is_consistent is True
    assert len(result.checks_performed) >= 3
    assert result.graph_integrity is True
    assert result.capability_freshness is True


@pytest.mark.asyncio
async def test_validate_graph_integrity_violation():
    from runtime_next.verification.consistency import (
        RuntimeConsistencyValidator,
    )

    validator = RuntimeConsistencyValidator()
    result = await validator.validate_all(
        graph_state={
            "nodes": {
                "n1": {
                    "state": "invalid_state_xyz",
                    "dependencies": [],
                },
            },
            "edges": [],
        },
    )

    assert result.is_consistent is False
    assert len(result.violations) >= 1
    assert any(
        "invalid_state_xyz" in str(v) for v in result.violations
    )


@pytest.mark.asyncio
async def test_edge_to_nonexistent_node():
    from runtime_next.verification.consistency import (
        RuntimeConsistencyValidator,
    )

    validator = RuntimeConsistencyValidator()
    result = await validator.validate_all(
        graph_state={
            "nodes": {"n1": {"state": "completed", "end_time": "..."}},
            "edges": [{"source_node_id": "n1", "target_node_id": "n2"}],
        },
    )

    assert result.is_consistent is False
    assert result.graph_integrity is False


@pytest.mark.asyncio
async def test_offline_capability():
    from runtime_next.verification.consistency import (
        RuntimeConsistencyValidator,
    )

    validator = RuntimeConsistencyValidator()
    result = await validator.validate_all(
        capability_state={
            "health": "offline",
            "timestamp": datetime(2024, 1, 1, 0, 0, 0),
        },
    )

    assert result.is_consistent is False
    assert result.capability_freshness is False


@pytest.mark.asyncio
async def test_dependency_cycle_detection():
    from runtime_next.verification.consistency import (
        RuntimeConsistencyValidator,
    )

    validator = RuntimeConsistencyValidator()
    result = await validator.validate_all(
        graph_state={
            "nodes": {
                "n1": {"state": "pending", "dependencies": ["n2"]},
                "n2": {"state": "pending", "dependencies": ["n1"]},
            },
            "edges": [
                {"source_node_id": "n1", "target_node_id": "n2"},
                {"source_node_id": "n2", "target_node_id": "n1"},
            ],
        },
    )

    assert result.is_consistent is False
    assert result.dependency_validity is False
    assert any(
        "cycle" in str(v.get("detail", "")).lower()
        or "circular" in str(v.get("detail", "")).lower()
        for v in result.violations
    )


@pytest.mark.asyncio
async def test_replay_divergence_detection():
    from runtime_next.verification.consistency import (
        RuntimeConsistencyValidator,
    )

    validator = RuntimeConsistencyValidator()
    result = await validator.validate_all(
        replay_state={
            "expected_hash": "abc123def456",
            "actual_hash": "xyz789",
            "divergent_nodes": ["n1", "n2"],
        },
    )

    assert result.is_consistent is False
    assert result.replay_consistency is False


def test_snapshot_hashing():
    from runtime_next.verification.consistency import (
        RuntimeConsistencyValidator,
    )

    validator = RuntimeConsistencyValidator()
    state = {"nodes": {"n1": {"state": "completed"}}}

    hash1 = validator.take_snapshot_hash("test", state)
    assert isinstance(hash1, str)
    assert len(hash1) == 64  # SHA256

    # Same state should match
    assert validator.verify_snapshot_hash("test", state) is True

    # Different state should not match
    assert (
        validator.verify_snapshot_hash(
            "test", {"nodes": {"n1": {"state": "failed"}}}
        )
        is False
    )


@pytest.mark.asyncio
async def test_check_history():
    from runtime_next.verification.consistency import (
        RuntimeConsistencyValidator,
    )

    validator = RuntimeConsistencyValidator()

    await validator.validate_all()
    await validator.validate_all()

    assert len(validator.check_history) == 2


@pytest.mark.asyncio
async def test_is_consistently_healthy():
    from runtime_next.verification.consistency import (
        RuntimeConsistencyValidator,
    )

    validator = RuntimeConsistencyValidator()

    await validator.validate_all()  # Empty state = consistent
    await validator.validate_all()

    assert validator.is_consistently_healthy(2) is True
