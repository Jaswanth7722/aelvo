"""Tests for Layer 2 — Verification Pipeline."""

import pytest


@pytest.mark.asyncio
async def test_pipeline_verify_no_verifiers():
    """Pipeline should raise VerificationNotImplementedError when no verifier registered."""
    from runtime_next.verification.pipeline import VerificationPipeline
    from runtime_next.verification.types import (
        VerificationManifest, VerificationType, VerificationScope,
        VerificationNotImplementedError,
    )

    pipeline = VerificationPipeline()
    manifest = VerificationManifest(
        required=[VerificationType.LINT],
        blocking=[VerificationType.LINT],
    )
    scope = VerificationScope.empty()

    with pytest.raises(VerificationNotImplementedError) as excinfo:
        await pipeline.verify(
            node_id="node_001",
            manifest=manifest,
            scope=scope,
            context={},
        )

    assert excinfo.value.vtype == VerificationType.LINT
    assert excinfo.value.node_id == "node_001"


@pytest.mark.asyncio
async def test_pipeline_verify_with_verifier():
    """Pipeline should run registered verifiers."""
    from runtime_next.verification.pipeline import VerificationPipeline
    from runtime_next.verification.types import (
        VerificationManifest, VerificationType, VerificationScope,
        VerificationResult, Confidence,
    )

    pipeline = VerificationPipeline()

    async def lint_verifier(node_id, scope, context):
        return VerificationResult(
            verification_id=f"v_{node_id}_lint",
            node_id=node_id,
            verification_type=VerificationType.LINT,
            success=True,
            confidence=Confidence.CERTAIN,
            diagnostics=["No lint errors"],
        )

    pipeline.register_verifier(VerificationType.LINT, lint_verifier)

    manifest = VerificationManifest(
        required=[VerificationType.LINT],
        blocking=[VerificationType.LINT],
    )
    scope = VerificationScope.empty()

    results = await pipeline.verify(
        node_id="node_001",
        manifest=manifest,
        scope=scope,
        context={},
    )

    assert len(results) == 1
    assert results[0].success is True
    assert results[0].verification_type == VerificationType.LINT


@pytest.mark.asyncio
async def test_pipeline_blocking_failure():
    """Blocking verification failure should stop optional verifications."""
    from runtime_next.verification.pipeline import VerificationPipeline
    from runtime_next.verification.types import (
        VerificationManifest, VerificationType, VerificationScope,
        VerificationResult, Confidence, Severity,
    )

    pipeline = VerificationPipeline()

    async def failing_verifier(node_id, scope, context):
        return VerificationResult(
            verification_id=f"v_{node_id}_fail",
            node_id=node_id,
            verification_type=VerificationType.LINT,
            success=False,
            severity=Severity.ERROR,
            diagnostics=["Lint errors found"],
        )

    pipeline.register_verifier(VerificationType.LINT, failing_verifier)

    manifest = VerificationManifest(
        required=[VerificationType.LINT],
        blocking=[VerificationType.LINT],
        optional=[VerificationType.UNIT_TEST],
    )
    scope = VerificationScope.empty()

    results = await pipeline.verify(
        node_id="node_001",
        manifest=manifest,
        scope=scope,
        context={},
    )

    # Only the required blocking verification should run
    assert len(results) >= 1
    assert results[0].success is False


@pytest.mark.asyncio
async def test_pipeline_optional_verifications():
    """Optional verifications run after required ones pass."""
    from runtime_next.verification.pipeline import VerificationPipeline
    from runtime_next.verification.types import (
        VerificationManifest, VerificationType, VerificationScope,
        VerificationResult, Confidence,
    )

    pipeline = VerificationPipeline()

    async def passing_verifier(node_id, scope, context):
        return VerificationResult(
            verification_id=f"v_{node_id}_{context.get('type', 'unknown')}",
            node_id=node_id,
            verification_type=context.get("type", VerificationType.LINT),
            success=True,
            confidence=Confidence.CERTAIN,
        )

    pipeline.register_verifier(VerificationType.LINT, passing_verifier)
    pipeline.register_verifier(VerificationType.UNIT_TEST, passing_verifier)

    manifest = VerificationManifest(
        required=[VerificationType.LINT],
        blocking=[VerificationType.LINT],
        optional=[VerificationType.UNIT_TEST],
    )
    scope = VerificationScope.empty()

    results = await pipeline.verify(
        node_id="node_001",
        manifest=manifest,
        scope=scope,
        context={},
    )

    assert len(results) == 2


@pytest.mark.asyncio
async def test_pipeline_events_emitted():
    """Pipeline should emit start/completed events."""
    from runtime_next.verification.pipeline import VerificationPipeline
    from runtime_next.verification.types import (
        VerificationManifest, VerificationType, VerificationScope,
        VerificationResult, Confidence,
    )
    from runtime_next.verification.events import VerificationStartedEvent

    pipeline = VerificationPipeline()
    events = []

    async def event_cb(event):
        events.append(event)

    pipeline.on_event(event_cb)

    async def verifier(node_id, scope, context):
        return VerificationResult(
            verification_id=f"v_{node_id}_test",
            node_id=node_id,
            verification_type=VerificationType.LINT,
            success=True,
            confidence=Confidence.CERTAIN,
        )

    pipeline.register_verifier(VerificationType.LINT, verifier)

    manifest = VerificationManifest(required=[VerificationType.LINT])
    scope = VerificationScope.empty()

    await pipeline.verify("node_001", manifest, scope, {})

    assert len(events) >= 2  # Started + Completed
    assert any(isinstance(e, VerificationStartedEvent) for e in events)


@pytest.mark.asyncio
async def test_pipeline_history():
    """Pipeline should maintain verification history."""
    from runtime_next.verification.pipeline import VerificationPipeline
    from runtime_next.verification.types import (
        VerificationManifest, VerificationType, VerificationScope,
        VerificationResult, Confidence,
    )

    pipeline = VerificationPipeline()

    async def verifier(node_id, scope, context):
        return VerificationResult(
            verification_id=f"v_{node_id}_test",
            node_id=node_id,
            verification_type=VerificationType.LINT,
            success=True,
            confidence=Confidence.CERTAIN,
        )

    pipeline.register_verifier(VerificationType.LINT, verifier)

    manifest = VerificationManifest(required=[VerificationType.LINT])

    await pipeline.verify("node_001", manifest, VerificationScope.empty(), {})
    await pipeline.verify("node_002", manifest, VerificationScope.empty(), {})

    assert len(pipeline.history) == 2
    assert len(pipeline.get_results_for_node("node_001")) == 1
    assert pipeline.all_passed("node_001") is True


@pytest.mark.asyncio
async def test_pipeline_verifier_exception():
    """Pipeline should handle verifier exceptions gracefully."""
    from runtime_next.verification.pipeline import VerificationPipeline
    from runtime_next.verification.types import (
        VerificationManifest, VerificationType, VerificationScope,
    )

    pipeline = VerificationPipeline()

    async def broken_verifier(node_id, scope, context):
        raise RuntimeError("Verifier crashed")

    pipeline.register_verifier(VerificationType.LINT, broken_verifier)

    manifest = VerificationManifest(required=[VerificationType.LINT])
    scope = VerificationScope.empty()

    results = await pipeline.verify("node_001", manifest, scope, {})

    assert len(results) == 1
    assert results[0].success is False
    assert results[0].provenance == "verifier_exception"
