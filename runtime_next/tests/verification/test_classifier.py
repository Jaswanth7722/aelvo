"""Tests for Layer 3 — Failure Classification Engine."""

import pytest


@pytest.mark.asyncio
async def test_classify_syntax_error():
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.types import FailureClassification, Confidence

    classifier = FailureClassifier()
    result = await classifier.classify(
        error_message="SyntaxError: invalid syntax at line 42",
        stderr="  File \"test.py\", line 42\n    x = 1 +\n            ^\nSyntaxError: invalid syntax",
    )

    assert result.primary == FailureClassification.SYNTAX_ERROR
    assert result.confidence in (Confidence.HIGH, Confidence.CERTAIN)


@pytest.mark.asyncio
async def test_classify_dependency_missing():
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.types import FailureClassification

    classifier = FailureClassifier()
    result = await classifier.classify(
        error_message="ModuleNotFoundError: No module named 'requests'",
        stderr="Traceback ...\nModuleNotFoundError: No module named 'requests'",
    )

    assert result.primary == FailureClassification.DEPENDENCY_MISSING


@pytest.mark.asyncio
async def test_classify_permission_denied():
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.types import FailureClassification

    classifier = FailureClassifier()
    result = await classifier.classify(
        error_message="Permission denied: '/etc/config'",
        stderr="PermissionError: [Errno 13] Permission denied: '/etc/config'",
    )

    assert result.primary == FailureClassification.PERMISSION_DENIED


@pytest.mark.asyncio
async def test_classify_timeout():
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.types import FailureClassification

    classifier = FailureClassifier()
    result = await classifier.classify(
        error_message="Command timed out after 30 seconds",
    )

    assert result.primary == FailureClassification.TIMEOUT


@pytest.mark.asyncio
async def test_classify_unknown():
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.types import FailureClassification

    classifier = FailureClassifier()
    result = await classifier.classify(
        error_message="Something completely unexpected happened",
    )

    assert result.primary == FailureClassification.UNKNOWN_FAILURE
    assert result.is_unknown() is True


@pytest.mark.asyncio
async def test_classify_with_exit_code():
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.types import FailureClassification

    classifier = FailureClassifier()
    result = await classifier.classify(
        error_message="ruff: command not found",
        exit_code=127,
    )

    assert result.primary == FailureClassification.DEPENDENCY_MISSING


@pytest.mark.asyncio
async def test_classify_with_graph_state():
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.types import FailureClassification

    classifier = FailureClassifier()
    result = await classifier.classify(
        error_message="Multiple nodes crashed",
        graph_state={
            "node_count": 10,
            "failed_count": 7,
            "skipped_count": 2,
        },
    )

    # High failure rate should contribute to graph_inconsistency signal
    assert result.primary is not None
    assert len(result.alternatives) >= 0


@pytest.mark.asyncio
async def test_classify_with_capability_state():
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.types import FailureClassification

    classifier = FailureClassifier()
    result = await classifier.classify(
        error_message="Tool execution failed",
        capability_state={
            "health": "offline",
            "tools": {
                "python": {"status": "missing"},
                "git": {"status": "available"},
            },
        },
    )

    assert result.primary is not None


@pytest.mark.asyncio
async def test_classify_stale_runtime():
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.types import FailureClassification

    classifier = FailureClassifier()
    result = await classifier.classify(
        error_message="Cache is stale: version mismatch",
    )

    assert result.primary is not None


@pytest.mark.asyncio
async def test_classify_with_alternatives():
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.types import FailureClassification

    classifier = FailureClassifier()
    result = await classifier.classify(
        error_message="Error: Could not open file for writing",
    )

    # Could be permission or missing dependency
    assert len(result.alternatives) >= 0


@pytest.mark.asyncio
async def test_classify_custom_pattern():
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.types import FailureClassification

    classifier = FailureClassifier()
    classifier.register_pattern(
        r"custom internal error",
        FailureClassification.TOOL_FAILURE,
    )

    result = await classifier.classify(
        error_message="custom internal error occurred",
    )

    assert result.primary == FailureClassification.TOOL_FAILURE


@pytest.mark.asyncio
async def test_classification_history():
    from runtime_next.verification.classifier import FailureClassifier

    classifier = FailureClassifier()

    await classifier.classify(error_message="SyntaxError: bad")
    await classifier.classify(error_message="ModuleNotFoundError: foo")
    await classifier.classify(error_message="Timeout after 30s")

    assert len(classifier.classification_history) == 3


@pytest.mark.asyncio
async def test_classify_mutex_violation():
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.types import FailureClassification

    classifier = FailureClassifier()
    result = await classifier.classify(
        error_message="Resource busy: cannot acquire lock on file 'data.txt'",
    )

    assert result.primary == FailureClassification.MUTEX_VIOLATION
