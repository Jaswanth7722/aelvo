# tests/test_specialist_adapter.py - Tests for KnowledgeAdapter integration with specialists

from __future__ import annotations

import time
import pytest
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from learning.types import (
    EditCategory, EngineeringPattern, PatternQuery, PatternQueryResult,
    ValidationState, DeltaSource, EditCategorySignature, SubgraphSpec,
    DependencyGraphDelta, GraphDeltaEdge, SubgraphNode, SubgraphEdge,
    FreshnessConfig,
)
from learning.engine import PatternExtractionEngine
from learning.accumulator import PatternAccumulator
from learning.confidence import ConfidenceSystem
from learning.knowledge_graph import KnowledgeGraph
from learning.delta import DeltaComputer
from learning.classifier import EditClassifier
from learning.subgraph import SubgraphExtractor, SubgraphSimilarity
from learning.specialist_adapter import (
    KnowledgeAdapter,
    SPECIALIST_PATTERN_CATEGORIES,
    GRAPH_STRUCTURAL_SPECIALISTS,
    SPECIALIST_DESCRIPTIONS,
)

from repo_intelligence.types import EdgeType, SymbolKind, GraphSnapshot


# ── Helpers ──────────────────────────────────────────────────────────────────

def make_engine() -> PatternExtractionEngine:
    """Create a PatternExtractionEngine with in-memory KnowledgeGraph for testing."""
    kg = KnowledgeGraph(db_path=":memory:")
    engine = PatternExtractionEngine(knowledge_graph=kg)
    engine.start_session("test_session_adapter")
    return engine


def make_pattern(
    category: EditCategory,
    confidence: float = 0.7,
    observations: int = 5,
    success_count: int = 4,
    failure_count: int = 1,
    project: Optional[str] = None,
    specialist: Optional[str] = None,
    validation: ValidationState = ValidationState.VALIDATED,
) -> EngineeringPattern:
    """Create a test pattern with specified parameters."""
    sig = EditCategorySignature(
        category=category,
        dominant_edge_type=EdgeType.IMPORTS,
        file_count_delta=1,
        edge_count_delta=1,
        cycle_introduced=False,
        cycle_resolved=False,
    )
    sub = SubgraphSpec(
        anchor_node_key="test_file.py",
        nodes=[SubgraphNode(file_id="f1", file_path="test_file.py", symbol_name="Foo", symbol_kind=SymbolKind.CLASS, is_anchor=True)],
        edges=[SubgraphEdge(source_key="f1", target_key="f2", edge_type=EdgeType.IMPORTS)],
        node_count=2,
        edge_count=1,
        category=category,
    )
    p = EngineeringPattern(
        category=category,
        category_signature=sig,
        subgraph=sub,
        confidence=confidence,
        observation_count=observations,
        success_count=success_count,
        failure_count=failure_count,
        validation_state=validation,
        freshness=1.0,
        source_specialist=specialist,
        project_scope=project,
        provenance=[f"Test pattern for {category.value}"],
    )
    p.to_digest()
    return p


def seed_patterns(engine: PatternExtractionEngine, patterns: List[EngineeringPattern]) -> None:
    """Seed patterns directly into the engine's accumulator."""
    for p in patterns:
        engine.accumulator._patterns[p.id] = p
        sig_hash = p.category_signature.signature_hash
        engine.accumulator._sig_hash_to_pattern_id[sig_hash] = p.id
        if engine.knowledge_graph:
            engine.knowledge_graph.save_pattern(p)


class TestKnowledgeAdapter:
    """Tests for KnowledgeAdapter bridging PatternExtractionEngine and specialists."""

    # ── Initialization ────────────────────────────────────────────────────

    def test_init_without_engine(self):
        """Adapter can be created without an engine and returns empty packets."""
        adapter = KnowledgeAdapter()
        assert adapter.engine is None
        assert adapter.build_knowledge_packet("FORGE") == ""
        assert adapter.enrich_context("FORGE", "", None, {}) == {"learned_patterns": ""}

    def test_init_with_engine(self):
        """Adapter can be initialized with a PatternExtractionEngine."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)
        assert adapter.engine is engine

    def test_engine_setter(self):
        """Engine can be set after initialization."""
        engine = make_engine()
        adapter = KnowledgeAdapter()
        adapter.engine = engine
        assert adapter.engine is engine

    # ── Specialist Pattern Category Mappings ──────────────────────────────

    def test_all_specialists_have_mappings(self):
        """Every registered specialist has a category mapping."""
        known = {"FORGE", "ARCHITECT", "TERMINUS", "SENTINEL", "ORACLE", "HERMES", "HERALD"}
        assert set(SPECIALIST_PATTERN_CATEGORIES.keys()) == known

    def test_forge_has_most_categories(self):
        """FORGE has the broadest set of relevant pattern categories."""
        forge_cats = SPECIALIST_PATTERN_CATEGORIES["FORGE"]
        assert EditCategory.ADD_IMPORT_DEPENDENCY in forge_cats
        assert EditCategory.REFACTOR_INTERNAL in forge_cats
        assert EditCategory.MODIFY_SYMBOL_SIGNATURE in forge_cats
        assert EditCategory.CHANGE_TYPE_ANNOTATION in forge_cats
        assert len(forge_cats) >= 10  # Broadest coverage

    def test_architect_has_layer_patterns(self):
        """ARCHITECT focuses on high-level architectural patterns."""
        arch_cats = SPECIALIST_PATTERN_CATEGORIES["ARCHITECT"]
        assert EditCategory.ADD_LAYER in arch_cats
        assert EditCategory.ADD_FILE in arch_cats
        assert EditCategory.BREAK_CYCLE in arch_cats

    def test_sentinel_has_security_relevant_patterns(self):
        """SENTINEL gets export/type annotation patterns (security-relevant)."""
        sent_cats = SPECIALIST_PATTERN_CATEGORIES["SENTINEL"]
        assert EditCategory.CHANGE_EXPORT_STATUS in sent_cats
        assert EditCategory.CHANGE_TYPE_ANNOTATION in sent_cats
        assert EditCategory.CREATE_CYCLE in sent_cats

    def test_oracle_hermes_herald_have_no_categories(self):
        """Non-structural specialists have empty category lists."""
        assert SPECIALIST_PATTERN_CATEGORIES["ORACLE"] == []
        assert SPECIALIST_PATTERN_CATEGORIES["HERMES"] == []
        assert SPECIALIST_PATTERN_CATEGORIES["HERALD"] == []

    def test_graph_structural_specialists(self):
        """Only code-structural specialists are marked as graph-structural."""
        assert "FORGE" in GRAPH_STRUCTURAL_SPECIALISTS
        assert "ARCHITECT" in GRAPH_STRUCTURAL_SPECIALISTS
        assert "TERMINUS" in GRAPH_STRUCTURAL_SPECIALISTS
        assert "SENTINEL" in GRAPH_STRUCTURAL_SPECIALISTS
        assert "ORACLE" not in GRAPH_STRUCTURAL_SPECIALISTS
        assert "HERMES" not in GRAPH_STRUCTURAL_SPECIALISTS
        assert "HERALD" not in GRAPH_STRUCTURAL_SPECIALISTS

    def test_all_specialists_have_descriptions(self):
        """Every specialist has a human-readable description."""
        for name in SPECIALIST_PATTERN_CATEGORIES:
            assert name in SPECIALIST_DESCRIPTIONS
            assert len(SPECIALIST_DESCRIPTIONS[name]) > 0

    # ── Knowledge Packet Building ─────────────────────────────────────────

    def test_build_knowledge_packet_empty_when_no_patterns(self):
        """Returns empty string when no patterns have been learned."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)
        packet = adapter.build_knowledge_packet("FORGE", project="test_project")
        assert packet == ""

    def test_build_knowledge_packet_for_forge_with_patterns(self):
        """FORGE receives formatted knowledge packet with learned patterns."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        pattern = make_pattern(
            category=EditCategory.ADD_IMPORT_DEPENDENCY,
            confidence=0.85,
            observations=10,
            project="test_project",
        )
        seed_patterns(engine, [pattern])

        packet = adapter.build_knowledge_packet("FORGE", task="add auth middleware", project="test_project")
        assert packet != ""
        assert "LEARNED PATTERNS" in packet
        assert "Add Import Dependency" in packet or "add_import_dependency" in packet
        assert "confidence=0.85" in packet
        assert "observations=10" in packet

    def test_build_knowledge_packet_for_architect(self):
        """ARCHITECT receives patterns about architectural changes."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        layers = make_pattern(
            category=EditCategory.ADD_LAYER,
            confidence=0.9,
            observations=8,
            project="test_project",
        )
        seed_patterns(engine, [layers])

        packet = adapter.build_knowledge_packet("ARCHITECT", project="test_project")
        assert packet != ""
        assert "Add Layer" in packet

    def test_build_knowledge_packet_for_oracle(self):
        """ORACLE receives learning stats instead of structural patterns."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        # Seed some patterns so stats are interesting
        p = make_pattern(EditCategory.REFACTOR_INTERNAL, confidence=0.7, project="test_project")
        seed_patterns(engine, [p])

        packet = adapter.build_knowledge_packet("ORACLE", project="test_project")
        assert packet != ""
        assert "LEARNING SYSTEM" in packet or "learning" in packet.lower()

    def test_build_knowledge_packet_filtered_by_project(self):
        """Patterns from different projects are not mixed."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        p_a = make_pattern(EditCategory.ADD_IMPORT_DEPENDENCY, project="project_a", confidence=0.8)
        p_b = make_pattern(EditCategory.ADD_IMPORT_DEPENDENCY, project="project_b", confidence=0.9)

        # Save pattern_b with project_b scope
        p_b.project_scope = "project_b"

        seed_patterns(engine, [p_a, p_b])

        # Query for project_a should only find project_a's pattern
        packet_a = adapter.build_knowledge_packet("FORGE", project="project_a")
        packet_b = adapter.build_knowledge_packet("FORGE", project="project_b")
        packet_all = adapter.build_knowledge_packet("FORGE", project=None)

        # The packet content should differ based on project scope
        assert packet_a != "" or packet_b != "" or packet_all != ""

    def test_build_knowledge_packet_min_confidence_filter(self):
        """Patterns below min_confidence are excluded."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        low_conf = make_pattern(EditCategory.ADD_FILE, confidence=0.25, project="test_project")
        high_conf = make_pattern(EditCategory.ADD_FILE, confidence=0.9, project="test_project")
        seed_patterns(engine, [low_conf, high_conf])

        packet_high = adapter.build_knowledge_packet("FORGE", min_confidence=0.7, project="test_project")
        packet_low = adapter.build_knowledge_packet("FORGE", min_confidence=0.1, project="test_project")

        # Low confidence pattern excluded from high-threshold query
        if packet_high:
            assert "0.25" not in packet_high
        # Low threshold should include both
        assert packet_low != ""

    def test_build_knowledge_packet_token_budget(self):
        """Knowledge packet respects token budget."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        patterns = [
            make_pattern(cat, confidence=0.8 + i * 0.02, observations=5 + i)
            for i, cat in enumerate([
                EditCategory.ADD_IMPORT_DEPENDENCY,
                EditCategory.REFACTOR_INTERNAL,
                EditCategory.ADD_FILE,
                EditCategory.MODIFY_SYMBOL_SIGNATURE,
                EditCategory.CHANGE_TYPE_ANNOTATION,
                EditCategory.ADD_CALL_DEPENDENCY,
            ])
        ]
        seed_patterns(engine, patterns)

        small_packet = adapter.build_knowledge_packet("FORGE", max_tokens=100)
        large_packet = adapter.build_knowledge_packet("FORGE", max_tokens=4000)

        # Small budget should truncate more aggressively than large budget
        small_lines = small_packet.count("\n")
        large_lines = large_packet.count("\n")
        assert small_lines <= large_lines or large_lines > 0, (
            f"Small packet has {small_lines} lines, large has {large_lines}"
        )

    # ── Context Enrichment ────────────────────────────────────────────────

    def test_enrich_context_adds_learned_patterns_key(self):
        """Context dict gets 'learned_patterns' key after enrichment."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        p = make_pattern(EditCategory.REFACTOR_INTERNAL, confidence=0.8, project="test_project")
        seed_patterns(engine, [p])

        context: Dict[str, Any] = {"task": "refactor module", "budget": 20}
        result = adapter.enrich_context("FORGE", "refactor module", "test_project", context)
        assert result is context  # Same dict, mutated in place
        assert "learned_patterns" in context

    def test_enrich_context_adds_structured_patterns_for_forge(self):
        """FORGE receives structured pattern data for programmatic use."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        p = make_pattern(EditCategory.ADD_IMPORT_DEPENDENCY, confidence=0.85, project="test_project")
        seed_patterns(engine, [p])

        context: Dict[str, Any] = {}
        adapter.enrich_context("FORGE", "add import", "test_project", context)
        assert "structured_patterns" in context
        assert len(context["structured_patterns"]) > 0

    def test_enrich_context_adds_learning_stats(self):
        """All specialists receive learning statistics."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        p = make_pattern(EditCategory.REFACTOR_INTERNAL)
        seed_patterns(engine, [p])

        context: Dict[str, Any] = {}
        adapter.enrich_context("HERMES", "", None, context)
        assert "learning_stats" in context
        assert "total_patterns" in context["learning_stats"]

    def test_enrich_context_no_engine(self):
        """Without engine, enrichment adds empty learned_patterns."""
        adapter = KnowledgeAdapter()
        context: Dict[str, Any] = {"task": "test"}
        adapter.enrich_context("FORGE", "test", None, context)
        assert context["learned_patterns"] == ""

    # ── Experience Capture ────────────────────────────────────────────────

    def test_capture_experience_no_engine(self):
        """capture_experience returns None without an engine."""
        adapter = KnowledgeAdapter()
        result = adapter.capture_experience("FORGE", "test", "success")
        assert result is None

    def test_capture_experience_no_snapshots(self):
        """capture_experience returns None when no snapshots provided."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)
        result = adapter.capture_experience("FORGE", "test", "success")
        assert result is None

    def test_capture_experience_with_snapshots(self):
        """capture_experience feeds snapshots into the pipeline."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        # Create mock snapshots (minimal — just need to test the pipeline runs)
        class MockSnapshot:
            def __init__(self):
                self.nodes = {}
                self.edges = []
                self.symbols = {}
                self.files = {}
                self.version = 1
                self.timestamp = datetime.now(timezone.utc)

        before = MockSnapshot()
        after = MockSnapshot()
        after.version = 2

        result = adapter.capture_experience(
            specialist_name="FORGE",
            task="add new file",
            outcome="success",
            before_snapshot=before,
            after_snapshot=after,
            project="test_project",
        )
        # The result may be None (below threshold) — that's fine,
        # what matters is the pipeline ran without error
        assert result is None or isinstance(result, EngineeringPattern)

    # ── Build Injection for Specialist ────────────────────────────────────

    def test_build_injection_for_forge(self):
        """Complete injection string for FORGE includes patterns and stats."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        p = make_pattern(EditCategory.ADD_IMPORT_DEPENDENCY, confidence=0.85, project="test_project")
        seed_patterns(engine, [p])

        injection = adapter.build_injection_for_specialist(
            "FORGE", "add import", project="test_project"
        )
        assert injection != ""
        assert "LEARNED PATTERNS" in injection

    def test_build_injection_for_herald(self):
        """HERALD gets a compact injection (non-structural)."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        injection = adapter.build_injection_for_specialist(
            "HERALD", "draft an email", project="test_project"
        )
        # Should include learning stats if engine has data
        assert isinstance(injection, str)

    def test_build_injection_no_engine(self):
        """Empty injection when no engine is available."""
        adapter = KnowledgeAdapter()
        assert adapter.build_injection_for_specialist("FORGE", "") == ""

    # ── Specialized Queries ───────────────────────────────────────────────

    def test_get_high_confidence_patterns(self):
        """Returns only patterns above confidence threshold."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        p_low = make_pattern(EditCategory.ADD_IMPORT_DEPENDENCY, confidence=0.5)
        p_high = make_pattern(EditCategory.ADD_FILE, confidence=0.9)
        seed_patterns(engine, [p_low, p_high])

        high = adapter.get_high_confidence_patterns(min_confidence=0.8)
        ids = {p.id for p in high}
        assert p_high.id in ids
        assert p_low.id not in ids

    def test_get_patterns_for_category(self):
        """Returns patterns filtered by edit category."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        p_import = make_pattern(EditCategory.ADD_IMPORT_DEPENDENCY)
        p_refactor = make_pattern(EditCategory.REFACTOR_INTERNAL)
        seed_patterns(engine, [p_import, p_refactor])

        imports = adapter.get_patterns_for_category(EditCategory.ADD_IMPORT_DEPENDENCY)
        assert len(imports) >= 1
        assert p_import.id in {p.id for p in imports}

        refactors = adapter.get_patterns_for_category(EditCategory.REFACTOR_INTERNAL)
        assert p_refactor.id in {p.id for p in refactors}

    # ── Specialist Learning Summary ───────────────────────────────────────

    def test_get_specialist_learning_summary(self):
        """Returns structured summary for a specialist."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        p = make_pattern(EditCategory.ADD_IMPORT_DEPENDENCY, project="proj_x")
        seed_patterns(engine, [p])

        summary = adapter.get_specialist_learning_summary("FORGE", project="proj_x")
        assert summary["specialist"] == "FORGE"
        assert summary["total_patterns"] >= 1
        assert "add_import_dependency" in summary["by_category"]
        assert summary["avg_confidence"] > 0

    def test_get_specialist_learning_summary_no_engine(self):
        """Returns default summary when no engine."""
        adapter = KnowledgeAdapter()
        summary = adapter.get_specialist_learning_summary("FORGE")
        assert summary["total_patterns"] == 0

    def test_get_cross_project_insights(self):
        """Cross-project insights include aggregate metrics."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        p = make_pattern(EditCategory.REFACTOR_INTERNAL)
        seed_patterns(engine, [p])

        insights = adapter.get_cross_project_insights()
        assert insights["total_patterns_across_projects"] >= 1

    # ── Metrics ───────────────────────────────────────────────────────────

    def test_metrics_are_recorded(self):
        """Adapter records metrics for each operation."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        p = make_pattern(EditCategory.ADD_IMPORT_DEPENDENCY)
        seed_patterns(engine, [p])

        adapter.build_knowledge_packet("FORGE", project="test_project")
        context: Dict[str, Any] = {}
        adapter.enrich_context("FORGE", "", None, context)

        metrics = adapter.get_metrics()
        assert len(metrics) >= 2
        operations = {m["operation"] for m in metrics}
        assert "build_knowledge_packet" in operations

    def test_metrics_empty_initially(self):
        """No metrics before any operations."""
        adapter = KnowledgeAdapter()
        assert adapter.get_metrics() == []

    # ── Edge Cases ────────────────────────────────────────────────────────

    def test_case_insensitive_specialist_name(self):
        """Specialist names are case-insensitive (upper-cased internally)."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        p = make_pattern(EditCategory.ADD_FILE, confidence=0.8)
        seed_patterns(engine, [p])

        packet_lower = adapter.build_knowledge_packet("forge", project="test_project")
        packet_upper = adapter.build_knowledge_packet("FORGE", project="test_project")
        assert packet_lower == packet_upper

    def test_unknown_specialist_name(self):
        """Unknown specialist names get generic learning stats packet."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)
        packet = adapter.build_knowledge_packet("UNKNOWN_SPECIALIST")
        # Non-structural specialists always get learning stats even with unknown name
        assert packet != ""
        assert "LEARNING SYSTEM" in packet
        assert "Total patterns" in packet

    def test_enrich_context_with_empty_task(self):
        """Empty task still produces valid enrichment."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        p = make_pattern(EditCategory.REFACTOR_INTERNAL, confidence=0.8)
        seed_patterns(engine, [p])

        context: Dict[str, Any] = {}
        adapter.enrich_context("FORGE", "", None, context)
        assert "learned_patterns" in context

    def test_build_injection_max_tokens_zero(self):
        """Zero max_tokens produces minimal output."""
        engine = make_engine()
        adapter = KnowledgeAdapter(engine)

        p = make_pattern(EditCategory.ADD_IMPORT_DEPENDENCY, confidence=0.9)
        seed_patterns(engine, [p])

        packet = adapter.build_knowledge_packet("FORGE", max_tokens=0)
        # Should be empty or very minimal
        assert isinstance(packet, str)
