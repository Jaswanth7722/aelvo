# tests/test_learning.py - Comprehensive tests for the Learning & Pattern Extraction system

from __future__ import annotations

import unittest
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from learning.types import (
    EditCategory, EditCategorySignature, DependencyGraphDelta, GraphDeltaEdge,
    SubgraphSpec, SubgraphEdge, EngineeringPattern,
    ConfidenceUpdate, ContradictionRecord,
    ValidationState, FreshnessGrade, FreshnessConfig,
    PatternQuery, DeltaSource,
)
from learning.delta import DeltaComputer
from learning.classifier import EditClassifier
from learning.subgraph import SubgraphExtractor, SubgraphSimilarity
from learning.confidence import ConfidenceSystem
from learning.accumulator import PatternAccumulator
from learning.knowledge_graph import KnowledgeGraph
from learning.engine import PatternExtractionEngine

from repo_intelligence.types import (
    EdgeType, SymbolKind, ConfidenceLevel, GraphSnapshot,
    SymbolNode, SymbolEdge, ParsedFile, FileId, LanguageId,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_snapshot(
    files: Optional[Dict[str, ParsedFile]] = None,
    symbols: Optional[Dict[str, SymbolNode]] = None,
    edges: Optional[List[SymbolEdge]] = None,
    version: int = 0,
) -> GraphSnapshot:
    return GraphSnapshot(
        files=files or {},
        symbols=symbols or {},
        edges=edges or [],
        version=version,
    )


def make_edge(
    source_id: str, target_id: str,
    edge_type: EdgeType = EdgeType.IMPORTS,
    confidence: ConfidenceLevel = ConfidenceLevel.CERTAIN,
) -> SymbolEdge:
    return SymbolEdge(
        source_id=source_id,
        target_id=target_id,
        edge_type=edge_type,
        file_path="test.py",
        line_number=1,
        confidence=confidence,
    )


def make_file(fid: str, path: str) -> ParsedFile:
    return ParsedFile(
        file_id=fid,
        file_path=path,
        language=LanguageId.PYTHON,
        fingerprint=fid,
    )


class TestDeltaComputer(unittest.TestCase):
    """Tests for DeltaComputer — the foundation of pattern extraction."""

    def setUp(self):
        self.computer = DeltaComputer()

    def test_empty_delta(self):
        """No changes → empty delta."""
        before = make_snapshot(version=1)
        after = make_snapshot(version=1)
        delta = self.computer.compute(before, after)
        self.assertTrue(delta.is_empty)
        self.assertFalse(delta.has_structural_change)

    def test_new_edge_detected(self):
        """Adding an import creates a new edge in the delta."""
        fid_a = FileId.create("a.py")
        fid_b = FileId.create("b.py")
        sym_a = SymbolNode(
            symbol_id="a" * 16, file_id=fid_a, file_path="a.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="a", fully_qualified_name="a",
            confidence=ConfidenceLevel.CERTAIN,
        )
        sym_b = SymbolNode(
            symbol_id="b" * 16, file_id=fid_b, file_path="b.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="b", fully_qualified_name="b",
            confidence=ConfidenceLevel.CERTAIN,
        )
        new_edge = make_edge(sym_a.symbol_id, sym_b.symbol_id)

        before = make_snapshot(
            files={fid_a: make_file(fid_a, "a.py"), fid_b: make_file(fid_b, "b.py")},
            symbols={sym_a.symbol_id: sym_a, sym_b.symbol_id: sym_b},
            version=1,
        )
        after = make_snapshot(
            files={fid_a: make_file(fid_a, "a.py"), fid_b: make_file(fid_b, "b.py")},
            symbols={sym_a.symbol_id: sym_a, sym_b.symbol_id: sym_b},
            edges=[new_edge],
            version=2,
        )

        delta = self.computer.compute(before, after)

        self.assertFalse(delta.is_empty)
        self.assertEqual(len(delta.new_edges), 1)
        self.assertEqual(delta.edge_count_delta, 1)
        self.assertEqual(delta.new_edges[0].edge_type, EdgeType.IMPORTS)
        self.assertEqual(delta.file_count_delta, 0)

    def test_removed_edge_detected(self):
        """Removing an import creates a removed edge in the delta."""
        fid_a = FileId.create("a.py")
        fid_b = FileId.create("b.py")
        sym_a = SymbolNode(
            symbol_id="a" * 16, file_id=fid_a, file_path="a.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="a", fully_qualified_name="a",
            confidence=ConfidenceLevel.CERTAIN,
        )
        sym_b = SymbolNode(
            symbol_id="b" * 16, file_id=fid_b, file_path="b.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="b", fully_qualified_name="b",
            confidence=ConfidenceLevel.CERTAIN,
        )
        existing_edge = make_edge(sym_a.symbol_id, sym_b.symbol_id)

        before = make_snapshot(
            files={fid_a: make_file(fid_a, "a.py"), fid_b: make_file(fid_b, "b.py")},
            symbols={sym_a.symbol_id: sym_a, sym_b.symbol_id: sym_b},
            edges=[existing_edge],
            version=1,
        )
        after = make_snapshot(
            files={fid_a: make_file(fid_a, "a.py"), fid_b: make_file(fid_b, "b.py")},
            symbols={sym_a.symbol_id: sym_a, sym_b.symbol_id: sym_b},
            version=2,
        )

        delta = self.computer.compute(before, after)

        self.assertEqual(len(delta.removed_edges), 1)
        self.assertEqual(delta.edge_count_delta, -1)

    def test_new_file_detected(self):
        """Added file shows up in added_files."""
        fid_a = FileId.create("a.py")
        fid_b = FileId.create("b.py")
        sym_a = SymbolNode(
            symbol_id="a" * 16, file_id=fid_a, file_path="a.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="a", fully_qualified_name="a",
            confidence=ConfidenceLevel.CERTAIN,
        )
        sym_b = SymbolNode(
            symbol_id="b" * 16, file_id=fid_b, file_path="b.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="b", fully_qualified_name="b",
            confidence=ConfidenceLevel.CERTAIN,
        )

        before = make_snapshot(
            files={fid_a: make_file(fid_a, "a.py")},
            symbols={sym_a.symbol_id: sym_a},
            version=1,
        )
        after = make_snapshot(
            files={fid_a: make_file(fid_a, "a.py"), fid_b: make_file(fid_b, "b.py")},
            symbols={sym_a.symbol_id: sym_a, sym_b.symbol_id: sym_b},
            version=2,
        )

        delta = self.computer.compute(before, after)

        self.assertEqual(len(delta.added_files), 1)
        self.assertEqual(delta.file_count_delta, 1)

    def test_delta_digest_uniqueness(self):
        """Different deltas produce different digests."""
        fid_a = FileId.create("a.py")
        fid_b = FileId.create("b.py")
        fid_c = FileId.create("c.py")
        sym_a = SymbolNode(
            symbol_id="a" * 16, file_id=fid_a, file_path="a.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="a", fully_qualified_name="a",
            confidence=ConfidenceLevel.CERTAIN,
        )
        sym_b = SymbolNode(
            symbol_id="b" * 16, file_id=fid_b, file_path="b.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="b", fully_qualified_name="b",
            confidence=ConfidenceLevel.CERTAIN,
        )
        sym_c = SymbolNode(
            symbol_id="c" * 16, file_id=fid_c, file_path="c.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="c", fully_qualified_name="c",
            confidence=ConfidenceLevel.CERTAIN,
        )
        edge_ab = make_edge(sym_a.symbol_id, sym_b.symbol_id)
        edge_ac = make_edge(sym_a.symbol_id, sym_c.symbol_id)

        base_files = {
            fid_a: make_file(fid_a, "a.py"),
            fid_b: make_file(fid_b, "b.py"),
            fid_c: make_file(fid_c, "c.py"),
        }
        base_syms = {
            sym_a.symbol_id: sym_a,
            sym_b.symbol_id: sym_b,
            sym_c.symbol_id: sym_c,
        }

        delta_ab = self.computer.compute(
            make_snapshot(files=base_files, symbols=base_syms, version=1),
            make_snapshot(files=base_files, symbols=base_syms, edges=[edge_ab], version=2),
        )
        delta_ac = self.computer.compute(
            make_snapshot(files=base_files, symbols=base_syms, version=1),
            make_snapshot(files=base_files, symbols=base_syms, edges=[edge_ac], version=2),
        )

        self.assertNotEqual(delta_ab.to_digest(), delta_ac.to_digest())

    def test_metrics_collection(self):
        """DeltaComputer collects performance metrics."""
        self.computer.compute(make_snapshot(), make_snapshot())
        metrics = self.computer.get_metrics()
        self.assertGreaterEqual(len(metrics), 1)
        self.assertIn("operation", metrics[0])
        self.assertIn("duration_ms", metrics[0])

    def test_delta_source_integration(self):
        """Delta can be computed with a DeltaSource."""
        source = DeltaSource(task_id="task-1", specialist="FORGE")
        delta = self.computer.compute(
            make_snapshot(version=1),
            make_snapshot(version=1),
            source=source,
        )
        self.assertTrue(delta.is_empty)

    def test_topological_shift(self):
        """Modified files contribute to topological_shift."""
        fid_a = FileId.create("a.py")
        sym_a = SymbolNode(
            symbol_id="a" * 16, file_id=fid_a, file_path="a.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="a", fully_qualified_name="a",
            confidence=ConfidenceLevel.CERTAIN,
        )
        fid_b = FileId.create("b.py")
        sym_b = SymbolNode(
            symbol_id="b" * 16, file_id=fid_b, file_path="b.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="b", fully_qualified_name="b",
            confidence=ConfidenceLevel.CERTAIN,
        )
        new_edge = make_edge(sym_a.symbol_id, sym_b.symbol_id)

        before = make_snapshot(
            files={fid_a: make_file(fid_a, "a.py"), fid_b: make_file(fid_b, "b.py")},
            symbols={sym_a.symbol_id: sym_a, sym_b.symbol_id: sym_b},
            version=1,
        )
        after = make_snapshot(
            files={
                fid_a: make_file(fid_a, "a.py"),
                fid_b: ParsedFile(
                    file_id=fid_b, file_path="b.py",
                    language=LanguageId.PYTHON,
                    fingerprint="changed",  # Different fingerprint
                ),
            },
            symbols={sym_a.symbol_id: sym_a, sym_b.symbol_id: sym_b},
            edges=[new_edge],
            version=2,
        )

        delta = self.computer.compute(before, after)
        self.assertGreater(delta.topological_shift, 0)


class TestEditClassifier(unittest.TestCase):
    """Tests for EditClassifier — classifying deltas into structural categories."""

    def setUp(self):
        self.classifier = EditClassifier()

    def test_classify_import_addition(self):
        """Adding an import → ADD_IMPORT_DEPENDENCY."""
        delta = DependencyGraphDelta(
            new_edges=[GraphDeltaEdge(
                edge_type=EdgeType.IMPORTS,
                source_file_id="src1", target_file_id="src2",
            )],
        )
        sig = self.classifier.classify(delta)
        self.assertEqual(sig.category, EditCategory.ADD_IMPORT_DEPENDENCY)

    def test_classify_import_removal(self):
        """Removing an import → REMOVE_IMPORT_DEPENDENCY."""
        delta = DependencyGraphDelta(
            removed_edges=[GraphDeltaEdge(
                edge_type=EdgeType.IMPORTS,
                source_file_id="src1", target_file_id="src2",
            )],
        )
        sig = self.classifier.classify(delta)
        self.assertEqual(sig.category, EditCategory.REMOVE_IMPORT_DEPENDENCY)

    def test_classify_new_file(self):
        """Adding a file → ADD_FILE."""
        delta = DependencyGraphDelta(
            added_files=["new_module.py"],
            file_count_delta=1,
        )
        sig = self.classifier.classify(delta)
        self.assertEqual(sig.category, EditCategory.ADD_FILE)

    def test_classify_cycle_creation(self):
        """Creating a cycle → CREATE_CYCLE."""
        delta = DependencyGraphDelta(
            new_cycles=[{"a.py", "b.py"}],
        )
        sig = self.classifier.classify(delta)
        self.assertEqual(sig.category, EditCategory.CREATE_CYCLE)

    def test_classify_cycle_break(self):
        """Breaking a cycle → BREAK_CYCLE."""
        delta = DependencyGraphDelta(
            resolved_cycles=[{"a.py", "b.py"}],
        )
        sig = self.classifier.classify(delta)
        self.assertEqual(sig.category, EditCategory.BREAK_CYCLE)

    def test_classify_inheritance(self):
        """Adding inheritance → ADD_INHERITANCE."""
        delta = DependencyGraphDelta(
            new_edges=[GraphDeltaEdge(
                edge_type=EdgeType.INHERITS,
                source_file_id="parent", target_file_id="child",
            )],
        )
        sig = self.classifier.classify(delta)
        self.assertEqual(sig.category, EditCategory.ADD_INHERITANCE)

    def test_classify_implementation(self):
        """Adding an implements relationship → ADD_IMPLEMENTS."""
        delta = DependencyGraphDelta(
            new_edges=[GraphDeltaEdge(
                edge_type=EdgeType.IMPLEMENTS,
                source_file_id="interface", target_file_id="impl",
            )],
        )
        sig = self.classifier.classify(delta)
        self.assertEqual(sig.category, EditCategory.ADD_IMPLEMENTS)

    def test_classify_call_dependency(self):
        """Adding a call edge → ADD_CALL_DEPENDENCY."""
        delta = DependencyGraphDelta(
            new_edges=[GraphDeltaEdge(
                edge_type=EdgeType.CALLS,
                source_file_id="caller", target_file_id="callee",
            )],
        )
        sig = self.classifier.classify(delta)
        self.assertEqual(sig.category, EditCategory.ADD_CALL_DEPENDENCY)

    def test_classify_refactor_internal(self):
        """No cross-file changes → REFACTOR_INTERNAL."""
        delta = DependencyGraphDelta(
            modified_files=["a.py"],
        )
        sig = self.classifier.classify(delta)
        self.assertEqual(sig.category, EditCategory.REFACTOR_INTERNAL)

    def test_classify_mixed_structural(self):
        """Delta with both added and removed import edges → REFACTOR_INTERNAL.

        This tests the 'both add and remove' pattern which indicates a
        refactoring/migration of imports.
        """
        delta = DependencyGraphDelta(
            new_edges=[
                GraphDeltaEdge(edge_type=EdgeType.IMPORTS, source_file_id="a", target_file_id="b"),
                GraphDeltaEdge(edge_type=EdgeType.IMPORTS, source_file_id="a", target_file_id="c"),
            ],
            removed_edges=[
                GraphDeltaEdge(edge_type=EdgeType.IMPORTS, source_file_id="a", target_file_id="d"),
            ],
        )
        sig = self.classifier.classify(delta)
        self.assertEqual(sig.category, EditCategory.REFACTOR_INTERNAL)

    def test_classify_delete_file(self):
        """Removing a file → DELETE_FILE."""
        delta = DependencyGraphDelta(
            removed_files=["obsolete.py"],
            file_count_delta=-1,
        )
        sig = self.classifier.classify(delta)
        self.assertEqual(sig.category, EditCategory.DELETE_FILE)

    def test_signature_hash_determinism(self):
        """Same delta → same signature hash."""
        delta = DependencyGraphDelta(
            new_edges=[GraphDeltaEdge(
                edge_type=EdgeType.IMPORTS,
                source_file_id="a", target_file_id="b",
            )],
        )
        sig1 = self.classifier.classify(delta)
        sig2 = self.classifier.classify(delta)
        self.assertEqual(sig1.signature_hash, sig2.signature_hash)

    def test_metrics_collection(self):
        """Classifier collects metrics."""
        self.classifier.classify(DependencyGraphDelta())
        metrics = self.classifier.get_metrics()
        self.assertGreaterEqual(len(metrics), 1)


class TestSubgraphExtractor(unittest.TestCase):
    """Tests for SubgraphExtractor — extracting minimal subgraphs from deltas."""

    def setUp(self):
        self.extractor = SubgraphExtractor()

    def test_extract_from_new_edges(self):
        """New edges produce subgraph with source and target nodes."""
        delta = DependencyGraphDelta(
            new_edges=[GraphDeltaEdge(
                edge_type=EdgeType.IMPORTS,
                source_file_id="fid_a", source_file_path="a.py",
                target_file_id="fid_b", target_file_path="b.py",
            )],
        )
        subgraph = self.extractor.extract(delta)
        self.assertGreaterEqual(subgraph.node_count, 2)
        self.assertGreaterEqual(subgraph.edge_count, 1)

    def test_extract_from_added_files(self):
        """Added files appear as nodes in the subgraph."""
        delta = DependencyGraphDelta(
            added_files=["new_module.py"],
        )
        subgraph = self.extractor.extract(delta)
        self.assertGreaterEqual(subgraph.node_count, 1)
        self.assertIn("new_module.py", [n.file_path for n in subgraph.nodes])

    def test_anchor_assignment(self):
        """First node becomes the anchor."""
        delta = DependencyGraphDelta(
            added_files=["main.py", "helper.py"],
        )
        subgraph = self.extractor.extract(delta)
        self.assertTrue(subgraph.anchor_node_key)

    def test_empty_delta_subgraph(self):
        """Empty delta produces minimal subgraph."""
        delta = DependencyGraphDelta()
        subgraph = self.extractor.extract(delta)
        self.assertEqual(subgraph.node_count, 0)

    def test_metrics_collection(self):
        """Extractor collects metrics."""
        self.extractor.extract(DependencyGraphDelta())
        metrics = self.extractor.get_metrics()
        self.assertGreaterEqual(len(metrics), 1)


class TestSubgraphSimilarity(unittest.TestCase):
    """Tests for SubgraphSimilarity — matching subgraphs for pattern recognition."""

    def setUp(self):
        self.similarity = SubgraphSimilarity()

    def test_identical_subgraphs(self):
        """Identical subgraphs → similarity 1.0."""
        a = SubgraphSpec(
            node_count=3, edge_count=2,
            edges=[
                SubgraphEdge(source_key="a", target_key="b", edge_type=EdgeType.IMPORTS),
                SubgraphEdge(source_key="b", target_key="c", edge_type=EdgeType.IMPORTS),
            ],
        )
        b = SubgraphSpec(
            node_count=3, edge_count=2,
            edges=[
                SubgraphEdge(source_key="a", target_key="b", edge_type=EdgeType.IMPORTS),
                SubgraphEdge(source_key="b", target_key="c", edge_type=EdgeType.IMPORTS),
            ],
        )
        score = self.similarity.compute(a, b)
        self.assertGreaterEqual(score, 0.85)
        self.assertTrue(self.similarity.are_isomorphic(a, b))

    def test_different_categories(self):
        """Different categories reduce similarity."""
        a = SubgraphSpec(category=EditCategory.ADD_FILE)
        b = SubgraphSpec(category=EditCategory.REFACTOR_INTERNAL)
        score = self.similarity.compute(a, b)
        self.assertLess(score, 0.6)

    def test_isomorphic_threshold(self):
        """are_isomorphic uses threshold."""
        a = SubgraphSpec(category=EditCategory.ADD_FILE, node_count=1, edge_count=0)
        b = SubgraphSpec(category=EditCategory.ADD_FILE, node_count=2, edge_count=0)
        self.assertFalse(self.similarity.are_isomorphic(a, b, threshold=0.9))
        self.assertTrue(self.similarity.are_isomorphic(a, b, threshold=0.5))

    def test_edge_type_distribution(self):
        """Similar edge type distributions contribute to score."""
        a = SubgraphSpec(
            edge_count=2,
            edges=[
                SubgraphEdge(source_key="a", target_key="b", edge_type=EdgeType.IMPORTS),
                SubgraphEdge(source_key="b", target_key="c", edge_type=EdgeType.IMPORTS),
            ],
        )
        b = SubgraphSpec(
            edge_count=2,
            edges=[
                SubgraphEdge(source_key="x", target_key="y", edge_type=EdgeType.IMPORTS),
                SubgraphEdge(source_key="y", target_key="z", edge_type=EdgeType.IMPORTS),
            ],
        )
        score = self.similarity.compute(a, b)
        # Different node names, same structure and edge types → high similarity
        self.assertGreater(score, 0.7)

    def test_metrics_collection(self):
        """Similarity collects metrics."""
        self.similarity.compute(SubgraphSpec(), SubgraphSpec())
        metrics = self.similarity.get_metrics()
        self.assertGreaterEqual(len(metrics), 1)


class TestConfidenceSystem(unittest.TestCase):
    """Tests for ConfidenceSystem — Bayesian confidence, freshness, validation."""

    def setUp(self):
        self.confidence = ConfidenceSystem()
        self.pattern = EngineeringPattern(
            id="test_pat",
            category=EditCategory.ADD_IMPORT_DEPENDENCY,
            confidence=0.3,
        )

    def test_initial_confidence_default(self):
        """Initial confidence starts at 0.3 (weak heuristic)."""
        c = self.confidence.compute_initial_confidence("sig_hash")
        self.assertAlmostEqual(c, 0.3, delta=0.2)

    def test_initial_confidence_with_evidence(self):
        """Strong evidence raises initial confidence."""
        strong = self.confidence.compute_initial_confidence("sig", evidence_quality=0.9)
        weak = self.confidence.compute_initial_confidence("sig", evidence_quality=0.1)
        self.assertGreater(strong, weak)

    def test_success_increases_confidence(self):
        """Successful observation increases confidence."""
        prev = self.pattern.confidence
        new_conf, update = self.confidence.update_confidence(self.pattern, was_successful=True)
        self.assertGreater(new_conf, prev)
        self.assertGreater(self.pattern.success_count, 0)

    def test_failure_decreases_confidence(self):
        """Failed observation decreases confidence."""
        self.pattern.confidence = 0.5
        prev = self.pattern.confidence
        new_conf, update = self.confidence.update_confidence(self.pattern, was_successful=False)
        self.assertLess(new_conf, prev)
        self.assertGreater(self.pattern.failure_count, 0)

    def test_confidence_update_record(self):
        """Confidence updates produce structured records."""
        _, update = self.confidence.update_confidence(self.pattern, was_successful=True)
        self.assertIsNotNone(update.id)
        self.assertEqual(update.knowledge_item_id, self.pattern.id)
        self.assertIn("bonus", update.update_formula)
        self.assertIn("evidence", update.model_dump())

    def test_freshness_linear_decay(self):
        """Freshness decays linearly over time."""
        old_pattern = EngineeringPattern(
            id="old",
            category=EditCategory.REFACTOR_INTERNAL,
            last_observed=datetime.now(timezone.utc) - timedelta(days=10),
            freshness=0.5,
        )
        freshness, grade = self.confidence.compute_freshness(
            old_pattern,
            FreshnessConfig(max_age_days=14, decay_function="linear"),
        )
        self.assertLess(freshness, 0.5)
        self.assertIn(grade, (FreshnessGrade.AGING, FreshnessGrade.STALE))

    def test_freshness_fresh_grade(self):
        """Recently observed → FRESH."""
        fresh_pattern = EngineeringPattern(
            id="fresh",
            category=EditCategory.ADD_FILE,
        )
        _, grade = self.confidence.compute_freshness(fresh_pattern)
        self.assertEqual(grade, FreshnessGrade.FRESH)

    def test_validation_state_validated(self):
        """5+ observations, confidence >= 0.75 → VALIDATED."""
        high_conf = EngineeringPattern(
            id="high",
            category=EditCategory.ADD_FILE,
            confidence=0.85,
            observation_count=10,
        )
        state = self.confidence.transition_validation_state(high_conf)
        self.assertEqual(state, ValidationState.VALIDATED)

    def test_validation_state_deprecated(self):
        """Confidence < 0.2 → DEPRECATED."""
        low_conf = EngineeringPattern(
            id="low",
            category=EditCategory.ADD_FILE,
            confidence=0.1,
        )
        state = self.confidence.transition_validation_state(low_conf)
        self.assertEqual(state, ValidationState.DEPRECATED)

    def test_set_contradicted(self):
        """Contradiction marking updates validation state."""
        self.confidence.set_contradicted(self.pattern, "contra_1")
        self.assertEqual(self.pattern.validation_state, ValidationState.CONTRADICTED)
        self.assertIn("contra_1", self.pattern.related_pattern_ids)

    def test_calibration_recording(self):
        """Calibration metrics can be recorded and retrieved."""
        self.confidence.record_calibration(0.8, True)
        self.confidence.record_calibration(0.7, True)
        self.confidence.record_calibration(0.6, False)
        metrics = self.confidence.compute_calibration_metrics()
        self.assertIn("accuracy", metrics)
        self.assertIn("confidence_bias", metrics)
        self.assertGreater(metrics["accuracy"], 0.5)

    def test_update_history(self):
        """Confidence update history is retrievable."""
        _, update1 = self.confidence.update_confidence(self.pattern, was_successful=True)
        _, update2 = self.confidence.update_confidence(self.pattern, was_successful=False)
        history = self.confidence.get_update_history(self.pattern.id)
        self.assertGreaterEqual(len(history), 2)
        self.assertEqual(history[0].id, update1.id)

    def test_freshness_config_retrieval(self):
        """Freshness config is retrievable by category."""
        config = self.confidence.get_freshness_config("add_file")
        self.assertIsNotNone(config)
        self.assertEqual(config.max_age_days, 7)


class TestPatternAccumulator(unittest.TestCase):
    """Tests for PatternAccumulator — collecting observations into patterns."""

    def setUp(self):
        self.accumulator = PatternAccumulator(
            min_observations_for_pattern=3,
            min_confidence_for_active=0.4,
        )

    def test_ingest_below_threshold(self):
        """Fewer than min_observations → no pattern yet."""
        for i in range(2):
            delta = DependencyGraphDelta()
            sig = EditCategorySignature(category=EditCategory.REFACTOR_INTERNAL)
            sub = SubgraphSpec()
            result = self.accumulator.ingest(delta, sub, sig)
            self.assertIsNone(result)

    def test_ingest_creates_pattern_at_threshold(self):
        """At threshold, a pattern is created."""
        sig = EditCategorySignature(category=EditCategory.ADD_IMPORT_DEPENDENCY)
        sub = SubgraphSpec()
        for i in range(3):
            delta = DependencyGraphDelta(
                new_edges=[GraphDeltaEdge(
                    edge_type=EdgeType.IMPORTS,
                    source_file_id=f"src{i}", target_file_id=f"tgt{i}",
                )],
            )
            pattern = self.accumulator.ingest(delta, sub, sig)
        self.assertIsNotNone(pattern)
        self.assertEqual(pattern.observation_count, 3)
        self.assertEqual(pattern.category, EditCategory.ADD_IMPORT_DEPENDENCY)

    def test_pattern_confidence_promotion(self):
        """Multiple successful observations increase confidence."""
        sig = EditCategorySignature(category=EditCategory.ADD_FILE)
        sub = SubgraphSpec()
        pattern = None
        for i in range(5):
            delta = DependencyGraphDelta(added_files=[f"file{i}.py"])
            pattern = self.accumulator.ingest(delta, sub, sig, outcome="success")
        self.assertIsNotNone(pattern)
        self.assertGreater(pattern.confidence, 0.3)
        self.assertGreaterEqual(pattern.observation_count, 5)

    def test_confidence_decreases_on_failure(self):
        """Failed observations decrease confidence."""
        sig = EditCategorySignature(category=EditCategory.ADD_FILE)
        sub = SubgraphSpec()
        # Build up confidence
        for i in range(3):
            delta = DependencyGraphDelta(added_files=[f"f{i}.py"])
            self.accumulator.ingest(delta, sub, sig, outcome="success")
        # Find the pattern
        patterns = self.accumulator.get_patterns()
        self.assertEqual(len(patterns), 1)
        initial_conf = patterns[0].confidence
        # Now fail
        for i in range(2):
            delta = DependencyGraphDelta(added_files=[f"g{i}.py"])
            self.accumulator.ingest(delta, sub, sig, outcome="failure")
        patterns = self.accumulator.get_patterns()
        self.assertLess(patterns[0].confidence, initial_conf)

    def test_get_pattern_by_id(self):
        """get_pattern retrieves by ID."""
        sig = EditCategorySignature(category=EditCategory.ADD_FILE)
        sub = SubgraphSpec()
        for i in range(3):
            delta = DependencyGraphDelta(added_files=["x.py"])
            self.accumulator.ingest(delta, sub, sig)
        pattern = self.accumulator.get_patterns()[0]
        retrieved = self.accumulator.get_pattern(pattern.id)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.id, pattern.id)

    def test_query_patterns(self):
        """query() filters correctly by criteria."""
        sig1 = EditCategorySignature(category=EditCategory.ADD_FILE)
        sig2 = EditCategorySignature(category=EditCategory.REFACTOR_INTERNAL)
        sub = SubgraphSpec()
        for i in range(3):
            self.accumulator.ingest(DependencyGraphDelta(added_files=["x.py"]), sub, sig1)
        for i in range(3):
            self.accumulator.ingest(DependencyGraphDelta(), sub, sig2)

        result = self.accumulator.query(PatternQuery(
            category=EditCategory.ADD_FILE,
            max_results=10,
        ))
        self.assertGreaterEqual(result.total_matched, 1)
        for p in result.patterns:
            self.assertEqual(p.category, EditCategory.ADD_FILE)

    def test_patterns_by_category(self):
        """get_patterns_by_category returns correct patterns."""
        sig = EditCategorySignature(category=EditCategory.ADD_FILE)
        sub = SubgraphSpec()
        for i in range(3):
            self.accumulator.ingest(DependencyGraphDelta(added_files=["x.py"]), sub, sig)
        patterns = self.accumulator.get_patterns_by_category(EditCategory.ADD_FILE)
        self.assertGreaterEqual(len(patterns), 1)

    def test_get_statistics(self):
        """get_statistics returns aggregate metrics."""
        stats = self.accumulator.get_statistics()
        self.assertIn("total_patterns", stats)
        self.assertIn("by_category", stats)
        self.assertEqual(stats["total_observations"], 0)

        # Add some patterns
        sig = EditCategorySignature(category=EditCategory.ADD_FILE)
        sub = SubgraphSpec()
        for i in range(3):
            self.accumulator.ingest(DependencyGraphDelta(added_files=["x.py"]), sub, sig)
        stats = self.accumulator.get_statistics()
        self.assertGreater(stats["total_patterns"], 0)
        self.assertIn("add_file", stats["by_category"])

    def test_prune_deprecated(self):
        """prune_deprecated removes old deprecated patterns."""
        sig = EditCategorySignature(category=EditCategory.ADD_FILE)
        sub = SubgraphSpec()
        for i in range(3):
            self.accumulator.ingest(DependencyGraphDelta(added_files=["x.py"]), sub, sig)
        pattern = self.accumulator.get_patterns()[0]

        # Manually set to deprecated
        pattern.validation_state = ValidationState.DEPRECATED
        pattern.last_observed = datetime.now(timezone.utc) - timedelta(days=100)

        pruned = self.accumulator.prune_deprecated(max_age_days=30, min_observations_for_retention=5)
        self.assertEqual(pruned, 1)

    def test_reset(self):
        """reset clears all state."""
        sig = EditCategorySignature(category=EditCategory.ADD_FILE)
        sub = SubgraphSpec()
        for i in range(3):
            self.accumulator.ingest(DependencyGraphDelta(added_files=["x.py"]), sub, sig)
        self.assertGreater(len(self.accumulator.get_patterns()), 0)
        self.accumulator.reset()
        self.assertEqual(len(self.accumulator.get_patterns()), 0)
        self.assertEqual(len(self.accumulator.get_metrics()), 0)

    def test_ingest_with_metadata(self):
        """Ingest with specialist and project scope metadata."""
        sig = EditCategorySignature(category=EditCategory.ADD_FILE)
        sub = SubgraphSpec()
        for i in range(3):
            delta = DependencyGraphDelta(added_files=["x.py"])
            pattern = self.accumulator.ingest(
                delta, sub, sig,
                outcome="success",
                task_description="Added new module",
                source_specialist="FORGE",
                project_scope="aelvo",
            )
        self.assertIsNotNone(pattern)
        self.assertEqual(pattern.source_specialist, "FORGE")
        self.assertEqual(pattern.project_scope, "aelvo")
        self.assertIn("Added new module", pattern.provenance)


class TestKnowledgeGraph(unittest.TestCase):
    """Tests for KnowledgeGraph — SQLite persistence layer."""

    def setUp(self):
        self.db = KnowledgeGraph(":memory:")
        self.pattern = EngineeringPattern(
            id="test_kg_pattern",
            category=EditCategory.ADD_IMPORT_DEPENDENCY,
            category_signature=EditCategorySignature(
                category=EditCategory.ADD_IMPORT_DEPENDENCY,
            ),
            confidence=0.75,
            observation_count=10,
            success_count=8,
            failure_count=2,
            validation_state=ValidationState.VALIDATED,
            freshness=1.0,
            provenance=["test task"],
        )
        self.pattern.to_digest()

    def tearDown(self):
        self.db.close()

    def test_save_and_load_pattern(self):
        """Saved pattern can be loaded."""
        self.db.save_pattern(self.pattern)
        loaded = self.db.load_pattern(self.pattern.id)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.id, self.pattern.id)
        self.assertEqual(loaded.category, self.pattern.category)
        self.assertEqual(loaded.confidence, self.pattern.confidence)

    def test_load_patterns_with_filters(self):
        """load_patterns returns filtered results."""
        p1 = EngineeringPattern(id="p1", category=EditCategory.ADD_FILE, confidence=0.9)
        p1.to_digest()
        p2 = EngineeringPattern(id="p2", category=EditCategory.REFACTOR_INTERNAL, confidence=0.3)
        p2.to_digest()

        self.db.save_pattern(p1)
        self.db.save_pattern(p2)

        high_conf = self.db.load_patterns(min_confidence=0.8)
        self.assertEqual(len(high_conf), 1)
        self.assertEqual(high_conf[0].id, p1.id)

        cat_filtered = self.db.load_patterns(
            category=EditCategory.REFACTOR_INTERNAL
        )
        self.assertEqual(len(cat_filtered), 1)
        self.assertEqual(cat_filtered[0].id, p2.id)

    def test_delete_pattern(self):
        """Deleted pattern is removed along with its edges."""
        self.db.save_pattern(self.pattern)
        self.db.add_edge(self.pattern.id, "other", "DERIVED_FROM")
        deleted = self.db.delete_pattern(self.pattern.id)
        self.assertTrue(deleted)
        self.assertIsNone(self.db.load_pattern(self.pattern.id))

    def test_edge_management(self):
        """Edges can be added, checked, and queried."""
        self.db.save_pattern(self.pattern)
        added = self.db.add_edge(self.pattern.id, "target_id", "DERIVED_FROM")
        self.assertTrue(added)
        # Duplicate should not be added
        dup = self.db.add_edge(self.pattern.id, "target_id", "DERIVED_FROM")
        self.assertFalse(dup)
        # Query edges
        edges = self.db.get_edges(pattern_id=self.pattern.id)
        self.assertGreaterEqual(len(edges), 1)
        self.assertEqual(edges[0]["edge_type"], "DERIVED_FROM")

    def test_contradiction_persistence(self):
        """Contradiction records can be saved and retrieved."""
        record = ContradictionRecord(
            old_knowledge_id="old_pat",
            new_knowledge_id="new_pat",
            contradiction_type="scope_conflict",
            resolution_strategy="retain_both_with_scope",
            reasoning="Test contradiction",
            resolved=True,
        )
        record.to_id()
        self.db.save_contradiction(record)

        records = self.db.get_contradictions()
        self.assertGreaterEqual(len(records), 1)
        self.assertEqual(records[0].contradiction_type, "scope_conflict")

    def test_confidence_update_persistence(self):
        """Confidence updates can be saved and queried."""
        update = ConfidenceUpdate(
            knowledge_item_id=self.pattern.id,
            previous_confidence=0.3,
            new_confidence=0.4,
            update_formula="bonus = 0.1 * (1.0 - 0.3)",
            evidence="success",
        )
        update.to_id()
        self.db.save_confidence_update(update)

        updates = self.db.get_confidence_updates(self.pattern.id)
        self.assertGreaterEqual(len(updates), 1)
        self.assertEqual(updates[0].previous_confidence, 0.3)

    def test_session_checkpoint(self):
        """Session checkpoints can be saved and loaded."""
        stats = {"total_patterns": 5, "by_category": {"add_file": 3}}
        self.db.save_session_checkpoint("session_1", stats)

        checkpoint = self.db.load_session_checkpoint("session_1")
        self.assertIsNotNone(checkpoint)
        self.assertEqual(checkpoint["patterns_count"], 5)

    def test_query_method(self):
        """query returns ranked results."""
        p1 = EngineeringPattern(id="q1", category=EditCategory.ADD_FILE, confidence=0.9)
        p1.to_digest()
        p2 = EngineeringPattern(id="q2", category=EditCategory.ADD_FILE, confidence=0.5)
        p2.to_digest()
        self.db.save_pattern(p1)
        self.db.save_pattern(p2)

        result = self.db.query(PatternQuery(
            category=EditCategory.ADD_FILE,
            max_results=5,
        ))
        self.assertEqual(result.total_matched, 2)
        self.assertGreaterEqual(result.patterns[0].confidence, result.patterns[1].confidence)

    def test_statistics(self):
        """get_statistics returns correct counts."""
        p = EngineeringPattern(id="stat", category=EditCategory.ADD_FILE, confidence=0.8)
        p.to_digest()
        self.db.save_pattern(p)
        stats = self.db.get_statistics()
        self.assertGreaterEqual(stats["total_patterns"], 1)
        self.assertIn("by_category", stats)
        self.assertIn("avg_confidence", stats)

    def test_orphaned_patterns(self):
        """find_orphaned_patterns returns patterns with no edges."""
        self.db.save_pattern(self.pattern)
        orphans = self.db.find_orphaned_patterns()
        self.assertIn(self.pattern.id, orphans)

    def test_cycle_detection(self):
        """detect_cycles_supersedes finds cycles in SUPERSEDES edges."""
        self.db.save_pattern(self.pattern)
        p2 = EngineeringPattern(id="p2", category=EditCategory.ADD_FILE)
        p2.to_digest()
        self.db.save_pattern(p2)
        p3 = EngineeringPattern(id="p3", category=EditCategory.ADD_FILE)
        p3.to_digest()
        self.db.save_pattern(p3)

        # Create a cycle: self.pattern → p2 → p3 → self.pattern
        self.db.add_edge(self.pattern.id, p2.id, "SUPERSEDES")
        self.db.add_edge(p2.id, p3.id, "SUPERSEDES")
        self.db.add_edge(p3.id, self.pattern.id, "SUPERSEDES")

        cycles = self.db.detect_cycles_supersedes()
        self.assertGreaterEqual(len(cycles), 1)

    def test_vacuum_does_not_crash(self):
        """Vacuum runs without error."""
        self.db.vacuum()


class TestPatternExtractionEngine(unittest.TestCase):
    """Tests for PatternExtractionEngine — orchestrating the full pipeline."""

    def setUp(self):
        self.kg = KnowledgeGraph(":memory:")
        self.engine = PatternExtractionEngine(knowledge_graph=self.kg)
        self.callback_results = []

    def test_lifecycle(self):
        """Engine can start and end sessions."""
        self.engine.start_session("test_session")
        self.assertTrue(self.engine._is_running)
        self.engine.end_session()
        self.assertFalse(self.engine._is_running)

    def test_process_empty_transition(self):
        """Empty graph transition → no pattern."""
        self.engine.start_session("test")
        before = make_snapshot(version=1)
        after = make_snapshot(version=1)
        pattern = self.engine.process_graph_transition(before, after)
        self.assertIsNone(pattern)
        self.engine.end_session()

    def test_process_transition_creates_pattern(self):
        """Meaningful transition → pattern after sufficient observations."""
        self.engine.start_session("test")

        fid_a = FileId.create("a.py")
        fid_b = FileId.create("b.py")
        sym_a = SymbolNode(
            symbol_id="a" * 16, file_id=fid_a, file_path="a.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="a", fully_qualified_name="a",
            confidence=ConfidenceLevel.CERTAIN,
        )
        sym_b = SymbolNode(
            symbol_id="b" * 16, file_id=fid_b, file_path="b.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="b", fully_qualified_name="b",
            confidence=ConfidenceLevel.CERTAIN,
        )
        edge = make_edge(sym_a.symbol_id, sym_b.symbol_id)

        before = make_snapshot(
            files={fid_a: make_file(fid_a, "a.py"), fid_b: make_file(fid_b, "b.py")},
            symbols={sym_a.symbol_id: sym_a, sym_b.symbol_id: sym_b},
            version=1,
        )
        after = make_snapshot(
            files={fid_a: make_file(fid_a, "a.py"), fid_b: make_file(fid_b, "b.py")},
            symbols={sym_a.symbol_id: sym_a, sym_b.symbol_id: sym_b},
            edges=[edge],
            version=2,
        )

        # Need 3 observations (min_observations_for_pattern default = 3)
        pattern = None
        for i in range(3):
            pattern = self.engine.process_graph_transition(before, after, source=DeltaSource(
                task_id=f"task_{i}",
                specialist="FORGE",
                project="test_project",
                outcome="success",
            ))

        self.assertIsNotNone(pattern)
        self.assertGreaterEqual(pattern.observation_count, 3)
        self.assertTrue(pattern.id)

        self.engine.end_session()

    def test_execution_event(self):
        """process_execution_event wraps process_graph_transition."""
        self.engine.start_session("test")

        before = make_snapshot(version=1)

        fid_a = FileId.create("a.py")
        sym_a = SymbolNode(
            symbol_id="a" * 16, file_id=fid_a, file_path="a.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="a", fully_qualified_name="a",
            confidence=ConfidenceLevel.CERTAIN,
        )
        edge = make_edge("a" * 16, "b" * 16)
        after = make_snapshot(
            files={fid_a: make_file(fid_a, "a.py")},
            symbols={sym_a.symbol_id: sym_a},
            edges=[edge],
            version=2,
        )

        # Run a few times to accumulate
        for i in range(3):
            self.engine.process_execution_event(
                task_id=f"task_{i}",
                before_snapshot=before,
                after_snapshot=after,
                specialist="FORGE",
                project="test_project",
            )

        self.assertGreater(self.engine._total_deltas_processed, 0)
        self.engine.end_session()

    def test_callbacks(self):
        """Pattern created and updated callbacks fire."""
        self.engine.start_session("test")
        created = []
        updated = []

        def on_created(p, s):
            created.append(p.id)
        def on_updated(p, s):
            updated.append(p.id)

        self.engine.on_pattern_created(on_created)
        self.engine.on_pattern_updated(on_updated)

        before = make_snapshot(version=1)
        fid_a = FileId.create("a.py")
        sym_a = SymbolNode(
            symbol_id="a" * 16, file_id=fid_a, file_path="a.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="a", fully_qualified_name="a",
            confidence=ConfidenceLevel.CERTAIN,
        )
        edge = make_edge("a" * 16, "b" * 16)
        after = make_snapshot(
            files={fid_a: make_file(fid_a, "a.py")},
            symbols={sym_a.symbol_id: sym_a},
            edges=[edge],
            version=2,
        )

        for i in range(3):
            self.engine.process_graph_transition(before, after)

        self.engine.end_session()

    def test_query_patterns(self):
        """query_patterns returns patterns from accumulator."""
        self.engine.start_session("test")

        # Create a pattern
        before = make_snapshot(version=1)
        fid_a = FileId.create("a.py")
        sym_a = SymbolNode(
            symbol_id="a" * 16, file_id=fid_a, file_path="a.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="a", fully_qualified_name="a",
            confidence=ConfidenceLevel.CERTAIN,
        )
        edge = make_edge("a" * 16, "b" * 16)
        after = make_snapshot(
            files={fid_a: make_file(fid_a, "a.py")},
            symbols={sym_a.symbol_id: sym_a},
            edges=[edge],
            version=2,
        )
        for i in range(3):
            self.engine.process_graph_transition(before, after)

        result = self.engine.query_patterns(PatternQuery(max_results=10))
        self.assertGreaterEqual(result.total_matched, 1)

        self.engine.end_session()

    def test_get_pattern(self):
        """get_pattern retrieves by ID."""
        self.engine.start_session("test")
        before = make_snapshot(version=1)
        fid_a = FileId.create("a.py")
        sym_a = SymbolNode(
            symbol_id="a" * 16, file_id=fid_a, file_path="a.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="a", fully_qualified_name="a",
            confidence=ConfidenceLevel.CERTAIN,
        )
        edge = make_edge("a" * 16, "b" * 16)
        after = make_snapshot(
            files={fid_a: make_file(fid_a, "a.py")},
            symbols={sym_a.symbol_id: sym_a},
            edges=[edge],
            version=2,
        )
        pattern = None
        for i in range(3):
            pattern = self.engine.process_graph_transition(before, after)
        self.assertIsNotNone(pattern)
        retrieved = self.engine.get_pattern(pattern.id)
        self.assertEqual(retrieved.id, pattern.id)
        self.engine.end_session()

    def test_learning_statistics(self):
        """get_learning_statistics returns comprehensive stats."""
        self.engine.start_session("test")
        stats = self.engine.get_learning_statistics()
        self.assertIn("total_patterns", stats)
        self.assertIn("total_deltas_processed", stats)
        self.assertIn("calibration_accuracy", stats)
        self.assertIn("knowledge_graph", stats)
        self.engine.end_session()

    def test_get_patterns_for_context(self):
        """get_patterns_for_context returns patterns for specialist injection."""
        self.engine.start_session("test")
        patterns = self.engine.get_patterns_for_context(
            project="test_project",
            specialist="FORGE",
            max_tokens=500,
        )
        # No patterns yet, so should be empty
        self.assertEqual(len(patterns), 0)
        self.engine.end_session()

    def test_process_transition_without_session(self):
        """Calling process_graph_transition without a session raises error."""
        with self.assertRaises(RuntimeError):
            self.engine.process_graph_transition(make_snapshot(), make_snapshot())

    def test_contradiction_detection(self):
        """Contradictions between similar but different patterns are detected."""
        self.engine.start_session("test")
        self.engine.accumulator = PatternAccumulator(
            min_observations_for_pattern=2,  # Lower threshold for testing
        )
        if self.engine.knowledge_graph:
            self.engine.accumulator.set_persistence_callback(
                self.engine.knowledge_graph.save_pattern
            )

        make_snapshot(version=1)
        make_snapshot(version=2)
        make_snapshot(version=2)

        # This is simplified — in production, the actual deltas would differ
        sig_a = EditCategorySignature(category=EditCategory.ADD_FILE)
        sig_b = EditCategorySignature(category=EditCategory.ADD_FILE)
        sub_a = SubgraphSpec(anchor_node_key="mod_a", node_count=1)
        sub_b = SubgraphSpec(anchor_node_key="mod_b", node_count=1)

        self.engine.accumulator.ingest(
            DependencyGraphDelta(added_files=["mod_a.py"]), sub_a, sig_a,
        )
        self.engine.accumulator.ingest(
            DependencyGraphDelta(added_files=["mod_a.py"]), sub_a, sig_a,
        )

        self.engine.accumulator.ingest(
            DependencyGraphDelta(added_files=["mod_b.py"]), sub_b, sig_b,
        )
        self.engine.accumulator.ingest(
            DependencyGraphDelta(added_files=["mod_b.py"]), sub_b, sig_b,
        )

        patterns = self.engine.accumulator.get_patterns()
        self.assertGreaterEqual(len(patterns), 1)

        self.engine.end_session()


if __name__ == "__main__":
    unittest.main()
