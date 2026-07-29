# learning/__init__.py - AELVO Pattern Extraction & Knowledge Learning System
# Dependency-graph-level pattern extraction engine

from learning.types import (
    EditCategory, GraphDeltaEdge, DependencyGraphDelta, EditCategorySignature,
    SubgraphNode, SubgraphEdge, SubgraphSpec, EngineeringPattern,
    PatternObservation, ConfidenceUpdate, ContradictionRecord,
    ValidationState, FreshnessGrade, PatternQuery, PatternQueryResult,
    FreshnessConfig, DeltaSource,
)
from learning.delta import DeltaComputer
from learning.classifier import EditClassifier
from learning.subgraph import SubgraphExtractor, SubgraphSimilarity
from learning.confidence import ConfidenceSystem
from learning.accumulator import PatternAccumulator
from learning.knowledge_graph import KnowledgeGraph
from learning.engine import PatternExtractionEngine
from learning.specialist_adapter import KnowledgeAdapter
from learning.analytics import AnalyticsEngine
from learning.consensus_memory import ConsensusMemory
from learning.collaboration_accumulator import CollaborationAccumulator
from learning.specialist_effectiveness import SpecialistEffectivenessTracker

__all__ = [
    # Types
    'EditCategory', 'GraphDeltaEdge', 'DependencyGraphDelta', 'EditCategorySignature',
    'SubgraphNode', 'SubgraphEdge', 'SubgraphSpec', 'EngineeringPattern',
    'PatternObservation', 'ConfidenceUpdate', 'ContradictionRecord',
    'ValidationState', 'FreshnessGrade', 'PatternQuery', 'PatternQueryResult',
    'FreshnessConfig', 'DeltaSource',
    # Analytics Types
    'SessionRecord', 'CalibrationBin', 'TrendPoint', 'TrendDirection',
    'TrendSeries', 'FirstAttemptRecord', 'SpecialistLearningCurve',
    # Phase 10 Collaboration & Consensus Types
    'ConsensusOutcome', 'CollaborationEventType', 'ConsensusMemoryRecord',
    'CollaborationObservation', 'CollaborationSignature', 'CollaborationPattern',
    'SpecialistEffectivenessRecord',
    # Subsystems
    'DeltaComputer', 'EditClassifier', 'SubgraphExtractor', 'SubgraphSimilarity',
    'ConfidenceSystem', 'PatternAccumulator', 'KnowledgeGraph', 'PatternExtractionEngine',
    'KnowledgeAdapter', 'AnalyticsEngine',
    # Phase 10 Subsystems
    'ConsensusMemory', 'CollaborationAccumulator', 'SpecialistEffectivenessTracker',
]
