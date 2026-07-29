from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any
from datetime import datetime


class ChunkingStrategy(str, Enum):
    FIXED_SIZE = "fixed_size"
    RECURSIVE = "recursive"
    SENTENCE = "sentence"
    SEMANTIC = "semantic"


class RetrievalStrategy(str, Enum):
    DENSE = "dense"
    SPARSE = "sparse"
    HYBRID = "hybrid"
    MULTI_QUERY = "multi_query"


class FusionStrategy(str, Enum):
    RRF = "rrf"
    WEIGHTED = "weighted"
    MAX = "max"


class ReRankStrategy(str, Enum):
    NONE = "none"
    CROSS_ENCODER = "cross_encoder"
    MMR = "mmr"
    RECENCY = "recency"


@dataclass
class Document:
    id: str
    content: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    source: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Chunk:
    id: str
    document_id: str
    content: str
    index: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    embedding: Optional[List[float]] = None
    token_count: int = 0


@dataclass
class RetrievalResult:
    chunk_id: str
    document_id: str
    content: str
    score: float = 0.0
    strategy: str = "dense"
    rank: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    source: str = ""


@dataclass
class RAGContext:
    query: str
    results: List[RetrievalResult] = field(default_factory=list)
    context_text: str = ""
    token_count: int = 0
    sources: List[str] = field(default_factory=list)
    durations: Dict[str, float] = field(default_factory=dict)
    strategy_used: str = "dense"


@dataclass
class IngestResult:
    document_id: str
    chunk_count: int = 0
    success: bool = False
    error: Optional[str] = None
    duration_ms: float = 0.0


class RAGConfig:
    def __init__(
        self,
        chunking_strategy: ChunkingStrategy = ChunkingStrategy.RECURSIVE,
        chunk_size: int = 512,
        chunk_overlap: int = 64,
        retrieval_strategy: RetrievalStrategy = RetrievalStrategy.HYBRID,
        fusion_strategy: FusionStrategy = FusionStrategy.RRF,
        rerank_strategy: ReRankStrategy = ReRankStrategy.NONE,
        dense_weight: float = 0.5,
        sparse_weight: float = 0.5,
        rrf_constant: int = 60,
        top_k: int = 10,
        min_score: float = 0.1,
        max_context_tokens: int = 4096,
        query_expansion: bool = False,
        enable_cache: bool = True,
        cache_ttl_seconds: int = 300,
        separator: str = "\n\n---\n\n",
        system_prompt_template: str = (
            "You are a helpful assistant with access to retrieved context.\n"
            "Use the following context to answer the user's question.\n"
            "If the context doesn't contain relevant information, say so.\n"
            "Always cite your sources.\n\n"
            "Context:\n{context}\n\n"
            "Question: {query}\n\n"
            "Answer:"
        ),
    ):
        self.chunking_strategy = chunking_strategy
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.retrieval_strategy = retrieval_strategy
        self.fusion_strategy = fusion_strategy
        self.rerank_strategy = rerank_strategy
        self.dense_weight = dense_weight
        self.sparse_weight = sparse_weight
        self.rrf_constant = rrf_constant
        self.top_k = top_k
        self.min_score = min_score
        self.max_context_tokens = max_context_tokens
        self.query_expansion = query_expansion
        self.enable_cache = enable_cache
        self.cache_ttl_seconds = cache_ttl_seconds
        self.separator = separator
        self.system_prompt_template = system_prompt_template


DEFAULT_RAG_CONFIG = RAGConfig()
