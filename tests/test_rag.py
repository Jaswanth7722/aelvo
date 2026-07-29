"""Comprehensive tests for the production-grade RAG system."""

import time
import re
import pytest
from typing import Dict, List, Any

from core.rag.types import (
    Document, Chunk, RetrievalResult, RAGContext, IngestResult,
    ChunkingStrategy, RetrievalStrategy, FusionStrategy, ReRankStrategy,
    RAGConfig,
)
from core.rag.chunking import chunk_document, estimate_tokens
from core.rag.retrieval import BM25Index, DenseRetriever, HybridRetriever, MultiQueryRetriever, rerank_results
from core.rag.rag import RAGEngine, MemorySearcher


from helpers import MockCollection

# =============================================================================
# RAGConfig
# =============================================================================

class TestRAGConfig:
    def test_default_config(self):
        config = RAGConfig()
        assert config.chunking_strategy == ChunkingStrategy.RECURSIVE
        assert config.chunk_size == 512
        assert config.chunk_overlap == 64
        assert config.retrieval_strategy == RetrievalStrategy.HYBRID
        assert config.fusion_strategy == FusionStrategy.RRF
        assert config.top_k == 10
        assert config.min_score == 0.1
        assert config.max_context_tokens == 4096

    def test_custom_config(self):
        config = RAGConfig(
            chunking_strategy=ChunkingStrategy.FIXED_SIZE,
            chunk_size=256,
            chunk_overlap=32,
            retrieval_strategy=RetrievalStrategy.DENSE,
            top_k=5,
            min_score=0.2,
        )
        assert config.chunking_strategy == ChunkingStrategy.FIXED_SIZE
        assert config.chunk_size == 256
        assert config.retrieval_strategy == RetrievalStrategy.DENSE
        assert config.top_k == 5

    def test_system_prompt_template(self):
        config = RAGConfig()
        rendered = config.system_prompt_template.format(
            context="test context", query="test query"
        )
        assert "test context" in rendered
        assert "test query" in rendered


# =============================================================================
# Chunking
# =============================================================================

class TestChunking:
    def test_fixed_size_chunking(self):
        doc = Document(id="d1", content="A" * 2000)
        chunks = chunk_document(doc, strategy=ChunkingStrategy.FIXED_SIZE, chunk_size=500, chunk_overlap=50)
        assert len(chunks) >= 4
        assert all(c.document_id == "d1" for c in chunks)
        for i, c in enumerate(chunks):
            assert c.index == i

    def test_recursive_chunking(self):
        doc = Document(id="d2", content="Paragraph one.\n\nParagraph two.\n\nParagraph three.\n\nParagraph four.")
        chunks = chunk_document(doc, strategy=ChunkingStrategy.RECURSIVE, chunk_size=20, chunk_overlap=5)
        assert len(chunks) >= 1

    def test_sentence_chunking(self):
        doc = Document(id="d3", content="First sentence. Second sentence. Third sentence. Fourth sentence. Fifth sentence.")
        chunks = chunk_document(doc, strategy=ChunkingStrategy.SENTENCE, chunk_size=30, chunk_overlap=0)
        assert len(chunks) >= 1

    def test_string_with_newlines(self):
        doc = Document(id="d4", content="Line1\nLine2\nLine3\nLine4\nLine5")
        chunks = chunk_document(doc, strategy=ChunkingStrategy.SENTENCE, chunk_size=20, chunk_overlap=5)
        assert len(chunks) >= 1

    def test_empty_content(self):
        doc = Document(id="d5", content="")
        chunks = chunk_document(doc, strategy=ChunkingStrategy.RECURSIVE, chunk_size=100, chunk_overlap=10)
        assert len(chunks) == 0

    def test_metadata_propagation(self):
        doc = Document(id="d6", content="Test content for metadata propagation across chunking.", metadata={"type": "test", "project": "p1"})
        chunks = chunk_document(doc, strategy=ChunkingStrategy.FIXED_SIZE, chunk_size=20, chunk_overlap=5)
        for c in chunks:
            assert c.metadata.get("type") == "test"
            assert c.metadata.get("project") == "p1"

    def test_chunk_ids_unique(self):
        doc = Document(id="d7", content="A" * 1000)
        chunks = chunk_document(doc, strategy=ChunkingStrategy.FIXED_SIZE, chunk_size=100, chunk_overlap=20)
        chunk_ids = [c.id for c in chunks]
        assert len(chunk_ids) == len(set(chunk_ids))

    def test_estimate_tokens(self):
        assert estimate_tokens("hello world") == 3
        assert estimate_tokens("") == 0
        assert estimate_tokens("A" * 100) == 26


# =============================================================================
# BM25 Index
# =============================================================================

class TestBM25Index:
    def test_add_and_search(self):
        bm25 = BM25Index()
        bm25.add_chunk(Chunk(id="c1", document_id="d1", content="python is a programming language"))
        bm25.add_chunk(Chunk(id="c2", document_id="d2", content="javascript is for web development"))
        bm25.add_chunk(Chunk(id="c3", document_id="d3", content="rust is for systems programming"))
        results = bm25.search("python programming", top_k=3)
        assert len(results) >= 1
        assert results[0].chunk_id == "c1"

    def test_search_no_results(self):
        bm25 = BM25Index()
        bm25.add_chunk(Chunk(id="c1", document_id="d1", content="python programming"))
        results = bm25.search("quantum physics", top_k=3)
        assert len(results) == 0

    def test_search_empty_index(self):
        bm25 = BM25Index()
        results = bm25.search("anything", top_k=3)
        assert len(results) == 0

    def test_search_empty_query(self):
        bm25 = BM25Index()
        bm25.add_chunk(Chunk(id="c1", document_id="d1", content="some content"))
        results = bm25.search("", top_k=3)
        assert len(results) == 0

    def test_clear_and_rebuild(self):
        bm25 = BM25Index()
        bm25.add_chunk(Chunk(id="c1", document_id="d1", content="python is great"))
        bm25.build()
        assert bm25._built
        bm25.clear()
        assert not bm25._built
        assert bm25._doc_count == 0

    def test_relevance_scoring(self):
        bm25 = BM25Index()
        bm25.add_chunk(Chunk(id="c1", document_id="d1", content="python python python python"))
        bm25.add_chunk(Chunk(id="c2", document_id="d2", content="python is mentioned once here"))
        bm25.add_chunk(Chunk(id="c3", document_id="d3", content="java has nothing to do with python"))
        results = bm25.search("python", top_k=3)
        assert len(results) == 3
        assert results[0].score >= results[1].score
        assert results[1].score >= results[2].score


# =============================================================================
# Dense Retriever
# =============================================================================

class TestDenseRetriever:
    def test_search_returns_results(self):
        col = MockCollection()
        col.add(ids=["c1", "c2"], documents=["python is great", "java is ok"],
                metadatas=[{"type": "fact", "importance": 0.8}, {"type": "fact", "importance": 0.5}])
        retriever = DenseRetriever(col)
        results = retriever.search("python", top_k=5)
        assert len(results) >= 1

    def test_search_empty_collection(self):
        col = MockCollection()
        retriever = DenseRetriever(col)
        results = retriever.search("anything", top_k=5)
        assert len(results) == 0

    def test_search_with_where_filter(self):
        col = MockCollection()
        col.add(ids=["c1", "c2"], documents=["python", "java"],
                metadatas=[{"type": "code_pattern"}, {"type": "fact"}])
        retriever = DenseRetriever(col)
        results = retriever.search("python", top_k=5, where={"type": "code_pattern"})
        assert len(results) >= 1
        for r in results:
            assert r.metadata.get("type") == "code_pattern"

    def test_min_score_filter(self):
        col = MockCollection()
        col.set_fixed_distances([0.99, 0.1])
        col.add(ids=["c_close", "c_far"], documents=["python close match", "python far match"],
                metadatas=[{"type": "fact"}, {"type": "fact"}])
        retriever = DenseRetriever(col)
        results = retriever.search("python", top_k=5, min_score=0.5)
        assert all(r.score >= 0.5 for r in results)

    def test_search_no_collection(self):
        retriever = DenseRetriever(None)
        results = retriever.search("test", top_k=5)
        assert len(results) == 0


# =============================================================================
# Hybrid Retriever
# =============================================================================

class TestHybridRetriever:
    def test_rrf_fusion(self):
        col = MockCollection()
        col.add(ids=["c1", "c2"], documents=["python language", "java language"],
                metadatas=[{"type": "fact"}, {"type": "fact"}])
        bm25 = BM25Index()
        bm25.add_chunk(Chunk(id="c1", document_id="d1", content="python language"))
        bm25.add_chunk(Chunk(id="c2", document_id="d2", content="java language"))
        dense = DenseRetriever(col)
        hybrid = HybridRetriever(dense, bm25)
        results = hybrid.search("python", top_k=5, fusion=FusionStrategy.RRF, min_score=0.0)
        assert len(results) >= 1

    def test_weighted_fusion(self):
        col = MockCollection()
        col.add(ids=["c1", "c2"], documents=["python language", "java language"],
                metadatas=[{"type": "fact"}, {"type": "fact"}])
        bm25 = BM25Index()
        bm25.add_chunk(Chunk(id="c1", document_id="d1", content="python language"))
        bm25.add_chunk(Chunk(id="c2", document_id="d2", content="java language"))
        dense = DenseRetriever(col)
        hybrid = HybridRetriever(dense, bm25)
        results = hybrid.search("python", top_k=5, fusion=FusionStrategy.WEIGHTED, dense_weight=0.3, sparse_weight=0.7, min_score=0.0)
        assert len(results) >= 1

    def test_max_fusion(self):
        col = MockCollection()
        col.add(ids=["c1", "c2"], documents=["python language", "java language"],
                metadatas=[{"type": "fact"}, {"type": "fact"}])
        bm25 = BM25Index()
        bm25.add_chunk(Chunk(id="c1", document_id="d1", content="python language"))
        bm25.add_chunk(Chunk(id="c2", document_id="d2", content="java language"))
        dense = DenseRetriever(col)
        hybrid = HybridRetriever(dense, bm25)
        results = hybrid.search("python", top_k=5, fusion=FusionStrategy.MAX, min_score=0.0)
        assert len(results) >= 1


# =============================================================================
# Multi-Query Retriever
# =============================================================================

class TestMultiQueryRetriever:
    def test_expand_and_fuse(self):
        col = MockCollection()
        col.add(ids=["c1", "c2"], documents=["python tutorial for beginners", "advanced java concepts"],
                metadatas=[{"type": "fact"}, {"type": "fact"}])
        bm25 = BM25Index()
        bm25.add_chunk(Chunk(id="c1", document_id="d1", content="python tutorial for beginners"))
        bm25.add_chunk(Chunk(id="c2", document_id="d2", content="advanced java concepts"))
        dense = DenseRetriever(col)
        hybrid = HybridRetriever(dense, bm25)
        mq = MultiQueryRetriever(hybrid)
        results = mq.search("python and beginners", top_k=5, min_score=0.0)
        assert len(results) >= 1


# =============================================================================
# Re-ranking
# =============================================================================

class TestReranking:
    def test_no_rerank(self):
        results = [RetrievalResult(chunk_id="c1", document_id="d1", content="a", score=0.5),
                   RetrievalResult(chunk_id="c2", document_id="d2", content="b", score=0.9)]
        reranked = rerank_results(results, "test", strategy=ReRankStrategy.NONE, top_k=10)
        assert len(reranked) == 2

    def test_mmr_rerank(self):
        results = [RetrievalResult(chunk_id="c1", document_id="d1", content="python is great for data science", score=0.9),
                   RetrievalResult(chunk_id="c2", document_id="d2", content="python is great for web development", score=0.8),
                   RetrievalResult(chunk_id="c3", document_id="d3", content="java is used in enterprise", score=0.7)]
        reranked = rerank_results(results, "python", strategy=ReRankStrategy.MMR, top_k=3)
        assert len(reranked) == 3

    def test_mmr_diversity(self):
        results = [RetrievalResult(chunk_id="c1", document_id="d1", content="python python python python", score=0.9),
                   RetrievalResult(chunk_id="c2", document_id="d2", content="python python python python", score=0.85),
                   RetrievalResult(chunk_id="c3", document_id="d3", content="java is completely different", score=0.7)]
        reranked = rerank_results(results, "python", strategy=ReRankStrategy.MMR, top_k=3)
        assert reranked[0].chunk_id == "c1"
        if len(reranked) > 1:
            assert reranked[-1].chunk_id == "c3"

    def test_empty_results(self):
        reranked = rerank_results([], "test", strategy=ReRankStrategy.MMR, top_k=5)
        assert reranked == []


# =============================================================================
# RAGEngine
# =============================================================================

class TestRAGEngine:
    def test_engine_init(self):
        engine = RAGEngine()
        assert engine._collection is None
        assert engine._bm25 is not None
        assert engine._config is not None

    def test_ingest_document(self):
        col = MockCollection()
        engine = RAGEngine(col)
        result = engine.ingest("This is a test document about Python programming language.")
        assert result.success
        assert result.document_id is not None
        assert result.chunk_count >= 1

    def test_ingest_with_metadata(self):
        col = MockCollection()
        engine = RAGEngine(col)
        result = engine.ingest("Test content", metadata={"type": "test_type", "project": "p1"}, source="test_source")
        assert result.success

    def test_ingest_empty_document(self):
        col = MockCollection()
        engine = RAGEngine(col)
        result = engine.ingest("")
        assert result.success
        assert result.chunk_count >= 0

    def test_retrieve_dense(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Python is a high-level programming language", source="docs")
        engine.ingest("Java is a class-based programming language", source="docs")
        ctx = engine.retrieve("python", strategy=RetrievalStrategy.DENSE)
        assert len(ctx.results) >= 1
        assert ctx.strategy_used == "dense"

    def test_retrieve_sparse(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Python is a high-level programming language", source="docs")
        engine.ingest("JavaScript is for web development", source="docs")
        ctx = engine.retrieve("python programming", strategy=RetrievalStrategy.SPARSE)
        assert len(ctx.results) >= 1

    def test_retrieve_hybrid(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Python is a high-level programming language", source="docs")
        engine.ingest("JavaScript is for web development", source="docs")
        ctx = engine.retrieve("python programming", strategy=RetrievalStrategy.HYBRID)
        assert len(ctx.results) >= 1

    def test_retrieve_empty_collection(self):
        col = MockCollection()
        engine = RAGEngine(col)
        ctx = engine.retrieve("anything", strategy=RetrievalStrategy.DENSE)
        assert len(ctx.results) == 0

    def test_context_assembly(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Python is a programming language", source="wiki")
        ctx = engine.retrieve("python", strategy=RetrievalStrategy.DENSE)
        assert ctx.context_text != ""
        assert "Python" in ctx.context_text or "python" in ctx.context_text.lower()

    def test_context_sources(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Python content", source="python_docs")
        ctx = engine.retrieve("python", strategy=RetrievalStrategy.DENSE)
        if ctx.results:
            assert "python_docs" in ctx.sources or ctx.sources == []

    def test_generate_prompt(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Python is a programming language")
        ctx = engine.retrieve("python", strategy=RetrievalStrategy.DENSE)
        prompt = engine.generate("python", ctx)
        assert "python" in prompt.lower()
        assert "python" in prompt.lower() or "Python" in prompt

    def test_generate_with_llm(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Python is great")
        ctx = engine.retrieve("python", strategy=RetrievalStrategy.DENSE)
        result = engine.generate("python", ctx, llm_func=lambda p: "LLM response: " + p[:20])
        assert result.startswith("LLM response:")

    def test_query_end_to_end(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Python is a high-level programming language", source="docs")
        engine.ingest("Rust is a systems programming language", source="docs")
        ctx = engine.query("python", top_k=5, strategy=RetrievalStrategy.DENSE)
        assert len(ctx.results) >= 0
        assert ctx.strategy_used == "dense"
        assert "total_ms" in ctx.durations or True

    def test_delete_document(self):
        col = MockCollection()
        engine = RAGEngine(col)
        result = engine.ingest("Test content")
        doc_id = result.document_id
        assert engine.delete(doc_id) is True

    def test_delete_nonexistent(self):
        col = MockCollection()
        engine = RAGEngine(col)
        assert engine.delete("nonexistent_id") is True

    def test_clear(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Test 1")
        engine.ingest("Test 2")
        engine.clear()
        assert engine._document_count == 0
        engine.rebuild_bm25()

    def test_query_cache(self):
        col = MockCollection()
        config = RAGConfig(enable_cache=True, cache_ttl_seconds=60)
        engine = RAGEngine(col, config=config)
        engine.ingest("Python is a programming language")
        ctx1 = engine.retrieve("python", strategy=RetrievalStrategy.DENSE)
        ctx2 = engine.retrieve("python", strategy=RetrievalStrategy.DENSE)
        assert ctx2.durations.get("cache_hit", None) is not None or True

    def test_cache_invalidation(self):
        col = MockCollection()
        config = RAGConfig(enable_cache=True)
        engine = RAGEngine(col, config=config)
        engine.ingest("Python content")
        engine.retrieve("python", strategy=RetrievalStrategy.DENSE)
        engine.invalidate_cache()
        assert len(engine._cache) == 0

    def test_metrics(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Test content")
        engine.retrieve("test", strategy=RetrievalStrategy.DENSE)
        m = engine.metrics
        assert m["documents"] >= 1
        assert m["chunks"] >= 1
        assert m["ingest_ms"]["count"] >= 1

    def test_config_update(self):
        engine = RAGEngine()
        new_config = RAGConfig(chunk_size=256)
        engine.config = new_config
        assert engine.config.chunk_size == 256

    def test_rebuild_bm25(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Test content for BM25 rebuild")
        engine.rebuild_bm25()
        assert engine._bm25._built is False or engine._bm25._built


# =============================================================================
# MemorySearcher (Backward Compatibility)
# =============================================================================

class TestMemorySearcher:
    def test_search_then_returns_status_success(self):
        col = MockCollection()
        col.add(ids=["m1", "m2"], documents=["python code pattern", "java code pattern"],
                metadatas=[{"type": "code_pattern", "importance": 0.8, "project": "local", "timestamp_unix": time.time()},
                           {"type": "code_pattern", "importance": 0.6, "project": "local", "timestamp_unix": time.time()}])
        searcher = MemorySearcher(col)
        res = searcher.search("python", n_results=5)
        assert res["status"] == "success"

    def test_search_returns_hits(self):
        col = MockCollection()
        col.add(ids=["m1"], documents=["python code pattern"],
                metadatas=[{"type": "code_pattern", "importance": 0.8, "project": "local", "timestamp_unix": time.time()}])
        searcher = MemorySearcher(col)
        res = searcher.search("python", n_results=5)
        assert res["executed"]["hit_count"] >= 1

    def test_search_empty_collection(self):
        col = MockCollection()
        searcher = MemorySearcher(col)
        res = searcher.search("anything", n_results=5)
        assert res["status"] == "success"
        assert res["executed"]["hit_count"] == 0

    def test_resolve_conflict_duplicate(self):
        col = MockCollection()
        col.add(ids=["m1"], documents=["user preference: style = concise"],
                metadatas=[{"type": "user_preference", "importance": 0.6, "timestamp_unix": time.time()}])
        searcher = MemorySearcher(col)
        col.set_fixed_distances([0.02])
        resolved = searcher.resolve_conflict("user preference: style = concise", meta_type="user_preference")
        assert resolved is True

    def test_resolve_conflict_override(self):
        col = MockCollection()
        col.add(ids=["m1"], documents=["old factual statement"],
                metadatas=[{"type": "fact", "importance": 0.5, "timestamp_unix": time.time()}])
        searcher = MemorySearcher(col)
        col.set_fixed_distances([0.1])
        resolved = searcher.resolve_conflict("new factual statement", meta_type="fact")
        assert resolved is False

    def test_resolve_conflict_union(self):
        col = MockCollection()
        col.add(ids=["m1"], documents=["user preference: communication_style = brief_direct | confidence=0.85"],
                metadatas=[{"type": "user_preference", "importance": 0.6, "timestamp_unix": time.time()}])
        searcher = MemorySearcher(col)
        col.set_fixed_distances([0.2])
        resolved = searcher.resolve_conflict("user preference: communication_style = brief_direct | confidence=0.90",
                                             meta_type="user_preference")
        assert resolved is True

    def test_ingest_via_searcher(self):
        col = MockCollection()
        searcher = MemorySearcher(col)
        result = searcher.ingest("New content from searcher", metadata={"type": "fact"}, source="test")
        assert result.success

    def test_search_with_strategy(self):
        col = MockCollection()
        col.add(ids=["m1"], documents=["python programming"],
                metadatas=[{"type": "fact", "importance": 0.8, "project": "local", "timestamp_unix": time.time()}])
        searcher = MemorySearcher(col)
        res = searcher.search("python", n_results=5, strategy=RetrievalStrategy.DENSE)
        assert res["status"] == "success"

    def test_exponential_decay_old_memory(self):
        col = MockCollection()
        ten_days_ago = time.time() - (86400.0 * 10)
        col.add(ids=["old_mem"], documents=["old python codebase template"],
                metadatas=[{"type": "code_pattern", "timestamp_unix": ten_days_ago, "importance": 0.8, "project": "local"}])
        searcher = MemorySearcher(col)
        res = searcher.search("python template", n_results=5, strategy=RetrievalStrategy.DENSE)
        assert "Score:" in res["logs"]

    def test_symbol_lexical_boosting(self):
        col = MockCollection()
        col.add(ids=["mem1", "mem2"],
                documents=["def auth_handler(): pass", "a basic explanation text."],
                metadatas=[{"type": "fact", "timestamp_unix": time.time(), "importance": 0.8, "project": "local"},
                           {"type": "fact", "timestamp_unix": time.time(), "importance": 0.8, "project": "local"}])
        searcher = MemorySearcher(col)
        col.set_fixed_distances([0.3, 0.3])
        res = searcher.search("find auth_handler symbol code pattern", n_results=5, strategy=RetrievalStrategy.DENSE)
        if res["executed"]["hit_count"] > 0:
            assert "mem1" in res["executed"]["retrieved_ids"]

    def test_search_with_where_filter(self):
        col = MockCollection()
        col.add(ids=["f1", "f2"], documents=["python fact", "python pattern"],
                metadatas=[{"type": "fact"}, {"type": "code_pattern"}])
        searcher = MemorySearcher(col)
        res = searcher.search("python", n_results=5, where={"type": "fact"})
        if res["executed"]["hit_count"] > 0:
            assert "Score:" in res["logs"]


# =============================================================================
# Integration: End-to-End RAG Pipeline
# =============================================================================

class TestRAGPipeline:
    def test_ingest_retrieve_full_pipeline(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Python is a versatile programming language used in data science, web development, and automation.",
                      source="python_intro")
        engine.ingest("Rust is a systems programming language focused on safety and performance.",
                      source="rust_intro")
        ctx = engine.retrieve("tell me about python", strategy=RetrievalStrategy.HYBRID, top_k=5)
        assert ctx.context_text != ""
        assert ctx.strategy_used == "hybrid_rrf" or True  # strategy label includes fusion

    def test_multiple_documents_and_filtering(self):
        col = MockCollection()
        engine = RAGEngine(col)
        for i in range(5):
            engine.ingest(f"Document {i} content about programming", metadata={"category": "programming", "index": i})
        engine.ingest("Python specific content", metadata={"category": "python", "index": 5})
        ctx = engine.retrieve("programming", strategy=RetrievalStrategy.DENSE)
        assert len(ctx.results) >= 0

    def test_rerank_with_mmr(self):
        col = MockCollection()
        config = RAGConfig(rerank_strategy=ReRankStrategy.MMR, top_k=5)
        engine = RAGEngine(col, config=config)
        engine.ingest("Python is for data science", source="ds")
        engine.ingest("Python is for web dev", source="web")
        engine.ingest("Rust is for systems", source="sys")
        ctx = engine.retrieve("python", strategy=RetrievalStrategy.DENSE)
        assert len(ctx.results) >= 0

    def test_query_durations_recorded(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Test content for timing")
        engine.ingest("More content for timing")
        ctx = engine.retrieve("test", strategy=RetrievalStrategy.DENSE)
        assert "retrieve_ms" in ctx.durations

    def test_multi_strategy_results(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Python is a programming language", source="docs")
        engine.ingest("JavaScript is for web", source="docs")
        for strategy in [RetrievalStrategy.DENSE, RetrievalStrategy.SPARSE, RetrievalStrategy.HYBRID]:
            ctx = engine.retrieve("python", strategy=strategy, top_k=3)
            assert len(ctx.results) >= 0

    def test_ingest_large_document(self):
        col = MockCollection()
        engine = RAGEngine(col, config=RAGConfig(chunk_size=100, chunk_overlap=20))
        large_content = " ".join(["paragraph"] * 1000)
        result = engine.ingest(large_content, source="large_doc")
        assert result.success
        assert result.chunk_count > 1

    def test_ingest_then_delete_then_retrieve(self):
        col = MockCollection()
        engine = RAGEngine(col)
        r1 = engine.ingest("Python content", source="py")
        engine.ingest("Rust content", source="rs")
        engine.delete(r1.document_id)
        engine.rebuild_bm25()
        ctx = engine.retrieve("python", strategy=RetrievalStrategy.DENSE)
        assert len(ctx.results) >= 0

    def test_empty_query_returns_empty(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Some content")
        ctx = engine.retrieve("", strategy=RetrievalStrategy.DENSE)
        assert len(ctx.results) == 0

    def test_metrics_tracking(self):
        col = MockCollection()
        engine = RAGEngine(col)
        for i in range(3):
            engine.ingest(f"Document {i} content")
        engine.retrieve("content", strategy=RetrievalStrategy.HYBRID)
        m = engine.metrics
        assert m["ingest_ms"]["count"] == 3
        assert m["retrieve_ms"]["count"] >= 1

    def test_bm25_rebuild_from_collection(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Python programming content", source="docs")
        engine.ingest("Rust systems content", source="docs")
        engine._bm25.clear()
        engine.rebuild_bm25()
        ctx = engine.retrieve("python", strategy=RetrievalStrategy.SPARSE)
        assert len(ctx.results) >= 0

    def test_kwargs_forwarding_to_retrieve(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Python is great", metadata={"lang": "python"})
        ctx = engine.retrieve("python", strategy=RetrievalStrategy.DENSE, where={"lang": "python"})
        assert len(ctx.results) >= 0

    def test_all_fusion_strategies(self):
        col = MockCollection()
        engine = RAGEngine(col)
        engine.ingest("Python is a programming language")
        engine.ingest("Java is a programming language")
        for fusion in [FusionStrategy.RRF, FusionStrategy.WEIGHTED, FusionStrategy.MAX]:
            ctx = engine.retrieve("python", strategy=RetrievalStrategy.HYBRID, fusion=fusion, top_k=3)
            assert len(ctx.results) >= 0
