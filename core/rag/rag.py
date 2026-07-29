from __future__ import annotations

import time
import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple, Callable

from core.rag.types import (
    Document, Chunk, RetrievalResult, RAGContext, IngestResult,
    RetrievalStrategy, FusionStrategy, ReRankStrategy,
    RAGConfig, DEFAULT_RAG_CONFIG,
)
from core.rag.chunking import chunk_document
from core.rag.retrieval import (
    DenseRetriever, BM25Index, HybridRetriever, MultiQueryRetriever,
    rerank_results,
)

log = logging.getLogger("aelvo.rag")


class RAGEngine:
    """Production-grade RAG engine with multi-strategy retrieval.

    Features:
    - Multi-strategy retrieval: dense vector, sparse BM25, hybrid, multi-query
    - Document chunking with configurable strategies
    - Re-ranking (MMR, recency)
    - BM25 index for sparse retrieval
    - Query result caching with TTL
    - Context assembly with source tracking
    - Observability via metrics and logging
    """

    def __init__(
        self,
        chroma_collection=None,
        config: Optional[RAGConfig] = None,
        embedding_fn: Optional[Callable] = None,
    ):
        self._collection = chroma_collection
        self._config = config or DEFAULT_RAG_CONFIG
        self._bm25 = BM25Index()
        self._embedding_fn = embedding_fn
        self._cache: Dict[str, Tuple[float, List[RetrievalResult]]] = {}
        self._metrics: Dict[str, List[float]] = {
            "ingest_ms": [],
            "retrieve_ms": [],
            "query_ms": [],
        }
        self._document_count = 0
        self._chunk_count = 0

    @property
    def config(self) -> RAGConfig:
        return self._config

    @config.setter
    def config(self, value: RAGConfig) -> None:
        self._config = value

    @property
    def metrics(self) -> Dict[str, Any]:
        return {
            "documents": self._document_count,
            "chunks": self._chunk_count,
            "bm25_built": self._bm25._built,
            "cache_size": len(self._cache),
            **{
                k: {
                    "count": len(v),
                    "avg_ms": round(sum(v) / max(1, len(v)), 2),
                    "total_ms": round(sum(v), 2),
                }
                for k, v in self._metrics.items()
            },
        }

    # â”€â”€ Ingest Pipeline â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def ingest(
        self,
        content: str,
        document_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        source: str = "",
    ) -> IngestResult:
        start = time.perf_counter()
        doc_id = document_id or self._generate_id("doc", content)
        doc = Document(
            id=doc_id,
            content=content,
            metadata=metadata or {},
            source=source,
        )
        try:
            chunks = chunk_document(
                doc,
                strategy=self._config.chunking_strategy,
                chunk_size=self._config.chunk_size,
                chunk_overlap=self._config.chunk_overlap,
            )
            if not chunks:
                chunks.append(Chunk(
                    id=doc_id + "_0",
                    document_id=doc_id,
                    content=content,
                    metadata={**doc.metadata, "source": source},
                ))
            if self._collection is not None:
                self._store_chunks(doc, chunks)
            self._bm25.add_chunks(chunks)
            self._bm25._built = False

            self._document_count += 1
            self._chunk_count += len(chunks)
            duration = (time.perf_counter() - start) * 1000
            self._metrics["ingest_ms"].append(duration)
            log.info("Ingested doc %s: %d chunks (%.1fms)", doc_id, len(chunks), duration)
            return IngestResult(
                document_id=doc_id,
                chunk_count=len(chunks),
                success=True,
                duration_ms=round(duration, 2),
            )
        except Exception as e:
            duration = (time.perf_counter() - start) * 1000
            log.error("Ingest failed for %s: %s", doc_id, e)
            return IngestResult(
                document_id=doc_id,
                success=False,
                error=str(e),
                duration_ms=round(duration, 2),
            )

    def ingest_document(self, document: Document) -> IngestResult:
        return self.ingest(
            content=document.content,
            document_id=document.id,
            metadata=document.metadata,
            source=document.source,
        )

    # â”€â”€ Retrieve Pipeline â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def retrieve(
        self,
        query: str,
        top_k: Optional[int] = None,
        strategy: Optional[RetrievalStrategy] = None,
        where: Optional[Dict[str, Any]] = None,
        fusion: Optional[FusionStrategy] = None,
    ) -> RAGContext:
        start = time.perf_counter()
        top_k = top_k or self._config.top_k
        strategy = strategy or self._config.retrieval_strategy
        fusion = fusion or self._config.fusion_strategy

        if not query.strip():
            return RAGContext(query=query, strategy_used=strategy.value)

        strategy_label = strategy.value
        if strategy == RetrievalStrategy.HYBRID:
            strategy_label = f"hybrid_{fusion.value}"
        elif strategy == RetrievalStrategy.MULTI_QUERY:
            strategy_label = f"multi_query_{fusion.value}"
        ctx = RAGContext(query=query, strategy_used=strategy_label)

        cache_key = self._cache_key(query, strategy, top_k, where)
        if self._config.enable_cache and cache_key in self._cache:
            cached_at, cached_results = self._cache[cache_key]
            if time.time() - cached_at < self._config.cache_ttl_seconds:
                ctx.results = cached_results
                ctx.sources = list({r.source for r in cached_results if r.source})
                ctx.context_text = self._assemble_context(cached_results)
                ctx.token_count = self._estimate_tokens(ctx.context_text)
                ctx.durations["cache_hit"] = 0.0
                log.debug("Cache hit for query: %s", query[:50])
                return ctx

        if strategy == RetrievalStrategy.DENSE:
            results = self._dense_search(query, top_k, where)
        elif strategy == RetrievalStrategy.SPARSE:
            results = self._sparse_search(query, top_k)
        elif strategy == RetrievalStrategy.MULTI_QUERY:
            results = self._multi_query_search(query, top_k, where, fusion)
        else:
            results = self._hybrid_search(query, top_k, where, fusion)

        rerank_strat = self._config.rerank_strategy
        if rerank_strat != ReRankStrategy.NONE:
            results = rerank_results(results, query, strategy=rerank_strat, top_k=top_k)

        if self._config.min_score > 0:
            results = [r for r in results if r.score >= self._config.min_score]

        for rank, r in enumerate(results):
            r.rank = rank

        ctx.results = results
        ctx.sources = list({r.source for r in results if r.source})
        ctx.context_text = self._assemble_context(results)
        ctx.token_count = self._estimate_tokens(ctx.context_text)

        if self._config.enable_cache and cache_key:
            self._cache[cache_key] = (time.time(), results)

        duration = (time.perf_counter() - start) * 1000
        self._metrics["retrieve_ms"].append(duration)
        ctx.durations["retrieve_ms"] = round(duration, 2)
        log.info("Retrieved %d results for '%s' (%.1fms)", len(results), query[:40], duration)
        return ctx

    # â”€â”€ Generate Pipeline â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def generate(
        self,
        query: str,
        context: RAGContext,
        llm_func: Optional[Callable[[str], str]] = None,
    ) -> str:
        prompt = self._config.system_prompt_template.format(
            context=context.context_text,
            query=query,
        )
        if llm_func is not None:
            return llm_func(prompt)
        return prompt

    def query(
        self,
        query: str,
        llm_func: Optional[Callable[[str], str]] = None,
        top_k: Optional[int] = None,
        strategy: Optional[RetrievalStrategy] = None,
        where: Optional[Dict[str, Any]] = None,
    ) -> RAGContext:
        start = time.perf_counter()
        ctx = self.retrieve(query, top_k=top_k, strategy=strategy, where=where)
        if llm_func is not None:
            ctx.context_text = self.generate(query, ctx, llm_func)
        duration = (time.perf_counter() - start) * 1000
        self._metrics["query_ms"].append(duration)
        ctx.durations["total_ms"] = round(duration, 2)
        return ctx

    # â”€â”€ Document Management â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def delete(self, document_id: str) -> bool:
        try:
            if self._collection is not None:
                all_chunks = self._collection.get(
                    where={"document_id": document_id}
                )
                if all_chunks.get("ids"):
                    self._collection.delete(ids=all_chunks["ids"])
            self._bm25.clear()
            self._cache.clear()
            self._document_count = max(0, self._document_count - 1)
            log.info("Deleted document %s", document_id)
            return True
        except Exception as e:
            log.error("Delete failed for %s: %s", document_id, e)
            return False

    def clear(self) -> None:
        try:
            if self._collection is not None:
                existing = self._collection.get()
                if existing.get("ids"):
                    self._collection.delete(ids=existing["ids"])
        except Exception as e:
            log.warning("Collection clear failed: %s", e)
        self._bm25.clear()
        self._cache.clear()
        self._document_count = 0
        self._chunk_count = 0
        log.info("RAG engine cleared")

    def rebuild_bm25(self) -> None:
        if self._collection is None:
            return
        try:
            self._bm25.clear()
            all_data = self._collection.get(include=["documents", "metadatas"])
            if not all_data.get("ids"):
                return
            for cid, content, meta in zip(
                all_data["ids"], all_data.get("documents", []), all_data.get("metadatas", [])
            ):
                chunk = Chunk(
                    id=cid,
                    document_id=meta.get("document_id", "") if isinstance(meta, dict) else "",
                    content=content or "",
                    index=meta.get("chunk_index", 0) if isinstance(meta, dict) else 0,
                    metadata=meta if isinstance(meta, dict) else {},
                )
                self._bm25.add_chunk(chunk)
            self._bm25.build()
            log.info("BM25 rebuilt from collection (%d chunks)", len(all_data["ids"]))
        except Exception as e:
            log.warning("BM25 rebuild failed: %s", e)

    # â”€â”€ Internal Retrieval Methods â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _dense_search(self, query: str, top_k: int, where: Optional[Dict]) -> List[RetrievalResult]:
        if self._collection is not None:
            retriever = DenseRetriever(self._collection, self._embedding_fn)
            return retriever.search(query, top_k=top_k, where=where, min_score=self._config.min_score)
        return []

    def _sparse_search(self, query: str, top_k: int) -> List[RetrievalResult]:
        return self._bm25.search(query, top_k=top_k, min_score=self._config.min_score)

    def _hybrid_search(
        self,
        query: str,
        top_k: int,
        where: Optional[Dict],
        fusion: FusionStrategy,
    ) -> List[RetrievalResult]:
        dense = DenseRetriever(self._collection, self._embedding_fn) if self._collection else DenseRetriever(None)
        hybrid = HybridRetriever(dense, self._bm25)
        return hybrid.search(
            query, top_k=top_k, fusion=fusion,
            dense_weight=self._config.dense_weight,
            sparse_weight=self._config.sparse_weight,
            rrf_constant=self._config.rrf_constant,
            min_score=self._config.min_score,
            where=where,
        )

    def _multi_query_search(
        self,
        query: str,
        top_k: int,
        where: Optional[Dict],
        fusion: FusionStrategy,
    ) -> List[RetrievalResult]:
        dense = DenseRetriever(self._collection, self._embedding_fn) if self._collection else DenseRetriever(None)
        hybrid = HybridRetriever(dense, self._bm25)
        mq = MultiQueryRetriever(hybrid)
        return mq.search(
            query, top_k=top_k, fusion=fusion,
            rrf_constant=self._config.rrf_constant,
            min_score=self._config.min_score,
            where=where,
        )

    # â”€â”€ Persistence â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _store_chunks(self, doc: Document, chunks: List[Chunk]) -> None:
        for chunk in chunks:
            meta = {
                **chunk.metadata,
                "document_id": doc.id,
                "chunk_id": chunk.id,
                "chunk_index": chunk.index,
                "source": doc.source,
                "timestamp_unix": time.time(),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            if self._collection is not None:
                try:
                    self._collection.add(
                        ids=[chunk.id],
                        documents=[chunk.content],
                        metadatas=[meta],
                    )
                except Exception as e:
                    log.warning("Chunk store failed for %s: %s", chunk.id, e)

    # â”€â”€ Cache â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _cache_key(self, query: str, strategy: RetrievalStrategy, top_k: int, where: Optional[Dict]) -> str:
        raw = f"{query}|{strategy.value}|{top_k}|{where}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def invalidate_cache(self) -> None:
        self._cache.clear()

    # â”€â”€ Context Assembly â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _assemble_context(self, results: List[RetrievalResult]) -> str:
        parts: List[str] = []
        for i, r in enumerate(results):
            source_tag = f"[Source: {r.source}]" if r.source else ""
            score_tag = f"[Relevance: {r.score:.3f}]"
            parts.append(f"Document {i + 1}: {source_tag} {score_tag}\n{r.content}")
        return self._config.separator.join(parts)

    # â”€â”€ Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _generate_id(self, prefix: str, content: str) -> str:
        raw = f"{prefix}_{content}_{datetime.now(timezone.utc).isoformat()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    @staticmethod
    def _estimate_tokens(text: str) -> int:
        return len(text) // 4 + 1


class MemorySearcher:
    """High-performance Vector Search with hybrid scoring, conflict resolution,
    and fallback federated search. Fully backward-compatible.

    Enhanced with multi-strategy retrieval, re-ranking, and caching via RAGEngine.
    """

    def __init__(self, chroma_collection, config: Optional[RAGConfig] = None):
        self.collection = chroma_collection
        self._engine = RAGEngine(chroma_collection, config=config)
        self._rebuild_bm25()

    # â”€â”€ Conflict Resolution (full backward compat) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def resolve_conflict(self, new_doc: str, meta_type: str = "fact") -> bool:
        try:
            results = self.collection.query(
                query_texts=[new_doc],
                n_results=1,
                include=["documents", "metadatas", "distances"],
            )
            if not results["ids"] or not results["ids"][0]:
                return False
            dist = results["distances"][0][0]
            similarity = 1.0 - dist
            existing_id = results["ids"][0][0]
            if similarity > 0.95:
                try:
                    meta = dict(results["metadatas"][0][0])
                    meta["usage_count"] = int(meta.get("usage_count", 0)) + 1
                    meta["importance"] = min(1.0, float(meta.get("importance", 0.5)) + 0.05)
                    self.collection.update(ids=[existing_id], metadatas=[meta])
                except Exception as _ex: log.debug("Silenced exception: %s", _ex)
                return True
            if similarity > 0.85 and meta_type in ("fact", "voluntary", "semantic"):
                self.collection.delete(ids=[existing_id])
                return False
            if 0.75 <= similarity < 0.85 and meta_type in ("user_preference", "fact", "voluntary"):
                existing_doc = results["documents"][0][0]
                existing_meta = dict(results["metadatas"][0][0])
                if new_doc not in existing_doc:
                    merged_doc = f"{existing_doc}\nUPDATED CONTEXT:\n{new_doc}"
                    existing_meta["usage_count"] = int(existing_meta.get("usage_count", 0)) + 1
                    existing_meta["importance"] = min(1.0, float(existing_meta.get("importance", 0.5)) + 0.05)
                    existing_meta["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
                    existing_meta["timestamp_unix"] = time.time()
                    try:
                        self.collection.update(
                            ids=[existing_id],
                            documents=[merged_doc],
                            metadatas=[existing_meta],
                        )
                        self._rebuild_bm25()
                        return True
                    except Exception as e:
                        log.error("Semantic Union update failed: %s", e)
            return False
        except Exception as e:
            log.error("Conflict Resolution Failure: %s", e)
            return False

    # â”€â”€ Search (backward compat + enhanced) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def search(
        self,
        query: str,
        n_results: int = 5,
        where: Dict[str, Any] = None,
        strategy: Optional[RetrievalStrategy] = None,
    ) -> Dict[str, Any]:
        ctx = self._engine.retrieve(
            query=query,
            top_k=n_results,
            where=where,
            strategy=strategy,
        )

        if ctx.results:
            return self._format_results(ctx, query)
        return self._fallback_federated_search(query, n_results, where)

    # â”€â”€ Ingestion â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def ingest(self, content: str, metadata: Optional[Dict[str, Any]] = None, source: str = "") -> IngestResult:
        return self._engine.ingest(content, metadata=metadata, source=source)

    # â”€â”€ Internal â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _format_results(self, ctx: RAGContext, query: str) -> Dict[str, Any]:
        formatted = []
        used_ids = []
        for r in ctx.results:
            m_type = r.metadata.get("type", "fact").upper() if r.metadata else "FACT"
            proj_tag = f"[{r.metadata.get('project', 'local')}] " if r.metadata and "project" in r.metadata else ""
            formatted.append(f"[{m_type}] (Score: {r.score}) {proj_tag}{r.content}")
            used_ids.append(r.chunk_id)
        report = "\n".join(formatted)
        return {
            "status": "success",
            "logs": (
                f"Weighted Vector Hits ({len(ctx.results)} hits found):\n\n{report}"
            ),
            "executed": {
                "query": query,
                "hit_count": len(ctx.results),
                "retrieved_ids": used_ids,
                "strategy": ctx.strategy_used,
                "duration_ms": ctx.durations.get("retrieve_ms", 0),
            },
        }

    def _fallback_federated_search(
        self, query: str, n_results: int, where: Dict[str, Any],
    ) -> Dict[str, Any]:
        federated_hits = []
        try:
            client = None
            if hasattr(self.collection, "_client"):
                client = self.collection._client
            elif hasattr(self.collection, "client"):
                client = self.collection.client
            if client and hasattr(client, "list_collections"):
                cols = client.list_collections()
                for col in cols:
                    if col.name == self.collection.name:
                        continue
                    try:
                        res = col.query(
                            query_texts=[query],
                            n_results=2,
                            include=["documents", "metadatas", "distances"],
                        )
                        if res["ids"] and res["ids"][0]:
                            for mid, doc, meta, dist in zip(
                                res["ids"][0], res["documents"][0],
                                res["metadatas"][0], res["distances"][0],
                            ):
                                sim = max(0.0, 1.0 - dist)
                                meta_copy = dict(meta)
                                meta_copy["project"] = f"FEDERATED:{col.name}"
                                federated_hits.append((round(sim * 0.7, 3), mid, doc, meta_copy))
                    except Exception as _ex: log.debug("Silenced exception: %s", _ex)
            if federated_hits:
                federated_hits.sort(key=lambda x: x[0], reverse=True)
                final_hits = federated_hits[:3]
                formatted = []
                used_ids = []
                for score, mid, doc, meta in final_hits:
                    m_type = meta.get("type", "fact").upper()
                    proj_tag = f"[{meta.get('project')}] "
                    formatted.append(f"[{m_type}] (Score: {score}) {proj_tag}{doc}")
                    used_ids.append(mid)
                return {
                    "status": "success",
                    "logs": f"Federated Hits ({len(final_hits)} hits found):\n\n" + "\n".join(formatted),
                    "executed": {"query": query, "hit_count": len(final_hits), "retrieved_ids": used_ids},
                }
        except Exception as e:
            log.error("Federated search failed: %s", e)
        return {"status": "success", "logs": "No conceptual matches found.", "executed": {"hit_count": 0}}

    def _rebuild_bm25(self) -> None:
        try:
            self._engine.rebuild_bm25()
        except Exception as e:
            log.debug("BM25 rebuild skipped: %s", e)

    def __getattr__(self, name):
        if hasattr(self._engine, name):
            return getattr(self._engine, name)
        raise AttributeError(f"'MemorySearcher' has no attribute '{name}'")


__all__ = ["RAGEngine", "MemorySearcher"]
