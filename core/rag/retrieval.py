from __future__ import annotations

import re
import math
import logging
from collections import Counter, defaultdict
from typing import Dict, List, Optional, Any, Callable

from core.rag.types import (
    Chunk, RetrievalResult, FusionStrategy,
    ReRankStrategy,
)

log = logging.getLogger("aelvo.rag.retrieval")


class BM25Index:
    """In-memory BM25 index for sparse retrieval."""

    def __init__(self, k1: float = 1.5, b: float = 0.75):
        self.k1 = k1
        self.b = b
        self._doc_count: int = 0
        self._avg_dl: float = 0.0
        self._doc_lengths: Dict[str, int] = {}
        self._doc_freq: Dict[str, int] = {}
        self._term_freq: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._chunks: Dict[str, Chunk] = {}
        self._built = False

    def add_chunk(self, chunk: Chunk) -> None:
        terms = self._tokenize(chunk.content)
        for term in set(terms):
            self._doc_freq[term] = self._doc_freq.get(term, 0) + 1
        for term in terms:
            self._term_freq[chunk.id][term] += 1
        self._doc_lengths[chunk.id] = len(terms)
        self._chunks[chunk.id] = chunk

    def add_chunks(self, chunks: List[Chunk]) -> None:
        for chunk in chunks:
            self.add_chunk(chunk)

    def build(self) -> None:
        if self._built:
            return
        self._doc_count = len(self._doc_lengths)
        total_length = sum(self._doc_lengths.values())
        self._avg_dl = total_length / max(1, self._doc_count)
        self._built = True
        log.debug("BM25 index built: %d docs, avg_dl=%.1f", self._doc_count, self._avg_dl)

    def search(
        self,
        query: str,
        top_k: int = 10,
        min_score: float = 0.0,
    ) -> List[RetrievalResult]:
        if not self._built:
            self.build()
        if self._doc_count == 0:
            return []
        query_terms = self._tokenize(query)
        if not query_terms:
            return []
        scores = Counter()
        for chunk_id, doc_len in self._doc_lengths.items():
            score = 0.0
            for term in query_terms:
                tf = self._term_freq[chunk_id].get(term, 0)
                if tf == 0:
                    continue
                df = self._doc_freq.get(term, 0)
                idf = math.log((self._doc_count - df + 0.5) / (df + 0.5) + 1.0)
                numerator = tf * (self.k1 + 1)
                denominator = tf + self.k1 * (1 - self.b + self.b * doc_len / max(1, self._avg_dl))
                score += idf * (numerator / denominator)
            scores[chunk_id] = score
        scored = [(score, cid) for cid, score in scores.items() if score > min_score]
        scored.sort(key=lambda x: x[0], reverse=True)
        results = []
        for rank, (score, cid) in enumerate(scored[:top_k]):
            chunk = self._chunks.get(cid)
            if chunk is None:
                continue
            results.append(RetrievalResult(
                chunk_id=cid,
                document_id=chunk.document_id,
                content=chunk.content,
                score=round(score, 4),
                strategy="sparse",
                rank=rank,
                metadata=chunk.metadata,
                source=chunk.metadata.get("source", ""),
            ))
        return results

    def clear(self) -> None:
        self._doc_count = 0
        self._avg_dl = 0.0
        self._doc_lengths.clear()
        self._doc_freq.clear()
        self._term_freq.clear()
        self._chunks.clear()
        self._built = False

    def _tokenize(self, text: str) -> List[str]:
        text_lower = text.lower()
        tokens = re.findall(r'\b[a-z0-9_]{2,}\b', text_lower)
        return [t for t in tokens if len(t) >= 2]


class DenseRetriever:
    """Vector-based dense retrieval via ChromaDB collection."""

    def __init__(self, chroma_collection, embedding_fn: Optional[Callable] = None):
        self._collection = chroma_collection
        self._embedding_fn = embedding_fn

    def search(
        self,
        query: str,
        top_k: int = 10,
        where: Optional[Dict[str, Any]] = None,
        min_score: float = 0.0,
        include_chunks: bool = True,
    ) -> List[RetrievalResult]:
        try:
            include = ["documents", "metadatas", "distances"]
            kwargs: Dict[str, Any] = {
                "query_texts": [query],
                "n_results": top_k,
                "include": include,
            }
            if where is not None:
                kwargs["where"] = where
            results = self._collection.query(**kwargs)
            if not results.get("ids") or not results["ids"][0]:
                return []
            ids = results["ids"][0]
            docs = results["documents"][0]
            metas = results["metadatas"][0]
            dists = results["distances"][0]
            scored: List[RetrievalResult] = []
            for rank, (cid, content, meta, dist) in enumerate(zip(ids, docs, metas, dists)):
                similarity = round(max(0.0, 1.0 - float(dist)), 4)
                if similarity < min_score:
                    continue
                chunk_id = meta.get("chunk_id", cid) if isinstance(meta, dict) else cid
                doc_id = meta.get("document_id", "") if isinstance(meta, dict) else ""
                scored.append(RetrievalResult(
                    chunk_id=chunk_id,
                    document_id=doc_id,
                    content=content,
                    score=round(similarity * self._boost(meta, query), 4),
                    strategy="dense",
                    rank=rank,
                    metadata=meta if isinstance(meta, dict) else {},
                    source=meta.get("source", "") if isinstance(meta, dict) else "",
                ))
            scored.sort(key=lambda x: x.score, reverse=True)
            return scored
        except Exception as e:
            log.warning("Dense search failed: %s", e)
            return []

    def _boost(self, meta: Any, query: str) -> float:
        boost = 1.0
        if not isinstance(meta, dict):
            return boost
        importance = float(meta.get("importance", 0.5))
        boost += (importance - 0.5) * 0.2
        query_symbols = set(re.findall(r'\b[A-Z][a-zA-Z0-9_]{2,}\b', query))
        if query_symbols:
            doc_text = str(meta.get("type", "")) + " " + str(meta.get("source", ""))
            matched = sum(1 for sym in query_symbols if sym.lower() in doc_text.lower())
            boost += matched * 0.1
        return round(min(boost, 1.5), 4)


class HybridRetriever:
    """Fuses dense and sparse retrieval results."""

    def __init__(
        self,
        dense_retriever: DenseRetriever,
        sparse_retriever: BM25Index,
    ):
        self._dense = dense_retriever
        self._sparse = sparse_retriever

    def search(
        self,
        query: str,
        top_k: int = 10,
        fusion: FusionStrategy = FusionStrategy.RRF,
        dense_weight: float = 0.5,
        sparse_weight: float = 0.5,
        rrf_constant: int = 60,
        min_score: float = 0.0,
        where: Optional[Dict[str, Any]] = None,
    ) -> List[RetrievalResult]:
        dense_results = self._dense.search(query, top_k=top_k * 2, where=where, min_score=0.0)
        sparse_results = self._sparse.search(query, top_k=top_k * 2, min_score=0.0)
        fused = self._fuse(
            dense_results, sparse_results,
            fusion=fusion, dense_weight=dense_weight,
            sparse_weight=sparse_weight, rrf_constant=rrf_constant,
        )
        fused.sort(key=lambda x: x.score, reverse=True)
        if min_score > 0:
            fused = [r for r in fused if r.score >= min_score]
        for rank, result in enumerate(fused[:top_k]):
            result.rank = rank
            result.strategy = f"hybrid_{fusion.value}"
        return fused[:top_k]

    def _fuse(
        self,
        dense: List[RetrievalResult],
        sparse: List[RetrievalResult],
        fusion: FusionStrategy,
        dense_weight: float,
        sparse_weight: float,
        rrf_constant: int,
    ) -> List[RetrievalResult]:
        if fusion == FusionStrategy.RRF:
            combined: Dict[str, RetrievalResult] = {}
            for result in dense:
                rank = result.rank if (result.rank is not None) else 0
                combined[result.chunk_id] = RetrievalResult(
                    chunk_id=result.chunk_id,
                    document_id=result.document_id,
                    content=result.content,
                    score=1.0 / (rrf_constant + rank + 1),
                    strategy="rrf",
                    rank=rank,
                    metadata=result.metadata,
                    source=result.source,
                )
            for result in sparse:
                rank = result.rank if (result.rank is not None) else 0
                if result.chunk_id in combined:
                    combined[result.chunk_id].score += 1.0 / (rrf_constant + rank + 1)
                else:
                    combined[result.chunk_id] = RetrievalResult(
                        chunk_id=result.chunk_id,
                        document_id=result.document_id,
                        content=result.content,
                        score=1.0 / (rrf_constant + rank + 1),
                        strategy="rrf",
                        rank=rank,
                        metadata=result.metadata,
                        source=result.source,
                    )
            for r in combined.values():
                r.score = round(r.score * rrf_constant, 4)
            return list(combined.values())

        combined: Dict[str, RetrievalResult] = {}
        for result in dense:
            combined[result.chunk_id] = RetrievalResult(
                chunk_id=result.chunk_id,
                document_id=result.document_id,
                content=result.content,
                score=result.score,
                strategy="dense",
                rank=result.rank,
                metadata=result.metadata,
                source=result.source,
            )
        for result in sparse:
            if result.chunk_id in combined:
                existing = combined[result.chunk_id]
                if fusion == FusionStrategy.WEIGHTED:
                    combined[result.chunk_id].score = (
                        existing.score * dense_weight + result.score * sparse_weight
                    )
                elif fusion == FusionStrategy.MAX:
                    combined[result.chunk_id].score = max(existing.score, result.score)
            else:
                combined[result.chunk_id] = RetrievalResult(
                    chunk_id=result.chunk_id,
                    document_id=result.document_id,
                    content=result.content,
                    score=result.score * sparse_weight,
                    strategy="sparse",
                    rank=result.rank,
                    metadata=result.metadata,
                    source=result.source,
                )
        return list(combined.values())


class MultiQueryRetriever:
    """Expands query into multiple sub-queries, retrieves for each, fuses."""

    def __init__(self, base_retriever: HybridRetriever, query_expander: Optional[Callable] = None):
        self._retriever = base_retriever
        self._expand = query_expander or self._default_expand

    def search(
        self,
        query: str,
        top_k: int = 10,
        fusion: FusionStrategy = FusionStrategy.RRF,
        rrf_constant: int = 60,
        min_score: float = 0.0,
        where: Optional[Dict[str, Any]] = None,
    ) -> List[RetrievalResult]:
        sub_queries = self._expand(query)
        all_results: Dict[str, List[RetrievalResult]] = {}
        for sq in sub_queries:
            results = self._retriever.search(
                sq, top_k=top_k, fusion=fusion, min_score=0.0, where=where
            )
            all_results[sq] = results
        fused: Dict[str, RetrievalResult] = {}
        for sq, results in all_results.items():
            for result in results:
                if result.chunk_id in fused:
                    existing = fused[result.chunk_id]
                    existing.score += 1.0 / (rrf_constant + result.rank + 1)
                else:
                    r = RetrievalResult(
                        chunk_id=result.chunk_id,
                        document_id=result.document_id,
                        content=result.content,
                        score=1.0 / (rrf_constant + result.rank + 1),
                        strategy="multi_query",
                        rank=result.rank,
                        metadata=result.metadata,
                        source=result.source,
                    )
                    fused[result.chunk_id] = r
        scored = list(fused.values())
        scored.sort(key=lambda x: x.score, reverse=True)
        if min_score > 0:
            scored = [r for r in scored if r.score >= min_score]
        for rank, result in enumerate(scored[:top_k]):
            result.rank = rank
        return scored[:top_k]

    def _default_expand(self, query: str) -> List[str]:
        parts = re.split(r'\b(?:and|or|but|however|additionally)\b', query, flags=re.IGNORECASE)
        cleaned = [p.strip() for p in parts if len(p.strip()) > 5]
        if not cleaned:
            cleaned = [query]
        return cleaned


def rerank_results(
    results: List[RetrievalResult],
    query: str,
    strategy: ReRankStrategy = ReRankStrategy.NONE,
    top_k: int = 10,
) -> List[RetrievalResult]:
    if not results or strategy == ReRankStrategy.NONE:
        return results[:top_k]
    if strategy == ReRankStrategy.RECENCY:
        results.sort(key=lambda r: float(r.metadata.get("timestamp_unix", 0)), reverse=True)
        return results[:top_k]
    if strategy == ReRankStrategy.MMR:
        return _mmr_rerank(results, query, top_k=top_k)
    return results[:top_k]


def _mmr_rerank(
    results: List[RetrievalResult],
    query: str,
    top_k: int = 10,
    lambda_param: float = 0.7,
) -> List[RetrievalResult]:
    if not results:
        return []
    selected: List[RetrievalResult] = []
    remaining = list(results)
    query_tokens = set(re.findall(r'\w+', query.lower()))
    while remaining and len(selected) < top_k:
        mmr_scores = []
        for i, r in enumerate(remaining):
            doc_tokens = set(re.findall(r'\w+', r.content.lower()))
            query_sim = len(query_tokens & doc_tokens) / max(1, len(query_tokens | doc_tokens))
            max_div = 0.0
            for s in selected:
                sel_tokens = set(re.findall(r'\w+', s.content.lower()))
                div = 1.0 - len(doc_tokens & sel_tokens) / max(1, len(doc_tokens | sel_tokens))
                max_div = max(max_div, div)
            mmr = lambda_param * query_sim + (1 - lambda_param) * max_div
            mmr_scores.append((mmr, i))
        mmr_scores.sort(key=lambda x: x[0], reverse=True)
        best_idx = mmr_scores[0][1]
        selected.append(remaining.pop(best_idx))
    return selected
