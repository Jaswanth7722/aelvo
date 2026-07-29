"""Shared test utilities for the AELVO test suite."""

from typing import Dict, List, Any


class MockCollection:
    """Mock ChromaDB Collection for RAG tests.

    Supports add, update, delete, get, and query with optional where filters.
    """

    def __init__(self):
        self.name = "mock_collection"
        self._ids: List[str] = []
        self._docs: List[str] = []
        self._metas: List[Dict[str, Any]] = []
        self._fixed_distances: List[float] | None = None

    def set_fixed_distances(self, dists: List[float]):
        self._fixed_distances = dists

    def add(self, ids, documents, metadatas=None):
        metadatas = metadatas or [{}] * len(ids)
        for mid, doc, meta in zip(ids, documents, metadatas):
            if mid not in self._ids:
                self._ids.append(mid)
                self._docs.append(doc)
                self._metas.append(dict(meta))
            else:
                idx = self._ids.index(mid)
                self._docs[idx] = doc
                self._metas[idx] = dict(meta)

    def update(self, ids, documents=None, metadatas=None):
        for idx, mid in enumerate(ids):
            if mid in self._ids:
                pos = self._ids.index(mid)
                if documents is not None:
                    self._docs[pos] = documents[idx]
                if metadatas is not None:
                    self._metas[pos] = dict(metadatas[idx])

    def delete(self, ids):
        for mid in ids:
            if mid in self._ids:
                pos = self._ids.index(mid)
                self._ids.pop(pos)
                self._docs.pop(pos)
                self._metas.pop(pos)

    def get(self, ids=None, where=None, include=None, limit=None):
        selected = ids or self._ids
        out_ids, out_docs, out_metas = [], [], []
        for mid in selected:
            if mid not in self._ids:
                continue
            pos = self._ids.index(mid)
            meta = self._metas[pos]
            if where and not self._matches_where(meta, where):
                continue
            out_ids.append(mid)
            out_docs.append(self._docs[pos])
            out_metas.append(dict(meta))
            if limit and len(out_ids) >= limit:
                break
        result = {"ids": out_ids, "documents": out_docs, "metadatas": out_metas}
        if "distances" in (include or []):
            result["distances"] = [[0.3] * len(out_ids)]
        return result

    def query(self, query_texts, n_results=5, where=None, include=None):
        out_ids, out_docs, out_metas = [], [], []
        for pos, mid in enumerate(self._ids):
            meta = self._metas[pos]
            if where and not self._matches_where(meta, where):
                continue
            out_ids.append(mid)
            out_docs.append(self._docs[pos])
            out_metas.append(dict(meta))
        if self._fixed_distances:
            distances = [self._fixed_distances[:len(out_ids)]]
        else:
            distances = [[0.3] * len(out_ids)]
        return {
            "ids": [out_ids],
            "documents": [out_docs],
            "metadatas": [out_metas],
            "distances": distances,
        }

    @staticmethod
    def _matches_where(meta, where):
        if not where:
            return True
        for key, expected in where.items():
            actual = meta.get(key)
            if isinstance(expected, dict) and "$in" in expected:
                if actual not in expected["$in"]:
                    return False
            elif actual != expected:
                return False
        return True
