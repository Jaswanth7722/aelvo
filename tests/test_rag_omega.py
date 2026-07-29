"""
High-Performance Hybrid RAG Integration and Unit Tests
Run with: pytest tests/test_rag_omega.py -v
"""
import time
from core.rag import MemorySearcher
from helpers import MockCollection


def test_exponential_decay_calculations():
    col = MockCollection()
    searcher = MemorySearcher(col)

    col.add(
        ids=["old_mem"],
        documents=["This is an old python codebase template."],
        metadatas=[{
            "type": "code_pattern",
            "timestamp_unix": time.time() - (86400.0 * 10),
            "importance": 0.8,
            "project": "local",
        }],
    )

    res = searcher.search("python template", n_results=5)
    assert res["status"] == "success"
    logs = res["logs"]
    assert "Score:" in logs


def test_jaccard_symbol_lexical_boosting():
    col = MockCollection()
    col.add(
        ids=["mem1", "mem2"],
        documents=["def auth_handler(): pass", "a basic explanation text."],
        metadatas=[
            {"type": "fact", "timestamp_unix": time.time(), "importance": 0.8, "project": "local"},
            {"type": "fact", "timestamp_unix": time.time(), "importance": 0.8, "project": "local"},
        ],
    )
    searcher = MemorySearcher(col)

    res = searcher.search("find auth_handler symbol code pattern", n_results=5)
    assert "mem1" in res["executed"]["retrieved_ids"]
    assert res["executed"]["retrieved_ids"][0] == "mem1"


def test_semantic_merger_union():
    col = MockCollection()
    col.set_fixed_distances([0.2])
    col.add(
        ids=["pref_1"],
        documents=["user preference: communication_style = brief_direct | confidence=0.85"],
        metadatas=[{"type": "user_preference", "timestamp_unix": time.time(), "importance": 0.6, "project": "local"}],
    )
    searcher = MemorySearcher(col)

    new_doc = "user preference: communication_style = brief_direct | confidence=0.90"
    resolved = searcher.resolve_conflict(new_doc, meta_type="user_preference")

    assert resolved is True
    assert col._docs[0] is not None
    assert "UPDATED CONTEXT" in col._docs[0]
