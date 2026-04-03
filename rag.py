# rag.py - Vector-Based Semantic Search Engine for AELVO Agentic OS
"""
Phase 5: The Vector Search Engine.
This module replaces the legacy SQL 'LIKE' search with a mathematical
Cosine Similarity engine using ChromaDB. It allows AELVO to perform 
conceptual retrieval across its entire memory history.
"""

import logging
import chromadb
from typing import Dict, List, Any

log = logging.getLogger("aelvo")

class MemorySearcher:
    """A high-performance Vector Search Engine using ChromaDB."""

    def __init__(self, chroma_collection: chromadb.Collection):
        self.collection = chroma_collection

    def resolve_conflict(self, new_doc: str, meta_type: str = "fact") -> bool:
        """Phase 8: Semantic Conflict Resolution. Prevents redundant/contradictory memory bloat."""
        try:
            # Query for extremely similar existing concepts
            results = self.collection.query(
                query_texts=[new_doc],
                n_results=1,
                include=["documents", "metadatas", "distances"]
            )
            
            if not results["ids"] or not results["ids"][0]:
                return False # No existing concept, proceed with insert
            
            dist = results["distances"][0][0]
            similarity = 1.0 - dist
            existing_id = results["ids"][0][0]
            
            # Logic A: Exact Conceptual Duplicate
            if similarity > 0.95:
                log.info(f"✓ Deduplicated atomic memory: {existing_id} (Similarity: {similarity:.3f})")
                return True # Conflict 'resolved' by skipping redundancy
            
            # Logic B: Factual Override (High Conceptual Overlap, but New Instruction)
            if similarity > 0.85 and meta_type == "fact":
                # If we're updating a fact, prune the older, stale version
                log.info(f"⚠ Pruning stale memory record: {existing_id} to make room for updated signal.")
                self.collection.delete(ids=[existing_id])
                
            return False # Proceed with updated insert
        except Exception as e:
            log.error(f"Conflict Resolution Failure: {e}")
            return False

    def search(self, query: str, n_results: int = 5) -> Dict[str, Any]:
        """Weighted Semantic Retrieval: Similarity * Importance * Recency."""
        import time
        NOW = time.time()
        try:
            results = self.collection.query(
                query_texts=[query],
                n_results=n_results,
                include=["documents", "metadatas", "distances"]
            )
            
            if not results["ids"] or not results["ids"][0]:
                return {"status": "success", "logs": "No conceptual matches found.", "executed": {"hit_count": 0}}

            docs, metas, dists, ids = results["documents"][0], results["metadatas"][0], results["distances"][0], results["ids"][0]
            
            formatted_hits = []
            used_ids = []
            
            for doc, meta, dist, mid in zip(docs, metas, dists, ids):
                # 1. Similarity Calculation (1.0 = exact, 0.0 = opposite)
                similarity = max(0, 1.0 - dist)
                
                # 2. Importance Factor (Signal Strength)
                importance = float(meta.get("importance", 0.5))
                
                # 3. Recency Decay (Time-Awareness)
                ts = float(meta.get("timestamp_unix", NOW))
                age_seconds = max(1, NOW - ts)
                # Recency decays over 48h (172800s)
                recency = 1.0 / (1.0 + (age_seconds / 172800))
                
                # FINAL RANKING SCORE
                score = round(similarity * importance * recency, 3)

                # HARD NOISE FILTER (Signal Extraction)
                if score < 0.15: # Suppress low-signal background noise
                    continue

                m_type = meta.get("type", "fact").upper()
                formatted_hits.append(f"[{m_type}] (Score: {score}) {doc}")
                used_ids.append(mid)

            # Context Slice (Limit to 3 hits to maintain reasoning focus)
            final_hits = formatted_hits[:3]
            final_ids = used_ids[:3]
            report = "\n".join(final_hits)
            
            return {
                "status": "success",
                "logs": f"Weighted Vector Hits ({len(final_hits)} hits found):\n\n{report}",
                "executed": {
                    "query": query,
                    "hit_count": len(final_hits),
                    "retrieved_ids": final_ids # For Feedback Loop in kernel
                }
            }
        except Exception as e:
            log.error(f"Weighted Ranker Failure: {e}")
            return {"status": "error", "logs": f"Search Failure: {str(e)}", "executed": {"query": query}}
