# indexer.py - Incremental Indexer
# Layer 6: Maintains graph currency via file watching and incremental updates

import asyncio
import time
import logging
from typing import Dict, List, Set, Optional, Callable, Any
from datetime import datetime
from collections import defaultdict

from repo_intelligence.types import (
    FileScanResult, ParsedFile, FileId, PerformanceMetrics,
    IndexerState, GenerationRecord
)

log = logging.getLogger("aelvo.repo_intelligence.indexer")


class IncrementalIndexer:
    def __init__(self, watch_enabled: bool = False):
        self.watch_enabled = watch_enabled
        self.state = IndexerState()
        self._reindex_callbacks: List[Callable[[str], None]] = []
        self._batch_reindex_callbacks: List[Callable[[List[str]], None]] = []
        self._pending_changes: List[str] = []
        self._batch_lock = asyncio.Lock()
        self._batch_task: Optional[asyncio.Task] = None
        self._stale_files: Set[str] = set()
        self.metrics: List[PerformanceMetrics] = []

    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))

    def compute_file_dependencies(
        self, file_id: str, all_files: Dict[str, ParsedFile]
    ) -> Set[str]:
        pf = all_files.get(file_id)
        if not pf:
            return set()
        deps = set()
        deps.add(file_id)
        for ref in pf.unresolved_references:
            if ref.startswith("__star_import__:"):
                continue
            for other_id, other_pf in all_files.items():
                if other_id == file_id:
                    continue
                for sym in other_pf.symbols:
                    if sym.symbol_name == ref or sym.fully_qualified_name == ref:
                        deps.add(other_id)
        return deps

    def register_generation(
        self, artifact_id: str, dependency_file_ids: Set[str]
    ) -> GenerationRecord:
        record = GenerationRecord(
            artifact_id=artifact_id,
            generation=self.state.generation_counter,
            dependency_file_ids=dependency_file_ids,
        )
        self.state.artifact_generations[artifact_id] = record
        self.state.generation_counter += 1
        return record

    def invalidate_for_file(self, file_id: str) -> Set[str]:
        invalidated = set()
        for artifact_id, record in self.state.artifact_generations.items():
            if file_id in record.dependency_file_ids:
                self.state.invalidated_artifacts.add(artifact_id)
                invalidated.add(artifact_id)
        self._stale_files.add(file_id)
        return invalidated

    def invalidate_for_files(self, file_ids: Set[str]) -> Set[str]:
        all_invalidated = set()
        for fid in file_ids:
            all_invalidated |= self.invalidate_for_file(fid)
        return all_invalidated

    def is_stale(self, artifact_id: str) -> bool:
        return artifact_id in self.state.invalidated_artifacts

    def mark_rebuilt(self, artifact_id: str) -> None:
        self.state.invalidated_artifacts.discard(artifact_id)
        record = self.state.artifact_generations.get(artifact_id)
        if record:
            record.generation = self.state.generation_counter
            self.state.generation_counter += 1

    def mark_file_fresh(self, file_id: str) -> None:
        self._stale_files.discard(file_id)

    def get_stale_files(self) -> Set[str]:
        return self._stale_files.copy()

    def on_reindex(self, callback: Callable[[str], None]) -> None:
        self._reindex_callbacks.append(callback)

    def on_batch_reindex(self, callback: Callable[[List[str]], None]) -> None:
        self._batch_reindex_callbacks.append(callback)

    def _notify_reindex(self, file_id: str) -> None:
        for cb in self._reindex_callbacks:
            try:
                cb(file_id)
            except Exception as e:
                log.error(f"Reindex callback error: {e}")

    def _notify_batch_reindex(self, file_ids: List[str]) -> None:
        for cb in self._batch_reindex_callbacks:
            try:
                cb(file_ids)
            except Exception as e:
                log.error(f"Batch reindex callback error: {e}")

    def process_scan_result(self, result: FileScanResult) -> Dict[str, Set[str]]:
        start = time.time()
        invalidation_map: Dict[str, Set[str]] = {}
        all_changed = set()
        for path in result.new_files:
            fid = FileId.create(path)
            invalidation_map[path] = self.invalidate_for_file(fid)
            all_changed.add(fid)
        for path in result.changed_files:
            fid = FileId.create(path)
            invalidation_map[path] = self.invalidate_for_file(fid)
            all_changed.add(fid)
        for path in result.deleted_files:
            fid = FileId.create(path)
            invalidation_map[path] = self.invalidate_for_file(fid)
            all_changed.add(fid)
        elapsed = (time.time() - start) * 1000
        self._record_metric("process_scan_result", elapsed)
        log.info(f"Processed scan result: {len(all_changed)} changed files, "
                 f"{sum(len(v) for v in invalidation_map.values())} artifacts invalidated ({elapsed:.0f}ms)")
        return invalidation_map

    def process_file_system_event(self, file_path: str) -> None:
        if not self.watch_enabled:
            return
        path = file_path.replace('\\', '/')
        self._pending_changes.append(path)
        if self._batch_task is None or self._batch_task.done():
            self._batch_task = asyncio.create_task(self._process_batch())

    async def _process_batch(self) -> None:
        async with self._batch_lock:
            await asyncio.sleep(0.5)
            batch = list(self._pending_changes)
            self._pending_changes.clear()
            if batch:
                start = time.time()
                fids = set()
                for path in batch:
                    fid = FileId.create(path)
                    fids.add(fid)
                    self._stale_files.add(path)
                    self.invalidate_for_file(fid)
                self._notify_batch_reindex(batch)
                elapsed = (time.time() - start) * 1000
                self._record_metric("process_batch", elapsed)
                log.debug(f"Processed batch of {len(batch)} file changes ({elapsed:.0f}ms)")

    def get_metrics(self) -> List[PerformanceMetrics]:
        return self.metrics.copy()
