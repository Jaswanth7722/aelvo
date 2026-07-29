# scanner.py - File Scanner for Repository Intelligence Engine
# Layer 1: Discovers files, detects languages, fingerprints for incremental updates

import asyncio
import os
import hashlib
import time
from pathlib import Path
from typing import List, Set, Optional, Dict
from concurrent.futures import ThreadPoolExecutor
import logging

from repo_intelligence.types import (
    LanguageId, FileScanResult, FileId, ParsedFile, PerformanceMetrics
)

log = logging.getLogger("aelvo.repo_intelligence.scanner")


class FileScanner:
    def __init__(
        self,
        workspace_root: str,
        exclusions: Optional[Set[str]] = None,
        custom_extensions: Optional[Dict[str, LanguageId]] = None,
        max_file_size_mb: int = 10,
        max_workers: int = 4,
        follow_symlinks: bool = False,
    ):
        self.workspace_root = Path(workspace_root).resolve()
        self.exclusions = self.DEFAULT_EXCLUSIONS | (exclusions or set())
        self.extension_map = {**self.EXTENSION_MAP, **(custom_extensions or {})}
        self.max_file_size = max_file_size_mb * 1024 * 1024
        self.follow_symlinks = follow_symlinks
        self.fingerprints: Dict[str, str] = {}
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.metrics: List[PerformanceMetrics] = []

    DEFAULT_EXCLUSIONS = {
        ".git", "__pycache__", "node_modules", ".venv", "venv",
        "target", "dist", "build", ".next", ".mypy_cache", ".pytest_cache",
        ".ruff_cache", "chroma_db", "backups", "workspace",
        ".tox", ".eggs", "eggs", ".nox", ".svn", ".hg",
        ".DS_Store", "Thumbs.db",
    }

    EXTENSION_MAP: Dict[str, LanguageId] = {
        ".py": LanguageId.PYTHON,
        ".pyx": LanguageId.PYTHON,
        ".pyi": LanguageId.PYTHON,
        ".ts": LanguageId.TYPESCRIPT,
        ".tsx": LanguageId.TYPESCRIPT,
        ".js": LanguageId.JAVASCRIPT,
        ".jsx": LanguageId.JAVASCRIPT,
        ".mjs": LanguageId.JAVASCRIPT,
        ".cjs": LanguageId.JAVASCRIPT,
        ".rs": LanguageId.RUST,
        ".go": LanguageId.GO,
        ".java": LanguageId.JAVA,
        ".c": LanguageId.C,
        ".h": LanguageId.C,
        ".cpp": LanguageId.CPP,
        ".cc": LanguageId.CPP,
        ".cxx": LanguageId.CPP,
        ".hpp": LanguageId.CPP,
        ".cs": LanguageId.CSHARP,
        ".swift": LanguageId.SWIFT,
        ".kt": LanguageId.KOTLIN,
        ".kts": LanguageId.KOTLIN,
        ".rb": LanguageId.RUBY,
        ".php": LanguageId.PHP,
    }

    SHEBANG_MAP: Dict[str, LanguageId] = {
        "python": LanguageId.PYTHON,
        "python3": LanguageId.PYTHON,
        "python2": LanguageId.PYTHON,
        "node": LanguageId.JAVASCRIPT,
        "bash": LanguageId.UNKNOWN,
        "sh": LanguageId.UNKNOWN,
    }

    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))

    def _is_excluded(self, path: Path) -> bool:
        for part in path.parts:
            if part in self.exclusions:
                return True
        if path.name.startswith(".") and path.name not in {".env", ".gitignore"}:
            return True
        return False

    def _detect_language_from_extension(self, file_path: Path) -> Optional[LanguageId]:
        suffix = file_path.suffix.lower()
        for ext, lang in self.extension_map.items():
            if file_path.name.endswith(ext):
                return lang
        for ext, lang in self.EXTENSION_MAP.items():
            if file_path.name.endswith(ext):
                return lang
        return None

    def _detect_language_from_content(self, file_path: Path) -> Optional[LanguageId]:
        try:
            with open(file_path, 'rb') as f:
                first_line = f.readline(100).decode('utf-8', errors='ignore')
                if first_line.startswith("#!"):
                    parts = first_line[2:].strip().split()
                    if not parts:
                        return None
                    interpreter = Path(parts[0]).name
                    lang = self.SHEBANG_MAP.get(interpreter)
                    if lang:
                        return lang
                    for part in parts[1:]:
                        lang = self.SHEBANG_MAP.get(part)
                        if lang:
                            return lang
        except (IOError, UnicodeDecodeError):
            pass
        return None

    def detect_language(self, file_path: Path) -> LanguageId:
        lang = self._detect_language_from_extension(file_path)
        if lang:
            return lang
        lang = self._detect_language_from_content(file_path)
        if lang:
            return lang
        return LanguageId.UNKNOWN

    def _compute_fingerprint(self, file_path: Path) -> str:
        try:
            sha256 = hashlib.sha256()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(65536), b''):
                    sha256.update(chunk)
            return sha256.hexdigest()
        except (IOError, OSError):
            return ""

    def _is_source_file(self, file_path: Path) -> bool:
        if self._detect_language_from_extension(file_path) is None:
            if self._detect_language_from_content(file_path) is None:
                return False
        try:
            return file_path.stat().st_size <= self.max_file_size
        except OSError:
            return False

    async def scan_file(self, file_path: Path) -> Optional[ParsedFile]:
        if self._is_excluded(file_path) or not self._is_source_file(file_path):
            return None
        try:
            start = time.time()
            loop = asyncio.get_event_loop()
            fingerprint = await loop.run_in_executor(
                self.executor, self._compute_fingerprint, file_path
            )
            if not fingerprint:
                return None
            language = await loop.run_in_executor(
                self.executor, self.detect_language, file_path
            )
            try:
                relative_path = file_path.relative_to(self.workspace_root)
            except ValueError:
                relative_path = file_path
            file_id = FileId.create(str(relative_path))
            file_size = file_path.stat().st_size
            parsed_file = ParsedFile(
                file_id=file_id,
                file_path=str(relative_path),
                language=language,
                fingerprint=fingerprint,
                parse_success=True,
                size_bytes=file_size,
            )
            self.fingerprints[str(relative_path)] = fingerprint
            elapsed = (time.time() - start) * 1000
            self._record_metric("scan_file", elapsed)
            return parsed_file
        except Exception as e:
            log.debug(f"Error scanning file {file_path}: {e}")
            return None

    async def scan_directory(self, directory: Optional[Path] = None) -> List[ParsedFile]:
        start = time.time()
        target_dir = directory or self.workspace_root
        if not target_dir.exists():
            log.warning(f"Directory does not exist: {target_dir}")
            return []
        all_files = []
        try:
            for root, dirs, files in os.walk(target_dir, followlinks=self.follow_symlinks):
                dirs[:] = [d for d in dirs if d not in self.exclusions]
                root_path = Path(root)
                for filename in files:
                    file_path = root_path / filename
                    if not self._is_excluded(file_path):
                        all_files.append(file_path)
        except OSError as e:
            log.error(f"Error walking directory {target_dir}: {e}")
            return []
        sem = asyncio.Semaphore(10)
        async def bounded_scan(fp):
            async with sem:
                return await self.scan_file(fp)
        tasks = [bounded_scan(file_path) for file_path in all_files]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        parsed_files = []
        for result in results:
            if isinstance(result, Exception):
                log.debug(f"Error during file scan: {result}")
            elif result is not None:
                parsed_files.append(result)
        elapsed = (time.time() - start) * 1000
        self._record_metric("scan_directory", elapsed)
        log.info(f"Scanned {len(parsed_files)} source files in {target_dir} ({elapsed:.0f}ms)")
        return parsed_files

    async def scan_incremental(self, previous_fingerprints: Dict[str, str]) -> FileScanResult:
        start = time.time()
        current_files = await self.scan_directory()
        current_paths = {pf.file_path for pf in current_files}
        previous_paths = set(previous_fingerprints.keys())
        new_files = []
        changed_files = []
        unchanged_files = []
        deleted_files = []
        for parsed_file in current_files:
            path = parsed_file.file_path
            old_fp = previous_fingerprints.get(path)
            if old_fp is None:
                new_files.append(path)
            elif old_fp != parsed_file.fingerprint:
                changed_files.append(path)
            else:
                unchanged_files.append(path)
        deleted_files = list(previous_paths - current_paths)
        result = FileScanResult(
            new_files=new_files,
            changed_files=changed_files,
            unchanged_files=unchanged_files,
            deleted_files=deleted_files,
        )
        elapsed = (time.time() - start) * 1000
        self._record_metric("scan_incremental", elapsed)
        if result.has_changes:
            log.info(f"Incremental scan: {len(new_files)} new, {len(changed_files)} changed, {len(deleted_files)} deleted ({elapsed:.0f}ms)")
        else:
            log.info(f"Incremental scan: no changes ({elapsed:.0f}ms)")
        return result

    def get_fingerprints(self) -> Dict[str, str]:
        return self.fingerprints.copy()

    def update_fingerprints(self, new_fingerprints: Dict[str, str]) -> None:
        self.fingerprints.update(new_fingerprints)

    def get_metrics(self) -> List[PerformanceMetrics]:
        return self.metrics.copy()

    def close(self) -> None:
        self.executor.shutdown(wait=True)
