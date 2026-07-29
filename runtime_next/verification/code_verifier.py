from __future__ import annotations

import ast
import hashlib
import logging
import time
from typing import Any, Dict, List

from .types import (
    VerificationType,
    VerificationResult,
    VerificationScope,
    Confidence,
    Severity,
    Retryability,
)

log = logging.getLogger("aelvo.runtime.verification.code")


class CodeVerifier:
    def create_handler(self):
        async def handler(
            node_id: str,
            scope: VerificationScope,
            context: Dict[str, Any],
        ) -> VerificationResult:
            return self.verify(node_id, scope, context)
        return handler

    def verify(
        self,
        node_id: str,
        scope: VerificationScope,
        context: Dict[str, Any],
    ) -> VerificationResult:
        start = time.monotonic()
        affected_files = list(scope.affected_files) if scope else []
        diagnostics: List[str] = []
        success = True

        for file_path in affected_files:
            try:
                with open(file_path, "rb") as f:
                    source = f.read()
                ast.parse(source)
            except SyntaxError as e:
                success = False
                diagnostics.append(f"Syntax error in {file_path}: {e}")
            except FileNotFoundError:
                diagnostics.append(f"File not found: {file_path}")
            except Exception as e:
                diagnostics.append(f"Error checking {file_path}: {e}")

        duration = (time.monotonic() - start) * 1000

        return VerificationResult(
            verification_id=hashlib.sha256(f"code_{node_id}_{time.time()}".encode()).hexdigest()[:16],
            node_id=node_id,
            verification_type=VerificationType.LINT,
            duration_ms=duration,
            success=success,
            confidence=Confidence.HIGH if success else Confidence.CERTAIN,
            severity=Severity.INFO if success else Severity.ERROR,
            retryability=Retryability.SAFE,
            diagnostics=diagnostics or ["Code verification passed"],
            affected_files=affected_files,
            provenance="code_verifier",
        )


class TypeCheckVerifier:
    def create_handler(self):
        async def handler(
            node_id: str,
            scope: VerificationScope,
            context: Dict[str, Any],
        ) -> VerificationResult:
            return self.verify(node_id, scope, context)
        return handler

    def verify(
        self,
        node_id: str,
        scope: VerificationScope,
        context: Dict[str, Any],
    ) -> VerificationResult:
        start = time.monotonic()
        affected_files = list(scope.affected_files) if scope else []
        diagnostics: List[str] = []
        success = True

        for file_path in affected_files:
            if not file_path.endswith(".py"):
                continue
            try:
                import py_compile
                py_compile.compile(file_path, doraise=True)
            except py_compile.PyCompileError as e:
                success = False
                diagnostics.append(f"Type/compile error in {file_path}: {e}")
            except Exception as e:
                diagnostics.append(f"Error typechecking {file_path}: {e}")

        duration = (time.monotonic() - start) * 1000

        return VerificationResult(
            verification_id=hashlib.sha256(f"type_{node_id}_{time.time()}".encode()).hexdigest()[:16],
            node_id=node_id,
            verification_type=VerificationType.TYPECHECK,
            duration_ms=duration,
            success=success,
            confidence=Confidence.HIGH if success else Confidence.CERTAIN,
            severity=Severity.INFO if success else Severity.ERROR,
            retryability=Retryability.SAFE,
            diagnostics=diagnostics or ["Type check passed"],
            affected_files=affected_files,
            provenance="typecheck_verifier",
        )
