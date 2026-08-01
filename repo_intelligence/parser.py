# parser.py - AST Parser Layer for Repository Intelligence Engine
# Layer 2: Parses source files and extracts symbols and relationships

import ast
import re
import asyncio
import time
from pathlib import Path
from typing import List, Optional, Dict, Tuple, Any
from concurrent.futures import ThreadPoolExecutor
import logging

from repo_intelligence.types import (
    LanguageId, SymbolId, FileId, SymbolNode, SymbolKind,
    ConfidenceLevel, ParsedFile, SymbolEdge, EdgeType, ArgumentInfo, PerformanceMetrics
)

log = logging.getLogger("aelvo.repo_intelligence.parser")


class PythonASTParser(ast.NodeVisitor):
    def __init__(self, file_path: str, file_id: str):
        self.file_path = file_path
        self.file_id = file_id
        self.symbols: List[SymbolNode] = []
        self.unresolved_references: List[str] = []
        self.edges: List[SymbolEdge] = []
        self.scope_stack: List[Optional[str]] = [None]
        self.imports: Dict[str, str] = {}
        self.from_imports: Dict[str, Tuple[str, str]] = {}
        self._current_class_bases: List[str] = []

    def _get_line_range(self, node: ast.AST) -> Tuple[int, int]:
        start = getattr(node, 'lineno', 0)
        end = getattr(node, 'end_lineno', start)
        return (start, end or start)

    def _get_docstring(self, node: ast.AST) -> Optional[str]:
        return ast.get_docstring(node)

    def _get_decorators(self, node: ast.AST) -> List[str]:
        decorators = []
        for decorator in getattr(node, 'decorator_list', []):
            if isinstance(decorator, ast.Name):
                decorators.append(decorator.id)
                self.unresolved_references.append(decorator.id)
            elif isinstance(decorator, ast.Call):
                if isinstance(decorator.func, ast.Name):
                    decorators.append(decorator.func.id)
                    self.unresolved_references.append(decorator.func.id)
                elif isinstance(decorator.func, ast.Attribute):
                    try:
                        decorators.append(ast.unparse(decorator.func))
                        self.unresolved_references.append(ast.unparse(decorator.func))
                    except Exception as _ex: log.warning("Silenced exception: %s", _ex)
        return decorators

    def _get_type_annotation_str(self, node: Optional[ast.AST]) -> Optional[str]:
        if node is None:
            return None
        try:
            return ast.unparse(node)
        except Exception:
            return str(node)

    def _generate_fully_qualified_name(self, name: str) -> str:
        parts = []
        for parent in self.scope_stack:
            if parent:
                sym = next((s for s in self.symbols if s.symbol_id == parent), None)
                if sym:
                    parts.append(sym.symbol_name)
        parts.append(name)
        return ".".join(parts)

    def _get_arguments(self, node: ast.FunctionDef) -> List[ArgumentInfo]:
        args = []
        for arg in node.args.args:
            arg_info = ArgumentInfo(
                name=arg.arg,
                type_annotation=self._get_type_annotation_str(arg.annotation),
            )
            args.append(arg_info)
        for i, default in enumerate(node.args.defaults):
            idx = len(node.args.args) - len(node.args.defaults) + i
            if 0 <= idx < len(args):
                args[idx].has_default = True
        return args

    def visit_Module(self, node: ast.Module):
        docstring = self._get_docstring(node)
        if docstring:
            module_sym = SymbolNode(
                symbol_id=SymbolId.create(self.file_path, "module"),
                file_id=self.file_id,
                file_path=self.file_path,
                line_range=(1, getattr(node, 'end_lineno', 1) or 1),
                symbol_kind=SymbolKind.MODULE,
                symbol_name=Path(self.file_path).stem,
                fully_qualified_name=Path(self.file_path).stem,
                parent_symbol_id=None,
                docstring=docstring,
                confidence=ConfidenceLevel.CERTAIN,
            )
            self.symbols.append(module_sym)
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import):
        for alias in node.names:
            imported_module = alias.name
            imported_as = alias.asname if alias.asname else imported_module
            self.imports[imported_as] = imported_module
            line_range = self._get_line_range(node)
            import_sym = SymbolNode(
                symbol_id=SymbolId.create(self.file_path, f"import.{imported_as}"),
                file_id=self.file_id,
                file_path=self.file_path,
                line_range=line_range,
                symbol_kind=SymbolKind.IMPORT,
                symbol_name=imported_as,
                fully_qualified_name=f"import.{imported_as}",
                parent_symbol_id=None,
                confidence=ConfidenceLevel.CERTAIN,
            )
            self.symbols.append(import_sym)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom):
        module = node.module if node.module else ''
        for alias in node.names:
            imported_name = alias.name
            imported_as = alias.asname if alias.asname else imported_name
            self.from_imports[imported_as] = (module, imported_name)
            line_range = self._get_line_range(node)
            import_sym = SymbolNode(
                symbol_id=SymbolId.create(self.file_path, f"from.{module}.{imported_as}"),
                file_id=self.file_id,
                file_path=self.file_path,
                line_range=line_range,
                symbol_kind=SymbolKind.IMPORT,
                symbol_name=imported_as,
                fully_qualified_name=f"{module}.{imported_as}" if module else imported_name,
                parent_symbol_id=None,
                confidence=ConfidenceLevel.CERTAIN,
            )
            self.symbols.append(import_sym)
            if imported_name == '*':
                self.unresolved_references.append(f"__star_import__:{module}")
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef):
        line_range = self._get_line_range(node)
        docstring = self._get_docstring(node)
        decorators = self._get_decorators(node)
        base_classes = []
        implemented = []
        for base in node.bases:
            if isinstance(base, ast.Name):
                base_classes.append(base.id)
                self.unresolved_references.append(base.id)
            elif isinstance(base, ast.Attribute):
                try:
                    name = ast.unparse(base)
                    base_classes.append(name)
                    self.unresolved_references.append(name)
                except Exception as _ex: log.warning("Silenced exception: %s", _ex)
        for keyword in getattr(node, 'keywords', []):
            if keyword.arg == 'metaclass':
                if isinstance(keyword.value, ast.Name):
                    self.unresolved_references.append(keyword.value.id)
        fq_name = self._generate_fully_qualified_name(node.name)
        class_id = SymbolId.create(self.file_path, fq_name)
        parent_sym = None
        if self.scope_stack[-1]:
            parent_sym = self.scope_stack[-1]
        class_sym = SymbolNode(
            symbol_id=class_id,
            file_id=self.file_id,
            file_path=self.file_path,
            line_range=line_range,
            symbol_kind=SymbolKind.CLASS,
            symbol_name=node.name,
            fully_qualified_name=fq_name,
            parent_symbol_id=parent_sym,
            docstring=docstring,
            decorators=decorators,
            is_exported=not node.name.startswith('_'),
            confidence=ConfidenceLevel.CERTAIN,
            base_class_names=base_classes,
            implemented_interfaces=implemented,
        )
        self.symbols.append(class_sym)
        for base_name in base_classes:
            self.edges.append(SymbolEdge(
                source_id=class_id,
                target_id=FileId.create(base_name),
                edge_type=EdgeType.INHERITS,
                file_path=self.file_path,
                line_number=line_range[0],
                confidence=ConfidenceLevel.CERTAIN,
            ))
        old_bases = self._current_class_bases
        self._current_class_bases = base_classes
        self.scope_stack.append(class_id)
        self.generic_visit(node)
        self.scope_stack.pop()
        self._current_class_bases = old_bases

    def visit_FunctionDef(self, node: ast.FunctionDef):
        self._process_function(node, is_async=False)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
        self._process_function(node, is_async=True)

    def _process_function(self, node: Any, is_async: bool):
        line_range = self._get_line_range(node)
        docstring = self._get_docstring(node)
        decorators = self._get_decorators(node)
        return_annotation = self._get_type_annotation_str(getattr(node, 'returns', None))
        fq_name = self._generate_fully_qualified_name(node.name)
        func_id = SymbolId.create(self.file_path, fq_name)
        args = self._get_arguments(node)
        parent = self.scope_stack[-1]
        is_method = parent is not None and any(
            s.symbol_kind == SymbolKind.CLASS and s.symbol_id == parent
            for s in self.symbols
        )
        kind = SymbolKind.METHOD if is_method else SymbolKind.FUNCTION
        func_sym = SymbolNode(
            symbol_id=func_id,
            file_id=self.file_id,
            file_path=self.file_path,
            line_range=line_range,
            symbol_kind=kind,
            symbol_name=node.name,
            fully_qualified_name=fq_name,
            parent_symbol_id=parent,
            docstring=docstring,
            decorators=decorators,
            type_annotation=return_annotation,
            is_exported=not node.name.startswith('_'),
            confidence=ConfidenceLevel.CERTAIN,
            is_async=is_async,
            arguments=args,
        )
        self.symbols.append(func_sym)
        self.scope_stack.append(func_id)
        self.generic_visit(node)
        self.scope_stack.pop()

    def visit_Assign(self, node: ast.Assign):
        depth = sum(1 for p in self.scope_stack if p is not None)
        if depth > 1:
            self.generic_visit(node)
            return
        line_range = self._get_line_range(node)
        for target in node.targets:
            if isinstance(target, ast.Name):
                fq_name = self._generate_fully_qualified_name(target.id)
                var_id = SymbolId.create(self.file_path, fq_name)
                type_ann = self._get_type_annotation_str(
                    getattr(target, 'annotation', None)
                )
            if not type_ann:
                type_comment = getattr(node, 'type_comment', None)
                if type_comment:
                    type_ann = type_comment
                var_sym = SymbolNode(
                    symbol_id=var_id,
                    file_id=self.file_id,
                    file_path=self.file_path,
                    line_range=line_range,
                    symbol_kind=SymbolKind.VARIABLE,
                    symbol_name=target.id,
                    fully_qualified_name=fq_name,
                    parent_symbol_id=self.scope_stack[-1],
                    type_annotation=type_ann,
                    is_exported=not target.id.startswith('_'),
                    confidence=ConfidenceLevel.CERTAIN,
                )
                self.symbols.append(var_sym)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call):
        line_range = self._get_line_range(node)
        caller_id = self.scope_stack[-1]
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
            self.unresolved_references.append(func_name)
            if caller_id:
                self.edges.append(SymbolEdge(
                    source_id=caller_id,
                    target_id=SymbolId.create(self.file_path, func_name),
                    edge_type=EdgeType.CALLS,
                    file_path=self.file_path,
                    line_number=line_range[0],
                    confidence=ConfidenceLevel.APPROXIMATE,
                ))
        elif isinstance(node.func, ast.Attribute):
            try:
                attr_name = ast.unparse(node.func)
                self.unresolved_references.append(attr_name)
                if caller_id:
                    self.edges.append(SymbolEdge(
                        source_id=caller_id,
                        target_id=SymbolId.create(self.file_path, attr_name),
                        edge_type=EdgeType.CALLS,
                        file_path=self.file_path,
                        line_number=line_range[0],
                        confidence=ConfidenceLevel.APPROXIMATE,
                    ))
                inner = node.func.value
                if isinstance(inner, ast.Name):
                    self.unresolved_references.append(inner.id)
            except Exception as _ex: log.warning("Silenced exception: %s", _ex)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign):
        depth = sum(1 for p in self.scope_stack if p is not None)
        if depth > 1:
            self.generic_visit(node)
            return
        if isinstance(node.target, ast.Name):
            line_range = self._get_line_range(node)
            fq_name = self._generate_fully_qualified_name(node.target.id)
            var_id = SymbolId.create(self.file_path, fq_name)
            type_ann = self._get_type_annotation_str(node.annotation)
            var_sym = SymbolNode(
                symbol_id=var_id,
                file_id=self.file_id,
                file_path=self.file_path,
                line_range=line_range,
                symbol_kind=SymbolKind.VARIABLE,
                symbol_name=node.target.id,
                fully_qualified_name=fq_name,
                parent_symbol_id=self.scope_stack[-1],
                type_annotation=type_ann,
                is_exported=not node.target.id.startswith('_'),
                confidence=ConfidenceLevel.CERTAIN,
            )
            self.symbols.append(var_sym)
        self.generic_visit(node)


class TypeScriptRegexParser:
    PATTERNS = {
        'class': re.compile(
            r'(?:export\s+)?(?:abstract\s+)?class\s+(\w+)(?:\s+extends\s+(\w+))?(?:\s+implements\s+([\w\s,]+))?',
            re.MULTILINE
        ),
        'function': re.compile(
            r'(?:export\s+)?(?:async\s+)?function\s+(\w+)\s*\([^)]*\)',
            re.MULTILINE
        ),
        'arrow_function': re.compile(
            r'(?:export\s+)?(?:const|let|var)\s+(\w+)\s*=\s*(?:async\s+)?\([^)]*\)\s*=>',
            re.MULTILINE
        ),
        'interface': re.compile(
            r'(?:export\s+)?interface\s+(\w+)(?:\s+extends\s+(\w+))?',
            re.MULTILINE
        ),
        'type_alias': re.compile(
            r'(?:export\s+)?type\s+(\w+)\s*=',
            re.MULTILINE
        ),
        'import_default': re.compile(
            r'import\s+(\w+)\s+from\s+[\'"]([^\'"]+)[\'"]',
            re.MULTILINE
        ),
        'import_named': re.compile(
            r'import\s+\{([^}]+)\}\s+from\s+[\'"]([^\'"]+)[\'"]',
            re.MULTILINE
        ),
        'import_namespace': re.compile(
            r'import\s+\*\s+as\s+(\w+)\s+from\s+[\'"]([^\'"]+)[\'"]',
            re.MULTILINE
        ),
        'export': re.compile(
            r'export\s+(?:default\s+)?(?:const|let|var|function|class|interface|type)\s+(\w+)',
            re.MULTILINE
        ),
    }

    def __init__(self, file_path: str, file_id: str):
        self.file_path = file_path
        self.file_id = file_id
        self.symbols: List[SymbolNode] = []
        self.unresolved_references: List[str] = []

    def parse(self, content: str) -> None:
        for match in self.PATTERNS['class'].finditer(content):
            class_name = match.group(1)
            base_class = match.group(2)
            line_num = content[:match.start()].count('\n') + 1
            bases = [base_class] if base_class else []
            sym = SymbolNode(
                symbol_id=SymbolId.create(self.file_path, f"class.{class_name}"),
                file_id=self.file_id,
                file_path=self.file_path,
                line_range=(line_num, line_num),
                symbol_kind=SymbolKind.CLASS,
                symbol_name=class_name,
                fully_qualified_name=class_name,
                parent_symbol_id=None,
                confidence=ConfidenceLevel.INFERRED,
                base_class_names=bases,
            )
            self.symbols.append(sym)
            if base_class:
                self.unresolved_references.append(base_class)

        for match in self.PATTERNS['function'].finditer(content):
            func_name = match.group(1)
            line_num = content[:match.start()].count('\n') + 1
            sym = SymbolNode(
                symbol_id=SymbolId.create(self.file_path, f"function.{func_name}"),
                file_id=self.file_id,
                file_path=self.file_path,
                line_range=(line_num, line_num),
                symbol_kind=SymbolKind.FUNCTION,
                symbol_name=func_name,
                fully_qualified_name=func_name,
                parent_symbol_id=None,
                confidence=ConfidenceLevel.INFERRED,
            )
            self.symbols.append(sym)

        for match in self.PATTERNS['arrow_function'].finditer(content):
            func_name = match.group(1)
            line_num = content[:match.start()].count('\n') + 1
            sym = SymbolNode(
                symbol_id=SymbolId.create(self.file_path, f"arrow.{func_name}"),
                file_id=self.file_id,
                file_path=self.file_path,
                line_range=(line_num, line_num),
                symbol_kind=SymbolKind.FUNCTION,
                symbol_name=func_name,
                fully_qualified_name=func_name,
                parent_symbol_id=None,
                confidence=ConfidenceLevel.INFERRED,
            )
            self.symbols.append(sym)

        for match in self.PATTERNS['interface'].finditer(content):
            iface_name = match.group(1)
            line_num = content[:match.start()].count('\n') + 1
            sym = SymbolNode(
                symbol_id=SymbolId.create(self.file_path, f"interface.{iface_name}"),
                file_id=self.file_id,
                file_path=self.file_path,
                line_range=(line_num, line_num),
                symbol_kind=SymbolKind.INTERFACE,
                symbol_name=iface_name,
                fully_qualified_name=iface_name,
                parent_symbol_id=None,
                confidence=ConfidenceLevel.INFERRED,
            )
            self.symbols.append(sym)

        for match in self.PATTERNS['type_alias'].finditer(content):
            type_name = match.group(1)
            line_num = content[:match.start()].count('\n') + 1
            sym = SymbolNode(
                symbol_id=SymbolId.create(self.file_path, f"type.{type_name}"),
                file_id=self.file_id,
                file_path=self.file_path,
                line_range=(line_num, line_num),
                symbol_kind=SymbolKind.TYPE_ALIAS,
                symbol_name=type_name,
                fully_qualified_name=type_name,
                parent_symbol_id=None,
                confidence=ConfidenceLevel.INFERRED,
            )
            self.symbols.append(sym)

        for match in self.PATTERNS['import_named'].finditer(content):
            imports = match.group(1)
            module = match.group(2)
            for imp in imports.split(','):
                imp = imp.strip()
                self.unresolved_references.append(f"{module}.{imp}")

        for match in self.PATTERNS['import_default'].finditer(content):
            name = match.group(1)
            module = match.group(2)
            self.unresolved_references.append(f"{module}.{name}")

        for match in self.PATTERNS['import_namespace'].finditer(content):
            name = match.group(1)
            module = match.group(2)
            self.unresolved_references.append(f"{module} namespace {name}")


class ASTParser:
    def __init__(self, max_workers: int = 4):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.metrics: List[PerformanceMetrics] = []

    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))

    async def parse_file(self, parsed_file: ParsedFile) -> ParsedFile:
        start = time.time()
        try:
            loop = asyncio.get_event_loop()
            file_path = Path(parsed_file.file_path)
            if not file_path.is_absolute():
                file_path = Path.cwd() / parsed_file.file_path
            if not file_path.exists():
                parsed_file.parse_success = False
                parsed_file.parse_error = "File not found"
                return parsed_file
            content = await loop.run_in_executor(
                self.executor, file_path.read_text, 'utf-8'
            )
            if parsed_file.language == LanguageId.PYTHON:
                parsed_file = await self._parse_python(parsed_file, content)
            elif parsed_file.language in (LanguageId.TYPESCRIPT, LanguageId.JAVASCRIPT):
                parsed_file = await self._parse_typescript(parsed_file, content)
            elif parsed_file.language == LanguageId.RUST:
                parsed_file = await self._parse_rust(parsed_file, content)
            elif parsed_file.language == LanguageId.GO:
                parsed_file = await self._parse_go(parsed_file, content)
            else:
                parsed_file.parse_success = True
                parsed_file.parse_error = f"Language {parsed_file.language} not fully supported"
            elapsed = (time.time() - start) * 1000
            self._record_metric(f"parse.{parsed_file.language.value}", elapsed)
            return parsed_file
        except SyntaxError as e:
            parsed_file.parse_success = False
            parsed_file.parse_error = f"Syntax error: {str(e)}"
            return parsed_file
        except Exception as e:
            parsed_file.parse_success = False
            parsed_file.parse_error = f"Parse error: {str(e)}"
            log.debug(f"Error parsing {parsed_file.file_path}: {e}")
            return parsed_file

    async def _parse_python(self, parsed_file: ParsedFile, content: str) -> ParsedFile:
        try:
            loop = asyncio.get_event_loop()

            def parse_content():
                tree = ast.parse(content)
                visitor = PythonASTParser(parsed_file.file_path, parsed_file.file_id)
                visitor.visit(tree)
                return visitor.symbols, visitor.unresolved_references, visitor.edges

            symbols, unresolved, edges = await loop.run_in_executor(self.executor, parse_content)
            parsed_file.symbols = symbols
            parsed_file.unresolved_references = unresolved
            parsed_file.parse_success = True
            return parsed_file
        except SyntaxError as e:
            try:
                tree = ast.parse(content, mode='exec')
                visitor = PythonASTParser(parsed_file.file_path, parsed_file.file_id)
                visitor.visit(tree)
                parsed_file.symbols = visitor.symbols
                parsed_file.unresolved_references = visitor.unresolved_references
            except Exception as _ex: log.warning("Silenced exception: %s", _ex)
            parsed_file.parse_success = False
            parsed_file.parse_error = f"Syntax error at line {e.lineno}: {str(e)}"
            return parsed_file

    async def _parse_typescript(self, parsed_file: ParsedFile, content: str) -> ParsedFile:
        try:
            loop = asyncio.get_event_loop()

            def parse_content():
                parser = TypeScriptRegexParser(parsed_file.file_path, parsed_file.file_id)
                parser.parse(content)
                return parser.symbols, parser.unresolved_references

            symbols, unresolved = await loop.run_in_executor(self.executor, parse_content)
            parsed_file.symbols = symbols
            parsed_file.unresolved_references = unresolved
            parsed_file.parse_success = True
            parsed_file.parse_error = "Regex-based parsing - limited accuracy"
            return parsed_file
        except Exception as e:
            parsed_file.parse_success = False
            parsed_file.parse_error = f"TypeScript parse error: {str(e)}"
            return parsed_file

    async def _parse_rust(self, parsed_file: ParsedFile, content: str) -> ParsedFile:
        patterns = {
            'fn': re.compile(r'(?:pub\s+)?(?:unsafe\s+)?fn\s+(\w+)\s*\([^)]*\)'),
            'struct': re.compile(r'(?:pub\s+)?struct\s+(\w+)(?:\s*\{[^}]*\})?'),
            'enum': re.compile(r'(?:pub\s+)?enum\s+(\w+)(?:\s*\{[^}]*\})?'),
            'trait': re.compile(r'(?:pub\s+)?(?:unsafe\s+)?trait\s+(\w+)'),
            'impl': re.compile(r'impl\s+(\w+)(?:\s+for\s+(\w+))?'),
        }
        symbols = []
        unresolved = []
        try:
            loop = asyncio.get_event_loop()

            def parse_content():
                results = []
                for kind, pattern in patterns.items():
                    for match in pattern.finditer(content):
                        name = match.group(1)
                        line_num = content[:match.start()].count('\n') + 1
                        kind_map = {
                            'fn': SymbolKind.FUNCTION,
                            'struct': SymbolKind.STRUCT,
                            'enum': SymbolKind.ENUM,
                            'trait': SymbolKind.TRAIT,
                            'impl': SymbolKind.CLASS,
                        }
                        results.append((name, kind, line_num, kind_map.get(kind, SymbolKind.CLASS)))
                return results

            parsed = await loop.run_in_executor(self.executor, parse_content)
            for name, kind, line_num, sym_kind in parsed:
                symbol = SymbolNode(
                    symbol_id=SymbolId.create(parsed_file.file_path, f"{kind}.{name}"),
                    file_id=parsed_file.file_id,
                    file_path=parsed_file.file_path,
                    line_range=(line_num, line_num),
                    symbol_kind=sym_kind,
                    symbol_name=name,
                    fully_qualified_name=name,
                    parent_symbol_id=None,
                    confidence=ConfidenceLevel.INFERRED,
                )
                symbols.append(symbol)
            parsed_file.symbols = symbols
            parsed_file.unresolved_references = unresolved
            parsed_file.parse_success = True
            parsed_file.parse_error = "Regex-based parsing - limited accuracy"
        except Exception as e:
            parsed_file.parse_success = False
            parsed_file.parse_error = f"Rust parse error: {str(e)}"
        return parsed_file

    async def _parse_go(self, parsed_file: ParsedFile, content: str) -> ParsedFile:
        patterns = {
            'func': re.compile(r'func\s+(?:\(\w+\s+\*\w+\)\s+)?(\w+)\s*\([^)]*\)'),
            'type': re.compile(r'type\s+(\w+)\s+struct'),
            'interface': re.compile(r'type\s+(\w+)\s+interface'),
        }
        symbols = []
        unresolved = []
        try:
            loop = asyncio.get_event_loop()

            def parse_content():
                results = []
                for kind, pattern in patterns.items():
                    for match in pattern.finditer(content):
                        name = match.group(1)
                        line_num = content[:match.start()].count('\n') + 1
                        kind_map = {
                            'func': SymbolKind.FUNCTION,
                            'type': SymbolKind.STRUCT,
                            'interface': SymbolKind.INTERFACE,
                        }
                        results.append((name, kind, line_num, kind_map.get(kind, SymbolKind.CLASS)))
                return results

            parsed = await loop.run_in_executor(self.executor, parse_content)
            for name, kind, line_num, sym_kind in parsed:
                symbol = SymbolNode(
                    symbol_id=SymbolId.create(parsed_file.file_path, f"{kind}.{name}"),
                    file_id=parsed_file.file_id,
                    file_path=parsed_file.file_path,
                    line_range=(line_num, line_num),
                    symbol_kind=sym_kind,
                    symbol_name=name,
                    fully_qualified_name=name,
                    parent_symbol_id=None,
                    confidence=ConfidenceLevel.INFERRED,
                )
                symbols.append(symbol)
            parsed_file.symbols = symbols
            parsed_file.unresolved_references = unresolved
            parsed_file.parse_success = True
            parsed_file.parse_error = "Regex-based parsing - limited accuracy"
        except Exception as e:
            parsed_file.parse_success = False
            parsed_file.parse_error = f"Go parse error: {str(e)}"
        return parsed_file

    async def parse_files(self, parsed_files: List[ParsedFile]) -> List[ParsedFile]:
        tasks = [self.parse_file(pf) for pf in parsed_files]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        parsed_results = []
        for result in results:
            if isinstance(result, Exception):
                log.debug(f"Error during parsing: {result}")
            elif result is not None:
                parsed_results.append(result)
        return parsed_results

    def get_metrics(self) -> List[PerformanceMetrics]:
        return self.metrics.copy()

    def close(self) -> None:
        self.executor.shutdown(wait=True)
