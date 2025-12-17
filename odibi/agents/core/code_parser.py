"""Python AST parser for extracting code structure and creating chunks.

Parses any Python codebase to extract classes, functions, modules, and their
relationships for indexing into a vector store.
"""

import ast
import hashlib
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class CodeChunk:
    """Represents a chunk of code for indexing."""

    id: str
    file_path: str
    module_name: str
    chunk_type: str
    name: str
    content: str
    repo: str = ""
    docstring: Optional[str] = None
    signature: Optional[str] = None
    parent_class: Optional[str] = None
    imports: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)
    line_start: int = 0
    line_end: int = 0
    engine_type: Optional[str] = None
    tags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for vector store."""
        return {
            "id": self.id,
            "file_path": self.file_path,
            "module_name": self.module_name,
            "chunk_type": self.chunk_type,
            "name": self.name,
            "content": self.content,
            "repo": self.repo,
            "docstring": self.docstring or "",
            "signature": self.signature or "",
            "parent_class": self.parent_class or "",
            "imports": self.imports,
            "dependencies": self.dependencies,
            "line_start": self.line_start,
            "line_end": self.line_end,
            "engine_type": self.engine_type or "",
            "tags": self.tags,
        }


class CodeParser:
    """Parser for extracting code structure from any codebase."""

    ENGINE_KEYWORDS = {
        "pandas": ["pandas", "pd.", "DataFrame", "Series", "apply", "groupby"],
        "spark": ["spark", "SparkSession", "pyspark", "udf", "broadcast", "rdd"],
        "polars": ["polars", "pl.", "LazyFrame", "collect"],
    }

    # File extensions to index (text-based files)
    INDEXABLE_EXTENSIONS = {
        # Code
        ".py",
        ".sql",
        ".scala",
        ".r",
        ".sh",
        ".bash",
        # Notebooks
        ".ipynb",
        # Config/Data
        ".yaml",
        ".yml",
        ".json",
        ".toml",
        ".ini",
        ".cfg",
        # Documentation
        ".md",
        ".rst",
        ".txt",
        # Web
        ".html",
        ".css",
        ".js",
        ".ts",
        ".jsx",
        ".tsx",
        ".svelte",
        ".vue",
    }

    # Directories to skip
    SKIP_DIRS = {
        "__pycache__",
        ".git",
        ".hg",
        ".svn",
        "node_modules",
        ".venv",
        "venv",
        ".tox",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        "dist",
        "build",
        "egg-info",
        ".eggs",
        ".ipynb_checkpoints",
        "_archive",
        ".odibi",
    }

    def __init__(self, root: str, repo_name: str | None = None):
        """Initialize the parser.

        Args:
            root: Path to the repository root.
            repo_name: Name for this repo (defaults to folder name).
        """
        self.root = Path(root)
        self.repo_name = repo_name or self.root.name
        self.source_lines: dict[str, list[str]] = {}

    def parse_file(self, file_path: Path) -> list[CodeChunk]:
        """Parse a single Python file and extract code chunks.

        Args:
            file_path: Path to the Python file.

        Returns:
            List of CodeChunk objects.
        """
        try:
            content = file_path.read_text(encoding="utf-8")
            self.source_lines[str(file_path)] = content.splitlines()
            tree = ast.parse(content)
        except (SyntaxError, UnicodeDecodeError) as e:
            print(f"Failed to parse {file_path}: {e}")
            return []

        rel_path = file_path.relative_to(self.root)
        module_name = str(rel_path).replace(os.sep, ".").replace(".py", "")

        chunks = []

        imports = self._extract_imports(tree)
        module_doc = ast.get_docstring(tree)

        module_chunk = CodeChunk(
            id=self._generate_id(str(rel_path), "module", module_name, 1),
            file_path=str(rel_path),
            module_name=module_name,
            chunk_type="module",
            name=module_name,
            content=self._get_module_summary(tree, content),
            repo=self.repo_name,
            docstring=module_doc,
            imports=imports,
            line_start=1,
            line_end=len(content.splitlines()),
            tags=self._infer_tags(module_name, content),
            engine_type=self._detect_engine(content),
        )
        chunks.append(module_chunk)

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                chunks.append(self._process_class(node, str(rel_path), module_name, imports))

                for item in node.body:
                    if isinstance(item, ast.FunctionDef):
                        chunks.append(
                            self._process_function(
                                item,
                                str(rel_path),
                                module_name,
                                imports,
                                parent_class=node.name,
                            )
                        )

            elif isinstance(node, ast.FunctionDef) and self._is_top_level(node, tree):
                chunks.append(self._process_function(node, str(rel_path), module_name, imports))

        return chunks

    def _should_skip_path(self, path: Path) -> bool:
        """Check if a path should be skipped."""
        path_parts = set(path.parts)
        return bool(path_parts & self.SKIP_DIRS)

    def _get_all_files(self, target_dir: Path) -> list[Path]:
        """Get all indexable files in a directory."""
        all_files = []
        for ext in self.INDEXABLE_EXTENSIONS:
            try:
                files = list(target_dir.rglob(f"*{ext}"))
                all_files.extend(files)
            except Exception as e:
                logger.warning(f"rglob failed for {ext}: {e}")
        return all_files

    def parse_directory(self, directory: Optional[Path] = None) -> list[CodeChunk]:
        """Parse all indexable files in a directory.

        Args:
            directory: Directory to parse (defaults to root or <root>/<repo_name> subfolder).

        Returns:
            List of all CodeChunk objects.
        """
        subdir = self.root / self.repo_name

        logger.info(f"parse_directory called with root={self.root}, repo={self.repo_name}")

        if directory:
            target_dir = directory
            logger.info(f"Using provided directory: {target_dir}")
        elif subdir.exists() and subdir.is_dir():
            target_dir = subdir
            logger.info(f"Using subdir: {target_dir}")
        else:
            target_dir = self.root
            logger.info(f"Falling back to root: {target_dir}")

        all_chunks = []
        files_found = 0
        files_skipped = 0
        files_parsed = 0
        files_by_type: dict[str, int] = {}

        all_files = self._get_all_files(target_dir)
        logger.info(f"Found {len(all_files)} indexable files in {target_dir}")

        for file_path in all_files:
            files_found += 1

            if self._should_skip_path(file_path):
                files_skipped += 1
                continue

            ext = file_path.suffix.lower()
            files_by_type[ext] = files_by_type.get(ext, 0) + 1
            files_parsed += 1

            if ext == ".py":
                chunks = self.parse_file(file_path)
            elif ext == ".ipynb":
                chunks = self._parse_notebook(file_path)
            else:
                chunks = self._parse_text_file(file_path)

            all_chunks.extend(chunks)

        logger.info(
            f"parse_directory complete: found={files_found}, skipped={files_skipped}, "
            f"parsed={files_parsed}, chunks={len(all_chunks)}"
        )
        logger.info(f"Files by type: {files_by_type}")
        return all_chunks

    def _process_class(
        self,
        node: ast.ClassDef,
        file_path: str,
        module_name: str,
        imports: list[str],
    ) -> CodeChunk:
        """Process a class definition."""
        source = self._get_source(file_path, node.lineno, node.end_lineno or node.lineno)
        docstring = ast.get_docstring(node)

        bases = [self._get_name(base) for base in node.bases]

        methods = [item.name for item in node.body if isinstance(item, ast.FunctionDef)]

        signature = f"class {node.name}"
        if bases:
            signature += f"({', '.join(bases)})"

        return CodeChunk(
            id=self._generate_id(file_path, "class", node.name, node.lineno),
            file_path=file_path,
            module_name=module_name,
            chunk_type="class",
            name=node.name,
            content=source,
            repo=self.repo_name,
            docstring=docstring,
            signature=signature,
            imports=imports,
            dependencies=bases + methods,
            line_start=node.lineno,
            line_end=node.end_lineno or node.lineno,
            tags=self._infer_class_tags(node.name, bases, docstring),
            engine_type=self._detect_engine(source),
        )

    def _process_function(
        self,
        node: ast.FunctionDef,
        file_path: str,
        module_name: str,
        imports: list[str],
        parent_class: Optional[str] = None,
    ) -> CodeChunk:
        """Process a function definition."""
        source = self._get_source(file_path, node.lineno, node.end_lineno or node.lineno)
        docstring = ast.get_docstring(node)

        signature = self._get_function_signature(node)

        dependencies = []
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                func_name = self._get_name(child.func)
                if func_name:
                    dependencies.append(func_name)

        chunk_type = "method" if parent_class else "function"
        full_name = f"{parent_class}.{node.name}" if parent_class else node.name

        decorators = [self._get_name(d) for d in node.decorator_list if self._get_name(d)]

        tags = self._infer_function_tags(node.name, decorators, docstring)

        return CodeChunk(
            id=self._generate_id(file_path, chunk_type, full_name, node.lineno),
            file_path=file_path,
            module_name=module_name,
            chunk_type=chunk_type,
            name=full_name,
            content=source,
            repo=self.repo_name,
            docstring=docstring,
            signature=signature,
            parent_class=parent_class,
            imports=imports,
            dependencies=list(set(dependencies)),
            line_start=node.lineno,
            line_end=node.end_lineno or node.lineno,
            tags=tags,
            engine_type=self._detect_engine(source),
        )

    def _extract_imports(self, tree: ast.Module) -> list[str]:
        """Extract all import statements from a module."""
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                for alias in node.names:
                    imports.append(f"{module}.{alias.name}")
        return imports

    def _get_function_signature(self, node: ast.FunctionDef) -> str:
        """Get the full function signature."""
        args = []
        for arg in node.args.args:
            arg_str = arg.arg
            if arg.annotation:
                arg_str += f": {self._get_name(arg.annotation)}"
            args.append(arg_str)

        defaults_offset = len(node.args.args) - len(node.args.defaults)
        for i, default in enumerate(node.args.defaults):
            arg_idx = defaults_offset + i
            if arg_idx < len(args):
                default_val = self._get_default_value(default)
                args[arg_idx] += f" = {default_val}"

        return_type = ""
        if node.returns:
            return_type = f" -> {self._get_name(node.returns)}"

        return f"def {node.name}({', '.join(args)}){return_type}"

    def _get_name(self, node) -> str:
        """Get the string name from an AST node."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return f"{self._get_name(node.value)}.{node.attr}"
        elif isinstance(node, ast.Subscript):
            return f"{self._get_name(node.value)}[{self._get_name(node.slice)}]"
        elif isinstance(node, ast.Constant):
            return repr(node.value)
        elif isinstance(node, ast.Tuple):
            return f"({', '.join(self._get_name(e) for e in node.elts)})"
        elif isinstance(node, ast.List):
            return f"[{', '.join(self._get_name(e) for e in node.elts)}]"
        return ""

    def _get_default_value(self, node) -> str:
        """Get string representation of default value."""
        if isinstance(node, ast.Constant):
            return repr(node.value)
        elif isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.List):
            return "[]"
        elif isinstance(node, ast.Dict):
            return "{}"
        elif isinstance(node, ast.Call):
            return f"{self._get_name(node.func)}(...)"
        return "..."

    def _get_source(self, file_path: str, start: int, end: int) -> str:
        """Get source code lines from a file."""
        lines = self.source_lines.get(str(self.root / file_path), [])
        if not lines:
            return ""
        return "\n".join(lines[start - 1 : end])

    def _get_module_summary(self, tree: ast.Module, content: str) -> str:
        """Get a summary of the module (first N lines + structure)."""
        lines = content.splitlines()[:50]

        classes = [node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]
        functions = [
            node.name
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and self._is_top_level(node, tree)
        ]

        summary_parts = ["\n".join(lines)]
        if classes:
            summary_parts.append(f"\n# Classes: {', '.join(classes)}")
        if functions:
            summary_parts.append(f"\n# Functions: {', '.join(functions)}")

        return "".join(summary_parts)

    def _is_top_level(self, node: ast.FunctionDef, tree: ast.Module) -> bool:
        """Check if a function is at the top level (not inside a class)."""
        for item in tree.body:
            if item is node:
                return True
        return False

    def _generate_id(self, file_path: str, chunk_type: str, name: str, line_start: int = 0) -> str:
        """Generate a unique ID for a chunk."""
        key = f"{self.repo_name}:{file_path}:{chunk_type}:{name}:{line_start}"
        return hashlib.md5(key.encode()).hexdigest()

    def _detect_engine(self, content: str) -> Optional[str]:
        """Detect which engine (pandas/spark/polars) the code is for."""
        content_lower = content.lower()
        for engine, keywords in self.ENGINE_KEYWORDS.items():
            for keyword in keywords:
                if keyword.lower() in content_lower:
                    return engine
        return None

    def _infer_tags(self, module_name: str, content: str) -> list[str]:
        """Infer tags based on module name and content."""
        tags = []

        if "test" in module_name.lower():
            tags.append("test")
        if "transformer" in content.lower():
            tags.append("transformer")
        if "pydantic" in content.lower() or "BaseModel" in content:
            tags.append("pydantic")
        if "registry" in content.lower() or "@transform" in content:
            tags.append("registry")
        if "lineage" in content.lower():
            tags.append("lineage")
        if "scd" in module_name.lower():
            tags.append("scd")
        if "delta" in content.lower():
            tags.append("delta")

        return list(set(tags))

    def _infer_class_tags(
        self,
        name: str,
        bases: list[str],
        docstring: Optional[str],
    ) -> list[str]:
        """Infer tags for a class."""
        tags = []

        if "BaseModel" in bases or "pydantic" in str(bases).lower():
            tags.append("pydantic")
        if "Engine" in name or "Engine" in bases:
            tags.append("engine")
        if "Transformer" in name:
            tags.append("transformer")
        if "Node" in name:
            tags.append("node")
        if "Config" in name:
            tags.append("config")
        if "Exception" in bases or "Error" in bases:
            tags.append("exception")

        return list(set(tags))

    def _infer_function_tags(
        self,
        name: str,
        decorators: list[str],
        docstring: Optional[str],
    ) -> list[str]:
        """Infer tags for a function."""
        tags = []

        if "transform" in decorators:
            tags.append("transform")
            tags.append("registry")
        if name.startswith("test_"):
            tags.append("test")
        if name.startswith("_"):
            tags.append("private")
        if "validate" in name.lower():
            tags.append("validation")
        if "execute" in name.lower():
            tags.append("execution")
        if "read" in name.lower():
            tags.append("read")
        if "write" in name.lower():
            tags.append("write")

        return list(set(tags))

    def _parse_text_file(self, file_path: Path) -> list[CodeChunk]:
        """Parse a text file (markdown, sql, yaml, etc.) as a single chunk."""
        try:
            content = file_path.read_text(encoding="utf-8", errors="ignore")
        except Exception as e:
            logger.warning(f"Failed to read {file_path}: {e}")
            return []

        if not content.strip():
            return []

        rel_path = file_path.relative_to(self.root)
        ext = file_path.suffix.lower()
        chunk_type = {
            ".md": "markdown",
            ".rst": "documentation",
            ".txt": "text",
            ".sql": "sql",
            ".yaml": "config",
            ".yml": "config",
            ".json": "config",
            ".toml": "config",
        }.get(ext, "file")

        chunk_id = hashlib.md5(f"{self.repo_name}:{rel_path}".encode()).hexdigest()

        return [
            CodeChunk(
                id=chunk_id,
                file_path=str(rel_path),
                module_name=str(rel_path),
                chunk_type=chunk_type,
                name=file_path.name,
                content=content[:50000],  # Limit size
                repo=self.repo_name,
                line_start=1,
                line_end=content.count("\n") + 1,
            )
        ]

    def _parse_notebook(self, file_path: Path) -> list[CodeChunk]:
        """Parse a Jupyter/Databricks notebook (.ipynb) into chunks."""
        import json

        try:
            content = file_path.read_text(encoding="utf-8", errors="ignore")
            nb = json.loads(content)
        except Exception as e:
            logger.warning(f"Failed to parse notebook {file_path}: {e}")
            return []

        rel_path = file_path.relative_to(self.root)
        chunks = []
        cell_num = 0

        for cell in nb.get("cells", []):
            cell_num += 1
            cell_type = cell.get("cell_type", "code")
            source = "".join(cell.get("source", []))

            if not source.strip():
                continue

            chunk_id = hashlib.md5(
                f"{self.repo_name}:{rel_path}:cell{cell_num}".encode()
            ).hexdigest()

            chunk_type = "notebook_code" if cell_type == "code" else "notebook_markdown"

            chunks.append(
                CodeChunk(
                    id=chunk_id,
                    file_path=str(rel_path),
                    module_name=f"{rel_path}#cell{cell_num}",
                    chunk_type=chunk_type,
                    name=f"{file_path.stem}_cell{cell_num}",
                    content=source[:10000],  # Limit cell size
                    repo=self.repo_name,
                    line_start=cell_num,
                    line_end=cell_num,
                )
            )

        return chunks


# Backward compatibility alias
OdibiCodeParser = CodeParser


def parse_codebase(root: str, repo_name: str | None = None) -> list[dict]:
    """Parse a Python codebase and return chunks as dictionaries.

    Args:
        root: Path to the repository root.
        repo_name: Name for this repo (defaults to folder name).

    Returns:
        List of chunk dictionaries ready for indexing.
    """
    parser = CodeParser(root, repo_name)
    chunks = parser.parse_directory()
    return [chunk.to_dict() for chunk in chunks]


# Legacy function name
def parse_odibi_codebase(odibi_root: str) -> list[dict]:
    """Parse the Odibi codebase (legacy function)."""
    return parse_codebase(odibi_root, "odibi")
