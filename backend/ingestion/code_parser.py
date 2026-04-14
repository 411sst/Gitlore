import logging
from pathlib import Path
from typing import Any

from tree_sitter import Language, Parser, Node

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Language registry — loaded lazily to avoid import-time cost
# ---------------------------------------------------------------------------

def _load_languages() -> dict[str, Language]:
    langs: dict[str, Language] = {}

    def _try(key: str, loader):
        try:
            langs[key] = Language(loader())
        except Exception as exc:
            logger.warning("Could not load tree-sitter language %r: %s", key, exc)

    import tree_sitter_python as _py; _try("python", _py.language)
    import tree_sitter_go as _go; _try("go", _go.language)
    import tree_sitter_javascript as _js; _try("javascript", _js.language)
    import tree_sitter_typescript as _ts
    _try("typescript", _ts.language_typescript)
    _try("tsx", _ts.language_tsx)
    import tree_sitter_java as _java; _try("java", _java.language)
    import tree_sitter_rust as _rust; _try("rust", _rust.language)
    import tree_sitter_c as _c; _try("c", _c.language)
    import tree_sitter_cpp as _cpp; _try("cpp", _cpp.language)

    return langs


_LANGUAGES: dict[str, Language] | None = None

def _get_languages() -> dict[str, Language]:
    global _LANGUAGES
    if _LANGUAGES is None:
        _LANGUAGES = _load_languages()
    return _LANGUAGES


_EXT_TO_LANG: dict[str, str] = {
    ".py": "python",
    ".go": "go",
    ".js": "javascript",
    ".jsx": "javascript",
    ".ts": "typescript",
    ".tsx": "tsx",
    ".java": "java",
    ".rs": "rust",
    ".c": "c",
    ".h": "c",
    ".cpp": "cpp",
    ".cc": "cpp",
    ".cxx": "cpp",
    ".hpp": "cpp",
}


def _lang_for_ext(ext: str) -> str | None:
    return _EXT_TO_LANG.get(ext.lower())


# ---------------------------------------------------------------------------
# Generic node-walking helpers
# ---------------------------------------------------------------------------

def _child_by_type(node: Node, *types: str) -> Node | None:
    for child in node.children:
        if child.type in types:
            return child
    return None


def _children_by_type(node: Node, *types: str) -> list[Node]:
    return [c for c in node.children if c.type in types]


def _node_text(node: Node, src: bytes) -> str:
    return src[node.start_byte:node.end_byte].decode("utf-8", errors="replace")


def _walk_all(node: Node):
    """Yield every node in the subtree (depth-first)."""
    yield node
    for child in node.children:
        yield from _walk_all(child)


# ---------------------------------------------------------------------------
# Language-specific extractors
# ---------------------------------------------------------------------------

class _PythonExtractor:
    @staticmethod
    def extract(root: Node, src: bytes) -> dict:
        functions, classes, imports = [], [], []

        for node in _walk_all(root):
            if node.child_count == 0:
                continue
            if node.type == "function_definition":
                name_node = _child_by_type(node, "identifier")
                params_node = _child_by_type(node, "parameters")
                body_node = _child_by_type(node, "block")
                docstring = _PythonExtractor._docstring(body_node, src)
                params = _PythonExtractor._params(params_node, src)
                functions.append({
                    "name": _node_text(name_node, src) if name_node else "",
                    "start_line": node.start_point[0] + 1,
                    "end_line": node.end_point[0] + 1,
                    "docstring": docstring,
                    "parameters": params,
                })

            elif node.type == "class_definition":
                name_node = _child_by_type(node, "identifier")
                classes.append({
                    "name": _node_text(name_node, src) if name_node else "",
                    "start_line": node.start_point[0] + 1,
                })

            elif node.type in ("import_statement", "import_from_statement"):
                imports.append(_node_text(node, src).strip())

        return {"functions": functions, "classes": classes, "imports": imports}

    @staticmethod
    def _docstring(block: Node | None, src: bytes) -> str | None:
        if not block:
            return None
        for child in block.children:
            if child.type == "expression_statement":
                for sub in child.children:
                    if sub.type == "string":
                        raw = _node_text(sub, src)
                        return raw.strip('"\' \t\n').strip('"""').strip("'''").strip()
        return None

    @staticmethod
    def _params(params_node: Node | None, src: bytes) -> list[str]:
        if not params_node:
            return []
        result = []
        for child in params_node.children:
            if child.type in (
                "identifier", "typed_parameter", "default_parameter",
                "typed_default_parameter", "list_splat_pattern",
                "dictionary_splat_pattern",
            ):
                result.append(_node_text(child, src).strip())
        return result


class _GoExtractor:
    @staticmethod
    def extract(root: Node, src: bytes) -> dict:
        functions, classes, imports = [], [], []

        for node in _walk_all(root):
            if node.child_count == 0:
                continue
            if node.type in ("function_declaration", "method_declaration"):
                name_node = _child_by_type(node, "identifier", "field_identifier")
                params_node = _child_by_type(node, "parameter_list")
                params = _GoExtractor._params(params_node, src)
                functions.append({
                    "name": _node_text(name_node, src) if name_node else "",
                    "start_line": node.start_point[0] + 1,
                    "end_line": node.end_point[0] + 1,
                    "docstring": None,
                    "parameters": params,
                })

            elif node.type == "type_declaration":
                for spec in _children_by_type(node, "type_spec"):
                    name_node = _child_by_type(spec, "type_identifier")
                    if name_node:
                        classes.append({
                            "name": _node_text(name_node, src),
                            "start_line": spec.start_point[0] + 1,
                        })

            elif node.type in ("import_declaration",):
                imports.append(_node_text(node, src).strip())

        return {"functions": functions, "classes": classes, "imports": imports}

    @staticmethod
    def _params(params_node: Node | None, src: bytes) -> list[str]:
        if not params_node:
            return []
        result = []
        for child in params_node.children:
            if child.type in ("parameter_declaration", "variadic_parameter_declaration"):
                result.append(_node_text(child, src).strip())
        return result


class _JSTSExtractor:
    """Handles JavaScript, TypeScript, and TSX."""

    @staticmethod
    def extract(root: Node, src: bytes) -> dict:
        functions, classes, imports = [], [], []

        for node in _walk_all(root):
            if node.child_count == 0:
                continue
            if node.type in (
                "function_declaration",
                "function",
                "generator_function_declaration",
                "generator_function",
            ):
                name_node = _child_by_type(node, "identifier")
                params_node = _child_by_type(node, "formal_parameters")
                functions.append({
                    "name": _node_text(name_node, src) if name_node else "<anonymous>",
                    "start_line": node.start_point[0] + 1,
                    "end_line": node.end_point[0] + 1,
                    "docstring": None,
                    "parameters": _JSTSExtractor._params(params_node, src),
                })

            elif node.type in ("method_definition", "method_signature"):
                name_node = _child_by_type(node, "property_identifier", "identifier")
                params_node = _child_by_type(node, "formal_parameters")
                functions.append({
                    "name": _node_text(name_node, src) if name_node else "",
                    "start_line": node.start_point[0] + 1,
                    "end_line": node.end_point[0] + 1,
                    "docstring": None,
                    "parameters": _JSTSExtractor._params(params_node, src),
                })

            elif node.type in ("arrow_function",):
                # Named only if parent is a variable_declarator
                parent = node.parent
                name = "<arrow>"
                if parent and parent.type == "variable_declarator":
                    n = _child_by_type(parent, "identifier")
                    if n:
                        name = _node_text(n, src)
                params_node = _child_by_type(node, "formal_parameters", "identifier")
                functions.append({
                    "name": name,
                    "start_line": node.start_point[0] + 1,
                    "end_line": node.end_point[0] + 1,
                    "docstring": None,
                    "parameters": _JSTSExtractor._params(params_node, src),
                })

            elif node.type in ("class_declaration", "class"):
                name_node = _child_by_type(node, "identifier", "type_identifier")
                classes.append({
                    "name": _node_text(name_node, src) if name_node else "<anonymous>",
                    "start_line": node.start_point[0] + 1,
                })

            elif node.type == "import_statement":
                imports.append(_node_text(node, src).strip())

        return {"functions": functions, "classes": classes, "imports": imports}

    @staticmethod
    def _params(params_node: Node | None, src: bytes) -> list[str]:
        if not params_node:
            return []
        if params_node.type == "identifier":
            return [_node_text(params_node, src)]
        result = []
        for child in params_node.children:
            if child.type in (
                "identifier", "required_parameter", "optional_parameter",
                "assignment_pattern", "rest_pattern",
            ):
                result.append(_node_text(child, src).strip())
        return result


class _JavaExtractor:
    @staticmethod
    def extract(root: Node, src: bytes) -> dict:
        functions, classes, imports = [], [], []

        for node in _walk_all(root):
            if node.child_count == 0:
                continue
            if node.type == "method_declaration":
                name_node = _child_by_type(node, "identifier")
                params_node = _child_by_type(node, "formal_parameters")
                functions.append({
                    "name": _node_text(name_node, src) if name_node else "",
                    "start_line": node.start_point[0] + 1,
                    "end_line": node.end_point[0] + 1,
                    "docstring": None,
                    "parameters": _JavaExtractor._params(params_node, src),
                })

            elif node.type in ("class_declaration", "interface_declaration", "enum_declaration"):
                name_node = _child_by_type(node, "identifier")
                classes.append({
                    "name": _node_text(name_node, src) if name_node else "",
                    "start_line": node.start_point[0] + 1,
                })

            elif node.type == "import_declaration":
                imports.append(_node_text(node, src).strip())

        return {"functions": functions, "classes": classes, "imports": imports}

    @staticmethod
    def _params(params_node: Node | None, src: bytes) -> list[str]:
        if not params_node:
            return []
        return [
            _node_text(c, src).strip()
            for c in params_node.children
            if c.type == "formal_parameter"
        ]


class _RustExtractor:
    @staticmethod
    def extract(root: Node, src: bytes) -> dict:
        functions, classes, imports = [], [], []

        for node in _walk_all(root):
            if node.child_count == 0:
                continue
            if node.type == "function_item":
                name_node = _child_by_type(node, "identifier")
                params_node = _child_by_type(node, "parameters")
                functions.append({
                    "name": _node_text(name_node, src) if name_node else "",
                    "start_line": node.start_point[0] + 1,
                    "end_line": node.end_point[0] + 1,
                    "docstring": None,
                    "parameters": _RustExtractor._params(params_node, src),
                })

            elif node.type in ("struct_item", "enum_item", "trait_item", "impl_item"):
                name_node = _child_by_type(node, "type_identifier")
                if name_node:
                    classes.append({
                        "name": _node_text(name_node, src),
                        "start_line": node.start_point[0] + 1,
                    })

            elif node.type == "use_declaration":
                imports.append(_node_text(node, src).strip())

        return {"functions": functions, "classes": classes, "imports": imports}

    @staticmethod
    def _params(params_node: Node | None, src: bytes) -> list[str]:
        if not params_node:
            return []
        return [
            _node_text(c, src).strip()
            for c in params_node.children
            if c.type in ("parameter", "self_parameter")
        ]


class _CExtractor:
    @staticmethod
    def extract(root: Node, src: bytes) -> dict:
        functions, classes, imports = [], [], []

        for node in _walk_all(root):
            if node.child_count == 0:
                continue
            if node.type == "function_definition":
                # declarator contains the name + params
                decl = _child_by_type(node, "function_declarator")
                if decl:
                    name_node = _child_by_type(decl, "identifier")
                    params_node = _child_by_type(decl, "parameter_list")
                else:
                    name_node, params_node = None, None
                functions.append({
                    "name": _node_text(name_node, src) if name_node else "",
                    "start_line": node.start_point[0] + 1,
                    "end_line": node.end_point[0] + 1,
                    "docstring": None,
                    "parameters": _CExtractor._params(params_node, src),
                })

            elif node.type == "struct_specifier":
                name_node = _child_by_type(node, "type_identifier")
                if name_node:
                    classes.append({
                        "name": _node_text(name_node, src),
                        "start_line": node.start_point[0] + 1,
                    })

            elif node.type == "preproc_include":
                imports.append(_node_text(node, src).strip())

        return {"functions": functions, "classes": classes, "imports": imports}

    @staticmethod
    def _params(params_node: Node | None, src: bytes) -> list[str]:
        if not params_node:
            return []
        return [
            _node_text(c, src).strip()
            for c in params_node.children
            if c.type == "parameter_declaration"
        ]


_EXTRACTORS: dict[str, Any] = {
    "python": _PythonExtractor,
    "go": _GoExtractor,
    "javascript": _JSTSExtractor,
    "typescript": _JSTSExtractor,
    "tsx": _JSTSExtractor,
    "java": _JavaExtractor,
    "rust": _RustExtractor,
    "c": _CExtractor,
    "cpp": _CExtractor,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class CodeParser:
    def __init__(self, repo_path: str):
        self.repo_path = repo_path
        self._parsers: dict[str, Parser] = {}

    def _get_parser(self, lang: str) -> Parser | None:
        if lang not in self._parsers:
            langs = _get_languages()
            if lang not in langs:
                return None
            self._parsers[lang] = Parser(langs[lang])
        return self._parsers[lang]

    def parse_file(self, file_path: str) -> dict:
        """Parse a single file and return structured AST info."""
        path = Path(file_path)
        lang = _lang_for_ext(path.suffix)
        if lang is None:
            raise ValueError(f"Unsupported extension: {path.suffix!r}")

        ts_parser = self._get_parser(lang)
        if ts_parser is None:
            raise ValueError(f"No tree-sitter parser available for language: {lang!r}")

        extractor = _EXTRACTORS.get(lang)
        if extractor is None:
            raise ValueError(f"No extractor defined for language: {lang!r}")

        src = path.read_bytes()
        tree = ts_parser.parse(src)
        extracted = extractor.extract(tree.root_node, src)

        return {
            "path": str(path),
            "language": lang,
            **extracted,
        }

    def parse_all(self, file_paths: list[str]) -> list[dict]:
        """Parse a list of files, skipping failures."""
        results = []
        for fp in file_paths:
            try:
                results.append(self.parse_file(fp))
            except Exception as exc:
                logger.warning("Skipping %s: %s", fp, exc)
        return results


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import glob
    import json
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    repo_dir = "/tmp/etcd_test"
    if not Path(repo_dir).exists():
        print(f"{repo_dir} does not exist. Run git_parser.py first.", file=sys.stderr)
        sys.exit(1)

    go_files = sorted(glob.glob(f"{repo_dir}/**/*.go", recursive=True))
    print(f"Found {len(go_files)} .go files in {repo_dir}")

    parser = CodeParser(repo_dir)
    results = parser.parse_all(go_files)

    # Collect all functions across all files
    all_functions = []
    for r in results:
        for fn in r["functions"]:
            all_functions.append({"file": r["path"], **fn})

    print(f"\nTotal functions found: {len(all_functions)}")
    print("\nFirst 3 function definitions:")
    for fn in all_functions[:3]:
        print(json.dumps(fn, indent=2))
