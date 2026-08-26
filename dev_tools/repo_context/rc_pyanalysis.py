"""AST-based Python symbol / import / call extraction.

Everything here is static analysis via the stdlib ``ast`` module. Source
files are parsed as text and never imported or executed.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass, field
from typing import Optional

from rc_common import SymbolRecord, ImportRecord, CallRecord

MODULE_SYMBOL = "<module>"

DECISION_NODE_TYPES = (
    ast.If, ast.For, ast.AsyncFor, ast.While, ast.Try, ast.With, ast.AsyncWith,
    ast.Assert,
)


def dotted_module_path(rel_posix: str) -> str:
    p = rel_posix[:-3] if rel_posix.endswith(".py") else rel_posix
    parts = [seg for seg in p.split("/") if seg]
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _complexity_count(node: ast.AST) -> int:
    total = 0
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        if isinstance(child, DECISION_NODE_TYPES):
            total += 1
        elif isinstance(child, ast.ExceptHandler):
            total += 1
        elif isinstance(child, ast.BoolOp):
            total += max(0, len(child.values) - 1)
        elif isinstance(child, ast.comprehension):
            total += 1 + len(child.ifs)
        total += _complexity_count(child)
    return total


def complexity_approx(node: ast.AST) -> int:
    return 1 + _complexity_count(node)


def _is_main_guard(node: ast.AST) -> bool:
    if not isinstance(node, ast.If):
        return False
    test = node.test
    return (isinstance(test, ast.Compare) and isinstance(test.left, ast.Name)
            and test.left.id == "__name__" and len(test.ops) == 1
            and isinstance(test.ops[0], ast.Eq) and len(test.comparators) == 1
            and isinstance(test.comparators[0], ast.Constant)
            and test.comparators[0].value == "__main__")


def _is_static_false_test(test: ast.AST) -> bool:
    """True for an `if` test that is statically guaranteed never to
    execute its `body` at runtime: a literal `False`, or the
    `typing.TYPE_CHECKING` convention (which is only True for static type
    checkers, never at actual runtime)."""
    if isinstance(test, ast.Constant) and test.value is False:
        return True
    if isinstance(test, ast.Name) and test.id == "TYPE_CHECKING":
        return True
    if isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING":
        return True
    return False


def _is_static_true_test(test: ast.AST) -> bool:
    """True for an `if` test that is statically guaranteed to always
    execute its `body` (making its `orelse`, if any, statically dead)."""
    return isinstance(test, ast.Constant) and test.value is True


def _unparse_safe(node) -> str:
    if node is None:
        return ""
    try:
        return ast.unparse(node)
    except Exception:
        return "<unparseable>"


def format_params(args: ast.arguments) -> str:
    parts = []

    def fmt(a: ast.arg, default=None) -> str:
        s = a.arg
        if a.annotation is not None:
            s += f": {_unparse_safe(a.annotation)}"
        if default is not None:
            s += f" = {_unparse_safe(default)}"
        return s

    posonly = list(getattr(args, "posonlyargs", []) or [])
    pos = list(args.args)
    n_pos = len(posonly) + len(pos)
    defaults = list(args.defaults)
    pad = [None] * (n_pos - len(defaults)) + defaults
    idx = 0
    for a in posonly:
        parts.append(fmt(a, pad[idx])); idx += 1
    if posonly:
        parts.append("/")
    for a in pos:
        parts.append(fmt(a, pad[idx])); idx += 1
    if args.vararg:
        parts.append("*" + fmt(args.vararg))
    elif args.kwonlyargs:
        parts.append("*")
    for a, d in zip(args.kwonlyargs, args.kw_defaults):
        parts.append(fmt(a, d))
    if args.kwarg:
        parts.append("**" + fmt(args.kwarg))
    return ", ".join(parts)


@dataclass
class RawCall:
    line: int
    call_expression: str
    callee_simple_name: str
    kind: str  # "name" | "self_attribute" | "attribute_on_name" | "other"
    base_name: str
    caller_symbol: str
    class_context: str


@dataclass
class ClassInfo:
    qualified_name: str
    bases: list = field(default_factory=list)  # raw unparsed base expressions
    methods: set = field(default_factory=set)   # simple method names


@dataclass
class PyFileAnalysis:
    symbols: list  # SymbolRecord
    imports: list  # ImportRecord (resolved_file/resolution_status left blank)
    raw_calls: list  # RawCall
    has_main_guard: bool
    top_level_index: dict  # simple name -> qualified name (module-level classes/functions)
    class_info: dict  # class qualified name -> ClassInfo
    local_names_by_symbol: dict  # function-type qualified name -> set of locally-bound simple names
    parent_of: dict  # qualified name -> parent qualified name, for every symbol in this file
    imports_by_scope: dict  # qualified name -> list[ImportRecord] declared directly in that scope
    module_reassigned_names: set  # names with a plain assignment at module level, separate from any def


def _lambda_param_names(node: ast.Lambda) -> frozenset:
    args = node.args
    names = set()
    for a in list(getattr(args, "posonlyargs", []) or []) + list(args.args) + list(args.kwonlyargs):
        names.add(a.arg)
    if args.vararg:
        names.add(args.vararg.arg)
    if args.kwarg:
        names.add(args.kwarg.arg)
    return frozenset(names)


_COMPREHENSION_TYPES = (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)


def _collect_local_bound_names(node) -> set:
    """Parameter names plus simple assignment targets bound directly in
    this function's own body (not inside a nested function/class, which
    has its own scope). Used to avoid resolving a bare call to a
    module-level function/class when the name is actually shadowed by a
    parameter or local variable in the enclosing scope."""
    names = set()
    args = node.args
    for a in list(getattr(args, "posonlyargs", []) or []) + list(args.args) + list(args.kwonlyargs):
        names.add(a.arg)
    if args.vararg:
        names.add(args.vararg.arg)
    if args.kwarg:
        names.add(args.kwarg.arg)

    def walk_stmts(stmts) -> None:
        # Operates on an *iterable of nodes* and checks each one directly
        # (not just its children) before recursing into its children --
        # otherwise a node passed in as the traversal root would never be
        # checked against itself, only its own children would.
        for stmt in stmts:
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                # Its body is a separate scope (don't descend), but its own
                # name is still bound in *this* scope for the whole
                # function, Python-style -- def/class is just an assignment.
                names.add(stmt.name)
                continue
            if isinstance(stmt, ast.Name) and isinstance(stmt.ctx, ast.Store):
                names.add(stmt.id)  # also catches walrus (:=) targets
            elif isinstance(stmt, ast.ExceptHandler) and stmt.name:
                names.add(stmt.name)
            if isinstance(stmt, _COMPREHENSION_TYPES):
                # A comprehension's own `for x in ...` targets are scoped
                # to the comprehension itself in Python 3, not to the
                # enclosing function -- don't treat them as locals here.
                # (A walrus `:=` used inside the comprehension DOES leak
                # to the enclosing scope and is still picked up normally,
                # via the generic Store-name check above, reached through
                # the recursive walk of elt/key/value/iter/ifs below.)
                elt_nodes = [stmt.elt] if hasattr(stmt, "elt") else [stmt.key, stmt.value]
                walk_stmts(elt_nodes)
                for gen in stmt.generators:
                    walk_stmts([gen.iter])
                    walk_stmts(gen.ifs)
                continue
            walk_stmts(ast.iter_child_nodes(stmt))

    walk_stmts(node.body)
    return names


def _collect_module_reassigned_names(tree: ast.Module) -> set:
    """Plain assignment targets (not def/class statements themselves)
    found directly at module level, opaque at nested function/class
    boundaries. Used to flag `def target(): ...` followed later by
    `target = something_else()` -- top_level_index alone can't tell us
    the name was rebound, so a bare `target()` call shouldn't be
    confidently resolved to the original def."""
    names = set()

    def walk_stmts(stmts) -> None:
        for stmt in stmts:
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue  # the def/class binding itself isn't a "rebinding"
            if isinstance(stmt, ast.Name) and isinstance(stmt.ctx, ast.Store):
                names.add(stmt.id)
            elif isinstance(stmt, ast.ExceptHandler) and stmt.name:
                names.add(stmt.name)
            walk_stmts(ast.iter_child_nodes(stmt))

    walk_stmts(tree.body)
    return names


def analyze_python_source(rel_path: str, source: str) -> PyFileAnalysis:
    tree = ast.parse(source, filename=rel_path)

    symbols: list[SymbolRecord] = []
    imports: list[ImportRecord] = []
    raw_calls: list[RawCall] = []
    top_level_index: dict = {}
    class_info: dict = {}
    local_names_by_symbol: dict = {}
    imports_by_scope: dict = {}
    has_main_guard = False

    source_module = dotted_module_path(rel_path)
    total_lines = len(source.splitlines()) or 1

    module_symbol = SymbolRecord(
        relative_path=rel_path,
        qualified_name=MODULE_SYMBOL,
        symbol_type="module",
        start_line=1,
        end_line=total_lines,
        parent_symbol="",
        decorators="",
        parameters="",
        base_classes="",
        return_annotation="",
        has_docstring=bool(ast.get_docstring(tree, clean=True)),
        docstring_first_line=(ast.get_docstring(tree, clean=True) or "").splitlines()[0][:200]
        if ast.get_docstring(tree, clean=True) else "",
        line_count=total_lines,
        complexity_approx=complexity_approx(tree),
        nested_symbols=",".join(
            n.name for n in tree.body
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        ),
    )
    symbols.append(module_symbol)

    def make_call(node: ast.Call, caller_symbol: str, class_ctx: str) -> RawCall:
        func = node.func
        if isinstance(func, ast.Name):
            return RawCall(node.lineno, _unparse_safe(func), func.id, "name", "", caller_symbol, class_ctx)
        if isinstance(func, ast.Attribute):
            simple = func.attr
            if isinstance(func.value, ast.Name):
                base = func.value.id
                kind = "self_attribute" if base == "self" else "attribute_on_name"
                return RawCall(node.lineno, _unparse_safe(func), simple, kind, base, caller_symbol, class_ctx)
            return RawCall(node.lineno, _unparse_safe(func), simple, "other", "", caller_symbol, class_ctx)
        return RawCall(node.lineno, _unparse_safe(func), "", "other", "", caller_symbol, class_ctx)

    def walk_expr_for_calls(expr, scope_qualname: str, class_ctx: str, parent_qualname: str) -> None:
        """Harvest calls from a definition-time expression (a decorator, a
        default argument value, a base-class expression, ...). These
        execute in the *enclosing* scope when the def/class statement
        itself runs, not inside the function/class body's own scope.
        Unlike a body statement, `expr` is not wrapped in an ast.Expr, so
        it can itself directly be the Call node -- walk_body alone would
        only see its children."""
        if expr is None:
            return
        if isinstance(expr, ast.Call):
            raw_calls.append(make_call(expr, scope_qualname, class_ctx))
        walk_body(expr, scope_qualname, class_ctx, parent_qualname)

    def record_import(stmt, scope_qualname: str, active: bool = True) -> None:
        """Record an Import/ImportFrom statement found anywhere in the tree
        -- not just at true module top level. Catches lazy imports inside
        functions/methods, imports under `if TYPE_CHECKING:`, etc., so that
        every import statement is reported per the documented contract.

        Also indexes the record under the scope it lexically belongs to
        (imports_by_scope), so call resolution can treat a function-local
        import as visible only within that function's own scope chain --
        not file-wide.

        `active=False` marks an import lexically inside a branch that is
        statically guaranteed never to execute at runtime (`if False:` /
        `if TYPE_CHECKING:`, see `_is_static_false_test`). It's still
        recorded in the flat `imports` list (and so still reported in
        python_imports.csv per contract), but deliberately withheld from
        `imports_by_scope` so call resolution never treats its binding as
        live -- a name only ever "imported" inside dead code must not be
        confidently resolved through it."""
        new_records = []
        if isinstance(stmt, ast.Import):
            for alias in stmt.names:
                new_records.append(ImportRecord(
                    source_file=rel_path, source_module=source_module, line=stmt.lineno,
                    import_type="import", imported_module=alias.name, imported_name="",
                    alias=alias.asname or "", level=0, resolved_file="", resolution_status="",
                ))
        elif isinstance(stmt, ast.ImportFrom):
            for alias in stmt.names:
                new_records.append(ImportRecord(
                    source_file=rel_path, source_module=source_module, line=stmt.lineno,
                    import_type="from_import", imported_module=stmt.module or "",
                    imported_name=alias.name, alias=alias.asname or "",
                    level=stmt.level or 0, resolved_file="", resolution_status="",
                ))
        imports.extend(new_records)
        if active:
            imports_by_scope.setdefault(scope_qualname, []).extend(new_records)

    def walk_body(node: ast.AST, scope_qualname: str, class_ctx: str, parent_qualname: str,
                  lambda_shadowed: frozenset = frozenset(), import_active: bool = True) -> None:
        for child in ast.iter_child_nodes(node):
            walk_child(child, scope_qualname, class_ctx, parent_qualname, lambda_shadowed, import_active)

    def walk_child(child: ast.AST, scope_qualname: str, class_ctx: str, parent_qualname: str,
                   lambda_shadowed: frozenset, import_active: bool) -> None:
        """Dispatch for a single statement/expression node reached either
        as a direct child during a generic walk_body recursion, or as a
        direct body statement from handle_class/handle_func/the top-level
        loop -- kept as one shared function so both paths agree on what
        counts as a dead `if` branch, a main guard, etc."""
        nonlocal has_main_guard
        if isinstance(child, ast.Call):
            call_rec = make_call(child, scope_qualname, class_ctx)
            # A lambda can't be given its own tracked scope the way a
            # def can (it's anonymous, expression-only), so rather
            # than risk resolving a call to the wrong target when a
            # lambda parameter shadows an outer name, conservatively
            # leave it unresolved.
            shadowed_name = (
                call_rec.callee_simple_name if call_rec.kind == "name" else call_rec.base_name
            )
            if lambda_shadowed and shadowed_name in lambda_shadowed:
                call_rec = RawCall(
                    call_rec.line, call_rec.call_expression, call_rec.callee_simple_name,
                    "other", "", scope_qualname, class_ctx,
                )
            raw_calls.append(call_rec)
            walk_body(child, scope_qualname, class_ctx, parent_qualname, lambda_shadowed, import_active)
        elif isinstance(child, ast.Lambda):
            walk_body(child, scope_qualname, class_ctx, parent_qualname,
                      lambda_shadowed | _lambda_param_names(child), import_active)
        elif isinstance(child, ast.ClassDef):
            handle_class(child, scope_qualname, parent_qualname)
        elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
            handle_func(child, scope_qualname, class_ctx, parent_qualname)
        elif isinstance(child, ast.If) and parent_qualname == MODULE_SYMBOL and _is_main_guard(child):
            has_main_guard = True
            walk_body(child, scope_qualname, class_ctx, parent_qualname, lambda_shadowed, import_active)
        elif isinstance(child, ast.If) and (_is_static_false_test(child.test) or _is_static_true_test(child.test)):
            # A statically-determinable dead branch: imports lexically
            # inside the half that never executes at runtime must not be
            # wired into imports_by_scope (see record_import), even though
            # the *other* half (or the test expression itself, which
            # always evaluates) is handled normally.
            body_active = import_active and not _is_static_false_test(child.test)
            orelse_active = import_active and not _is_static_true_test(child.test)
            walk_child(child.test, scope_qualname, class_ctx, parent_qualname, lambda_shadowed, import_active)
            for stmt in child.body:
                walk_child(stmt, scope_qualname, class_ctx, parent_qualname, lambda_shadowed, body_active)
            for stmt in child.orelse:
                walk_child(stmt, scope_qualname, class_ctx, parent_qualname, lambda_shadowed, orelse_active)
        elif isinstance(child, (ast.Import, ast.ImportFrom)):
            record_import(child, scope_qualname, import_active)
            walk_body(child, scope_qualname, class_ctx, parent_qualname, lambda_shadowed, import_active)
        else:
            walk_body(child, scope_qualname, class_ctx, parent_qualname, lambda_shadowed, import_active)

    def handle_class(node: ast.ClassDef, parent_qualname: str, grandparent_qualname: str) -> None:
        qualname = f"{parent_qualname}.{node.name}" if parent_qualname != MODULE_SYMBOL else node.name
        bases = [_unparse_safe(b) for b in node.bases]
        docstring = ast.get_docstring(node, clean=True)
        rec = SymbolRecord(
            relative_path=rel_path,
            qualified_name=qualname,
            symbol_type="class",
            start_line=node.lineno,
            end_line=getattr(node, "end_lineno", node.lineno),
            parent_symbol=parent_qualname,
            decorators=",".join(_unparse_safe(d) for d in node.decorator_list),
            parameters="",
            base_classes=", ".join(bases),
            return_annotation="",
            has_docstring=bool(docstring),
            docstring_first_line=(docstring or "").splitlines()[0][:200] if docstring else "",
            line_count=(getattr(node, "end_lineno", node.lineno) - node.lineno + 1),
            complexity_approx=complexity_approx(node),
            nested_symbols=",".join(
                n.name for n in node.body
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            ),
        )
        symbols.append(rec)
        if parent_qualname == MODULE_SYMBOL:
            top_level_index[node.name] = qualname
        for dec in node.decorator_list:
            walk_expr_for_calls(dec, parent_qualname, "", parent_qualname)
        for base in node.bases:
            walk_expr_for_calls(base, parent_qualname, "", parent_qualname)
        for kw in node.keywords:
            walk_expr_for_calls(kw.value, parent_qualname, "", parent_qualname)
        info = class_info.setdefault(qualname, ClassInfo(qualified_name=qualname, bases=bases))
        for n in node.body:
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                info.methods.add(n.name)
        for stmt in node.body:
            walk_child(stmt, qualname, qualname, parent_qualname, frozenset(), True)

    def handle_func(node, parent_qualname: str, class_ctx: str, grandparent_qualname: str) -> None:
        qualname = f"{parent_qualname}.{node.name}" if parent_qualname != MODULE_SYMBOL else node.name
        is_async = isinstance(node, ast.AsyncFunctionDef)
        if parent_qualname == MODULE_SYMBOL:
            symbol_type = "async_function" if is_async else "function"
        elif parent_qualname in class_info or grandparent_qualname == parent_qualname:
            symbol_type = "method"
        else:
            symbol_type = "nested_function"
        # A function whose immediate parent is itself a class -> method.
        # We detect that via class_info membership captured before recursing.
        docstring = ast.get_docstring(node, clean=True)
        end_line = getattr(node, "end_lineno", node.lineno)
        rec = SymbolRecord(
            relative_path=rel_path,
            qualified_name=qualname,
            symbol_type=symbol_type,
            start_line=node.lineno,
            end_line=end_line,
            parent_symbol=parent_qualname,
            decorators=",".join(_unparse_safe(d) for d in node.decorator_list),
            parameters=format_params(node.args),
            base_classes="",
            return_annotation=_unparse_safe(node.returns),
            has_docstring=bool(docstring),
            docstring_first_line=(docstring or "").splitlines()[0][:200] if docstring else "",
            line_count=(end_line - node.lineno + 1),
            complexity_approx=complexity_approx(node),
            nested_symbols=",".join(
                n.name for n in node.body
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            ),
        )
        symbols.append(rec)
        if parent_qualname == MODULE_SYMBOL:
            top_level_index[node.name] = qualname
        local_names_by_symbol[qualname] = _collect_local_bound_names(node)
        for dec in node.decorator_list:
            walk_expr_for_calls(dec, parent_qualname, "", parent_qualname)
        for default in list(node.args.defaults) + list(node.args.kw_defaults):
            walk_expr_for_calls(default, parent_qualname, "", parent_qualname)
        # Without `from __future__ import annotations`, annotation
        # expressions execute at definition time too (e.g. `-> register():`).
        all_args = (
            list(getattr(node.args, "posonlyargs", []) or []) + list(node.args.args)
            + list(node.args.kwonlyargs)
            + ([node.args.vararg] if node.args.vararg else [])
            + ([node.args.kwarg] if node.args.kwarg else [])
        )
        for a in all_args:
            walk_expr_for_calls(a.annotation, parent_qualname, "", parent_qualname)
        walk_expr_for_calls(node.returns, parent_qualname, "", parent_qualname)
        # class_ctx passed through for self-resolution of nested defs.
        effective_class_ctx = class_ctx if symbol_type in ("method",) else (class_ctx if symbol_type == "nested_function" else "")
        for stmt in node.body:
            walk_child(stmt, qualname, effective_class_ctx, parent_qualname, frozenset(), True)

    for stmt in tree.body:
        walk_child(stmt, MODULE_SYMBOL, "", MODULE_SYMBOL, frozenset(), True)

    parent_of = {s.qualified_name: s.parent_symbol for s in symbols}
    return PyFileAnalysis(
        symbols=symbols, imports=imports, raw_calls=raw_calls,
        has_main_guard=has_main_guard, top_level_index=top_level_index,
        class_info=class_info, local_names_by_symbol=local_names_by_symbol,
        parent_of=parent_of, imports_by_scope=imports_by_scope,
        module_reassigned_names=_collect_module_reassigned_names(tree),
    )


def resolve_import_record(imp: ImportRecord, module_index: dict, all_top_level_index: dict) -> tuple[str, str]:
    """Conservative import resolution.

    For `import X`, X must match exactly one scanned file's dotted module
    path. For `from X import Y`, Y may statically refer to either an
    attribute defined inside module X, or a submodule `X.Y` -- both are
    valid Python and we can't execute code to tell them apart. We prefer
    whichever interpretation has exactly one candidate; if X defines a
    top-level symbol named Y *and* a submodule X.Y also exists, that's a
    genuine ambiguity and is reported as such rather than guessed.
    """
    if imp.import_type == "import":
        candidates = module_index.get(imp.imported_module, [])
        if len(candidates) == 1:
            return candidates[0], "resolved"
        if len(candidates) == 0:
            return "", "unresolved_external_or_missing"
        return "", "ambiguous"

    source_dir_parts = imp.source_file.split("/")[:-1]
    if imp.level and imp.level > 0:
        trim = max(0, len(source_dir_parts) - (imp.level - 1))
        base_parts = source_dir_parts[:trim]
        target_module = ".".join(base_parts + ([imp.imported_module] if imp.imported_module else []))
    else:
        target_module = imp.imported_module or ""

    module_candidates = module_index.get(target_module, []) if target_module else []
    submodule_path = f"{target_module}.{imp.imported_name}" if target_module else imp.imported_name
    submodule_candidates = module_index.get(submodule_path, []) if imp.imported_name else []

    if len(module_candidates) > 1 or len(submodule_candidates) > 1:
        return "", "ambiguous"

    if len(module_candidates) == 1 and len(submodule_candidates) == 1:
        target_file = module_candidates[0]
        is_attr = imp.imported_name in all_top_level_index.get(target_file, {})
        if is_attr:
            return "", "ambiguous"
        return submodule_candidates[0], "resolved"

    if len(submodule_candidates) == 1:
        return submodule_candidates[0], "resolved"

    if len(module_candidates) == 1:
        return module_candidates[0], "resolved"

    return "", "unresolved_external_or_missing"


def build_import_bindings(imports: list) -> dict:
    """local_name -> [(line, target_file, target_symbol, kind), ...], sorted
    by line. A list rather than a single overwritten value, so that
    re-importing the same local name later in the same scope (`from a
    import f; f(); from b import f; f()`) doesn't retroactively change
    which binding an earlier call resolves through -- the caller picks
    the entry active at its own line (see _lookup_in_scope_chain)."""
    bindings: dict = {}
    for imp in imports:
        if imp.resolution_status != "resolved":
            continue
        if imp.import_type == "import":
            if imp.alias:
                local = imp.alias
            elif "." not in imp.imported_module:
                local = imp.imported_module
            else:
                continue
            entry = (imp.line, imp.resolved_file, "", "module_alias")
        else:
            local = imp.alias or imp.imported_name
            entry = (imp.line, imp.resolved_file, imp.imported_name, "imported_name")
        bindings.setdefault(local, []).append(entry)
    for entries in bindings.values():
        entries.sort(key=lambda e: e[0])
    return bindings


def build_bindings_by_scope(imports_by_scope: dict) -> dict:
    """qualified scope name -> import-bindings dict for imports declared
    directly in that scope (see build_import_bindings). Keeping this
    per-scope, rather than one file-wide dict, is what stops a
    function-local `import` from being treated as visible to unrelated
    functions elsewhere in the same file."""
    return {scope: build_import_bindings(recs) for scope, recs in imports_by_scope.items()}


def _lookup_in_scope_chain(name: str, caller_symbol: str, parent_of: dict,
                            local_names_by_symbol: dict, bindings_by_scope: dict,
                            class_info: dict, call_line: int):
    """Walk caller_symbol's lexical scope chain (its own scope, then each
    enclosing function/module scope) looking for `name`. Returns
    ("shadowed", None) if a parameter/local variable binds it first,
    ("import", binding_tuple) if an import binds it first, or (None, None)
    if neither -- in which case the caller should fall back to
    module-level top_level_index (same-module function/class defs, which
    aren't scope-restricted the way local imports are).

    A class body's own bindings only count for calls made directly in
    that class body (the first scope checked); they're skipped while
    climbing past a class on the way to an outer scope, matching real
    Python scoping (methods/nested defs don't see class-body names).

    If `name` is imported more than once in the same scope (e.g. `from a
    import f; f(); from b import f; f()`), the binding used is whichever
    import's line is the latest one at or before call_line -- so an
    earlier call keeps resolving through the import that was actually
    active for it, not whichever import happens to appear last in the
    file. If every import of `name` in this scope comes *after*
    call_line, Python still treats `name` as local to the whole scope
    (import is just an assignment), so this is "used before bound"
    rather than a hit in an enclosing scope -- reported as shadowed."""
    current = caller_symbol
    seen = set()
    is_own_scope = True
    while current and current not in seen:
        seen.add(current)
        local_names = local_names_by_symbol.get(current)
        if local_names and name in local_names:
            return "shadowed", None
        if is_own_scope or current not in class_info:
            scope_bindings = bindings_by_scope.get(current)
            entries = scope_bindings.get(name) if scope_bindings else None
            if entries:
                active = None
                for line, target_file, target_symbol, kind in entries:
                    if line <= call_line:
                        active = (target_file, target_symbol, kind)
                    else:
                        break
                if active is not None:
                    return "import", active
                return "shadowed", None
        is_own_scope = False
        current = parent_of.get(current)
    return None, None


def resolve_calls(raw_calls: list, caller_file: str, top_level_index: dict,
                   class_info: dict, bindings_by_scope: dict,
                   all_top_level_index: dict, all_class_info: dict,
                   local_names_by_symbol: dict, parent_of: dict,
                   module_reassigned_names: set) -> list:
    results = []
    for rc in raw_calls:
        candidate_file = ""
        candidate_symbol = ""
        confidence = "unresolved"
        explanation = ""

        if rc.kind == "name":
            name = rc.callee_simple_name
            lookup_kind, binding = _lookup_in_scope_chain(
                name, rc.caller_symbol, parent_of, local_names_by_symbol, bindings_by_scope, class_info, rc.line,
            )
            if lookup_kind == "shadowed":
                explanation = (
                    f"'{name}' is shadowed by a parameter or local variable in the enclosing "
                    f"scope; not statically resolvable to the module-level definition"
                )
            elif lookup_kind == "import":
                target_file, target_symbol, kind = binding
                if target_file and target_file in all_top_level_index and name_in_index(
                        all_top_level_index[target_file], target_symbol if kind == "imported_name" else name):
                    lookup_name = target_symbol if kind == "imported_name" else name
                    candidate_file = target_file
                    candidate_symbol = all_top_level_index[target_file][lookup_name]
                    is_class = candidate_symbol in all_class_info.get(target_file, {})
                    confidence = "medium"
                    explanation = (
                        f"resolved via unambiguous import binding '{name}'"
                        + (" (constructor call)" if is_class else "")
                    )
                else:
                    explanation = f"name '{name}' is bound by an import that could not be resolved to a single repo file"
            elif name in top_level_index and name in module_reassigned_names:
                explanation = (
                    f"'{name}' is reassigned elsewhere at module level (in addition to its def), "
                    f"so which definition is bound at this call site can't be determined statically"
                )
            elif name in top_level_index:
                candidate_file = caller_file
                candidate_symbol = top_level_index[name]
                is_class = candidate_symbol in class_info
                confidence = "high"
                explanation = (
                    "constructor call to class defined in the same module"
                    if is_class else "direct call to a function defined in the same module"
                )
            else:
                explanation = "name not defined in this module and not bound via a resolvable import"

        elif rc.kind == "self_attribute":
            method = rc.callee_simple_name
            ctx = rc.class_context
            info = class_info.get(ctx)
            if info and method in info.methods:
                candidate_file = caller_file
                candidate_symbol = f"{ctx}.{method}"
                confidence = "high"
                explanation = "self.<method>() resolved within the enclosing class body"
            elif info:
                found = False
                for base in info.bases:
                    if base in top_level_index and top_level_index[base] in class_info:
                        base_qual = top_level_index[base]
                        if method in class_info[base_qual].methods:
                            candidate_file = caller_file
                            candidate_symbol = f"{base_qual}.{method}"
                            confidence = "medium"
                            explanation = f"resolved via base class '{base}' defined in the same file"
                            found = True
                            break
                    else:
                        # `base` may be a plain name (`Alias`, possibly
                        # itself an import alias) or a module-qualified
                        # expression (`base.Parent` from `import base`).
                        # The scope-bound identifier to resolve is always
                        # the part before the last dot; the class name to
                        # look for in the target file is whatever's left.
                        if "." in base:
                            module_part, dotted_class_name = base.rsplit(".", 1)
                        else:
                            module_part, dotted_class_name = base, base
                        base_lookup_kind, base_binding = _lookup_in_scope_chain(
                            module_part, parent_of.get(ctx, ctx), parent_of, local_names_by_symbol,
                            bindings_by_scope, class_info, rc.line,
                        )
                        if base_lookup_kind == "import":
                            target_file, target_symbol, kind = base_binding
                            # For `from base import Parent as Alias`, the
                            # binding's target_symbol is the real name
                            # ("Parent") -- `base` is only the local alias
                            # ("Alias") used in this file's class header,
                            # and won't match anything in the target file.
                            # For `import base; class C(base.Parent)`,
                            # there's no from-import target_symbol -- use
                            # the class name split off of `base` itself.
                            lookup_name = target_symbol if (kind == "imported_name" and target_symbol) else dotted_class_name
                            base_info_map = all_class_info.get(target_file, {})
                            base_qual = None
                            for qn in base_info_map:
                                if qn.split(".")[-1] == lookup_name:
                                    base_qual = qn
                                    break
                            if base_qual and method in base_info_map[base_qual].methods:
                                candidate_file = target_file
                                candidate_symbol = f"{base_qual}.{method}"
                                confidence = "medium"
                                explanation = (
                                    f"resolved via imported base class '{base}'"
                                    + (f" (imported as '{lookup_name}')" if lookup_name != base else "")
                                )
                                found = True
                                break
                if not found:
                    explanation = "self.<method>() not found on the class or its statically known bases"
            else:
                explanation = "self.<method>() used outside a statically known class context"

        elif rc.kind == "attribute_on_name":
            base = rc.base_name
            base_lookup_kind, base_binding = _lookup_in_scope_chain(
                base, rc.caller_symbol, parent_of, local_names_by_symbol, bindings_by_scope, class_info, rc.line,
            )
            if base_lookup_kind == "shadowed":
                explanation = f"'{base}' is shadowed by a parameter or local variable in the enclosing scope"
            elif base_lookup_kind == "import":
                target_file, _, kind = base_binding
                if kind == "module_alias" and target_file and target_file in all_top_level_index:
                    idx = all_top_level_index[target_file]
                    if rc.callee_simple_name in idx:
                        candidate_file = target_file
                        candidate_symbol = idx[rc.callee_simple_name]
                        confidence = "medium"
                        explanation = f"resolved via module alias '{base}.{rc.callee_simple_name}'"
                    else:
                        explanation = f"'{base}' resolves to a repo module but '{rc.callee_simple_name}' is not a known top-level symbol there"
                else:
                    explanation = f"'{base}' is not bound to an unambiguous module import"
            else:
                explanation = "attribute call target is not a statically known module alias (possible instance method dispatch)"
        else:
            explanation = "dynamic or unsupported call expression; not statically resolvable"

        results.append(CallRecord(
            caller_file=caller_file, caller_symbol=rc.caller_symbol, line=rc.line,
            call_expression=rc.call_expression, callee_simple_name=rc.callee_simple_name,
            candidate_file=candidate_file, candidate_symbol=candidate_symbol,
            confidence=confidence, explanation=explanation,
        ))
    return results


def name_in_index(index: dict, name: str) -> bool:
    return bool(name) and name in index
