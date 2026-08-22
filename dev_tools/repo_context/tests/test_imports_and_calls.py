import csv

from conftest import run_tool, write_files


def _read(out, name):
    with open(out / name, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_absolute_and_relative_imports(repo, out):
    write_files(repo, {
        "pkg/__init__.py": "",
        "pkg/sub/__init__.py": "",
        "pkg/sub/leaf.py": "def leaf_func():\n    return 1\n",
        "pkg/mid.py": (
            "from .sub.leaf import leaf_func\n"
            "from . import mid_sibling\n"
            "import pkg.sub.leaf\n"
        ),
        "pkg/mid_sibling.py": "x = 1\n",
        "top.py": "import pkg.sub.leaf as leafmod\n",
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    imports = _read(out, "python_imports.csv")
    by_line = {(r["source_file"], r["line"]): r for r in imports}

    rel_leaf = by_line[("pkg/mid.py", "1")]
    assert rel_leaf["resolution_status"] == "resolved"
    assert rel_leaf["resolved_file"] == "pkg/sub/leaf.py"

    rel_sibling = by_line[("pkg/mid.py", "2")]
    assert rel_sibling["resolution_status"] == "resolved"
    assert rel_sibling["resolved_file"] == "pkg/mid_sibling.py"

    abs_import = by_line[("pkg/mid.py", "3")]
    assert abs_import["resolution_status"] == "resolved"
    assert abs_import["resolved_file"] == "pkg/sub/leaf.py"

    top_import = by_line[("top.py", "1")]
    assert top_import["resolution_status"] == "resolved"
    assert top_import["resolved_file"] == "pkg/sub/leaf.py"


def test_same_module_and_imported_function_calls(repo, out):
    write_files(repo, {
        "lib.py": "def helper():\n    return 1\n",
        "app.py": (
            "from lib import helper\n\n"
            "def local():\n"
            "    return 1\n\n"
            "def run():\n"
            "    a = local()\n"
            "    b = helper()\n"
            "    return a + b\n"
        ),
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    calls = _read(out, "python_calls.csv")
    by_expr = {(r["caller_file"], r["call_expression"]): r for r in calls}

    same_module = by_expr[("app.py", "local")]
    assert same_module["confidence"] == "high"
    assert same_module["candidate_file"] == "app.py"
    assert same_module["candidate_symbol"] == "local"

    via_import = by_expr[("app.py", "helper")]
    assert via_import["confidence"] == "medium"
    assert via_import["candidate_file"] == "lib.py"
    assert via_import["candidate_symbol"] == "helper"


def test_unresolved_call_is_preserved_not_guessed(repo, out):
    write_files(repo, {
        "app.py": (
            "def run(obj):\n"
            "    obj.dynamic_method()\n"
            "    unknown_function()\n"
        ),
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    calls = _read(out, "python_calls.csv")
    by_expr = {r["call_expression"]: r for r in calls}
    assert by_expr["unknown_function"]["confidence"] == "unresolved"
    assert by_expr["obj.dynamic_method"]["confidence"] == "unresolved"
    for r in (by_expr["unknown_function"], by_expr["obj.dynamic_method"]):
        assert r["candidate_file"] == ""
        assert r["candidate_symbol"] == ""
        assert r["explanation"]


def test_ambiguous_import_resolution(repo, out):
    # Both a/b.py and a/b/__init__.py reduce to the dotted module path "a.b".
    write_files(repo, {
        "a/b.py": "def f():\n    return 1\n",
        "a/b/__init__.py": "def g():\n    return 2\n",
        "user.py": "import a.b\n",
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    imports = _read(out, "python_imports.csv")
    row = next(r for r in imports if r["source_file"] == "user.py")
    assert row["resolution_status"] == "ambiguous"
    assert row["resolved_file"] == ""


def test_imports_in_nested_scopes_are_recorded(repo, out):
    write_files(repo, {
        "a.py": (
            "from typing import TYPE_CHECKING\n\n"
            "if TYPE_CHECKING:\n"
            "    import json as type_only_json\n\n\n"
            "def run():\n"
            "    import os\n"
            "    return os.getcwd()\n\n\n"
            "class Widget:\n"
            "    def method(self):\n"
            "        from collections import OrderedDict\n"
            "        return OrderedDict()\n"
        ),
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    imports = _read(out, "python_imports.csv")
    by_line = {int(r["line"]): r for r in imports}
    assert by_line[1]["imported_name"] == "TYPE_CHECKING"
    assert by_line[4]["alias"] == "type_only_json"
    assert by_line[8]["imported_module"] == "os"
    assert by_line[14]["imported_name"] == "OrderedDict"


def test_call_shadowed_by_parameter_is_not_resolved_to_module_function(repo, out):
    write_files(repo, {
        "a.py": (
            "def target():\n"
            "    return 1\n\n\n"
            "def caller(target):\n"
            "    return target()\n"
        ),
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    calls = _read(out, "python_calls.csv")
    row = next(r for r in calls if r["call_expression"] == "target")
    assert row["confidence"] == "unresolved"
    assert row["candidate_symbol"] == ""
    assert "shadow" in row["explanation"].lower()


def test_function_local_import_does_not_leak_into_unrelated_function(repo, out):
    write_files(repo, {
        "lib.py": "def helper():\n    return 1\n",
        "a.py": (
            "def a():\n"
            "    from lib import helper\n"
            "    return helper()\n\n\n"
            "def b():\n"
            "    return helper()\n"
        ),
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    calls = _read(out, "python_calls.csv")
    row_a = next(r for r in calls if r["caller_symbol"] == "a")
    row_b = next(r for r in calls if r["caller_symbol"] == "b")
    assert row_a["confidence"] == "medium"
    assert row_a["candidate_file"] == "lib.py"
    assert row_b["confidence"] == "unresolved"
    assert row_b["candidate_file"] == ""


def test_nested_def_name_shadows_module_level_symbol_throughout_function(repo, out):
    # Python scoping isn't source-order-sensitive: a name def'd anywhere in
    # a function is local to that function for its entire body.
    write_files(repo, {
        "a.py": (
            "def target():\n"
            "    return 1\n\n\n"
            "def outer():\n"
            "    target()\n\n"
            "    def target():\n"
            "        return 2\n"
        ),
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    calls = _read(out, "python_calls.csv")
    row = next(r for r in calls if r["caller_symbol"] == "outer")
    assert row["confidence"] == "unresolved"
    assert "shadow" in row["explanation"].lower()


def test_aliased_base_class_import_resolves_inherited_method(repo, out):
    write_files(repo, {
        "base.py": "class Parent:\n    def inherited(self):\n        return 1\n",
        "a.py": (
            "from base import Parent as Alias\n\n\n"
            "class Child(Alias):\n"
            "    def run(self):\n"
            "        return self.inherited()\n"
        ),
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    calls = _read(out, "python_calls.csv")
    row = next(r for r in calls if r["call_expression"] == "self.inherited")
    assert row["confidence"] == "medium"
    assert row["candidate_file"] == "base.py"
    assert row["candidate_symbol"] == "Parent.inherited"


def test_module_level_rebinding_is_not_confidently_resolved(repo, out):
    write_files(repo, {
        "a.py": (
            "def target():\n"
            "    return 1\n\n\n"
            "def factory():\n"
            "    return 2\n\n\n"
            "target = factory()\n"
            "target()\n"
        ),
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    calls = _read(out, "python_calls.csv")
    factory_call = next(r for r in calls if r["call_expression"] == "factory")
    target_call = next(r for r in calls if r["call_expression"] == "target")
    assert factory_call["confidence"] == "high"
    assert target_call["confidence"] == "unresolved"
    assert "reassigned" in target_call["explanation"].lower()


def test_module_qualified_base_class_resolves_inherited_method(repo, out):
    write_files(repo, {
        "base.py": "class Parent:\n    def inherited(self):\n        return 1\n",
        "a.py": (
            "import base\n\n\n"
            "class Child(base.Parent):\n"
            "    def run(self):\n"
            "        return self.inherited()\n"
        ),
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    calls = _read(out, "python_calls.csv")
    row = next(r for r in calls if r["call_expression"] == "self.inherited")
    assert row["confidence"] == "medium"
    assert row["candidate_file"] == "base.py"
    assert row["candidate_symbol"] == "Parent.inherited"


def test_duplicate_import_resolved_by_call_site_order(repo, out):
    write_files(repo, {
        "a.py": "def f():\n    return 1\n",
        "b.py": "def f():\n    return 2\n",
        "main.py": "from a import f\nf()\nfrom b import f\nf()\n",
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    calls = _read(out, "python_calls.csv")
    by_line = {int(r["line"]): r for r in calls if r["call_expression"] == "f"}
    assert by_line[2]["candidate_file"] == "a.py"
    assert by_line[4]["candidate_file"] == "b.py"


def test_definition_time_calls_in_decorators_defaults_and_annotations(repo, out):
    write_files(repo, {
        "a.py": (
            "def register():\n"
            "    return int\n\n\n"
            "def f(value: int) -> register():\n"
            "    return value\n"
        ),
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    calls = _read(out, "python_calls.csv")
    row = next(r for r in calls if r["call_expression"] == "register")
    assert row["confidence"] == "high"
    assert row["candidate_symbol"] == "register"
    assert row["caller_symbol"] == "<module>"  # executes when `def f` is evaluated, not inside f


def test_lambda_parameter_shadowing_is_not_confidently_resolved(repo, out):
    write_files(repo, {
        "a.py": "def factory():\n    return 1\n\n\nfn = lambda factory: factory()\n",
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    calls = _read(out, "python_calls.csv")
    row = next(r for r in calls if r["call_expression"] == "factory")
    assert row["confidence"] == "unresolved"
    assert row["candidate_symbol"] == ""


def test_import_inside_dead_if_false_branch_does_not_activate_call_resolution(repo, out):
    write_files(repo, {
        "lib.py": "def helper():\n    return 1\n",
        "a.py": (
            "if False:\n"
            "    from lib import helper\n\n"
            "helper()\n"
        ),
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    calls = _read(out, "python_calls.csv")
    row = next(r for r in calls if r["call_expression"] == "helper")
    assert row["confidence"] == "unresolved"
    assert row["candidate_file"] == ""

    # Still reported in the flat import list per contract, even though it
    # never activates a runtime binding.
    imports = _read(out, "python_imports.csv")
    assert any(r["source_file"] == "a.py" and r["imported_name"] == "helper" for r in imports)


def test_import_inside_type_checking_branch_does_not_activate_call_resolution(repo, out):
    write_files(repo, {
        "lib.py": "def helper():\n    return 1\n",
        "a.py": (
            "from typing import TYPE_CHECKING\n\n"
            "if TYPE_CHECKING:\n"
            "    from lib import helper\n\n"
            "helper()\n"
        ),
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    calls = _read(out, "python_calls.csv")
    row = next(r for r in calls if r["call_expression"] == "helper")
    assert row["confidence"] == "unresolved"
    assert row["candidate_file"] == ""


def test_import_in_live_else_of_dead_if_false_branch_still_resolves(repo, out):
    write_files(repo, {
        "lib.py": "def helper():\n    return 1\n",
        "a.py": (
            "if False:\n"
            "    pass\n"
            "else:\n"
            "    from lib import helper\n\n"
            "helper()\n"
        ),
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    calls = _read(out, "python_calls.csv")
    row = next(r for r in calls if r["call_expression"] == "helper")
    assert row["confidence"] == "medium"
    assert row["candidate_file"] == "lib.py"


def test_comprehension_target_does_not_shadow_module_level_symbol(repo, out):
    write_files(repo, {
        "a.py": (
            "def target():\n    return 1\n\n\n"
            "def outer(xs):\n"
            "    [target for target in xs]\n"
            "    return target()\n"
        ),
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    calls = _read(out, "python_calls.csv")
    row = next(r for r in calls if r["caller_symbol"] == "outer" and r["call_expression"] == "target")
    assert row["confidence"] == "high"
    assert row["candidate_symbol"] == "target"


def test_self_method_call_within_known_class(repo, out):
    write_files(repo, {
        "widget.py": (
            "class Widget:\n"
            "    def helper(self):\n"
            "        return 1\n\n"
            "    def run(self):\n"
            "        return self.helper()\n"
        ),
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    calls = _read(out, "python_calls.csv")
    row = next(r for r in calls if r["call_expression"] == "self.helper")
    assert row["confidence"] == "high"
    assert row["candidate_symbol"] == "Widget.helper"
