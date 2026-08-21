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
