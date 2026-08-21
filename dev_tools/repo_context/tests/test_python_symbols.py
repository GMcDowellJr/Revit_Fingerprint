import csv

from conftest import run_tool, write_files

SOURCE = '''"""Module docstring."""


def top_func(a, b=2):
    """Top func doc."""
    def inner(c):
        return c + 1
    return inner(a) + b


class Thing:
    """A thing."""

    def method_one(self):
        return 1

    async def method_two(self):
        return 2


async def top_async(x):
    return x
'''


def _symbols(out):
    with open(out / "python_symbols.csv", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_functions_classes_methods_nested_async(repo, out):
    write_files(repo, {"mod.py": SOURCE})
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    rows = {r["qualified_name"]: r for r in _symbols(out)}

    assert rows["<module>"]["symbol_type"] == "module"
    assert rows["<module>"]["has_docstring"] == "true"

    assert rows["top_func"]["symbol_type"] == "function"
    assert rows["top_func"]["parent_symbol"] == "<module>"
    assert "inner" in rows["top_func"]["nested_symbols"]

    assert rows["top_func.inner"]["symbol_type"] == "nested_function"
    assert rows["top_func.inner"]["parent_symbol"] == "top_func"

    assert rows["Thing"]["symbol_type"] == "class"
    assert rows["Thing"]["has_docstring"] == "true"

    assert rows["Thing.method_one"]["symbol_type"] == "method"
    assert rows["Thing.method_two"]["symbol_type"] == "method"

    assert rows["top_async"]["symbol_type"] == "async_function"

    # line ranges are sane and end >= start
    for r in rows.values():
        assert int(r["end_line"]) >= int(r["start_line"])
        assert int(r["complexity_approx"]) >= 1


def test_utf8_bom_prefixed_python_file_still_parses(repo, out):
    (repo / "a.py").parent.mkdir(parents=True, exist_ok=True)
    (repo / "a.py").write_bytes(b"\xef\xbb\xbf" + b"def f():\n    return 1\n")
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    with open(out / "parse_warnings.csv", newline="", encoding="utf-8") as fh:
        warnings = list(csv.DictReader(fh))
    assert warnings == []

    rows = {r["qualified_name"] for r in _symbols(out)}
    assert "f" in rows


def test_syntax_error_handling_does_not_abort_scan(repo, out):
    write_files(repo, {
        "broken.py": "def broken(:\n    pass\n",
        "ok.py": "def fine():\n    return 1\n",
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    with open(out / "parse_warnings.csv", newline="", encoding="utf-8") as fh:
        warnings = list(csv.DictReader(fh))
    assert any(w["relative_path"] == "broken.py" for w in warnings)

    with open(out / "file_inventory.csv", newline="", encoding="utf-8") as fh:
        inventory = {r["relative_path"]: r for r in csv.DictReader(fh)}
    assert inventory["broken.py"]["parse_status"] == "failed"
    assert inventory["ok.py"]["parse_status"] == "ok"

    rows = {r["qualified_name"]: r for r in _symbols(out) if r["relative_path"] == "ok.py"}
    assert "fine" in rows
