from pathlib import Path

from tools import gen_map


def test_generator_supports_arbitrary_layout(tmp_path: Path) -> None:
    (tmp_path / "odd folder").mkdir()
    (tmp_path / "odd folder" / "first.py").write_text(
        "import json\ndef main():\n    helper()\ndef helper():\n    return json.dumps({})\n",
        encoding="utf-8",
    )
    (tmp_path / "second.py").write_text("def other():\n    return 2\n", encoding="utf-8")

    assert gen_map.main([str(tmp_path), "--output-dir", str(tmp_path / "maps")]) == 0

    code_map = (tmp_path / "maps" / f"{gen_map._slug(tmp_path.name)}_code_map_authoritative.md").read_text()
    symbol_index = (tmp_path / "maps" / f"{gen_map._slug(tmp_path.name)}_symbol_index.md").read_text()
    trace_map = (tmp_path / "maps" / f"{gen_map._slug(tmp_path.name)}_trace_map.md").read_text()
    assert "odd folder/first.py" in code_map
    assert "`other` — `second.py:L1`" in symbol_index
    assert "odd folder/first.py:main` → `odd folder/first.py:helper" in trace_map


def test_trace_does_not_merge_duplicate_function_names(tmp_path: Path) -> None:
    (tmp_path / "one.py").write_text("def main():\n    helper()\ndef helper():\n    return 1\n", encoding="utf-8")
    (tmp_path / "two.py").write_text("def main():\n    return 2\n", encoding="utf-8")
    files, _ = gen_map.build_index(tmp_path)
    output = tmp_path / "trace.md"
    gen_map.write_trace_map(output, "sample", files, 4)
    trace = output.read_text(encoding="utf-8")
    assert trace.count("## Trace: `") == 2
    assert "one.py:main` → `one.py:helper" in trace
    assert "two.py:main` →" not in trace


def test_parse_error_is_reported_without_stopping_other_files(tmp_path: Path) -> None:
    (tmp_path / "good.py").write_text("def run():\n    return 1\n", encoding="utf-8")
    (tmp_path / "broken.py").write_text("def broken(:\n", encoding="utf-8")
    files, warnings = gen_map.build_index(tmp_path)
    assert list(files) == ["good.py"]
    assert len(warnings) == 1
    assert warnings[0].startswith("broken.py: SyntaxError:")
