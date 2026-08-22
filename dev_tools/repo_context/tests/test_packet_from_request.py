import json

from conftest import run_tool, write_files


def _scan(repo, out):
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr


def _request(out, name, data):
    path = out / name
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def _packet(repo, out, request_path, extra=None):
    return run_tool(["packet", str(repo), "--output", str(out), "--request", str(request_path)] + (extra or []))


def test_valid_request_resolves_file_and_symbol_selectors(repo, out):
    write_files(repo, {
        "core/helper.py": "def add(a, b):\n    return a + b\n",
        "tools/report.py": "from core.helper import add\n\n\ndef build():\n    return add(1, 2)\n",
    })
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "How is build computed?",
        "selectors": {"files": [], "symbols": [{"name": "build"}], "search_terms": [], "lines": []},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "resolved" in text
    assert "def build():" in text
    assert "explicit_symbol_selector" in text
    assert "caller_expansion" not in text  # nothing calls build()
    assert "callee_expansion" in text  # build() calls add()


def test_ambiguous_symbol_is_reported_not_silently_resolved(repo, out):
    write_files(repo, {
        "core/a.py": "def dup():\n    return 1\n",
        "core/b.py": "def dup():\n    return 2\n",
    })
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "what does dup do",
        "selectors": {"files": [], "symbols": [{"name": "dup"}], "search_terms": [], "lines": []},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
    [res] = sidecar["resolution_report"]
    assert res["status"] == "ambiguous"
    assert len(res["candidates"]) == 2
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "core/a.py" in text and "core/b.py" in text  # both candidates surfaced


def test_qualified_symbol_via_file_field_resolves_unambiguously(repo, out):
    write_files(repo, {
        "core/a.py": "def dup():\n    return 1\n",
        "core/b.py": "def dup():\n    return 2\n",
    })
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "what does dup do",
        "selectors": {"files": [], "symbols": [{"name": "dup", "file": "core/b.py"}], "search_terms": [], "lines": []},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "return 2" in text
    assert "return 1" not in text


def test_missing_selector_is_reported_but_other_selectors_still_processed(repo, out):
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["core/a.py"], "symbols": [{"name": "does_not_exist"}], "search_terms": [], "lines": []},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "def f():" in text  # the valid file selector still got processed
    assert "missing" in text
    assert "does_not_exist" in text


def test_strict_mode_aborts_on_any_unresolved_selector(repo, out):
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q", "strict": True,
        "selectors": {"files": ["core/a.py"], "symbols": [{"name": "does_not_exist"}], "search_terms": [], "lines": []},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 1
    assert "strict mode" in result.stderr
    assert not (out / "packets" / "packet_req.md").exists()


def test_hard_budget_conflict_on_explicit_selector_aborts_without_partial_packet(repo, out):
    lines = []
    for i in range(300):
        lines += [f"def func_{i}():", f"    return {i}", ""]
    write_files(repo, {"big.py": "\n".join(lines) + "\n"})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["big.py"], "symbols": [], "search_terms": [], "lines": []},
        "limits": {"max_estimated_tokens": 1, "max_files": 12},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 1
    assert "do not fit" in result.stderr
    assert not (out / "packets" / "packet_req.md").exists()


def test_invalid_schema_version_is_rejected_before_resolution(repo, out):
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "0.1", "question": "q",
        "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 1
    assert "invalid packet_request.json" in result.stderr
    assert not (out / "packets" / "packet_req.md").exists()


def test_path_traversal_selector_is_rejected(repo, out):
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["../outside.py"], "symbols": [], "search_terms": [], "lines": []},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 1
    assert "invalid packet_request.json" in result.stderr


def test_search_term_matches_and_related_tests_are_included(repo, out):
    write_files(repo, {
        "core/a.py": "def f():\n    return 'needle_term'\n",
        "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 'needle_term'\n",
    })
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": ["needle_term"], "lines": []},
        "expansion": {"include_related_tests": True},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "exact_search_match" in text
    assert "tests/test_a.py" in text


def test_line_selector_resolves_enclosing_symbol(repo, out):
    write_files(repo, {"core/a.py": "def f():\n    x = 1\n    return x\n"})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [], "search_terms": [], "lines": [{"file": "core/a.py", "line": 2}]},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "explicit_line_selector" in text
    assert "def f():" in text


def test_stale_source_since_scan_withholds_excerpt(repo, out):
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    write_files(repo, {"core/a.py": "def f():\n    return 999\n"})
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "withheld" in text
    assert "return 999" not in text


def test_resolution_sidecar_json_is_written(repo, out):
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
    assert sidecar["schema_version"] == "1.0"
    assert sidecar["question"] == "q"
    assert sidecar["resolution_report"][0]["status"] == "resolved"


def test_name_override_controls_output_filename(repo, out):
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
    })
    result = _packet(repo, out, req, extra=["--name", "custom_stem"])
    assert result.returncode == 0, result.stderr
    assert (out / "packets" / "packet_custom_stem.md").exists()
