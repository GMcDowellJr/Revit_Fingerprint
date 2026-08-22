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


def test_explicit_selectors_beyond_max_files_is_a_hard_conflict_not_silent_drop(repo, out):
    # Regression: naming more distinct explicit files than limits.max_files
    # allows used to succeed with a partial packet, leaving the resolution
    # report claiming "resolved" for files that were never actually
    # rendered. This must behave like any other explicit-selector conflict:
    # abort, report why, and write no packet at all.
    write_files(repo, {
        "a.py": "def f():\n    return 1\n",
        "b.py": "def g():\n    return 2\n",
    })
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
        "limits": {"max_estimated_tokens": 12000, "max_files": 1},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 1
    assert "max_files" in result.stderr
    assert not (out / "packets" / "packet_req.md").exists()


def test_strict_mode_catches_unresolved_search_terms(repo, out):
    # Regression: search terms weren't part of all_resolutions at all, so
    # strict mode couldn't abort on a zero-match term or an invalid regex.
    write_files(repo, {"a.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q", "strict": True,
        "selectors": {"files": [], "symbols": [], "search_terms": ["no_such_term_anywhere"], "lines": []},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 1
    assert "strict mode" in result.stderr
    assert not (out / "packets" / "packet_req.md").exists()

    req2 = _request(out, "req2.json", {
        "schema_version": "1.0", "question": "q", "strict": True,
        "selectors": {"files": [], "symbols": [], "search_terms": ["("], "lines": []},
        "expansion": {"search_as_regex": True},
    })
    result2 = _packet(repo, out, req2)
    assert result2.returncode == 1
    assert "strict mode" in result2.stderr


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


def test_whole_file_symbol_listing_is_charged_against_budget(repo, out):
    # Regression: the "Top-level symbols:" listing for an explicit whole-
    # file selector used to be appended without any budget accounting, so
    # a file with many top-level definitions could blow past
    # limits.max_estimated_tokens while the packet reported far less
    # usage than it actually rendered.
    lines = []
    for i in range(300):
        lines += [f"def func_{i}():", f"    return {i}", ""]
    write_files(repo, {"big.py": "\n".join(lines) + "\n"})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["big.py"], "symbols": [], "search_terms": [], "lines": []},
        "limits": {"max_estimated_tokens": 20000, "max_files": 12},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))

    # The reported estimated-token usage must not understate the packet's
    # actual rendered size by a wide margin (the symbol-listing bug made
    # this true even though every "func_i" line is metadata, not excerpt).
    assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
    assert text.count("func_") <= 300 * 2  # listing entries + (bounded) excerpt lines only, no runaway duplication
    assert "Omitted" in text or text.count("(function, lines") <= 300


def test_search_terms_share_a_single_global_max_files_cap(repo, out):
    # Regression: max_files was previously enforced per search term (a
    # fresh `shown_files` set each iteration), so two different terms
    # matching two different files could each individually stay "within"
    # limits.max_files while the combined focus-file set exceeded it.
    write_files(repo, {
        "a.py": "def f():\n    return 'alpha_needle'\n",
        "b.py": "def g():\n    return 'beta_needle'\n",
    })
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [], "search_terms": ["alpha_needle", "beta_needle"], "lines": []},
        "limits": {"max_estimated_tokens": 12000, "max_files": 1},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
    assert len(sidecar["focus_files"]) <= 1
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "Omitted" in text or "omitted beyond limits.max_files" in text


def _git_init_commit(repo) -> str:
    import subprocess
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
    subprocess.run(["git", "-c", "user.email=t@t.com", "-c", "user.name=t", "commit", "-qm", "init"],
                   cwd=repo, check=True)
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
                           ).stdout.strip()


def test_include_graphify_expansion_lists_revision_aligned_community_peers(repo, out):
    write_files(repo, {
        "core/a.py": "def f():\n    return 1\n",
        "core/b.py": "def g():\n    return 2\n",
    })
    commit = _git_init_commit(repo)
    graph = {
        "built_at_commit": commit,
        "nodes": [
            {"source_file": "core/a.py", "community": 5, "community_name": "Widgets"},
            {"source_file": "core/b.py", "community": 5, "community_name": "Widgets"},
        ],
    }
    (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
    (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
        "expansion": {"include_graphify": True},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "graphify_expansion" in text
    assert "core/b.py" in text


def test_include_graphify_withheld_when_current_commit_unavailable(repo, out):
    # Regression: when the scanned tree isn't a git repository (no HEAD
    # commit to check against), a graphify-out/graph.json with any
    # built_at_commit used to be accepted unconditionally instead of
    # being withheld -- revision alignment can't be proven either way.
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    graph = {
        "built_at_commit": "deadbeef",
        "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
    }
    (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
    (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
    _scan(repo, out)  # no git init -- current commit is unavailable
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
        "expansion": {"include_graphify": True},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "graphify_expansion" not in text
    assert "revision alignment cannot be proven" in text


def test_selector_resolution_report_is_charged_against_budget(repo, out):
    # Regression: the "Selector resolution report" section (one entry per
    # requested selector, however many) was appended without any budget
    # accounting, so a request naming hundreds of missing/ambiguous
    # selectors could produce a large packet while reporting ~0 estimated
    # tokens used under a tiny limits.max_estimated_tokens.
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    missing_files = [f"missing_{i}.py" for i in range(200)]
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": missing_files, "symbols": [], "search_terms": [], "lines": []},
        "limits": {"max_estimated_tokens": 1, "max_files": 500},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    # With a 4-character budget, almost nothing should have rendered --
    # certainly not a full 200-entry resolution report.
    assert len(text) < 2000
    assert text.count("missing_") < 200
    assert "Estimated tokens used: ~0" in text


def test_related_test_expansion_respects_global_max_files(repo, out):
    # Regression: related-test expansion appended directly to focus_files
    # under a hard-coded 10,000 ceiling instead of going through the same
    # note_focus_file() gate as every other tier, so it could silently
    # exceed limits.max_files.
    write_files(repo, {
        "core/a.py": "def f():\n    return 1\n",
        "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 1\n",
    })
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
        "expansion": {"include_related_tests": True},
        "limits": {"max_estimated_tokens": 12000, "max_files": 1},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
    assert sidecar["focus_files"] == ["core/a.py"]
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "omitted: limits.max_files" in text


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
