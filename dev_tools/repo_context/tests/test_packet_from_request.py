import json

from conftest import run_tool, write_files  # noqa: F401 -- conftest import also puts TOOL_DIR on sys.path
import rc_request as rr


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


def test_expansion_never_preempts_a_later_explicit_selector(repo, out):
    # Regression: expansions (callers/callees/imports/tests) for one
    # explicit symbol were rendered immediately after it and before the
    # *next* explicit selector got its turn, so a budget that easily fits
    # every explicit selector's own content could still fail if an early
    # selector's expansion ate the remaining room first. Explicit content
    # must all be attempted before any expansion spends a single char.
    write_files(repo, {
        "core/a.py": "def h():\n    return 42\n\n\ndef f():\n    return h()\n",
        "core/b.py": "def g():\n    return 2\n",
    })
    _scan(repo, out)

    # Establish the true no-expansion cost for both explicit symbols.
    baseline_req = _request(out, "baseline.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"},
                                                 {"name": "g", "file": "core/b.py"}], "search_terms": [], "lines": []},
        "expansion": {"include_callers": False, "include_callees": False, "include_imports": False,
                      "include_related_tests": False},
        "limits": {"max_estimated_tokens": 200, "max_files": 12},
    })
    baseline_result = _packet(repo, out, baseline_req)
    assert baseline_result.returncode == 0, baseline_result.stderr
    baseline_sidecar = json.loads((out / "packets" / "packet_baseline.resolution.json").read_text(encoding="utf-8"))
    no_expansion_tokens = baseline_sidecar["estimated_tokens_used"]

    # A budget comfortably above the no-expansion cost, but too small to
    # also fit f's callee-expansion listing -- both explicit symbols must
    # still render in full; only the expansion may be omitted.
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"},
                                                 {"name": "g", "file": "core/b.py"}], "search_terms": [], "lines": []},
        "expansion": {"include_callers": False, "include_callees": True, "include_imports": False,
                      "include_related_tests": False},
        "limits": {"max_estimated_tokens": no_expansion_tokens + 10, "max_files": 12},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "### Symbol: `f`" in text
    assert "### Symbol: `g`" in text
    assert "return 2" in text  # g's own body, never sacrificed for f's expansion


def test_search_match_does_not_reserve_focus_file_slot_unless_rendered(repo, out):
    # Regression: note_focus_file(rel) was called before checking whether
    # the match's own rendered line fit the remaining budget, so a match
    # that ultimately never appears in the packet could still consume the
    # sole limits.max_files slot -- and the resolution sidecar would then
    # misleadingly name that file as a "focus file" despite showing zero
    # evidence for it.
    write_files(repo, {"a.py": f"needle = {'x' * 500!r}\n"})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [], "search_terms": ["needle"], "lines": []},
        "limits": {"max_estimated_tokens": 10, "max_files": 1},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
    assert sidecar["focus_files"] == []
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "a.py:1" not in text


def test_duplicate_explicit_selectors_are_evaluated_once_not_per_occurrence(repo, out):
    # Regression: a request naming the same file selector many times over
    # re-attempted rendering (and, if it failed, re-appended an identical
    # conflict message) once per occurrence -- an unbounded-output shape
    # for a request that just repeats one selector, independent of the
    # token-budget accounting fixes for distinct content.
    write_files(repo, {"empty.py": ""})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["empty.py"] * 1000, "symbols": [], "search_terms": [], "lines": []},
        "limits": {"max_estimated_tokens": 1, "max_files": 12},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 1
    assert result.stderr.count("empty.py") <= 2  # one conflict line, not one per duplicate


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


def test_explicit_file_excerpt_renders_before_optional_symbol_inventory(repo, out):
    # Regression: the file explicit-selector loop spent budget on the
    # "Top-level symbols:" inventory listing *before* rendering the file's
    # own mandatory excerpt. A tight-but-sufficient budget (enough for the
    # header + full excerpt, but not also the full inventory) let the
    # optional inventory crowd out the mandatory excerpt, forcing a hard
    # explicit_conflicts abort even though the file's actual requested
    # content would have fit on its own. The excerpt must always render
    # first; only the inventory may be truncated/omitted.
    n = 150
    lines = [f"def f_{i:03d}(): pass" for i in range(n)]
    write_files(repo, {"core/big.py": "\n".join(lines) + "\n"})
    _scan(repo, out)

    no_expansion = {"include_callers": False, "include_callees": False, "include_imports": False,
                    "include_related_tests": False, "include_graphify": False}
    generous_req = _request(out, "generous.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["core/big.py"], "symbols": [], "search_terms": [], "lines": []},
        "expansion": no_expansion,
        "limits": {"max_estimated_tokens": 200000, "max_files": 12},
    })
    generous_result = _packet(repo, out, generous_req)
    assert generous_result.returncode == 0, generous_result.stderr
    full_text = (out / "packets" / "packet_generous.md").read_text(encoding="utf-8")
    listing_lines = [l for l in full_text.splitlines() if "(function, lines" in l]
    assert len(listing_lines) == n  # nothing tight yet -- every symbol is listed
    listing_chars = sum(len(l) for l in listing_lines) + len("Top-level symbols:")
    full_tokens = json.loads(
        (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
    )["estimated_tokens_used"]

    # Cut roughly half the inventory listing's worth of room from the
    # fully-fitting budget: still comfortably enough for the header + full
    # excerpt (which the listing bug never touched), but not enough for
    # the full inventory listing too.
    constrained_tokens = full_tokens - (listing_chars // 2 // 4)

    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["core/big.py"], "symbols": [], "search_terms": [], "lines": []},
        "expansion": no_expansion,
        "limits": {"max_estimated_tokens": constrained_tokens, "max_files": 12},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    # Mandatory excerpt: every function's source line, including the very
    # last one, must still be present in full.
    assert "def f_000(): pass" in text
    assert "def f_149(): pass" in text
    # Optional inventory: truncated instead, never the excerpt.
    listing_lines_constrained = [l for l in text.splitlines() if "(function, lines" in l]
    assert len(listing_lines_constrained) < n
    sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
    assert any("top-level symbol" in o.lower() for o in sidecar["omissions"])


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


def test_invalid_regex_notices_are_charged_against_budget(repo, out):
    # Regression: each invalid-regex search term appended a notice with no
    # budget accounting, so a request with many invalid regex terms could
    # produce a large packet while reporting ~0 estimated tokens used
    # under a tiny limits.max_estimated_tokens. This also exercises a
    # second-order version of the same bug: each skipped notice fell back
    # to an *unbudgeted* budget.omissions entry, and the final "## Omitted
    # / unresolved" section rendered that whole list without any size
    # accounting either -- both layers had to be fixed for this to pass.
    write_files(repo, {"a.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    bad_terms = [f"(unclosed_{i}" for i in range(200)]
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [], "search_terms": bad_terms, "lines": []},
        "expansion": {"search_as_regex": True},
        "limits": {"max_estimated_tokens": 1, "max_files": 500},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert len(text) < 3000
    assert text.count("not a valid regex") < 200


def test_pathological_regex_search_term_times_out_instead_of_hanging(repo, out, monkeypatch):
    # Regression: search_as_regex ran a caller-supplied pattern through
    # plain re.search with no bound on evaluation time. A syntactically
    # valid but pathological pattern like `(a+)+$` against a long, nearly-
    # matching line triggers catastrophic backtracking -- confirmed to
    # still be running after 20+ seconds for just 35 characters on this
    # engine -- which could hang the CLI indefinitely for an LLM-produced
    # or malicious request. Each term's evaluation must be bounded.
    monkeypatch.setattr(rr, "_REGEX_SEARCH_TIMEOUT_SECONDS", 0.5)
    write_files(repo, {"core/a.py": "x = '" + "a" * 35 + "!'\n"})
    _scan(repo, out)
    files_rows = rr._load_csv(out / "file_inventory.csv")
    resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["(a+)+$"], True, files_rows)
    assert resolutions[0].status == "invalid"
    assert "exceeded" in resolutions[0].detail
    assert matches_by_term["(a+)+$"] == []

    # A normal (non-pathological) regex must still work correctly under
    # the same bounded path -- the timeout mechanism must not break
    # ordinary regex search.
    resolutions2, matches_by_term2, _ = rr.resolve_search_terms(repo, ["a{3}"], True, files_rows)
    assert resolutions2[0].status == "resolved"
    assert matches_by_term2["a{3}"]


def test_graphify_peer_listing_respects_max_files(repo, out):
    # Regression: Graphify community-peer paths were emitted without
    # going through note_focus_file, so they could exceed limits.max_files
    # while the resolution sidecar's focus_files list stayed under it.
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
        "limits": {"max_estimated_tokens": 12000, "max_files": 1},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
    assert sidecar["focus_files"] == ["core/a.py"]
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    # core/b.py is legitimately named in the omission explanation -- it
    # must never appear as a rendered, [origin: graphify_expansion]-tagged
    # evidence item.
    assert "[origin: graphify_expansion]" not in text
    assert "Graphify community peer `core/b.py`" in text
    assert "limits.max_files" in text


def test_graphify_withheld_on_dirty_worktree_even_with_matching_commit(repo, out):
    # Regression: a matching built_at_commit was accepted even when the
    # scanned worktree had uncommitted changes -- a matching commit hash
    # alone doesn't prove graph.json's communities still describe what's
    # actually on disk if a tracked file was modified since that commit.
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    commit = _git_init_commit(repo)
    graph = {
        "built_at_commit": commit,
        "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
    }
    (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
    (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
    # Modify a tracked file after the commit, without committing again --
    # this is what makes the worktree dirty even though HEAD still equals
    # the commit graph.json names.
    write_files(repo, {"core/a.py": "def f():\n    return 999\n"})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
        "expansion": {"include_graphify": True},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "graphify_expansion" not in text
    assert "worktree is dirty" in text


def test_graphify_not_withheld_for_dirtiness_confined_to_output_dir(repo):
    # Regression: get_git_info(root) reported "dirty" for *any* uncommitted
    # change anywhere in the worktree, including this tool's own freshly
    # written --output directory when it lives inside the scanned repo (as
    # it does for this project's own repo_context/). scan/packet always
    # write fresh output before this check runs, so every single run
    # against such a repo made the worktree look dirty and withheld
    # Graphify evidence for a reason that has nothing to do with the
    # scanned *source* changing.
    output_dir = repo / "repo_context"
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    commit = _git_init_commit(repo)
    graph = {
        "built_at_commit": commit,
        "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
    }
    (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
    (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
    _scan(repo, output_dir)  # writes brand-new, untracked files *inside* repo -- this alone used to dirty git status
    req = _request(output_dir, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
        "expansion": {"include_graphify": True},
    })
    result = _packet(repo, output_dir, req)
    assert result.returncode == 0, result.stderr
    text = (output_dir / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "worktree is dirty" not in text


def test_overlong_question_is_rejected(repo, out):
    # Regression: `question` is copied verbatim into every packet's
    # header with no budget accounting and no schema length limit -- an
    # oversized value could make a packet exceed limits.max_estimated_tokens
    # through the header alone.
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "x" * 5000,
        "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
        "limits": {"max_estimated_tokens": 1, "max_files": 12},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 1
    assert "too long" in result.stderr
    assert not (out / "packets" / "packet_req.md").exists()


def test_caller_callee_import_expansion_respects_max_files(repo, out):
    # Regression: callers/callees/internal-imports listings emitted every
    # referenced file without going through note_focus_file, so they could
    # exceed limits.max_files while the resolution sidecar's focus_files
    # stayed under it (the related-test and Graphify branches already
    # enforced this; these three didn't).
    write_files(repo, {
        "core/a.py": "from core.b import g\nfrom core.c import h\n\n\ndef f():\n    g()\n    return h()\n",
        "core/b.py": "def g():\n    return 1\n",
        "core/c.py": "from core.a import f\n\n\ndef h():\n    return f()\n",
    })
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
        "expansion": {"include_callers": True, "include_callees": True, "include_imports": True},
        "limits": {"max_estimated_tokens": 12000, "max_files": 1},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
    assert sidecar["focus_files"] == ["core/a.py"]
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "[origin: caller_expansion]" not in text
    assert "[origin: callee_expansion]" not in text
    assert "limits.max_files" in text


def _git_init_commit(repo) -> str:
    import subprocess
    # graphify-out/graph.json is written *after* this commit (it needs the
    # resulting commit hash for built_at_commit) -- gitignore it first so
    # that later write leaves the worktree clean (git ignores it) rather
    # than untracked/dirty, which the dirty-worktree Graphify check would
    # otherwise (correctly) treat as unverifiable.
    (repo / ".gitignore").write_text("graphify-out/\n", encoding="utf-8")
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
