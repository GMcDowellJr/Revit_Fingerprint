import json
import time

from conftest import run_tool, write_files  # noqa: F401 -- conftest import also puts TOOL_DIR on sys.path
import rc_packet
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
    # 200 tokens comfortably clears the fixed framing's own floor (see
    # rc_request.py's "Reserve the fixed framing's budget cost up front")
    # but nowhere near big.py's ~900 lines -- this exercises the explicit
    # *selector* conflict, not the separate framing-alone conflict.
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["big.py"], "symbols": [], "search_terms": [], "lines": []},
        "limits": {"max_estimated_tokens": 200, "max_files": 12},
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

    # Establish the true no-expansion cost for both explicit symbols. Use a
    # generous budget for this measurement run -- the fixed framing
    # (header/resolution-report/footer) is now reserved against the budget
    # up front (see rc_request.py's "Reserve the fixed framing's budget
    # cost up front"), so a too-tight value here would fail on framing
    # alone rather than actually measuring the two symbols' cost.
    baseline_req = _request(out, "baseline.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"},
                                                 {"name": "g", "file": "core/b.py"}], "search_terms": [], "lines": []},
        "expansion": {"include_callers": False, "include_callees": False, "include_imports": False,
                      "include_related_tests": False},
        "limits": {"max_estimated_tokens": 1000, "max_files": 12},
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
    # 150 tokens is just above the fixed framing's own floor (header +
    # resolution-report heading + footer + packet-size summary line must
    # be reserved first -- see rc_request.py's "Reserve the fixed
    # framing's budget cost up front") but well short of also fitting the
    # ~500-char match line.
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [], "search_terms": ["needle"], "lines": []},
        "limits": {"max_estimated_tokens": 150, "max_files": 1},
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


def test_line_range_extending_past_enclosing_symbol_renders_in_full(repo, out):
    # Regression: the enclosing-symbol lookup only checked that the
    # requested range's *start* line fell inside a symbol, not that the
    # symbol also contained the *end* line. A range starting inside a
    # tiny function that ends immediately (line 2) but requested through
    # line 7 got silently truncated to just that function's own bounds
    # (line 2) -- the resolution report still claimed "resolved" while
    # lines 3-7 were dropped entirely from the packet.
    write_files(repo, {"core/a.py": (
        "x = 0\n"             # 1
        "def tiny(): pass\n"  # 2 (a symbol whose own bounds are just this one line)
        "y = 1\n"             # 3
        "z = 2\n"             # 4
        "w = 3\n"             # 5
        "v = 4\n"             # 6
        "u = 5\n"             # 7
    )})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [], "search_terms": [],
                      "lines": [{"file": "core/a.py", "line": 2, "end_line": 7}]},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    # The full requested range must render -- not truncated to `tiny`'s
    # own 1-line bounds.
    assert "def tiny(): pass" in text
    assert "u = 5" in text
    assert "Enclosing symbol:" not in text  # no symbol contains both endpoints


def test_enclosing_symbol_note_is_charged_against_budget(repo, out):
    # Regression: the "Enclosing symbol: ..." metadata line for a line
    # selector was appended with no budget.allow()/spend() at all -- a
    # long qualified name could make the actual packet bigger than the
    # sidecar's reported estimated_tokens_used implied.
    write_files(repo, {"core/a.py": "def " + "x" * 80 + "():\n    y = 1\n    return y\n"})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [], "search_terms": [], "lines": [{"file": "core/a.py", "line": 2}]},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
    assert "Enclosing symbol:" in text
    assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5


def test_search_match_collection_is_capped(repo, out):
    # Regression: every matching line was collected into an in-memory
    # list before max_files/the packet budget were ever applied during
    # Tier-2 rendering -- a common term across a large repository could
    # accumulate an unbounded number of tuples. The direct --search
    # packet path already caps collection at max_files * 5; the request
    # path needs the same bound.
    files = {f"core/mod_{i:03d}.py": "needle\n" * 50 for i in range(50)}
    write_files(repo, files)
    _scan(repo, out)
    files_rows = rr._load_csv(out / "file_inventory.csv")
    resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["needle"], False, files_rows, 3)
    assert len(matches_by_term["needle"]) <= 3 * 5


def test_redacted_excerpt_is_charged_not_the_raw_source(repo, out):
    # Regression: budget.allow() was checked against the *raw* excerpt
    # text, and redact_secrets() (which can make a secret-shaped value
    # longer via its placeholder) only ran afterward on content already
    # verified to fit -- so the actually-written (redacted) body could
    # end up bigger than what the budget check had approved.
    # The assignment key must be literally "token" (etc.) immediately
    # followed by `=`/`:` for _SECRET_ASSIGNMENT_PATTERN to match at all
    # -- a value just short enough that the fixed-length
    # "[REDACTED-POSSIBLE-SECRET]" placeholder (26 chars) is *longer*
    # than the whole original "token = '...'" span it replaces (~22
    # chars for a 12-char value), so redaction measurably grows the text.
    lines = [f"token = 'abcdefghi{i:03d}'" for i in range(100)]
    write_files(repo, {"core/a.py": "\n".join(lines) + "\n"})
    _scan(repo, out)
    # Measure the real (redacted) cost via a generous run rather than
    # guessing a token limit -- an explicit file selector hard-aborts the
    # whole packet if it doesn't fit (a separate, correct invariant), so
    # the budget must be sized to comfortably fit the redacted excerpt.
    generous_req = _request(out, "generous.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
        "limits": {"max_estimated_tokens": 200000, "max_files": 12},
    })
    generous_result = _packet(repo, out, generous_req)
    assert generous_result.returncode == 0, generous_result.stderr
    full_tokens = json.loads(
        (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
    )["estimated_tokens_used"]
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
        "limits": {"max_estimated_tokens": full_tokens + 10, "max_files": 12},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    # The reported usage must not understate the packet's actual size --
    # this bug's own repro showed ~890 tokens actually used while a 750
    # limit was reported as satisfied.
    assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5


def test_regex_search_rejected_when_bounding_is_unsupported(repo, out, monkeypatch):
    # Regression: on a platform without SIGALRM (Windows), search_as_regex
    # fell back to running the pattern completely unbounded -- exactly
    # the hang this whole mechanism exists to prevent, just gated behind
    # a platform check instead of being fixed. Simulate that platform by
    # removing signal.SIGALRM for the duration of this test.
    import signal
    monkeypatch.delattr(signal, "SIGALRM", raising=False)
    write_files(repo, {"core/a.py": "hello\n"})
    _scan(repo, out)
    files_rows = rr._load_csv(out / "file_inventory.csv")
    resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["(a+)+$"], True, files_rows, 12)
    assert resolutions[0].status == "invalid"
    assert "SIGALRM" in resolutions[0].detail or "unbounded" in resolutions[0].detail.lower()
    assert matches_by_term["(a+)+$"] == []


def test_aggregate_search_deadline_applies_to_literal_terms_too(repo, out, monkeypatch):
    # Regression: the aggregate wall-clock deadline only applied when
    # search_as_regex was true. A request with hundreds/thousands of
    # absent *literal* terms re-reads every included text file once per
    # term with no bound at all, since collect_cap only limits how many
    # matches pile up, not how many full scans happen for a term that
    # matches nothing. The deadline must apply regardless of
    # search_as_regex.
    monkeypatch.setattr(rr, "_REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS", 0)
    write_files(repo, {"core/a.py": "needle\n"})
    _scan(repo, out)
    files_rows = rr._load_csv(out / "file_inventory.csv")
    resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["needle", "other"], False, files_rows, 12)
    assert all(r.status == "invalid" for r in resolutions)
    assert all("aggregate" in r.detail for r in resolutions)


def test_search_match_redacts_before_truncating(repo, out):
    # Regression: the rendered search-match line truncated to 200 chars
    # *before* calling redact_secrets(). A secret-shaped value whose
    # closing quote fell beyond character 200 had that quote cut off
    # first, breaking _SECRET_ASSIGNMENT_PATTERN's closing-quote
    # backreference -- redact_secrets() then never matched at all, and
    # the (truncated) secret prefix leaked into the packet unredacted.
    secret_value = "x" * 250
    write_files(repo, {"core/a.py": f'token = "{secret_value}"  # NEEDLE_MARKER\n'})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [], "search_terms": ["NEEDLE_MARKER"], "lines": []},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "xxxxxxxxxx" not in text
    assert "REDACTED" in text


def test_packet_header_and_footer_charged_against_budget(repo, out):
    # Regression: the fixed header (title/root/question/provenance/
    # limits) and footer were written with no budget accounting
    # whatsoever -- an accepted (<=4000-char) question alone could make
    # the real packet many times bigger than limits.max_estimated_tokens
    # while the sidecar still reported a number near zero.
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    long_question = "why? " * 700  # comfortably under MAX_QUESTION_LENGTH (4000), still substantial
    # A search-term selector, not an explicit file/symbol/line selector --
    # the latter now correctly hard-aborts the whole packet if it can't
    # fit alongside the (now-charged) header, which is a separate, correct
    # invariant this test isn't about. A generous budget here so the
    # request succeeds; the header (which embeds the question verbatim)
    # is charged/reserved up front regardless (see rc_request.py's
    # "Reserve the fixed framing's budget cost up front").
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": long_question,
        "selectors": {"files": [], "symbols": [], "search_terms": ["nonexistent_term_xyz"], "lines": []},
        "limits": {"max_estimated_tokens": 2000, "max_files": 12},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
    assert sidecar["estimated_tokens_used"] > 100  # the question alone is ~700+ chars
    assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5

    # And a budget too small to fit the header (which embeds the question
    # verbatim) plus the footer must now hard-abort outright, rather than
    # silently "succeed" with a packet whose true size is many times over
    # the requested cap -- exactly the shape of the original bug this
    # regression test exists for.
    tiny_req = _request(out, "tiny.json", {
        "schema_version": "1.0", "question": long_question,
        "selectors": {"files": [], "symbols": [], "search_terms": ["nonexistent_term_xyz"], "lines": []},
        "limits": {"max_estimated_tokens": 1, "max_files": 12},
    })
    tiny_result = _packet(repo, out, tiny_req)
    assert tiny_result.returncode == 1
    assert "too small to fit" in tiny_result.stderr
    assert not (out / "packets" / "packet_tiny.md").exists()


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


def test_file_inventories_deferred_until_every_explicit_file_renders(repo, out):
    # Regression: one explicit file selector's "Top-level symbols:"
    # inventory was still rendered before the *next* explicit file
    # selector got its guaranteed shot at the budget. With a 30-function
    # `a.py` selected before a tiny `b.py`, a.py's inventory could consume
    # enough of a tight-but-otherwise-sufficient budget that b.py's own
    # excerpt no longer fit -- reversing the selector order changed the
    # outcome under the same limit, which is exactly the ordering-
    # dependence this fix removes (every file's excerpt must render before
    # *any* file's inventory spends a char).
    n = 60
    lines = [f"def f_{i:03d}(): pass" for i in range(n)]
    write_files(repo, {
        "a.py": "\n".join(lines) + "\n",
        "b.py": "def tiny():\n    return 1\n",
    })
    _scan(repo, out)

    no_expansion = {"include_callers": False, "include_callees": False, "include_imports": False,
                    "include_related_tests": False, "include_graphify": False}
    generous_req = _request(out, "generous.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
        "expansion": no_expansion,
        "limits": {"max_estimated_tokens": 200000, "max_files": 12},
    })
    generous_result = _packet(repo, out, generous_req)
    assert generous_result.returncode == 0, generous_result.stderr
    full_text = (out / "packets" / "packet_generous.md").read_text(encoding="utf-8")
    listing_lines = [l for l in full_text.splitlines() if "(function, lines" in l]
    assert len(listing_lines) == n + 1  # a.py's n functions plus b.py's own single "tiny" entry
    listing_chars = sum(len(l) for l in listing_lines) + len("Top-level symbols:")
    full_tokens = json.loads(
        (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
    )["estimated_tokens_used"]

    # Cut roughly half of a.py's inventory worth of room from the fully-
    # fitting total -- still comfortably enough for both files' headers +
    # excerpts (which this bug never touched), but not enough for a.py's
    # full inventory too.
    constrained_tokens = full_tokens - (listing_chars // 2 // 4)

    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
        "expansion": no_expansion,
        "limits": {"max_estimated_tokens": constrained_tokens, "max_files": 12},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    # b.py's mandatory excerpt (a *later* explicit selector) must render
    # regardless of a.py's inventory being tight.
    assert "def tiny():" in text
    assert "def f_000(): pass" in text  # a.py's own excerpt still renders in full too
    listing_lines_constrained = [l for l in text.splitlines() if "(function, lines" in l]
    assert len(listing_lines_constrained) < n  # a.py's inventory got truncated instead


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
    # 300 tokens clears the fixed framing's own floor (header + selector-
    # resolution report + footer, reserved up front -- see rc_request.py's
    # "Reserve the fixed framing's budget cost up front") but is nowhere
    # near enough to fit all 200 invalid-regex notices.
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [], "search_terms": bad_terms, "lines": []},
        "expansion": {"search_as_regex": True},
        "limits": {"max_estimated_tokens": 300, "max_files": 500},
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
    resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["(a+)+$"], True, files_rows, 12)
    assert resolutions[0].status == "invalid"
    assert "exceeded" in resolutions[0].detail
    assert matches_by_term["(a+)+$"] == []

    # A normal (non-pathological) regex must still work correctly under
    # the same bounded path -- the timeout mechanism must not break
    # ordinary regex search.
    resolutions2, matches_by_term2, _ = rr.resolve_search_terms(repo, ["a{3}"], True, files_rows, 12)
    assert resolutions2[0].status == "resolved"
    assert matches_by_term2["a{3}"]


def test_aggregate_regex_search_time_is_capped_across_all_terms(repo, out, monkeypatch):
    # Regression: the per-term SIGALRM bound stops any *one* pathological
    # pattern from hanging forever, but the schema places no cap on how
    # many search_terms a request can carry -- a request with several
    # distinct catastrophic-backtracking patterns could still burn a full
    # per-term allowance for *each one* before packet budgeting even
    # began. Three terms each requesting up to 1.0s, under a 1.5s
    # aggregate cap, must finish in well under 3.0s total, with the terms
    # beyond the aggregate deadline reported as skipped rather than each
    # getting their own fresh timeout.
    monkeypatch.setattr(rr, "_REGEX_SEARCH_TIMEOUT_SECONDS", 1.0)
    monkeypatch.setattr(rr, "_REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS", 1.5)
    write_files(repo, {"core/a.py": "x = '" + "a" * 35 + "!'\n"})
    _scan(repo, out)
    files_rows = rr._load_csv(out / "file_inventory.csv")
    start = time.monotonic()
    # Three distinct-looking patterns, all pathological against the same
    # run of `a` characters (a term repeated verbatim would risk being
    # deduplicated by a caller upstream of this function; these aren't).
    resolutions, matches_by_term, _ = rr.resolve_search_terms(
        repo, ["(a+)+$", "(a+)*$", "(a|aa)+$"], True, files_rows, 12,
    )
    elapsed = time.monotonic() - start
    assert elapsed < 2.5  # comfortably bounded, not the ~3.0s+ three full per-term timeouts would take
    assert all(r.status == "invalid" for r in resolutions)
    # At least the last term must be skipped outright by the aggregate
    # deadline rather than getting its own fresh per-term timeout.
    assert any("aggregate" in r.detail for r in resolutions)


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
    # core/b.py must never appear as a rendered, [origin:
    # graphify_expansion]-tagged evidence item -- it's beyond
    # limits.max_files, reported only via the batched omission note.
    assert "[origin: graphify_expansion]" not in text
    assert "Graphify community peer(s)" in text
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


def test_callee_expansion_continues_past_a_rejected_file(repo, out):
    # Regression: the callee-expansion loop `break`-ed the whole listing
    # on the first callee whose file was beyond limits.max_files, even
    # though a *later* callee might be in a file already in focus_files
    # (free -- no new slot needed). `f`'s first callee `g` lives in a
    # different, not-yet-focused file; its second callee `h` lives in the
    # same file as `f` itself (already focused, since that's the selected
    # symbol's own file). With max_files:1, `g` must be skipped but `h`
    # must still render -- not silently dropped along with it.
    write_files(repo, {
        "core/a.py": "from core.other import g\n\n\ndef h():\n    return 1\n\n\ndef f():\n    g()\n    h()\n    return 1\n",
        "core/other.py": "def g():\n    return 2\n",
    })
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
        "expansion": {"include_callers": False, "include_callees": True, "include_imports": False,
                      "include_related_tests": False, "include_graphify": False},
        "limits": {"max_files": 1},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "-> `h`" in text  # the already-focused-file callee must still render
    assert "core/other.py" not in text  # the beyond-max_files callee stays omitted
    sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
    assert sidecar["focus_files"] == ["core/a.py"]


def test_search_term_matches_continue_past_a_rejected_file(repo, out):
    # Regression: the search-match loop `break`-ed on the first match
    # whose file was beyond limits.max_files, even though a *later* match
    # (for the same term) might be in a file already in focus_files. With
    # `z/main.py` explicitly selected and `needle_term` matching both
    # `a/other.py` (scanned first, alphabetically) and `z/main.py`
    # (already focused), max_files:1 must still render the z/main.py
    # match instead of losing both to the first rejection.
    write_files(repo, {
        "a/other.py": "# needle_term\n",
        "z/main.py": "# needle_term\n",
    })
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["z/main.py"], "symbols": [], "search_terms": ["needle_term"], "lines": []},
        "limits": {"max_files": 1},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "z/main.py:1" in text
    assert "a/other.py" not in text
    sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
    assert sidecar["focus_files"] == ["z/main.py"]


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
    # 300 tokens clears the fixed framing's own floor (header + footer,
    # reserved up front -- see rc_request.py's "Reserve the fixed framing's
    # budget cost up front") but is nowhere near enough to fit all 200
    # resolution-report entries.
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": missing_files, "symbols": [], "search_terms": [], "lines": []},
        "limits": {"max_estimated_tokens": 300, "max_files": 500},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
    # Certainly not a full 200-entry resolution report rendered in the
    # packet body -- it must be truncated with a count-of-omitted note,
    # not a wild understatement of the packet's actual size the way ~0
    # tokens for an 18KB packet was before this fix.
    assert len(text) < 2000
    assert text.count("missing_") < 200
    assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5


def test_file_level_expansions_render_once_per_file_not_per_symbol(repo, out):
    # Regression: _symbol_expansion() rendered imports/related-tests/
    # Graphify-peer sections (all keyed by the containing *file*, not the
    # symbol) on every call -- but an explicit file selector called it
    # once per top-level symbol in that file, so a multi-function file
    # got its "Internal imports of X" / "Related tests for X" sections
    # duplicated once per function instead of appearing once.
    write_files(repo, {
        "core/a.py": (
            "from core.dep import helper\n\n\n"
            "def f():\n    return helper()\n\n\n"
            "def g():\n    return helper()\n\n\n"
            "def h():\n    return helper()\n"
        ),
        "core/dep.py": "def helper():\n    return 1\n",
        "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 1\n",
    })
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
        "expansion": {"include_callers": False, "include_callees": False, "include_imports": True,
                      "include_related_tests": True, "include_graphify": False},
        "limits": {"max_estimated_tokens": 12000, "max_files": 12},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    # Three top-level functions in core/a.py, but these file-level
    # sections must appear exactly once, not three times.
    assert text.count("Internal imports of `core/a.py`") == 1
    assert text.count("Related tests for `core/a.py`") == 1


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
    assert "beyond limits.max_files" in text


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


def test_file_level_expansion_runs_for_a_symbol_free_selected_file(repo, out):
    # Regression: the loop driving file-level expansion (imports/related-
    # tests/Graphify peers) skipped straight past `_maybe_file_expansion()`
    # whenever an explicitly selected file had no top-level symbols (e.g.
    # an __init__.py re-export shim, or a plain module that only does
    # imports at module scope). Since imports/related-tests describe the
    # *file*, not any symbol in it, a symbol-free explicitly selected file
    # previously got zero import/related-test expansion evidence even
    # when include_imports/include_related_tests were explicitly on.
    write_files(repo, {
        "core/__init__.py": "from core.dep import helper\n",
        "core/dep.py": "def helper():\n    return 1\n",
    })
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["core/__init__.py"], "symbols": [], "search_terms": [], "lines": []},
        "expansion": {"include_callers": False, "include_callees": False, "include_imports": True,
                      "include_related_tests": False},
        "limits": {"max_estimated_tokens": 12000, "max_files": 12},
    })
    result = _packet(repo, out, req)
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    assert "Internal imports of `core/__init__.py`" in text
    assert "core/dep.py" in text


def test_fixed_framing_is_reserved_before_tier1_content_spends_budget(repo, out):
    # Regression: the header/selector-resolution-report/footer were
    # charged against the budget only *after* Tier-1 explicit-selector
    # content had already been allowed to spend against the full,
    # unreserved budget. That let a request's explicit content "fit" a
    # budget that, once framing's real cost landed on top afterward, the
    # packet's actual rendered size exceeded -- generation still reported
    # success despite the true size being over limits.max_estimated_tokens.
    # Framing must be reserved (and charged) first, so a too-tight budget
    # now correctly surfaces as an explicit_conflicts abort instead of an
    # over-budget "successful" packet.
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    _scan(repo, out)

    # A single fixed request/output name reused for *every* call below --
    # the packet header renders the request filename verbatim
    # (`- Request file: \`{name}.json\` ...`), so varying the name's
    # length between calls (e.g. "probe166" vs. "boundary" vs. "under")
    # shifts the header's own size and, right at a ~1-token-wide margin,
    # can flip whether a given max_estimated_tokens value fits -- making
    # the boundary this test measures depend on which name happened to be
    # used for which call, not just on max_estimated_tokens. Reusing one
    # name removes that variable; each call's result depends only on
    # max_tokens.
    name = "req"

    def _gen(max_tokens):
        req = _request(out, f"{name}.json", {
            "schema_version": "1.0", "question": "q",
            "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
            "expansion": {"include_callers": False, "include_callees": False, "include_imports": False,
                          "include_related_tests": False},
            "limits": {"max_estimated_tokens": max_tokens, "max_files": 12},
        })
        return rr.generate_packet_from_request(repo, out, req, name_override=name)

    # Sanity-bound the binary search: a generous budget must succeed.
    packet_path, _, err = _gen(2000)
    assert packet_path is not None, err

    lo, hi = 1, 2000
    while lo < hi:
        mid = (lo + hi) // 2
        packet_path, _, _ = _gen(mid)
        if packet_path is not None:
            hi = mid
        else:
            lo = mid + 1
    min_success_tokens = hi

    # At the smallest budget that still succeeds, the packet's own
    # reported size must not exceed what was actually requested -- this
    # is exactly the invariant framing-charged-too-late violated (it
    # would report success here with estimated_tokens_used well above
    # min_success_tokens, since Tier-1 content had already spent as if
    # framing were free).
    packet_path, _, err = _gen(min_success_tokens)
    assert packet_path is not None, err
    sidecar = json.loads((out / "packets" / f"packet_{name}.resolution.json").read_text(encoding="utf-8"))
    assert sidecar["estimated_tokens_used"] <= min_success_tokens

    # One token below that boundary must hard-abort -- not silently
    # succeed with a packet whose true size exceeds the requested cap.
    packet_path, _, err = _gen(min_success_tokens - 1)
    assert packet_path is None
    assert "do not fit" in (err or "")


def test_search_term_match_cannot_exceed_budget_via_late_footer_charge(repo, out):
    # Regression (fresh Codex evidence after the first framing-reservation
    # fix): reserving the header up front, then charging the selector-
    # resolution report entries via budget.allow(), and only *then*
    # unconditionally spending the footer, still let a resolution-report
    # entry get allowed against a budget that hadn't yet accounted for the
    # footer's own cost -- the footer's later unconditional spend then
    # pushed the packet's true size back over the cap anyway. Concretely:
    # a 100-token request whose only content is one ~500-character search
    # match still reported estimated_tokens_used around 136, over the
    # 100-token cap. Header and footer must be reserved *together*, as one
    # atomic unit, before the resolution report (or anything else) spends.
    write_files(repo, {"a.py": f"needle = {'x' * 500!r}\n"})
    _scan(repo, out)
    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [], "search_terms": ["needle"], "lines": []},
        "limits": {"max_estimated_tokens": 100, "max_files": 12},
    })
    packet_path, _, err = rr.generate_packet_from_request(repo, out, req, name_override="req")
    # Either this cleanly hard-aborts (the fixed framing alone doesn't fit
    # this tiny budget) or, if it succeeds, its real size must not exceed
    # what was requested -- what it must never do is "succeed" while its
    # true size exceeds the cap.
    if packet_path is not None:
        sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
        assert sidecar["estimated_tokens_used"] <= 100
    else:
        assert "too small to fit" in err


def _spy_iter_safe_lines(monkeypatch, module):
    """Wraps module._iter_safe_lines so every line it actually yields is
    counted, without changing its behavior (including .close()
    forwarding). Returns the shared counter dict; read counter["count"]
    after the call under test."""
    real = module._iter_safe_lines
    counter = {"count": 0}

    def spy(root, rel_path, start, end):
        gen = real(root, rel_path, start, end)
        if gen is None:
            return None

        def _wrapped():
            try:
                for item in gen:
                    counter["count"] += 1
                    yield item
            finally:
                gen.close()

        return _wrapped()

    monkeypatch.setattr(module, "_iter_safe_lines", spy)
    return counter


def test_explicit_excerpt_streams_and_stops_instead_of_materializing_whole_range(repo, out, monkeypatch):
    # Regression: an explicit file selector's excerpt was fully
    # materialized via _safe_excerpt(root, rel, 1, line_count) *before*
    # any budget check -- for a file the scanner deliberately keeps in
    # the inventory without reading whole (files over MAX_TEXT_READ_BYTES
    # still get a real, streamed-counted line_count -- see rc_scan.py),
    # this could allocate the file's entire content in memory just to
    # then learn the excerpt doesn't fit. The excerpt must instead be
    # streamed and stop reading as soon as it's clear the remaining
    # budget is exceeded.
    big_lines = [f"line number {i:06d} of a much larger file body" for i in range(5000)]
    write_files(repo, {"big.py": "\n".join(big_lines) + "\n"})
    _scan(repo, out)
    counter = _spy_iter_safe_lines(monkeypatch, rr)

    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": ["big.py"], "symbols": [], "search_terms": [], "lines": []},
        "limits": {"max_estimated_tokens": 200, "max_files": 12},
    })
    packet_path, _, err = rr.generate_packet_from_request(repo, out, req, name_override="req")
    # The explicit selector doesn't fit this tiny budget -- correctly a
    # hard conflict, per the existing explicit-selector contract -- but
    # what matters here is that reaching that conclusion did not require
    # reading anywhere near all 5000 lines of the file first.
    assert packet_path is None
    assert counter["count"] < 500, f"read {counter['count']} lines before bailing out -- not streaming"


def test_search_term_scan_streams_and_stops_once_collect_cap_is_reached(repo, out, monkeypatch):
    # Regression: _scan_term_matches() called _safe_excerpt(root, rel, 1,
    # 10_000_000) per file, materializing up to ten million lines before
    # any matching happened -- a large included file (the scanner
    # deliberately keeps files over MAX_TEXT_READ_BYTES in the inventory
    # without reading them whole -- see rc_scan.py) could exhaust memory
    # searching for a term that matches many times, or not at all. A
    # literal search must still scan every line to find every match up to
    # collect_cap, but it must do so line-by-line as the file streams in
    # -- not by pre-loading the whole range into memory first -- and it
    # must stop consuming the file entirely once collect_cap matches have
    # been found, not keep reading to the end regardless.
    #
    # max_files=1 makes collect_cap (max(1, max_files) * 5) a small,
    # known value (5): put 10 matches up front, comfortably more than
    # collect_cap, followed by thousands of filler lines the scan must
    # never need to reach.
    big_lines = ["needle_marker"] * 10 + [f"filler line {i:06d}" for i in range(5000)]
    write_files(repo, {"big.py": "\n".join(big_lines) + "\n"})
    _scan(repo, out)
    counter = _spy_iter_safe_lines(monkeypatch, rr)

    req = _request(out, "req.json", {
        "schema_version": "1.0", "question": "q",
        "selectors": {"files": [], "symbols": [], "search_terms": ["needle_marker"], "lines": []},
        "limits": {"max_estimated_tokens": 12000, "max_files": 1},
    })
    packet_path, _, err = rr.generate_packet_from_request(repo, out, req, name_override="req")
    assert packet_path is not None, err
    text = packet_path.read_text(encoding="utf-8")
    assert "needle_marker" in text
    # collect_cap is 5 here -- the scan must stop shortly after finding
    # the 5th match, not read all ~5010 lines of the file.
    assert counter["count"] < 500, f"read {counter['count']} lines after collect_cap was reached -- not streaming"


def test_every_rendered_fragment_is_charged_against_the_budget(repo, out):
    # Regression: several pieces of the rendered packet were appended to
    # `out`/`header_lines` without ever being passed to budget.spend() at
    # their own true size -- the excerpt code-fence markers ("```\n"/
    # "\n```\n"), the "Estimated tokens used" summary line itself, the
    # "## Selector resolution report" section heading, and (most subtly)
    # the "\n".join(out)/"\n".join(header_lines) separators inserted
    # *between* every other already-charged fragment, none of which any
    # individual budget.spend() call accounted for. Each was individually
    # small, but together they let a packet's real rendered size exceed
    # limits.max_estimated_tokens while the sidecar still reported success
    # at (or under) the requested cap. At the minimal successful budget
    # for a plain one-file selector, the packet's true byte size must
    # never exceed limits.max_estimated_tokens * 4.
    write_files(repo, {"a.py": "def f():\n    return 1\n"})
    _scan(repo, out)

    name = "req"

    def _gen(max_tokens):
        req = _request(out, f"{name}.json", {
            "schema_version": "1.0", "question": "q",
            "selectors": {"files": ["a.py"], "symbols": [], "search_terms": [], "lines": []},
            "expansion": {"include_callers": False, "include_callees": False, "include_imports": False,
                          "include_related_tests": False},
            "limits": {"max_estimated_tokens": max_tokens, "max_files": 12},
        })
        return rr.generate_packet_from_request(repo, out, req, name_override=name)

    lo, hi = 1, 2000
    while lo < hi:
        mid = (lo + hi) // 2
        packet_path, _, _ = _gen(mid)
        if packet_path is not None:
            hi = mid
        else:
            lo = mid + 1
    min_success_tokens = hi

    packet_path, _, err = _gen(min_success_tokens)
    assert packet_path is not None, err
    text = packet_path.read_text(encoding="utf-8")
    assert len(text) <= min_success_tokens * 4, (
        f"packet's real size ({len(text)} chars) exceeds the requested budget "
        f"({min_success_tokens * 4} chars) at the minimal successful token count"
    )
