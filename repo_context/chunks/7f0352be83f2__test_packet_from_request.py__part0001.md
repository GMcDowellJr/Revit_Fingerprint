# Chunk of dev_tools/repo_context/tests/test_packet_from_request.py

- Source relative path: `dev_tools/repo_context/tests/test_packet_from_request.py`
- Chunk: 1 of 3
- Original line range: 1-494
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _scan, _request, _packet, test_valid_request_resolves_file_and_symbol_selectors, test_ambiguous_symbol_is_reported_not_silently_resolved, test_qualified_symbol_via_file_field_resolves_unambiguously, test_missing_selector_is_reported_but_other_selectors_still_processed, test_strict_mode_aborts_on_any_unresolved_selector, test_hard_budget_conflict_on_explicit_selector_aborts_without_partial_packet, test_expansion_never_preempts_a_later_explicit_selector, test_search_match_does_not_reserve_focus_file_slot_unless_rendered, test_duplicate_explicit_selectors_are_evaluated_once_not_per_occurrence, test_explicit_selectors_beyond_max_files_is_a_hard_conflict_not_silent_drop, test_strict_mode_catches_unresolved_search_terms, test_invalid_schema_version_is_rejected_before_resolution, test_path_traversal_selector_is_rejected, test_search_term_matches_and_related_tests_are_included, test_line_selector_resolves_enclosing_symbol, test_line_range_extending_past_enclosing_symbol_renders_in_full, test_enclosing_symbol_note_is_charged_against_budget, test_search_match_collection_is_capped, test_redacted_excerpt_is_charged_not_the_raw_source, test_regex_search_rejected_when_bounding_is_unsupported, test_aggregate_search_deadline_applies_to_literal_terms_too, test_search_match_redacts_before_truncating
- Source SHA-256: c374abd0f274680032eecfb5f8298535b3b81413a7f4f6737d033f9738d5a6a9
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| import json
     2| import time
     3| 
     4| from conftest import run_tool, write_files  # noqa: F401 -- conftest import also puts TOOL_DIR on sys.path
     5| import rc_packet
     6| import rc_request as rr
     7| 
     8| 
     9| def _scan(repo, out):
    10|     result = run_tool(["scan", str(repo), "--output", str(out)])
    11|     assert result.returncode == 0, result.stderr
    12| 
    13| 
    14| def _request(out, name, data):
    15|     path = out / name
    16|     path.write_text(json.dumps(data), encoding="utf-8")
    17|     return path
    18| 
    19| 
    20| def _packet(repo, out, request_path, extra=None):
    21|     return run_tool(["packet", str(repo), "--output", str(out), "--request", str(request_path)] + (extra or []))
    22| 
    23| 
    24| def test_valid_request_resolves_file_and_symbol_selectors(repo, out):
    25|     write_files(repo, {
    26|         "core/helper.py": "def add(a, b):\n    return a + b\n",
    27|         "tools/report.py": "from core.helper import add\n\n\ndef build():\n    return add(1, 2)\n",
    28|     })
    29|     _scan(repo, out)
    30|     req = _request(out, "req.json", {
    31|         "schema_version": "1.0", "question": "How is build computed?",
    32|         "selectors": {"files": [], "symbols": [{"name": "build"}], "search_terms": [], "lines": []},
    33|     })
    34|     result = _packet(repo, out, req)
    35|     assert result.returncode == 0, result.stderr
    36|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    37|     assert "resolved" in text
    38|     assert "def build():" in text
    39|     assert "explicit_symbol_selector" in text
    40|     assert "caller_expansion" not in text  # nothing calls build()
    41|     assert "callee_expansion" in text  # build() calls add()
    42| 
    43| 
    44| def test_ambiguous_symbol_is_reported_not_silently_resolved(repo, out):
    45|     write_files(repo, {
    46|         "core/a.py": "def dup():\n    return 1\n",
    47|         "core/b.py": "def dup():\n    return 2\n",
    48|     })
    49|     _scan(repo, out)
    50|     req = _request(out, "req.json", {
    51|         "schema_version": "1.0", "question": "what does dup do",
    52|         "selectors": {"files": [], "symbols": [{"name": "dup"}], "search_terms": [], "lines": []},
    53|     })
    54|     result = _packet(repo, out, req)
    55|     assert result.returncode == 0, result.stderr
    56|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
    57|     [res] = sidecar["resolution_report"]
    58|     assert res["status"] == "ambiguous"
    59|     assert len(res["candidates"]) == 2
    60|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    61|     assert "core/a.py" in text and "core/b.py" in text  # both candidates surfaced
    62| 
    63| 
    64| def test_qualified_symbol_via_file_field_resolves_unambiguously(repo, out):
    65|     write_files(repo, {
    66|         "core/a.py": "def dup():\n    return 1\n",
    67|         "core/b.py": "def dup():\n    return 2\n",
    68|     })
    69|     _scan(repo, out)
    70|     req = _request(out, "req.json", {
    71|         "schema_version": "1.0", "question": "what does dup do",
    72|         "selectors": {"files": [], "symbols": [{"name": "dup", "file": "core/b.py"}], "search_terms": [], "lines": []},
    73|     })
    74|     result = _packet(repo, out, req)
    75|     assert result.returncode == 0, result.stderr
    76|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    77|     assert "return 2" in text
    78|     assert "return 1" not in text
    79| 
    80| 
    81| def test_missing_selector_is_reported_but_other_selectors_still_processed(repo, out):
    82|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    83|     _scan(repo, out)
    84|     req = _request(out, "req.json", {
    85|         "schema_version": "1.0", "question": "q",
    86|         "selectors": {"files": ["core/a.py"], "symbols": [{"name": "does_not_exist"}], "search_terms": [], "lines": []},
    87|     })
    88|     result = _packet(repo, out, req)
    89|     assert result.returncode == 0, result.stderr
    90|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    91|     assert "def f():" in text  # the valid file selector still got processed
    92|     assert "missing" in text
    93|     assert "does_not_exist" in text
    94| 
    95| 
    96| def test_strict_mode_aborts_on_any_unresolved_selector(repo, out):
    97|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    98|     _scan(repo, out)
    99|     req = _request(out, "req.json", {
   100|         "schema_version": "1.0", "question": "q", "strict": True,
   101|         "selectors": {"files": ["core/a.py"], "symbols": [{"name": "does_not_exist"}], "search_terms": [], "lines": []},
   102|     })
   103|     result = _packet(repo, out, req)
   104|     assert result.returncode == 1
   105|     assert "strict mode" in result.stderr
   106|     assert not (out / "packets" / "packet_req.md").exists()
   107| 
   108| 
   109| def test_hard_budget_conflict_on_explicit_selector_aborts_without_partial_packet(repo, out):
   110|     lines = []
   111|     for i in range(300):
   112|         lines += [f"def func_{i}():", f"    return {i}", ""]
   113|     write_files(repo, {"big.py": "\n".join(lines) + "\n"})
   114|     _scan(repo, out)
   115|     # 200 tokens comfortably clears the fixed framing's own floor (see
   116|     # rc_request.py's "Reserve the fixed framing's budget cost up front")
   117|     # but nowhere near big.py's ~900 lines -- this exercises the explicit
   118|     # *selector* conflict, not the separate framing-alone conflict.
   119|     req = _request(out, "req.json", {
   120|         "schema_version": "1.0", "question": "q",
   121|         "selectors": {"files": ["big.py"], "symbols": [], "search_terms": [], "lines": []},
   122|         "limits": {"max_estimated_tokens": 200, "max_files": 12},
   123|     })
   124|     result = _packet(repo, out, req)
   125|     assert result.returncode == 1
   126|     assert "do not fit" in result.stderr
   127|     assert not (out / "packets" / "packet_req.md").exists()
   128| 
   129| 
   130| def test_expansion_never_preempts_a_later_explicit_selector(repo, out):
   131|     # Regression: expansions (callers/callees/imports/tests) for one
   132|     # explicit symbol were rendered immediately after it and before the
   133|     # *next* explicit selector got its turn, so a budget that easily fits
   134|     # every explicit selector's own content could still fail if an early
   135|     # selector's expansion ate the remaining room first. Explicit content
   136|     # must all be attempted before any expansion spends a single char.
   137|     write_files(repo, {
   138|         "core/a.py": "def h():\n    return 42\n\n\ndef f():\n    return h()\n",
   139|         "core/b.py": "def g():\n    return 2\n",
   140|     })
   141|     _scan(repo, out)
   142| 
   143|     # Establish the true no-expansion cost for both explicit symbols. Use a
   144|     # generous budget for this measurement run -- the fixed framing
   145|     # (header/resolution-report/footer) is now reserved against the budget
   146|     # up front (see rc_request.py's "Reserve the fixed framing's budget
   147|     # cost up front"), so a too-tight value here would fail on framing
   148|     # alone rather than actually measuring the two symbols' cost.
   149|     baseline_req = _request(out, "baseline.json", {
   150|         "schema_version": "1.0", "question": "q",
   151|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"},
   152|                                                  {"name": "g", "file": "core/b.py"}], "search_terms": [], "lines": []},
   153|         "expansion": {"include_callers": False, "include_callees": False, "include_imports": False,
   154|                       "include_related_tests": False},
   155|         "limits": {"max_estimated_tokens": 1000, "max_files": 12},
   156|     })
   157|     baseline_result = _packet(repo, out, baseline_req)
   158|     assert baseline_result.returncode == 0, baseline_result.stderr
   159|     baseline_sidecar = json.loads((out / "packets" / "packet_baseline.resolution.json").read_text(encoding="utf-8"))
   160|     no_expansion_tokens = baseline_sidecar["estimated_tokens_used"]
   161| 
   162|     # A budget comfortably above the no-expansion cost, but too small to
   163|     # also fit f's callee-expansion listing -- both explicit symbols must
   164|     # still render in full; only the expansion may be omitted.
   165|     req = _request(out, "req.json", {
   166|         "schema_version": "1.0", "question": "q",
   167|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"},
   168|                                                  {"name": "g", "file": "core/b.py"}], "search_terms": [], "lines": []},
   169|         "expansion": {"include_callers": False, "include_callees": True, "include_imports": False,
   170|                       "include_related_tests": False},
   171|         "limits": {"max_estimated_tokens": no_expansion_tokens + 10, "max_files": 12},
   172|     })
   173|     result = _packet(repo, out, req)
   174|     assert result.returncode == 0, result.stderr
   175|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   176|     assert "### Symbol: `f`" in text
   177|     assert "### Symbol: `g`" in text
   178|     assert "return 2" in text  # g's own body, never sacrificed for f's expansion
   179| 
   180| 
   181| def test_search_match_does_not_reserve_focus_file_slot_unless_rendered(repo, out):
   182|     # Regression: note_focus_file(rel) was called before checking whether
   183|     # the match's own rendered line fit the remaining budget, so a match
   184|     # that ultimately never appears in the packet could still consume the
   185|     # sole limits.max_files slot -- and the resolution sidecar would then
   186|     # misleadingly name that file as a "focus file" despite showing zero
   187|     # evidence for it.
   188|     write_files(repo, {"a.py": f"needle = {'x' * 500!r}\n"})
   189|     _scan(repo, out)
   190|     # 150 tokens is just above the fixed framing's own floor (header +
   191|     # resolution-report heading + footer + packet-size summary line must
   192|     # be reserved first -- see rc_request.py's "Reserve the fixed
   193|     # framing's budget cost up front") but well short of also fitting the
   194|     # ~500-char match line.
   195|     req = _request(out, "req.json", {
   196|         "schema_version": "1.0", "question": "q",
   197|         "selectors": {"files": [], "symbols": [], "search_terms": ["needle"], "lines": []},
   198|         "limits": {"max_estimated_tokens": 150, "max_files": 1},
   199|     })
   200|     result = _packet(repo, out, req)
   201|     assert result.returncode == 0, result.stderr
   202|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   203|     assert sidecar["focus_files"] == []
   204|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   205|     assert "a.py:1" not in text
   206| 
   207| 
   208| def test_duplicate_explicit_selectors_are_evaluated_once_not_per_occurrence(repo, out):
   209|     # Regression: a request naming the same file selector many times over
   210|     # re-attempted rendering (and, if it failed, re-appended an identical
   211|     # conflict message) once per occurrence -- an unbounded-output shape
   212|     # for a request that just repeats one selector, independent of the
   213|     # token-budget accounting fixes for distinct content.
   214|     write_files(repo, {"empty.py": ""})
   215|     _scan(repo, out)
   216|     req = _request(out, "req.json", {
   217|         "schema_version": "1.0", "question": "q",
   218|         "selectors": {"files": ["empty.py"] * 1000, "symbols": [], "search_terms": [], "lines": []},
   219|         "limits": {"max_estimated_tokens": 1, "max_files": 12},
   220|     })
   221|     result = _packet(repo, out, req)
   222|     assert result.returncode == 1
   223|     assert result.stderr.count("empty.py") <= 2  # one conflict line, not one per duplicate
   224| 
   225| 
   226| def test_explicit_selectors_beyond_max_files_is_a_hard_conflict_not_silent_drop(repo, out):
   227|     # Regression: naming more distinct explicit files than limits.max_files
   228|     # allows used to succeed with a partial packet, leaving the resolution
   229|     # report claiming "resolved" for files that were never actually
   230|     # rendered. This must behave like any other explicit-selector conflict:
   231|     # abort, report why, and write no packet at all.
   232|     write_files(repo, {
   233|         "a.py": "def f():\n    return 1\n",
   234|         "b.py": "def g():\n    return 2\n",
   235|     })
   236|     _scan(repo, out)
   237|     req = _request(out, "req.json", {
   238|         "schema_version": "1.0", "question": "q",
   239|         "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
   240|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   241|     })
   242|     result = _packet(repo, out, req)
   243|     assert result.returncode == 1
   244|     assert "max_files" in result.stderr
   245|     assert not (out / "packets" / "packet_req.md").exists()
   246| 
   247| 
   248| def test_strict_mode_catches_unresolved_search_terms(repo, out):
   249|     # Regression: search terms weren't part of all_resolutions at all, so
   250|     # strict mode couldn't abort on a zero-match term or an invalid regex.
   251|     write_files(repo, {"a.py": "def f():\n    return 1\n"})
   252|     _scan(repo, out)
   253|     req = _request(out, "req.json", {
   254|         "schema_version": "1.0", "question": "q", "strict": True,
   255|         "selectors": {"files": [], "symbols": [], "search_terms": ["no_such_term_anywhere"], "lines": []},
   256|     })
   257|     result = _packet(repo, out, req)
   258|     assert result.returncode == 1
   259|     assert "strict mode" in result.stderr
   260|     assert not (out / "packets" / "packet_req.md").exists()
   261| 
   262|     req2 = _request(out, "req2.json", {
   263|         "schema_version": "1.0", "question": "q", "strict": True,
   264|         "selectors": {"files": [], "symbols": [], "search_terms": ["("], "lines": []},
   265|         "expansion": {"search_as_regex": True},
   266|     })
   267|     result2 = _packet(repo, out, req2)
   268|     assert result2.returncode == 1
   269|     assert "strict mode" in result2.stderr
   270| 
   271| 
   272| def test_invalid_schema_version_is_rejected_before_resolution(repo, out):
   273|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   274|     _scan(repo, out)
   275|     req = _request(out, "req.json", {
   276|         "schema_version": "0.1", "question": "q",
   277|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   278|     })
   279|     result = _packet(repo, out, req)
   280|     assert result.returncode == 1
   281|     assert "invalid packet_request.json" in result.stderr
   282|     assert not (out / "packets" / "packet_req.md").exists()
   283| 
   284| 
   285| def test_path_traversal_selector_is_rejected(repo, out):
   286|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   287|     _scan(repo, out)
   288|     req = _request(out, "req.json", {
   289|         "schema_version": "1.0", "question": "q",
   290|         "selectors": {"files": ["../outside.py"], "symbols": [], "search_terms": [], "lines": []},
   291|     })
   292|     result = _packet(repo, out, req)
   293|     assert result.returncode == 1
   294|     assert "invalid packet_request.json" in result.stderr
   295| 
   296| 
   297| def test_search_term_matches_and_related_tests_are_included(repo, out):
   298|     write_files(repo, {
   299|         "core/a.py": "def f():\n    return 'needle_term'\n",
   300|         "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 'needle_term'\n",
   301|     })
   302|     _scan(repo, out)
   303|     req = _request(out, "req.json", {
   304|         "schema_version": "1.0", "question": "q",
   305|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": ["needle_term"], "lines": []},
   306|         "expansion": {"include_related_tests": True},
   307|     })
   308|     result = _packet(repo, out, req)
   309|     assert result.returncode == 0, result.stderr
   310|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   311|     assert "exact_search_match" in text
   312|     assert "tests/test_a.py" in text
   313| 
   314| 
   315| def test_line_selector_resolves_enclosing_symbol(repo, out):
   316|     write_files(repo, {"core/a.py": "def f():\n    x = 1\n    return x\n"})
   317|     _scan(repo, out)
   318|     req = _request(out, "req.json", {
   319|         "schema_version": "1.0", "question": "q",
   320|         "selectors": {"files": [], "symbols": [], "search_terms": [], "lines": [{"file": "core/a.py", "line": 2}]},
   321|     })
   322|     result = _packet(repo, out, req)
   323|     assert result.returncode == 0, result.stderr
   324|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   325|     assert "explicit_line_selector" in text
   326|     assert "def f():" in text
   327| 
   328| 
   329| def test_line_range_extending_past_enclosing_symbol_renders_in_full(repo, out):
   330|     # Regression: the enclosing-symbol lookup only checked that the
   331|     # requested range's *start* line fell inside a symbol, not that the
   332|     # symbol also contained the *end* line. A range starting inside a
   333|     # tiny function that ends immediately (line 2) but requested through
   334|     # line 7 got silently truncated to just that function's own bounds
   335|     # (line 2) -- the resolution report still claimed "resolved" while
   336|     # lines 3-7 were dropped entirely from the packet.
   337|     write_files(repo, {"core/a.py": (
   338|         "x = 0\n"             # 1
   339|         "def tiny(): pass\n"  # 2 (a symbol whose own bounds are just this one line)
   340|         "y = 1\n"             # 3
   341|         "z = 2\n"             # 4
   342|         "w = 3\n"             # 5
   343|         "v = 4\n"             # 6
   344|         "u = 5\n"             # 7
   345|     )})
   346|     _scan(repo, out)
   347|     req = _request(out, "req.json", {
   348|         "schema_version": "1.0", "question": "q",
   349|         "selectors": {"files": [], "symbols": [], "search_terms": [],
   350|                       "lines": [{"file": "core/a.py", "line": 2, "end_line": 7}]},
   351|     })
   352|     result = _packet(repo, out, req)
   353|     assert result.returncode == 0, result.stderr
   354|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   355|     # The full requested range must render -- not truncated to `tiny`'s
   356|     # own 1-line bounds.
   357|     assert "def tiny(): pass" in text
   358|     assert "u = 5" in text
   359|     assert "Enclosing symbol:" not in text  # no symbol contains both endpoints
   360| 
   361| 
   362| def test_enclosing_symbol_note_is_charged_against_budget(repo, out):
   363|     # Regression: the "Enclosing symbol: ..." metadata line for a line
   364|     # selector was appended with no budget.allow()/spend() at all -- a
   365|     # long qualified name could make the actual packet bigger than the
   366|     # sidecar's reported estimated_tokens_used implied.
   367|     write_files(repo, {"core/a.py": "def " + "x" * 80 + "():\n    y = 1\n    return y\n"})
   368|     _scan(repo, out)
   369|     req = _request(out, "req.json", {
   370|         "schema_version": "1.0", "question": "q",
   371|         "selectors": {"files": [], "symbols": [], "search_terms": [], "lines": [{"file": "core/a.py", "line": 2}]},
   372|     })
   373|     result = _packet(repo, out, req)
   374|     assert result.returncode == 0, result.stderr
   375|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   376|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   377|     assert "Enclosing symbol:" in text
   378|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   379| 
   380| 
   381| def test_search_match_collection_is_capped(repo, out):
   382|     # Regression: every matching line was collected into an in-memory
   383|     # list before max_files/the packet budget were ever applied during
   384|     # Tier-2 rendering -- a common term across a large repository could
   385|     # accumulate an unbounded number of tuples. The direct --search
   386|     # packet path already caps collection at max_files * 5; the request
   387|     # path needs the same bound.
   388|     files = {f"core/mod_{i:03d}.py": "needle\n" * 50 for i in range(50)}
   389|     write_files(repo, files)
   390|     _scan(repo, out)
   391|     files_rows = rr._load_csv(out / "file_inventory.csv")
   392|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["needle"], False, files_rows, 3)
   393|     assert len(matches_by_term["needle"]) <= 3 * 5
   394| 
   395| 
   396| def test_redacted_excerpt_is_charged_not_the_raw_source(repo, out):
   397|     # Regression: budget.allow() was checked against the *raw* excerpt
   398|     # text, and redact_secrets() (which can make a secret-shaped value
   399|     # longer via its placeholder) only ran afterward on content already
   400|     # verified to fit -- so the actually-written (redacted) body could
   401|     # end up bigger than what the budget check had approved.
   402|     # The assignment key must be literally "token" (etc.) immediately
   403|     # followed by `=`/`:` for _SECRET_ASSIGNMENT_PATTERN to match at all
   404|     # -- a value just short enough that the fixed-length
   405|     # "[REDACTED-POSSIBLE-SECRET]" placeholder (26 chars) is *longer*
   406|     # than the whole original "token = '...'" span it replaces (~22
   407|     # chars for a 12-char value), so redaction measurably grows the text.
   408|     lines = [f"token = 'abcdefghi{i:03d}'" for i in range(100)]
   409|     write_files(repo, {"core/a.py": "\n".join(lines) + "\n"})
   410|     _scan(repo, out)
   411|     # Measure the real (redacted) cost via a generous run rather than
   412|     # guessing a token limit -- an explicit file selector hard-aborts the
   413|     # whole packet if it doesn't fit (a separate, correct invariant), so
   414|     # the budget must be sized to comfortably fit the redacted excerpt.
   415|     generous_req = _request(out, "generous.json", {
   416|         "schema_version": "1.0", "question": "q",
   417|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   418|         "limits": {"max_estimated_tokens": 200000, "max_files": 12},
   419|     })
   420|     generous_result = _packet(repo, out, generous_req)
   421|     assert generous_result.returncode == 0, generous_result.stderr
   422|     full_tokens = json.loads(
   423|         (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
   424|     )["estimated_tokens_used"]
   425|     req = _request(out, "req.json", {
   426|         "schema_version": "1.0", "question": "q",
   427|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   428|         "limits": {"max_estimated_tokens": full_tokens + 10, "max_files": 12},
   429|     })
   430|     result = _packet(repo, out, req)
   431|     assert result.returncode == 0, result.stderr
   432|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   433|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   434|     # The reported usage must not understate the packet's actual size --
   435|     # this bug's own repro showed ~890 tokens actually used while a 750
   436|     # limit was reported as satisfied.
   437|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   438| 
   439| 
   440| def test_regex_search_rejected_when_bounding_is_unsupported(repo, out, monkeypatch):
   441|     # Regression: on a platform without SIGALRM (Windows), search_as_regex
   442|     # fell back to running the pattern completely unbounded -- exactly
   443|     # the hang this whole mechanism exists to prevent, just gated behind
   444|     # a platform check instead of being fixed. Simulate that platform by
   445|     # removing signal.SIGALRM for the duration of this test.
   446|     import signal
   447|     monkeypatch.delattr(signal, "SIGALRM", raising=False)
   448|     write_files(repo, {"core/a.py": "hello\n"})
   449|     _scan(repo, out)
   450|     files_rows = rr._load_csv(out / "file_inventory.csv")
   451|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["(a+)+$"], True, files_rows, 12)
   452|     assert resolutions[0].status == "invalid"
   453|     assert "SIGALRM" in resolutions[0].detail or "unbounded" in resolutions[0].detail.lower()
   454|     assert matches_by_term["(a+)+$"] == []
   455| 
   456| 
   457| def test_aggregate_search_deadline_applies_to_literal_terms_too(repo, out, monkeypatch):
   458|     # Regression: the aggregate wall-clock deadline only applied when
   459|     # search_as_regex was true. A request with hundreds/thousands of
   460|     # absent *literal* terms re-reads every included text file once per
   461|     # term with no bound at all, since collect_cap only limits how many
   462|     # matches pile up, not how many full scans happen for a term that
   463|     # matches nothing. The deadline must apply regardless of
   464|     # search_as_regex.
   465|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS", 0)
   466|     write_files(repo, {"core/a.py": "needle\n"})
   467|     _scan(repo, out)
   468|     files_rows = rr._load_csv(out / "file_inventory.csv")
   469|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["needle", "other"], False, files_rows, 12)
   470|     assert all(r.status == "invalid" for r in resolutions)
   471|     assert all("aggregate" in r.detail for r in resolutions)
   472| 
   473| 
   474| def test_search_match_redacts_before_truncating(repo, out):
   475|     # Regression: the rendered search-match line truncated to 200 chars
   476|     # *before* calling redact_secrets(). A secret-shaped value whose
   477|     # closing quote fell beyond character 200 had that quote cut off
   478|     # first, breaking _SECRET_ASSIGNMENT_PATTERN's closing-quote
   479|     # backreference -- redact_secrets() then never matched at all, and
   480|     # the (truncated) secret prefix leaked into the packet unredacted.
   481|     secret_value = "x" * 250
   482|     write_files(repo, {"core/a.py": f'token = "{secret_value}"  # NEEDLE_MARKER\n'})
   483|     _scan(repo, out)
   484|     req = _request(out, "req.json", {
   485|         "schema_version": "1.0", "question": "q",
   486|         "selectors": {"files": [], "symbols": [], "search_terms": ["NEEDLE_MARKER"], "lines": []},
   487|     })
   488|     result = _packet(repo, out, req)
   489|     assert result.returncode == 0, result.stderr
   490|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   491|     assert "xxxxxxxxxx" not in text
   492|     assert "REDACTED" in text
   493| 
   494| 
```
