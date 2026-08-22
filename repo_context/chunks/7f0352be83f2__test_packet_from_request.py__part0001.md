# Chunk of dev_tools/repo_context/tests/test_packet_from_request.py

- Source relative path: `dev_tools/repo_context/tests/test_packet_from_request.py`
- Chunk: 1 of 3
- Original line range: 1-512
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _scan, _request, _packet, test_valid_request_resolves_file_and_symbol_selectors, test_ambiguous_symbol_is_reported_not_silently_resolved, test_qualified_symbol_via_file_field_resolves_unambiguously, test_missing_selector_is_reported_but_other_selectors_still_processed, test_strict_mode_aborts_on_any_unresolved_selector, test_hard_budget_conflict_on_explicit_selector_aborts_without_partial_packet, test_expansion_never_preempts_a_later_explicit_selector, test_search_match_does_not_reserve_focus_file_slot_unless_rendered, test_duplicate_explicit_selectors_are_evaluated_once_not_per_occurrence, test_explicit_selectors_beyond_max_files_is_a_hard_conflict_not_silent_drop, test_strict_mode_catches_unresolved_search_terms, test_invalid_schema_version_is_rejected_before_resolution, test_path_traversal_selector_is_rejected, test_search_term_matches_and_related_tests_are_included, test_line_selector_resolves_enclosing_symbol, test_line_range_extending_past_enclosing_symbol_renders_in_full, test_enclosing_symbol_note_is_charged_against_budget, test_search_match_collection_is_capped, test_redacted_excerpt_is_charged_not_the_raw_source, test_regex_search_rejected_when_bounding_is_unsupported, test_aggregate_search_deadline_applies_to_literal_terms_too, test_search_match_redacts_before_truncating, test_packet_header_and_footer_charged_against_budget
- Source SHA-256: 30ed034adfbd24213b55c30a03d99cc5f8036eb9a7f7a36502023450b6d37a45
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| import json
     2| import time
     3| 
     4| from conftest import run_tool, write_files  # noqa: F401 -- conftest import also puts TOOL_DIR on sys.path
     5| import rc_request as rr
     6| 
     7| 
     8| def _scan(repo, out):
     9|     result = run_tool(["scan", str(repo), "--output", str(out)])
    10|     assert result.returncode == 0, result.stderr
    11| 
    12| 
    13| def _request(out, name, data):
    14|     path = out / name
    15|     path.write_text(json.dumps(data), encoding="utf-8")
    16|     return path
    17| 
    18| 
    19| def _packet(repo, out, request_path, extra=None):
    20|     return run_tool(["packet", str(repo), "--output", str(out), "--request", str(request_path)] + (extra or []))
    21| 
    22| 
    23| def test_valid_request_resolves_file_and_symbol_selectors(repo, out):
    24|     write_files(repo, {
    25|         "core/helper.py": "def add(a, b):\n    return a + b\n",
    26|         "tools/report.py": "from core.helper import add\n\n\ndef build():\n    return add(1, 2)\n",
    27|     })
    28|     _scan(repo, out)
    29|     req = _request(out, "req.json", {
    30|         "schema_version": "1.0", "question": "How is build computed?",
    31|         "selectors": {"files": [], "symbols": [{"name": "build"}], "search_terms": [], "lines": []},
    32|     })
    33|     result = _packet(repo, out, req)
    34|     assert result.returncode == 0, result.stderr
    35|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    36|     assert "resolved" in text
    37|     assert "def build():" in text
    38|     assert "explicit_symbol_selector" in text
    39|     assert "caller_expansion" not in text  # nothing calls build()
    40|     assert "callee_expansion" in text  # build() calls add()
    41| 
    42| 
    43| def test_ambiguous_symbol_is_reported_not_silently_resolved(repo, out):
    44|     write_files(repo, {
    45|         "core/a.py": "def dup():\n    return 1\n",
    46|         "core/b.py": "def dup():\n    return 2\n",
    47|     })
    48|     _scan(repo, out)
    49|     req = _request(out, "req.json", {
    50|         "schema_version": "1.0", "question": "what does dup do",
    51|         "selectors": {"files": [], "symbols": [{"name": "dup"}], "search_terms": [], "lines": []},
    52|     })
    53|     result = _packet(repo, out, req)
    54|     assert result.returncode == 0, result.stderr
    55|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
    56|     [res] = sidecar["resolution_report"]
    57|     assert res["status"] == "ambiguous"
    58|     assert len(res["candidates"]) == 2
    59|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    60|     assert "core/a.py" in text and "core/b.py" in text  # both candidates surfaced
    61| 
    62| 
    63| def test_qualified_symbol_via_file_field_resolves_unambiguously(repo, out):
    64|     write_files(repo, {
    65|         "core/a.py": "def dup():\n    return 1\n",
    66|         "core/b.py": "def dup():\n    return 2\n",
    67|     })
    68|     _scan(repo, out)
    69|     req = _request(out, "req.json", {
    70|         "schema_version": "1.0", "question": "what does dup do",
    71|         "selectors": {"files": [], "symbols": [{"name": "dup", "file": "core/b.py"}], "search_terms": [], "lines": []},
    72|     })
    73|     result = _packet(repo, out, req)
    74|     assert result.returncode == 0, result.stderr
    75|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    76|     assert "return 2" in text
    77|     assert "return 1" not in text
    78| 
    79| 
    80| def test_missing_selector_is_reported_but_other_selectors_still_processed(repo, out):
    81|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    82|     _scan(repo, out)
    83|     req = _request(out, "req.json", {
    84|         "schema_version": "1.0", "question": "q",
    85|         "selectors": {"files": ["core/a.py"], "symbols": [{"name": "does_not_exist"}], "search_terms": [], "lines": []},
    86|     })
    87|     result = _packet(repo, out, req)
    88|     assert result.returncode == 0, result.stderr
    89|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
    90|     assert "def f():" in text  # the valid file selector still got processed
    91|     assert "missing" in text
    92|     assert "does_not_exist" in text
    93| 
    94| 
    95| def test_strict_mode_aborts_on_any_unresolved_selector(repo, out):
    96|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    97|     _scan(repo, out)
    98|     req = _request(out, "req.json", {
    99|         "schema_version": "1.0", "question": "q", "strict": True,
   100|         "selectors": {"files": ["core/a.py"], "symbols": [{"name": "does_not_exist"}], "search_terms": [], "lines": []},
   101|     })
   102|     result = _packet(repo, out, req)
   103|     assert result.returncode == 1
   104|     assert "strict mode" in result.stderr
   105|     assert not (out / "packets" / "packet_req.md").exists()
   106| 
   107| 
   108| def test_hard_budget_conflict_on_explicit_selector_aborts_without_partial_packet(repo, out):
   109|     lines = []
   110|     for i in range(300):
   111|         lines += [f"def func_{i}():", f"    return {i}", ""]
   112|     write_files(repo, {"big.py": "\n".join(lines) + "\n"})
   113|     _scan(repo, out)
   114|     req = _request(out, "req.json", {
   115|         "schema_version": "1.0", "question": "q",
   116|         "selectors": {"files": ["big.py"], "symbols": [], "search_terms": [], "lines": []},
   117|         "limits": {"max_estimated_tokens": 1, "max_files": 12},
   118|     })
   119|     result = _packet(repo, out, req)
   120|     assert result.returncode == 1
   121|     assert "do not fit" in result.stderr
   122|     assert not (out / "packets" / "packet_req.md").exists()
   123| 
   124| 
   125| def test_expansion_never_preempts_a_later_explicit_selector(repo, out):
   126|     # Regression: expansions (callers/callees/imports/tests) for one
   127|     # explicit symbol were rendered immediately after it and before the
   128|     # *next* explicit selector got its turn, so a budget that easily fits
   129|     # every explicit selector's own content could still fail if an early
   130|     # selector's expansion ate the remaining room first. Explicit content
   131|     # must all be attempted before any expansion spends a single char.
   132|     write_files(repo, {
   133|         "core/a.py": "def h():\n    return 42\n\n\ndef f():\n    return h()\n",
   134|         "core/b.py": "def g():\n    return 2\n",
   135|     })
   136|     _scan(repo, out)
   137| 
   138|     # Establish the true no-expansion cost for both explicit symbols. Use a
   139|     # generous budget for this measurement run -- the fixed framing
   140|     # (header/resolution-report/footer) is now reserved against the budget
   141|     # up front (see rc_request.py's "Reserve the fixed framing's budget
   142|     # cost up front"), so a too-tight value here would fail on framing
   143|     # alone rather than actually measuring the two symbols' cost.
   144|     baseline_req = _request(out, "baseline.json", {
   145|         "schema_version": "1.0", "question": "q",
   146|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"},
   147|                                                  {"name": "g", "file": "core/b.py"}], "search_terms": [], "lines": []},
   148|         "expansion": {"include_callers": False, "include_callees": False, "include_imports": False,
   149|                       "include_related_tests": False},
   150|         "limits": {"max_estimated_tokens": 1000, "max_files": 12},
   151|     })
   152|     baseline_result = _packet(repo, out, baseline_req)
   153|     assert baseline_result.returncode == 0, baseline_result.stderr
   154|     baseline_sidecar = json.loads((out / "packets" / "packet_baseline.resolution.json").read_text(encoding="utf-8"))
   155|     no_expansion_tokens = baseline_sidecar["estimated_tokens_used"]
   156| 
   157|     # A budget comfortably above the no-expansion cost, but too small to
   158|     # also fit f's callee-expansion listing -- both explicit symbols must
   159|     # still render in full; only the expansion may be omitted.
   160|     req = _request(out, "req.json", {
   161|         "schema_version": "1.0", "question": "q",
   162|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"},
   163|                                                  {"name": "g", "file": "core/b.py"}], "search_terms": [], "lines": []},
   164|         "expansion": {"include_callers": False, "include_callees": True, "include_imports": False,
   165|                       "include_related_tests": False},
   166|         "limits": {"max_estimated_tokens": no_expansion_tokens + 10, "max_files": 12},
   167|     })
   168|     result = _packet(repo, out, req)
   169|     assert result.returncode == 0, result.stderr
   170|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   171|     assert "### Symbol: `f`" in text
   172|     assert "### Symbol: `g`" in text
   173|     assert "return 2" in text  # g's own body, never sacrificed for f's expansion
   174| 
   175| 
   176| def test_search_match_does_not_reserve_focus_file_slot_unless_rendered(repo, out):
   177|     # Regression: note_focus_file(rel) was called before checking whether
   178|     # the match's own rendered line fit the remaining budget, so a match
   179|     # that ultimately never appears in the packet could still consume the
   180|     # sole limits.max_files slot -- and the resolution sidecar would then
   181|     # misleadingly name that file as a "focus file" despite showing zero
   182|     # evidence for it.
   183|     write_files(repo, {"a.py": f"needle = {'x' * 500!r}\n"})
   184|     _scan(repo, out)
   185|     req = _request(out, "req.json", {
   186|         "schema_version": "1.0", "question": "q",
   187|         "selectors": {"files": [], "symbols": [], "search_terms": ["needle"], "lines": []},
   188|         "limits": {"max_estimated_tokens": 10, "max_files": 1},
   189|     })
   190|     result = _packet(repo, out, req)
   191|     assert result.returncode == 0, result.stderr
   192|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   193|     assert sidecar["focus_files"] == []
   194|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   195|     assert "a.py:1" not in text
   196| 
   197| 
   198| def test_duplicate_explicit_selectors_are_evaluated_once_not_per_occurrence(repo, out):
   199|     # Regression: a request naming the same file selector many times over
   200|     # re-attempted rendering (and, if it failed, re-appended an identical
   201|     # conflict message) once per occurrence -- an unbounded-output shape
   202|     # for a request that just repeats one selector, independent of the
   203|     # token-budget accounting fixes for distinct content.
   204|     write_files(repo, {"empty.py": ""})
   205|     _scan(repo, out)
   206|     req = _request(out, "req.json", {
   207|         "schema_version": "1.0", "question": "q",
   208|         "selectors": {"files": ["empty.py"] * 1000, "symbols": [], "search_terms": [], "lines": []},
   209|         "limits": {"max_estimated_tokens": 1, "max_files": 12},
   210|     })
   211|     result = _packet(repo, out, req)
   212|     assert result.returncode == 1
   213|     assert result.stderr.count("empty.py") <= 2  # one conflict line, not one per duplicate
   214| 
   215| 
   216| def test_explicit_selectors_beyond_max_files_is_a_hard_conflict_not_silent_drop(repo, out):
   217|     # Regression: naming more distinct explicit files than limits.max_files
   218|     # allows used to succeed with a partial packet, leaving the resolution
   219|     # report claiming "resolved" for files that were never actually
   220|     # rendered. This must behave like any other explicit-selector conflict:
   221|     # abort, report why, and write no packet at all.
   222|     write_files(repo, {
   223|         "a.py": "def f():\n    return 1\n",
   224|         "b.py": "def g():\n    return 2\n",
   225|     })
   226|     _scan(repo, out)
   227|     req = _request(out, "req.json", {
   228|         "schema_version": "1.0", "question": "q",
   229|         "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
   230|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   231|     })
   232|     result = _packet(repo, out, req)
   233|     assert result.returncode == 1
   234|     assert "max_files" in result.stderr
   235|     assert not (out / "packets" / "packet_req.md").exists()
   236| 
   237| 
   238| def test_strict_mode_catches_unresolved_search_terms(repo, out):
   239|     # Regression: search terms weren't part of all_resolutions at all, so
   240|     # strict mode couldn't abort on a zero-match term or an invalid regex.
   241|     write_files(repo, {"a.py": "def f():\n    return 1\n"})
   242|     _scan(repo, out)
   243|     req = _request(out, "req.json", {
   244|         "schema_version": "1.0", "question": "q", "strict": True,
   245|         "selectors": {"files": [], "symbols": [], "search_terms": ["no_such_term_anywhere"], "lines": []},
   246|     })
   247|     result = _packet(repo, out, req)
   248|     assert result.returncode == 1
   249|     assert "strict mode" in result.stderr
   250|     assert not (out / "packets" / "packet_req.md").exists()
   251| 
   252|     req2 = _request(out, "req2.json", {
   253|         "schema_version": "1.0", "question": "q", "strict": True,
   254|         "selectors": {"files": [], "symbols": [], "search_terms": ["("], "lines": []},
   255|         "expansion": {"search_as_regex": True},
   256|     })
   257|     result2 = _packet(repo, out, req2)
   258|     assert result2.returncode == 1
   259|     assert "strict mode" in result2.stderr
   260| 
   261| 
   262| def test_invalid_schema_version_is_rejected_before_resolution(repo, out):
   263|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   264|     _scan(repo, out)
   265|     req = _request(out, "req.json", {
   266|         "schema_version": "0.1", "question": "q",
   267|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   268|     })
   269|     result = _packet(repo, out, req)
   270|     assert result.returncode == 1
   271|     assert "invalid packet_request.json" in result.stderr
   272|     assert not (out / "packets" / "packet_req.md").exists()
   273| 
   274| 
   275| def test_path_traversal_selector_is_rejected(repo, out):
   276|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   277|     _scan(repo, out)
   278|     req = _request(out, "req.json", {
   279|         "schema_version": "1.0", "question": "q",
   280|         "selectors": {"files": ["../outside.py"], "symbols": [], "search_terms": [], "lines": []},
   281|     })
   282|     result = _packet(repo, out, req)
   283|     assert result.returncode == 1
   284|     assert "invalid packet_request.json" in result.stderr
   285| 
   286| 
   287| def test_search_term_matches_and_related_tests_are_included(repo, out):
   288|     write_files(repo, {
   289|         "core/a.py": "def f():\n    return 'needle_term'\n",
   290|         "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 'needle_term'\n",
   291|     })
   292|     _scan(repo, out)
   293|     req = _request(out, "req.json", {
   294|         "schema_version": "1.0", "question": "q",
   295|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": ["needle_term"], "lines": []},
   296|         "expansion": {"include_related_tests": True},
   297|     })
   298|     result = _packet(repo, out, req)
   299|     assert result.returncode == 0, result.stderr
   300|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   301|     assert "exact_search_match" in text
   302|     assert "tests/test_a.py" in text
   303| 
   304| 
   305| def test_line_selector_resolves_enclosing_symbol(repo, out):
   306|     write_files(repo, {"core/a.py": "def f():\n    x = 1\n    return x\n"})
   307|     _scan(repo, out)
   308|     req = _request(out, "req.json", {
   309|         "schema_version": "1.0", "question": "q",
   310|         "selectors": {"files": [], "symbols": [], "search_terms": [], "lines": [{"file": "core/a.py", "line": 2}]},
   311|     })
   312|     result = _packet(repo, out, req)
   313|     assert result.returncode == 0, result.stderr
   314|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   315|     assert "explicit_line_selector" in text
   316|     assert "def f():" in text
   317| 
   318| 
   319| def test_line_range_extending_past_enclosing_symbol_renders_in_full(repo, out):
   320|     # Regression: the enclosing-symbol lookup only checked that the
   321|     # requested range's *start* line fell inside a symbol, not that the
   322|     # symbol also contained the *end* line. A range starting inside a
   323|     # tiny function that ends immediately (line 2) but requested through
   324|     # line 7 got silently truncated to just that function's own bounds
   325|     # (line 2) -- the resolution report still claimed "resolved" while
   326|     # lines 3-7 were dropped entirely from the packet.
   327|     write_files(repo, {"core/a.py": (
   328|         "x = 0\n"             # 1
   329|         "def tiny(): pass\n"  # 2 (a symbol whose own bounds are just this one line)
   330|         "y = 1\n"             # 3
   331|         "z = 2\n"             # 4
   332|         "w = 3\n"             # 5
   333|         "v = 4\n"             # 6
   334|         "u = 5\n"             # 7
   335|     )})
   336|     _scan(repo, out)
   337|     req = _request(out, "req.json", {
   338|         "schema_version": "1.0", "question": "q",
   339|         "selectors": {"files": [], "symbols": [], "search_terms": [],
   340|                       "lines": [{"file": "core/a.py", "line": 2, "end_line": 7}]},
   341|     })
   342|     result = _packet(repo, out, req)
   343|     assert result.returncode == 0, result.stderr
   344|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   345|     # The full requested range must render -- not truncated to `tiny`'s
   346|     # own 1-line bounds.
   347|     assert "def tiny(): pass" in text
   348|     assert "u = 5" in text
   349|     assert "Enclosing symbol:" not in text  # no symbol contains both endpoints
   350| 
   351| 
   352| def test_enclosing_symbol_note_is_charged_against_budget(repo, out):
   353|     # Regression: the "Enclosing symbol: ..." metadata line for a line
   354|     # selector was appended with no budget.allow()/spend() at all -- a
   355|     # long qualified name could make the actual packet bigger than the
   356|     # sidecar's reported estimated_tokens_used implied.
   357|     write_files(repo, {"core/a.py": "def " + "x" * 80 + "():\n    y = 1\n    return y\n"})
   358|     _scan(repo, out)
   359|     req = _request(out, "req.json", {
   360|         "schema_version": "1.0", "question": "q",
   361|         "selectors": {"files": [], "symbols": [], "search_terms": [], "lines": [{"file": "core/a.py", "line": 2}]},
   362|     })
   363|     result = _packet(repo, out, req)
   364|     assert result.returncode == 0, result.stderr
   365|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   366|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   367|     assert "Enclosing symbol:" in text
   368|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   369| 
   370| 
   371| def test_search_match_collection_is_capped(repo, out):
   372|     # Regression: every matching line was collected into an in-memory
   373|     # list before max_files/the packet budget were ever applied during
   374|     # Tier-2 rendering -- a common term across a large repository could
   375|     # accumulate an unbounded number of tuples. The direct --search
   376|     # packet path already caps collection at max_files * 5; the request
   377|     # path needs the same bound.
   378|     files = {f"core/mod_{i:03d}.py": "needle\n" * 50 for i in range(50)}
   379|     write_files(repo, files)
   380|     _scan(repo, out)
   381|     files_rows = rr._load_csv(out / "file_inventory.csv")
   382|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["needle"], False, files_rows, 3)
   383|     assert len(matches_by_term["needle"]) <= 3 * 5
   384| 
   385| 
   386| def test_redacted_excerpt_is_charged_not_the_raw_source(repo, out):
   387|     # Regression: budget.allow() was checked against the *raw* excerpt
   388|     # text, and redact_secrets() (which can make a secret-shaped value
   389|     # longer via its placeholder) only ran afterward on content already
   390|     # verified to fit -- so the actually-written (redacted) body could
   391|     # end up bigger than what the budget check had approved.
   392|     # The assignment key must be literally "token" (etc.) immediately
   393|     # followed by `=`/`:` for _SECRET_ASSIGNMENT_PATTERN to match at all
   394|     # -- a value just short enough that the fixed-length
   395|     # "[REDACTED-POSSIBLE-SECRET]" placeholder (26 chars) is *longer*
   396|     # than the whole original "token = '...'" span it replaces (~22
   397|     # chars for a 12-char value), so redaction measurably grows the text.
   398|     lines = [f"token = 'abcdefghi{i:03d}'" for i in range(100)]
   399|     write_files(repo, {"core/a.py": "\n".join(lines) + "\n"})
   400|     _scan(repo, out)
   401|     # Measure the real (redacted) cost via a generous run rather than
   402|     # guessing a token limit -- an explicit file selector hard-aborts the
   403|     # whole packet if it doesn't fit (a separate, correct invariant), so
   404|     # the budget must be sized to comfortably fit the redacted excerpt.
   405|     generous_req = _request(out, "generous.json", {
   406|         "schema_version": "1.0", "question": "q",
   407|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   408|         "limits": {"max_estimated_tokens": 200000, "max_files": 12},
   409|     })
   410|     generous_result = _packet(repo, out, generous_req)
   411|     assert generous_result.returncode == 0, generous_result.stderr
   412|     full_tokens = json.loads(
   413|         (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
   414|     )["estimated_tokens_used"]
   415|     req = _request(out, "req.json", {
   416|         "schema_version": "1.0", "question": "q",
   417|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   418|         "limits": {"max_estimated_tokens": full_tokens + 10, "max_files": 12},
   419|     })
   420|     result = _packet(repo, out, req)
   421|     assert result.returncode == 0, result.stderr
   422|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   423|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   424|     # The reported usage must not understate the packet's actual size --
   425|     # this bug's own repro showed ~890 tokens actually used while a 750
   426|     # limit was reported as satisfied.
   427|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   428| 
   429| 
   430| def test_regex_search_rejected_when_bounding_is_unsupported(repo, out, monkeypatch):
   431|     # Regression: on a platform without SIGALRM (Windows), search_as_regex
   432|     # fell back to running the pattern completely unbounded -- exactly
   433|     # the hang this whole mechanism exists to prevent, just gated behind
   434|     # a platform check instead of being fixed. Simulate that platform by
   435|     # removing signal.SIGALRM for the duration of this test.
   436|     import signal
   437|     monkeypatch.delattr(signal, "SIGALRM", raising=False)
   438|     write_files(repo, {"core/a.py": "hello\n"})
   439|     _scan(repo, out)
   440|     files_rows = rr._load_csv(out / "file_inventory.csv")
   441|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["(a+)+$"], True, files_rows, 12)
   442|     assert resolutions[0].status == "invalid"
   443|     assert "SIGALRM" in resolutions[0].detail or "unbounded" in resolutions[0].detail.lower()
   444|     assert matches_by_term["(a+)+$"] == []
   445| 
   446| 
   447| def test_aggregate_search_deadline_applies_to_literal_terms_too(repo, out, monkeypatch):
   448|     # Regression: the aggregate wall-clock deadline only applied when
   449|     # search_as_regex was true. A request with hundreds/thousands of
   450|     # absent *literal* terms re-reads every included text file once per
   451|     # term with no bound at all, since collect_cap only limits how many
   452|     # matches pile up, not how many full scans happen for a term that
   453|     # matches nothing. The deadline must apply regardless of
   454|     # search_as_regex.
   455|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS", 0)
   456|     write_files(repo, {"core/a.py": "needle\n"})
   457|     _scan(repo, out)
   458|     files_rows = rr._load_csv(out / "file_inventory.csv")
   459|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["needle", "other"], False, files_rows, 12)
   460|     assert all(r.status == "invalid" for r in resolutions)
   461|     assert all("aggregate" in r.detail for r in resolutions)
   462| 
   463| 
   464| def test_search_match_redacts_before_truncating(repo, out):
   465|     # Regression: the rendered search-match line truncated to 200 chars
   466|     # *before* calling redact_secrets(). A secret-shaped value whose
   467|     # closing quote fell beyond character 200 had that quote cut off
   468|     # first, breaking _SECRET_ASSIGNMENT_PATTERN's closing-quote
   469|     # backreference -- redact_secrets() then never matched at all, and
   470|     # the (truncated) secret prefix leaked into the packet unredacted.
   471|     secret_value = "x" * 250
   472|     write_files(repo, {"core/a.py": f'token = "{secret_value}"  # NEEDLE_MARKER\n'})
   473|     _scan(repo, out)
   474|     req = _request(out, "req.json", {
   475|         "schema_version": "1.0", "question": "q",
   476|         "selectors": {"files": [], "symbols": [], "search_terms": ["NEEDLE_MARKER"], "lines": []},
   477|     })
   478|     result = _packet(repo, out, req)
   479|     assert result.returncode == 0, result.stderr
   480|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   481|     assert "xxxxxxxxxx" not in text
   482|     assert "REDACTED" in text
   483| 
   484| 
   485| def test_packet_header_and_footer_charged_against_budget(repo, out):
   486|     # Regression: the fixed header (title/root/question/provenance/
   487|     # limits) and footer were written with no budget accounting
   488|     # whatsoever -- an accepted (<=4000-char) question alone could make
   489|     # the real packet many times bigger than limits.max_estimated_tokens
   490|     # while the sidecar still reported a number near zero.
   491|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   492|     _scan(repo, out)
   493|     long_question = "why? " * 700  # comfortably under MAX_QUESTION_LENGTH (4000), still substantial
   494|     # A search-term selector, not an explicit file/symbol/line selector --
   495|     # the latter now correctly hard-aborts the whole packet if it can't
   496|     # fit alongside the (now-charged) header, which is a separate, correct
   497|     # invariant this test isn't about. A search term is soft/omittable, so
   498|     # the packet still succeeds even when the header alone consumes most
   499|     # of an unreasonably tiny budget.
   500|     req = _request(out, "req.json", {
   501|         "schema_version": "1.0", "question": long_question,
   502|         "selectors": {"files": [], "symbols": [], "search_terms": ["nonexistent_term_xyz"], "lines": []},
   503|         "limits": {"max_estimated_tokens": 1, "max_files": 12},
   504|     })
   505|     result = _packet(repo, out, req)
   506|     assert result.returncode == 0, result.stderr
   507|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   508|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   509|     assert sidecar["estimated_tokens_used"] > 100  # the question alone is ~700+ chars
   510|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   511| 
   512| 
```
