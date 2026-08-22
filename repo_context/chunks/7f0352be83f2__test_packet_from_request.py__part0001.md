# Chunk of dev_tools/repo_context/tests/test_packet_from_request.py

- Source relative path: `dev_tools/repo_context/tests/test_packet_from_request.py`
- Chunk: 1 of 3
- Original line range: 1-520
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _scan, _request, _packet, test_valid_request_resolves_file_and_symbol_selectors, test_ambiguous_symbol_is_reported_not_silently_resolved, test_qualified_symbol_via_file_field_resolves_unambiguously, test_missing_selector_is_reported_but_other_selectors_still_processed, test_strict_mode_aborts_on_any_unresolved_selector, test_hard_budget_conflict_on_explicit_selector_aborts_without_partial_packet, test_expansion_never_preempts_a_later_explicit_selector, test_search_match_does_not_reserve_focus_file_slot_unless_rendered, test_duplicate_explicit_selectors_are_evaluated_once_not_per_occurrence, test_explicit_selectors_beyond_max_files_is_a_hard_conflict_not_silent_drop, test_strict_mode_catches_unresolved_search_terms, test_invalid_schema_version_is_rejected_before_resolution, test_path_traversal_selector_is_rejected, test_search_term_matches_and_related_tests_are_included, test_line_selector_resolves_enclosing_symbol, test_line_range_extending_past_enclosing_symbol_renders_in_full, test_enclosing_symbol_note_is_charged_against_budget, test_search_match_collection_is_capped, test_redacted_excerpt_is_charged_not_the_raw_source, test_regex_search_rejected_when_bounding_is_unsupported, test_aggregate_search_deadline_applies_to_literal_terms_too, test_search_match_redacts_before_truncating, test_packet_header_and_footer_charged_against_budget, test_stale_source_since_scan_withholds_excerpt
- Source SHA-256: 759d9ce2ca0219228b05d56db69e1b045fd96066288de392f13f77203a94949c
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
   138|     # Establish the true no-expansion cost for both explicit symbols.
   139|     baseline_req = _request(out, "baseline.json", {
   140|         "schema_version": "1.0", "question": "q",
   141|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"},
   142|                                                  {"name": "g", "file": "core/b.py"}], "search_terms": [], "lines": []},
   143|         "expansion": {"include_callers": False, "include_callees": False, "include_imports": False,
   144|                       "include_related_tests": False},
   145|         "limits": {"max_estimated_tokens": 200, "max_files": 12},
   146|     })
   147|     baseline_result = _packet(repo, out, baseline_req)
   148|     assert baseline_result.returncode == 0, baseline_result.stderr
   149|     baseline_sidecar = json.loads((out / "packets" / "packet_baseline.resolution.json").read_text(encoding="utf-8"))
   150|     no_expansion_tokens = baseline_sidecar["estimated_tokens_used"]
   151| 
   152|     # A budget comfortably above the no-expansion cost, but too small to
   153|     # also fit f's callee-expansion listing -- both explicit symbols must
   154|     # still render in full; only the expansion may be omitted.
   155|     req = _request(out, "req.json", {
   156|         "schema_version": "1.0", "question": "q",
   157|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"},
   158|                                                  {"name": "g", "file": "core/b.py"}], "search_terms": [], "lines": []},
   159|         "expansion": {"include_callers": False, "include_callees": True, "include_imports": False,
   160|                       "include_related_tests": False},
   161|         "limits": {"max_estimated_tokens": no_expansion_tokens + 10, "max_files": 12},
   162|     })
   163|     result = _packet(repo, out, req)
   164|     assert result.returncode == 0, result.stderr
   165|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   166|     assert "### Symbol: `f`" in text
   167|     assert "### Symbol: `g`" in text
   168|     assert "return 2" in text  # g's own body, never sacrificed for f's expansion
   169| 
   170| 
   171| def test_search_match_does_not_reserve_focus_file_slot_unless_rendered(repo, out):
   172|     # Regression: note_focus_file(rel) was called before checking whether
   173|     # the match's own rendered line fit the remaining budget, so a match
   174|     # that ultimately never appears in the packet could still consume the
   175|     # sole limits.max_files slot -- and the resolution sidecar would then
   176|     # misleadingly name that file as a "focus file" despite showing zero
   177|     # evidence for it.
   178|     write_files(repo, {"a.py": f"needle = {'x' * 500!r}\n"})
   179|     _scan(repo, out)
   180|     req = _request(out, "req.json", {
   181|         "schema_version": "1.0", "question": "q",
   182|         "selectors": {"files": [], "symbols": [], "search_terms": ["needle"], "lines": []},
   183|         "limits": {"max_estimated_tokens": 10, "max_files": 1},
   184|     })
   185|     result = _packet(repo, out, req)
   186|     assert result.returncode == 0, result.stderr
   187|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   188|     assert sidecar["focus_files"] == []
   189|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   190|     assert "a.py:1" not in text
   191| 
   192| 
   193| def test_duplicate_explicit_selectors_are_evaluated_once_not_per_occurrence(repo, out):
   194|     # Regression: a request naming the same file selector many times over
   195|     # re-attempted rendering (and, if it failed, re-appended an identical
   196|     # conflict message) once per occurrence -- an unbounded-output shape
   197|     # for a request that just repeats one selector, independent of the
   198|     # token-budget accounting fixes for distinct content.
   199|     write_files(repo, {"empty.py": ""})
   200|     _scan(repo, out)
   201|     req = _request(out, "req.json", {
   202|         "schema_version": "1.0", "question": "q",
   203|         "selectors": {"files": ["empty.py"] * 1000, "symbols": [], "search_terms": [], "lines": []},
   204|         "limits": {"max_estimated_tokens": 1, "max_files": 12},
   205|     })
   206|     result = _packet(repo, out, req)
   207|     assert result.returncode == 1
   208|     assert result.stderr.count("empty.py") <= 2  # one conflict line, not one per duplicate
   209| 
   210| 
   211| def test_explicit_selectors_beyond_max_files_is_a_hard_conflict_not_silent_drop(repo, out):
   212|     # Regression: naming more distinct explicit files than limits.max_files
   213|     # allows used to succeed with a partial packet, leaving the resolution
   214|     # report claiming "resolved" for files that were never actually
   215|     # rendered. This must behave like any other explicit-selector conflict:
   216|     # abort, report why, and write no packet at all.
   217|     write_files(repo, {
   218|         "a.py": "def f():\n    return 1\n",
   219|         "b.py": "def g():\n    return 2\n",
   220|     })
   221|     _scan(repo, out)
   222|     req = _request(out, "req.json", {
   223|         "schema_version": "1.0", "question": "q",
   224|         "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
   225|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   226|     })
   227|     result = _packet(repo, out, req)
   228|     assert result.returncode == 1
   229|     assert "max_files" in result.stderr
   230|     assert not (out / "packets" / "packet_req.md").exists()
   231| 
   232| 
   233| def test_strict_mode_catches_unresolved_search_terms(repo, out):
   234|     # Regression: search terms weren't part of all_resolutions at all, so
   235|     # strict mode couldn't abort on a zero-match term or an invalid regex.
   236|     write_files(repo, {"a.py": "def f():\n    return 1\n"})
   237|     _scan(repo, out)
   238|     req = _request(out, "req.json", {
   239|         "schema_version": "1.0", "question": "q", "strict": True,
   240|         "selectors": {"files": [], "symbols": [], "search_terms": ["no_such_term_anywhere"], "lines": []},
   241|     })
   242|     result = _packet(repo, out, req)
   243|     assert result.returncode == 1
   244|     assert "strict mode" in result.stderr
   245|     assert not (out / "packets" / "packet_req.md").exists()
   246| 
   247|     req2 = _request(out, "req2.json", {
   248|         "schema_version": "1.0", "question": "q", "strict": True,
   249|         "selectors": {"files": [], "symbols": [], "search_terms": ["("], "lines": []},
   250|         "expansion": {"search_as_regex": True},
   251|     })
   252|     result2 = _packet(repo, out, req2)
   253|     assert result2.returncode == 1
   254|     assert "strict mode" in result2.stderr
   255| 
   256| 
   257| def test_invalid_schema_version_is_rejected_before_resolution(repo, out):
   258|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   259|     _scan(repo, out)
   260|     req = _request(out, "req.json", {
   261|         "schema_version": "0.1", "question": "q",
   262|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   263|     })
   264|     result = _packet(repo, out, req)
   265|     assert result.returncode == 1
   266|     assert "invalid packet_request.json" in result.stderr
   267|     assert not (out / "packets" / "packet_req.md").exists()
   268| 
   269| 
   270| def test_path_traversal_selector_is_rejected(repo, out):
   271|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   272|     _scan(repo, out)
   273|     req = _request(out, "req.json", {
   274|         "schema_version": "1.0", "question": "q",
   275|         "selectors": {"files": ["../outside.py"], "symbols": [], "search_terms": [], "lines": []},
   276|     })
   277|     result = _packet(repo, out, req)
   278|     assert result.returncode == 1
   279|     assert "invalid packet_request.json" in result.stderr
   280| 
   281| 
   282| def test_search_term_matches_and_related_tests_are_included(repo, out):
   283|     write_files(repo, {
   284|         "core/a.py": "def f():\n    return 'needle_term'\n",
   285|         "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 'needle_term'\n",
   286|     })
   287|     _scan(repo, out)
   288|     req = _request(out, "req.json", {
   289|         "schema_version": "1.0", "question": "q",
   290|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": ["needle_term"], "lines": []},
   291|         "expansion": {"include_related_tests": True},
   292|     })
   293|     result = _packet(repo, out, req)
   294|     assert result.returncode == 0, result.stderr
   295|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   296|     assert "exact_search_match" in text
   297|     assert "tests/test_a.py" in text
   298| 
   299| 
   300| def test_line_selector_resolves_enclosing_symbol(repo, out):
   301|     write_files(repo, {"core/a.py": "def f():\n    x = 1\n    return x\n"})
   302|     _scan(repo, out)
   303|     req = _request(out, "req.json", {
   304|         "schema_version": "1.0", "question": "q",
   305|         "selectors": {"files": [], "symbols": [], "search_terms": [], "lines": [{"file": "core/a.py", "line": 2}]},
   306|     })
   307|     result = _packet(repo, out, req)
   308|     assert result.returncode == 0, result.stderr
   309|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   310|     assert "explicit_line_selector" in text
   311|     assert "def f():" in text
   312| 
   313| 
   314| def test_line_range_extending_past_enclosing_symbol_renders_in_full(repo, out):
   315|     # Regression: the enclosing-symbol lookup only checked that the
   316|     # requested range's *start* line fell inside a symbol, not that the
   317|     # symbol also contained the *end* line. A range starting inside a
   318|     # tiny function that ends immediately (line 2) but requested through
   319|     # line 7 got silently truncated to just that function's own bounds
   320|     # (line 2) -- the resolution report still claimed "resolved" while
   321|     # lines 3-7 were dropped entirely from the packet.
   322|     write_files(repo, {"core/a.py": (
   323|         "x = 0\n"             # 1
   324|         "def tiny(): pass\n"  # 2 (a symbol whose own bounds are just this one line)
   325|         "y = 1\n"             # 3
   326|         "z = 2\n"             # 4
   327|         "w = 3\n"             # 5
   328|         "v = 4\n"             # 6
   329|         "u = 5\n"             # 7
   330|     )})
   331|     _scan(repo, out)
   332|     req = _request(out, "req.json", {
   333|         "schema_version": "1.0", "question": "q",
   334|         "selectors": {"files": [], "symbols": [], "search_terms": [],
   335|                       "lines": [{"file": "core/a.py", "line": 2, "end_line": 7}]},
   336|     })
   337|     result = _packet(repo, out, req)
   338|     assert result.returncode == 0, result.stderr
   339|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   340|     # The full requested range must render -- not truncated to `tiny`'s
   341|     # own 1-line bounds.
   342|     assert "def tiny(): pass" in text
   343|     assert "u = 5" in text
   344|     assert "Enclosing symbol:" not in text  # no symbol contains both endpoints
   345| 
   346| 
   347| def test_enclosing_symbol_note_is_charged_against_budget(repo, out):
   348|     # Regression: the "Enclosing symbol: ..." metadata line for a line
   349|     # selector was appended with no budget.allow()/spend() at all -- a
   350|     # long qualified name could make the actual packet bigger than the
   351|     # sidecar's reported estimated_tokens_used implied.
   352|     write_files(repo, {"core/a.py": "def " + "x" * 80 + "():\n    y = 1\n    return y\n"})
   353|     _scan(repo, out)
   354|     req = _request(out, "req.json", {
   355|         "schema_version": "1.0", "question": "q",
   356|         "selectors": {"files": [], "symbols": [], "search_terms": [], "lines": [{"file": "core/a.py", "line": 2}]},
   357|     })
   358|     result = _packet(repo, out, req)
   359|     assert result.returncode == 0, result.stderr
   360|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   361|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   362|     assert "Enclosing symbol:" in text
   363|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   364| 
   365| 
   366| def test_search_match_collection_is_capped(repo, out):
   367|     # Regression: every matching line was collected into an in-memory
   368|     # list before max_files/the packet budget were ever applied during
   369|     # Tier-2 rendering -- a common term across a large repository could
   370|     # accumulate an unbounded number of tuples. The direct --search
   371|     # packet path already caps collection at max_files * 5; the request
   372|     # path needs the same bound.
   373|     files = {f"core/mod_{i:03d}.py": "needle\n" * 50 for i in range(50)}
   374|     write_files(repo, files)
   375|     _scan(repo, out)
   376|     files_rows = rr._load_csv(out / "file_inventory.csv")
   377|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["needle"], False, files_rows, 3)
   378|     assert len(matches_by_term["needle"]) <= 3 * 5
   379| 
   380| 
   381| def test_redacted_excerpt_is_charged_not_the_raw_source(repo, out):
   382|     # Regression: budget.allow() was checked against the *raw* excerpt
   383|     # text, and redact_secrets() (which can make a secret-shaped value
   384|     # longer via its placeholder) only ran afterward on content already
   385|     # verified to fit -- so the actually-written (redacted) body could
   386|     # end up bigger than what the budget check had approved.
   387|     # The assignment key must be literally "token" (etc.) immediately
   388|     # followed by `=`/`:` for _SECRET_ASSIGNMENT_PATTERN to match at all
   389|     # -- a value just short enough that the fixed-length
   390|     # "[REDACTED-POSSIBLE-SECRET]" placeholder (26 chars) is *longer*
   391|     # than the whole original "token = '...'" span it replaces (~22
   392|     # chars for a 12-char value), so redaction measurably grows the text.
   393|     lines = [f"token = 'abcdefghi{i:03d}'" for i in range(100)]
   394|     write_files(repo, {"core/a.py": "\n".join(lines) + "\n"})
   395|     _scan(repo, out)
   396|     # Measure the real (redacted) cost via a generous run rather than
   397|     # guessing a token limit -- an explicit file selector hard-aborts the
   398|     # whole packet if it doesn't fit (a separate, correct invariant), so
   399|     # the budget must be sized to comfortably fit the redacted excerpt.
   400|     generous_req = _request(out, "generous.json", {
   401|         "schema_version": "1.0", "question": "q",
   402|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   403|         "limits": {"max_estimated_tokens": 200000, "max_files": 12},
   404|     })
   405|     generous_result = _packet(repo, out, generous_req)
   406|     assert generous_result.returncode == 0, generous_result.stderr
   407|     full_tokens = json.loads(
   408|         (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
   409|     )["estimated_tokens_used"]
   410|     req = _request(out, "req.json", {
   411|         "schema_version": "1.0", "question": "q",
   412|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   413|         "limits": {"max_estimated_tokens": full_tokens + 10, "max_files": 12},
   414|     })
   415|     result = _packet(repo, out, req)
   416|     assert result.returncode == 0, result.stderr
   417|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   418|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   419|     # The reported usage must not understate the packet's actual size --
   420|     # this bug's own repro showed ~890 tokens actually used while a 750
   421|     # limit was reported as satisfied.
   422|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   423| 
   424| 
   425| def test_regex_search_rejected_when_bounding_is_unsupported(repo, out, monkeypatch):
   426|     # Regression: on a platform without SIGALRM (Windows), search_as_regex
   427|     # fell back to running the pattern completely unbounded -- exactly
   428|     # the hang this whole mechanism exists to prevent, just gated behind
   429|     # a platform check instead of being fixed. Simulate that platform by
   430|     # removing signal.SIGALRM for the duration of this test.
   431|     import signal
   432|     monkeypatch.delattr(signal, "SIGALRM", raising=False)
   433|     write_files(repo, {"core/a.py": "hello\n"})
   434|     _scan(repo, out)
   435|     files_rows = rr._load_csv(out / "file_inventory.csv")
   436|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["(a+)+$"], True, files_rows, 12)
   437|     assert resolutions[0].status == "invalid"
   438|     assert "SIGALRM" in resolutions[0].detail or "unbounded" in resolutions[0].detail.lower()
   439|     assert matches_by_term["(a+)+$"] == []
   440| 
   441| 
   442| def test_aggregate_search_deadline_applies_to_literal_terms_too(repo, out, monkeypatch):
   443|     # Regression: the aggregate wall-clock deadline only applied when
   444|     # search_as_regex was true. A request with hundreds/thousands of
   445|     # absent *literal* terms re-reads every included text file once per
   446|     # term with no bound at all, since collect_cap only limits how many
   447|     # matches pile up, not how many full scans happen for a term that
   448|     # matches nothing. The deadline must apply regardless of
   449|     # search_as_regex.
   450|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS", 0)
   451|     write_files(repo, {"core/a.py": "needle\n"})
   452|     _scan(repo, out)
   453|     files_rows = rr._load_csv(out / "file_inventory.csv")
   454|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["needle", "other"], False, files_rows, 12)
   455|     assert all(r.status == "invalid" for r in resolutions)
   456|     assert all("aggregate" in r.detail for r in resolutions)
   457| 
   458| 
   459| def test_search_match_redacts_before_truncating(repo, out):
   460|     # Regression: the rendered search-match line truncated to 200 chars
   461|     # *before* calling redact_secrets(). A secret-shaped value whose
   462|     # closing quote fell beyond character 200 had that quote cut off
   463|     # first, breaking _SECRET_ASSIGNMENT_PATTERN's closing-quote
   464|     # backreference -- redact_secrets() then never matched at all, and
   465|     # the (truncated) secret prefix leaked into the packet unredacted.
   466|     secret_value = "x" * 250
   467|     write_files(repo, {"core/a.py": f'token = "{secret_value}"  # NEEDLE_MARKER\n'})
   468|     _scan(repo, out)
   469|     req = _request(out, "req.json", {
   470|         "schema_version": "1.0", "question": "q",
   471|         "selectors": {"files": [], "symbols": [], "search_terms": ["NEEDLE_MARKER"], "lines": []},
   472|     })
   473|     result = _packet(repo, out, req)
   474|     assert result.returncode == 0, result.stderr
   475|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   476|     assert "xxxxxxxxxx" not in text
   477|     assert "REDACTED" in text
   478| 
   479| 
   480| def test_packet_header_and_footer_charged_against_budget(repo, out):
   481|     # Regression: the fixed header (title/root/question/provenance/
   482|     # limits) and footer were written with no budget accounting
   483|     # whatsoever -- an accepted (<=4000-char) question alone could make
   484|     # the real packet many times bigger than limits.max_estimated_tokens
   485|     # while the sidecar still reported a number near zero.
   486|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   487|     _scan(repo, out)
   488|     long_question = "why? " * 700  # comfortably under MAX_QUESTION_LENGTH (4000), still substantial
   489|     # A search-term selector, not an explicit file/symbol/line selector --
   490|     # the latter now correctly hard-aborts the whole packet if it can't
   491|     # fit alongside the (now-charged) header, which is a separate, correct
   492|     # invariant this test isn't about. A search term is soft/omittable, so
   493|     # the packet still succeeds even when the header alone consumes most
   494|     # of an unreasonably tiny budget.
   495|     req = _request(out, "req.json", {
   496|         "schema_version": "1.0", "question": long_question,
   497|         "selectors": {"files": [], "symbols": [], "search_terms": ["nonexistent_term_xyz"], "lines": []},
   498|         "limits": {"max_estimated_tokens": 1, "max_files": 12},
   499|     })
   500|     result = _packet(repo, out, req)
   501|     assert result.returncode == 0, result.stderr
   502|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   503|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   504|     assert sidecar["estimated_tokens_used"] > 100  # the question alone is ~700+ chars
   505|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   506| 
   507| 
   508| def test_stale_source_since_scan_withholds_excerpt(repo, out):
   509|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   510|     _scan(repo, out)
   511|     write_files(repo, {"core/a.py": "def f():\n    return 999\n"})
   512|     req = _request(out, "req.json", {
   513|         "schema_version": "1.0", "question": "q",
   514|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   515|     })
   516|     result = _packet(repo, out, req)
   517|     assert result.returncode == 0, result.stderr
   518|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   519|     assert "withheld" in text
   520|     assert "return 999" not in text
```
