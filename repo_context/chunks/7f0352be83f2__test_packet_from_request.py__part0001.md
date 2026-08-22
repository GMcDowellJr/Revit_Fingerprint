# Chunk of dev_tools/repo_context/tests/test_packet_from_request.py

- Source relative path: `dev_tools/repo_context/tests/test_packet_from_request.py`
- Chunk: 1 of 3
- Original line range: 1-492
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _scan, _request, _packet, test_valid_request_resolves_file_and_symbol_selectors, test_ambiguous_symbol_is_reported_not_silently_resolved, test_qualified_symbol_via_file_field_resolves_unambiguously, test_missing_selector_is_reported_but_other_selectors_still_processed, test_strict_mode_aborts_on_any_unresolved_selector, test_hard_budget_conflict_on_explicit_selector_aborts_without_partial_packet, test_expansion_never_preempts_a_later_explicit_selector, test_search_match_does_not_reserve_focus_file_slot_unless_rendered, test_duplicate_explicit_selectors_are_evaluated_once_not_per_occurrence, test_explicit_selectors_beyond_max_files_is_a_hard_conflict_not_silent_drop, test_strict_mode_catches_unresolved_search_terms, test_invalid_schema_version_is_rejected_before_resolution, test_path_traversal_selector_is_rejected, test_search_term_matches_and_related_tests_are_included, test_line_selector_resolves_enclosing_symbol, test_line_range_extending_past_enclosing_symbol_renders_in_full, test_enclosing_symbol_note_is_charged_against_budget, test_search_match_collection_is_capped, test_redacted_excerpt_is_charged_not_the_raw_source, test_regex_search_rejected_when_bounding_is_unsupported, test_aggregate_search_deadline_applies_to_literal_terms_too, test_search_match_redacts_before_truncating
- Source SHA-256: 0cd19f41559a24f6cef43f7a7eb4785e7345e4e86bf582ab66c5d61dfdabca92
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
   114|     # 200 tokens comfortably clears the fixed framing's own floor (see
   115|     # rc_request.py's "Reserve the fixed framing's budget cost up front")
   116|     # but nowhere near big.py's ~900 lines -- this exercises the explicit
   117|     # *selector* conflict, not the separate framing-alone conflict.
   118|     req = _request(out, "req.json", {
   119|         "schema_version": "1.0", "question": "q",
   120|         "selectors": {"files": ["big.py"], "symbols": [], "search_terms": [], "lines": []},
   121|         "limits": {"max_estimated_tokens": 200, "max_files": 12},
   122|     })
   123|     result = _packet(repo, out, req)
   124|     assert result.returncode == 1
   125|     assert "do not fit" in result.stderr
   126|     assert not (out / "packets" / "packet_req.md").exists()
   127| 
   128| 
   129| def test_expansion_never_preempts_a_later_explicit_selector(repo, out):
   130|     # Regression: expansions (callers/callees/imports/tests) for one
   131|     # explicit symbol were rendered immediately after it and before the
   132|     # *next* explicit selector got its turn, so a budget that easily fits
   133|     # every explicit selector's own content could still fail if an early
   134|     # selector's expansion ate the remaining room first. Explicit content
   135|     # must all be attempted before any expansion spends a single char.
   136|     write_files(repo, {
   137|         "core/a.py": "def h():\n    return 42\n\n\ndef f():\n    return h()\n",
   138|         "core/b.py": "def g():\n    return 2\n",
   139|     })
   140|     _scan(repo, out)
   141| 
   142|     # Establish the true no-expansion cost for both explicit symbols. Use a
   143|     # generous budget for this measurement run -- the fixed framing
   144|     # (header/resolution-report/footer) is now reserved against the budget
   145|     # up front (see rc_request.py's "Reserve the fixed framing's budget
   146|     # cost up front"), so a too-tight value here would fail on framing
   147|     # alone rather than actually measuring the two symbols' cost.
   148|     baseline_req = _request(out, "baseline.json", {
   149|         "schema_version": "1.0", "question": "q",
   150|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"},
   151|                                                  {"name": "g", "file": "core/b.py"}], "search_terms": [], "lines": []},
   152|         "expansion": {"include_callers": False, "include_callees": False, "include_imports": False,
   153|                       "include_related_tests": False},
   154|         "limits": {"max_estimated_tokens": 1000, "max_files": 12},
   155|     })
   156|     baseline_result = _packet(repo, out, baseline_req)
   157|     assert baseline_result.returncode == 0, baseline_result.stderr
   158|     baseline_sidecar = json.loads((out / "packets" / "packet_baseline.resolution.json").read_text(encoding="utf-8"))
   159|     no_expansion_tokens = baseline_sidecar["estimated_tokens_used"]
   160| 
   161|     # A budget comfortably above the no-expansion cost, but too small to
   162|     # also fit f's callee-expansion listing -- both explicit symbols must
   163|     # still render in full; only the expansion may be omitted.
   164|     req = _request(out, "req.json", {
   165|         "schema_version": "1.0", "question": "q",
   166|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"},
   167|                                                  {"name": "g", "file": "core/b.py"}], "search_terms": [], "lines": []},
   168|         "expansion": {"include_callers": False, "include_callees": True, "include_imports": False,
   169|                       "include_related_tests": False},
   170|         "limits": {"max_estimated_tokens": no_expansion_tokens + 10, "max_files": 12},
   171|     })
   172|     result = _packet(repo, out, req)
   173|     assert result.returncode == 0, result.stderr
   174|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   175|     assert "### Symbol: `f`" in text
   176|     assert "### Symbol: `g`" in text
   177|     assert "return 2" in text  # g's own body, never sacrificed for f's expansion
   178| 
   179| 
   180| def test_search_match_does_not_reserve_focus_file_slot_unless_rendered(repo, out):
   181|     # Regression: note_focus_file(rel) was called before checking whether
   182|     # the match's own rendered line fit the remaining budget, so a match
   183|     # that ultimately never appears in the packet could still consume the
   184|     # sole limits.max_files slot -- and the resolution sidecar would then
   185|     # misleadingly name that file as a "focus file" despite showing zero
   186|     # evidence for it.
   187|     write_files(repo, {"a.py": f"needle = {'x' * 500!r}\n"})
   188|     _scan(repo, out)
   189|     # 130 tokens is just above the fixed framing's own floor (header +
   190|     # resolution report + footer must be reserved first -- see
   191|     # rc_request.py's "Reserve the fixed framing's budget cost up front")
   192|     # but well short of also fitting the ~500-char match line.
   193|     req = _request(out, "req.json", {
   194|         "schema_version": "1.0", "question": "q",
   195|         "selectors": {"files": [], "symbols": [], "search_terms": ["needle"], "lines": []},
   196|         "limits": {"max_estimated_tokens": 130, "max_files": 1},
   197|     })
   198|     result = _packet(repo, out, req)
   199|     assert result.returncode == 0, result.stderr
   200|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   201|     assert sidecar["focus_files"] == []
   202|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   203|     assert "a.py:1" not in text
   204| 
   205| 
   206| def test_duplicate_explicit_selectors_are_evaluated_once_not_per_occurrence(repo, out):
   207|     # Regression: a request naming the same file selector many times over
   208|     # re-attempted rendering (and, if it failed, re-appended an identical
   209|     # conflict message) once per occurrence -- an unbounded-output shape
   210|     # for a request that just repeats one selector, independent of the
   211|     # token-budget accounting fixes for distinct content.
   212|     write_files(repo, {"empty.py": ""})
   213|     _scan(repo, out)
   214|     req = _request(out, "req.json", {
   215|         "schema_version": "1.0", "question": "q",
   216|         "selectors": {"files": ["empty.py"] * 1000, "symbols": [], "search_terms": [], "lines": []},
   217|         "limits": {"max_estimated_tokens": 1, "max_files": 12},
   218|     })
   219|     result = _packet(repo, out, req)
   220|     assert result.returncode == 1
   221|     assert result.stderr.count("empty.py") <= 2  # one conflict line, not one per duplicate
   222| 
   223| 
   224| def test_explicit_selectors_beyond_max_files_is_a_hard_conflict_not_silent_drop(repo, out):
   225|     # Regression: naming more distinct explicit files than limits.max_files
   226|     # allows used to succeed with a partial packet, leaving the resolution
   227|     # report claiming "resolved" for files that were never actually
   228|     # rendered. This must behave like any other explicit-selector conflict:
   229|     # abort, report why, and write no packet at all.
   230|     write_files(repo, {
   231|         "a.py": "def f():\n    return 1\n",
   232|         "b.py": "def g():\n    return 2\n",
   233|     })
   234|     _scan(repo, out)
   235|     req = _request(out, "req.json", {
   236|         "schema_version": "1.0", "question": "q",
   237|         "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
   238|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   239|     })
   240|     result = _packet(repo, out, req)
   241|     assert result.returncode == 1
   242|     assert "max_files" in result.stderr
   243|     assert not (out / "packets" / "packet_req.md").exists()
   244| 
   245| 
   246| def test_strict_mode_catches_unresolved_search_terms(repo, out):
   247|     # Regression: search terms weren't part of all_resolutions at all, so
   248|     # strict mode couldn't abort on a zero-match term or an invalid regex.
   249|     write_files(repo, {"a.py": "def f():\n    return 1\n"})
   250|     _scan(repo, out)
   251|     req = _request(out, "req.json", {
   252|         "schema_version": "1.0", "question": "q", "strict": True,
   253|         "selectors": {"files": [], "symbols": [], "search_terms": ["no_such_term_anywhere"], "lines": []},
   254|     })
   255|     result = _packet(repo, out, req)
   256|     assert result.returncode == 1
   257|     assert "strict mode" in result.stderr
   258|     assert not (out / "packets" / "packet_req.md").exists()
   259| 
   260|     req2 = _request(out, "req2.json", {
   261|         "schema_version": "1.0", "question": "q", "strict": True,
   262|         "selectors": {"files": [], "symbols": [], "search_terms": ["("], "lines": []},
   263|         "expansion": {"search_as_regex": True},
   264|     })
   265|     result2 = _packet(repo, out, req2)
   266|     assert result2.returncode == 1
   267|     assert "strict mode" in result2.stderr
   268| 
   269| 
   270| def test_invalid_schema_version_is_rejected_before_resolution(repo, out):
   271|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   272|     _scan(repo, out)
   273|     req = _request(out, "req.json", {
   274|         "schema_version": "0.1", "question": "q",
   275|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   276|     })
   277|     result = _packet(repo, out, req)
   278|     assert result.returncode == 1
   279|     assert "invalid packet_request.json" in result.stderr
   280|     assert not (out / "packets" / "packet_req.md").exists()
   281| 
   282| 
   283| def test_path_traversal_selector_is_rejected(repo, out):
   284|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   285|     _scan(repo, out)
   286|     req = _request(out, "req.json", {
   287|         "schema_version": "1.0", "question": "q",
   288|         "selectors": {"files": ["../outside.py"], "symbols": [], "search_terms": [], "lines": []},
   289|     })
   290|     result = _packet(repo, out, req)
   291|     assert result.returncode == 1
   292|     assert "invalid packet_request.json" in result.stderr
   293| 
   294| 
   295| def test_search_term_matches_and_related_tests_are_included(repo, out):
   296|     write_files(repo, {
   297|         "core/a.py": "def f():\n    return 'needle_term'\n",
   298|         "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 'needle_term'\n",
   299|     })
   300|     _scan(repo, out)
   301|     req = _request(out, "req.json", {
   302|         "schema_version": "1.0", "question": "q",
   303|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": ["needle_term"], "lines": []},
   304|         "expansion": {"include_related_tests": True},
   305|     })
   306|     result = _packet(repo, out, req)
   307|     assert result.returncode == 0, result.stderr
   308|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   309|     assert "exact_search_match" in text
   310|     assert "tests/test_a.py" in text
   311| 
   312| 
   313| def test_line_selector_resolves_enclosing_symbol(repo, out):
   314|     write_files(repo, {"core/a.py": "def f():\n    x = 1\n    return x\n"})
   315|     _scan(repo, out)
   316|     req = _request(out, "req.json", {
   317|         "schema_version": "1.0", "question": "q",
   318|         "selectors": {"files": [], "symbols": [], "search_terms": [], "lines": [{"file": "core/a.py", "line": 2}]},
   319|     })
   320|     result = _packet(repo, out, req)
   321|     assert result.returncode == 0, result.stderr
   322|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   323|     assert "explicit_line_selector" in text
   324|     assert "def f():" in text
   325| 
   326| 
   327| def test_line_range_extending_past_enclosing_symbol_renders_in_full(repo, out):
   328|     # Regression: the enclosing-symbol lookup only checked that the
   329|     # requested range's *start* line fell inside a symbol, not that the
   330|     # symbol also contained the *end* line. A range starting inside a
   331|     # tiny function that ends immediately (line 2) but requested through
   332|     # line 7 got silently truncated to just that function's own bounds
   333|     # (line 2) -- the resolution report still claimed "resolved" while
   334|     # lines 3-7 were dropped entirely from the packet.
   335|     write_files(repo, {"core/a.py": (
   336|         "x = 0\n"             # 1
   337|         "def tiny(): pass\n"  # 2 (a symbol whose own bounds are just this one line)
   338|         "y = 1\n"             # 3
   339|         "z = 2\n"             # 4
   340|         "w = 3\n"             # 5
   341|         "v = 4\n"             # 6
   342|         "u = 5\n"             # 7
   343|     )})
   344|     _scan(repo, out)
   345|     req = _request(out, "req.json", {
   346|         "schema_version": "1.0", "question": "q",
   347|         "selectors": {"files": [], "symbols": [], "search_terms": [],
   348|                       "lines": [{"file": "core/a.py", "line": 2, "end_line": 7}]},
   349|     })
   350|     result = _packet(repo, out, req)
   351|     assert result.returncode == 0, result.stderr
   352|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   353|     # The full requested range must render -- not truncated to `tiny`'s
   354|     # own 1-line bounds.
   355|     assert "def tiny(): pass" in text
   356|     assert "u = 5" in text
   357|     assert "Enclosing symbol:" not in text  # no symbol contains both endpoints
   358| 
   359| 
   360| def test_enclosing_symbol_note_is_charged_against_budget(repo, out):
   361|     # Regression: the "Enclosing symbol: ..." metadata line for a line
   362|     # selector was appended with no budget.allow()/spend() at all -- a
   363|     # long qualified name could make the actual packet bigger than the
   364|     # sidecar's reported estimated_tokens_used implied.
   365|     write_files(repo, {"core/a.py": "def " + "x" * 80 + "():\n    y = 1\n    return y\n"})
   366|     _scan(repo, out)
   367|     req = _request(out, "req.json", {
   368|         "schema_version": "1.0", "question": "q",
   369|         "selectors": {"files": [], "symbols": [], "search_terms": [], "lines": [{"file": "core/a.py", "line": 2}]},
   370|     })
   371|     result = _packet(repo, out, req)
   372|     assert result.returncode == 0, result.stderr
   373|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   374|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   375|     assert "Enclosing symbol:" in text
   376|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   377| 
   378| 
   379| def test_search_match_collection_is_capped(repo, out):
   380|     # Regression: every matching line was collected into an in-memory
   381|     # list before max_files/the packet budget were ever applied during
   382|     # Tier-2 rendering -- a common term across a large repository could
   383|     # accumulate an unbounded number of tuples. The direct --search
   384|     # packet path already caps collection at max_files * 5; the request
   385|     # path needs the same bound.
   386|     files = {f"core/mod_{i:03d}.py": "needle\n" * 50 for i in range(50)}
   387|     write_files(repo, files)
   388|     _scan(repo, out)
   389|     files_rows = rr._load_csv(out / "file_inventory.csv")
   390|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["needle"], False, files_rows, 3)
   391|     assert len(matches_by_term["needle"]) <= 3 * 5
   392| 
   393| 
   394| def test_redacted_excerpt_is_charged_not_the_raw_source(repo, out):
   395|     # Regression: budget.allow() was checked against the *raw* excerpt
   396|     # text, and redact_secrets() (which can make a secret-shaped value
   397|     # longer via its placeholder) only ran afterward on content already
   398|     # verified to fit -- so the actually-written (redacted) body could
   399|     # end up bigger than what the budget check had approved.
   400|     # The assignment key must be literally "token" (etc.) immediately
   401|     # followed by `=`/`:` for _SECRET_ASSIGNMENT_PATTERN to match at all
   402|     # -- a value just short enough that the fixed-length
   403|     # "[REDACTED-POSSIBLE-SECRET]" placeholder (26 chars) is *longer*
   404|     # than the whole original "token = '...'" span it replaces (~22
   405|     # chars for a 12-char value), so redaction measurably grows the text.
   406|     lines = [f"token = 'abcdefghi{i:03d}'" for i in range(100)]
   407|     write_files(repo, {"core/a.py": "\n".join(lines) + "\n"})
   408|     _scan(repo, out)
   409|     # Measure the real (redacted) cost via a generous run rather than
   410|     # guessing a token limit -- an explicit file selector hard-aborts the
   411|     # whole packet if it doesn't fit (a separate, correct invariant), so
   412|     # the budget must be sized to comfortably fit the redacted excerpt.
   413|     generous_req = _request(out, "generous.json", {
   414|         "schema_version": "1.0", "question": "q",
   415|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   416|         "limits": {"max_estimated_tokens": 200000, "max_files": 12},
   417|     })
   418|     generous_result = _packet(repo, out, generous_req)
   419|     assert generous_result.returncode == 0, generous_result.stderr
   420|     full_tokens = json.loads(
   421|         (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
   422|     )["estimated_tokens_used"]
   423|     req = _request(out, "req.json", {
   424|         "schema_version": "1.0", "question": "q",
   425|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   426|         "limits": {"max_estimated_tokens": full_tokens + 10, "max_files": 12},
   427|     })
   428|     result = _packet(repo, out, req)
   429|     assert result.returncode == 0, result.stderr
   430|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   431|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   432|     # The reported usage must not understate the packet's actual size --
   433|     # this bug's own repro showed ~890 tokens actually used while a 750
   434|     # limit was reported as satisfied.
   435|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   436| 
   437| 
   438| def test_regex_search_rejected_when_bounding_is_unsupported(repo, out, monkeypatch):
   439|     # Regression: on a platform without SIGALRM (Windows), search_as_regex
   440|     # fell back to running the pattern completely unbounded -- exactly
   441|     # the hang this whole mechanism exists to prevent, just gated behind
   442|     # a platform check instead of being fixed. Simulate that platform by
   443|     # removing signal.SIGALRM for the duration of this test.
   444|     import signal
   445|     monkeypatch.delattr(signal, "SIGALRM", raising=False)
   446|     write_files(repo, {"core/a.py": "hello\n"})
   447|     _scan(repo, out)
   448|     files_rows = rr._load_csv(out / "file_inventory.csv")
   449|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["(a+)+$"], True, files_rows, 12)
   450|     assert resolutions[0].status == "invalid"
   451|     assert "SIGALRM" in resolutions[0].detail or "unbounded" in resolutions[0].detail.lower()
   452|     assert matches_by_term["(a+)+$"] == []
   453| 
   454| 
   455| def test_aggregate_search_deadline_applies_to_literal_terms_too(repo, out, monkeypatch):
   456|     # Regression: the aggregate wall-clock deadline only applied when
   457|     # search_as_regex was true. A request with hundreds/thousands of
   458|     # absent *literal* terms re-reads every included text file once per
   459|     # term with no bound at all, since collect_cap only limits how many
   460|     # matches pile up, not how many full scans happen for a term that
   461|     # matches nothing. The deadline must apply regardless of
   462|     # search_as_regex.
   463|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS", 0)
   464|     write_files(repo, {"core/a.py": "needle\n"})
   465|     _scan(repo, out)
   466|     files_rows = rr._load_csv(out / "file_inventory.csv")
   467|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["needle", "other"], False, files_rows, 12)
   468|     assert all(r.status == "invalid" for r in resolutions)
   469|     assert all("aggregate" in r.detail for r in resolutions)
   470| 
   471| 
   472| def test_search_match_redacts_before_truncating(repo, out):
   473|     # Regression: the rendered search-match line truncated to 200 chars
   474|     # *before* calling redact_secrets(). A secret-shaped value whose
   475|     # closing quote fell beyond character 200 had that quote cut off
   476|     # first, breaking _SECRET_ASSIGNMENT_PATTERN's closing-quote
   477|     # backreference -- redact_secrets() then never matched at all, and
   478|     # the (truncated) secret prefix leaked into the packet unredacted.
   479|     secret_value = "x" * 250
   480|     write_files(repo, {"core/a.py": f'token = "{secret_value}"  # NEEDLE_MARKER\n'})
   481|     _scan(repo, out)
   482|     req = _request(out, "req.json", {
   483|         "schema_version": "1.0", "question": "q",
   484|         "selectors": {"files": [], "symbols": [], "search_terms": ["NEEDLE_MARKER"], "lines": []},
   485|     })
   486|     result = _packet(repo, out, req)
   487|     assert result.returncode == 0, result.stderr
   488|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   489|     assert "xxxxxxxxxx" not in text
   490|     assert "REDACTED" in text
   491| 
   492| 
```
