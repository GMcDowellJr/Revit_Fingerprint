# Chunk of dev_tools/repo_context/tests/test_packet_from_request.py

- Source relative path: `dev_tools/repo_context/tests/test_packet_from_request.py`
- Chunk: 2 of 3
- Original line range: 495-994
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_packet_header_and_footer_charged_against_budget, test_stale_source_since_scan_withholds_excerpt, test_resolution_sidecar_json_is_written, test_whole_file_symbol_listing_is_charged_against_budget, test_file_inventories_deferred_until_every_explicit_file_renders, test_explicit_file_excerpt_renders_before_optional_symbol_inventory, test_search_terms_share_a_single_global_max_files_cap, test_invalid_regex_notices_are_charged_against_budget, test_pathological_regex_search_term_times_out_instead_of_hanging, test_aggregate_regex_search_time_is_capped_across_all_terms, test_graphify_peer_listing_respects_max_files, test_graphify_withheld_on_dirty_worktree_even_with_matching_commit, test_graphify_not_withheld_for_dirtiness_confined_to_output_dir, test_callee_expansion_continues_past_a_rejected_file, test_search_term_matches_continue_past_a_rejected_file, test_overlong_question_is_rejected
- Source SHA-256: c374abd0f274680032eecfb5f8298535b3b81413a7f4f6737d033f9738d5a6a9
- Starts inside symbol: no
- Ends inside symbol: no

```
   495| def test_packet_header_and_footer_charged_against_budget(repo, out):
   496|     # Regression: the fixed header (title/root/question/provenance/
   497|     # limits) and footer were written with no budget accounting
   498|     # whatsoever -- an accepted (<=4000-char) question alone could make
   499|     # the real packet many times bigger than limits.max_estimated_tokens
   500|     # while the sidecar still reported a number near zero.
   501|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   502|     _scan(repo, out)
   503|     long_question = "why? " * 700  # comfortably under MAX_QUESTION_LENGTH (4000), still substantial
   504|     # A search-term selector, not an explicit file/symbol/line selector --
   505|     # the latter now correctly hard-aborts the whole packet if it can't
   506|     # fit alongside the (now-charged) header, which is a separate, correct
   507|     # invariant this test isn't about. A generous budget here so the
   508|     # request succeeds; the header (which embeds the question verbatim)
   509|     # is charged/reserved up front regardless (see rc_request.py's
   510|     # "Reserve the fixed framing's budget cost up front").
   511|     req = _request(out, "req.json", {
   512|         "schema_version": "1.0", "question": long_question,
   513|         "selectors": {"files": [], "symbols": [], "search_terms": ["nonexistent_term_xyz"], "lines": []},
   514|         "limits": {"max_estimated_tokens": 2000, "max_files": 12},
   515|     })
   516|     result = _packet(repo, out, req)
   517|     assert result.returncode == 0, result.stderr
   518|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   519|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   520|     assert sidecar["estimated_tokens_used"] > 100  # the question alone is ~700+ chars
   521|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   522| 
   523|     # And a budget too small to fit the header (which embeds the question
   524|     # verbatim) plus the footer must now hard-abort outright, rather than
   525|     # silently "succeed" with a packet whose true size is many times over
   526|     # the requested cap -- exactly the shape of the original bug this
   527|     # regression test exists for.
   528|     tiny_req = _request(out, "tiny.json", {
   529|         "schema_version": "1.0", "question": long_question,
   530|         "selectors": {"files": [], "symbols": [], "search_terms": ["nonexistent_term_xyz"], "lines": []},
   531|         "limits": {"max_estimated_tokens": 1, "max_files": 12},
   532|     })
   533|     tiny_result = _packet(repo, out, tiny_req)
   534|     assert tiny_result.returncode == 1
   535|     assert "too small to fit" in tiny_result.stderr
   536|     assert not (out / "packets" / "packet_tiny.md").exists()
   537| 
   538| 
   539| def test_stale_source_since_scan_withholds_excerpt(repo, out):
   540|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   541|     _scan(repo, out)
   542|     write_files(repo, {"core/a.py": "def f():\n    return 999\n"})
   543|     req = _request(out, "req.json", {
   544|         "schema_version": "1.0", "question": "q",
   545|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   546|     })
   547|     result = _packet(repo, out, req)
   548|     assert result.returncode == 0, result.stderr
   549|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   550|     assert "withheld" in text
   551|     assert "return 999" not in text
   552| 
   553| 
   554| def test_resolution_sidecar_json_is_written(repo, out):
   555|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   556|     _scan(repo, out)
   557|     req = _request(out, "req.json", {
   558|         "schema_version": "1.0", "question": "q",
   559|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   560|     })
   561|     result = _packet(repo, out, req)
   562|     assert result.returncode == 0, result.stderr
   563|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   564|     assert sidecar["schema_version"] == "1.0"
   565|     assert sidecar["question"] == "q"
   566|     assert sidecar["resolution_report"][0]["status"] == "resolved"
   567| 
   568| 
   569| def test_whole_file_symbol_listing_is_charged_against_budget(repo, out):
   570|     # Regression: the "Top-level symbols:" listing for an explicit whole-
   571|     # file selector used to be appended without any budget accounting, so
   572|     # a file with many top-level definitions could blow past
   573|     # limits.max_estimated_tokens while the packet reported far less
   574|     # usage than it actually rendered.
   575|     lines = []
   576|     for i in range(300):
   577|         lines += [f"def func_{i}():", f"    return {i}", ""]
   578|     write_files(repo, {"big.py": "\n".join(lines) + "\n"})
   579|     _scan(repo, out)
   580|     req = _request(out, "req.json", {
   581|         "schema_version": "1.0", "question": "q",
   582|         "selectors": {"files": ["big.py"], "symbols": [], "search_terms": [], "lines": []},
   583|         "limits": {"max_estimated_tokens": 20000, "max_files": 12},
   584|     })
   585|     result = _packet(repo, out, req)
   586|     assert result.returncode == 0, result.stderr
   587|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   588|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   589| 
   590|     # The reported estimated-token usage must not understate the packet's
   591|     # actual rendered size by a wide margin (the symbol-listing bug made
   592|     # this true even though every "func_i" line is metadata, not excerpt).
   593|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   594|     assert text.count("func_") <= 300 * 2  # listing entries + (bounded) excerpt lines only, no runaway duplication
   595|     assert "Omitted" in text or text.count("(function, lines") <= 300
   596| 
   597| 
   598| def test_file_inventories_deferred_until_every_explicit_file_renders(repo, out):
   599|     # Regression: one explicit file selector's "Top-level symbols:"
   600|     # inventory was still rendered before the *next* explicit file
   601|     # selector got its guaranteed shot at the budget. With a 30-function
   602|     # `a.py` selected before a tiny `b.py`, a.py's inventory could consume
   603|     # enough of a tight-but-otherwise-sufficient budget that b.py's own
   604|     # excerpt no longer fit -- reversing the selector order changed the
   605|     # outcome under the same limit, which is exactly the ordering-
   606|     # dependence this fix removes (every file's excerpt must render before
   607|     # *any* file's inventory spends a char).
   608|     n = 60
   609|     lines = [f"def f_{i:03d}(): pass" for i in range(n)]
   610|     write_files(repo, {
   611|         "a.py": "\n".join(lines) + "\n",
   612|         "b.py": "def tiny():\n    return 1\n",
   613|     })
   614|     _scan(repo, out)
   615| 
   616|     no_expansion = {"include_callers": False, "include_callees": False, "include_imports": False,
   617|                     "include_related_tests": False, "include_graphify": False}
   618|     generous_req = _request(out, "generous.json", {
   619|         "schema_version": "1.0", "question": "q",
   620|         "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
   621|         "expansion": no_expansion,
   622|         "limits": {"max_estimated_tokens": 200000, "max_files": 12},
   623|     })
   624|     generous_result = _packet(repo, out, generous_req)
   625|     assert generous_result.returncode == 0, generous_result.stderr
   626|     full_text = (out / "packets" / "packet_generous.md").read_text(encoding="utf-8")
   627|     listing_lines = [l for l in full_text.splitlines() if "(function, lines" in l]
   628|     assert len(listing_lines) == n + 1  # a.py's n functions plus b.py's own single "tiny" entry
   629|     listing_chars = sum(len(l) for l in listing_lines) + len("Top-level symbols:")
   630|     full_tokens = json.loads(
   631|         (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
   632|     )["estimated_tokens_used"]
   633| 
   634|     # Cut roughly half of a.py's inventory worth of room from the fully-
   635|     # fitting total -- still comfortably enough for both files' headers +
   636|     # excerpts (which this bug never touched), but not enough for a.py's
   637|     # full inventory too.
   638|     constrained_tokens = full_tokens - (listing_chars // 2 // 4)
   639| 
   640|     req = _request(out, "req.json", {
   641|         "schema_version": "1.0", "question": "q",
   642|         "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
   643|         "expansion": no_expansion,
   644|         "limits": {"max_estimated_tokens": constrained_tokens, "max_files": 12},
   645|     })
   646|     result = _packet(repo, out, req)
   647|     assert result.returncode == 0, result.stderr
   648|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   649|     # b.py's mandatory excerpt (a *later* explicit selector) must render
   650|     # regardless of a.py's inventory being tight.
   651|     assert "def tiny():" in text
   652|     assert "def f_000(): pass" in text  # a.py's own excerpt still renders in full too
   653|     listing_lines_constrained = [l for l in text.splitlines() if "(function, lines" in l]
   654|     assert len(listing_lines_constrained) < n  # a.py's inventory got truncated instead
   655| 
   656| 
   657| def test_explicit_file_excerpt_renders_before_optional_symbol_inventory(repo, out):
   658|     # Regression: the file explicit-selector loop spent budget on the
   659|     # "Top-level symbols:" inventory listing *before* rendering the file's
   660|     # own mandatory excerpt. A tight-but-sufficient budget (enough for the
   661|     # header + full excerpt, but not also the full inventory) let the
   662|     # optional inventory crowd out the mandatory excerpt, forcing a hard
   663|     # explicit_conflicts abort even though the file's actual requested
   664|     # content would have fit on its own. The excerpt must always render
   665|     # first; only the inventory may be truncated/omitted.
   666|     n = 150
   667|     lines = [f"def f_{i:03d}(): pass" for i in range(n)]
   668|     write_files(repo, {"core/big.py": "\n".join(lines) + "\n"})
   669|     _scan(repo, out)
   670| 
   671|     no_expansion = {"include_callers": False, "include_callees": False, "include_imports": False,
   672|                     "include_related_tests": False, "include_graphify": False}
   673|     generous_req = _request(out, "generous.json", {
   674|         "schema_version": "1.0", "question": "q",
   675|         "selectors": {"files": ["core/big.py"], "symbols": [], "search_terms": [], "lines": []},
   676|         "expansion": no_expansion,
   677|         "limits": {"max_estimated_tokens": 200000, "max_files": 12},
   678|     })
   679|     generous_result = _packet(repo, out, generous_req)
   680|     assert generous_result.returncode == 0, generous_result.stderr
   681|     full_text = (out / "packets" / "packet_generous.md").read_text(encoding="utf-8")
   682|     listing_lines = [l for l in full_text.splitlines() if "(function, lines" in l]
   683|     assert len(listing_lines) == n  # nothing tight yet -- every symbol is listed
   684|     listing_chars = sum(len(l) for l in listing_lines) + len("Top-level symbols:")
   685|     full_tokens = json.loads(
   686|         (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
   687|     )["estimated_tokens_used"]
   688| 
   689|     # Cut roughly half the inventory listing's worth of room from the
   690|     # fully-fitting budget: still comfortably enough for the header + full
   691|     # excerpt (which the listing bug never touched), but not enough for
   692|     # the full inventory listing too.
   693|     constrained_tokens = full_tokens - (listing_chars // 2 // 4)
   694| 
   695|     req = _request(out, "req.json", {
   696|         "schema_version": "1.0", "question": "q",
   697|         "selectors": {"files": ["core/big.py"], "symbols": [], "search_terms": [], "lines": []},
   698|         "expansion": no_expansion,
   699|         "limits": {"max_estimated_tokens": constrained_tokens, "max_files": 12},
   700|     })
   701|     result = _packet(repo, out, req)
   702|     assert result.returncode == 0, result.stderr
   703|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   704|     # Mandatory excerpt: every function's source line, including the very
   705|     # last one, must still be present in full.
   706|     assert "def f_000(): pass" in text
   707|     assert "def f_149(): pass" in text
   708|     # Optional inventory: truncated instead, never the excerpt.
   709|     listing_lines_constrained = [l for l in text.splitlines() if "(function, lines" in l]
   710|     assert len(listing_lines_constrained) < n
   711|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   712|     assert any("top-level symbol" in o.lower() for o in sidecar["omissions"])
   713| 
   714| 
   715| def test_search_terms_share_a_single_global_max_files_cap(repo, out):
   716|     # Regression: max_files was previously enforced per search term (a
   717|     # fresh `shown_files` set each iteration), so two different terms
   718|     # matching two different files could each individually stay "within"
   719|     # limits.max_files while the combined focus-file set exceeded it.
   720|     write_files(repo, {
   721|         "a.py": "def f():\n    return 'alpha_needle'\n",
   722|         "b.py": "def g():\n    return 'beta_needle'\n",
   723|     })
   724|     _scan(repo, out)
   725|     req = _request(out, "req.json", {
   726|         "schema_version": "1.0", "question": "q",
   727|         "selectors": {"files": [], "symbols": [], "search_terms": ["alpha_needle", "beta_needle"], "lines": []},
   728|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   729|     })
   730|     result = _packet(repo, out, req)
   731|     assert result.returncode == 0, result.stderr
   732|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   733|     assert len(sidecar["focus_files"]) <= 1
   734|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   735|     assert "Omitted" in text or "omitted beyond limits.max_files" in text
   736| 
   737| 
   738| def test_invalid_regex_notices_are_charged_against_budget(repo, out):
   739|     # Regression: each invalid-regex search term appended a notice with no
   740|     # budget accounting, so a request with many invalid regex terms could
   741|     # produce a large packet while reporting ~0 estimated tokens used
   742|     # under a tiny limits.max_estimated_tokens. This also exercises a
   743|     # second-order version of the same bug: each skipped notice fell back
   744|     # to an *unbudgeted* budget.omissions entry, and the final "## Omitted
   745|     # / unresolved" section rendered that whole list without any size
   746|     # accounting either -- both layers had to be fixed for this to pass.
   747|     write_files(repo, {"a.py": "def f():\n    return 1\n"})
   748|     _scan(repo, out)
   749|     bad_terms = [f"(unclosed_{i}" for i in range(200)]
   750|     # 300 tokens clears the fixed framing's own floor (header + selector-
   751|     # resolution report + footer, reserved up front -- see rc_request.py's
   752|     # "Reserve the fixed framing's budget cost up front") but is nowhere
   753|     # near enough to fit all 200 invalid-regex notices.
   754|     req = _request(out, "req.json", {
   755|         "schema_version": "1.0", "question": "q",
   756|         "selectors": {"files": [], "symbols": [], "search_terms": bad_terms, "lines": []},
   757|         "expansion": {"search_as_regex": True},
   758|         "limits": {"max_estimated_tokens": 300, "max_files": 500},
   759|     })
   760|     result = _packet(repo, out, req)
   761|     assert result.returncode == 0, result.stderr
   762|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   763|     assert len(text) < 3000
   764|     assert text.count("not a valid regex") < 200
   765| 
   766| 
   767| def test_pathological_regex_search_term_times_out_instead_of_hanging(repo, out, monkeypatch):
   768|     # Regression: search_as_regex ran a caller-supplied pattern through
   769|     # plain re.search with no bound on evaluation time. A syntactically
   770|     # valid but pathological pattern like `(a+)+$` against a long, nearly-
   771|     # matching line triggers catastrophic backtracking -- confirmed to
   772|     # still be running after 20+ seconds for just 35 characters on this
   773|     # engine -- which could hang the CLI indefinitely for an LLM-produced
   774|     # or malicious request. Each term's evaluation must be bounded.
   775|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TIMEOUT_SECONDS", 0.5)
   776|     write_files(repo, {"core/a.py": "x = '" + "a" * 35 + "!'\n"})
   777|     _scan(repo, out)
   778|     files_rows = rr._load_csv(out / "file_inventory.csv")
   779|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["(a+)+$"], True, files_rows, 12)
   780|     assert resolutions[0].status == "invalid"
   781|     assert "exceeded" in resolutions[0].detail
   782|     assert matches_by_term["(a+)+$"] == []
   783| 
   784|     # A normal (non-pathological) regex must still work correctly under
   785|     # the same bounded path -- the timeout mechanism must not break
   786|     # ordinary regex search.
   787|     resolutions2, matches_by_term2, _ = rr.resolve_search_terms(repo, ["a{3}"], True, files_rows, 12)
   788|     assert resolutions2[0].status == "resolved"
   789|     assert matches_by_term2["a{3}"]
   790| 
   791| 
   792| def test_aggregate_regex_search_time_is_capped_across_all_terms(repo, out, monkeypatch):
   793|     # Regression: the per-term SIGALRM bound stops any *one* pathological
   794|     # pattern from hanging forever, but the schema places no cap on how
   795|     # many search_terms a request can carry -- a request with several
   796|     # distinct catastrophic-backtracking patterns could still burn a full
   797|     # per-term allowance for *each one* before packet budgeting even
   798|     # began. Three terms each requesting up to 1.0s, under a 1.5s
   799|     # aggregate cap, must finish in well under 3.0s total, with the terms
   800|     # beyond the aggregate deadline reported as skipped rather than each
   801|     # getting their own fresh timeout.
   802|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TIMEOUT_SECONDS", 1.0)
   803|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS", 1.5)
   804|     write_files(repo, {"core/a.py": "x = '" + "a" * 35 + "!'\n"})
   805|     _scan(repo, out)
   806|     files_rows = rr._load_csv(out / "file_inventory.csv")
   807|     start = time.monotonic()
   808|     # Three distinct-looking patterns, all pathological against the same
   809|     # run of `a` characters (a term repeated verbatim would risk being
   810|     # deduplicated by a caller upstream of this function; these aren't).
   811|     resolutions, matches_by_term, _ = rr.resolve_search_terms(
   812|         repo, ["(a+)+$", "(a+)*$", "(a|aa)+$"], True, files_rows, 12,
   813|     )
   814|     elapsed = time.monotonic() - start
   815|     assert elapsed < 2.5  # comfortably bounded, not the ~3.0s+ three full per-term timeouts would take
   816|     assert all(r.status == "invalid" for r in resolutions)
   817|     # At least the last term must be skipped outright by the aggregate
   818|     # deadline rather than getting its own fresh per-term timeout.
   819|     assert any("aggregate" in r.detail for r in resolutions)
   820| 
   821| 
   822| def test_graphify_peer_listing_respects_max_files(repo, out):
   823|     # Regression: Graphify community-peer paths were emitted without
   824|     # going through note_focus_file, so they could exceed limits.max_files
   825|     # while the resolution sidecar's focus_files list stayed under it.
   826|     write_files(repo, {
   827|         "core/a.py": "def f():\n    return 1\n",
   828|         "core/b.py": "def g():\n    return 2\n",
   829|     })
   830|     commit = _git_init_commit(repo)
   831|     graph = {
   832|         "built_at_commit": commit,
   833|         "nodes": [
   834|             {"source_file": "core/a.py", "community": 5, "community_name": "Widgets"},
   835|             {"source_file": "core/b.py", "community": 5, "community_name": "Widgets"},
   836|         ],
   837|     }
   838|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   839|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   840|     _scan(repo, out)
   841|     req = _request(out, "req.json", {
   842|         "schema_version": "1.0", "question": "q",
   843|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   844|         "expansion": {"include_graphify": True},
   845|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   846|     })
   847|     result = _packet(repo, out, req)
   848|     assert result.returncode == 0, result.stderr
   849|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   850|     assert sidecar["focus_files"] == ["core/a.py"]
   851|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   852|     # core/b.py must never appear as a rendered, [origin:
   853|     # graphify_expansion]-tagged evidence item -- it's beyond
   854|     # limits.max_files, reported only via the batched omission note.
   855|     assert "[origin: graphify_expansion]" not in text
   856|     assert "Graphify community peer(s)" in text
   857|     assert "limits.max_files" in text
   858| 
   859| 
   860| def test_graphify_withheld_on_dirty_worktree_even_with_matching_commit(repo, out):
   861|     # Regression: a matching built_at_commit was accepted even when the
   862|     # scanned worktree had uncommitted changes -- a matching commit hash
   863|     # alone doesn't prove graph.json's communities still describe what's
   864|     # actually on disk if a tracked file was modified since that commit.
   865|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   866|     commit = _git_init_commit(repo)
   867|     graph = {
   868|         "built_at_commit": commit,
   869|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
   870|     }
   871|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   872|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   873|     # Modify a tracked file after the commit, without committing again --
   874|     # this is what makes the worktree dirty even though HEAD still equals
   875|     # the commit graph.json names.
   876|     write_files(repo, {"core/a.py": "def f():\n    return 999\n"})
   877|     _scan(repo, out)
   878|     req = _request(out, "req.json", {
   879|         "schema_version": "1.0", "question": "q",
   880|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   881|         "expansion": {"include_graphify": True},
   882|     })
   883|     result = _packet(repo, out, req)
   884|     assert result.returncode == 0, result.stderr
   885|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   886|     assert "graphify_expansion" not in text
   887|     assert "worktree is dirty" in text
   888| 
   889| 
   890| def test_graphify_not_withheld_for_dirtiness_confined_to_output_dir(repo):
   891|     # Regression: get_git_info(root) reported "dirty" for *any* uncommitted
   892|     # change anywhere in the worktree, including this tool's own freshly
   893|     # written --output directory when it lives inside the scanned repo (as
   894|     # it does for this project's own repo_context/). scan/packet always
   895|     # write fresh output before this check runs, so every single run
   896|     # against such a repo made the worktree look dirty and withheld
   897|     # Graphify evidence for a reason that has nothing to do with the
   898|     # scanned *source* changing.
   899|     output_dir = repo / "repo_context"
   900|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   901|     commit = _git_init_commit(repo)
   902|     graph = {
   903|         "built_at_commit": commit,
   904|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
   905|     }
   906|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   907|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   908|     _scan(repo, output_dir)  # writes brand-new, untracked files *inside* repo -- this alone used to dirty git status
   909|     req = _request(output_dir, "req.json", {
   910|         "schema_version": "1.0", "question": "q",
   911|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   912|         "expansion": {"include_graphify": True},
   913|     })
   914|     result = _packet(repo, output_dir, req)
   915|     assert result.returncode == 0, result.stderr
   916|     text = (output_dir / "packets" / "packet_req.md").read_text(encoding="utf-8")
   917|     assert "worktree is dirty" not in text
   918| 
   919| 
   920| def test_callee_expansion_continues_past_a_rejected_file(repo, out):
   921|     # Regression: the callee-expansion loop `break`-ed the whole listing
   922|     # on the first callee whose file was beyond limits.max_files, even
   923|     # though a *later* callee might be in a file already in focus_files
   924|     # (free -- no new slot needed). `f`'s first callee `g` lives in a
   925|     # different, not-yet-focused file; its second callee `h` lives in the
   926|     # same file as `f` itself (already focused, since that's the selected
   927|     # symbol's own file). With max_files:1, `g` must be skipped but `h`
   928|     # must still render -- not silently dropped along with it.
   929|     write_files(repo, {
   930|         "core/a.py": "from core.other import g\n\n\ndef h():\n    return 1\n\n\ndef f():\n    g()\n    h()\n    return 1\n",
   931|         "core/other.py": "def g():\n    return 2\n",
   932|     })
   933|     _scan(repo, out)
   934|     req = _request(out, "req.json", {
   935|         "schema_version": "1.0", "question": "q",
   936|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   937|         "expansion": {"include_callers": False, "include_callees": True, "include_imports": False,
   938|                       "include_related_tests": False, "include_graphify": False},
   939|         "limits": {"max_files": 1},
   940|     })
   941|     result = _packet(repo, out, req)
   942|     assert result.returncode == 0, result.stderr
   943|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   944|     assert "-> `h`" in text  # the already-focused-file callee must still render
   945|     assert "core/other.py" not in text  # the beyond-max_files callee stays omitted
   946|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   947|     assert sidecar["focus_files"] == ["core/a.py"]
   948| 
   949| 
   950| def test_search_term_matches_continue_past_a_rejected_file(repo, out):
   951|     # Regression: the search-match loop `break`-ed on the first match
   952|     # whose file was beyond limits.max_files, even though a *later* match
   953|     # (for the same term) might be in a file already in focus_files. With
   954|     # `z/main.py` explicitly selected and `needle_term` matching both
   955|     # `a/other.py` (scanned first, alphabetically) and `z/main.py`
   956|     # (already focused), max_files:1 must still render the z/main.py
   957|     # match instead of losing both to the first rejection.
   958|     write_files(repo, {
   959|         "a/other.py": "# needle_term\n",
   960|         "z/main.py": "# needle_term\n",
   961|     })
   962|     _scan(repo, out)
   963|     req = _request(out, "req.json", {
   964|         "schema_version": "1.0", "question": "q",
   965|         "selectors": {"files": ["z/main.py"], "symbols": [], "search_terms": ["needle_term"], "lines": []},
   966|         "limits": {"max_files": 1},
   967|     })
   968|     result = _packet(repo, out, req)
   969|     assert result.returncode == 0, result.stderr
   970|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   971|     assert "z/main.py:1" in text
   972|     assert "a/other.py" not in text
   973|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   974|     assert sidecar["focus_files"] == ["z/main.py"]
   975| 
   976| 
   977| def test_overlong_question_is_rejected(repo, out):
   978|     # Regression: `question` is copied verbatim into every packet's
   979|     # header with no budget accounting and no schema length limit -- an
   980|     # oversized value could make a packet exceed limits.max_estimated_tokens
   981|     # through the header alone.
   982|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   983|     _scan(repo, out)
   984|     req = _request(out, "req.json", {
   985|         "schema_version": "1.0", "question": "x" * 5000,
   986|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   987|         "limits": {"max_estimated_tokens": 1, "max_files": 12},
   988|     })
   989|     result = _packet(repo, out, req)
   990|     assert result.returncode == 1
   991|     assert "too long" in result.stderr
   992|     assert not (out / "packets" / "packet_req.md").exists()
   993| 
   994| 
```
