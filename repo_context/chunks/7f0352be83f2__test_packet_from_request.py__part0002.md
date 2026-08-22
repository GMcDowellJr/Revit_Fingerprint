# Chunk of dev_tools/repo_context/tests/test_packet_from_request.py

- Source relative path: `dev_tools/repo_context/tests/test_packet_from_request.py`
- Chunk: 2 of 3
- Original line range: 493-992
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_packet_header_and_footer_charged_against_budget, test_stale_source_since_scan_withholds_excerpt, test_resolution_sidecar_json_is_written, test_whole_file_symbol_listing_is_charged_against_budget, test_file_inventories_deferred_until_every_explicit_file_renders, test_explicit_file_excerpt_renders_before_optional_symbol_inventory, test_search_terms_share_a_single_global_max_files_cap, test_invalid_regex_notices_are_charged_against_budget, test_pathological_regex_search_term_times_out_instead_of_hanging, test_aggregate_regex_search_time_is_capped_across_all_terms, test_graphify_peer_listing_respects_max_files, test_graphify_withheld_on_dirty_worktree_even_with_matching_commit, test_graphify_not_withheld_for_dirtiness_confined_to_output_dir, test_callee_expansion_continues_past_a_rejected_file, test_search_term_matches_continue_past_a_rejected_file, test_overlong_question_is_rejected
- Source SHA-256: 0cd19f41559a24f6cef43f7a7eb4785e7345e4e86bf582ab66c5d61dfdabca92
- Starts inside symbol: no
- Ends inside symbol: no

```
   493| def test_packet_header_and_footer_charged_against_budget(repo, out):
   494|     # Regression: the fixed header (title/root/question/provenance/
   495|     # limits) and footer were written with no budget accounting
   496|     # whatsoever -- an accepted (<=4000-char) question alone could make
   497|     # the real packet many times bigger than limits.max_estimated_tokens
   498|     # while the sidecar still reported a number near zero.
   499|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   500|     _scan(repo, out)
   501|     long_question = "why? " * 700  # comfortably under MAX_QUESTION_LENGTH (4000), still substantial
   502|     # A search-term selector, not an explicit file/symbol/line selector --
   503|     # the latter now correctly hard-aborts the whole packet if it can't
   504|     # fit alongside the (now-charged) header, which is a separate, correct
   505|     # invariant this test isn't about. A generous budget here so the
   506|     # request succeeds; the header (which embeds the question verbatim)
   507|     # is charged/reserved up front regardless (see rc_request.py's
   508|     # "Reserve the fixed framing's budget cost up front").
   509|     req = _request(out, "req.json", {
   510|         "schema_version": "1.0", "question": long_question,
   511|         "selectors": {"files": [], "symbols": [], "search_terms": ["nonexistent_term_xyz"], "lines": []},
   512|         "limits": {"max_estimated_tokens": 2000, "max_files": 12},
   513|     })
   514|     result = _packet(repo, out, req)
   515|     assert result.returncode == 0, result.stderr
   516|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   517|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   518|     assert sidecar["estimated_tokens_used"] > 100  # the question alone is ~700+ chars
   519|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   520| 
   521|     # And a budget too small to fit the header (which embeds the question
   522|     # verbatim) plus the footer must now hard-abort outright, rather than
   523|     # silently "succeed" with a packet whose true size is many times over
   524|     # the requested cap -- exactly the shape of the original bug this
   525|     # regression test exists for.
   526|     tiny_req = _request(out, "tiny.json", {
   527|         "schema_version": "1.0", "question": long_question,
   528|         "selectors": {"files": [], "symbols": [], "search_terms": ["nonexistent_term_xyz"], "lines": []},
   529|         "limits": {"max_estimated_tokens": 1, "max_files": 12},
   530|     })
   531|     tiny_result = _packet(repo, out, tiny_req)
   532|     assert tiny_result.returncode == 1
   533|     assert "too small to fit" in tiny_result.stderr
   534|     assert not (out / "packets" / "packet_tiny.md").exists()
   535| 
   536| 
   537| def test_stale_source_since_scan_withholds_excerpt(repo, out):
   538|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   539|     _scan(repo, out)
   540|     write_files(repo, {"core/a.py": "def f():\n    return 999\n"})
   541|     req = _request(out, "req.json", {
   542|         "schema_version": "1.0", "question": "q",
   543|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   544|     })
   545|     result = _packet(repo, out, req)
   546|     assert result.returncode == 0, result.stderr
   547|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   548|     assert "withheld" in text
   549|     assert "return 999" not in text
   550| 
   551| 
   552| def test_resolution_sidecar_json_is_written(repo, out):
   553|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   554|     _scan(repo, out)
   555|     req = _request(out, "req.json", {
   556|         "schema_version": "1.0", "question": "q",
   557|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   558|     })
   559|     result = _packet(repo, out, req)
   560|     assert result.returncode == 0, result.stderr
   561|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   562|     assert sidecar["schema_version"] == "1.0"
   563|     assert sidecar["question"] == "q"
   564|     assert sidecar["resolution_report"][0]["status"] == "resolved"
   565| 
   566| 
   567| def test_whole_file_symbol_listing_is_charged_against_budget(repo, out):
   568|     # Regression: the "Top-level symbols:" listing for an explicit whole-
   569|     # file selector used to be appended without any budget accounting, so
   570|     # a file with many top-level definitions could blow past
   571|     # limits.max_estimated_tokens while the packet reported far less
   572|     # usage than it actually rendered.
   573|     lines = []
   574|     for i in range(300):
   575|         lines += [f"def func_{i}():", f"    return {i}", ""]
   576|     write_files(repo, {"big.py": "\n".join(lines) + "\n"})
   577|     _scan(repo, out)
   578|     req = _request(out, "req.json", {
   579|         "schema_version": "1.0", "question": "q",
   580|         "selectors": {"files": ["big.py"], "symbols": [], "search_terms": [], "lines": []},
   581|         "limits": {"max_estimated_tokens": 20000, "max_files": 12},
   582|     })
   583|     result = _packet(repo, out, req)
   584|     assert result.returncode == 0, result.stderr
   585|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   586|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   587| 
   588|     # The reported estimated-token usage must not understate the packet's
   589|     # actual rendered size by a wide margin (the symbol-listing bug made
   590|     # this true even though every "func_i" line is metadata, not excerpt).
   591|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   592|     assert text.count("func_") <= 300 * 2  # listing entries + (bounded) excerpt lines only, no runaway duplication
   593|     assert "Omitted" in text or text.count("(function, lines") <= 300
   594| 
   595| 
   596| def test_file_inventories_deferred_until_every_explicit_file_renders(repo, out):
   597|     # Regression: one explicit file selector's "Top-level symbols:"
   598|     # inventory was still rendered before the *next* explicit file
   599|     # selector got its guaranteed shot at the budget. With a 30-function
   600|     # `a.py` selected before a tiny `b.py`, a.py's inventory could consume
   601|     # enough of a tight-but-otherwise-sufficient budget that b.py's own
   602|     # excerpt no longer fit -- reversing the selector order changed the
   603|     # outcome under the same limit, which is exactly the ordering-
   604|     # dependence this fix removes (every file's excerpt must render before
   605|     # *any* file's inventory spends a char).
   606|     n = 60
   607|     lines = [f"def f_{i:03d}(): pass" for i in range(n)]
   608|     write_files(repo, {
   609|         "a.py": "\n".join(lines) + "\n",
   610|         "b.py": "def tiny():\n    return 1\n",
   611|     })
   612|     _scan(repo, out)
   613| 
   614|     no_expansion = {"include_callers": False, "include_callees": False, "include_imports": False,
   615|                     "include_related_tests": False, "include_graphify": False}
   616|     generous_req = _request(out, "generous.json", {
   617|         "schema_version": "1.0", "question": "q",
   618|         "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
   619|         "expansion": no_expansion,
   620|         "limits": {"max_estimated_tokens": 200000, "max_files": 12},
   621|     })
   622|     generous_result = _packet(repo, out, generous_req)
   623|     assert generous_result.returncode == 0, generous_result.stderr
   624|     full_text = (out / "packets" / "packet_generous.md").read_text(encoding="utf-8")
   625|     listing_lines = [l for l in full_text.splitlines() if "(function, lines" in l]
   626|     assert len(listing_lines) == n + 1  # a.py's n functions plus b.py's own single "tiny" entry
   627|     listing_chars = sum(len(l) for l in listing_lines) + len("Top-level symbols:")
   628|     full_tokens = json.loads(
   629|         (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
   630|     )["estimated_tokens_used"]
   631| 
   632|     # Cut roughly half of a.py's inventory worth of room from the fully-
   633|     # fitting total -- still comfortably enough for both files' headers +
   634|     # excerpts (which this bug never touched), but not enough for a.py's
   635|     # full inventory too.
   636|     constrained_tokens = full_tokens - (listing_chars // 2 // 4)
   637| 
   638|     req = _request(out, "req.json", {
   639|         "schema_version": "1.0", "question": "q",
   640|         "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
   641|         "expansion": no_expansion,
   642|         "limits": {"max_estimated_tokens": constrained_tokens, "max_files": 12},
   643|     })
   644|     result = _packet(repo, out, req)
   645|     assert result.returncode == 0, result.stderr
   646|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   647|     # b.py's mandatory excerpt (a *later* explicit selector) must render
   648|     # regardless of a.py's inventory being tight.
   649|     assert "def tiny():" in text
   650|     assert "def f_000(): pass" in text  # a.py's own excerpt still renders in full too
   651|     listing_lines_constrained = [l for l in text.splitlines() if "(function, lines" in l]
   652|     assert len(listing_lines_constrained) < n  # a.py's inventory got truncated instead
   653| 
   654| 
   655| def test_explicit_file_excerpt_renders_before_optional_symbol_inventory(repo, out):
   656|     # Regression: the file explicit-selector loop spent budget on the
   657|     # "Top-level symbols:" inventory listing *before* rendering the file's
   658|     # own mandatory excerpt. A tight-but-sufficient budget (enough for the
   659|     # header + full excerpt, but not also the full inventory) let the
   660|     # optional inventory crowd out the mandatory excerpt, forcing a hard
   661|     # explicit_conflicts abort even though the file's actual requested
   662|     # content would have fit on its own. The excerpt must always render
   663|     # first; only the inventory may be truncated/omitted.
   664|     n = 150
   665|     lines = [f"def f_{i:03d}(): pass" for i in range(n)]
   666|     write_files(repo, {"core/big.py": "\n".join(lines) + "\n"})
   667|     _scan(repo, out)
   668| 
   669|     no_expansion = {"include_callers": False, "include_callees": False, "include_imports": False,
   670|                     "include_related_tests": False, "include_graphify": False}
   671|     generous_req = _request(out, "generous.json", {
   672|         "schema_version": "1.0", "question": "q",
   673|         "selectors": {"files": ["core/big.py"], "symbols": [], "search_terms": [], "lines": []},
   674|         "expansion": no_expansion,
   675|         "limits": {"max_estimated_tokens": 200000, "max_files": 12},
   676|     })
   677|     generous_result = _packet(repo, out, generous_req)
   678|     assert generous_result.returncode == 0, generous_result.stderr
   679|     full_text = (out / "packets" / "packet_generous.md").read_text(encoding="utf-8")
   680|     listing_lines = [l for l in full_text.splitlines() if "(function, lines" in l]
   681|     assert len(listing_lines) == n  # nothing tight yet -- every symbol is listed
   682|     listing_chars = sum(len(l) for l in listing_lines) + len("Top-level symbols:")
   683|     full_tokens = json.loads(
   684|         (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
   685|     )["estimated_tokens_used"]
   686| 
   687|     # Cut roughly half the inventory listing's worth of room from the
   688|     # fully-fitting budget: still comfortably enough for the header + full
   689|     # excerpt (which the listing bug never touched), but not enough for
   690|     # the full inventory listing too.
   691|     constrained_tokens = full_tokens - (listing_chars // 2 // 4)
   692| 
   693|     req = _request(out, "req.json", {
   694|         "schema_version": "1.0", "question": "q",
   695|         "selectors": {"files": ["core/big.py"], "symbols": [], "search_terms": [], "lines": []},
   696|         "expansion": no_expansion,
   697|         "limits": {"max_estimated_tokens": constrained_tokens, "max_files": 12},
   698|     })
   699|     result = _packet(repo, out, req)
   700|     assert result.returncode == 0, result.stderr
   701|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   702|     # Mandatory excerpt: every function's source line, including the very
   703|     # last one, must still be present in full.
   704|     assert "def f_000(): pass" in text
   705|     assert "def f_149(): pass" in text
   706|     # Optional inventory: truncated instead, never the excerpt.
   707|     listing_lines_constrained = [l for l in text.splitlines() if "(function, lines" in l]
   708|     assert len(listing_lines_constrained) < n
   709|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   710|     assert any("top-level symbol" in o.lower() for o in sidecar["omissions"])
   711| 
   712| 
   713| def test_search_terms_share_a_single_global_max_files_cap(repo, out):
   714|     # Regression: max_files was previously enforced per search term (a
   715|     # fresh `shown_files` set each iteration), so two different terms
   716|     # matching two different files could each individually stay "within"
   717|     # limits.max_files while the combined focus-file set exceeded it.
   718|     write_files(repo, {
   719|         "a.py": "def f():\n    return 'alpha_needle'\n",
   720|         "b.py": "def g():\n    return 'beta_needle'\n",
   721|     })
   722|     _scan(repo, out)
   723|     req = _request(out, "req.json", {
   724|         "schema_version": "1.0", "question": "q",
   725|         "selectors": {"files": [], "symbols": [], "search_terms": ["alpha_needle", "beta_needle"], "lines": []},
   726|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   727|     })
   728|     result = _packet(repo, out, req)
   729|     assert result.returncode == 0, result.stderr
   730|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   731|     assert len(sidecar["focus_files"]) <= 1
   732|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   733|     assert "Omitted" in text or "omitted beyond limits.max_files" in text
   734| 
   735| 
   736| def test_invalid_regex_notices_are_charged_against_budget(repo, out):
   737|     # Regression: each invalid-regex search term appended a notice with no
   738|     # budget accounting, so a request with many invalid regex terms could
   739|     # produce a large packet while reporting ~0 estimated tokens used
   740|     # under a tiny limits.max_estimated_tokens. This also exercises a
   741|     # second-order version of the same bug: each skipped notice fell back
   742|     # to an *unbudgeted* budget.omissions entry, and the final "## Omitted
   743|     # / unresolved" section rendered that whole list without any size
   744|     # accounting either -- both layers had to be fixed for this to pass.
   745|     write_files(repo, {"a.py": "def f():\n    return 1\n"})
   746|     _scan(repo, out)
   747|     bad_terms = [f"(unclosed_{i}" for i in range(200)]
   748|     # 300 tokens clears the fixed framing's own floor (header + selector-
   749|     # resolution report + footer, reserved up front -- see rc_request.py's
   750|     # "Reserve the fixed framing's budget cost up front") but is nowhere
   751|     # near enough to fit all 200 invalid-regex notices.
   752|     req = _request(out, "req.json", {
   753|         "schema_version": "1.0", "question": "q",
   754|         "selectors": {"files": [], "symbols": [], "search_terms": bad_terms, "lines": []},
   755|         "expansion": {"search_as_regex": True},
   756|         "limits": {"max_estimated_tokens": 300, "max_files": 500},
   757|     })
   758|     result = _packet(repo, out, req)
   759|     assert result.returncode == 0, result.stderr
   760|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   761|     assert len(text) < 3000
   762|     assert text.count("not a valid regex") < 200
   763| 
   764| 
   765| def test_pathological_regex_search_term_times_out_instead_of_hanging(repo, out, monkeypatch):
   766|     # Regression: search_as_regex ran a caller-supplied pattern through
   767|     # plain re.search with no bound on evaluation time. A syntactically
   768|     # valid but pathological pattern like `(a+)+$` against a long, nearly-
   769|     # matching line triggers catastrophic backtracking -- confirmed to
   770|     # still be running after 20+ seconds for just 35 characters on this
   771|     # engine -- which could hang the CLI indefinitely for an LLM-produced
   772|     # or malicious request. Each term's evaluation must be bounded.
   773|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TIMEOUT_SECONDS", 0.5)
   774|     write_files(repo, {"core/a.py": "x = '" + "a" * 35 + "!'\n"})
   775|     _scan(repo, out)
   776|     files_rows = rr._load_csv(out / "file_inventory.csv")
   777|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["(a+)+$"], True, files_rows, 12)
   778|     assert resolutions[0].status == "invalid"
   779|     assert "exceeded" in resolutions[0].detail
   780|     assert matches_by_term["(a+)+$"] == []
   781| 
   782|     # A normal (non-pathological) regex must still work correctly under
   783|     # the same bounded path -- the timeout mechanism must not break
   784|     # ordinary regex search.
   785|     resolutions2, matches_by_term2, _ = rr.resolve_search_terms(repo, ["a{3}"], True, files_rows, 12)
   786|     assert resolutions2[0].status == "resolved"
   787|     assert matches_by_term2["a{3}"]
   788| 
   789| 
   790| def test_aggregate_regex_search_time_is_capped_across_all_terms(repo, out, monkeypatch):
   791|     # Regression: the per-term SIGALRM bound stops any *one* pathological
   792|     # pattern from hanging forever, but the schema places no cap on how
   793|     # many search_terms a request can carry -- a request with several
   794|     # distinct catastrophic-backtracking patterns could still burn a full
   795|     # per-term allowance for *each one* before packet budgeting even
   796|     # began. Three terms each requesting up to 1.0s, under a 1.5s
   797|     # aggregate cap, must finish in well under 3.0s total, with the terms
   798|     # beyond the aggregate deadline reported as skipped rather than each
   799|     # getting their own fresh timeout.
   800|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TIMEOUT_SECONDS", 1.0)
   801|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS", 1.5)
   802|     write_files(repo, {"core/a.py": "x = '" + "a" * 35 + "!'\n"})
   803|     _scan(repo, out)
   804|     files_rows = rr._load_csv(out / "file_inventory.csv")
   805|     start = time.monotonic()
   806|     # Three distinct-looking patterns, all pathological against the same
   807|     # run of `a` characters (a term repeated verbatim would risk being
   808|     # deduplicated by a caller upstream of this function; these aren't).
   809|     resolutions, matches_by_term, _ = rr.resolve_search_terms(
   810|         repo, ["(a+)+$", "(a+)*$", "(a|aa)+$"], True, files_rows, 12,
   811|     )
   812|     elapsed = time.monotonic() - start
   813|     assert elapsed < 2.5  # comfortably bounded, not the ~3.0s+ three full per-term timeouts would take
   814|     assert all(r.status == "invalid" for r in resolutions)
   815|     # At least the last term must be skipped outright by the aggregate
   816|     # deadline rather than getting its own fresh per-term timeout.
   817|     assert any("aggregate" in r.detail for r in resolutions)
   818| 
   819| 
   820| def test_graphify_peer_listing_respects_max_files(repo, out):
   821|     # Regression: Graphify community-peer paths were emitted without
   822|     # going through note_focus_file, so they could exceed limits.max_files
   823|     # while the resolution sidecar's focus_files list stayed under it.
   824|     write_files(repo, {
   825|         "core/a.py": "def f():\n    return 1\n",
   826|         "core/b.py": "def g():\n    return 2\n",
   827|     })
   828|     commit = _git_init_commit(repo)
   829|     graph = {
   830|         "built_at_commit": commit,
   831|         "nodes": [
   832|             {"source_file": "core/a.py", "community": 5, "community_name": "Widgets"},
   833|             {"source_file": "core/b.py", "community": 5, "community_name": "Widgets"},
   834|         ],
   835|     }
   836|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   837|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   838|     _scan(repo, out)
   839|     req = _request(out, "req.json", {
   840|         "schema_version": "1.0", "question": "q",
   841|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   842|         "expansion": {"include_graphify": True},
   843|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   844|     })
   845|     result = _packet(repo, out, req)
   846|     assert result.returncode == 0, result.stderr
   847|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   848|     assert sidecar["focus_files"] == ["core/a.py"]
   849|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   850|     # core/b.py must never appear as a rendered, [origin:
   851|     # graphify_expansion]-tagged evidence item -- it's beyond
   852|     # limits.max_files, reported only via the batched omission note.
   853|     assert "[origin: graphify_expansion]" not in text
   854|     assert "Graphify community peer(s)" in text
   855|     assert "limits.max_files" in text
   856| 
   857| 
   858| def test_graphify_withheld_on_dirty_worktree_even_with_matching_commit(repo, out):
   859|     # Regression: a matching built_at_commit was accepted even when the
   860|     # scanned worktree had uncommitted changes -- a matching commit hash
   861|     # alone doesn't prove graph.json's communities still describe what's
   862|     # actually on disk if a tracked file was modified since that commit.
   863|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   864|     commit = _git_init_commit(repo)
   865|     graph = {
   866|         "built_at_commit": commit,
   867|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
   868|     }
   869|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   870|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   871|     # Modify a tracked file after the commit, without committing again --
   872|     # this is what makes the worktree dirty even though HEAD still equals
   873|     # the commit graph.json names.
   874|     write_files(repo, {"core/a.py": "def f():\n    return 999\n"})
   875|     _scan(repo, out)
   876|     req = _request(out, "req.json", {
   877|         "schema_version": "1.0", "question": "q",
   878|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   879|         "expansion": {"include_graphify": True},
   880|     })
   881|     result = _packet(repo, out, req)
   882|     assert result.returncode == 0, result.stderr
   883|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   884|     assert "graphify_expansion" not in text
   885|     assert "worktree is dirty" in text
   886| 
   887| 
   888| def test_graphify_not_withheld_for_dirtiness_confined_to_output_dir(repo):
   889|     # Regression: get_git_info(root) reported "dirty" for *any* uncommitted
   890|     # change anywhere in the worktree, including this tool's own freshly
   891|     # written --output directory when it lives inside the scanned repo (as
   892|     # it does for this project's own repo_context/). scan/packet always
   893|     # write fresh output before this check runs, so every single run
   894|     # against such a repo made the worktree look dirty and withheld
   895|     # Graphify evidence for a reason that has nothing to do with the
   896|     # scanned *source* changing.
   897|     output_dir = repo / "repo_context"
   898|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   899|     commit = _git_init_commit(repo)
   900|     graph = {
   901|         "built_at_commit": commit,
   902|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
   903|     }
   904|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   905|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   906|     _scan(repo, output_dir)  # writes brand-new, untracked files *inside* repo -- this alone used to dirty git status
   907|     req = _request(output_dir, "req.json", {
   908|         "schema_version": "1.0", "question": "q",
   909|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   910|         "expansion": {"include_graphify": True},
   911|     })
   912|     result = _packet(repo, output_dir, req)
   913|     assert result.returncode == 0, result.stderr
   914|     text = (output_dir / "packets" / "packet_req.md").read_text(encoding="utf-8")
   915|     assert "worktree is dirty" not in text
   916| 
   917| 
   918| def test_callee_expansion_continues_past_a_rejected_file(repo, out):
   919|     # Regression: the callee-expansion loop `break`-ed the whole listing
   920|     # on the first callee whose file was beyond limits.max_files, even
   921|     # though a *later* callee might be in a file already in focus_files
   922|     # (free -- no new slot needed). `f`'s first callee `g` lives in a
   923|     # different, not-yet-focused file; its second callee `h` lives in the
   924|     # same file as `f` itself (already focused, since that's the selected
   925|     # symbol's own file). With max_files:1, `g` must be skipped but `h`
   926|     # must still render -- not silently dropped along with it.
   927|     write_files(repo, {
   928|         "core/a.py": "from core.other import g\n\n\ndef h():\n    return 1\n\n\ndef f():\n    g()\n    h()\n    return 1\n",
   929|         "core/other.py": "def g():\n    return 2\n",
   930|     })
   931|     _scan(repo, out)
   932|     req = _request(out, "req.json", {
   933|         "schema_version": "1.0", "question": "q",
   934|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   935|         "expansion": {"include_callers": False, "include_callees": True, "include_imports": False,
   936|                       "include_related_tests": False, "include_graphify": False},
   937|         "limits": {"max_files": 1},
   938|     })
   939|     result = _packet(repo, out, req)
   940|     assert result.returncode == 0, result.stderr
   941|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   942|     assert "-> `h`" in text  # the already-focused-file callee must still render
   943|     assert "core/other.py" not in text  # the beyond-max_files callee stays omitted
   944|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   945|     assert sidecar["focus_files"] == ["core/a.py"]
   946| 
   947| 
   948| def test_search_term_matches_continue_past_a_rejected_file(repo, out):
   949|     # Regression: the search-match loop `break`-ed on the first match
   950|     # whose file was beyond limits.max_files, even though a *later* match
   951|     # (for the same term) might be in a file already in focus_files. With
   952|     # `z/main.py` explicitly selected and `needle_term` matching both
   953|     # `a/other.py` (scanned first, alphabetically) and `z/main.py`
   954|     # (already focused), max_files:1 must still render the z/main.py
   955|     # match instead of losing both to the first rejection.
   956|     write_files(repo, {
   957|         "a/other.py": "# needle_term\n",
   958|         "z/main.py": "# needle_term\n",
   959|     })
   960|     _scan(repo, out)
   961|     req = _request(out, "req.json", {
   962|         "schema_version": "1.0", "question": "q",
   963|         "selectors": {"files": ["z/main.py"], "symbols": [], "search_terms": ["needle_term"], "lines": []},
   964|         "limits": {"max_files": 1},
   965|     })
   966|     result = _packet(repo, out, req)
   967|     assert result.returncode == 0, result.stderr
   968|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   969|     assert "z/main.py:1" in text
   970|     assert "a/other.py" not in text
   971|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   972|     assert sidecar["focus_files"] == ["z/main.py"]
   973| 
   974| 
   975| def test_overlong_question_is_rejected(repo, out):
   976|     # Regression: `question` is copied verbatim into every packet's
   977|     # header with no budget accounting and no schema length limit -- an
   978|     # oversized value could make a packet exceed limits.max_estimated_tokens
   979|     # through the header alone.
   980|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   981|     _scan(repo, out)
   982|     req = _request(out, "req.json", {
   983|         "schema_version": "1.0", "question": "x" * 5000,
   984|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   985|         "limits": {"max_estimated_tokens": 1, "max_files": 12},
   986|     })
   987|     result = _packet(repo, out, req)
   988|     assert result.returncode == 1
   989|     assert "too long" in result.stderr
   990|     assert not (out / "packets" / "packet_req.md").exists()
   991| 
   992| 
```
