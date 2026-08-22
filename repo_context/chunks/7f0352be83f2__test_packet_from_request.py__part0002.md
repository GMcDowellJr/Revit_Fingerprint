# Chunk of dev_tools/repo_context/tests/test_packet_from_request.py

- Source relative path: `dev_tools/repo_context/tests/test_packet_from_request.py`
- Chunk: 2 of 3
- Original line range: 494-993
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_packet_header_and_footer_charged_against_budget, test_stale_source_since_scan_withholds_excerpt, test_resolution_sidecar_json_is_written, test_whole_file_symbol_listing_is_charged_against_budget, test_file_inventories_deferred_until_every_explicit_file_renders, test_explicit_file_excerpt_renders_before_optional_symbol_inventory, test_search_terms_share_a_single_global_max_files_cap, test_invalid_regex_notices_are_charged_against_budget, test_pathological_regex_search_term_times_out_instead_of_hanging, test_aggregate_regex_search_time_is_capped_across_all_terms, test_graphify_peer_listing_respects_max_files, test_graphify_withheld_on_dirty_worktree_even_with_matching_commit, test_graphify_not_withheld_for_dirtiness_confined_to_output_dir, test_callee_expansion_continues_past_a_rejected_file, test_search_term_matches_continue_past_a_rejected_file, test_overlong_question_is_rejected
- Source SHA-256: 832629a18a31295543da3b69cd0c0e509e3cd7f8abc9a473690264a8ccc3a31c
- Starts inside symbol: no
- Ends inside symbol: no

```
   494| def test_packet_header_and_footer_charged_against_budget(repo, out):
   495|     # Regression: the fixed header (title/root/question/provenance/
   496|     # limits) and footer were written with no budget accounting
   497|     # whatsoever -- an accepted (<=4000-char) question alone could make
   498|     # the real packet many times bigger than limits.max_estimated_tokens
   499|     # while the sidecar still reported a number near zero.
   500|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   501|     _scan(repo, out)
   502|     long_question = "why? " * 700  # comfortably under MAX_QUESTION_LENGTH (4000), still substantial
   503|     # A search-term selector, not an explicit file/symbol/line selector --
   504|     # the latter now correctly hard-aborts the whole packet if it can't
   505|     # fit alongside the (now-charged) header, which is a separate, correct
   506|     # invariant this test isn't about. A generous budget here so the
   507|     # request succeeds; the header (which embeds the question verbatim)
   508|     # is charged/reserved up front regardless (see rc_request.py's
   509|     # "Reserve the fixed framing's budget cost up front").
   510|     req = _request(out, "req.json", {
   511|         "schema_version": "1.0", "question": long_question,
   512|         "selectors": {"files": [], "symbols": [], "search_terms": ["nonexistent_term_xyz"], "lines": []},
   513|         "limits": {"max_estimated_tokens": 2000, "max_files": 12},
   514|     })
   515|     result = _packet(repo, out, req)
   516|     assert result.returncode == 0, result.stderr
   517|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   518|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   519|     assert sidecar["estimated_tokens_used"] > 100  # the question alone is ~700+ chars
   520|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   521| 
   522|     # And a budget too small to fit the header (which embeds the question
   523|     # verbatim) plus the footer must now hard-abort outright, rather than
   524|     # silently "succeed" with a packet whose true size is many times over
   525|     # the requested cap -- exactly the shape of the original bug this
   526|     # regression test exists for.
   527|     tiny_req = _request(out, "tiny.json", {
   528|         "schema_version": "1.0", "question": long_question,
   529|         "selectors": {"files": [], "symbols": [], "search_terms": ["nonexistent_term_xyz"], "lines": []},
   530|         "limits": {"max_estimated_tokens": 1, "max_files": 12},
   531|     })
   532|     tiny_result = _packet(repo, out, tiny_req)
   533|     assert tiny_result.returncode == 1
   534|     assert "too small to fit" in tiny_result.stderr
   535|     assert not (out / "packets" / "packet_tiny.md").exists()
   536| 
   537| 
   538| def test_stale_source_since_scan_withholds_excerpt(repo, out):
   539|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   540|     _scan(repo, out)
   541|     write_files(repo, {"core/a.py": "def f():\n    return 999\n"})
   542|     req = _request(out, "req.json", {
   543|         "schema_version": "1.0", "question": "q",
   544|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   545|     })
   546|     result = _packet(repo, out, req)
   547|     assert result.returncode == 0, result.stderr
   548|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   549|     assert "withheld" in text
   550|     assert "return 999" not in text
   551| 
   552| 
   553| def test_resolution_sidecar_json_is_written(repo, out):
   554|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   555|     _scan(repo, out)
   556|     req = _request(out, "req.json", {
   557|         "schema_version": "1.0", "question": "q",
   558|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   559|     })
   560|     result = _packet(repo, out, req)
   561|     assert result.returncode == 0, result.stderr
   562|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   563|     assert sidecar["schema_version"] == "1.0"
   564|     assert sidecar["question"] == "q"
   565|     assert sidecar["resolution_report"][0]["status"] == "resolved"
   566| 
   567| 
   568| def test_whole_file_symbol_listing_is_charged_against_budget(repo, out):
   569|     # Regression: the "Top-level symbols:" listing for an explicit whole-
   570|     # file selector used to be appended without any budget accounting, so
   571|     # a file with many top-level definitions could blow past
   572|     # limits.max_estimated_tokens while the packet reported far less
   573|     # usage than it actually rendered.
   574|     lines = []
   575|     for i in range(300):
   576|         lines += [f"def func_{i}():", f"    return {i}", ""]
   577|     write_files(repo, {"big.py": "\n".join(lines) + "\n"})
   578|     _scan(repo, out)
   579|     req = _request(out, "req.json", {
   580|         "schema_version": "1.0", "question": "q",
   581|         "selectors": {"files": ["big.py"], "symbols": [], "search_terms": [], "lines": []},
   582|         "limits": {"max_estimated_tokens": 20000, "max_files": 12},
   583|     })
   584|     result = _packet(repo, out, req)
   585|     assert result.returncode == 0, result.stderr
   586|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   587|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   588| 
   589|     # The reported estimated-token usage must not understate the packet's
   590|     # actual rendered size by a wide margin (the symbol-listing bug made
   591|     # this true even though every "func_i" line is metadata, not excerpt).
   592|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   593|     assert text.count("func_") <= 300 * 2  # listing entries + (bounded) excerpt lines only, no runaway duplication
   594|     assert "Omitted" in text or text.count("(function, lines") <= 300
   595| 
   596| 
   597| def test_file_inventories_deferred_until_every_explicit_file_renders(repo, out):
   598|     # Regression: one explicit file selector's "Top-level symbols:"
   599|     # inventory was still rendered before the *next* explicit file
   600|     # selector got its guaranteed shot at the budget. With a 30-function
   601|     # `a.py` selected before a tiny `b.py`, a.py's inventory could consume
   602|     # enough of a tight-but-otherwise-sufficient budget that b.py's own
   603|     # excerpt no longer fit -- reversing the selector order changed the
   604|     # outcome under the same limit, which is exactly the ordering-
   605|     # dependence this fix removes (every file's excerpt must render before
   606|     # *any* file's inventory spends a char).
   607|     n = 60
   608|     lines = [f"def f_{i:03d}(): pass" for i in range(n)]
   609|     write_files(repo, {
   610|         "a.py": "\n".join(lines) + "\n",
   611|         "b.py": "def tiny():\n    return 1\n",
   612|     })
   613|     _scan(repo, out)
   614| 
   615|     no_expansion = {"include_callers": False, "include_callees": False, "include_imports": False,
   616|                     "include_related_tests": False, "include_graphify": False}
   617|     generous_req = _request(out, "generous.json", {
   618|         "schema_version": "1.0", "question": "q",
   619|         "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
   620|         "expansion": no_expansion,
   621|         "limits": {"max_estimated_tokens": 200000, "max_files": 12},
   622|     })
   623|     generous_result = _packet(repo, out, generous_req)
   624|     assert generous_result.returncode == 0, generous_result.stderr
   625|     full_text = (out / "packets" / "packet_generous.md").read_text(encoding="utf-8")
   626|     listing_lines = [l for l in full_text.splitlines() if "(function, lines" in l]
   627|     assert len(listing_lines) == n + 1  # a.py's n functions plus b.py's own single "tiny" entry
   628|     listing_chars = sum(len(l) for l in listing_lines) + len("Top-level symbols:")
   629|     full_tokens = json.loads(
   630|         (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
   631|     )["estimated_tokens_used"]
   632| 
   633|     # Cut roughly half of a.py's inventory worth of room from the fully-
   634|     # fitting total -- still comfortably enough for both files' headers +
   635|     # excerpts (which this bug never touched), but not enough for a.py's
   636|     # full inventory too.
   637|     constrained_tokens = full_tokens - (listing_chars // 2 // 4)
   638| 
   639|     req = _request(out, "req.json", {
   640|         "schema_version": "1.0", "question": "q",
   641|         "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
   642|         "expansion": no_expansion,
   643|         "limits": {"max_estimated_tokens": constrained_tokens, "max_files": 12},
   644|     })
   645|     result = _packet(repo, out, req)
   646|     assert result.returncode == 0, result.stderr
   647|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   648|     # b.py's mandatory excerpt (a *later* explicit selector) must render
   649|     # regardless of a.py's inventory being tight.
   650|     assert "def tiny():" in text
   651|     assert "def f_000(): pass" in text  # a.py's own excerpt still renders in full too
   652|     listing_lines_constrained = [l for l in text.splitlines() if "(function, lines" in l]
   653|     assert len(listing_lines_constrained) < n  # a.py's inventory got truncated instead
   654| 
   655| 
   656| def test_explicit_file_excerpt_renders_before_optional_symbol_inventory(repo, out):
   657|     # Regression: the file explicit-selector loop spent budget on the
   658|     # "Top-level symbols:" inventory listing *before* rendering the file's
   659|     # own mandatory excerpt. A tight-but-sufficient budget (enough for the
   660|     # header + full excerpt, but not also the full inventory) let the
   661|     # optional inventory crowd out the mandatory excerpt, forcing a hard
   662|     # explicit_conflicts abort even though the file's actual requested
   663|     # content would have fit on its own. The excerpt must always render
   664|     # first; only the inventory may be truncated/omitted.
   665|     n = 150
   666|     lines = [f"def f_{i:03d}(): pass" for i in range(n)]
   667|     write_files(repo, {"core/big.py": "\n".join(lines) + "\n"})
   668|     _scan(repo, out)
   669| 
   670|     no_expansion = {"include_callers": False, "include_callees": False, "include_imports": False,
   671|                     "include_related_tests": False, "include_graphify": False}
   672|     generous_req = _request(out, "generous.json", {
   673|         "schema_version": "1.0", "question": "q",
   674|         "selectors": {"files": ["core/big.py"], "symbols": [], "search_terms": [], "lines": []},
   675|         "expansion": no_expansion,
   676|         "limits": {"max_estimated_tokens": 200000, "max_files": 12},
   677|     })
   678|     generous_result = _packet(repo, out, generous_req)
   679|     assert generous_result.returncode == 0, generous_result.stderr
   680|     full_text = (out / "packets" / "packet_generous.md").read_text(encoding="utf-8")
   681|     listing_lines = [l for l in full_text.splitlines() if "(function, lines" in l]
   682|     assert len(listing_lines) == n  # nothing tight yet -- every symbol is listed
   683|     listing_chars = sum(len(l) for l in listing_lines) + len("Top-level symbols:")
   684|     full_tokens = json.loads(
   685|         (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
   686|     )["estimated_tokens_used"]
   687| 
   688|     # Cut roughly half the inventory listing's worth of room from the
   689|     # fully-fitting budget: still comfortably enough for the header + full
   690|     # excerpt (which the listing bug never touched), but not enough for
   691|     # the full inventory listing too.
   692|     constrained_tokens = full_tokens - (listing_chars // 2 // 4)
   693| 
   694|     req = _request(out, "req.json", {
   695|         "schema_version": "1.0", "question": "q",
   696|         "selectors": {"files": ["core/big.py"], "symbols": [], "search_terms": [], "lines": []},
   697|         "expansion": no_expansion,
   698|         "limits": {"max_estimated_tokens": constrained_tokens, "max_files": 12},
   699|     })
   700|     result = _packet(repo, out, req)
   701|     assert result.returncode == 0, result.stderr
   702|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   703|     # Mandatory excerpt: every function's source line, including the very
   704|     # last one, must still be present in full.
   705|     assert "def f_000(): pass" in text
   706|     assert "def f_149(): pass" in text
   707|     # Optional inventory: truncated instead, never the excerpt.
   708|     listing_lines_constrained = [l for l in text.splitlines() if "(function, lines" in l]
   709|     assert len(listing_lines_constrained) < n
   710|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   711|     assert any("top-level symbol" in o.lower() for o in sidecar["omissions"])
   712| 
   713| 
   714| def test_search_terms_share_a_single_global_max_files_cap(repo, out):
   715|     # Regression: max_files was previously enforced per search term (a
   716|     # fresh `shown_files` set each iteration), so two different terms
   717|     # matching two different files could each individually stay "within"
   718|     # limits.max_files while the combined focus-file set exceeded it.
   719|     write_files(repo, {
   720|         "a.py": "def f():\n    return 'alpha_needle'\n",
   721|         "b.py": "def g():\n    return 'beta_needle'\n",
   722|     })
   723|     _scan(repo, out)
   724|     req = _request(out, "req.json", {
   725|         "schema_version": "1.0", "question": "q",
   726|         "selectors": {"files": [], "symbols": [], "search_terms": ["alpha_needle", "beta_needle"], "lines": []},
   727|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   728|     })
   729|     result = _packet(repo, out, req)
   730|     assert result.returncode == 0, result.stderr
   731|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   732|     assert len(sidecar["focus_files"]) <= 1
   733|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   734|     assert "Omitted" in text or "omitted beyond limits.max_files" in text
   735| 
   736| 
   737| def test_invalid_regex_notices_are_charged_against_budget(repo, out):
   738|     # Regression: each invalid-regex search term appended a notice with no
   739|     # budget accounting, so a request with many invalid regex terms could
   740|     # produce a large packet while reporting ~0 estimated tokens used
   741|     # under a tiny limits.max_estimated_tokens. This also exercises a
   742|     # second-order version of the same bug: each skipped notice fell back
   743|     # to an *unbudgeted* budget.omissions entry, and the final "## Omitted
   744|     # / unresolved" section rendered that whole list without any size
   745|     # accounting either -- both layers had to be fixed for this to pass.
   746|     write_files(repo, {"a.py": "def f():\n    return 1\n"})
   747|     _scan(repo, out)
   748|     bad_terms = [f"(unclosed_{i}" for i in range(200)]
   749|     # 300 tokens clears the fixed framing's own floor (header + selector-
   750|     # resolution report + footer, reserved up front -- see rc_request.py's
   751|     # "Reserve the fixed framing's budget cost up front") but is nowhere
   752|     # near enough to fit all 200 invalid-regex notices.
   753|     req = _request(out, "req.json", {
   754|         "schema_version": "1.0", "question": "q",
   755|         "selectors": {"files": [], "symbols": [], "search_terms": bad_terms, "lines": []},
   756|         "expansion": {"search_as_regex": True},
   757|         "limits": {"max_estimated_tokens": 300, "max_files": 500},
   758|     })
   759|     result = _packet(repo, out, req)
   760|     assert result.returncode == 0, result.stderr
   761|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   762|     assert len(text) < 3000
   763|     assert text.count("not a valid regex") < 200
   764| 
   765| 
   766| def test_pathological_regex_search_term_times_out_instead_of_hanging(repo, out, monkeypatch):
   767|     # Regression: search_as_regex ran a caller-supplied pattern through
   768|     # plain re.search with no bound on evaluation time. A syntactically
   769|     # valid but pathological pattern like `(a+)+$` against a long, nearly-
   770|     # matching line triggers catastrophic backtracking -- confirmed to
   771|     # still be running after 20+ seconds for just 35 characters on this
   772|     # engine -- which could hang the CLI indefinitely for an LLM-produced
   773|     # or malicious request. Each term's evaluation must be bounded.
   774|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TIMEOUT_SECONDS", 0.5)
   775|     write_files(repo, {"core/a.py": "x = '" + "a" * 35 + "!'\n"})
   776|     _scan(repo, out)
   777|     files_rows = rr._load_csv(out / "file_inventory.csv")
   778|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["(a+)+$"], True, files_rows, 12)
   779|     assert resolutions[0].status == "invalid"
   780|     assert "exceeded" in resolutions[0].detail
   781|     assert matches_by_term["(a+)+$"] == []
   782| 
   783|     # A normal (non-pathological) regex must still work correctly under
   784|     # the same bounded path -- the timeout mechanism must not break
   785|     # ordinary regex search.
   786|     resolutions2, matches_by_term2, _ = rr.resolve_search_terms(repo, ["a{3}"], True, files_rows, 12)
   787|     assert resolutions2[0].status == "resolved"
   788|     assert matches_by_term2["a{3}"]
   789| 
   790| 
   791| def test_aggregate_regex_search_time_is_capped_across_all_terms(repo, out, monkeypatch):
   792|     # Regression: the per-term SIGALRM bound stops any *one* pathological
   793|     # pattern from hanging forever, but the schema places no cap on how
   794|     # many search_terms a request can carry -- a request with several
   795|     # distinct catastrophic-backtracking patterns could still burn a full
   796|     # per-term allowance for *each one* before packet budgeting even
   797|     # began. Three terms each requesting up to 1.0s, under a 1.5s
   798|     # aggregate cap, must finish in well under 3.0s total, with the terms
   799|     # beyond the aggregate deadline reported as skipped rather than each
   800|     # getting their own fresh timeout.
   801|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TIMEOUT_SECONDS", 1.0)
   802|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS", 1.5)
   803|     write_files(repo, {"core/a.py": "x = '" + "a" * 35 + "!'\n"})
   804|     _scan(repo, out)
   805|     files_rows = rr._load_csv(out / "file_inventory.csv")
   806|     start = time.monotonic()
   807|     # Three distinct-looking patterns, all pathological against the same
   808|     # run of `a` characters (a term repeated verbatim would risk being
   809|     # deduplicated by a caller upstream of this function; these aren't).
   810|     resolutions, matches_by_term, _ = rr.resolve_search_terms(
   811|         repo, ["(a+)+$", "(a+)*$", "(a|aa)+$"], True, files_rows, 12,
   812|     )
   813|     elapsed = time.monotonic() - start
   814|     assert elapsed < 2.5  # comfortably bounded, not the ~3.0s+ three full per-term timeouts would take
   815|     assert all(r.status == "invalid" for r in resolutions)
   816|     # At least the last term must be skipped outright by the aggregate
   817|     # deadline rather than getting its own fresh per-term timeout.
   818|     assert any("aggregate" in r.detail for r in resolutions)
   819| 
   820| 
   821| def test_graphify_peer_listing_respects_max_files(repo, out):
   822|     # Regression: Graphify community-peer paths were emitted without
   823|     # going through note_focus_file, so they could exceed limits.max_files
   824|     # while the resolution sidecar's focus_files list stayed under it.
   825|     write_files(repo, {
   826|         "core/a.py": "def f():\n    return 1\n",
   827|         "core/b.py": "def g():\n    return 2\n",
   828|     })
   829|     commit = _git_init_commit(repo)
   830|     graph = {
   831|         "built_at_commit": commit,
   832|         "nodes": [
   833|             {"source_file": "core/a.py", "community": 5, "community_name": "Widgets"},
   834|             {"source_file": "core/b.py", "community": 5, "community_name": "Widgets"},
   835|         ],
   836|     }
   837|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   838|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   839|     _scan(repo, out)
   840|     req = _request(out, "req.json", {
   841|         "schema_version": "1.0", "question": "q",
   842|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   843|         "expansion": {"include_graphify": True},
   844|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   845|     })
   846|     result = _packet(repo, out, req)
   847|     assert result.returncode == 0, result.stderr
   848|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   849|     assert sidecar["focus_files"] == ["core/a.py"]
   850|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   851|     # core/b.py must never appear as a rendered, [origin:
   852|     # graphify_expansion]-tagged evidence item -- it's beyond
   853|     # limits.max_files, reported only via the batched omission note.
   854|     assert "[origin: graphify_expansion]" not in text
   855|     assert "Graphify community peer(s)" in text
   856|     assert "limits.max_files" in text
   857| 
   858| 
   859| def test_graphify_withheld_on_dirty_worktree_even_with_matching_commit(repo, out):
   860|     # Regression: a matching built_at_commit was accepted even when the
   861|     # scanned worktree had uncommitted changes -- a matching commit hash
   862|     # alone doesn't prove graph.json's communities still describe what's
   863|     # actually on disk if a tracked file was modified since that commit.
   864|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   865|     commit = _git_init_commit(repo)
   866|     graph = {
   867|         "built_at_commit": commit,
   868|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
   869|     }
   870|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   871|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   872|     # Modify a tracked file after the commit, without committing again --
   873|     # this is what makes the worktree dirty even though HEAD still equals
   874|     # the commit graph.json names.
   875|     write_files(repo, {"core/a.py": "def f():\n    return 999\n"})
   876|     _scan(repo, out)
   877|     req = _request(out, "req.json", {
   878|         "schema_version": "1.0", "question": "q",
   879|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   880|         "expansion": {"include_graphify": True},
   881|     })
   882|     result = _packet(repo, out, req)
   883|     assert result.returncode == 0, result.stderr
   884|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   885|     assert "graphify_expansion" not in text
   886|     assert "worktree is dirty" in text
   887| 
   888| 
   889| def test_graphify_not_withheld_for_dirtiness_confined_to_output_dir(repo):
   890|     # Regression: get_git_info(root) reported "dirty" for *any* uncommitted
   891|     # change anywhere in the worktree, including this tool's own freshly
   892|     # written --output directory when it lives inside the scanned repo (as
   893|     # it does for this project's own repo_context/). scan/packet always
   894|     # write fresh output before this check runs, so every single run
   895|     # against such a repo made the worktree look dirty and withheld
   896|     # Graphify evidence for a reason that has nothing to do with the
   897|     # scanned *source* changing.
   898|     output_dir = repo / "repo_context"
   899|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   900|     commit = _git_init_commit(repo)
   901|     graph = {
   902|         "built_at_commit": commit,
   903|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
   904|     }
   905|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   906|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   907|     _scan(repo, output_dir)  # writes brand-new, untracked files *inside* repo -- this alone used to dirty git status
   908|     req = _request(output_dir, "req.json", {
   909|         "schema_version": "1.0", "question": "q",
   910|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   911|         "expansion": {"include_graphify": True},
   912|     })
   913|     result = _packet(repo, output_dir, req)
   914|     assert result.returncode == 0, result.stderr
   915|     text = (output_dir / "packets" / "packet_req.md").read_text(encoding="utf-8")
   916|     assert "worktree is dirty" not in text
   917| 
   918| 
   919| def test_callee_expansion_continues_past_a_rejected_file(repo, out):
   920|     # Regression: the callee-expansion loop `break`-ed the whole listing
   921|     # on the first callee whose file was beyond limits.max_files, even
   922|     # though a *later* callee might be in a file already in focus_files
   923|     # (free -- no new slot needed). `f`'s first callee `g` lives in a
   924|     # different, not-yet-focused file; its second callee `h` lives in the
   925|     # same file as `f` itself (already focused, since that's the selected
   926|     # symbol's own file). With max_files:1, `g` must be skipped but `h`
   927|     # must still render -- not silently dropped along with it.
   928|     write_files(repo, {
   929|         "core/a.py": "from core.other import g\n\n\ndef h():\n    return 1\n\n\ndef f():\n    g()\n    h()\n    return 1\n",
   930|         "core/other.py": "def g():\n    return 2\n",
   931|     })
   932|     _scan(repo, out)
   933|     req = _request(out, "req.json", {
   934|         "schema_version": "1.0", "question": "q",
   935|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   936|         "expansion": {"include_callers": False, "include_callees": True, "include_imports": False,
   937|                       "include_related_tests": False, "include_graphify": False},
   938|         "limits": {"max_files": 1},
   939|     })
   940|     result = _packet(repo, out, req)
   941|     assert result.returncode == 0, result.stderr
   942|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   943|     assert "-> `h`" in text  # the already-focused-file callee must still render
   944|     assert "core/other.py" not in text  # the beyond-max_files callee stays omitted
   945|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   946|     assert sidecar["focus_files"] == ["core/a.py"]
   947| 
   948| 
   949| def test_search_term_matches_continue_past_a_rejected_file(repo, out):
   950|     # Regression: the search-match loop `break`-ed on the first match
   951|     # whose file was beyond limits.max_files, even though a *later* match
   952|     # (for the same term) might be in a file already in focus_files. With
   953|     # `z/main.py` explicitly selected and `needle_term` matching both
   954|     # `a/other.py` (scanned first, alphabetically) and `z/main.py`
   955|     # (already focused), max_files:1 must still render the z/main.py
   956|     # match instead of losing both to the first rejection.
   957|     write_files(repo, {
   958|         "a/other.py": "# needle_term\n",
   959|         "z/main.py": "# needle_term\n",
   960|     })
   961|     _scan(repo, out)
   962|     req = _request(out, "req.json", {
   963|         "schema_version": "1.0", "question": "q",
   964|         "selectors": {"files": ["z/main.py"], "symbols": [], "search_terms": ["needle_term"], "lines": []},
   965|         "limits": {"max_files": 1},
   966|     })
   967|     result = _packet(repo, out, req)
   968|     assert result.returncode == 0, result.stderr
   969|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   970|     assert "z/main.py:1" in text
   971|     assert "a/other.py" not in text
   972|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   973|     assert sidecar["focus_files"] == ["z/main.py"]
   974| 
   975| 
   976| def test_overlong_question_is_rejected(repo, out):
   977|     # Regression: `question` is copied verbatim into every packet's
   978|     # header with no budget accounting and no schema length limit -- an
   979|     # oversized value could make a packet exceed limits.max_estimated_tokens
   980|     # through the header alone.
   981|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   982|     _scan(repo, out)
   983|     req = _request(out, "req.json", {
   984|         "schema_version": "1.0", "question": "x" * 5000,
   985|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   986|         "limits": {"max_estimated_tokens": 1, "max_files": 12},
   987|     })
   988|     result = _packet(repo, out, req)
   989|     assert result.returncode == 1
   990|     assert "too long" in result.stderr
   991|     assert not (out / "packets" / "packet_req.md").exists()
   992| 
   993| 
```
