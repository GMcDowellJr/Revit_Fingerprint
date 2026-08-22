# Chunk of dev_tools/repo_context/tests/test_packet_from_request.py

- Source relative path: `dev_tools/repo_context/tests/test_packet_from_request.py`
- Chunk: 2 of 3
- Original line range: 513-1008
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_stale_source_since_scan_withholds_excerpt, test_resolution_sidecar_json_is_written, test_whole_file_symbol_listing_is_charged_against_budget, test_file_inventories_deferred_until_every_explicit_file_renders, test_explicit_file_excerpt_renders_before_optional_symbol_inventory, test_search_terms_share_a_single_global_max_files_cap, test_invalid_regex_notices_are_charged_against_budget, test_pathological_regex_search_term_times_out_instead_of_hanging, test_aggregate_regex_search_time_is_capped_across_all_terms, test_graphify_peer_listing_respects_max_files, test_graphify_withheld_on_dirty_worktree_even_with_matching_commit, test_graphify_not_withheld_for_dirtiness_confined_to_output_dir, test_callee_expansion_continues_past_a_rejected_file, test_search_term_matches_continue_past_a_rejected_file, test_overlong_question_is_rejected, test_caller_callee_import_expansion_respects_max_files, _git_init_commit
- Source SHA-256: 30ed034adfbd24213b55c30a03d99cc5f8036eb9a7f7a36502023450b6d37a45
- Starts inside symbol: no
- Ends inside symbol: no

```
   513| def test_stale_source_since_scan_withholds_excerpt(repo, out):
   514|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   515|     _scan(repo, out)
   516|     write_files(repo, {"core/a.py": "def f():\n    return 999\n"})
   517|     req = _request(out, "req.json", {
   518|         "schema_version": "1.0", "question": "q",
   519|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   520|     })
   521|     result = _packet(repo, out, req)
   522|     assert result.returncode == 0, result.stderr
   523|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   524|     assert "withheld" in text
   525|     assert "return 999" not in text
   526| 
   527| 
   528| def test_resolution_sidecar_json_is_written(repo, out):
   529|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   530|     _scan(repo, out)
   531|     req = _request(out, "req.json", {
   532|         "schema_version": "1.0", "question": "q",
   533|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   534|     })
   535|     result = _packet(repo, out, req)
   536|     assert result.returncode == 0, result.stderr
   537|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   538|     assert sidecar["schema_version"] == "1.0"
   539|     assert sidecar["question"] == "q"
   540|     assert sidecar["resolution_report"][0]["status"] == "resolved"
   541| 
   542| 
   543| def test_whole_file_symbol_listing_is_charged_against_budget(repo, out):
   544|     # Regression: the "Top-level symbols:" listing for an explicit whole-
   545|     # file selector used to be appended without any budget accounting, so
   546|     # a file with many top-level definitions could blow past
   547|     # limits.max_estimated_tokens while the packet reported far less
   548|     # usage than it actually rendered.
   549|     lines = []
   550|     for i in range(300):
   551|         lines += [f"def func_{i}():", f"    return {i}", ""]
   552|     write_files(repo, {"big.py": "\n".join(lines) + "\n"})
   553|     _scan(repo, out)
   554|     req = _request(out, "req.json", {
   555|         "schema_version": "1.0", "question": "q",
   556|         "selectors": {"files": ["big.py"], "symbols": [], "search_terms": [], "lines": []},
   557|         "limits": {"max_estimated_tokens": 20000, "max_files": 12},
   558|     })
   559|     result = _packet(repo, out, req)
   560|     assert result.returncode == 0, result.stderr
   561|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   562|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   563| 
   564|     # The reported estimated-token usage must not understate the packet's
   565|     # actual rendered size by a wide margin (the symbol-listing bug made
   566|     # this true even though every "func_i" line is metadata, not excerpt).
   567|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   568|     assert text.count("func_") <= 300 * 2  # listing entries + (bounded) excerpt lines only, no runaway duplication
   569|     assert "Omitted" in text or text.count("(function, lines") <= 300
   570| 
   571| 
   572| def test_file_inventories_deferred_until_every_explicit_file_renders(repo, out):
   573|     # Regression: one explicit file selector's "Top-level symbols:"
   574|     # inventory was still rendered before the *next* explicit file
   575|     # selector got its guaranteed shot at the budget. With a 30-function
   576|     # `a.py` selected before a tiny `b.py`, a.py's inventory could consume
   577|     # enough of a tight-but-otherwise-sufficient budget that b.py's own
   578|     # excerpt no longer fit -- reversing the selector order changed the
   579|     # outcome under the same limit, which is exactly the ordering-
   580|     # dependence this fix removes (every file's excerpt must render before
   581|     # *any* file's inventory spends a char).
   582|     n = 60
   583|     lines = [f"def f_{i:03d}(): pass" for i in range(n)]
   584|     write_files(repo, {
   585|         "a.py": "\n".join(lines) + "\n",
   586|         "b.py": "def tiny():\n    return 1\n",
   587|     })
   588|     _scan(repo, out)
   589| 
   590|     no_expansion = {"include_callers": False, "include_callees": False, "include_imports": False,
   591|                     "include_related_tests": False, "include_graphify": False}
   592|     generous_req = _request(out, "generous.json", {
   593|         "schema_version": "1.0", "question": "q",
   594|         "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
   595|         "expansion": no_expansion,
   596|         "limits": {"max_estimated_tokens": 200000, "max_files": 12},
   597|     })
   598|     generous_result = _packet(repo, out, generous_req)
   599|     assert generous_result.returncode == 0, generous_result.stderr
   600|     full_text = (out / "packets" / "packet_generous.md").read_text(encoding="utf-8")
   601|     listing_lines = [l for l in full_text.splitlines() if "(function, lines" in l]
   602|     assert len(listing_lines) == n + 1  # a.py's n functions plus b.py's own single "tiny" entry
   603|     listing_chars = sum(len(l) for l in listing_lines) + len("Top-level symbols:")
   604|     full_tokens = json.loads(
   605|         (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
   606|     )["estimated_tokens_used"]
   607| 
   608|     # Cut roughly half of a.py's inventory worth of room from the fully-
   609|     # fitting total -- still comfortably enough for both files' headers +
   610|     # excerpts (which this bug never touched), but not enough for a.py's
   611|     # full inventory too.
   612|     constrained_tokens = full_tokens - (listing_chars // 2 // 4)
   613| 
   614|     req = _request(out, "req.json", {
   615|         "schema_version": "1.0", "question": "q",
   616|         "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
   617|         "expansion": no_expansion,
   618|         "limits": {"max_estimated_tokens": constrained_tokens, "max_files": 12},
   619|     })
   620|     result = _packet(repo, out, req)
   621|     assert result.returncode == 0, result.stderr
   622|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   623|     # b.py's mandatory excerpt (a *later* explicit selector) must render
   624|     # regardless of a.py's inventory being tight.
   625|     assert "def tiny():" in text
   626|     assert "def f_000(): pass" in text  # a.py's own excerpt still renders in full too
   627|     listing_lines_constrained = [l for l in text.splitlines() if "(function, lines" in l]
   628|     assert len(listing_lines_constrained) < n  # a.py's inventory got truncated instead
   629| 
   630| 
   631| def test_explicit_file_excerpt_renders_before_optional_symbol_inventory(repo, out):
   632|     # Regression: the file explicit-selector loop spent budget on the
   633|     # "Top-level symbols:" inventory listing *before* rendering the file's
   634|     # own mandatory excerpt. A tight-but-sufficient budget (enough for the
   635|     # header + full excerpt, but not also the full inventory) let the
   636|     # optional inventory crowd out the mandatory excerpt, forcing a hard
   637|     # explicit_conflicts abort even though the file's actual requested
   638|     # content would have fit on its own. The excerpt must always render
   639|     # first; only the inventory may be truncated/omitted.
   640|     n = 150
   641|     lines = [f"def f_{i:03d}(): pass" for i in range(n)]
   642|     write_files(repo, {"core/big.py": "\n".join(lines) + "\n"})
   643|     _scan(repo, out)
   644| 
   645|     no_expansion = {"include_callers": False, "include_callees": False, "include_imports": False,
   646|                     "include_related_tests": False, "include_graphify": False}
   647|     generous_req = _request(out, "generous.json", {
   648|         "schema_version": "1.0", "question": "q",
   649|         "selectors": {"files": ["core/big.py"], "symbols": [], "search_terms": [], "lines": []},
   650|         "expansion": no_expansion,
   651|         "limits": {"max_estimated_tokens": 200000, "max_files": 12},
   652|     })
   653|     generous_result = _packet(repo, out, generous_req)
   654|     assert generous_result.returncode == 0, generous_result.stderr
   655|     full_text = (out / "packets" / "packet_generous.md").read_text(encoding="utf-8")
   656|     listing_lines = [l for l in full_text.splitlines() if "(function, lines" in l]
   657|     assert len(listing_lines) == n  # nothing tight yet -- every symbol is listed
   658|     listing_chars = sum(len(l) for l in listing_lines) + len("Top-level symbols:")
   659|     full_tokens = json.loads(
   660|         (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
   661|     )["estimated_tokens_used"]
   662| 
   663|     # Cut roughly half the inventory listing's worth of room from the
   664|     # fully-fitting budget: still comfortably enough for the header + full
   665|     # excerpt (which the listing bug never touched), but not enough for
   666|     # the full inventory listing too.
   667|     constrained_tokens = full_tokens - (listing_chars // 2 // 4)
   668| 
   669|     req = _request(out, "req.json", {
   670|         "schema_version": "1.0", "question": "q",
   671|         "selectors": {"files": ["core/big.py"], "symbols": [], "search_terms": [], "lines": []},
   672|         "expansion": no_expansion,
   673|         "limits": {"max_estimated_tokens": constrained_tokens, "max_files": 12},
   674|     })
   675|     result = _packet(repo, out, req)
   676|     assert result.returncode == 0, result.stderr
   677|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   678|     # Mandatory excerpt: every function's source line, including the very
   679|     # last one, must still be present in full.
   680|     assert "def f_000(): pass" in text
   681|     assert "def f_149(): pass" in text
   682|     # Optional inventory: truncated instead, never the excerpt.
   683|     listing_lines_constrained = [l for l in text.splitlines() if "(function, lines" in l]
   684|     assert len(listing_lines_constrained) < n
   685|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   686|     assert any("top-level symbol" in o.lower() for o in sidecar["omissions"])
   687| 
   688| 
   689| def test_search_terms_share_a_single_global_max_files_cap(repo, out):
   690|     # Regression: max_files was previously enforced per search term (a
   691|     # fresh `shown_files` set each iteration), so two different terms
   692|     # matching two different files could each individually stay "within"
   693|     # limits.max_files while the combined focus-file set exceeded it.
   694|     write_files(repo, {
   695|         "a.py": "def f():\n    return 'alpha_needle'\n",
   696|         "b.py": "def g():\n    return 'beta_needle'\n",
   697|     })
   698|     _scan(repo, out)
   699|     req = _request(out, "req.json", {
   700|         "schema_version": "1.0", "question": "q",
   701|         "selectors": {"files": [], "symbols": [], "search_terms": ["alpha_needle", "beta_needle"], "lines": []},
   702|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   703|     })
   704|     result = _packet(repo, out, req)
   705|     assert result.returncode == 0, result.stderr
   706|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   707|     assert len(sidecar["focus_files"]) <= 1
   708|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   709|     assert "Omitted" in text or "omitted beyond limits.max_files" in text
   710| 
   711| 
   712| def test_invalid_regex_notices_are_charged_against_budget(repo, out):
   713|     # Regression: each invalid-regex search term appended a notice with no
   714|     # budget accounting, so a request with many invalid regex terms could
   715|     # produce a large packet while reporting ~0 estimated tokens used
   716|     # under a tiny limits.max_estimated_tokens. This also exercises a
   717|     # second-order version of the same bug: each skipped notice fell back
   718|     # to an *unbudgeted* budget.omissions entry, and the final "## Omitted
   719|     # / unresolved" section rendered that whole list without any size
   720|     # accounting either -- both layers had to be fixed for this to pass.
   721|     write_files(repo, {"a.py": "def f():\n    return 1\n"})
   722|     _scan(repo, out)
   723|     bad_terms = [f"(unclosed_{i}" for i in range(200)]
   724|     req = _request(out, "req.json", {
   725|         "schema_version": "1.0", "question": "q",
   726|         "selectors": {"files": [], "symbols": [], "search_terms": bad_terms, "lines": []},
   727|         "expansion": {"search_as_regex": True},
   728|         "limits": {"max_estimated_tokens": 1, "max_files": 500},
   729|     })
   730|     result = _packet(repo, out, req)
   731|     assert result.returncode == 0, result.stderr
   732|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   733|     assert len(text) < 3000
   734|     assert text.count("not a valid regex") < 200
   735| 
   736| 
   737| def test_pathological_regex_search_term_times_out_instead_of_hanging(repo, out, monkeypatch):
   738|     # Regression: search_as_regex ran a caller-supplied pattern through
   739|     # plain re.search with no bound on evaluation time. A syntactically
   740|     # valid but pathological pattern like `(a+)+$` against a long, nearly-
   741|     # matching line triggers catastrophic backtracking -- confirmed to
   742|     # still be running after 20+ seconds for just 35 characters on this
   743|     # engine -- which could hang the CLI indefinitely for an LLM-produced
   744|     # or malicious request. Each term's evaluation must be bounded.
   745|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TIMEOUT_SECONDS", 0.5)
   746|     write_files(repo, {"core/a.py": "x = '" + "a" * 35 + "!'\n"})
   747|     _scan(repo, out)
   748|     files_rows = rr._load_csv(out / "file_inventory.csv")
   749|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["(a+)+$"], True, files_rows, 12)
   750|     assert resolutions[0].status == "invalid"
   751|     assert "exceeded" in resolutions[0].detail
   752|     assert matches_by_term["(a+)+$"] == []
   753| 
   754|     # A normal (non-pathological) regex must still work correctly under
   755|     # the same bounded path -- the timeout mechanism must not break
   756|     # ordinary regex search.
   757|     resolutions2, matches_by_term2, _ = rr.resolve_search_terms(repo, ["a{3}"], True, files_rows, 12)
   758|     assert resolutions2[0].status == "resolved"
   759|     assert matches_by_term2["a{3}"]
   760| 
   761| 
   762| def test_aggregate_regex_search_time_is_capped_across_all_terms(repo, out, monkeypatch):
   763|     # Regression: the per-term SIGALRM bound stops any *one* pathological
   764|     # pattern from hanging forever, but the schema places no cap on how
   765|     # many search_terms a request can carry -- a request with several
   766|     # distinct catastrophic-backtracking patterns could still burn a full
   767|     # per-term allowance for *each one* before packet budgeting even
   768|     # began. Three terms each requesting up to 1.0s, under a 1.5s
   769|     # aggregate cap, must finish in well under 3.0s total, with the terms
   770|     # beyond the aggregate deadline reported as skipped rather than each
   771|     # getting their own fresh timeout.
   772|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TIMEOUT_SECONDS", 1.0)
   773|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS", 1.5)
   774|     write_files(repo, {"core/a.py": "x = '" + "a" * 35 + "!'\n"})
   775|     _scan(repo, out)
   776|     files_rows = rr._load_csv(out / "file_inventory.csv")
   777|     start = time.monotonic()
   778|     # Three distinct-looking patterns, all pathological against the same
   779|     # run of `a` characters (a term repeated verbatim would risk being
   780|     # deduplicated by a caller upstream of this function; these aren't).
   781|     resolutions, matches_by_term, _ = rr.resolve_search_terms(
   782|         repo, ["(a+)+$", "(a+)*$", "(a|aa)+$"], True, files_rows, 12,
   783|     )
   784|     elapsed = time.monotonic() - start
   785|     assert elapsed < 2.5  # comfortably bounded, not the ~3.0s+ three full per-term timeouts would take
   786|     assert all(r.status == "invalid" for r in resolutions)
   787|     # At least the last term must be skipped outright by the aggregate
   788|     # deadline rather than getting its own fresh per-term timeout.
   789|     assert any("aggregate" in r.detail for r in resolutions)
   790| 
   791| 
   792| def test_graphify_peer_listing_respects_max_files(repo, out):
   793|     # Regression: Graphify community-peer paths were emitted without
   794|     # going through note_focus_file, so they could exceed limits.max_files
   795|     # while the resolution sidecar's focus_files list stayed under it.
   796|     write_files(repo, {
   797|         "core/a.py": "def f():\n    return 1\n",
   798|         "core/b.py": "def g():\n    return 2\n",
   799|     })
   800|     commit = _git_init_commit(repo)
   801|     graph = {
   802|         "built_at_commit": commit,
   803|         "nodes": [
   804|             {"source_file": "core/a.py", "community": 5, "community_name": "Widgets"},
   805|             {"source_file": "core/b.py", "community": 5, "community_name": "Widgets"},
   806|         ],
   807|     }
   808|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   809|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   810|     _scan(repo, out)
   811|     req = _request(out, "req.json", {
   812|         "schema_version": "1.0", "question": "q",
   813|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   814|         "expansion": {"include_graphify": True},
   815|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   816|     })
   817|     result = _packet(repo, out, req)
   818|     assert result.returncode == 0, result.stderr
   819|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   820|     assert sidecar["focus_files"] == ["core/a.py"]
   821|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   822|     # core/b.py must never appear as a rendered, [origin:
   823|     # graphify_expansion]-tagged evidence item -- it's beyond
   824|     # limits.max_files, reported only via the batched omission note.
   825|     assert "[origin: graphify_expansion]" not in text
   826|     assert "Graphify community peer(s)" in text
   827|     assert "limits.max_files" in text
   828| 
   829| 
   830| def test_graphify_withheld_on_dirty_worktree_even_with_matching_commit(repo, out):
   831|     # Regression: a matching built_at_commit was accepted even when the
   832|     # scanned worktree had uncommitted changes -- a matching commit hash
   833|     # alone doesn't prove graph.json's communities still describe what's
   834|     # actually on disk if a tracked file was modified since that commit.
   835|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   836|     commit = _git_init_commit(repo)
   837|     graph = {
   838|         "built_at_commit": commit,
   839|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
   840|     }
   841|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   842|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   843|     # Modify a tracked file after the commit, without committing again --
   844|     # this is what makes the worktree dirty even though HEAD still equals
   845|     # the commit graph.json names.
   846|     write_files(repo, {"core/a.py": "def f():\n    return 999\n"})
   847|     _scan(repo, out)
   848|     req = _request(out, "req.json", {
   849|         "schema_version": "1.0", "question": "q",
   850|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   851|         "expansion": {"include_graphify": True},
   852|     })
   853|     result = _packet(repo, out, req)
   854|     assert result.returncode == 0, result.stderr
   855|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   856|     assert "graphify_expansion" not in text
   857|     assert "worktree is dirty" in text
   858| 
   859| 
   860| def test_graphify_not_withheld_for_dirtiness_confined_to_output_dir(repo):
   861|     # Regression: get_git_info(root) reported "dirty" for *any* uncommitted
   862|     # change anywhere in the worktree, including this tool's own freshly
   863|     # written --output directory when it lives inside the scanned repo (as
   864|     # it does for this project's own repo_context/). scan/packet always
   865|     # write fresh output before this check runs, so every single run
   866|     # against such a repo made the worktree look dirty and withheld
   867|     # Graphify evidence for a reason that has nothing to do with the
   868|     # scanned *source* changing.
   869|     output_dir = repo / "repo_context"
   870|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   871|     commit = _git_init_commit(repo)
   872|     graph = {
   873|         "built_at_commit": commit,
   874|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
   875|     }
   876|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   877|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   878|     _scan(repo, output_dir)  # writes brand-new, untracked files *inside* repo -- this alone used to dirty git status
   879|     req = _request(output_dir, "req.json", {
   880|         "schema_version": "1.0", "question": "q",
   881|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   882|         "expansion": {"include_graphify": True},
   883|     })
   884|     result = _packet(repo, output_dir, req)
   885|     assert result.returncode == 0, result.stderr
   886|     text = (output_dir / "packets" / "packet_req.md").read_text(encoding="utf-8")
   887|     assert "worktree is dirty" not in text
   888| 
   889| 
   890| def test_callee_expansion_continues_past_a_rejected_file(repo, out):
   891|     # Regression: the callee-expansion loop `break`-ed the whole listing
   892|     # on the first callee whose file was beyond limits.max_files, even
   893|     # though a *later* callee might be in a file already in focus_files
   894|     # (free -- no new slot needed). `f`'s first callee `g` lives in a
   895|     # different, not-yet-focused file; its second callee `h` lives in the
   896|     # same file as `f` itself (already focused, since that's the selected
   897|     # symbol's own file). With max_files:1, `g` must be skipped but `h`
   898|     # must still render -- not silently dropped along with it.
   899|     write_files(repo, {
   900|         "core/a.py": "from core.other import g\n\n\ndef h():\n    return 1\n\n\ndef f():\n    g()\n    h()\n    return 1\n",
   901|         "core/other.py": "def g():\n    return 2\n",
   902|     })
   903|     _scan(repo, out)
   904|     req = _request(out, "req.json", {
   905|         "schema_version": "1.0", "question": "q",
   906|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   907|         "expansion": {"include_callers": False, "include_callees": True, "include_imports": False,
   908|                       "include_related_tests": False, "include_graphify": False},
   909|         "limits": {"max_files": 1},
   910|     })
   911|     result = _packet(repo, out, req)
   912|     assert result.returncode == 0, result.stderr
   913|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   914|     assert "-> `h`" in text  # the already-focused-file callee must still render
   915|     assert "core/other.py" not in text  # the beyond-max_files callee stays omitted
   916|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   917|     assert sidecar["focus_files"] == ["core/a.py"]
   918| 
   919| 
   920| def test_search_term_matches_continue_past_a_rejected_file(repo, out):
   921|     # Regression: the search-match loop `break`-ed on the first match
   922|     # whose file was beyond limits.max_files, even though a *later* match
   923|     # (for the same term) might be in a file already in focus_files. With
   924|     # `z/main.py` explicitly selected and `needle_term` matching both
   925|     # `a/other.py` (scanned first, alphabetically) and `z/main.py`
   926|     # (already focused), max_files:1 must still render the z/main.py
   927|     # match instead of losing both to the first rejection.
   928|     write_files(repo, {
   929|         "a/other.py": "# needle_term\n",
   930|         "z/main.py": "# needle_term\n",
   931|     })
   932|     _scan(repo, out)
   933|     req = _request(out, "req.json", {
   934|         "schema_version": "1.0", "question": "q",
   935|         "selectors": {"files": ["z/main.py"], "symbols": [], "search_terms": ["needle_term"], "lines": []},
   936|         "limits": {"max_files": 1},
   937|     })
   938|     result = _packet(repo, out, req)
   939|     assert result.returncode == 0, result.stderr
   940|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   941|     assert "z/main.py:1" in text
   942|     assert "a/other.py" not in text
   943|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   944|     assert sidecar["focus_files"] == ["z/main.py"]
   945| 
   946| 
   947| def test_overlong_question_is_rejected(repo, out):
   948|     # Regression: `question` is copied verbatim into every packet's
   949|     # header with no budget accounting and no schema length limit -- an
   950|     # oversized value could make a packet exceed limits.max_estimated_tokens
   951|     # through the header alone.
   952|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   953|     _scan(repo, out)
   954|     req = _request(out, "req.json", {
   955|         "schema_version": "1.0", "question": "x" * 5000,
   956|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   957|         "limits": {"max_estimated_tokens": 1, "max_files": 12},
   958|     })
   959|     result = _packet(repo, out, req)
   960|     assert result.returncode == 1
   961|     assert "too long" in result.stderr
   962|     assert not (out / "packets" / "packet_req.md").exists()
   963| 
   964| 
   965| def test_caller_callee_import_expansion_respects_max_files(repo, out):
   966|     # Regression: callers/callees/internal-imports listings emitted every
   967|     # referenced file without going through note_focus_file, so they could
   968|     # exceed limits.max_files while the resolution sidecar's focus_files
   969|     # stayed under it (the related-test and Graphify branches already
   970|     # enforced this; these three didn't).
   971|     write_files(repo, {
   972|         "core/a.py": "from core.b import g\nfrom core.c import h\n\n\ndef f():\n    g()\n    return h()\n",
   973|         "core/b.py": "def g():\n    return 1\n",
   974|         "core/c.py": "from core.a import f\n\n\ndef h():\n    return f()\n",
   975|     })
   976|     _scan(repo, out)
   977|     req = _request(out, "req.json", {
   978|         "schema_version": "1.0", "question": "q",
   979|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   980|         "expansion": {"include_callers": True, "include_callees": True, "include_imports": True},
   981|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   982|     })
   983|     result = _packet(repo, out, req)
   984|     assert result.returncode == 0, result.stderr
   985|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   986|     assert sidecar["focus_files"] == ["core/a.py"]
   987|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   988|     assert "[origin: caller_expansion]" not in text
   989|     assert "[origin: callee_expansion]" not in text
   990|     assert "limits.max_files" in text
   991| 
   992| 
   993| def _git_init_commit(repo) -> str:
   994|     import subprocess
   995|     # graphify-out/graph.json is written *after* this commit (it needs the
   996|     # resulting commit hash for built_at_commit) -- gitignore it first so
   997|     # that later write leaves the worktree clean (git ignores it) rather
   998|     # than untracked/dirty, which the dirty-worktree Graphify check would
   999|     # otherwise (correctly) treat as unverifiable.
  1000|     (repo / ".gitignore").write_text("graphify-out/\n", encoding="utf-8")
  1001|     subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
  1002|     subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
  1003|     subprocess.run(["git", "-c", "user.email=t@t.com", "-c", "user.name=t", "commit", "-qm", "init"],
  1004|                    cwd=repo, check=True)
  1005|     return subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
  1006|                            ).stdout.strip()
  1007| 
  1008| 
```
