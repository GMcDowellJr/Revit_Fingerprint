# Chunk of dev_tools/repo_context/tests/test_packet_from_request.py

- Source relative path: `dev_tools/repo_context/tests/test_packet_from_request.py`
- Chunk: 2 of 3
- Original line range: 521-1031
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_resolution_sidecar_json_is_written, test_whole_file_symbol_listing_is_charged_against_budget, test_file_inventories_deferred_until_every_explicit_file_renders, test_explicit_file_excerpt_renders_before_optional_symbol_inventory, test_search_terms_share_a_single_global_max_files_cap, test_invalid_regex_notices_are_charged_against_budget, test_pathological_regex_search_term_times_out_instead_of_hanging, test_aggregate_regex_search_time_is_capped_across_all_terms, test_graphify_peer_listing_respects_max_files, test_graphify_withheld_on_dirty_worktree_even_with_matching_commit, test_graphify_not_withheld_for_dirtiness_confined_to_output_dir, test_callee_expansion_continues_past_a_rejected_file, test_search_term_matches_continue_past_a_rejected_file, test_overlong_question_is_rejected, test_caller_callee_import_expansion_respects_max_files, _git_init_commit, test_include_graphify_expansion_lists_revision_aligned_community_peers
- Source SHA-256: 759d9ce2ca0219228b05d56db69e1b045fd96066288de392f13f77203a94949c
- Starts inside symbol: no
- Ends inside symbol: no

```
   521| 
   522| 
   523| def test_resolution_sidecar_json_is_written(repo, out):
   524|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   525|     _scan(repo, out)
   526|     req = _request(out, "req.json", {
   527|         "schema_version": "1.0", "question": "q",
   528|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   529|     })
   530|     result = _packet(repo, out, req)
   531|     assert result.returncode == 0, result.stderr
   532|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   533|     assert sidecar["schema_version"] == "1.0"
   534|     assert sidecar["question"] == "q"
   535|     assert sidecar["resolution_report"][0]["status"] == "resolved"
   536| 
   537| 
   538| def test_whole_file_symbol_listing_is_charged_against_budget(repo, out):
   539|     # Regression: the "Top-level symbols:" listing for an explicit whole-
   540|     # file selector used to be appended without any budget accounting, so
   541|     # a file with many top-level definitions could blow past
   542|     # limits.max_estimated_tokens while the packet reported far less
   543|     # usage than it actually rendered.
   544|     lines = []
   545|     for i in range(300):
   546|         lines += [f"def func_{i}():", f"    return {i}", ""]
   547|     write_files(repo, {"big.py": "\n".join(lines) + "\n"})
   548|     _scan(repo, out)
   549|     req = _request(out, "req.json", {
   550|         "schema_version": "1.0", "question": "q",
   551|         "selectors": {"files": ["big.py"], "symbols": [], "search_terms": [], "lines": []},
   552|         "limits": {"max_estimated_tokens": 20000, "max_files": 12},
   553|     })
   554|     result = _packet(repo, out, req)
   555|     assert result.returncode == 0, result.stderr
   556|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   557|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   558| 
   559|     # The reported estimated-token usage must not understate the packet's
   560|     # actual rendered size by a wide margin (the symbol-listing bug made
   561|     # this true even though every "func_i" line is metadata, not excerpt).
   562|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   563|     assert text.count("func_") <= 300 * 2  # listing entries + (bounded) excerpt lines only, no runaway duplication
   564|     assert "Omitted" in text or text.count("(function, lines") <= 300
   565| 
   566| 
   567| def test_file_inventories_deferred_until_every_explicit_file_renders(repo, out):
   568|     # Regression: one explicit file selector's "Top-level symbols:"
   569|     # inventory was still rendered before the *next* explicit file
   570|     # selector got its guaranteed shot at the budget. With a 30-function
   571|     # `a.py` selected before a tiny `b.py`, a.py's inventory could consume
   572|     # enough of a tight-but-otherwise-sufficient budget that b.py's own
   573|     # excerpt no longer fit -- reversing the selector order changed the
   574|     # outcome under the same limit, which is exactly the ordering-
   575|     # dependence this fix removes (every file's excerpt must render before
   576|     # *any* file's inventory spends a char).
   577|     n = 60
   578|     lines = [f"def f_{i:03d}(): pass" for i in range(n)]
   579|     write_files(repo, {
   580|         "a.py": "\n".join(lines) + "\n",
   581|         "b.py": "def tiny():\n    return 1\n",
   582|     })
   583|     _scan(repo, out)
   584| 
   585|     no_expansion = {"include_callers": False, "include_callees": False, "include_imports": False,
   586|                     "include_related_tests": False, "include_graphify": False}
   587|     generous_req = _request(out, "generous.json", {
   588|         "schema_version": "1.0", "question": "q",
   589|         "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
   590|         "expansion": no_expansion,
   591|         "limits": {"max_estimated_tokens": 200000, "max_files": 12},
   592|     })
   593|     generous_result = _packet(repo, out, generous_req)
   594|     assert generous_result.returncode == 0, generous_result.stderr
   595|     full_text = (out / "packets" / "packet_generous.md").read_text(encoding="utf-8")
   596|     listing_lines = [l for l in full_text.splitlines() if "(function, lines" in l]
   597|     assert len(listing_lines) == n + 1  # a.py's n functions plus b.py's own single "tiny" entry
   598|     listing_chars = sum(len(l) for l in listing_lines) + len("Top-level symbols:")
   599|     full_tokens = json.loads(
   600|         (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
   601|     )["estimated_tokens_used"]
   602| 
   603|     # Cut roughly half of a.py's inventory worth of room from the fully-
   604|     # fitting total -- still comfortably enough for both files' headers +
   605|     # excerpts (which this bug never touched), but not enough for a.py's
   606|     # full inventory too.
   607|     constrained_tokens = full_tokens - (listing_chars // 2 // 4)
   608| 
   609|     req = _request(out, "req.json", {
   610|         "schema_version": "1.0", "question": "q",
   611|         "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
   612|         "expansion": no_expansion,
   613|         "limits": {"max_estimated_tokens": constrained_tokens, "max_files": 12},
   614|     })
   615|     result = _packet(repo, out, req)
   616|     assert result.returncode == 0, result.stderr
   617|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   618|     # b.py's mandatory excerpt (a *later* explicit selector) must render
   619|     # regardless of a.py's inventory being tight.
   620|     assert "def tiny():" in text
   621|     assert "def f_000(): pass" in text  # a.py's own excerpt still renders in full too
   622|     listing_lines_constrained = [l for l in text.splitlines() if "(function, lines" in l]
   623|     assert len(listing_lines_constrained) < n  # a.py's inventory got truncated instead
   624| 
   625| 
   626| def test_explicit_file_excerpt_renders_before_optional_symbol_inventory(repo, out):
   627|     # Regression: the file explicit-selector loop spent budget on the
   628|     # "Top-level symbols:" inventory listing *before* rendering the file's
   629|     # own mandatory excerpt. A tight-but-sufficient budget (enough for the
   630|     # header + full excerpt, but not also the full inventory) let the
   631|     # optional inventory crowd out the mandatory excerpt, forcing a hard
   632|     # explicit_conflicts abort even though the file's actual requested
   633|     # content would have fit on its own. The excerpt must always render
   634|     # first; only the inventory may be truncated/omitted.
   635|     n = 150
   636|     lines = [f"def f_{i:03d}(): pass" for i in range(n)]
   637|     write_files(repo, {"core/big.py": "\n".join(lines) + "\n"})
   638|     _scan(repo, out)
   639| 
   640|     no_expansion = {"include_callers": False, "include_callees": False, "include_imports": False,
   641|                     "include_related_tests": False, "include_graphify": False}
   642|     generous_req = _request(out, "generous.json", {
   643|         "schema_version": "1.0", "question": "q",
   644|         "selectors": {"files": ["core/big.py"], "symbols": [], "search_terms": [], "lines": []},
   645|         "expansion": no_expansion,
   646|         "limits": {"max_estimated_tokens": 200000, "max_files": 12},
   647|     })
   648|     generous_result = _packet(repo, out, generous_req)
   649|     assert generous_result.returncode == 0, generous_result.stderr
   650|     full_text = (out / "packets" / "packet_generous.md").read_text(encoding="utf-8")
   651|     listing_lines = [l for l in full_text.splitlines() if "(function, lines" in l]
   652|     assert len(listing_lines) == n  # nothing tight yet -- every symbol is listed
   653|     listing_chars = sum(len(l) for l in listing_lines) + len("Top-level symbols:")
   654|     full_tokens = json.loads(
   655|         (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
   656|     )["estimated_tokens_used"]
   657| 
   658|     # Cut roughly half the inventory listing's worth of room from the
   659|     # fully-fitting budget: still comfortably enough for the header + full
   660|     # excerpt (which the listing bug never touched), but not enough for
   661|     # the full inventory listing too.
   662|     constrained_tokens = full_tokens - (listing_chars // 2 // 4)
   663| 
   664|     req = _request(out, "req.json", {
   665|         "schema_version": "1.0", "question": "q",
   666|         "selectors": {"files": ["core/big.py"], "symbols": [], "search_terms": [], "lines": []},
   667|         "expansion": no_expansion,
   668|         "limits": {"max_estimated_tokens": constrained_tokens, "max_files": 12},
   669|     })
   670|     result = _packet(repo, out, req)
   671|     assert result.returncode == 0, result.stderr
   672|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   673|     # Mandatory excerpt: every function's source line, including the very
   674|     # last one, must still be present in full.
   675|     assert "def f_000(): pass" in text
   676|     assert "def f_149(): pass" in text
   677|     # Optional inventory: truncated instead, never the excerpt.
   678|     listing_lines_constrained = [l for l in text.splitlines() if "(function, lines" in l]
   679|     assert len(listing_lines_constrained) < n
   680|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   681|     assert any("top-level symbol" in o.lower() for o in sidecar["omissions"])
   682| 
   683| 
   684| def test_search_terms_share_a_single_global_max_files_cap(repo, out):
   685|     # Regression: max_files was previously enforced per search term (a
   686|     # fresh `shown_files` set each iteration), so two different terms
   687|     # matching two different files could each individually stay "within"
   688|     # limits.max_files while the combined focus-file set exceeded it.
   689|     write_files(repo, {
   690|         "a.py": "def f():\n    return 'alpha_needle'\n",
   691|         "b.py": "def g():\n    return 'beta_needle'\n",
   692|     })
   693|     _scan(repo, out)
   694|     req = _request(out, "req.json", {
   695|         "schema_version": "1.0", "question": "q",
   696|         "selectors": {"files": [], "symbols": [], "search_terms": ["alpha_needle", "beta_needle"], "lines": []},
   697|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   698|     })
   699|     result = _packet(repo, out, req)
   700|     assert result.returncode == 0, result.stderr
   701|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   702|     assert len(sidecar["focus_files"]) <= 1
   703|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   704|     assert "Omitted" in text or "omitted beyond limits.max_files" in text
   705| 
   706| 
   707| def test_invalid_regex_notices_are_charged_against_budget(repo, out):
   708|     # Regression: each invalid-regex search term appended a notice with no
   709|     # budget accounting, so a request with many invalid regex terms could
   710|     # produce a large packet while reporting ~0 estimated tokens used
   711|     # under a tiny limits.max_estimated_tokens. This also exercises a
   712|     # second-order version of the same bug: each skipped notice fell back
   713|     # to an *unbudgeted* budget.omissions entry, and the final "## Omitted
   714|     # / unresolved" section rendered that whole list without any size
   715|     # accounting either -- both layers had to be fixed for this to pass.
   716|     write_files(repo, {"a.py": "def f():\n    return 1\n"})
   717|     _scan(repo, out)
   718|     bad_terms = [f"(unclosed_{i}" for i in range(200)]
   719|     req = _request(out, "req.json", {
   720|         "schema_version": "1.0", "question": "q",
   721|         "selectors": {"files": [], "symbols": [], "search_terms": bad_terms, "lines": []},
   722|         "expansion": {"search_as_regex": True},
   723|         "limits": {"max_estimated_tokens": 1, "max_files": 500},
   724|     })
   725|     result = _packet(repo, out, req)
   726|     assert result.returncode == 0, result.stderr
   727|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   728|     assert len(text) < 3000
   729|     assert text.count("not a valid regex") < 200
   730| 
   731| 
   732| def test_pathological_regex_search_term_times_out_instead_of_hanging(repo, out, monkeypatch):
   733|     # Regression: search_as_regex ran a caller-supplied pattern through
   734|     # plain re.search with no bound on evaluation time. A syntactically
   735|     # valid but pathological pattern like `(a+)+$` against a long, nearly-
   736|     # matching line triggers catastrophic backtracking -- confirmed to
   737|     # still be running after 20+ seconds for just 35 characters on this
   738|     # engine -- which could hang the CLI indefinitely for an LLM-produced
   739|     # or malicious request. Each term's evaluation must be bounded.
   740|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TIMEOUT_SECONDS", 0.5)
   741|     write_files(repo, {"core/a.py": "x = '" + "a" * 35 + "!'\n"})
   742|     _scan(repo, out)
   743|     files_rows = rr._load_csv(out / "file_inventory.csv")
   744|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["(a+)+$"], True, files_rows, 12)
   745|     assert resolutions[0].status == "invalid"
   746|     assert "exceeded" in resolutions[0].detail
   747|     assert matches_by_term["(a+)+$"] == []
   748| 
   749|     # A normal (non-pathological) regex must still work correctly under
   750|     # the same bounded path -- the timeout mechanism must not break
   751|     # ordinary regex search.
   752|     resolutions2, matches_by_term2, _ = rr.resolve_search_terms(repo, ["a{3}"], True, files_rows, 12)
   753|     assert resolutions2[0].status == "resolved"
   754|     assert matches_by_term2["a{3}"]
   755| 
   756| 
   757| def test_aggregate_regex_search_time_is_capped_across_all_terms(repo, out, monkeypatch):
   758|     # Regression: the per-term SIGALRM bound stops any *one* pathological
   759|     # pattern from hanging forever, but the schema places no cap on how
   760|     # many search_terms a request can carry -- a request with several
   761|     # distinct catastrophic-backtracking patterns could still burn a full
   762|     # per-term allowance for *each one* before packet budgeting even
   763|     # began. Three terms each requesting up to 1.0s, under a 1.5s
   764|     # aggregate cap, must finish in well under 3.0s total, with the terms
   765|     # beyond the aggregate deadline reported as skipped rather than each
   766|     # getting their own fresh timeout.
   767|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TIMEOUT_SECONDS", 1.0)
   768|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS", 1.5)
   769|     write_files(repo, {"core/a.py": "x = '" + "a" * 35 + "!'\n"})
   770|     _scan(repo, out)
   771|     files_rows = rr._load_csv(out / "file_inventory.csv")
   772|     start = time.monotonic()
   773|     # Three distinct-looking patterns, all pathological against the same
   774|     # run of `a` characters (a term repeated verbatim would risk being
   775|     # deduplicated by a caller upstream of this function; these aren't).
   776|     resolutions, matches_by_term, _ = rr.resolve_search_terms(
   777|         repo, ["(a+)+$", "(a+)*$", "(a|aa)+$"], True, files_rows, 12,
   778|     )
   779|     elapsed = time.monotonic() - start
   780|     assert elapsed < 2.5  # comfortably bounded, not the ~3.0s+ three full per-term timeouts would take
   781|     assert all(r.status == "invalid" for r in resolutions)
   782|     # At least the last term must be skipped outright by the aggregate
   783|     # deadline rather than getting its own fresh per-term timeout.
   784|     assert any("aggregate" in r.detail for r in resolutions)
   785| 
   786| 
   787| def test_graphify_peer_listing_respects_max_files(repo, out):
   788|     # Regression: Graphify community-peer paths were emitted without
   789|     # going through note_focus_file, so they could exceed limits.max_files
   790|     # while the resolution sidecar's focus_files list stayed under it.
   791|     write_files(repo, {
   792|         "core/a.py": "def f():\n    return 1\n",
   793|         "core/b.py": "def g():\n    return 2\n",
   794|     })
   795|     commit = _git_init_commit(repo)
   796|     graph = {
   797|         "built_at_commit": commit,
   798|         "nodes": [
   799|             {"source_file": "core/a.py", "community": 5, "community_name": "Widgets"},
   800|             {"source_file": "core/b.py", "community": 5, "community_name": "Widgets"},
   801|         ],
   802|     }
   803|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   804|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   805|     _scan(repo, out)
   806|     req = _request(out, "req.json", {
   807|         "schema_version": "1.0", "question": "q",
   808|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   809|         "expansion": {"include_graphify": True},
   810|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   811|     })
   812|     result = _packet(repo, out, req)
   813|     assert result.returncode == 0, result.stderr
   814|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   815|     assert sidecar["focus_files"] == ["core/a.py"]
   816|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   817|     # core/b.py must never appear as a rendered, [origin:
   818|     # graphify_expansion]-tagged evidence item -- it's beyond
   819|     # limits.max_files, reported only via the batched omission note.
   820|     assert "[origin: graphify_expansion]" not in text
   821|     assert "Graphify community peer(s)" in text
   822|     assert "limits.max_files" in text
   823| 
   824| 
   825| def test_graphify_withheld_on_dirty_worktree_even_with_matching_commit(repo, out):
   826|     # Regression: a matching built_at_commit was accepted even when the
   827|     # scanned worktree had uncommitted changes -- a matching commit hash
   828|     # alone doesn't prove graph.json's communities still describe what's
   829|     # actually on disk if a tracked file was modified since that commit.
   830|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   831|     commit = _git_init_commit(repo)
   832|     graph = {
   833|         "built_at_commit": commit,
   834|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
   835|     }
   836|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   837|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   838|     # Modify a tracked file after the commit, without committing again --
   839|     # this is what makes the worktree dirty even though HEAD still equals
   840|     # the commit graph.json names.
   841|     write_files(repo, {"core/a.py": "def f():\n    return 999\n"})
   842|     _scan(repo, out)
   843|     req = _request(out, "req.json", {
   844|         "schema_version": "1.0", "question": "q",
   845|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   846|         "expansion": {"include_graphify": True},
   847|     })
   848|     result = _packet(repo, out, req)
   849|     assert result.returncode == 0, result.stderr
   850|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   851|     assert "graphify_expansion" not in text
   852|     assert "worktree is dirty" in text
   853| 
   854| 
   855| def test_graphify_not_withheld_for_dirtiness_confined_to_output_dir(repo):
   856|     # Regression: get_git_info(root) reported "dirty" for *any* uncommitted
   857|     # change anywhere in the worktree, including this tool's own freshly
   858|     # written --output directory when it lives inside the scanned repo (as
   859|     # it does for this project's own repo_context/). scan/packet always
   860|     # write fresh output before this check runs, so every single run
   861|     # against such a repo made the worktree look dirty and withheld
   862|     # Graphify evidence for a reason that has nothing to do with the
   863|     # scanned *source* changing.
   864|     output_dir = repo / "repo_context"
   865|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   866|     commit = _git_init_commit(repo)
   867|     graph = {
   868|         "built_at_commit": commit,
   869|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
   870|     }
   871|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   872|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   873|     _scan(repo, output_dir)  # writes brand-new, untracked files *inside* repo -- this alone used to dirty git status
   874|     req = _request(output_dir, "req.json", {
   875|         "schema_version": "1.0", "question": "q",
   876|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   877|         "expansion": {"include_graphify": True},
   878|     })
   879|     result = _packet(repo, output_dir, req)
   880|     assert result.returncode == 0, result.stderr
   881|     text = (output_dir / "packets" / "packet_req.md").read_text(encoding="utf-8")
   882|     assert "worktree is dirty" not in text
   883| 
   884| 
   885| def test_callee_expansion_continues_past_a_rejected_file(repo, out):
   886|     # Regression: the callee-expansion loop `break`-ed the whole listing
   887|     # on the first callee whose file was beyond limits.max_files, even
   888|     # though a *later* callee might be in a file already in focus_files
   889|     # (free -- no new slot needed). `f`'s first callee `g` lives in a
   890|     # different, not-yet-focused file; its second callee `h` lives in the
   891|     # same file as `f` itself (already focused, since that's the selected
   892|     # symbol's own file). With max_files:1, `g` must be skipped but `h`
   893|     # must still render -- not silently dropped along with it.
   894|     write_files(repo, {
   895|         "core/a.py": "from core.other import g\n\n\ndef h():\n    return 1\n\n\ndef f():\n    g()\n    h()\n    return 1\n",
   896|         "core/other.py": "def g():\n    return 2\n",
   897|     })
   898|     _scan(repo, out)
   899|     req = _request(out, "req.json", {
   900|         "schema_version": "1.0", "question": "q",
   901|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   902|         "expansion": {"include_callers": False, "include_callees": True, "include_imports": False,
   903|                       "include_related_tests": False, "include_graphify": False},
   904|         "limits": {"max_files": 1},
   905|     })
   906|     result = _packet(repo, out, req)
   907|     assert result.returncode == 0, result.stderr
   908|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   909|     assert "-> `h`" in text  # the already-focused-file callee must still render
   910|     assert "core/other.py" not in text  # the beyond-max_files callee stays omitted
   911|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   912|     assert sidecar["focus_files"] == ["core/a.py"]
   913| 
   914| 
   915| def test_search_term_matches_continue_past_a_rejected_file(repo, out):
   916|     # Regression: the search-match loop `break`-ed on the first match
   917|     # whose file was beyond limits.max_files, even though a *later* match
   918|     # (for the same term) might be in a file already in focus_files. With
   919|     # `z/main.py` explicitly selected and `needle_term` matching both
   920|     # `a/other.py` (scanned first, alphabetically) and `z/main.py`
   921|     # (already focused), max_files:1 must still render the z/main.py
   922|     # match instead of losing both to the first rejection.
   923|     write_files(repo, {
   924|         "a/other.py": "# needle_term\n",
   925|         "z/main.py": "# needle_term\n",
   926|     })
   927|     _scan(repo, out)
   928|     req = _request(out, "req.json", {
   929|         "schema_version": "1.0", "question": "q",
   930|         "selectors": {"files": ["z/main.py"], "symbols": [], "search_terms": ["needle_term"], "lines": []},
   931|         "limits": {"max_files": 1},
   932|     })
   933|     result = _packet(repo, out, req)
   934|     assert result.returncode == 0, result.stderr
   935|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   936|     assert "z/main.py:1" in text
   937|     assert "a/other.py" not in text
   938|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   939|     assert sidecar["focus_files"] == ["z/main.py"]
   940| 
   941| 
   942| def test_overlong_question_is_rejected(repo, out):
   943|     # Regression: `question` is copied verbatim into every packet's
   944|     # header with no budget accounting and no schema length limit -- an
   945|     # oversized value could make a packet exceed limits.max_estimated_tokens
   946|     # through the header alone.
   947|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   948|     _scan(repo, out)
   949|     req = _request(out, "req.json", {
   950|         "schema_version": "1.0", "question": "x" * 5000,
   951|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   952|         "limits": {"max_estimated_tokens": 1, "max_files": 12},
   953|     })
   954|     result = _packet(repo, out, req)
   955|     assert result.returncode == 1
   956|     assert "too long" in result.stderr
   957|     assert not (out / "packets" / "packet_req.md").exists()
   958| 
   959| 
   960| def test_caller_callee_import_expansion_respects_max_files(repo, out):
   961|     # Regression: callers/callees/internal-imports listings emitted every
   962|     # referenced file without going through note_focus_file, so they could
   963|     # exceed limits.max_files while the resolution sidecar's focus_files
   964|     # stayed under it (the related-test and Graphify branches already
   965|     # enforced this; these three didn't).
   966|     write_files(repo, {
   967|         "core/a.py": "from core.b import g\nfrom core.c import h\n\n\ndef f():\n    g()\n    return h()\n",
   968|         "core/b.py": "def g():\n    return 1\n",
   969|         "core/c.py": "from core.a import f\n\n\ndef h():\n    return f()\n",
   970|     })
   971|     _scan(repo, out)
   972|     req = _request(out, "req.json", {
   973|         "schema_version": "1.0", "question": "q",
   974|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   975|         "expansion": {"include_callers": True, "include_callees": True, "include_imports": True},
   976|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   977|     })
   978|     result = _packet(repo, out, req)
   979|     assert result.returncode == 0, result.stderr
   980|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   981|     assert sidecar["focus_files"] == ["core/a.py"]
   982|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   983|     assert "[origin: caller_expansion]" not in text
   984|     assert "[origin: callee_expansion]" not in text
   985|     assert "limits.max_files" in text
   986| 
   987| 
   988| def _git_init_commit(repo) -> str:
   989|     import subprocess
   990|     # graphify-out/graph.json is written *after* this commit (it needs the
   991|     # resulting commit hash for built_at_commit) -- gitignore it first so
   992|     # that later write leaves the worktree clean (git ignores it) rather
   993|     # than untracked/dirty, which the dirty-worktree Graphify check would
   994|     # otherwise (correctly) treat as unverifiable.
   995|     (repo / ".gitignore").write_text("graphify-out/\n", encoding="utf-8")
   996|     subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
   997|     subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
   998|     subprocess.run(["git", "-c", "user.email=t@t.com", "-c", "user.name=t", "commit", "-qm", "init"],
   999|                    cwd=repo, check=True)
  1000|     return subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
  1001|                            ).stdout.strip()
  1002| 
  1003| 
  1004| def test_include_graphify_expansion_lists_revision_aligned_community_peers(repo, out):
  1005|     write_files(repo, {
  1006|         "core/a.py": "def f():\n    return 1\n",
  1007|         "core/b.py": "def g():\n    return 2\n",
  1008|     })
  1009|     commit = _git_init_commit(repo)
  1010|     graph = {
  1011|         "built_at_commit": commit,
  1012|         "nodes": [
  1013|             {"source_file": "core/a.py", "community": 5, "community_name": "Widgets"},
  1014|             {"source_file": "core/b.py", "community": 5, "community_name": "Widgets"},
  1015|         ],
  1016|     }
  1017|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
  1018|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
  1019|     _scan(repo, out)
  1020|     req = _request(out, "req.json", {
  1021|         "schema_version": "1.0", "question": "q",
  1022|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
  1023|         "expansion": {"include_graphify": True},
  1024|     })
  1025|     result = _packet(repo, out, req)
  1026|     assert result.returncode == 0, result.stderr
  1027|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1028|     assert "graphify_expansion" in text
  1029|     assert "core/b.py" in text
  1030| 
  1031| 
```
