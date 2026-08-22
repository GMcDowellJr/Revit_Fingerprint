# Chunk of dev_tools/repo_context/tests/test_packet_from_request.py

- Source relative path: `dev_tools/repo_context/tests/test_packet_from_request.py`
- Chunk: 2 of 3
- Original line range: 500-1018
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_whole_file_symbol_listing_is_charged_against_budget, test_file_inventories_deferred_until_every_explicit_file_renders, test_explicit_file_excerpt_renders_before_optional_symbol_inventory, test_search_terms_share_a_single_global_max_files_cap, test_invalid_regex_notices_are_charged_against_budget, test_pathological_regex_search_term_times_out_instead_of_hanging, test_aggregate_regex_search_time_is_capped_across_all_terms, test_graphify_peer_listing_respects_max_files, test_graphify_withheld_on_dirty_worktree_even_with_matching_commit, test_graphify_not_withheld_for_dirtiness_confined_to_output_dir, test_callee_expansion_continues_past_a_rejected_file, test_search_term_matches_continue_past_a_rejected_file, test_overlong_question_is_rejected, test_caller_callee_import_expansion_respects_max_files, _git_init_commit, test_include_graphify_expansion_lists_revision_aligned_community_peers, test_include_graphify_withheld_when_current_commit_unavailable
- Source SHA-256: df517eeb144275c222d2e91539a9089bdf2618474d39d425f1db5b88a6702bfa
- Starts inside symbol: no
- Ends inside symbol: no

```
   500| def test_whole_file_symbol_listing_is_charged_against_budget(repo, out):
   501|     # Regression: the "Top-level symbols:" listing for an explicit whole-
   502|     # file selector used to be appended without any budget accounting, so
   503|     # a file with many top-level definitions could blow past
   504|     # limits.max_estimated_tokens while the packet reported far less
   505|     # usage than it actually rendered.
   506|     lines = []
   507|     for i in range(300):
   508|         lines += [f"def func_{i}():", f"    return {i}", ""]
   509|     write_files(repo, {"big.py": "\n".join(lines) + "\n"})
   510|     _scan(repo, out)
   511|     req = _request(out, "req.json", {
   512|         "schema_version": "1.0", "question": "q",
   513|         "selectors": {"files": ["big.py"], "symbols": [], "search_terms": [], "lines": []},
   514|         "limits": {"max_estimated_tokens": 20000, "max_files": 12},
   515|     })
   516|     result = _packet(repo, out, req)
   517|     assert result.returncode == 0, result.stderr
   518|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   519|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   520| 
   521|     # The reported estimated-token usage must not understate the packet's
   522|     # actual rendered size by a wide margin (the symbol-listing bug made
   523|     # this true even though every "func_i" line is metadata, not excerpt).
   524|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
   525|     assert text.count("func_") <= 300 * 2  # listing entries + (bounded) excerpt lines only, no runaway duplication
   526|     assert "Omitted" in text or text.count("(function, lines") <= 300
   527| 
   528| 
   529| def test_file_inventories_deferred_until_every_explicit_file_renders(repo, out):
   530|     # Regression: one explicit file selector's "Top-level symbols:"
   531|     # inventory was still rendered before the *next* explicit file
   532|     # selector got its guaranteed shot at the budget. With a 30-function
   533|     # `a.py` selected before a tiny `b.py`, a.py's inventory could consume
   534|     # enough of a tight-but-otherwise-sufficient budget that b.py's own
   535|     # excerpt no longer fit -- reversing the selector order changed the
   536|     # outcome under the same limit, which is exactly the ordering-
   537|     # dependence this fix removes (every file's excerpt must render before
   538|     # *any* file's inventory spends a char).
   539|     n = 60
   540|     lines = [f"def f_{i:03d}(): pass" for i in range(n)]
   541|     write_files(repo, {
   542|         "a.py": "\n".join(lines) + "\n",
   543|         "b.py": "def tiny():\n    return 1\n",
   544|     })
   545|     _scan(repo, out)
   546| 
   547|     no_expansion = {"include_callers": False, "include_callees": False, "include_imports": False,
   548|                     "include_related_tests": False, "include_graphify": False}
   549|     generous_req = _request(out, "generous.json", {
   550|         "schema_version": "1.0", "question": "q",
   551|         "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
   552|         "expansion": no_expansion,
   553|         "limits": {"max_estimated_tokens": 200000, "max_files": 12},
   554|     })
   555|     generous_result = _packet(repo, out, generous_req)
   556|     assert generous_result.returncode == 0, generous_result.stderr
   557|     full_text = (out / "packets" / "packet_generous.md").read_text(encoding="utf-8")
   558|     listing_lines = [l for l in full_text.splitlines() if "(function, lines" in l]
   559|     assert len(listing_lines) == n + 1  # a.py's n functions plus b.py's own single "tiny" entry
   560|     listing_chars = sum(len(l) for l in listing_lines) + len("Top-level symbols:")
   561|     full_tokens = json.loads(
   562|         (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
   563|     )["estimated_tokens_used"]
   564| 
   565|     # Cut roughly half of a.py's inventory worth of room from the fully-
   566|     # fitting total -- still comfortably enough for both files' headers +
   567|     # excerpts (which this bug never touched), but not enough for a.py's
   568|     # full inventory too.
   569|     constrained_tokens = full_tokens - (listing_chars // 2 // 4)
   570| 
   571|     req = _request(out, "req.json", {
   572|         "schema_version": "1.0", "question": "q",
   573|         "selectors": {"files": ["a.py", "b.py"], "symbols": [], "search_terms": [], "lines": []},
   574|         "expansion": no_expansion,
   575|         "limits": {"max_estimated_tokens": constrained_tokens, "max_files": 12},
   576|     })
   577|     result = _packet(repo, out, req)
   578|     assert result.returncode == 0, result.stderr
   579|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   580|     # b.py's mandatory excerpt (a *later* explicit selector) must render
   581|     # regardless of a.py's inventory being tight.
   582|     assert "def tiny():" in text
   583|     assert "def f_000(): pass" in text  # a.py's own excerpt still renders in full too
   584|     listing_lines_constrained = [l for l in text.splitlines() if "(function, lines" in l]
   585|     assert len(listing_lines_constrained) < n  # a.py's inventory got truncated instead
   586| 
   587| 
   588| def test_explicit_file_excerpt_renders_before_optional_symbol_inventory(repo, out):
   589|     # Regression: the file explicit-selector loop spent budget on the
   590|     # "Top-level symbols:" inventory listing *before* rendering the file's
   591|     # own mandatory excerpt. A tight-but-sufficient budget (enough for the
   592|     # header + full excerpt, but not also the full inventory) let the
   593|     # optional inventory crowd out the mandatory excerpt, forcing a hard
   594|     # explicit_conflicts abort even though the file's actual requested
   595|     # content would have fit on its own. The excerpt must always render
   596|     # first; only the inventory may be truncated/omitted.
   597|     n = 150
   598|     lines = [f"def f_{i:03d}(): pass" for i in range(n)]
   599|     write_files(repo, {"core/big.py": "\n".join(lines) + "\n"})
   600|     _scan(repo, out)
   601| 
   602|     no_expansion = {"include_callers": False, "include_callees": False, "include_imports": False,
   603|                     "include_related_tests": False, "include_graphify": False}
   604|     generous_req = _request(out, "generous.json", {
   605|         "schema_version": "1.0", "question": "q",
   606|         "selectors": {"files": ["core/big.py"], "symbols": [], "search_terms": [], "lines": []},
   607|         "expansion": no_expansion,
   608|         "limits": {"max_estimated_tokens": 200000, "max_files": 12},
   609|     })
   610|     generous_result = _packet(repo, out, generous_req)
   611|     assert generous_result.returncode == 0, generous_result.stderr
   612|     full_text = (out / "packets" / "packet_generous.md").read_text(encoding="utf-8")
   613|     listing_lines = [l for l in full_text.splitlines() if "(function, lines" in l]
   614|     assert len(listing_lines) == n  # nothing tight yet -- every symbol is listed
   615|     listing_chars = sum(len(l) for l in listing_lines) + len("Top-level symbols:")
   616|     full_tokens = json.loads(
   617|         (out / "packets" / "packet_generous.resolution.json").read_text(encoding="utf-8")
   618|     )["estimated_tokens_used"]
   619| 
   620|     # Cut roughly half the inventory listing's worth of room from the
   621|     # fully-fitting budget: still comfortably enough for the header + full
   622|     # excerpt (which the listing bug never touched), but not enough for
   623|     # the full inventory listing too.
   624|     constrained_tokens = full_tokens - (listing_chars // 2 // 4)
   625| 
   626|     req = _request(out, "req.json", {
   627|         "schema_version": "1.0", "question": "q",
   628|         "selectors": {"files": ["core/big.py"], "symbols": [], "search_terms": [], "lines": []},
   629|         "expansion": no_expansion,
   630|         "limits": {"max_estimated_tokens": constrained_tokens, "max_files": 12},
   631|     })
   632|     result = _packet(repo, out, req)
   633|     assert result.returncode == 0, result.stderr
   634|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   635|     # Mandatory excerpt: every function's source line, including the very
   636|     # last one, must still be present in full.
   637|     assert "def f_000(): pass" in text
   638|     assert "def f_149(): pass" in text
   639|     # Optional inventory: truncated instead, never the excerpt.
   640|     listing_lines_constrained = [l for l in text.splitlines() if "(function, lines" in l]
   641|     assert len(listing_lines_constrained) < n
   642|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   643|     assert any("top-level symbol" in o.lower() for o in sidecar["omissions"])
   644| 
   645| 
   646| def test_search_terms_share_a_single_global_max_files_cap(repo, out):
   647|     # Regression: max_files was previously enforced per search term (a
   648|     # fresh `shown_files` set each iteration), so two different terms
   649|     # matching two different files could each individually stay "within"
   650|     # limits.max_files while the combined focus-file set exceeded it.
   651|     write_files(repo, {
   652|         "a.py": "def f():\n    return 'alpha_needle'\n",
   653|         "b.py": "def g():\n    return 'beta_needle'\n",
   654|     })
   655|     _scan(repo, out)
   656|     req = _request(out, "req.json", {
   657|         "schema_version": "1.0", "question": "q",
   658|         "selectors": {"files": [], "symbols": [], "search_terms": ["alpha_needle", "beta_needle"], "lines": []},
   659|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   660|     })
   661|     result = _packet(repo, out, req)
   662|     assert result.returncode == 0, result.stderr
   663|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   664|     assert len(sidecar["focus_files"]) <= 1
   665|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   666|     assert "Omitted" in text or "omitted beyond limits.max_files" in text
   667| 
   668| 
   669| def test_invalid_regex_notices_are_charged_against_budget(repo, out):
   670|     # Regression: each invalid-regex search term appended a notice with no
   671|     # budget accounting, so a request with many invalid regex terms could
   672|     # produce a large packet while reporting ~0 estimated tokens used
   673|     # under a tiny limits.max_estimated_tokens. This also exercises a
   674|     # second-order version of the same bug: each skipped notice fell back
   675|     # to an *unbudgeted* budget.omissions entry, and the final "## Omitted
   676|     # / unresolved" section rendered that whole list without any size
   677|     # accounting either -- both layers had to be fixed for this to pass.
   678|     write_files(repo, {"a.py": "def f():\n    return 1\n"})
   679|     _scan(repo, out)
   680|     bad_terms = [f"(unclosed_{i}" for i in range(200)]
   681|     req = _request(out, "req.json", {
   682|         "schema_version": "1.0", "question": "q",
   683|         "selectors": {"files": [], "symbols": [], "search_terms": bad_terms, "lines": []},
   684|         "expansion": {"search_as_regex": True},
   685|         "limits": {"max_estimated_tokens": 1, "max_files": 500},
   686|     })
   687|     result = _packet(repo, out, req)
   688|     assert result.returncode == 0, result.stderr
   689|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   690|     assert len(text) < 3000
   691|     assert text.count("not a valid regex") < 200
   692| 
   693| 
   694| def test_pathological_regex_search_term_times_out_instead_of_hanging(repo, out, monkeypatch):
   695|     # Regression: search_as_regex ran a caller-supplied pattern through
   696|     # plain re.search with no bound on evaluation time. A syntactically
   697|     # valid but pathological pattern like `(a+)+$` against a long, nearly-
   698|     # matching line triggers catastrophic backtracking -- confirmed to
   699|     # still be running after 20+ seconds for just 35 characters on this
   700|     # engine -- which could hang the CLI indefinitely for an LLM-produced
   701|     # or malicious request. Each term's evaluation must be bounded.
   702|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TIMEOUT_SECONDS", 0.5)
   703|     write_files(repo, {"core/a.py": "x = '" + "a" * 35 + "!'\n"})
   704|     _scan(repo, out)
   705|     files_rows = rr._load_csv(out / "file_inventory.csv")
   706|     resolutions, matches_by_term, _ = rr.resolve_search_terms(repo, ["(a+)+$"], True, files_rows, 12)
   707|     assert resolutions[0].status == "invalid"
   708|     assert "exceeded" in resolutions[0].detail
   709|     assert matches_by_term["(a+)+$"] == []
   710| 
   711|     # A normal (non-pathological) regex must still work correctly under
   712|     # the same bounded path -- the timeout mechanism must not break
   713|     # ordinary regex search.
   714|     resolutions2, matches_by_term2, _ = rr.resolve_search_terms(repo, ["a{3}"], True, files_rows, 12)
   715|     assert resolutions2[0].status == "resolved"
   716|     assert matches_by_term2["a{3}"]
   717| 
   718| 
   719| def test_aggregate_regex_search_time_is_capped_across_all_terms(repo, out, monkeypatch):
   720|     # Regression: the per-term SIGALRM bound stops any *one* pathological
   721|     # pattern from hanging forever, but the schema places no cap on how
   722|     # many search_terms a request can carry -- a request with several
   723|     # distinct catastrophic-backtracking patterns could still burn a full
   724|     # per-term allowance for *each one* before packet budgeting even
   725|     # began. Three terms each requesting up to 1.0s, under a 1.5s
   726|     # aggregate cap, must finish in well under 3.0s total, with the terms
   727|     # beyond the aggregate deadline reported as skipped rather than each
   728|     # getting their own fresh timeout.
   729|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TIMEOUT_SECONDS", 1.0)
   730|     monkeypatch.setattr(rr, "_REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS", 1.5)
   731|     write_files(repo, {"core/a.py": "x = '" + "a" * 35 + "!'\n"})
   732|     _scan(repo, out)
   733|     files_rows = rr._load_csv(out / "file_inventory.csv")
   734|     start = time.monotonic()
   735|     # Three distinct-looking patterns, all pathological against the same
   736|     # run of `a` characters (a term repeated verbatim would risk being
   737|     # deduplicated by a caller upstream of this function; these aren't).
   738|     resolutions, matches_by_term, _ = rr.resolve_search_terms(
   739|         repo, ["(a+)+$", "(a+)*$", "(a|aa)+$"], True, files_rows, 12,
   740|     )
   741|     elapsed = time.monotonic() - start
   742|     assert elapsed < 2.5  # comfortably bounded, not the ~3.0s+ three full per-term timeouts would take
   743|     assert all(r.status == "invalid" for r in resolutions)
   744|     # At least the last term must be skipped outright by the aggregate
   745|     # deadline rather than getting its own fresh per-term timeout.
   746|     assert any("aggregate" in r.detail for r in resolutions)
   747| 
   748| 
   749| def test_graphify_peer_listing_respects_max_files(repo, out):
   750|     # Regression: Graphify community-peer paths were emitted without
   751|     # going through note_focus_file, so they could exceed limits.max_files
   752|     # while the resolution sidecar's focus_files list stayed under it.
   753|     write_files(repo, {
   754|         "core/a.py": "def f():\n    return 1\n",
   755|         "core/b.py": "def g():\n    return 2\n",
   756|     })
   757|     commit = _git_init_commit(repo)
   758|     graph = {
   759|         "built_at_commit": commit,
   760|         "nodes": [
   761|             {"source_file": "core/a.py", "community": 5, "community_name": "Widgets"},
   762|             {"source_file": "core/b.py", "community": 5, "community_name": "Widgets"},
   763|         ],
   764|     }
   765|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   766|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   767|     _scan(repo, out)
   768|     req = _request(out, "req.json", {
   769|         "schema_version": "1.0", "question": "q",
   770|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   771|         "expansion": {"include_graphify": True},
   772|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   773|     })
   774|     result = _packet(repo, out, req)
   775|     assert result.returncode == 0, result.stderr
   776|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   777|     assert sidecar["focus_files"] == ["core/a.py"]
   778|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   779|     # core/b.py must never appear as a rendered, [origin:
   780|     # graphify_expansion]-tagged evidence item -- it's beyond
   781|     # limits.max_files, reported only via the batched omission note.
   782|     assert "[origin: graphify_expansion]" not in text
   783|     assert "Graphify community peer(s)" in text
   784|     assert "limits.max_files" in text
   785| 
   786| 
   787| def test_graphify_withheld_on_dirty_worktree_even_with_matching_commit(repo, out):
   788|     # Regression: a matching built_at_commit was accepted even when the
   789|     # scanned worktree had uncommitted changes -- a matching commit hash
   790|     # alone doesn't prove graph.json's communities still describe what's
   791|     # actually on disk if a tracked file was modified since that commit.
   792|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   793|     commit = _git_init_commit(repo)
   794|     graph = {
   795|         "built_at_commit": commit,
   796|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
   797|     }
   798|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   799|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   800|     # Modify a tracked file after the commit, without committing again --
   801|     # this is what makes the worktree dirty even though HEAD still equals
   802|     # the commit graph.json names.
   803|     write_files(repo, {"core/a.py": "def f():\n    return 999\n"})
   804|     _scan(repo, out)
   805|     req = _request(out, "req.json", {
   806|         "schema_version": "1.0", "question": "q",
   807|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   808|         "expansion": {"include_graphify": True},
   809|     })
   810|     result = _packet(repo, out, req)
   811|     assert result.returncode == 0, result.stderr
   812|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   813|     assert "graphify_expansion" not in text
   814|     assert "worktree is dirty" in text
   815| 
   816| 
   817| def test_graphify_not_withheld_for_dirtiness_confined_to_output_dir(repo):
   818|     # Regression: get_git_info(root) reported "dirty" for *any* uncommitted
   819|     # change anywhere in the worktree, including this tool's own freshly
   820|     # written --output directory when it lives inside the scanned repo (as
   821|     # it does for this project's own repo_context/). scan/packet always
   822|     # write fresh output before this check runs, so every single run
   823|     # against such a repo made the worktree look dirty and withheld
   824|     # Graphify evidence for a reason that has nothing to do with the
   825|     # scanned *source* changing.
   826|     output_dir = repo / "repo_context"
   827|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   828|     commit = _git_init_commit(repo)
   829|     graph = {
   830|         "built_at_commit": commit,
   831|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
   832|     }
   833|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   834|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   835|     _scan(repo, output_dir)  # writes brand-new, untracked files *inside* repo -- this alone used to dirty git status
   836|     req = _request(output_dir, "req.json", {
   837|         "schema_version": "1.0", "question": "q",
   838|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   839|         "expansion": {"include_graphify": True},
   840|     })
   841|     result = _packet(repo, output_dir, req)
   842|     assert result.returncode == 0, result.stderr
   843|     text = (output_dir / "packets" / "packet_req.md").read_text(encoding="utf-8")
   844|     assert "worktree is dirty" not in text
   845| 
   846| 
   847| def test_callee_expansion_continues_past_a_rejected_file(repo, out):
   848|     # Regression: the callee-expansion loop `break`-ed the whole listing
   849|     # on the first callee whose file was beyond limits.max_files, even
   850|     # though a *later* callee might be in a file already in focus_files
   851|     # (free -- no new slot needed). `f`'s first callee `g` lives in a
   852|     # different, not-yet-focused file; its second callee `h` lives in the
   853|     # same file as `f` itself (already focused, since that's the selected
   854|     # symbol's own file). With max_files:1, `g` must be skipped but `h`
   855|     # must still render -- not silently dropped along with it.
   856|     write_files(repo, {
   857|         "core/a.py": "from core.other import g\n\n\ndef h():\n    return 1\n\n\ndef f():\n    g()\n    h()\n    return 1\n",
   858|         "core/other.py": "def g():\n    return 2\n",
   859|     })
   860|     _scan(repo, out)
   861|     req = _request(out, "req.json", {
   862|         "schema_version": "1.0", "question": "q",
   863|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   864|         "expansion": {"include_callers": False, "include_callees": True, "include_imports": False,
   865|                       "include_related_tests": False, "include_graphify": False},
   866|         "limits": {"max_files": 1},
   867|     })
   868|     result = _packet(repo, out, req)
   869|     assert result.returncode == 0, result.stderr
   870|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   871|     assert "-> `h`" in text  # the already-focused-file callee must still render
   872|     assert "core/other.py" not in text  # the beyond-max_files callee stays omitted
   873|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   874|     assert sidecar["focus_files"] == ["core/a.py"]
   875| 
   876| 
   877| def test_search_term_matches_continue_past_a_rejected_file(repo, out):
   878|     # Regression: the search-match loop `break`-ed on the first match
   879|     # whose file was beyond limits.max_files, even though a *later* match
   880|     # (for the same term) might be in a file already in focus_files. With
   881|     # `z/main.py` explicitly selected and `needle_term` matching both
   882|     # `a/other.py` (scanned first, alphabetically) and `z/main.py`
   883|     # (already focused), max_files:1 must still render the z/main.py
   884|     # match instead of losing both to the first rejection.
   885|     write_files(repo, {
   886|         "a/other.py": "# needle_term\n",
   887|         "z/main.py": "# needle_term\n",
   888|     })
   889|     _scan(repo, out)
   890|     req = _request(out, "req.json", {
   891|         "schema_version": "1.0", "question": "q",
   892|         "selectors": {"files": ["z/main.py"], "symbols": [], "search_terms": ["needle_term"], "lines": []},
   893|         "limits": {"max_files": 1},
   894|     })
   895|     result = _packet(repo, out, req)
   896|     assert result.returncode == 0, result.stderr
   897|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   898|     assert "z/main.py:1" in text
   899|     assert "a/other.py" not in text
   900|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   901|     assert sidecar["focus_files"] == ["z/main.py"]
   902| 
   903| 
   904| def test_overlong_question_is_rejected(repo, out):
   905|     # Regression: `question` is copied verbatim into every packet's
   906|     # header with no budget accounting and no schema length limit -- an
   907|     # oversized value could make a packet exceed limits.max_estimated_tokens
   908|     # through the header alone.
   909|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
   910|     _scan(repo, out)
   911|     req = _request(out, "req.json", {
   912|         "schema_version": "1.0", "question": "x" * 5000,
   913|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
   914|         "limits": {"max_estimated_tokens": 1, "max_files": 12},
   915|     })
   916|     result = _packet(repo, out, req)
   917|     assert result.returncode == 1
   918|     assert "too long" in result.stderr
   919|     assert not (out / "packets" / "packet_req.md").exists()
   920| 
   921| 
   922| def test_caller_callee_import_expansion_respects_max_files(repo, out):
   923|     # Regression: callers/callees/internal-imports listings emitted every
   924|     # referenced file without going through note_focus_file, so they could
   925|     # exceed limits.max_files while the resolution sidecar's focus_files
   926|     # stayed under it (the related-test and Graphify branches already
   927|     # enforced this; these three didn't).
   928|     write_files(repo, {
   929|         "core/a.py": "from core.b import g\nfrom core.c import h\n\n\ndef f():\n    g()\n    return h()\n",
   930|         "core/b.py": "def g():\n    return 1\n",
   931|         "core/c.py": "from core.a import f\n\n\ndef h():\n    return f()\n",
   932|     })
   933|     _scan(repo, out)
   934|     req = _request(out, "req.json", {
   935|         "schema_version": "1.0", "question": "q",
   936|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   937|         "expansion": {"include_callers": True, "include_callees": True, "include_imports": True},
   938|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
   939|     })
   940|     result = _packet(repo, out, req)
   941|     assert result.returncode == 0, result.stderr
   942|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
   943|     assert sidecar["focus_files"] == ["core/a.py"]
   944|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   945|     assert "[origin: caller_expansion]" not in text
   946|     assert "[origin: callee_expansion]" not in text
   947|     assert "limits.max_files" in text
   948| 
   949| 
   950| def _git_init_commit(repo) -> str:
   951|     import subprocess
   952|     # graphify-out/graph.json is written *after* this commit (it needs the
   953|     # resulting commit hash for built_at_commit) -- gitignore it first so
   954|     # that later write leaves the worktree clean (git ignores it) rather
   955|     # than untracked/dirty, which the dirty-worktree Graphify check would
   956|     # otherwise (correctly) treat as unverifiable.
   957|     (repo / ".gitignore").write_text("graphify-out/\n", encoding="utf-8")
   958|     subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
   959|     subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
   960|     subprocess.run(["git", "-c", "user.email=t@t.com", "-c", "user.name=t", "commit", "-qm", "init"],
   961|                    cwd=repo, check=True)
   962|     return subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
   963|                            ).stdout.strip()
   964| 
   965| 
   966| def test_include_graphify_expansion_lists_revision_aligned_community_peers(repo, out):
   967|     write_files(repo, {
   968|         "core/a.py": "def f():\n    return 1\n",
   969|         "core/b.py": "def g():\n    return 2\n",
   970|     })
   971|     commit = _git_init_commit(repo)
   972|     graph = {
   973|         "built_at_commit": commit,
   974|         "nodes": [
   975|             {"source_file": "core/a.py", "community": 5, "community_name": "Widgets"},
   976|             {"source_file": "core/b.py", "community": 5, "community_name": "Widgets"},
   977|         ],
   978|     }
   979|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
   980|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
   981|     _scan(repo, out)
   982|     req = _request(out, "req.json", {
   983|         "schema_version": "1.0", "question": "q",
   984|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
   985|         "expansion": {"include_graphify": True},
   986|     })
   987|     result = _packet(repo, out, req)
   988|     assert result.returncode == 0, result.stderr
   989|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
   990|     assert "graphify_expansion" in text
   991|     assert "core/b.py" in text
   992| 
   993| 
   994| def test_include_graphify_withheld_when_current_commit_unavailable(repo, out):
   995|     # Regression: when the scanned tree isn't a git repository (no HEAD
   996|     # commit to check against), a graphify-out/graph.json with any
   997|     # built_at_commit used to be accepted unconditionally instead of
   998|     # being withheld -- revision alignment can't be proven either way.
   999|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1000|     graph = {
  1001|         "built_at_commit": "deadbeef",
  1002|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
  1003|     }
  1004|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
  1005|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
  1006|     _scan(repo, out)  # no git init -- current commit is unavailable
  1007|     req = _request(out, "req.json", {
  1008|         "schema_version": "1.0", "question": "q",
  1009|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
  1010|         "expansion": {"include_graphify": True},
  1011|     })
  1012|     result = _packet(repo, out, req)
  1013|     assert result.returncode == 0, result.stderr
  1014|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1015|     assert "graphify_expansion" not in text
  1016|     assert "revision alignment cannot be proven" in text
  1017| 
  1018| 
```
