# Chunk of dev_tools/repo_context/tests/test_packet_from_request.py

- Source relative path: `dev_tools/repo_context/tests/test_packet_from_request.py`
- Chunk: 3 of 3
- Original line range: 994-1408
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_caller_callee_import_expansion_respects_max_files, _git_init_commit, test_include_graphify_expansion_lists_revision_aligned_community_peers, test_include_graphify_withheld_when_current_commit_unavailable, test_selector_resolution_report_is_charged_against_budget, test_file_level_expansions_render_once_per_file_not_per_symbol, test_related_test_expansion_respects_global_max_files, test_name_override_controls_output_filename, test_file_level_expansion_runs_for_a_symbol_free_selected_file, test_fixed_framing_is_reserved_before_tier1_content_spends_budget, test_fixed_framing_is_reserved_before_tier1_content_spends_budget._gen, test_search_term_match_cannot_exceed_budget_via_late_footer_charge, _spy_iter_safe_lines, _spy_iter_safe_lines.spy, _spy_iter_safe_lines.spy._wrapped, test_explicit_excerpt_streams_and_stops_instead_of_materializing_whole_range, test_search_term_scan_streams_and_stops_once_collect_cap_is_reached
- Source SHA-256: 832629a18a31295543da3b69cd0c0e509e3cd7f8abc9a473690264a8ccc3a31c
- Starts inside symbol: no
- Ends inside symbol: no

```
   994| def test_caller_callee_import_expansion_respects_max_files(repo, out):
   995|     # Regression: callers/callees/internal-imports listings emitted every
   996|     # referenced file without going through note_focus_file, so they could
   997|     # exceed limits.max_files while the resolution sidecar's focus_files
   998|     # stayed under it (the related-test and Graphify branches already
   999|     # enforced this; these three didn't).
  1000|     write_files(repo, {
  1001|         "core/a.py": "from core.b import g\nfrom core.c import h\n\n\ndef f():\n    g()\n    return h()\n",
  1002|         "core/b.py": "def g():\n    return 1\n",
  1003|         "core/c.py": "from core.a import f\n\n\ndef h():\n    return f()\n",
  1004|     })
  1005|     _scan(repo, out)
  1006|     req = _request(out, "req.json", {
  1007|         "schema_version": "1.0", "question": "q",
  1008|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
  1009|         "expansion": {"include_callers": True, "include_callees": True, "include_imports": True},
  1010|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
  1011|     })
  1012|     result = _packet(repo, out, req)
  1013|     assert result.returncode == 0, result.stderr
  1014|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1015|     assert sidecar["focus_files"] == ["core/a.py"]
  1016|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1017|     assert "[origin: caller_expansion]" not in text
  1018|     assert "[origin: callee_expansion]" not in text
  1019|     assert "limits.max_files" in text
  1020| 
  1021| 
  1022| def _git_init_commit(repo) -> str:
  1023|     import subprocess
  1024|     # graphify-out/graph.json is written *after* this commit (it needs the
  1025|     # resulting commit hash for built_at_commit) -- gitignore it first so
  1026|     # that later write leaves the worktree clean (git ignores it) rather
  1027|     # than untracked/dirty, which the dirty-worktree Graphify check would
  1028|     # otherwise (correctly) treat as unverifiable.
  1029|     (repo / ".gitignore").write_text("graphify-out/\n", encoding="utf-8")
  1030|     subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
  1031|     subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
  1032|     subprocess.run(["git", "-c", "user.email=t@t.com", "-c", "user.name=t", "commit", "-qm", "init"],
  1033|                    cwd=repo, check=True)
  1034|     return subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
  1035|                            ).stdout.strip()
  1036| 
  1037| 
  1038| def test_include_graphify_expansion_lists_revision_aligned_community_peers(repo, out):
  1039|     write_files(repo, {
  1040|         "core/a.py": "def f():\n    return 1\n",
  1041|         "core/b.py": "def g():\n    return 2\n",
  1042|     })
  1043|     commit = _git_init_commit(repo)
  1044|     graph = {
  1045|         "built_at_commit": commit,
  1046|         "nodes": [
  1047|             {"source_file": "core/a.py", "community": 5, "community_name": "Widgets"},
  1048|             {"source_file": "core/b.py", "community": 5, "community_name": "Widgets"},
  1049|         ],
  1050|     }
  1051|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
  1052|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
  1053|     _scan(repo, out)
  1054|     req = _request(out, "req.json", {
  1055|         "schema_version": "1.0", "question": "q",
  1056|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
  1057|         "expansion": {"include_graphify": True},
  1058|     })
  1059|     result = _packet(repo, out, req)
  1060|     assert result.returncode == 0, result.stderr
  1061|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1062|     assert "graphify_expansion" in text
  1063|     assert "core/b.py" in text
  1064| 
  1065| 
  1066| def test_include_graphify_withheld_when_current_commit_unavailable(repo, out):
  1067|     # Regression: when the scanned tree isn't a git repository (no HEAD
  1068|     # commit to check against), a graphify-out/graph.json with any
  1069|     # built_at_commit used to be accepted unconditionally instead of
  1070|     # being withheld -- revision alignment can't be proven either way.
  1071|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1072|     graph = {
  1073|         "built_at_commit": "deadbeef",
  1074|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
  1075|     }
  1076|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
  1077|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
  1078|     _scan(repo, out)  # no git init -- current commit is unavailable
  1079|     req = _request(out, "req.json", {
  1080|         "schema_version": "1.0", "question": "q",
  1081|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
  1082|         "expansion": {"include_graphify": True},
  1083|     })
  1084|     result = _packet(repo, out, req)
  1085|     assert result.returncode == 0, result.stderr
  1086|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1087|     assert "graphify_expansion" not in text
  1088|     assert "revision alignment cannot be proven" in text
  1089| 
  1090| 
  1091| def test_selector_resolution_report_is_charged_against_budget(repo, out):
  1092|     # Regression: the "Selector resolution report" section (one entry per
  1093|     # requested selector, however many) was appended without any budget
  1094|     # accounting, so a request naming hundreds of missing/ambiguous
  1095|     # selectors could produce a large packet while reporting ~0 estimated
  1096|     # tokens used under a tiny limits.max_estimated_tokens.
  1097|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1098|     _scan(repo, out)
  1099|     missing_files = [f"missing_{i}.py" for i in range(200)]
  1100|     # 300 tokens clears the fixed framing's own floor (header + footer,
  1101|     # reserved up front -- see rc_request.py's "Reserve the fixed framing's
  1102|     # budget cost up front") but is nowhere near enough to fit all 200
  1103|     # resolution-report entries.
  1104|     req = _request(out, "req.json", {
  1105|         "schema_version": "1.0", "question": "q",
  1106|         "selectors": {"files": missing_files, "symbols": [], "search_terms": [], "lines": []},
  1107|         "limits": {"max_estimated_tokens": 300, "max_files": 500},
  1108|     })
  1109|     result = _packet(repo, out, req)
  1110|     assert result.returncode == 0, result.stderr
  1111|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1112|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1113|     # Certainly not a full 200-entry resolution report rendered in the
  1114|     # packet body -- it must be truncated with a count-of-omitted note,
  1115|     # not a wild understatement of the packet's actual size the way ~0
  1116|     # tokens for an 18KB packet was before this fix.
  1117|     assert len(text) < 2000
  1118|     assert text.count("missing_") < 200
  1119|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
  1120| 
  1121| 
  1122| def test_file_level_expansions_render_once_per_file_not_per_symbol(repo, out):
  1123|     # Regression: _symbol_expansion() rendered imports/related-tests/
  1124|     # Graphify-peer sections (all keyed by the containing *file*, not the
  1125|     # symbol) on every call -- but an explicit file selector called it
  1126|     # once per top-level symbol in that file, so a multi-function file
  1127|     # got its "Internal imports of X" / "Related tests for X" sections
  1128|     # duplicated once per function instead of appearing once.
  1129|     write_files(repo, {
  1130|         "core/a.py": (
  1131|             "from core.dep import helper\n\n\n"
  1132|             "def f():\n    return helper()\n\n\n"
  1133|             "def g():\n    return helper()\n\n\n"
  1134|             "def h():\n    return helper()\n"
  1135|         ),
  1136|         "core/dep.py": "def helper():\n    return 1\n",
  1137|         "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 1\n",
  1138|     })
  1139|     _scan(repo, out)
  1140|     req = _request(out, "req.json", {
  1141|         "schema_version": "1.0", "question": "q",
  1142|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1143|         "expansion": {"include_callers": False, "include_callees": False, "include_imports": True,
  1144|                       "include_related_tests": True, "include_graphify": False},
  1145|         "limits": {"max_estimated_tokens": 12000, "max_files": 12},
  1146|     })
  1147|     result = _packet(repo, out, req)
  1148|     assert result.returncode == 0, result.stderr
  1149|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1150|     # Three top-level functions in core/a.py, but these file-level
  1151|     # sections must appear exactly once, not three times.
  1152|     assert text.count("Internal imports of `core/a.py`") == 1
  1153|     assert text.count("Related tests for `core/a.py`") == 1
  1154| 
  1155| 
  1156| def test_related_test_expansion_respects_global_max_files(repo, out):
  1157|     # Regression: related-test expansion appended directly to focus_files
  1158|     # under a hard-coded 10,000 ceiling instead of going through the same
  1159|     # note_focus_file() gate as every other tier, so it could silently
  1160|     # exceed limits.max_files.
  1161|     write_files(repo, {
  1162|         "core/a.py": "def f():\n    return 1\n",
  1163|         "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 1\n",
  1164|     })
  1165|     _scan(repo, out)
  1166|     req = _request(out, "req.json", {
  1167|         "schema_version": "1.0", "question": "q",
  1168|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1169|         "expansion": {"include_related_tests": True},
  1170|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
  1171|     })
  1172|     result = _packet(repo, out, req)
  1173|     assert result.returncode == 0, result.stderr
  1174|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1175|     assert sidecar["focus_files"] == ["core/a.py"]
  1176|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1177|     assert "beyond limits.max_files" in text
  1178| 
  1179| 
  1180| def test_name_override_controls_output_filename(repo, out):
  1181|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1182|     _scan(repo, out)
  1183|     req = _request(out, "req.json", {
  1184|         "schema_version": "1.0", "question": "q",
  1185|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1186|     })
  1187|     result = _packet(repo, out, req, extra=["--name", "custom_stem"])
  1188|     assert result.returncode == 0, result.stderr
  1189|     assert (out / "packets" / "packet_custom_stem.md").exists()
  1190| 
  1191| 
  1192| def test_file_level_expansion_runs_for_a_symbol_free_selected_file(repo, out):
  1193|     # Regression: the loop driving file-level expansion (imports/related-
  1194|     # tests/Graphify peers) skipped straight past `_maybe_file_expansion()`
  1195|     # whenever an explicitly selected file had no top-level symbols (e.g.
  1196|     # an __init__.py re-export shim, or a plain module that only does
  1197|     # imports at module scope). Since imports/related-tests describe the
  1198|     # *file*, not any symbol in it, a symbol-free explicitly selected file
  1199|     # previously got zero import/related-test expansion evidence even
  1200|     # when include_imports/include_related_tests were explicitly on.
  1201|     write_files(repo, {
  1202|         "core/__init__.py": "from core.dep import helper\n",
  1203|         "core/dep.py": "def helper():\n    return 1\n",
  1204|     })
  1205|     _scan(repo, out)
  1206|     req = _request(out, "req.json", {
  1207|         "schema_version": "1.0", "question": "q",
  1208|         "selectors": {"files": ["core/__init__.py"], "symbols": [], "search_terms": [], "lines": []},
  1209|         "expansion": {"include_callers": False, "include_callees": False, "include_imports": True,
  1210|                       "include_related_tests": False},
  1211|         "limits": {"max_estimated_tokens": 12000, "max_files": 12},
  1212|     })
  1213|     result = _packet(repo, out, req)
  1214|     assert result.returncode == 0, result.stderr
  1215|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1216|     assert "Internal imports of `core/__init__.py`" in text
  1217|     assert "core/dep.py" in text
  1218| 
  1219| 
  1220| def test_fixed_framing_is_reserved_before_tier1_content_spends_budget(repo, out):
  1221|     # Regression: the header/selector-resolution-report/footer were
  1222|     # charged against the budget only *after* Tier-1 explicit-selector
  1223|     # content had already been allowed to spend against the full,
  1224|     # unreserved budget. That let a request's explicit content "fit" a
  1225|     # budget that, once framing's real cost landed on top afterward, the
  1226|     # packet's actual rendered size exceeded -- generation still reported
  1227|     # success despite the true size being over limits.max_estimated_tokens.
  1228|     # Framing must be reserved (and charged) first, so a too-tight budget
  1229|     # now correctly surfaces as an explicit_conflicts abort instead of an
  1230|     # over-budget "successful" packet.
  1231|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1232|     _scan(repo, out)
  1233| 
  1234|     # A single fixed request/output name reused for *every* call below --
  1235|     # the packet header renders the request filename verbatim
  1236|     # (`- Request file: \`{name}.json\` ...`), so varying the name's
  1237|     # length between calls (e.g. "probe166" vs. "boundary" vs. "under")
  1238|     # shifts the header's own size and, right at a ~1-token-wide margin,
  1239|     # can flip whether a given max_estimated_tokens value fits -- making
  1240|     # the boundary this test measures depend on which name happened to be
  1241|     # used for which call, not just on max_estimated_tokens. Reusing one
  1242|     # name removes that variable; each call's result depends only on
  1243|     # max_tokens.
  1244|     name = "req"
  1245| 
  1246|     def _gen(max_tokens):
  1247|         req = _request(out, f"{name}.json", {
  1248|             "schema_version": "1.0", "question": "q",
  1249|             "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1250|             "expansion": {"include_callers": False, "include_callees": False, "include_imports": False,
  1251|                           "include_related_tests": False},
  1252|             "limits": {"max_estimated_tokens": max_tokens, "max_files": 12},
  1253|         })
  1254|         return rr.generate_packet_from_request(repo, out, req, name_override=name)
  1255| 
  1256|     # Sanity-bound the binary search: a generous budget must succeed.
  1257|     packet_path, _, err = _gen(2000)
  1258|     assert packet_path is not None, err
  1259| 
  1260|     lo, hi = 1, 2000
  1261|     while lo < hi:
  1262|         mid = (lo + hi) // 2
  1263|         packet_path, _, _ = _gen(mid)
  1264|         if packet_path is not None:
  1265|             hi = mid
  1266|         else:
  1267|             lo = mid + 1
  1268|     min_success_tokens = hi
  1269| 
  1270|     # At the smallest budget that still succeeds, the packet's own
  1271|     # reported size must not exceed what was actually requested -- this
  1272|     # is exactly the invariant framing-charged-too-late violated (it
  1273|     # would report success here with estimated_tokens_used well above
  1274|     # min_success_tokens, since Tier-1 content had already spent as if
  1275|     # framing were free).
  1276|     packet_path, _, err = _gen(min_success_tokens)
  1277|     assert packet_path is not None, err
  1278|     sidecar = json.loads((out / "packets" / f"packet_{name}.resolution.json").read_text(encoding="utf-8"))
  1279|     assert sidecar["estimated_tokens_used"] <= min_success_tokens
  1280| 
  1281|     # One token below that boundary must hard-abort -- not silently
  1282|     # succeed with a packet whose true size exceeds the requested cap.
  1283|     packet_path, _, err = _gen(min_success_tokens - 1)
  1284|     assert packet_path is None
  1285|     assert "do not fit" in (err or "")
  1286| 
  1287| 
  1288| def test_search_term_match_cannot_exceed_budget_via_late_footer_charge(repo, out):
  1289|     # Regression (fresh Codex evidence after the first framing-reservation
  1290|     # fix): reserving the header up front, then charging the selector-
  1291|     # resolution report entries via budget.allow(), and only *then*
  1292|     # unconditionally spending the footer, still let a resolution-report
  1293|     # entry get allowed against a budget that hadn't yet accounted for the
  1294|     # footer's own cost -- the footer's later unconditional spend then
  1295|     # pushed the packet's true size back over the cap anyway. Concretely:
  1296|     # a 100-token request whose only content is one ~500-character search
  1297|     # match still reported estimated_tokens_used around 136, over the
  1298|     # 100-token cap. Header and footer must be reserved *together*, as one
  1299|     # atomic unit, before the resolution report (or anything else) spends.
  1300|     write_files(repo, {"a.py": f"needle = {'x' * 500!r}\n"})
  1301|     _scan(repo, out)
  1302|     req = _request(out, "req.json", {
  1303|         "schema_version": "1.0", "question": "q",
  1304|         "selectors": {"files": [], "symbols": [], "search_terms": ["needle"], "lines": []},
  1305|         "limits": {"max_estimated_tokens": 100, "max_files": 12},
  1306|     })
  1307|     packet_path, _, err = rr.generate_packet_from_request(repo, out, req, name_override="req")
  1308|     # Either this cleanly hard-aborts (the fixed framing alone doesn't fit
  1309|     # this tiny budget) or, if it succeeds, its real size must not exceed
  1310|     # what was requested -- what it must never do is "succeed" while its
  1311|     # true size exceeds the cap.
  1312|     if packet_path is not None:
  1313|         sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1314|         assert sidecar["estimated_tokens_used"] <= 100
  1315|     else:
  1316|         assert "too small to fit" in err
  1317| 
  1318| 
  1319| def _spy_iter_safe_lines(monkeypatch, module):
  1320|     """Wraps module._iter_safe_lines so every line it actually yields is
  1321|     counted, without changing its behavior (including .close()
  1322|     forwarding). Returns the shared counter dict; read counter["count"]
  1323|     after the call under test."""
  1324|     real = module._iter_safe_lines
  1325|     counter = {"count": 0}
  1326| 
  1327|     def spy(root, rel_path, start, end):
  1328|         gen = real(root, rel_path, start, end)
  1329|         if gen is None:
  1330|             return None
  1331| 
  1332|         def _wrapped():
  1333|             try:
  1334|                 for item in gen:
  1335|                     counter["count"] += 1
  1336|                     yield item
  1337|             finally:
  1338|                 gen.close()
  1339| 
  1340|         return _wrapped()
  1341| 
  1342|     monkeypatch.setattr(module, "_iter_safe_lines", spy)
  1343|     return counter
  1344| 
  1345| 
  1346| def test_explicit_excerpt_streams_and_stops_instead_of_materializing_whole_range(repo, out, monkeypatch):
  1347|     # Regression: an explicit file selector's excerpt was fully
  1348|     # materialized via _safe_excerpt(root, rel, 1, line_count) *before*
  1349|     # any budget check -- for a file the scanner deliberately keeps in
  1350|     # the inventory without reading whole (files over MAX_TEXT_READ_BYTES
  1351|     # still get a real, streamed-counted line_count -- see rc_scan.py),
  1352|     # this could allocate the file's entire content in memory just to
  1353|     # then learn the excerpt doesn't fit. The excerpt must instead be
  1354|     # streamed and stop reading as soon as it's clear the remaining
  1355|     # budget is exceeded.
  1356|     big_lines = [f"line number {i:06d} of a much larger file body" for i in range(5000)]
  1357|     write_files(repo, {"big.py": "\n".join(big_lines) + "\n"})
  1358|     _scan(repo, out)
  1359|     counter = _spy_iter_safe_lines(monkeypatch, rr)
  1360| 
  1361|     req = _request(out, "req.json", {
  1362|         "schema_version": "1.0", "question": "q",
  1363|         "selectors": {"files": ["big.py"], "symbols": [], "search_terms": [], "lines": []},
  1364|         "limits": {"max_estimated_tokens": 200, "max_files": 12},
  1365|     })
  1366|     packet_path, _, err = rr.generate_packet_from_request(repo, out, req, name_override="req")
  1367|     # The explicit selector doesn't fit this tiny budget -- correctly a
  1368|     # hard conflict, per the existing explicit-selector contract -- but
  1369|     # what matters here is that reaching that conclusion did not require
  1370|     # reading anywhere near all 5000 lines of the file first.
  1371|     assert packet_path is None
  1372|     assert counter["count"] < 500, f"read {counter['count']} lines before bailing out -- not streaming"
  1373| 
  1374| 
  1375| def test_search_term_scan_streams_and_stops_once_collect_cap_is_reached(repo, out, monkeypatch):
  1376|     # Regression: _scan_term_matches() called _safe_excerpt(root, rel, 1,
  1377|     # 10_000_000) per file, materializing up to ten million lines before
  1378|     # any matching happened -- a large included file (the scanner
  1379|     # deliberately keeps files over MAX_TEXT_READ_BYTES in the inventory
  1380|     # without reading them whole -- see rc_scan.py) could exhaust memory
  1381|     # searching for a term that matches many times, or not at all. A
  1382|     # literal search must still scan every line to find every match up to
  1383|     # collect_cap, but it must do so line-by-line as the file streams in
  1384|     # -- not by pre-loading the whole range into memory first -- and it
  1385|     # must stop consuming the file entirely once collect_cap matches have
  1386|     # been found, not keep reading to the end regardless.
  1387|     #
  1388|     # max_files=1 makes collect_cap (max(1, max_files) * 5) a small,
  1389|     # known value (5): put 10 matches up front, comfortably more than
  1390|     # collect_cap, followed by thousands of filler lines the scan must
  1391|     # never need to reach.
  1392|     big_lines = ["needle_marker"] * 10 + [f"filler line {i:06d}" for i in range(5000)]
  1393|     write_files(repo, {"big.py": "\n".join(big_lines) + "\n"})
  1394|     _scan(repo, out)
  1395|     counter = _spy_iter_safe_lines(monkeypatch, rr)
  1396| 
  1397|     req = _request(out, "req.json", {
  1398|         "schema_version": "1.0", "question": "q",
  1399|         "selectors": {"files": [], "symbols": [], "search_terms": ["needle_marker"], "lines": []},
  1400|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
  1401|     })
  1402|     packet_path, _, err = rr.generate_packet_from_request(repo, out, req, name_override="req")
  1403|     assert packet_path is not None, err
  1404|     text = packet_path.read_text(encoding="utf-8")
  1405|     assert "needle_marker" in text
  1406|     # collect_cap is 5 here -- the scan must stop shortly after finding
  1407|     # the 5th match, not read all ~5010 lines of the file.
  1408|     assert counter["count"] < 500, f"read {counter['count']} lines after collect_cap was reached -- not streaming"
```
