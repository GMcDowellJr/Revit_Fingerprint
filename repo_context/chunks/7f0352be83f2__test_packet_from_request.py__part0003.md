# Chunk of dev_tools/repo_context/tests/test_packet_from_request.py

- Source relative path: `dev_tools/repo_context/tests/test_packet_from_request.py`
- Chunk: 3 of 3
- Original line range: 995-1457
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_caller_callee_import_expansion_respects_max_files, _git_init_commit, test_include_graphify_expansion_lists_revision_aligned_community_peers, test_include_graphify_withheld_when_current_commit_unavailable, test_selector_resolution_report_is_charged_against_budget, test_file_level_expansions_render_once_per_file_not_per_symbol, test_related_test_expansion_respects_global_max_files, test_name_override_controls_output_filename, test_file_level_expansion_runs_for_a_symbol_free_selected_file, test_fixed_framing_is_reserved_before_tier1_content_spends_budget, test_fixed_framing_is_reserved_before_tier1_content_spends_budget._gen, test_search_term_match_cannot_exceed_budget_via_late_footer_charge, _spy_iter_safe_lines, _spy_iter_safe_lines.spy, _spy_iter_safe_lines.spy._wrapped, test_explicit_excerpt_streams_and_stops_instead_of_materializing_whole_range, test_search_term_scan_streams_and_stops_once_collect_cap_is_reached, test_every_rendered_fragment_is_charged_against_the_budget, test_every_rendered_fragment_is_charged_against_the_budget._gen
- Source SHA-256: c374abd0f274680032eecfb5f8298535b3b81413a7f4f6737d033f9738d5a6a9
- Starts inside symbol: no
- Ends inside symbol: no

```
   995| def test_caller_callee_import_expansion_respects_max_files(repo, out):
   996|     # Regression: callers/callees/internal-imports listings emitted every
   997|     # referenced file without going through note_focus_file, so they could
   998|     # exceed limits.max_files while the resolution sidecar's focus_files
   999|     # stayed under it (the related-test and Graphify branches already
  1000|     # enforced this; these three didn't).
  1001|     write_files(repo, {
  1002|         "core/a.py": "from core.b import g\nfrom core.c import h\n\n\ndef f():\n    g()\n    return h()\n",
  1003|         "core/b.py": "def g():\n    return 1\n",
  1004|         "core/c.py": "from core.a import f\n\n\ndef h():\n    return f()\n",
  1005|     })
  1006|     _scan(repo, out)
  1007|     req = _request(out, "req.json", {
  1008|         "schema_version": "1.0", "question": "q",
  1009|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
  1010|         "expansion": {"include_callers": True, "include_callees": True, "include_imports": True},
  1011|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
  1012|     })
  1013|     result = _packet(repo, out, req)
  1014|     assert result.returncode == 0, result.stderr
  1015|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1016|     assert sidecar["focus_files"] == ["core/a.py"]
  1017|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1018|     assert "[origin: caller_expansion]" not in text
  1019|     assert "[origin: callee_expansion]" not in text
  1020|     assert "limits.max_files" in text
  1021| 
  1022| 
  1023| def _git_init_commit(repo) -> str:
  1024|     import subprocess
  1025|     # graphify-out/graph.json is written *after* this commit (it needs the
  1026|     # resulting commit hash for built_at_commit) -- gitignore it first so
  1027|     # that later write leaves the worktree clean (git ignores it) rather
  1028|     # than untracked/dirty, which the dirty-worktree Graphify check would
  1029|     # otherwise (correctly) treat as unverifiable.
  1030|     (repo / ".gitignore").write_text("graphify-out/\n", encoding="utf-8")
  1031|     subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
  1032|     subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
  1033|     subprocess.run(["git", "-c", "user.email=t@t.com", "-c", "user.name=t", "commit", "-qm", "init"],
  1034|                    cwd=repo, check=True)
  1035|     return subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
  1036|                            ).stdout.strip()
  1037| 
  1038| 
  1039| def test_include_graphify_expansion_lists_revision_aligned_community_peers(repo, out):
  1040|     write_files(repo, {
  1041|         "core/a.py": "def f():\n    return 1\n",
  1042|         "core/b.py": "def g():\n    return 2\n",
  1043|     })
  1044|     commit = _git_init_commit(repo)
  1045|     graph = {
  1046|         "built_at_commit": commit,
  1047|         "nodes": [
  1048|             {"source_file": "core/a.py", "community": 5, "community_name": "Widgets"},
  1049|             {"source_file": "core/b.py", "community": 5, "community_name": "Widgets"},
  1050|         ],
  1051|     }
  1052|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
  1053|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
  1054|     _scan(repo, out)
  1055|     req = _request(out, "req.json", {
  1056|         "schema_version": "1.0", "question": "q",
  1057|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
  1058|         "expansion": {"include_graphify": True},
  1059|     })
  1060|     result = _packet(repo, out, req)
  1061|     assert result.returncode == 0, result.stderr
  1062|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1063|     assert "graphify_expansion" in text
  1064|     assert "core/b.py" in text
  1065| 
  1066| 
  1067| def test_include_graphify_withheld_when_current_commit_unavailable(repo, out):
  1068|     # Regression: when the scanned tree isn't a git repository (no HEAD
  1069|     # commit to check against), a graphify-out/graph.json with any
  1070|     # built_at_commit used to be accepted unconditionally instead of
  1071|     # being withheld -- revision alignment can't be proven either way.
  1072|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1073|     graph = {
  1074|         "built_at_commit": "deadbeef",
  1075|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
  1076|     }
  1077|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
  1078|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
  1079|     _scan(repo, out)  # no git init -- current commit is unavailable
  1080|     req = _request(out, "req.json", {
  1081|         "schema_version": "1.0", "question": "q",
  1082|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
  1083|         "expansion": {"include_graphify": True},
  1084|     })
  1085|     result = _packet(repo, out, req)
  1086|     assert result.returncode == 0, result.stderr
  1087|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1088|     assert "graphify_expansion" not in text
  1089|     assert "revision alignment cannot be proven" in text
  1090| 
  1091| 
  1092| def test_selector_resolution_report_is_charged_against_budget(repo, out):
  1093|     # Regression: the "Selector resolution report" section (one entry per
  1094|     # requested selector, however many) was appended without any budget
  1095|     # accounting, so a request naming hundreds of missing/ambiguous
  1096|     # selectors could produce a large packet while reporting ~0 estimated
  1097|     # tokens used under a tiny limits.max_estimated_tokens.
  1098|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1099|     _scan(repo, out)
  1100|     missing_files = [f"missing_{i}.py" for i in range(200)]
  1101|     # 300 tokens clears the fixed framing's own floor (header + footer,
  1102|     # reserved up front -- see rc_request.py's "Reserve the fixed framing's
  1103|     # budget cost up front") but is nowhere near enough to fit all 200
  1104|     # resolution-report entries.
  1105|     req = _request(out, "req.json", {
  1106|         "schema_version": "1.0", "question": "q",
  1107|         "selectors": {"files": missing_files, "symbols": [], "search_terms": [], "lines": []},
  1108|         "limits": {"max_estimated_tokens": 300, "max_files": 500},
  1109|     })
  1110|     result = _packet(repo, out, req)
  1111|     assert result.returncode == 0, result.stderr
  1112|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1113|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1114|     # Certainly not a full 200-entry resolution report rendered in the
  1115|     # packet body -- it must be truncated with a count-of-omitted note,
  1116|     # not a wild understatement of the packet's actual size the way ~0
  1117|     # tokens for an 18KB packet was before this fix.
  1118|     assert len(text) < 2000
  1119|     assert text.count("missing_") < 200
  1120|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
  1121| 
  1122| 
  1123| def test_file_level_expansions_render_once_per_file_not_per_symbol(repo, out):
  1124|     # Regression: _symbol_expansion() rendered imports/related-tests/
  1125|     # Graphify-peer sections (all keyed by the containing *file*, not the
  1126|     # symbol) on every call -- but an explicit file selector called it
  1127|     # once per top-level symbol in that file, so a multi-function file
  1128|     # got its "Internal imports of X" / "Related tests for X" sections
  1129|     # duplicated once per function instead of appearing once.
  1130|     write_files(repo, {
  1131|         "core/a.py": (
  1132|             "from core.dep import helper\n\n\n"
  1133|             "def f():\n    return helper()\n\n\n"
  1134|             "def g():\n    return helper()\n\n\n"
  1135|             "def h():\n    return helper()\n"
  1136|         ),
  1137|         "core/dep.py": "def helper():\n    return 1\n",
  1138|         "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 1\n",
  1139|     })
  1140|     _scan(repo, out)
  1141|     req = _request(out, "req.json", {
  1142|         "schema_version": "1.0", "question": "q",
  1143|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1144|         "expansion": {"include_callers": False, "include_callees": False, "include_imports": True,
  1145|                       "include_related_tests": True, "include_graphify": False},
  1146|         "limits": {"max_estimated_tokens": 12000, "max_files": 12},
  1147|     })
  1148|     result = _packet(repo, out, req)
  1149|     assert result.returncode == 0, result.stderr
  1150|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1151|     # Three top-level functions in core/a.py, but these file-level
  1152|     # sections must appear exactly once, not three times.
  1153|     assert text.count("Internal imports of `core/a.py`") == 1
  1154|     assert text.count("Related tests for `core/a.py`") == 1
  1155| 
  1156| 
  1157| def test_related_test_expansion_respects_global_max_files(repo, out):
  1158|     # Regression: related-test expansion appended directly to focus_files
  1159|     # under a hard-coded 10,000 ceiling instead of going through the same
  1160|     # note_focus_file() gate as every other tier, so it could silently
  1161|     # exceed limits.max_files.
  1162|     write_files(repo, {
  1163|         "core/a.py": "def f():\n    return 1\n",
  1164|         "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 1\n",
  1165|     })
  1166|     _scan(repo, out)
  1167|     req = _request(out, "req.json", {
  1168|         "schema_version": "1.0", "question": "q",
  1169|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1170|         "expansion": {"include_related_tests": True},
  1171|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
  1172|     })
  1173|     result = _packet(repo, out, req)
  1174|     assert result.returncode == 0, result.stderr
  1175|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1176|     assert sidecar["focus_files"] == ["core/a.py"]
  1177|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1178|     assert "beyond limits.max_files" in text
  1179| 
  1180| 
  1181| def test_name_override_controls_output_filename(repo, out):
  1182|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1183|     _scan(repo, out)
  1184|     req = _request(out, "req.json", {
  1185|         "schema_version": "1.0", "question": "q",
  1186|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1187|     })
  1188|     result = _packet(repo, out, req, extra=["--name", "custom_stem"])
  1189|     assert result.returncode == 0, result.stderr
  1190|     assert (out / "packets" / "packet_custom_stem.md").exists()
  1191| 
  1192| 
  1193| def test_file_level_expansion_runs_for_a_symbol_free_selected_file(repo, out):
  1194|     # Regression: the loop driving file-level expansion (imports/related-
  1195|     # tests/Graphify peers) skipped straight past `_maybe_file_expansion()`
  1196|     # whenever an explicitly selected file had no top-level symbols (e.g.
  1197|     # an __init__.py re-export shim, or a plain module that only does
  1198|     # imports at module scope). Since imports/related-tests describe the
  1199|     # *file*, not any symbol in it, a symbol-free explicitly selected file
  1200|     # previously got zero import/related-test expansion evidence even
  1201|     # when include_imports/include_related_tests were explicitly on.
  1202|     write_files(repo, {
  1203|         "core/__init__.py": "from core.dep import helper\n",
  1204|         "core/dep.py": "def helper():\n    return 1\n",
  1205|     })
  1206|     _scan(repo, out)
  1207|     req = _request(out, "req.json", {
  1208|         "schema_version": "1.0", "question": "q",
  1209|         "selectors": {"files": ["core/__init__.py"], "symbols": [], "search_terms": [], "lines": []},
  1210|         "expansion": {"include_callers": False, "include_callees": False, "include_imports": True,
  1211|                       "include_related_tests": False},
  1212|         "limits": {"max_estimated_tokens": 12000, "max_files": 12},
  1213|     })
  1214|     result = _packet(repo, out, req)
  1215|     assert result.returncode == 0, result.stderr
  1216|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1217|     assert "Internal imports of `core/__init__.py`" in text
  1218|     assert "core/dep.py" in text
  1219| 
  1220| 
  1221| def test_fixed_framing_is_reserved_before_tier1_content_spends_budget(repo, out):
  1222|     # Regression: the header/selector-resolution-report/footer were
  1223|     # charged against the budget only *after* Tier-1 explicit-selector
  1224|     # content had already been allowed to spend against the full,
  1225|     # unreserved budget. That let a request's explicit content "fit" a
  1226|     # budget that, once framing's real cost landed on top afterward, the
  1227|     # packet's actual rendered size exceeded -- generation still reported
  1228|     # success despite the true size being over limits.max_estimated_tokens.
  1229|     # Framing must be reserved (and charged) first, so a too-tight budget
  1230|     # now correctly surfaces as an explicit_conflicts abort instead of an
  1231|     # over-budget "successful" packet.
  1232|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1233|     _scan(repo, out)
  1234| 
  1235|     # A single fixed request/output name reused for *every* call below --
  1236|     # the packet header renders the request filename verbatim
  1237|     # (`- Request file: \`{name}.json\` ...`), so varying the name's
  1238|     # length between calls (e.g. "probe166" vs. "boundary" vs. "under")
  1239|     # shifts the header's own size and, right at a ~1-token-wide margin,
  1240|     # can flip whether a given max_estimated_tokens value fits -- making
  1241|     # the boundary this test measures depend on which name happened to be
  1242|     # used for which call, not just on max_estimated_tokens. Reusing one
  1243|     # name removes that variable; each call's result depends only on
  1244|     # max_tokens.
  1245|     name = "req"
  1246| 
  1247|     def _gen(max_tokens):
  1248|         req = _request(out, f"{name}.json", {
  1249|             "schema_version": "1.0", "question": "q",
  1250|             "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1251|             "expansion": {"include_callers": False, "include_callees": False, "include_imports": False,
  1252|                           "include_related_tests": False},
  1253|             "limits": {"max_estimated_tokens": max_tokens, "max_files": 12},
  1254|         })
  1255|         return rr.generate_packet_from_request(repo, out, req, name_override=name)
  1256| 
  1257|     # Sanity-bound the binary search: a generous budget must succeed.
  1258|     packet_path, _, err = _gen(2000)
  1259|     assert packet_path is not None, err
  1260| 
  1261|     lo, hi = 1, 2000
  1262|     while lo < hi:
  1263|         mid = (lo + hi) // 2
  1264|         packet_path, _, _ = _gen(mid)
  1265|         if packet_path is not None:
  1266|             hi = mid
  1267|         else:
  1268|             lo = mid + 1
  1269|     min_success_tokens = hi
  1270| 
  1271|     # At the smallest budget that still succeeds, the packet's own
  1272|     # reported size must not exceed what was actually requested -- this
  1273|     # is exactly the invariant framing-charged-too-late violated (it
  1274|     # would report success here with estimated_tokens_used well above
  1275|     # min_success_tokens, since Tier-1 content had already spent as if
  1276|     # framing were free).
  1277|     packet_path, _, err = _gen(min_success_tokens)
  1278|     assert packet_path is not None, err
  1279|     sidecar = json.loads((out / "packets" / f"packet_{name}.resolution.json").read_text(encoding="utf-8"))
  1280|     assert sidecar["estimated_tokens_used"] <= min_success_tokens
  1281| 
  1282|     # One token below that boundary must hard-abort -- not silently
  1283|     # succeed with a packet whose true size exceeds the requested cap.
  1284|     packet_path, _, err = _gen(min_success_tokens - 1)
  1285|     assert packet_path is None
  1286|     assert "do not fit" in (err or "")
  1287| 
  1288| 
  1289| def test_search_term_match_cannot_exceed_budget_via_late_footer_charge(repo, out):
  1290|     # Regression (fresh Codex evidence after the first framing-reservation
  1291|     # fix): reserving the header up front, then charging the selector-
  1292|     # resolution report entries via budget.allow(), and only *then*
  1293|     # unconditionally spending the footer, still let a resolution-report
  1294|     # entry get allowed against a budget that hadn't yet accounted for the
  1295|     # footer's own cost -- the footer's later unconditional spend then
  1296|     # pushed the packet's true size back over the cap anyway. Concretely:
  1297|     # a 100-token request whose only content is one ~500-character search
  1298|     # match still reported estimated_tokens_used around 136, over the
  1299|     # 100-token cap. Header and footer must be reserved *together*, as one
  1300|     # atomic unit, before the resolution report (or anything else) spends.
  1301|     write_files(repo, {"a.py": f"needle = {'x' * 500!r}\n"})
  1302|     _scan(repo, out)
  1303|     req = _request(out, "req.json", {
  1304|         "schema_version": "1.0", "question": "q",
  1305|         "selectors": {"files": [], "symbols": [], "search_terms": ["needle"], "lines": []},
  1306|         "limits": {"max_estimated_tokens": 100, "max_files": 12},
  1307|     })
  1308|     packet_path, _, err = rr.generate_packet_from_request(repo, out, req, name_override="req")
  1309|     # Either this cleanly hard-aborts (the fixed framing alone doesn't fit
  1310|     # this tiny budget) or, if it succeeds, its real size must not exceed
  1311|     # what was requested -- what it must never do is "succeed" while its
  1312|     # true size exceeds the cap.
  1313|     if packet_path is not None:
  1314|         sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1315|         assert sidecar["estimated_tokens_used"] <= 100
  1316|     else:
  1317|         assert "too small to fit" in err
  1318| 
  1319| 
  1320| def _spy_iter_safe_lines(monkeypatch, module):
  1321|     """Wraps module._iter_safe_lines so every line it actually yields is
  1322|     counted, without changing its behavior (including .close()
  1323|     forwarding). Returns the shared counter dict; read counter["count"]
  1324|     after the call under test."""
  1325|     real = module._iter_safe_lines
  1326|     counter = {"count": 0}
  1327| 
  1328|     def spy(root, rel_path, start, end):
  1329|         gen = real(root, rel_path, start, end)
  1330|         if gen is None:
  1331|             return None
  1332| 
  1333|         def _wrapped():
  1334|             try:
  1335|                 for item in gen:
  1336|                     counter["count"] += 1
  1337|                     yield item
  1338|             finally:
  1339|                 gen.close()
  1340| 
  1341|         return _wrapped()
  1342| 
  1343|     monkeypatch.setattr(module, "_iter_safe_lines", spy)
  1344|     return counter
  1345| 
  1346| 
  1347| def test_explicit_excerpt_streams_and_stops_instead_of_materializing_whole_range(repo, out, monkeypatch):
  1348|     # Regression: an explicit file selector's excerpt was fully
  1349|     # materialized via _safe_excerpt(root, rel, 1, line_count) *before*
  1350|     # any budget check -- for a file the scanner deliberately keeps in
  1351|     # the inventory without reading whole (files over MAX_TEXT_READ_BYTES
  1352|     # still get a real, streamed-counted line_count -- see rc_scan.py),
  1353|     # this could allocate the file's entire content in memory just to
  1354|     # then learn the excerpt doesn't fit. The excerpt must instead be
  1355|     # streamed and stop reading as soon as it's clear the remaining
  1356|     # budget is exceeded.
  1357|     big_lines = [f"line number {i:06d} of a much larger file body" for i in range(5000)]
  1358|     write_files(repo, {"big.py": "\n".join(big_lines) + "\n"})
  1359|     _scan(repo, out)
  1360|     counter = _spy_iter_safe_lines(monkeypatch, rr)
  1361| 
  1362|     req = _request(out, "req.json", {
  1363|         "schema_version": "1.0", "question": "q",
  1364|         "selectors": {"files": ["big.py"], "symbols": [], "search_terms": [], "lines": []},
  1365|         "limits": {"max_estimated_tokens": 200, "max_files": 12},
  1366|     })
  1367|     packet_path, _, err = rr.generate_packet_from_request(repo, out, req, name_override="req")
  1368|     # The explicit selector doesn't fit this tiny budget -- correctly a
  1369|     # hard conflict, per the existing explicit-selector contract -- but
  1370|     # what matters here is that reaching that conclusion did not require
  1371|     # reading anywhere near all 5000 lines of the file first.
  1372|     assert packet_path is None
  1373|     assert counter["count"] < 500, f"read {counter['count']} lines before bailing out -- not streaming"
  1374| 
  1375| 
  1376| def test_search_term_scan_streams_and_stops_once_collect_cap_is_reached(repo, out, monkeypatch):
  1377|     # Regression: _scan_term_matches() called _safe_excerpt(root, rel, 1,
  1378|     # 10_000_000) per file, materializing up to ten million lines before
  1379|     # any matching happened -- a large included file (the scanner
  1380|     # deliberately keeps files over MAX_TEXT_READ_BYTES in the inventory
  1381|     # without reading them whole -- see rc_scan.py) could exhaust memory
  1382|     # searching for a term that matches many times, or not at all. A
  1383|     # literal search must still scan every line to find every match up to
  1384|     # collect_cap, but it must do so line-by-line as the file streams in
  1385|     # -- not by pre-loading the whole range into memory first -- and it
  1386|     # must stop consuming the file entirely once collect_cap matches have
  1387|     # been found, not keep reading to the end regardless.
  1388|     #
  1389|     # max_files=1 makes collect_cap (max(1, max_files) * 5) a small,
  1390|     # known value (5): put 10 matches up front, comfortably more than
  1391|     # collect_cap, followed by thousands of filler lines the scan must
  1392|     # never need to reach.
  1393|     big_lines = ["needle_marker"] * 10 + [f"filler line {i:06d}" for i in range(5000)]
  1394|     write_files(repo, {"big.py": "\n".join(big_lines) + "\n"})
  1395|     _scan(repo, out)
  1396|     counter = _spy_iter_safe_lines(monkeypatch, rr)
  1397| 
  1398|     req = _request(out, "req.json", {
  1399|         "schema_version": "1.0", "question": "q",
  1400|         "selectors": {"files": [], "symbols": [], "search_terms": ["needle_marker"], "lines": []},
  1401|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
  1402|     })
  1403|     packet_path, _, err = rr.generate_packet_from_request(repo, out, req, name_override="req")
  1404|     assert packet_path is not None, err
  1405|     text = packet_path.read_text(encoding="utf-8")
  1406|     assert "needle_marker" in text
  1407|     # collect_cap is 5 here -- the scan must stop shortly after finding
  1408|     # the 5th match, not read all ~5010 lines of the file.
  1409|     assert counter["count"] < 500, f"read {counter['count']} lines after collect_cap was reached -- not streaming"
  1410| 
  1411| 
  1412| def test_every_rendered_fragment_is_charged_against_the_budget(repo, out):
  1413|     # Regression: several pieces of the rendered packet were appended to
  1414|     # `out`/`header_lines` without ever being passed to budget.spend() at
  1415|     # their own true size -- the excerpt code-fence markers ("```\n"/
  1416|     # "\n```\n"), the "Estimated tokens used" summary line itself, the
  1417|     # "## Selector resolution report" section heading, and (most subtly)
  1418|     # the "\n".join(out)/"\n".join(header_lines) separators inserted
  1419|     # *between* every other already-charged fragment, none of which any
  1420|     # individual budget.spend() call accounted for. Each was individually
  1421|     # small, but together they let a packet's real rendered size exceed
  1422|     # limits.max_estimated_tokens while the sidecar still reported success
  1423|     # at (or under) the requested cap. At the minimal successful budget
  1424|     # for a plain one-file selector, the packet's true byte size must
  1425|     # never exceed limits.max_estimated_tokens * 4.
  1426|     write_files(repo, {"a.py": "def f():\n    return 1\n"})
  1427|     _scan(repo, out)
  1428| 
  1429|     name = "req"
  1430| 
  1431|     def _gen(max_tokens):
  1432|         req = _request(out, f"{name}.json", {
  1433|             "schema_version": "1.0", "question": "q",
  1434|             "selectors": {"files": ["a.py"], "symbols": [], "search_terms": [], "lines": []},
  1435|             "expansion": {"include_callers": False, "include_callees": False, "include_imports": False,
  1436|                           "include_related_tests": False},
  1437|             "limits": {"max_estimated_tokens": max_tokens, "max_files": 12},
  1438|         })
  1439|         return rr.generate_packet_from_request(repo, out, req, name_override=name)
  1440| 
  1441|     lo, hi = 1, 2000
  1442|     while lo < hi:
  1443|         mid = (lo + hi) // 2
  1444|         packet_path, _, _ = _gen(mid)
  1445|         if packet_path is not None:
  1446|             hi = mid
  1447|         else:
  1448|             lo = mid + 1
  1449|     min_success_tokens = hi
  1450| 
  1451|     packet_path, _, err = _gen(min_success_tokens)
  1452|     assert packet_path is not None, err
  1453|     text = packet_path.read_text(encoding="utf-8")
  1454|     assert len(text) <= min_success_tokens * 4, (
  1455|         f"packet's real size ({len(text)} chars) exceeds the requested budget "
  1456|         f"({min_success_tokens * 4} chars) at the minimal successful token count"
  1457|     )
```
