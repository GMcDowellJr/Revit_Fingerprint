# Chunk of dev_tools/repo_context/tests/test_packet_from_request.py

- Source relative path: `dev_tools/repo_context/tests/test_packet_from_request.py`
- Chunk: 3 of 3
- Original line range: 993-1303
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_caller_callee_import_expansion_respects_max_files, _git_init_commit, test_include_graphify_expansion_lists_revision_aligned_community_peers, test_include_graphify_withheld_when_current_commit_unavailable, test_selector_resolution_report_is_charged_against_budget, test_file_level_expansions_render_once_per_file_not_per_symbol, test_related_test_expansion_respects_global_max_files, test_name_override_controls_output_filename, test_file_level_expansion_runs_for_a_symbol_free_selected_file, test_fixed_framing_is_reserved_before_tier1_content_spends_budget, test_fixed_framing_is_reserved_before_tier1_content_spends_budget._gen, test_search_term_match_cannot_exceed_budget_via_late_footer_charge
- Source SHA-256: 0cd19f41559a24f6cef43f7a7eb4785e7345e4e86bf582ab66c5d61dfdabca92
- Starts inside symbol: no
- Ends inside symbol: no

```
   993| def test_caller_callee_import_expansion_respects_max_files(repo, out):
   994|     # Regression: callers/callees/internal-imports listings emitted every
   995|     # referenced file without going through note_focus_file, so they could
   996|     # exceed limits.max_files while the resolution sidecar's focus_files
   997|     # stayed under it (the related-test and Graphify branches already
   998|     # enforced this; these three didn't).
   999|     write_files(repo, {
  1000|         "core/a.py": "from core.b import g\nfrom core.c import h\n\n\ndef f():\n    g()\n    return h()\n",
  1001|         "core/b.py": "def g():\n    return 1\n",
  1002|         "core/c.py": "from core.a import f\n\n\ndef h():\n    return f()\n",
  1003|     })
  1004|     _scan(repo, out)
  1005|     req = _request(out, "req.json", {
  1006|         "schema_version": "1.0", "question": "q",
  1007|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
  1008|         "expansion": {"include_callers": True, "include_callees": True, "include_imports": True},
  1009|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
  1010|     })
  1011|     result = _packet(repo, out, req)
  1012|     assert result.returncode == 0, result.stderr
  1013|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1014|     assert sidecar["focus_files"] == ["core/a.py"]
  1015|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1016|     assert "[origin: caller_expansion]" not in text
  1017|     assert "[origin: callee_expansion]" not in text
  1018|     assert "limits.max_files" in text
  1019| 
  1020| 
  1021| def _git_init_commit(repo) -> str:
  1022|     import subprocess
  1023|     # graphify-out/graph.json is written *after* this commit (it needs the
  1024|     # resulting commit hash for built_at_commit) -- gitignore it first so
  1025|     # that later write leaves the worktree clean (git ignores it) rather
  1026|     # than untracked/dirty, which the dirty-worktree Graphify check would
  1027|     # otherwise (correctly) treat as unverifiable.
  1028|     (repo / ".gitignore").write_text("graphify-out/\n", encoding="utf-8")
  1029|     subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
  1030|     subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
  1031|     subprocess.run(["git", "-c", "user.email=t@t.com", "-c", "user.name=t", "commit", "-qm", "init"],
  1032|                    cwd=repo, check=True)
  1033|     return subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
  1034|                            ).stdout.strip()
  1035| 
  1036| 
  1037| def test_include_graphify_expansion_lists_revision_aligned_community_peers(repo, out):
  1038|     write_files(repo, {
  1039|         "core/a.py": "def f():\n    return 1\n",
  1040|         "core/b.py": "def g():\n    return 2\n",
  1041|     })
  1042|     commit = _git_init_commit(repo)
  1043|     graph = {
  1044|         "built_at_commit": commit,
  1045|         "nodes": [
  1046|             {"source_file": "core/a.py", "community": 5, "community_name": "Widgets"},
  1047|             {"source_file": "core/b.py", "community": 5, "community_name": "Widgets"},
  1048|         ],
  1049|     }
  1050|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
  1051|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
  1052|     _scan(repo, out)
  1053|     req = _request(out, "req.json", {
  1054|         "schema_version": "1.0", "question": "q",
  1055|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
  1056|         "expansion": {"include_graphify": True},
  1057|     })
  1058|     result = _packet(repo, out, req)
  1059|     assert result.returncode == 0, result.stderr
  1060|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1061|     assert "graphify_expansion" in text
  1062|     assert "core/b.py" in text
  1063| 
  1064| 
  1065| def test_include_graphify_withheld_when_current_commit_unavailable(repo, out):
  1066|     # Regression: when the scanned tree isn't a git repository (no HEAD
  1067|     # commit to check against), a graphify-out/graph.json with any
  1068|     # built_at_commit used to be accepted unconditionally instead of
  1069|     # being withheld -- revision alignment can't be proven either way.
  1070|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1071|     graph = {
  1072|         "built_at_commit": "deadbeef",
  1073|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
  1074|     }
  1075|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
  1076|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
  1077|     _scan(repo, out)  # no git init -- current commit is unavailable
  1078|     req = _request(out, "req.json", {
  1079|         "schema_version": "1.0", "question": "q",
  1080|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
  1081|         "expansion": {"include_graphify": True},
  1082|     })
  1083|     result = _packet(repo, out, req)
  1084|     assert result.returncode == 0, result.stderr
  1085|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1086|     assert "graphify_expansion" not in text
  1087|     assert "revision alignment cannot be proven" in text
  1088| 
  1089| 
  1090| def test_selector_resolution_report_is_charged_against_budget(repo, out):
  1091|     # Regression: the "Selector resolution report" section (one entry per
  1092|     # requested selector, however many) was appended without any budget
  1093|     # accounting, so a request naming hundreds of missing/ambiguous
  1094|     # selectors could produce a large packet while reporting ~0 estimated
  1095|     # tokens used under a tiny limits.max_estimated_tokens.
  1096|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1097|     _scan(repo, out)
  1098|     missing_files = [f"missing_{i}.py" for i in range(200)]
  1099|     # 300 tokens clears the fixed framing's own floor (header + footer,
  1100|     # reserved up front -- see rc_request.py's "Reserve the fixed framing's
  1101|     # budget cost up front") but is nowhere near enough to fit all 200
  1102|     # resolution-report entries.
  1103|     req = _request(out, "req.json", {
  1104|         "schema_version": "1.0", "question": "q",
  1105|         "selectors": {"files": missing_files, "symbols": [], "search_terms": [], "lines": []},
  1106|         "limits": {"max_estimated_tokens": 300, "max_files": 500},
  1107|     })
  1108|     result = _packet(repo, out, req)
  1109|     assert result.returncode == 0, result.stderr
  1110|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1111|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1112|     # Certainly not a full 200-entry resolution report rendered in the
  1113|     # packet body -- it must be truncated with a count-of-omitted note,
  1114|     # not a wild understatement of the packet's actual size the way ~0
  1115|     # tokens for an 18KB packet was before this fix.
  1116|     assert len(text) < 2000
  1117|     assert text.count("missing_") < 200
  1118|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
  1119| 
  1120| 
  1121| def test_file_level_expansions_render_once_per_file_not_per_symbol(repo, out):
  1122|     # Regression: _symbol_expansion() rendered imports/related-tests/
  1123|     # Graphify-peer sections (all keyed by the containing *file*, not the
  1124|     # symbol) on every call -- but an explicit file selector called it
  1125|     # once per top-level symbol in that file, so a multi-function file
  1126|     # got its "Internal imports of X" / "Related tests for X" sections
  1127|     # duplicated once per function instead of appearing once.
  1128|     write_files(repo, {
  1129|         "core/a.py": (
  1130|             "from core.dep import helper\n\n\n"
  1131|             "def f():\n    return helper()\n\n\n"
  1132|             "def g():\n    return helper()\n\n\n"
  1133|             "def h():\n    return helper()\n"
  1134|         ),
  1135|         "core/dep.py": "def helper():\n    return 1\n",
  1136|         "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 1\n",
  1137|     })
  1138|     _scan(repo, out)
  1139|     req = _request(out, "req.json", {
  1140|         "schema_version": "1.0", "question": "q",
  1141|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1142|         "expansion": {"include_callers": False, "include_callees": False, "include_imports": True,
  1143|                       "include_related_tests": True, "include_graphify": False},
  1144|         "limits": {"max_estimated_tokens": 12000, "max_files": 12},
  1145|     })
  1146|     result = _packet(repo, out, req)
  1147|     assert result.returncode == 0, result.stderr
  1148|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1149|     # Three top-level functions in core/a.py, but these file-level
  1150|     # sections must appear exactly once, not three times.
  1151|     assert text.count("Internal imports of `core/a.py`") == 1
  1152|     assert text.count("Related tests for `core/a.py`") == 1
  1153| 
  1154| 
  1155| def test_related_test_expansion_respects_global_max_files(repo, out):
  1156|     # Regression: related-test expansion appended directly to focus_files
  1157|     # under a hard-coded 10,000 ceiling instead of going through the same
  1158|     # note_focus_file() gate as every other tier, so it could silently
  1159|     # exceed limits.max_files.
  1160|     write_files(repo, {
  1161|         "core/a.py": "def f():\n    return 1\n",
  1162|         "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 1\n",
  1163|     })
  1164|     _scan(repo, out)
  1165|     req = _request(out, "req.json", {
  1166|         "schema_version": "1.0", "question": "q",
  1167|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1168|         "expansion": {"include_related_tests": True},
  1169|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
  1170|     })
  1171|     result = _packet(repo, out, req)
  1172|     assert result.returncode == 0, result.stderr
  1173|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1174|     assert sidecar["focus_files"] == ["core/a.py"]
  1175|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1176|     assert "beyond limits.max_files" in text
  1177| 
  1178| 
  1179| def test_name_override_controls_output_filename(repo, out):
  1180|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1181|     _scan(repo, out)
  1182|     req = _request(out, "req.json", {
  1183|         "schema_version": "1.0", "question": "q",
  1184|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1185|     })
  1186|     result = _packet(repo, out, req, extra=["--name", "custom_stem"])
  1187|     assert result.returncode == 0, result.stderr
  1188|     assert (out / "packets" / "packet_custom_stem.md").exists()
  1189| 
  1190| 
  1191| def test_file_level_expansion_runs_for_a_symbol_free_selected_file(repo, out):
  1192|     # Regression: the loop driving file-level expansion (imports/related-
  1193|     # tests/Graphify peers) skipped straight past `_maybe_file_expansion()`
  1194|     # whenever an explicitly selected file had no top-level symbols (e.g.
  1195|     # an __init__.py re-export shim, or a plain module that only does
  1196|     # imports at module scope). Since imports/related-tests describe the
  1197|     # *file*, not any symbol in it, a symbol-free explicitly selected file
  1198|     # previously got zero import/related-test expansion evidence even
  1199|     # when include_imports/include_related_tests were explicitly on.
  1200|     write_files(repo, {
  1201|         "core/__init__.py": "from core.dep import helper\n",
  1202|         "core/dep.py": "def helper():\n    return 1\n",
  1203|     })
  1204|     _scan(repo, out)
  1205|     req = _request(out, "req.json", {
  1206|         "schema_version": "1.0", "question": "q",
  1207|         "selectors": {"files": ["core/__init__.py"], "symbols": [], "search_terms": [], "lines": []},
  1208|         "expansion": {"include_callers": False, "include_callees": False, "include_imports": True,
  1209|                       "include_related_tests": False},
  1210|         "limits": {"max_estimated_tokens": 12000, "max_files": 12},
  1211|     })
  1212|     result = _packet(repo, out, req)
  1213|     assert result.returncode == 0, result.stderr
  1214|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1215|     assert "Internal imports of `core/__init__.py`" in text
  1216|     assert "core/dep.py" in text
  1217| 
  1218| 
  1219| def test_fixed_framing_is_reserved_before_tier1_content_spends_budget(repo, out):
  1220|     # Regression: the header/selector-resolution-report/footer were
  1221|     # charged against the budget only *after* Tier-1 explicit-selector
  1222|     # content had already been allowed to spend against the full,
  1223|     # unreserved budget. That let a request's explicit content "fit" a
  1224|     # budget that, once framing's real cost landed on top afterward, the
  1225|     # packet's actual rendered size exceeded -- generation still reported
  1226|     # success despite the true size being over limits.max_estimated_tokens.
  1227|     # Framing must be reserved (and charged) first, so a too-tight budget
  1228|     # now correctly surfaces as an explicit_conflicts abort instead of an
  1229|     # over-budget "successful" packet.
  1230|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1231|     _scan(repo, out)
  1232| 
  1233|     def _gen(name, max_tokens):
  1234|         req = _request(out, f"{name}.json", {
  1235|             "schema_version": "1.0", "question": "q",
  1236|             "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1237|             "expansion": {"include_callers": False, "include_callees": False, "include_imports": False,
  1238|                           "include_related_tests": False},
  1239|             "limits": {"max_estimated_tokens": max_tokens, "max_files": 12},
  1240|         })
  1241|         return rr.generate_packet_from_request(repo, out, req, name_override=name)
  1242| 
  1243|     # Sanity-bound the binary search: a generous budget must succeed.
  1244|     packet_path, _, err = _gen("hi", 2000)
  1245|     assert packet_path is not None, err
  1246| 
  1247|     lo, hi = 1, 2000
  1248|     while lo < hi:
  1249|         mid = (lo + hi) // 2
  1250|         packet_path, _, _ = _gen(f"probe{mid}", mid)
  1251|         if packet_path is not None:
  1252|             hi = mid
  1253|         else:
  1254|             lo = mid + 1
  1255|     min_success_tokens = hi
  1256| 
  1257|     # At the smallest budget that still succeeds, the packet's own
  1258|     # reported size must not exceed what was actually requested -- this
  1259|     # is exactly the invariant framing-charged-too-late violated (it
  1260|     # would report success here with estimated_tokens_used well above
  1261|     # min_success_tokens, since Tier-1 content had already spent as if
  1262|     # framing were free).
  1263|     packet_path, _, err = _gen("boundary", min_success_tokens)
  1264|     assert packet_path is not None, err
  1265|     sidecar = json.loads((out / "packets" / "packet_boundary.resolution.json").read_text(encoding="utf-8"))
  1266|     assert sidecar["estimated_tokens_used"] <= min_success_tokens
  1267| 
  1268|     # One token below that boundary must hard-abort -- not silently
  1269|     # succeed with a packet whose true size exceeds the requested cap.
  1270|     packet_path, _, err = _gen("under", min_success_tokens - 1)
  1271|     assert packet_path is None
  1272|     assert "do not fit" in (err or "")
  1273| 
  1274| 
  1275| def test_search_term_match_cannot_exceed_budget_via_late_footer_charge(repo, out):
  1276|     # Regression (fresh Codex evidence after the first framing-reservation
  1277|     # fix): reserving the header up front, then charging the selector-
  1278|     # resolution report entries via budget.allow(), and only *then*
  1279|     # unconditionally spending the footer, still let a resolution-report
  1280|     # entry get allowed against a budget that hadn't yet accounted for the
  1281|     # footer's own cost -- the footer's later unconditional spend then
  1282|     # pushed the packet's true size back over the cap anyway. Concretely:
  1283|     # a 100-token request whose only content is one ~500-character search
  1284|     # match still reported estimated_tokens_used around 136, over the
  1285|     # 100-token cap. Header and footer must be reserved *together*, as one
  1286|     # atomic unit, before the resolution report (or anything else) spends.
  1287|     write_files(repo, {"a.py": f"needle = {'x' * 500!r}\n"})
  1288|     _scan(repo, out)
  1289|     req = _request(out, "req.json", {
  1290|         "schema_version": "1.0", "question": "q",
  1291|         "selectors": {"files": [], "symbols": [], "search_terms": ["needle"], "lines": []},
  1292|         "limits": {"max_estimated_tokens": 100, "max_files": 12},
  1293|     })
  1294|     packet_path, _, err = rr.generate_packet_from_request(repo, out, req, name_override="req")
  1295|     # Either this cleanly hard-aborts (the fixed framing alone doesn't fit
  1296|     # this tiny budget) or, if it succeeds, its real size must not exceed
  1297|     # what was requested -- what it must never do is "succeed" while its
  1298|     # true size exceeds the cap.
  1299|     if packet_path is not None:
  1300|         sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1301|         assert sidecar["estimated_tokens_used"] <= 100
  1302|     else:
  1303|         assert "too small to fit" in err
```
