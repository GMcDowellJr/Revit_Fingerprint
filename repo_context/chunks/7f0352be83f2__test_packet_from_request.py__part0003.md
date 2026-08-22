# Chunk of dev_tools/repo_context/tests/test_packet_from_request.py

- Source relative path: `dev_tools/repo_context/tests/test_packet_from_request.py`
- Chunk: 3 of 3
- Original line range: 1009-1243
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_include_graphify_expansion_lists_revision_aligned_community_peers, test_include_graphify_withheld_when_current_commit_unavailable, test_selector_resolution_report_is_charged_against_budget, test_file_level_expansions_render_once_per_file_not_per_symbol, test_related_test_expansion_respects_global_max_files, test_name_override_controls_output_filename, test_file_level_expansion_runs_for_a_symbol_free_selected_file, test_fixed_framing_is_reserved_before_tier1_content_spends_budget, test_fixed_framing_is_reserved_before_tier1_content_spends_budget._gen
- Source SHA-256: 30ed034adfbd24213b55c30a03d99cc5f8036eb9a7f7a36502023450b6d37a45
- Starts inside symbol: no
- Ends inside symbol: no

```
  1009| def test_include_graphify_expansion_lists_revision_aligned_community_peers(repo, out):
  1010|     write_files(repo, {
  1011|         "core/a.py": "def f():\n    return 1\n",
  1012|         "core/b.py": "def g():\n    return 2\n",
  1013|     })
  1014|     commit = _git_init_commit(repo)
  1015|     graph = {
  1016|         "built_at_commit": commit,
  1017|         "nodes": [
  1018|             {"source_file": "core/a.py", "community": 5, "community_name": "Widgets"},
  1019|             {"source_file": "core/b.py", "community": 5, "community_name": "Widgets"},
  1020|         ],
  1021|     }
  1022|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
  1023|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
  1024|     _scan(repo, out)
  1025|     req = _request(out, "req.json", {
  1026|         "schema_version": "1.0", "question": "q",
  1027|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
  1028|         "expansion": {"include_graphify": True},
  1029|     })
  1030|     result = _packet(repo, out, req)
  1031|     assert result.returncode == 0, result.stderr
  1032|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1033|     assert "graphify_expansion" in text
  1034|     assert "core/b.py" in text
  1035| 
  1036| 
  1037| def test_include_graphify_withheld_when_current_commit_unavailable(repo, out):
  1038|     # Regression: when the scanned tree isn't a git repository (no HEAD
  1039|     # commit to check against), a graphify-out/graph.json with any
  1040|     # built_at_commit used to be accepted unconditionally instead of
  1041|     # being withheld -- revision alignment can't be proven either way.
  1042|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1043|     graph = {
  1044|         "built_at_commit": "deadbeef",
  1045|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
  1046|     }
  1047|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
  1048|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
  1049|     _scan(repo, out)  # no git init -- current commit is unavailable
  1050|     req = _request(out, "req.json", {
  1051|         "schema_version": "1.0", "question": "q",
  1052|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
  1053|         "expansion": {"include_graphify": True},
  1054|     })
  1055|     result = _packet(repo, out, req)
  1056|     assert result.returncode == 0, result.stderr
  1057|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1058|     assert "graphify_expansion" not in text
  1059|     assert "revision alignment cannot be proven" in text
  1060| 
  1061| 
  1062| def test_selector_resolution_report_is_charged_against_budget(repo, out):
  1063|     # Regression: the "Selector resolution report" section (one entry per
  1064|     # requested selector, however many) was appended without any budget
  1065|     # accounting, so a request naming hundreds of missing/ambiguous
  1066|     # selectors could produce a large packet while reporting ~0 estimated
  1067|     # tokens used under a tiny limits.max_estimated_tokens.
  1068|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1069|     _scan(repo, out)
  1070|     missing_files = [f"missing_{i}.py" for i in range(200)]
  1071|     req = _request(out, "req.json", {
  1072|         "schema_version": "1.0", "question": "q",
  1073|         "selectors": {"files": missing_files, "symbols": [], "search_terms": [], "lines": []},
  1074|         "limits": {"max_estimated_tokens": 1, "max_files": 500},
  1075|     })
  1076|     result = _packet(repo, out, req)
  1077|     assert result.returncode == 0, result.stderr
  1078|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1079|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1080|     # With a 4-character budget, almost nothing should have rendered --
  1081|     # certainly not a full 200-entry resolution report. The fixed header
  1082|     # framing is itself now charged (a separate fix), so the reported
  1083|     # figure is the framing's own honest cost, not exactly 0 -- but it
  1084|     # must stay small and, above all, must not be a wild understatement
  1085|     # of the packet's actual size the way ~0 tokens for an 18KB packet
  1086|     # was before either fix.
  1087|     assert len(text) < 2000
  1088|     assert text.count("missing_") < 200
  1089|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
  1090| 
  1091| 
  1092| def test_file_level_expansions_render_once_per_file_not_per_symbol(repo, out):
  1093|     # Regression: _symbol_expansion() rendered imports/related-tests/
  1094|     # Graphify-peer sections (all keyed by the containing *file*, not the
  1095|     # symbol) on every call -- but an explicit file selector called it
  1096|     # once per top-level symbol in that file, so a multi-function file
  1097|     # got its "Internal imports of X" / "Related tests for X" sections
  1098|     # duplicated once per function instead of appearing once.
  1099|     write_files(repo, {
  1100|         "core/a.py": (
  1101|             "from core.dep import helper\n\n\n"
  1102|             "def f():\n    return helper()\n\n\n"
  1103|             "def g():\n    return helper()\n\n\n"
  1104|             "def h():\n    return helper()\n"
  1105|         ),
  1106|         "core/dep.py": "def helper():\n    return 1\n",
  1107|         "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 1\n",
  1108|     })
  1109|     _scan(repo, out)
  1110|     req = _request(out, "req.json", {
  1111|         "schema_version": "1.0", "question": "q",
  1112|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1113|         "expansion": {"include_callers": False, "include_callees": False, "include_imports": True,
  1114|                       "include_related_tests": True, "include_graphify": False},
  1115|         "limits": {"max_estimated_tokens": 12000, "max_files": 12},
  1116|     })
  1117|     result = _packet(repo, out, req)
  1118|     assert result.returncode == 0, result.stderr
  1119|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1120|     # Three top-level functions in core/a.py, but these file-level
  1121|     # sections must appear exactly once, not three times.
  1122|     assert text.count("Internal imports of `core/a.py`") == 1
  1123|     assert text.count("Related tests for `core/a.py`") == 1
  1124| 
  1125| 
  1126| def test_related_test_expansion_respects_global_max_files(repo, out):
  1127|     # Regression: related-test expansion appended directly to focus_files
  1128|     # under a hard-coded 10,000 ceiling instead of going through the same
  1129|     # note_focus_file() gate as every other tier, so it could silently
  1130|     # exceed limits.max_files.
  1131|     write_files(repo, {
  1132|         "core/a.py": "def f():\n    return 1\n",
  1133|         "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 1\n",
  1134|     })
  1135|     _scan(repo, out)
  1136|     req = _request(out, "req.json", {
  1137|         "schema_version": "1.0", "question": "q",
  1138|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1139|         "expansion": {"include_related_tests": True},
  1140|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
  1141|     })
  1142|     result = _packet(repo, out, req)
  1143|     assert result.returncode == 0, result.stderr
  1144|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1145|     assert sidecar["focus_files"] == ["core/a.py"]
  1146|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1147|     assert "beyond limits.max_files" in text
  1148| 
  1149| 
  1150| def test_name_override_controls_output_filename(repo, out):
  1151|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1152|     _scan(repo, out)
  1153|     req = _request(out, "req.json", {
  1154|         "schema_version": "1.0", "question": "q",
  1155|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1156|     })
  1157|     result = _packet(repo, out, req, extra=["--name", "custom_stem"])
  1158|     assert result.returncode == 0, result.stderr
  1159|     assert (out / "packets" / "packet_custom_stem.md").exists()
  1160| 
  1161| 
  1162| def test_file_level_expansion_runs_for_a_symbol_free_selected_file(repo, out):
  1163|     # Regression: the loop driving file-level expansion (imports/related-
  1164|     # tests/Graphify peers) skipped straight past `_maybe_file_expansion()`
  1165|     # whenever an explicitly selected file had no top-level symbols (e.g.
  1166|     # an __init__.py re-export shim, or a plain module that only does
  1167|     # imports at module scope). Since imports/related-tests describe the
  1168|     # *file*, not any symbol in it, a symbol-free explicitly selected file
  1169|     # previously got zero import/related-test expansion evidence even
  1170|     # when include_imports/include_related_tests were explicitly on.
  1171|     write_files(repo, {
  1172|         "core/__init__.py": "from core.dep import helper\n",
  1173|         "core/dep.py": "def helper():\n    return 1\n",
  1174|     })
  1175|     _scan(repo, out)
  1176|     req = _request(out, "req.json", {
  1177|         "schema_version": "1.0", "question": "q",
  1178|         "selectors": {"files": ["core/__init__.py"], "symbols": [], "search_terms": [], "lines": []},
  1179|         "expansion": {"include_callers": False, "include_callees": False, "include_imports": True,
  1180|                       "include_related_tests": False},
  1181|         "limits": {"max_estimated_tokens": 12000, "max_files": 12},
  1182|     })
  1183|     result = _packet(repo, out, req)
  1184|     assert result.returncode == 0, result.stderr
  1185|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1186|     assert "Internal imports of `core/__init__.py`" in text
  1187|     assert "core/dep.py" in text
  1188| 
  1189| 
  1190| def test_fixed_framing_is_reserved_before_tier1_content_spends_budget(repo, out):
  1191|     # Regression: the header/selector-resolution-report/footer were
  1192|     # charged against the budget only *after* Tier-1 explicit-selector
  1193|     # content had already been allowed to spend against the full,
  1194|     # unreserved budget. That let a request's explicit content "fit" a
  1195|     # budget that, once framing's real cost landed on top afterward, the
  1196|     # packet's actual rendered size exceeded -- generation still reported
  1197|     # success despite the true size being over limits.max_estimated_tokens.
  1198|     # Framing must be reserved (and charged) first, so a too-tight budget
  1199|     # now correctly surfaces as an explicit_conflicts abort instead of an
  1200|     # over-budget "successful" packet.
  1201|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1202|     _scan(repo, out)
  1203| 
  1204|     def _gen(name, max_tokens):
  1205|         req = _request(out, f"{name}.json", {
  1206|             "schema_version": "1.0", "question": "q",
  1207|             "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1208|             "expansion": {"include_callers": False, "include_callees": False, "include_imports": False,
  1209|                           "include_related_tests": False},
  1210|             "limits": {"max_estimated_tokens": max_tokens, "max_files": 12},
  1211|         })
  1212|         return rr.generate_packet_from_request(repo, out, req, name_override=name)
  1213| 
  1214|     # Sanity-bound the binary search: a generous budget must succeed.
  1215|     packet_path, _, err = _gen("hi", 2000)
  1216|     assert packet_path is not None, err
  1217| 
  1218|     lo, hi = 1, 2000
  1219|     while lo < hi:
  1220|         mid = (lo + hi) // 2
  1221|         packet_path, _, _ = _gen(f"probe{mid}", mid)
  1222|         if packet_path is not None:
  1223|             hi = mid
  1224|         else:
  1225|             lo = mid + 1
  1226|     min_success_tokens = hi
  1227| 
  1228|     # At the smallest budget that still succeeds, the packet's own
  1229|     # reported size must not exceed what was actually requested -- this
  1230|     # is exactly the invariant framing-charged-too-late violated (it
  1231|     # would report success here with estimated_tokens_used well above
  1232|     # min_success_tokens, since Tier-1 content had already spent as if
  1233|     # framing were free).
  1234|     packet_path, _, err = _gen("boundary", min_success_tokens)
  1235|     assert packet_path is not None, err
  1236|     sidecar = json.loads((out / "packets" / "packet_boundary.resolution.json").read_text(encoding="utf-8"))
  1237|     assert sidecar["estimated_tokens_used"] <= min_success_tokens
  1238| 
  1239|     # One token below that boundary must hard-abort -- not silently
  1240|     # succeed with a packet whose true size exceeds the requested cap.
  1241|     packet_path, _, err = _gen("under", min_success_tokens - 1)
  1242|     assert packet_path is None
  1243|     assert "do not fit" in (err or "")
```
