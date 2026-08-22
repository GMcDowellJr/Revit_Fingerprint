# Chunk of dev_tools/repo_context/tests/test_packet_from_request.py

- Source relative path: `dev_tools/repo_context/tests/test_packet_from_request.py`
- Chunk: 3 of 3
- Original line range: 1032-1154
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_include_graphify_withheld_when_current_commit_unavailable, test_selector_resolution_report_is_charged_against_budget, test_file_level_expansions_render_once_per_file_not_per_symbol, test_related_test_expansion_respects_global_max_files, test_name_override_controls_output_filename
- Source SHA-256: 759d9ce2ca0219228b05d56db69e1b045fd96066288de392f13f77203a94949c
- Starts inside symbol: no
- Ends inside symbol: no

```
  1032| def test_include_graphify_withheld_when_current_commit_unavailable(repo, out):
  1033|     # Regression: when the scanned tree isn't a git repository (no HEAD
  1034|     # commit to check against), a graphify-out/graph.json with any
  1035|     # built_at_commit used to be accepted unconditionally instead of
  1036|     # being withheld -- revision alignment can't be proven either way.
  1037|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1038|     graph = {
  1039|         "built_at_commit": "deadbeef",
  1040|         "nodes": [{"source_file": "core/a.py", "community": 5, "community_name": "Widgets"}],
  1041|     }
  1042|     (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
  1043|     (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
  1044|     _scan(repo, out)  # no git init -- current commit is unavailable
  1045|     req = _request(out, "req.json", {
  1046|         "schema_version": "1.0", "question": "q",
  1047|         "selectors": {"files": [], "symbols": [{"name": "f", "file": "core/a.py"}], "search_terms": [], "lines": []},
  1048|         "expansion": {"include_graphify": True},
  1049|     })
  1050|     result = _packet(repo, out, req)
  1051|     assert result.returncode == 0, result.stderr
  1052|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1053|     assert "graphify_expansion" not in text
  1054|     assert "revision alignment cannot be proven" in text
  1055| 
  1056| 
  1057| def test_selector_resolution_report_is_charged_against_budget(repo, out):
  1058|     # Regression: the "Selector resolution report" section (one entry per
  1059|     # requested selector, however many) was appended without any budget
  1060|     # accounting, so a request naming hundreds of missing/ambiguous
  1061|     # selectors could produce a large packet while reporting ~0 estimated
  1062|     # tokens used under a tiny limits.max_estimated_tokens.
  1063|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1064|     _scan(repo, out)
  1065|     missing_files = [f"missing_{i}.py" for i in range(200)]
  1066|     req = _request(out, "req.json", {
  1067|         "schema_version": "1.0", "question": "q",
  1068|         "selectors": {"files": missing_files, "symbols": [], "search_terms": [], "lines": []},
  1069|         "limits": {"max_estimated_tokens": 1, "max_files": 500},
  1070|     })
  1071|     result = _packet(repo, out, req)
  1072|     assert result.returncode == 0, result.stderr
  1073|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1074|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1075|     # With a 4-character budget, almost nothing should have rendered --
  1076|     # certainly not a full 200-entry resolution report. The fixed header
  1077|     # framing is itself now charged (a separate fix), so the reported
  1078|     # figure is the framing's own honest cost, not exactly 0 -- but it
  1079|     # must stay small and, above all, must not be a wild understatement
  1080|     # of the packet's actual size the way ~0 tokens for an 18KB packet
  1081|     # was before either fix.
  1082|     assert len(text) < 2000
  1083|     assert text.count("missing_") < 200
  1084|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
  1085| 
  1086| 
  1087| def test_file_level_expansions_render_once_per_file_not_per_symbol(repo, out):
  1088|     # Regression: _symbol_expansion() rendered imports/related-tests/
  1089|     # Graphify-peer sections (all keyed by the containing *file*, not the
  1090|     # symbol) on every call -- but an explicit file selector called it
  1091|     # once per top-level symbol in that file, so a multi-function file
  1092|     # got its "Internal imports of X" / "Related tests for X" sections
  1093|     # duplicated once per function instead of appearing once.
  1094|     write_files(repo, {
  1095|         "core/a.py": (
  1096|             "from core.dep import helper\n\n\n"
  1097|             "def f():\n    return helper()\n\n\n"
  1098|             "def g():\n    return helper()\n\n\n"
  1099|             "def h():\n    return helper()\n"
  1100|         ),
  1101|         "core/dep.py": "def helper():\n    return 1\n",
  1102|         "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 1\n",
  1103|     })
  1104|     _scan(repo, out)
  1105|     req = _request(out, "req.json", {
  1106|         "schema_version": "1.0", "question": "q",
  1107|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1108|         "expansion": {"include_callers": False, "include_callees": False, "include_imports": True,
  1109|                       "include_related_tests": True, "include_graphify": False},
  1110|         "limits": {"max_estimated_tokens": 12000, "max_files": 12},
  1111|     })
  1112|     result = _packet(repo, out, req)
  1113|     assert result.returncode == 0, result.stderr
  1114|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1115|     # Three top-level functions in core/a.py, but these file-level
  1116|     # sections must appear exactly once, not three times.
  1117|     assert text.count("Internal imports of `core/a.py`") == 1
  1118|     assert text.count("Related tests for `core/a.py`") == 1
  1119| 
  1120| 
  1121| def test_related_test_expansion_respects_global_max_files(repo, out):
  1122|     # Regression: related-test expansion appended directly to focus_files
  1123|     # under a hard-coded 10,000 ceiling instead of going through the same
  1124|     # note_focus_file() gate as every other tier, so it could silently
  1125|     # exceed limits.max_files.
  1126|     write_files(repo, {
  1127|         "core/a.py": "def f():\n    return 1\n",
  1128|         "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 1\n",
  1129|     })
  1130|     _scan(repo, out)
  1131|     req = _request(out, "req.json", {
  1132|         "schema_version": "1.0", "question": "q",
  1133|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1134|         "expansion": {"include_related_tests": True},
  1135|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
  1136|     })
  1137|     result = _packet(repo, out, req)
  1138|     assert result.returncode == 0, result.stderr
  1139|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1140|     assert sidecar["focus_files"] == ["core/a.py"]
  1141|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1142|     assert "beyond limits.max_files" in text
  1143| 
  1144| 
  1145| def test_name_override_controls_output_filename(repo, out):
  1146|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1147|     _scan(repo, out)
  1148|     req = _request(out, "req.json", {
  1149|         "schema_version": "1.0", "question": "q",
  1150|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1151|     })
  1152|     result = _packet(repo, out, req, extra=["--name", "custom_stem"])
  1153|     assert result.returncode == 0, result.stderr
  1154|     assert (out / "packets" / "packet_custom_stem.md").exists()
```
