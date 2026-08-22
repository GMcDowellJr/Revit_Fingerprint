# Chunk of dev_tools/repo_context/tests/test_packet_from_request.py

- Source relative path: `dev_tools/repo_context/tests/test_packet_from_request.py`
- Chunk: 3 of 3
- Original line range: 1019-1082
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_selector_resolution_report_is_charged_against_budget, test_related_test_expansion_respects_global_max_files, test_name_override_controls_output_filename
- Source SHA-256: df517eeb144275c222d2e91539a9089bdf2618474d39d425f1db5b88a6702bfa
- Starts inside symbol: no
- Ends inside symbol: no

```
  1019| def test_selector_resolution_report_is_charged_against_budget(repo, out):
  1020|     # Regression: the "Selector resolution report" section (one entry per
  1021|     # requested selector, however many) was appended without any budget
  1022|     # accounting, so a request naming hundreds of missing/ambiguous
  1023|     # selectors could produce a large packet while reporting ~0 estimated
  1024|     # tokens used under a tiny limits.max_estimated_tokens.
  1025|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1026|     _scan(repo, out)
  1027|     missing_files = [f"missing_{i}.py" for i in range(200)]
  1028|     req = _request(out, "req.json", {
  1029|         "schema_version": "1.0", "question": "q",
  1030|         "selectors": {"files": missing_files, "symbols": [], "search_terms": [], "lines": []},
  1031|         "limits": {"max_estimated_tokens": 1, "max_files": 500},
  1032|     })
  1033|     result = _packet(repo, out, req)
  1034|     assert result.returncode == 0, result.stderr
  1035|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1036|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1037|     # With a 4-character budget, almost nothing should have rendered --
  1038|     # certainly not a full 200-entry resolution report. The fixed header
  1039|     # framing is itself now charged (a separate fix), so the reported
  1040|     # figure is the framing's own honest cost, not exactly 0 -- but it
  1041|     # must stay small and, above all, must not be a wild understatement
  1042|     # of the packet's actual size the way ~0 tokens for an 18KB packet
  1043|     # was before either fix.
  1044|     assert len(text) < 2000
  1045|     assert text.count("missing_") < 200
  1046|     assert sidecar["estimated_tokens_used"] * 4 >= len(text) * 0.5
  1047| 
  1048| 
  1049| def test_related_test_expansion_respects_global_max_files(repo, out):
  1050|     # Regression: related-test expansion appended directly to focus_files
  1051|     # under a hard-coded 10,000 ceiling instead of going through the same
  1052|     # note_focus_file() gate as every other tier, so it could silently
  1053|     # exceed limits.max_files.
  1054|     write_files(repo, {
  1055|         "core/a.py": "def f():\n    return 1\n",
  1056|         "tests/test_a.py": "from core.a import f\n\n\ndef test_f():\n    assert f() == 1\n",
  1057|     })
  1058|     _scan(repo, out)
  1059|     req = _request(out, "req.json", {
  1060|         "schema_version": "1.0", "question": "q",
  1061|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1062|         "expansion": {"include_related_tests": True},
  1063|         "limits": {"max_estimated_tokens": 12000, "max_files": 1},
  1064|     })
  1065|     result = _packet(repo, out, req)
  1066|     assert result.returncode == 0, result.stderr
  1067|     sidecar = json.loads((out / "packets" / "packet_req.resolution.json").read_text(encoding="utf-8"))
  1068|     assert sidecar["focus_files"] == ["core/a.py"]
  1069|     text = (out / "packets" / "packet_req.md").read_text(encoding="utf-8")
  1070|     assert "beyond limits.max_files" in text
  1071| 
  1072| 
  1073| def test_name_override_controls_output_filename(repo, out):
  1074|     write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
  1075|     _scan(repo, out)
  1076|     req = _request(out, "req.json", {
  1077|         "schema_version": "1.0", "question": "q",
  1078|         "selectors": {"files": ["core/a.py"], "symbols": [], "search_terms": [], "lines": []},
  1079|     })
  1080|     result = _packet(repo, out, req, extra=["--name", "custom_stem"])
  1081|     assert result.returncode == 0, result.stderr
  1082|     assert (out / "packets" / "packet_custom_stem.md").exists()
```
