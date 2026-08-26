# Chunk of tools/probes/probe_line_patterns.py

- Source relative path: `tools/probes/probe_line_patterns.py`
- Chunk: 3 of 3
- Original line range: 990-1105
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _probe_revit_version, _probe_document_identity, _probe_run_id, _probe_wrap
- Source SHA-256: 09adef2ca571518818011587b6cc8376cc961624aa6e3b91af87f79082b3d74b
- Starts inside symbol: no
- Ends inside symbol: no

```
   990| 
   991| _reflection_records_0 = _run_reflection_sweep(selected, "LinePatternElement", "line_patterns")
   992| _reflection_records = _reflection_records_0
   993| 
   994| # Assemble labeled output payload
   995| OUT_payload = [
   996|     {
   997|         "kind": "reflection",
   998|         "domain": "line_patterns",
   999|         "records": _reflection_records
  1000|     },
  1001|     {
  1002|         "kind": "inventory",
  1003|         "domain": "line_patterns",
  1004|         "records": param_inventory
  1005|     },
  1006|     {
  1007|         "kind": "crosswalk",
  1008|         "domain": "line_patterns",
  1009|         "records": optional_crosswalk
  1010|     }
  1011| ]
  1012| 
  1013| # Optional: write to JSON for future reference (valid JSON, stable order)
  1014| file_written = None
  1015| write_error = None
  1016| 
  1017| # -------------------------
  1018| # Unified run metadata (release-separated, not date-filename-separated)
  1019| # -------------------------
  1020| # extraction_date lives as JSON metadata, not as a filename token; the
  1021| # filename groups by Revit release (revit_version) plus an opaque run_id so
  1022| # repeated runs don't collide. See tools/probes/build_probe_inventory.py,
  1023| # which consumes this shape directly.
  1024| 
  1025| import uuid as _uuid_mod
  1026| 
  1027| def _probe_revit_version():
  1028|     try:
  1029|         _uiapp = DocumentManager.Instance.CurrentUIApplication
  1030|         _app = _uiapp.Application if _uiapp is not None else None
  1031|         v = _safe(lambda: _app.VersionNumber, None)
  1032|         return str(v) if v else None
  1033|     except:
  1034|         return None
  1035| 
  1036| def _probe_document_identity():
  1037|     return {
  1038|         "title": _safe(lambda: doc.Title, None),
  1039|         "path_name": _safe(lambda: doc.PathName, None),
  1040|         "is_workshared": _safe(lambda: bool(doc.IsWorkshared), None),
  1041|     }
  1042| 
  1043| def _probe_run_id():
  1044|     try:
  1045|         return datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + _uuid_mod.uuid4().hex[:6]
  1046|     except:
  1047|         return _uuid_mod.uuid4().hex[:12]
  1048| 
  1049| _PROBE_RUN_ID = _probe_run_id()
  1050| _PROBE_REVIT_VERSION = _probe_revit_version() or "unknown"
  1051| 
  1052| def _probe_wrap(domain, out_payload):
  1053|     return {
  1054|         "run_metadata": {
  1055|             "run_id": _PROBE_RUN_ID,
  1056|             "extraction_date": datetime.now().isoformat(),
  1057|             "revit_version": _PROBE_REVIT_VERSION,
  1058|             "tool_version": None,
  1059|             "document": _probe_document_identity(),
  1060|             "source": "single_probe",
  1061|             "probe": domain,
  1062|         },
  1063|         "domains": {domain: out_payload},
  1064|     }
  1065| 
  1066| 
  1067| if write_json:
  1068|     try:
  1069|         # Choose default directory: RVT folder if possible, else temp
  1070|         rvt_path = _safe(lambda: doc.PathName, None)
  1071|         default_dir = None
  1072| 
  1073|         if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
  1074|             try:
  1075|                 default_dir = os.path.dirname(rvt_path)
  1076|             except:
  1077|                 default_dir = None
  1078| 
  1079|         if not default_dir:
  1080|             default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()
  1081| 
  1082|         date_stamp = datetime.now().strftime("%Y-%m-%d")
  1083|         fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)
  1084| 
  1085|         # IN[4] is treated as an output directory (not a filename)
  1086|         target_dir = out_path if out_path else default_dir
  1087|         target_path = os.path.join(target_dir, fixed_name)
  1088| 
  1089|         if target_dir and not os.path.exists(target_dir):
  1090|             os.makedirs(target_dir)
  1091| 
  1092|         with open(target_path, "w") as f:
  1093|             json.dump(_probe_wrap("line_patterns", OUT_payload), f, indent=2, sort_keys=True)
  1094| 
  1095|         file_written = target_path
  1096| 
  1097|     except Exception as ex:
  1098|         write_error = "{}: {}".format(type(ex).__name__, ex)
  1099| 
  1100| # Attach write metadata to inventory header (keeps OUT shape stable)
  1101| OUT_payload[0]["file_written"] = file_written
  1102| if write_error:
  1103|     OUT_payload[0]["file_write_error"] = write_error
  1104| 
  1105| OUT = OUT_payload
```
