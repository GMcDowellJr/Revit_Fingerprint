# Chunk of tools/probes/probe_fill_patterns.py

- Source relative path: `tools/probes/probe_fill_patterns.py`
- Chunk: 3 of 3
- Original line range: 1026-1079
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _probe_wrap
- Source SHA-256: b7e1557e7ca19327a8137f4deb5ab42ac2779f1fdaf52c22e2c857bbd8e6f712
- Starts inside symbol: no
- Ends inside symbol: no

```
  1026| def _probe_wrap(domain, out_payload):
  1027|     return {
  1028|         "run_metadata": {
  1029|             "run_id": _PROBE_RUN_ID,
  1030|             "extraction_date": datetime.now().isoformat(),
  1031|             "revit_version": _PROBE_REVIT_VERSION,
  1032|             "tool_version": None,
  1033|             "document": _probe_document_identity(),
  1034|             "source": "single_probe",
  1035|             "probe": domain,
  1036|         },
  1037|         "domains": {domain: out_payload},
  1038|     }
  1039| 
  1040| 
  1041| if write_json:
  1042|     try:
  1043|         # Choose default directory: RVT folder if possible, else temp
  1044|         rvt_path = _safe(lambda: doc.PathName, None)
  1045|         default_dir = None
  1046| 
  1047|         if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
  1048|             try:
  1049|                 default_dir = os.path.dirname(rvt_path)
  1050|             except:
  1051|                 default_dir = None
  1052| 
  1053|         if not default_dir:
  1054|             default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()
  1055| 
  1056|         date_stamp = datetime.now().strftime("%Y-%m-%d")
  1057|         fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)
  1058| 
  1059|         # IN[5] is treated as an output directory (not a filename)
  1060|         target_dir = out_path if out_path else default_dir
  1061|         target_path = os.path.join(target_dir, fixed_name)
  1062| 
  1063|         if target_dir and not os.path.exists(target_dir):
  1064|             os.makedirs(target_dir)
  1065| 
  1066|         with open(target_path, "w") as f:
  1067|             json.dump(_probe_wrap("fill_patterns", OUT_payload), f, indent=2, sort_keys=True)
  1068| 
  1069|         file_written = target_path
  1070| 
  1071|     except Exception as ex:
  1072|         write_error = "{}: {}".format(type(ex).__name__, ex)
  1073| 
  1074| # Attach write metadata to inventory header (keeps OUT shape stable)
  1075| OUT_payload[0]["file_written"] = file_written
  1076| if write_error:
  1077|     OUT_payload[0]["file_write_error"] = write_error
  1078| 
  1079| OUT = OUT_payload
```
