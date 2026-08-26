# Chunk of tools/probes/probe_line_styles.py

- Source relative path: `tools/probes/probe_line_styles.py`
- Chunk: 3 of 3
- Original line range: 973-1007
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: 37339fee28db23b0f48664b771d0c3bb9d107f1271f8c7ea137315329b786ee7
- Starts inside symbol: no
- Ends inside symbol: no

```
   973| 
   974| 
   975| if write_json:
   976|     try:
   977|         rvt_path = _safe(lambda: doc.PathName, None)
   978|         default_dir = None
   979| 
   980|         if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
   981|             default_dir = _safe(lambda: os.path.dirname(rvt_path), None)
   982| 
   983|         if not default_dir:
   984|             default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()
   985| 
   986|         date_stamp = datetime.now().strftime("%Y-%m-%d")
   987|         fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)
   988| 
   989|         target_dir = out_path if out_path else default_dir
   990|         target_path = os.path.join(target_dir, fixed_name)
   991| 
   992|         if target_dir and not os.path.exists(target_dir):
   993|             os.makedirs(target_dir)
   994| 
   995|         with open(target_path, "w") as f:
   996|             json.dump(_probe_wrap("line_styles", OUT_payload), f, indent=2, sort_keys=True)
   997| 
   998|         file_written = target_path
   999| 
  1000|     except Exception as ex:
  1001|         write_error = "{}: {}".format(type(ex).__name__, ex)
  1002| 
  1003| OUT_payload[0]["file_written"] = file_written
  1004| if write_error:
  1005|     OUT_payload[0]["file_write_error"] = write_error
  1006| 
  1007| OUT = OUT_payload
```
