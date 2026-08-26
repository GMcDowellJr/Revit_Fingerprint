# Chunk of tools/probes/probe_phase_graphics.py

- Source relative path: `tools/probes/probe_phase_graphics.py`
- Chunk: 3 of 3
- Original line range: 987-1038
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _probe_wrap
- Source SHA-256: 7c3277cf7d241a99aedfa2014cd4c5b4c1c314e89283d9945dd9c402f059fbe9
- Starts inside symbol: no
- Ends inside symbol: no

```
   987| 
   988| _PROBE_RUN_ID = _probe_run_id()
   989| _PROBE_REVIT_VERSION = _probe_revit_version() or "unknown"
   990| 
   991| def _probe_wrap(domain, out_payload):
   992|     return {
   993|         "run_metadata": {
   994|             "run_id": _PROBE_RUN_ID,
   995|             "extraction_date": datetime.now().isoformat(),
   996|             "revit_version": _PROBE_REVIT_VERSION,
   997|             "tool_version": None,
   998|             "document": _probe_document_identity(),
   999|             "source": "single_probe",
  1000|             "probe": domain,
  1001|         },
  1002|         "domains": {domain: out_payload},
  1003|     }
  1004| 
  1005| 
  1006| if write_json:
  1007|     try:
  1008|         rvt_path = _safe(lambda: doc.PathName, None)
  1009|         default_dir = None
  1010| 
  1011|         if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
  1012|             default_dir = _safe(lambda: os.path.dirname(rvt_path), None)
  1013| 
  1014|         if not default_dir:
  1015|             default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()
  1016| 
  1017|         date_stamp = datetime.now().strftime("%Y-%m-%d")
  1018|         fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)
  1019| 
  1020|         target_dir = out_path if out_path else default_dir
  1021|         target_path = os.path.join(target_dir, fixed_name)
  1022| 
  1023|         if target_dir and not os.path.exists(target_dir):
  1024|             os.makedirs(target_dir)
  1025| 
  1026|         with open(target_path, "w") as f:
  1027|             json.dump(_probe_wrap("phase_graphics", OUT_payload), f, indent=2, sort_keys=True)
  1028| 
  1029|         file_written = target_path
  1030| 
  1031|     except Exception as ex:
  1032|         write_error = "{}: {}".format(type(ex).__name__, ex)
  1033| 
  1034| OUT_payload[0]["file_written"] = file_written
  1035| if write_error:
  1036|     OUT_payload[0]["file_write_error"] = write_error
  1037| 
  1038| OUT = OUT_payload
```
