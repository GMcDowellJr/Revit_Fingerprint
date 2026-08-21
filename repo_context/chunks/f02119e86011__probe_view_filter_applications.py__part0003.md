# Chunk of tools/probes/probe_view_filter_applications.py

- Source relative path: `tools/probes/probe_view_filter_applications.py`
- Chunk: 3 of 3
- Original line range: 952-1100
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _run_reflection_sweep, _probe_revit_version, _probe_document_identity, _probe_run_id, _probe_wrap
- Source SHA-256: 54662f061e5f0ad2fd398cf9882aff71bf5cf2f3312ded3f562bef5b2eabfb1b
- Starts inside symbol: no
- Ends inside symbol: no

```
   952| def _run_reflection_sweep(sample_objs, type_label, domain_name, max_members=200):
   953|     idx = {}
   954|     for obj in sample_objs:
   955|         if obj is None:
   956|             continue
   957|         for member_kind, name in _reflect_member_names(obj)[:max_members]:
   958|             ok, raw_v, err = _reflect_try_get(obj, member_kind, name)
   959|             key = "refl.{}.{}".format(type_label, name)
   960|             if key not in idx:
   961|                 idx[key] = {
   962|                     "domain": domain_name, "member_key": key, "member_kind": member_kind,
   963|                     "type_label": type_label, "example": None, "example_error": None,
   964|                     "ok_count": 0, "error_count": 0, "unique_value_count": 0, "_seen": set(),
   965|                 }
   966|             e = idx[key]
   967|             if not ok:
   968|                 e["error_count"] += 1
   969|                 if e["example_error"] is None and err:
   970|                     e["example_error"] = err
   971|                 continue
   972|             contract = _reflect_contract(raw_v)
   973|             e["ok_count"] += 1
   974|             sig = (str(contract.get("storage")), str(contract.get("norm")))
   975|             if sig not in e["_seen"]:
   976|                 e["_seen"].add(sig)
   977|                 e["unique_value_count"] += 1
   978|             if e["example"] is None or (contract.get("display") is not None and e["example"].get("display") is None):
   979|                 e["example"] = contract
   980|     records = []
   981|     for key in sorted(idx.keys()):
   982|         e = idx[key]
   983|         records.append({
   984|             "domain": e["domain"], "member_key": e["member_key"], "member_kind": e["member_kind"],
   985|             "type_label": e["type_label"], "example": e["example"], "example_error": e["example_error"],
   986|             "observed": {"ok_count": e["ok_count"], "error_count": e["error_count"], "unique_value_count": e["unique_value_count"]},
   987|         })
   988|     return records
   989| 
   990| _reflection_records_0 = _run_reflection_sweep(_reflect_ogs_samples, "OverrideGraphicSettings", "view_filter_applications")
   991| _reflection_records_1 = _run_reflection_sweep(_reflect_filter_samples, "ParameterFilterElement", "view_filter_applications")
   992| _reflection_records = _reflection_records_0 + _reflection_records_1
   993| 
   994| OUT_payload = [
   995|     {
   996|         "kind": "reflection",
   997|         "domain": "view_filter_applications",
   998|         "records": _reflection_records
   999|     },
  1000|     {
  1001|         "kind": "inventory",
  1002|         "domain": "view_filter_applications",
  1003|         "records": param_inventory
  1004|     },
  1005|     {
  1006|         "kind": "crosswalk",
  1007|         "domain": "view_filter_applications",
  1008|         "records": optional_crosswalk
  1009|     }
  1010| ]
  1011| 
  1012| file_written = None
  1013| write_error = None
  1014| 
  1015| # -------------------------
  1016| # Unified run metadata (release-separated, not date-filename-separated)
  1017| # -------------------------
  1018| # extraction_date lives as JSON metadata, not as a filename token; the
  1019| # filename groups by Revit release (revit_version) plus an opaque run_id so
  1020| # repeated runs don't collide. See tools/probes/build_probe_inventory.py,
  1021| # which consumes this shape directly.
  1022| 
  1023| import uuid as _uuid_mod
  1024| 
  1025| def _probe_revit_version():
  1026|     try:
  1027|         _uiapp = DocumentManager.Instance.CurrentUIApplication
  1028|         _app = _uiapp.Application if _uiapp is not None else None
  1029|         v = _safe(lambda: _app.VersionNumber, None)
  1030|         return str(v) if v else None
  1031|     except:
  1032|         return None
  1033| 
  1034| def _probe_document_identity():
  1035|     return {
  1036|         "title": _safe(lambda: doc.Title, None),
  1037|         "path_name": _safe(lambda: doc.PathName, None),
  1038|         "is_workshared": _safe(lambda: bool(doc.IsWorkshared), None),
  1039|     }
  1040| 
  1041| def _probe_run_id():
  1042|     try:
  1043|         return datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + _uuid_mod.uuid4().hex[:6]
  1044|     except:
  1045|         return _uuid_mod.uuid4().hex[:12]
  1046| 
  1047| _PROBE_RUN_ID = _probe_run_id()
  1048| _PROBE_REVIT_VERSION = _probe_revit_version() or "unknown"
  1049| 
  1050| def _probe_wrap(domain, out_payload):
  1051|     return {
  1052|         "run_metadata": {
  1053|             "run_id": _PROBE_RUN_ID,
  1054|             "extraction_date": datetime.now().isoformat(),
  1055|             "revit_version": _PROBE_REVIT_VERSION,
  1056|             "tool_version": None,
  1057|             "document": _probe_document_identity(),
  1058|             "source": "single_probe",
  1059|             "probe": domain,
  1060|         },
  1061|         "domains": {domain: out_payload},
  1062|     }
  1063| 
  1064| 
  1065| if write_json:
  1066|     try:
  1067|         rvt_path = _safe(lambda: doc.PathName, None)
  1068|         default_dir = None
  1069| 
  1070|         if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
  1071|             try:
  1072|                 default_dir = os.path.dirname(rvt_path)
  1073|             except:
  1074|                 default_dir = None
  1075| 
  1076|         if not default_dir:
  1077|             default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()
  1078| 
  1079|         date_stamp = datetime.now().strftime("%Y-%m-%d")
  1080|         fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)
  1081| 
  1082|         target_dir = out_path if out_path else default_dir
  1083|         target_path = os.path.join(target_dir, fixed_name)
  1084| 
  1085|         if target_dir and not os.path.exists(target_dir):
  1086|             os.makedirs(target_dir)
  1087| 
  1088|         with open(target_path, "w") as f:
  1089|             json.dump(_probe_wrap("view_filter_applications", OUT_payload), f, indent=2, sort_keys=True)
  1090| 
  1091|         file_written = target_path
  1092| 
  1093|     except Exception as ex:
  1094|         write_error = "{}: {}".format(type(ex).__name__, ex)
  1095| 
  1096| OUT_payload[0]["file_written"] = file_written
  1097| if write_error:
  1098|     OUT_payload[0]["file_write_error"] = write_error
  1099| 
  1100| OUT = OUT_payload
```
