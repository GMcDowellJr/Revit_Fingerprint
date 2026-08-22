# Chunk of tools/probes/probe_dimension_types.py

- Source relative path: `tools/probes/probe_dimension_types.py`
- Chunk: 3 of 3
- Original line range: 986-1160
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _find_tick_param, _probe_revit_version, _probe_document_identity, _probe_run_id, _probe_wrap
- Source SHA-256: c9e2998c9a3c2f218a004c3c8351e8c52a44975202334b7fe5365c73fa869cc7
- Starts inside symbol: no
- Ends inside symbol: no

```
   986| def _find_tick_param(dt):
   987|     for cand in DIM_TICK_PARAM_CANDIDATES:
   988|         p = _safe(lambda: dt.LookupParameter(cand), None)
   989|         if p is not None:
   990|             return (cand, p)
   991|     return (None, None)
   992| 
   993| # Keep crosswalk compact: one representative DimensionType per distinct arrowhead id
   994| seen_arrowhead_ids = set()
   995| 
   996| if enable_crosswalk:
   997|     # Optional extra input: max crosswalk rows to emit (default 50)
   998|     crosswalk_limit = IN[5] if len(IN) > 5 and IN[5] is not None else 50
   999| 
  1000|     for dt in selected:
  1001|         if len(optional_crosswalk) >= int(crosswalk_limit):
  1002|             break
  1003| 
  1004|         row = {
  1005|             "dim_type.id": _safe(lambda: dt.Id.IntegerValue, None),
  1006|             "dim_type.name": _safe(lambda: _safe_type_name(dt), None),
  1007|             "dim_type.shape": None,
  1008|             "tick_param.matched_name": None,
  1009|             "tick_param": None,
  1010|             "arrowhead.resolved": False,
  1011|             "arrowhead.type_id": None,
  1012|             "arrowhead.name": None
  1013|         }
  1014| 
  1015|         shape_key, shape_label, shape_family, _ = _get_dim_shape_info(dt)
  1016|         row["dim_type.shape"] = shape_key
  1017|         row["dim_type.shape_label"] = shape_label
  1018|         row["dim_type.shape_family"] = shape_family
  1019| 
  1020|         matched, p = _find_tick_param(dt)
  1021|         row["tick_param.matched_name"] = matched
  1022|         row["tick_param"] = _format_param_contract(p)
  1023| 
  1024|         if row["tick_param"]["storage"] != "ElementId" or row["tick_param"]["raw"] is None:
  1025|             continue
  1026| 
  1027|         ah_id = int(row["tick_param"]["raw"])
  1028|         if ah_id in seen_arrowhead_ids:
  1029|             continue
  1030| 
  1031|         row["arrowhead.type_id"] = ah_id
  1032| 
  1033|         ref = _safe(lambda: doc.GetElement(ElementId(ah_id)), None)
  1034|         if ref is not None:
  1035|             row["arrowhead.name"] = _safe(lambda: _safe_type_name(ref), None)
  1036| 
  1037|         row["arrowhead.resolved"] = True if row["arrowhead.name"] is not None else False
  1038| 
  1039|         if not row["arrowhead.resolved"]:
  1040|             continue
  1041| 
  1042|         seen_arrowhead_ids.add(ah_id)
  1043|         optional_crosswalk.append(row)
  1044| 
  1045| 
  1046| # -------------------------
  1047| # Assemble labeled output payload
  1048| # -------------------------
  1049| 
  1050| OUT_payload = [
  1051|     {
  1052|         "kind": "inventory",
  1053|         "domain": "dimension_types",
  1054|         "records": param_inventory,
  1055|         "diagnostics": {
  1056|             "selected_type_sample_count": len(selected),
  1057|             "discovered_type_count": len(dim_types),
  1058|             "discovery_notes": discovery_notes,
  1059|             "format_surface_member_samples": synth_member_samples[:5],
  1060|             "format_signature_crosswalk_count": len(synth_crosswalk_rows)
  1061|         }
  1062|     },
  1063|     {
  1064|         "kind": "crosswalk",
  1065|         "domain": "dimension_types",
  1066|         "records": optional_crosswalk
  1067|     }
  1068| ]
  1069| 
  1070| # Optional: write to JSON for future reference (valid JSON, stable order)
  1071| file_written = None
  1072| write_error = None
  1073| 
  1074| # -------------------------
  1075| # Unified run metadata (release-separated, not date-filename-separated)
  1076| # -------------------------
  1077| # extraction_date lives as JSON metadata, not as a filename token; the
  1078| # filename groups by Revit release (revit_version) plus an opaque run_id so
  1079| # repeated runs don't collide. See tools/probes/build_probe_inventory.py,
  1080| # which consumes this shape directly.
  1081| 
  1082| import uuid as _uuid_mod
  1083| 
  1084| def _probe_revit_version():
  1085|     try:
  1086|         _uiapp = DocumentManager.Instance.CurrentUIApplication
  1087|         _app = _uiapp.Application if _uiapp is not None else None
  1088|         v = _safe(lambda: _app.VersionNumber, None)
  1089|         return str(v) if v else None
  1090|     except:
  1091|         return None
  1092| 
  1093| def _probe_document_identity():
  1094|     return {
  1095|         "title": _safe(lambda: doc.Title, None),
  1096|         "path_name": _safe(lambda: doc.PathName, None),
  1097|         "is_workshared": _safe(lambda: bool(doc.IsWorkshared), None),
  1098|     }
  1099| 
  1100| def _probe_run_id():
  1101|     try:
  1102|         return datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + _uuid_mod.uuid4().hex[:6]
  1103|     except:
  1104|         return _uuid_mod.uuid4().hex[:12]
  1105| 
  1106| _PROBE_RUN_ID = _probe_run_id()
  1107| _PROBE_REVIT_VERSION = _probe_revit_version() or "unknown"
  1108| 
  1109| def _probe_wrap(domain, out_payload):
  1110|     return {
  1111|         "run_metadata": {
  1112|             "run_id": _PROBE_RUN_ID,
  1113|             "extraction_date": datetime.now().isoformat(),
  1114|             "revit_version": _PROBE_REVIT_VERSION,
  1115|             "tool_version": None,
  1116|             "document": _probe_document_identity(),
  1117|             "source": "single_probe",
  1118|             "probe": domain,
  1119|         },
  1120|         "domains": {domain: out_payload},
  1121|     }
  1122| 
  1123| 
  1124| if write_json:
  1125|     try:
  1126|         rvt_path = _safe(lambda: doc.PathName, None)
  1127|         default_dir = None
  1128| 
  1129|         if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
  1130|             try:
  1131|                 default_dir = os.path.dirname(rvt_path)
  1132|             except:
  1133|                 default_dir = None
  1134| 
  1135|         if not default_dir:
  1136|             default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()
  1137| 
  1138|         date_stamp = datetime.now().strftime("%Y-%m-%d")
  1139|         fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)
  1140| 
  1141|         target_dir = out_path if out_path else default_dir
  1142|         target_path = os.path.join(target_dir, fixed_name)
  1143| 
  1144|         if target_dir and not os.path.exists(target_dir):
  1145|             os.makedirs(target_dir)
  1146| 
  1147|         with open(target_path, "w") as f:
  1148|             json.dump(_probe_wrap("dimension_types", OUT_payload), f, indent=2, sort_keys=True)
  1149| 
  1150|         file_written = target_path
  1151| 
  1152|     except Exception as ex:
  1153|         write_error = "{}: {}".format(type(ex).__name__, ex)
  1154| 
  1155| # Attach write metadata to inventory header (keeps OUT shape stable)
  1156| OUT_payload[0]["file_written"] = file_written
  1157| if write_error:
  1158|     OUT_payload[0]["file_write_error"] = write_error
  1159| 
  1160| OUT = OUT_payload
```
