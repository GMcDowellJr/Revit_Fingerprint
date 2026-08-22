# Chunk of tools/probes/probe_text_types.py

- Source relative path: `tools/probes/probe_text_types.py`
- Chunk: 3 of 3
- Original line range: 906-1125
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _reflect_contract, _run_reflection_sweep, _probe_revit_version, _probe_document_identity, _probe_run_id, _probe_wrap
- Source SHA-256: 87bfb05b1c55eb50cd88eaad88e7bb9c9a5d9f1f5e0657eb612ffc30f9ac7ced
- Starts inside symbol: no
- Ends inside symbol: no

```
   906| def _reflect_contract(raw_v):
   907|     if raw_v is None:
   908|         return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
   909|     if raw_v is _METHOD_NOT_INVOKED_SENTINEL:
   910|         # Identity check ("is"), not equality -- _METHOD_NOT_INVOKED_SENTINEL is a
   911|         # unique object(), never a string, specifically so a genuine reflected
   912|         # property or allowlisted-method return whose real value happens to be
   913|         # the literal text "<method not invoked>" cannot collide with this
   914|         # placeholder and get misclassified/dropped (flagged in PR #398 review:
   915|         # an earlier version of this check compared by value against a string
   916|         # constant, which had exactly that collision risk). Checked before
   917|         # isinstance(raw_v, str) specifically so it never reaches that branch.
   918|         return {"q": "not_invoked", "storage": "None", "raw": None, "display": None, "norm": None}
   919|     if isinstance(raw_v, bool):
   920|         return {"q": "ok", "storage": "Integer", "raw": int(raw_v), "display": str(raw_v), "norm": int(raw_v)}
   921|     if isinstance(raw_v, int):
   922|         return {"q": "ok", "storage": "Integer", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   923|     if isinstance(raw_v, float):
   924|         return {"q": "ok", "storage": "Double", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   925|     if isinstance(raw_v, str):
   926|         return {"q": "ok", "storage": "String", "raw": raw_v, "display": raw_v, "norm": raw_v}
   927|     try:
   928|         if hasattr(raw_v, "IntegerValue"):
   929|             iv = int(raw_v.IntegerValue)
   930|             return {"q": "ok", "storage": "ElementId", "raw": iv, "display": str(iv), "norm": iv}
   931|     except:
   932|         pass
   933|     try:
   934|         if hasattr(raw_v, "ToString"):
   935|             s = raw_v.ToString()
   936|             if s and "Autodesk.Revit" not in s and "System." not in s:
   937|                 return {"q": "ok", "storage": "None", "raw": None, "display": s, "norm": s}
   938|     except:
   939|         pass
   940|     try:
   941|         ids = []
   942|         saw_item = False
   943|         for item in raw_v:
   944|             saw_item = True
   945|             if not hasattr(item, "IntegerValue"):
   946|                 raise TypeError("non-ElementId item in collection")
   947|             ids.append(int(item.IntegerValue))
   948|         if not saw_item:
   949|             # An empty collection is vacuously "every item has .IntegerValue"
   950|             # -- there's nothing to fail the check against, so item-by-item
   951|             # duck-typing alone can never tell an empty ElementId collection
   952|             # (GetMonitoredLinkElementIds returning [] because a type has no
   953|             # monitored links) apart from an empty collection of anything
   954|             # else (GetEntitySchemaGuids -> IList<Guid>, GetSubelements ->
   955|             # IList<Subelement>, both returning [] because that instance
   956|             # happens to have zero). A CLR generic-type reflection check
   957|             # (raw_v.GetType().GetGenericArguments()) was tried here and
   958|             # found not to reliably discriminate types against a live
   959|             # Revit/pythonnet session (still produced the same false
   960|             # positives), so it was dropped rather than kept as an
   961|             # unreliable safety net. Per this project's fail-soft principle
   962|             # (never silently collapse distinct states), an empty collection
   963|             # of unconfirmed item type gets its own explicit q value instead
   964|             # of defaulting to "ok" (would reintroduce this exact bug) or
   965|             # bare "unsupported" (would make it indistinguishable from a
   966|             # totally opaque complex-object failure). storage stays "None"
   967|             # (not "ElementIdList") so find_crosswalk_candidates.py's
   968|             # _is_elementid_typed() correctly does not treat this as a
   969|             # reference candidate.
   970|             return {"q": "unsupported.empty_type_unconfirmed", "storage": "None", "raw": [], "display": "", "norm": ()}
   971|         disp = ",".join(str(i) for i in ids)
   972|         return {"q": "ok", "storage": "ElementIdList", "raw": ids, "display": disp, "norm": tuple(ids)}
   973|     except:
   974|         pass
   975|     return {"q": "unsupported", "storage": "None", "raw": None, "display": None, "norm": None}
   976| 
   977| def _run_reflection_sweep(sample_objs, type_label, domain_name, max_members=200):
   978|     idx = {}
   979|     for obj in sample_objs:
   980|         if obj is None:
   981|             continue
   982|         for member_kind, name in _reflect_member_names(obj)[:max_members]:
   983|             ok, raw_v, err = _reflect_try_get(obj, member_kind, name)
   984|             key = "refl.{}.{}".format(type_label, name)
   985|             if key not in idx:
   986|                 idx[key] = {
   987|                     "domain": domain_name, "member_key": key, "member_kind": member_kind,
   988|                     "type_label": type_label, "example": None, "example_error": None,
   989|                     "ok_count": 0, "error_count": 0, "unique_value_count": 0, "_seen": set(),
   990|                 }
   991|             e = idx[key]
   992|             if not ok:
   993|                 e["error_count"] += 1
   994|                 if e["example_error"] is None and err:
   995|                     e["example_error"] = err
   996|                 continue
   997|             contract = _reflect_contract(raw_v)
   998|             e["ok_count"] += 1
   999|             sig = (str(contract.get("storage")), str(contract.get("norm")))
  1000|             if sig not in e["_seen"]:
  1001|                 e["_seen"].add(sig)
  1002|                 e["unique_value_count"] += 1
  1003|             if e["example"] is None or (contract.get("display") is not None and e["example"].get("display") is None):
  1004|                 e["example"] = contract
  1005|     records = []
  1006|     for key in sorted(idx.keys()):
  1007|         e = idx[key]
  1008|         records.append({
  1009|             "domain": e["domain"], "member_key": e["member_key"], "member_kind": e["member_kind"],
  1010|             "type_label": e["type_label"], "example": e["example"], "example_error": e["example_error"],
  1011|             "observed": {"ok_count": e["ok_count"], "error_count": e["error_count"], "unique_value_count": e["unique_value_count"]},
  1012|         })
  1013|     return records
  1014| 
  1015| _reflection_records_0 = _run_reflection_sweep(selected, "TextNoteType", "text_types")
  1016| _reflection_records = _reflection_records_0
  1017| 
  1018| OUT_payload = [
  1019|     {
  1020|         "kind": "reflection",
  1021|         "domain": "text_types",
  1022|         "records": _reflection_records
  1023|     },
  1024|     {
  1025|         "kind": "inventory",
  1026|         "domain": "text_types",
  1027|         "records": param_inventory
  1028|     },
  1029|     {
  1030|         "kind": "crosswalk",
  1031|         "domain": "text_types",
  1032|         "records": optional_crosswalk
  1033|     }
  1034| ]
  1035| 
  1036| # Optional: write to JSON for future reference (valid JSON, stable order)
  1037| file_written = None
  1038| write_error = None
  1039| 
  1040| # -------------------------
  1041| # Unified run metadata (release-separated, not date-filename-separated)
  1042| # -------------------------
  1043| # extraction_date lives as JSON metadata, not as a filename token; the
  1044| # filename groups by Revit release (revit_version) plus an opaque run_id so
  1045| # repeated runs don't collide. See tools/probes/build_probe_inventory.py,
  1046| # which consumes this shape directly.
  1047| 
  1048| import uuid as _uuid_mod
  1049| 
  1050| def _probe_revit_version():
  1051|     try:
  1052|         _uiapp = DocumentManager.Instance.CurrentUIApplication
  1053|         _app = _uiapp.Application if _uiapp is not None else None
  1054|         v = _safe(lambda: _app.VersionNumber, None)
  1055|         return str(v) if v else None
  1056|     except:
  1057|         return None
  1058| 
  1059| def _probe_document_identity():
  1060|     return {
  1061|         "title": _safe(lambda: doc.Title, None),
  1062|         "path_name": _safe(lambda: doc.PathName, None),
  1063|         "is_workshared": _safe(lambda: bool(doc.IsWorkshared), None),
  1064|     }
  1065| 
  1066| def _probe_run_id():
  1067|     try:
  1068|         return datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + _uuid_mod.uuid4().hex[:6]
  1069|     except:
  1070|         return _uuid_mod.uuid4().hex[:12]
  1071| 
  1072| _PROBE_RUN_ID = _probe_run_id()
  1073| _PROBE_REVIT_VERSION = _probe_revit_version() or "unknown"
  1074| 
  1075| def _probe_wrap(domain, out_payload):
  1076|     return {
  1077|         "run_metadata": {
  1078|             "run_id": _PROBE_RUN_ID,
  1079|             "extraction_date": datetime.now().isoformat(),
  1080|             "revit_version": _PROBE_REVIT_VERSION,
  1081|             "tool_version": None,
  1082|             "document": _probe_document_identity(),
  1083|             "source": "single_probe",
  1084|             "probe": domain,
  1085|         },
  1086|         "domains": {domain: out_payload},
  1087|     }
  1088| 
  1089| 
  1090| if write_json:
  1091|     try:
  1092|         rvt_path = _safe(lambda: doc.PathName, None)
  1093|         default_dir = None
  1094| 
  1095|         if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
  1096|             try:
  1097|                 default_dir = os.path.dirname(rvt_path)
  1098|             except:
  1099|                 default_dir = None
  1100| 
  1101|         if not default_dir:
  1102|             default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()
  1103| 
  1104|         date_stamp = datetime.now().strftime("%Y-%m-%d")
  1105|         fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)
  1106| 
  1107|         target_dir = out_path if out_path else default_dir
  1108|         target_path = os.path.join(target_dir, fixed_name)
  1109| 
  1110|         if target_dir and not os.path.exists(target_dir):
  1111|             os.makedirs(target_dir)
  1112| 
  1113|         with open(target_path, "w") as f:
  1114|             json.dump(_probe_wrap("text_types", OUT_payload), f, indent=2, sort_keys=True)
  1115| 
  1116|         file_written = target_path
  1117| 
  1118|     except Exception as ex:
  1119|         write_error = "{}: {}".format(type(ex).__name__, ex)
  1120| 
  1121| OUT_payload[0]["file_written"] = file_written
  1122| if write_error:
  1123|     OUT_payload[0]["file_write_error"] = write_error
  1124| 
  1125| OUT = OUT_payload
```
