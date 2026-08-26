# Chunk of tools/probes/probe_view_filter_definitions.py

- Source relative path: `tools/probes/probe_view_filter_definitions.py`
- Chunk: 3 of 3
- Original line range: 852-1055
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _resolve_workset, _probe_revit_version, _probe_document_identity, _probe_run_id, _probe_wrap
- Source SHA-256: 55514de8160889f7abe33d957fabe54c77f668e09919199e92f16fe13ccf2c44
- Starts inside symbol: no
- Ends inside symbol: no

```
   852| 
   853| _reflection_records_0 = _run_reflection_sweep(selected, "ParameterFilterElement", "view_filter_definitions")
   854| _reflection_records = _reflection_records_0
   855| 
   856| # -------------------------
   857| # Crosswalk (optional): ParameterFilterElement -> applying views/templates.
   858| # Reuses the exact View.GetFilters() call probe_view_filter_applications.py
   859| # already uses, from the filter's side instead of the view's side. One row
   860| # per discovered filter (from `filters`, the full pre-bucket-sample list --
   861| # every definition gets a crosswalk row, not just the reflection-sampled
   862| # subset), aggregated usage counts rather than a raw per-view join, since
   863| # "is this filter used by anything" is the directly useful governance
   864| # question (orphan/purge-candidate signal).
   865| # -------------------------
   866| 
   867| filter_usage = {}  # filter_id_int -> {"live_view_count": int, "template_count": int, "sample_names": [...]}
   868| if enable_crosswalk:
   869|     scan_views = _safe(lambda: list(FilteredElementCollector(doc).OfClass(View).ToElements()), default=[])
   870|     try:
   871|         vcap = int(max_views_to_scan)
   872|         if vcap >= 0:
   873|             scan_views = scan_views[:vcap]
   874|     except:
   875|         pass
   876| 
   877|     for v in scan_views:
   878|         if v is None:
   879|             continue
   880|         is_template = _safe(lambda: bool(v.IsTemplate), False)
   881|         # Guarded: some view types can throw on GetFilters (same guard
   882|         # probe_view_filter_applications.py uses).
   883|         fids = _safe(lambda: list(v.GetFilters()), default=None)
   884|         if not fids:
   885|             continue
   886|         vname = _safe(lambda: v.Name, None)
   887|         for fid_obj in fids:
   888|             fid = _safe(lambda: fid_obj.IntegerValue, None)
   889|             if fid is None:
   890|                 continue
   891|             entry = filter_usage.setdefault(fid, {"live_view_count": 0, "template_count": 0, "sample_names": []})
   892|             if is_template:
   893|                 entry["template_count"] += 1
   894|             else:
   895|                 entry["live_view_count"] += 1
   896|             if vname and len(entry["sample_names"]) < 5:
   897|                 entry["sample_names"].append(vname)
   898| 
   899| optional_crosswalk = []
   900| 
   901| 
   902| def _resolve_workset(doc, ws_id_obj):
   903|     """Resolve an Element.WorksetId value to (name, resolved_bool) via
   904|     WorksetTable.GetWorkset() -- NOT doc.GetElement(). WorksetId is a
   905|     distinct .NET type from ElementId (both happen to expose .IntegerValue,
   906|     which is why reflection reports this member as ElementId-storage), and
   907|     Workset is not derived from Element, so doc.GetElement() would never
   908|     resolve it even with the right type assumed."""
   909|     if ws_id_obj is None:
   910|         return (None, False)
   911|     wt_table = _safe(lambda: doc.GetWorksetTable(), None)
   912|     if wt_table is None:
   913|         return (None, False)
   914|     ws = _safe(lambda: wt_table.GetWorkset(ws_id_obj), None)
   915|     if ws is None:
   916|         return (None, False)
   917|     name = _safe(lambda: ws.Name, None)
   918|     return (name, name is not None)
   919| 
   920| 
   921| for f in filters:
   922|     fid = _safe(lambda: f.Id.IntegerValue, None)
   923|     if fid is None:
   924|         continue
   925|     fname = _safe(lambda: f.Name, None)
   926|     f_ws_id_obj = _safe(lambda: f.WorksetId, None)
   927|     f_ws_name, _f_ws_resolved = _resolve_workset(doc, f_ws_id_obj)
   928|     f_ws_id_int = _safe(lambda: f_ws_id_obj.IntegerValue, None) if f_ws_id_obj is not None else None
   929|     usage = filter_usage.get(fid, {"live_view_count": 0, "template_count": 0, "sample_names": []})
   930|     total = usage["live_view_count"] + usage["template_count"]
   931|     cat_ids = _safe(lambda: list(f.GetCategories()), default=[])
   932|     cat_ids_int = [_safe(lambda cid=cid: cid.IntegerValue, None) for cid in cat_ids]
   933|     cat_names = [_resolve_category_name(cid) for cid in cat_ids_int if cid is not None]
   934|     optional_crosswalk.append({
   935|         "filter.id": fid,
   936|         "filter.name": fname,
   937|         "filter.workset_id": f_ws_id_int,
   938|         "filter.workset_name": f_ws_name,
   939|         "filter.is_applied_anywhere": total > 0,
   940|         "applied_live_view_count": usage["live_view_count"],
   941|         "applied_template_count": usage["template_count"],
   942|         "sample_applied_names": usage["sample_names"],
   943|         "get_categories.ids": cat_ids_int,
   944|         "get_categories.names": cat_names,
   945|     })
   946| 
   947| OUT_payload = [
   948|     {
   949|         "kind": "reflection",
   950|         "domain": "view_filter_definitions",
   951|         "records": _reflection_records
   952|     },
   953|     {
   954|         "kind": "inventory",
   955|         "domain": "view_filter_definitions",
   956|         "records": param_inventory
   957|     },
   958|     {
   959|         "kind": "crosswalk",
   960|         "domain": "view_filter_definitions",
   961|         "records": optional_crosswalk
   962|     }
   963| ]
   964| 
   965| 
   966| # -------------------------
   967| # Optional: write to JSON
   968| # -------------------------
   969| 
   970| file_written = None
   971| write_error = None
   972| 
   973| # -------------------------
   974| # Unified run metadata (release-separated, not date-filename-separated)
   975| # -------------------------
   976| # extraction_date lives as JSON metadata, not as a filename token; the
   977| # filename groups by Revit release (revit_version) plus an opaque run_id so
   978| # repeated runs don't collide. See tools/probes/build_probe_inventory.py,
   979| # which consumes this shape directly.
   980| 
   981| import uuid as _uuid_mod
   982| 
   983| def _probe_revit_version():
   984|     try:
   985|         _uiapp = DocumentManager.Instance.CurrentUIApplication
   986|         _app = _uiapp.Application if _uiapp is not None else None
   987|         v = _safe(lambda: _app.VersionNumber, None)
   988|         return str(v) if v else None
   989|     except:
   990|         return None
   991| 
   992| def _probe_document_identity():
   993|     return {
   994|         "title": _safe(lambda: doc.Title, None),
   995|         "path_name": _safe(lambda: doc.PathName, None),
   996|         "is_workshared": _safe(lambda: bool(doc.IsWorkshared), None),
   997|     }
   998| 
   999| def _probe_run_id():
  1000|     try:
  1001|         return datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + _uuid_mod.uuid4().hex[:6]
  1002|     except:
  1003|         return _uuid_mod.uuid4().hex[:12]
  1004| 
  1005| _PROBE_RUN_ID = _probe_run_id()
  1006| _PROBE_REVIT_VERSION = _probe_revit_version() or "unknown"
  1007| 
  1008| def _probe_wrap(domain, out_payload):
  1009|     return {
  1010|         "run_metadata": {
  1011|             "run_id": _PROBE_RUN_ID,
  1012|             "extraction_date": datetime.now().isoformat(),
  1013|             "revit_version": _PROBE_REVIT_VERSION,
  1014|             "tool_version": None,
  1015|             "document": _probe_document_identity(),
  1016|             "source": "single_probe",
  1017|             "probe": domain,
  1018|         },
  1019|         "domains": {domain: out_payload},
  1020|     }
  1021| 
  1022| 
  1023| if write_json:
  1024|     try:
  1025|         rvt_path = _safe(lambda: doc.PathName, None)
  1026|         default_dir = None
  1027| 
  1028|         if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
  1029|             default_dir = _safe(lambda: os.path.dirname(rvt_path), None)
  1030| 
  1031|         if not default_dir:
  1032|             default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()
  1033| 
  1034|         date_stamp = datetime.now().strftime("%Y-%m-%d")
  1035|         fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)
  1036| 
  1037|         target_dir = out_path if out_path else default_dir
  1038|         target_path = os.path.join(target_dir, fixed_name)
  1039| 
  1040|         if target_dir and not os.path.exists(target_dir):
  1041|             os.makedirs(target_dir)
  1042| 
  1043|         with open(target_path, "w") as f:
  1044|             json.dump(_probe_wrap("view_filter_definitions", OUT_payload), f, indent=2, sort_keys=True)
  1045| 
  1046|         file_written = target_path
  1047| 
  1048|     except Exception as ex:
  1049|         write_error = "{}: {}".format(type(ex).__name__, ex)
  1050| 
  1051| OUT_payload[0]["file_written"] = file_written
  1052| if write_error:
  1053|     OUT_payload[0]["file_write_error"] = write_error
  1054| 
  1055| OUT = OUT_payload
```
