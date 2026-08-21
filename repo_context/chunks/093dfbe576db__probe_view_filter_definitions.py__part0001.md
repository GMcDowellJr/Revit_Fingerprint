# Chunk of tools/probes/probe_view_filter_definitions.py

- Source relative path: `tools/probes/probe_view_filter_definitions.py`
- Chunk: 1 of 3
- Original line range: 1-333
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _safe, _sha1, _as_param_payload, _maybe_set_example, _observe, _bucket_label_from_categories, _resolve_category_name, _element_filter_kind, _get_subfilters, _get_rules_from_element_parameter_filter, _rule_parameter_id, _rule_evaluator_name, _rule_value_best_effort, _flatten_element_filter
- Source SHA-256: 55514de8160889f7abe33d957fabe54c77f668e09919199e92f16fe13ccf2c44
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| # Dynamo Python (Revit) — Breadth Probe: view_filter_definitions (INVENTORY OUTPUT)
     2| #
     3| # DOMAIN = "view_filter_definitions"
     4| #
     5| # OUT = [
     6| #   {
     7| #     "kind": "inventory",
     8| #     "domain": "view_filter_definitions",
     9| #     "records": param_inventory,
    10| #     "file_written": "<path>|None",        # present only if write_json=True
    11| #     "file_write_error": "<error>|None"    # present only on failure
    12| #   },
    13| #   {
    14| #     "kind": "crosswalk",
    15| #     "domain": "view_filter_definitions",
    16| #     "records": optional_crosswalk         # ParameterFilterElement -> applying views/templates
    17| #   }
    18| # ]
    19| #
    20| # Inputs:
    21| #   IN[0] max_filters_to_inspect (int)
    22| #        Maximum number of ParameterFilterElement instances to inspect.
    23| #        Default: 500
    24| #
    25| #   IN[1] per_category_sig_limit (int)
    26| #        Sample at most N filters per distinct category-signature bucket
    27| #        (sorted category ids). Default: 5
    28| #
    29| #   IN[2] write_json (bool)
    30| #        When True, serialize OUT to a valid JSON file on disk.
    31| #        Default: False
    32| #
    33| #   IN[3] output_directory (str)
    34| #        Directory path where JSON will be written.
    35| #        Filename is fixed as: probes_<revit_version>_<run_id>.json
    36| #        If None, falls back to RVT directory, then TEMP.
    37| #
    38| #   IN[4] max_rules_to_read_per_filter (int)
    39| #        Hard safety cap on number of rules flattened per filter.
    40| #        Default: 200
    41| #
    42| #   IN[5] enable_crosswalk (bool)
    43| #        Whether to emit a ParameterFilterElement -> applying-views/templates
    44| #        crosswalk (one row per filter: how many live views and how many
    45| #        view templates have it in View.GetFilters(), plus a small name
    46| #        sample). Same View.GetFilters() call probe_view_filter_applications.py
    47| #        already uses, applied from the filter's side instead of the view's
    48| #        side -- that probe's crosswalk tells you what a given view applies;
    49| #        this one tells you whether a given filter is used by anything at
    50| #        all (orphan/adoption signal). Appended as a new position rather
    51| #        than inserted earlier so existing positional callers don't shift.
    52| #        Default: False
    53| #
    54| #   IN[6] max_views_to_scan (int)
    55| #        When crosswalk enabled, scan at most N views+templates for applied
    56| #        filters. Default: 2000
    57| #
    58| # Reference pattern: probe_arrowheads.py :contentReference[oaicite:0]{index=0}
    59| 
    60| 
    61| import clr
    62| import os
    63| import json
    64| import hashlib
    65| from datetime import datetime
    66| 
    67| clr.AddReference("RevitServices")
    68| from RevitServices.Persistence import DocumentManager
    69| 
    70| clr.AddReference("RevitAPI")
    71| from Autodesk.Revit.DB import (
    72|     FilteredElementCollector,
    73|     ElementId,
    74|     StorageType,
    75|     UnitUtils,
    76|     UnitTypeId,
    77|     BuiltInParameter,
    78|     Category,
    79|     ParameterFilterElement,
    80|     LogicalAndFilter,
    81|     LogicalOrFilter,
    82|     ElementParameterFilter,
    83|     View
    84| )
    85| 
    86| doc = DocumentManager.Instance.CurrentDBDocument
    87| 
    88| max_filters_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 500
    89| per_category_sig_limit = IN[1] if len(IN) > 1 and IN[1] is not None else 5
    90| write_json = IN[2] if len(IN) > 2 and IN[2] is not None else False
    91| out_path = IN[3] if len(IN) > 3 and IN[3] is not None else None
    92| max_rules_to_read_per_filter = IN[4] if len(IN) > 4 and IN[4] is not None else 200
    93| enable_crosswalk = IN[5] if len(IN) > 5 and IN[5] is not None else False
    94| max_views_to_scan = IN[6] if len(IN) > 6 and IN[6] is not None else 2000
    95| 
    96| 
    97| # -------------------------
    98| # Helpers (defensive)
    99| # -------------------------
   100| 
   101| def _safe(fn, default=None):
   102|     try:
   103|         return fn()
   104|     except:
   105|         return default
   106| 
   107| def _sha1(s):
   108|     try:
   109|         b = s.encode("utf-8")
   110|         return hashlib.sha1(b).hexdigest()
   111|     except:
   112|         return None
   113| 
   114| def _as_param_payload(q, storage, raw, display, norm):
   115|     return {
   116|         "q": q,
   117|         "storage": storage,
   118|         "raw": raw,
   119|         "display": display,
   120|         "norm": norm
   121|     }
   122| 
   123| def _maybe_set_example(entry, pv):
   124|     # Keep exactly one example: prefer first "ok" encountered, otherwise first non-ok.
   125|     if pv is None:
   126|         return
   127|     ex = entry.get("example")
   128|     if ex is None:
   129|         entry["example"] = {
   130|             "q": pv.get("q"),
   131|             "storage": pv.get("storage"),
   132|             "raw": pv.get("raw"),
   133|             "display": pv.get("display"),
   134|             "norm": pv.get("norm")
   135|         }
   136|         return
   137|     if ex.get("q") != "ok" and pv.get("q") == "ok":
   138|         entry["example"] = {
   139|             "q": pv.get("q"),
   140|             "storage": pv.get("storage"),
   141|             "raw": pv.get("raw"),
   142|             "display": pv.get("display"),
   143|             "norm": pv.get("norm")
   144|         }
   145| 
   146| def _observe(param_index, param_key, pv, bucket_label):
   147|     if param_key not in param_index:
   148|         param_index[param_key] = {
   149|             "storage_types": set(),
   150|             "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
   151|             "example": None,
   152|             "observed_on_buckets": set()
   153|         }
   154| 
   155|     entry = param_index[param_key]
   156|     q = pv.get("q") or "unreadable"
   157|     st = pv.get("storage")
   158| 
   159|     if st is not None:
   160|         entry["storage_types"].add(st)
   161|     if q not in entry["q_counts"]:
   162|         entry["q_counts"][q] = 0
   163|     entry["q_counts"][q] += 1
   164| 
   165|     if bucket_label:
   166|         entry["observed_on_buckets"].add(bucket_label)
   167| 
   168|     _maybe_set_example(entry, pv)
   169| 
   170| def _bucket_label_from_categories(cat_ids_sorted):
   171|     # Stable breadth bucket label
   172|     if not cat_ids_sorted:
   173|         return "0|<none>"
   174|     try:
   175|         return "{}|{}".format(len(cat_ids_sorted), "|".join([str(i) for i in cat_ids_sorted[:25]]))
   176|     except:
   177|         return "{}|<unreadable>".format(len(cat_ids_sorted) if cat_ids_sorted else 0)
   178| 
   179| def _resolve_category_name(cat_id_int):
   180|     # Category ids for view filters are category ids, not elements; use Category.GetCategory if possible.
   181|     try:
   182|         cat = Category.GetCategory(doc, ElementId(int(cat_id_int)))
   183|         return _safe(lambda: cat.Name, None) if cat is not None else None
   184|     except:
   185|         return None
   186| 
   187| 
   188| # -------------------------
   189| # Filter-rule flattening (best-effort, version-tolerant)
   190| # -------------------------
   191| 
   192| def _element_filter_kind(ef):
   193|     if ef is None:
   194|         return None
   195|     try:
   196|         return ef.GetType().Name
   197|     except:
   198|         return None
   199| 
   200| def _get_subfilters(ef):
   201|     # LogicalAndFilter / LogicalOrFilter support GetFilters()
   202|     if ef is None:
   203|         return []
   204|     try:
   205|         if isinstance(ef, LogicalAndFilter) or isinstance(ef, LogicalOrFilter):
   206|             subs = _safe(lambda: list(ef.GetFilters()), default=[])
   207|             return subs if subs else []
   208|     except:
   209|         pass
   210|     return []
   211| 
   212| def _get_rules_from_element_parameter_filter(epf):
   213|     # ElementParameterFilter supports GetRules() in most modern APIs; fallback to reflection-style access.
   214|     if epf is None:
   215|         return []
   216|     rules = _safe(lambda: list(epf.GetRules()), default=None)
   217|     if rules is not None:
   218|         return rules
   219|     # fallback: try property names that sometimes exist
   220|     for attr in ("Rules", "GetRules", "GetElementFilterRules"):
   221|         try:
   222|             v = getattr(epf, attr)
   223|             if callable(v):
   224|                 rr = v()
   225|                 return list(rr) if rr is not None else []
   226|             return list(v) if v is not None else []
   227|         except:
   228|             continue
   229|     return []
   230| 
   231| def _rule_parameter_id(rule):
   232|     # Try common methods/properties
   233|     for attr in ("GetRuleParameter", "ParameterId", "GetParameterId"):
   234|         try:
   235|             v = getattr(rule, attr)
   236|             if callable(v):
   237|                 pid = v()
   238|             else:
   239|                 pid = v
   240|             if isinstance(pid, ElementId):
   241|                 return _safe(lambda: pid.IntegerValue, None)
   242|             # sometimes already an int
   243|             if pid is not None:
   244|                 return int(pid)
   245|         except:
   246|             continue
   247|     return None
   248| 
   249| def _rule_evaluator_name(rule):
   250|     # Not always accessible; attempt best-effort.
   251|     for attr in ("GetEvaluator", "Evaluator"):
   252|         try:
   253|             v = getattr(rule, attr)
   254|             ev = v() if callable(v) else v
   255|             if ev is None:
   256|                 continue
   257|             return _safe(lambda: ev.GetType().Name, None) or _safe(lambda: str(ev), None)
   258|         except:
   259|             continue
   260|     return None
   261| 
   262| def _rule_value_best_effort(rule):
   263|     # Many rule types differ; attempt common properties first; else string fallback.
   264|     for attr in ("RuleString", "StringValue", "RuleValue", "Value", "DoubleValue", "IntegerValue"):
   265|         try:
   266|             v = getattr(rule, attr)
   267|             vv = v() if callable(v) else v
   268|             if vv is not None:
   269|                 return vv
   270|         except:
   271|             continue
   272|     # final fallback: string form
   273|     return _safe(lambda: str(rule), None)
   274| 
   275| def _flatten_element_filter(ef, hard_cap):
   276|     """
   277|     Returns:
   278|       logic: "and" | "or" | "single" | "unknown"
   279|       rules: list of dicts (best-effort)
   280|     """
   281|     if ef is None:
   282|         return ("missing", [])
   283| 
   284|     kind = _element_filter_kind(ef)
   285| 
   286|     # Logical container?
   287|     if isinstance(ef, LogicalAndFilter):
   288|         logic = "and"
   289|         rules_out = []
   290|         for sub in _get_subfilters(ef):
   291|             lg, rr = _flatten_element_filter(sub, hard_cap)
   292|             for r in rr:
   293|                 if len(rules_out) >= hard_cap:
   294|                     break
   295|                 rules_out.append(r)
   296|             if len(rules_out) >= hard_cap:
   297|                 break
   298|         return (logic, rules_out)
   299| 
   300|     if isinstance(ef, LogicalOrFilter):
   301|         logic = "or"
   302|         rules_out = []
   303|         for sub in _get_subfilters(ef):
   304|             lg, rr = _flatten_element_filter(sub, hard_cap)
   305|             for r in rr:
   306|                 if len(rules_out) >= hard_cap:
   307|                     break
   308|                 rules_out.append(r)
   309|             if len(rules_out) >= hard_cap:
   310|                 break
   311|         return (logic, rules_out)
   312| 
   313|     # Parameter filter leaf?
   314|     if isinstance(ef, ElementParameterFilter):
   315|         rules = _get_rules_from_element_parameter_filter(ef)
   316|         rules_out = []
   317|         for rule in rules:
   318|             if len(rules_out) >= hard_cap:
   319|                 break
   320|             rtype = _safe(lambda: rule.GetType().Name, None)
   321|             pid = _rule_parameter_id(rule)
   322|             ev = _rule_evaluator_name(rule)
   323|             val = _rule_value_best_effort(rule)
   324|             rules_out.append({
   325|                 "rule.type": rtype,
   326|                 "rule.param_id": pid,
   327|                 "rule.evaluator": ev,
   328|                 "rule.value": val
   329|             })
   330|         return ("single", rules_out)
   331| 
   332|     # Unknown leaf type — keep kind for diagnostics, no rules
   333|     return ("unknown:{}".format(kind), [])
```
