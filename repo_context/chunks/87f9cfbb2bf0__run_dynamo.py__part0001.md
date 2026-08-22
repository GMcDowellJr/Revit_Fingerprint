# Chunk of runner/run_dynamo.py

- Source relative path: `runner/run_dynamo.py`
- Chunk: 1 of 5
- Original line range: 1-519
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _looks_like_unc_path, _is_probably_sync_path, _is_repo_root, _read_tool_version, _use_filename_stamp, _extract_v2_hash, _extract_legacy_quality, _extract_v2_block_reasons, _looks_like_revit_unique_id, _has_v2_surface, _domain_run, _build_workset_name_to_unique_id_ctx, _enabled
- Source SHA-256: ab34a2ab032f21677da5f5b1b79a0089611b4fad01f19f048caf0e2893056e4b
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| # -*- coding: utf-8 -*-
     2| """
     3| Dynamo runner for Revit Fingerprint extraction.
     4| 
     5| This runner:
     6| - Acquires the Revit document from Dynamo context
     7| - Selects which domains to run (allowlist mechanism)
     8| - Assembles final JSON output
     9| 
    10| Current implementation (M5): full modular architecture with behavioral view templates
    11| """
    12| 
    13| import clr
    14| import json
    15| import sys
    16| import os
    17| import time
    18| import hashlib
    19| 
    20| _SCRIPT_START = time.perf_counter()
    21| 
    22| # --- ensure unsafe-location flags exist before use ---
    23| def _looks_like_unc_path(p):
    24|     try:
    25|         s = str(p)
    26|     except Exception:
    27|         return False
    28|     return s.startswith("\\\\")
    29| 
    30| def _is_probably_sync_path(p):
    31|     """
    32|     Heuristic, Windows-centric: previously used to hard-block sync paths.
    33|     Retained for detection only — callers decide whether to block or warn.
    34|     """
    35|     try:
    36|         s = os.path.abspath(str(p))
    37|     except Exception:
    38|         return False
    39|     sl = s.lower()
    40|     for m in ("\\onedrive\\", "\\sharepoint\\", "\\microsoft teams\\"):
    41|         if m in sl:
    42|             return True
    43|     if "\\documents\\" in sl and ("- sharepoint" in sl or "sharepoint" in sl):
    44|         return True
    45|     return False
    46| 
    47| def _is_repo_root(p):
    48|     try:
    49|         base = os.path.abspath(str(p))
    50|     except Exception:
    51|         return False
    52|     expected = (
    53|         os.path.join(base, "runner", "run_dynamo.py"),
    54|         os.path.join(base, "core"),
    55|         os.path.join(base, "domains"),
    56|     )
    57|     for e in expected:
    58|         if not os.path.exists(e):
    59|             return False
    60|     return True
    61| 
    62| # Prefer explicit repo-root signals (thin_runner) before __file__ fallback.
    63| _repo_override = ""
    64| for _k in ("REVIT_FINGERPRINT_REPO_ROOT_SELECTED", "REVIT_FINGERPRINT_REPO_DIR"):
    65|     try:
    66|         _v = str(os.environ.get(_k, "")).strip()
    67|     except Exception:
    68|         _v = ""
    69|     if _v and _is_repo_root(_v):
    70|         _repo_override = os.path.abspath(_v)
    71|         break
    72| 
    73| if _repo_override:
    74|     _REPO_ROOT = _repo_override
    75| else:
    76|     # runner/.. is the repo root
    77|     try:
    78|         _SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    79|     except Exception:
    80|         _SCRIPT_DIR = os.getcwd()
    81|     _REPO_ROOT = os.path.dirname(_SCRIPT_DIR)
    82| 
    83| _UNSAFE_REASONS = []
    84| if _looks_like_unc_path(_REPO_ROOT):
    85|     _UNSAFE_REASONS.append("repo_root_is_unc_path")
    86| 
    87| _SYNC_WARNINGS = []
    88| if _is_probably_sync_path(_REPO_ROOT):
    89|     _SYNC_WARNINGS.append("repo_root_looks_like_sharepoint_onedrive_sync")
    90| 
    91| def _read_tool_version(repo_root):
    92|     try:
    93|         p = os.path.join(repo_root, "VERSION.txt")
    94|         if not os.path.exists(p):
    95|             return None
    96|         with open(p, "r") as f:
    97|             s = f.read().strip()
    98|         return s if s else None
    99|     except Exception:
   100|         return None
   101| 
   102| _TOOL_VERSION = _read_tool_version(_REPO_ROOT)
   103| # --- end unsafe-location flags ---
   104| 
   105| if _UNSAFE_REASONS:
   106|     OUT = json.dumps(
   107|         {
   108|             "status": "blocked",
   109|             "error": "Unsafe execution location: UNC/network paths are not supported.",
   110|             "repo_root": _REPO_ROOT,
   111|             "unsafe_reasons": _UNSAFE_REASONS,
   112|             "sync_warnings": _SYNC_WARNINGS,
   113|             "_meta": {
   114|                 "runner": "M5",
   115|                 "runner_file": __file__,
   116|                 "tool_version": _TOOL_VERSION,
   117|             },
   118|         },
   119|         indent=2,
   120|         sort_keys=True,
   121|     )
   122|     raise SystemExit
   123| 
   124| # Add repo root to path for imports
   125| if _REPO_ROOT not in sys.path:
   126|     sys.path.insert(0, _REPO_ROOT)
   127| 
   128| # Contract + dependency utilities (must be imported after sys.path adjustment)
   129| from core import contracts
   130| from core.collect import CollectCtx, build_purgeable_id_set
   131| from core.context import DocViewContext
   132| from core.deps import Blocked, require_domain
   133| from core import naming as fp_naming
   134| from core.timing_collector import TimingCollector
   135| 
   136| # Revit/Dynamo plumbing
   137| clr.AddReference("RevitServices")
   138| from RevitServices.Persistence import DocumentManager
   139| 
   140| # Import domain extractors
   141| from domains import identity, units, line_patterns, line_styles
   142| from domains import arrowheads, text_types
   143| from domains import view_filter_definitions, view_filter_applications_view_templates
   144| from domains import phases, phase_filters, phase_graphics
   145| from domains import view_category_overrides_model
   146| from domains import view_category_overrides_annotation
   147| # Split domains: object_styles
   148| from domains import object_styles
   149| from domains import fill_patterns
   150| from domains import materials
   151| from domains import wall_types, floor_types, roof_types, ceiling_types
   152| from domains import dimension_types
   153| from domains import loaded_family_types
   154| from domains import view_templates
   155| from domains import worksets
   156| from domains import browser_organization
   157| from core.manifest import build_manifest
   158| from core.features import build_features
   159| from core.canonical_items import canonicalize_record
   160| from runner.extraction_context import build_extraction_context, operator_deployment_config_path
   161| 
   162| # Domain selection configuration
   163| # Set to None to run all domains, or provide a list of domain names to run specific domains
   164| ENABLED_DOMAINS = None  # None = all domains
   165| 
   166| # Hash computation uses record.v2 identity_basis items (semantic mode).
   167| # Legacy pipe-delimited hashing removed in PR #XXX.
   168| 
   169| _DOMAIN_VERSION = "1"
   170| 
   171| def _use_filename_stamp():
   172|     """
   173|     Returns True unless explicitly disabled.
   174| 
   175|     Accepts common Dynamo / shell representations for false.
   176|     """
   177|     try:
   178|         v = os.environ.get("REVIT_FINGERPRINT_FILENAME_STAMP", "")
   179|     except Exception:
   180|         v = ""
   181|     v = str(v).strip().lower()
   182| 
   183|     if not v:
   184|         return True
   185| 
   186|     if v in ("0", "false", "no", "off", "n", "f"):
   187|         return False
   188| 
   189|     # Handle "0.0" / "1.0" style values
   190|     try:
   191|         fv = float(v)
   192|         if fv == 0.0:
   193|             return False
   194|         if fv == 1.0:
   195|             return True
   196|     except Exception:
   197|         pass
   198| 
   199|     # Default: enabled
   200|     return True
   201| 
   202| def _extract_v2_hash(payload):
   203|     """
   204|     Best-effort extraction of the contract semantic hash (v2) without changing legacy behavior.
   205|     """
   206|     try:
   207|         if isinstance(payload, dict):
   208|             # Primary: current domain contract surface
   209|             if "hash_v2" in payload:
   210|                 return payload.get("hash_v2", None)
   211| 
   212|             # Fallback: future/alternate nesting (do not require domains to emit this)
   213|             sv2 = payload.get("semantic_v2", None)
   214|             if isinstance(sv2, dict) and "hash" in sv2:
   215|                 return sv2.get("hash", None)
   216|     except Exception as e:
   217|         pass
   218|     return None
   219| 
   220| 
   221| def _extract_legacy_quality(payload):
   222|     q = {}
   223|     try:
   224|         if isinstance(payload, dict) and "count" in payload:
   225|             q["count"] = payload.get("count", None)
   226|         if isinstance(payload, dict) and "raw_count" in payload:
   227|             q["raw_count"] = payload.get("raw_count", None)
   228|     except Exception as e:
   229|         pass
   230|     return q
   231| 
   232| def _extract_v2_block_reasons(payload):
   233|     """Best-effort extraction of v2 block reasons from a domain payload.
   234| 
   235|     Domains are allowed to evolve their internal debug surfaces; the runner
   236|     lifts these into the authoritative contract diag.
   237|     """
   238|     if not isinstance(payload, dict):
   239|         return {}
   240| 
   241|     # Current domains (PR6–PR8) typically emit one of these.
   242|     for k in ("debug_v2_block_reasons", "v2_block_reasons", "semantic_v2_block_reasons"):
   243|         try:
   244|             v = payload.get(k, None)
   245|         except Exception:
   246|             v = None
   247|         if isinstance(v, dict) and v:
   248|             # Keep values stable: prefer ints/bools/strings only.
   249|             out = {}
   250|             for rk, rv in v.items():
   251|                 try:
   252|                     key = str(rk)
   253|                 except Exception:
   254|                     continue
   255|                 if rv is None:
   256|                     out[key] = True
   257|                 elif isinstance(rv, (bool, int, float, str)):
   258|                     out[key] = rv
   259|                 else:
   260|                     out[key] = True
   261|             return out
   262| 
   263|     # Some domains only expose a simple blocked flag.
   264|     for k in ("debug_v2_blocked", "v2_blocked"):
   265|         try:
   266|             if bool(payload.get(k, False)) is True:
   267|                 return {"blocked": True}
   268|         except Exception:
   269|             pass
   270| 
   271|     return {}
   272| 
   273| 
   274| def _looks_like_revit_unique_id(v):
   275|     """Heuristic: detect Revit UniqueId strings."""
   276|     try:
   277|         s = str(v or "")
   278|     except Exception:
   279|         return False
   280|     if not s or len(s) < 45:
   281|         return False
   282|     import re as _re
   283|     return bool(_re.match(r"^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}-[0-9A-Fa-f]{8}$", s))
   284| 
   285| def _has_v2_surface(payload):
   286|     """Return True if the domain payload appears to implement a v2 hash surface."""
   287|     if not isinstance(payload, dict):
   288|         return False
   289|     try:
   290|         if "hash_v2" in payload:
   291|             return True
   292|     except Exception:
   293|         pass
   294|     try:
   295|         sv2 = payload.get("semantic_v2", None)
   296|         if isinstance(sv2, dict) and ("hash" in sv2):
   297|             return True
   298|     except Exception:
   299|         pass
   300|     return False
   301| 
   302| def _domain_run(domain_name, fn, doc, ctx, contract_domains, run_diag, runner_notes, *, require_v2_hash=True):
   303|     """Runs a domain extractor and records a contract envelope.
   304| 
   305|     Returns legacy_payload (or None on failure).
   306|     """
   307|     import traceback as _traceback
   308| 
   309|     domain_name = str(domain_name)
   310| 
   311|     # Timing instrumentation: wrap domain extraction
   312|     _tc = ctx.get("_timing") if isinstance(ctx, dict) else None
   313|     _timing_label = "domain:{}".format(domain_name)
   314| 
   315|     try:
   316|         if _tc is not None:
   317|             try:
   318|                 _tc.set_active_domain(domain_name)
   319|                 _tc.start_timer(_timing_label)
   320|             except Exception:
   321|                 pass
   322| 
   323|         legacy = fn(doc, ctx)
   324| 
   325|         # Domains may optionally emit contract signals into their legacy payload.
   326|         # Runner lifts these into the authoritative contract envelope and strips them from the legacy payload.
   327|         domain_status = contracts.DOMAIN_STATUS_OK
   328|         block_reasons = []
   329|         domain_diag = {
   330|             "api_reachable": True,
   331|         }
   332| 
   333|         if isinstance(legacy, dict):
   334|             try:
   335|                 _st = legacy.pop("_domain_status", None)
   336|                 if isinstance(_st, str) and _st in contracts.VALID_DOMAIN_STATUSES:
   337|                     domain_status = _st
   338|             except Exception:
   339|                 pass
   340| 
   341|             try:
   342|                 _br = legacy.pop("_domain_block_reasons", None)
   343|                 if isinstance(_br, list):
   344|                     block_reasons = [str(x) for x in _br]
   345|             except Exception:
   346|                 pass
   347| 
   348|             try:
   349|                 _dg = legacy.pop("_domain_diag", None)
   350|                 if isinstance(_dg, dict):
   351|                     # Merge domain diag into base diag (domain wins on key collisions)
   352|                     domain_diag.update(_dg)
   353|             except Exception:
   354|                 pass
   355| 
   356|         hash_value = _extract_v2_hash(legacy)
   357| 
   358|         # Lift v2 diagnostics into the contract envelope.
   359|         domain_diag["has_v2"] = bool(_has_v2_surface(legacy))
   360|         try:
   361|             recs = legacy.get("records", None) if isinstance(legacy, dict) else None
   362|             if isinstance(recs, list):
   363|                 domain_diag["details_records_count"] = len(recs)
   364|                 v2_count = 0
   365|                 sample_items = None
   366|                 uid_like_values = 0
   367|                 uid_key_count = 0
   368|                 for r in recs[:3]:
   369|                     if isinstance(r, dict) and r.get("schema_version", None) == "record.v2":
   370|                         v2_count += 1
   371|                         ib = r.get("identity_basis", {}) if isinstance(r.get("identity_basis", {}), dict) else {}
   372|                         items = ib.get("items", []) if isinstance(ib.get("items", []), list) else []
   373|                         if sample_items is None and items:
   374|                             sample_items = items
   375|                         for it in items:
   376|                             if not isinstance(it, dict):
   377|                                 continue
   378|                             k = str(it.get("k", ""))
   379|                             if ("uid" in k) or k.endswith("_uid"):
   380|                                 uid_key_count += 1
   381|                             if _looks_like_revit_unique_id(it.get("v", None)):
   382|                                 uid_like_values += 1
   383|                 domain_diag["records_v2_sample_count"] = v2_count
   384|                 if sample_items is not None:
   385|                     domain_diag["v2_sample_identity_keys"] = [str(it.get("k", "")) for it in sample_items[:12]]
   386|                 domain_diag["v2_uid_key_count_in_sample"] = int(uid_key_count)
   387|                 domain_diag["v2_uid_like_values_in_sample"] = int(uid_like_values)
   388|         except Exception:
   389|             pass
   390|         v2_reasons = _extract_v2_block_reasons(legacy)
   391|         if v2_reasons:
   392|             domain_diag["v2_block_reasons"] = v2_reasons
   393| 
   394|         # Lift count/raw_count into the contract diagnostics.
   395|         quality = _extract_legacy_quality(legacy)
   396|         if "count" in quality:
   397|             domain_diag["count"] = quality["count"]
   398|         if "raw_count" in quality:
   399|             domain_diag["raw_count"] = quality["raw_count"]
   400| 
   401|         # Empty-population exemption: if the domain explicitly emits raw_count=0 and
   402|         # hash_v2=None with debug_v2_blocked=False, that is a valid "no content" state —
   403|         # not a hash failure.  Only block on no_semantic_hash when raw_count > 0 (i.e. the
   404|         # domain had candidates but produced no hash, which is a genuine problem).
   405|         _raw_count = quality.get("raw_count", None)
   406|         _empty_population = (_raw_count is not None and _raw_count == 0
   407|                              and not bool((legacy or {}).get("debug_v2_blocked", True)))
   408| 
   409|         if require_v2_hash and domain_status == contracts.DOMAIN_STATUS_OK and hash_value is None and not _empty_population:
   410|             domain_status = contracts.DOMAIN_STATUS_BLOCKED
   411|             if v2_reasons:
   412|                 block_reasons = sorted({str(k) for k in v2_reasons.keys()})
   413|             else:
   414|                 block_reasons = ["no_semantic_hash"]
   415|         elif domain_status == contracts.DOMAIN_STATUS_BLOCKED and not block_reasons:
   416|             block_reasons = sorted({str(k) for k in v2_reasons.keys()}) if v2_reasons else ["blocked"]
   417| 
   418|         env = contracts.new_domain_envelope(
   419|             domain=domain_name,
   420|             domain_version=_DOMAIN_VERSION,
   421|             status=domain_status,
   422|             block_reasons=block_reasons,
   423|             diag=domain_diag,
   424|             records=None,
   425|             hash_value=hash_value,
   426|         )
   427|         contract_domains[domain_name] = env
   428| 
   429|         # End timing on success
   430|         if _tc is not None:
   431|             try:
   432|                 _tc.end_timer(_timing_label)
   433|                 _tc.set_active_domain(None)
   434|             except Exception:
   435|                 pass
   436| 
   437|         return legacy
   438| 
   439|     except Exception as e:
   440|         # End timing on failure
   441|         if _tc is not None:
   442|             try:
   443|                 _tc.end_timer(_timing_label)
   444|                 _tc.set_active_domain(None)
   445|             except Exception:
   446|                 pass
   447| 
   448|         # Hard fail: downstream must not infer success.
   449|         contracts.add_bounded_error(
   450|             run_diag,
   451|             domain=domain_name,
   452|             status=contracts.DOMAIN_STATUS_FAILED,
   453|             code="domain_exception",
   454|             message=str(e),
   455|         )
   456|         contract_domains[domain_name] = contracts.new_domain_envelope(
   457|             domain=domain_name,
   458|             domain_version=_DOMAIN_VERSION,
   459|             status=contracts.DOMAIN_STATUS_FAILED,
   460|             block_reasons=[],
   461|             diag={
   462|                 "api_reachable": True,
   463|                 "error": str(e),
   464|                 "traceback": _traceback.format_exc(),
   465|             },
   466|             records=None,
   467|             hash_value=None,
   468|         )
   469|         runner_notes.append("One or more domains failed; see _contract.run_diag and _contract.domains.*.diag")
   470|         return None
   471| 
   472| def _build_workset_name_to_unique_id_ctx(worksets_legacy):
   473|     """Build a workset name -> unique_id crosswalk for browser_organization.
   474| 
   475|     domains/worksets.py (Area 3) is out of scope to modify for the
   476|     browser_organization area, and doesn't currently export a ctx map of its
   477|     own (unlike materials.py/line_patterns.py/phases.py/object_styles.py).
   478|     This builds the crosswalk here, in the runner, from Area 3's own
   479|     already-computed "worksets" records -- not a fresh independent worksets
   480|     sweep -- so domains/browser_organization.py can join
   481|     BrowserOrganization.WorksetId-resolved names back to Area 3's actual
   482|     evidence instead of re-deriving worksets.py's discovery/classification
   483|     logic itself.
   484|     """
   485|     out = {}
   486|     if not isinstance(worksets_legacy, dict):
   487|         return out
   488|     for rec in worksets_legacy.get("records", None) or []:
   489|         if not isinstance(rec, dict):
   490|             continue
   491|         items = ((rec.get("identity_basis", None) or {}).get("items", None)) or []
   492|         name_v = None
   493|         uid_v = None
   494|         for it in items:
   495|             if not isinstance(it, dict):
   496|                 continue
   497|             k = it.get("k")
   498|             if k == "workset.name" and it.get("q") == "ok":
   499|                 name_v = it.get("v")
   500|             elif k == "workset.unique_id" and it.get("q") == "ok":
   501|                 uid_v = it.get("v")
   502|         if name_v and uid_v:
   503|             out[name_v] = uid_v
   504|     return out
   505| 
   506| def _enabled(domain_name):
   507|     """
   508|     Allowlist gate for domain execution.
   509|     - ENABLED_DOMAINS = None  -> run all domains
   510|     - ENABLED_DOMAINS = [...] -> run only listed domains (exact key match)
   511|     """
   512|     if ENABLED_DOMAINS is None:
   513|         return True
   514|     try:
   515|         allowed = set(ENABLED_DOMAINS)
   516|     except Exception as e:
   517|         allowed = set()
   518|     return domain_name in allowed
   519| 
```
