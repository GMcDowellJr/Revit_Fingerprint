# Chunk of runner/run_dynamo.py

- Source relative path: `runner/run_dynamo.py`
- Chunk: 4 of 5
- Original line range: 924-1164
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: run_fingerprint
- Source SHA-256: ab34a2ab032f21677da5f5b1b79a0089611b4fad01f19f048caf0e2893056e4b
- Starts inside symbol: run_fingerprint
- Ends inside symbol: no

```
   924|                 runner_notes,
   925|             )
   926|             if legacy is not None:
   927|                 fingerprint["view_category_overrides_model"] = legacy
   928|         except Blocked as b:
   929|             contract_domains["view_category_overrides_model"] = contracts.new_domain_envelope(
   930|                 domain="view_category_overrides_model",
   931|                 domain_version=_DOMAIN_VERSION,
   932|                 status=contracts.DOMAIN_STATUS_BLOCKED,
   933|                 block_reasons=list(b.reasons),
   934|                 diag={
   935|                     "blocked_code": b.code,
   936|                     "upstream": b.upstream,
   937|                 },
   938|                 records=None,
   939|                 hash_value=None,
   940|             )
   941|             contracts.add_bounded_error(
   942|                 run_diag,
   943|                 domain="view_category_overrides_model",
   944|                 status=contracts.DOMAIN_STATUS_BLOCKED,
   945|                 code=b.code,
   946|                 message=";".join(list(b.reasons)),
   947|             )
   948| 
   949|     if _enabled("view_category_overrides_annotation"):
   950|         legacy = _domain_run(
   951|             "view_category_overrides_annotation",
   952|             view_category_overrides_annotation.extract,
   953|             doc,
   954|             ctx,
   955|             contract_domains,
   956|             run_diag,
   957|             runner_notes,
   958|         )
   959|         if legacy is not None:
   960|             fingerprint["view_category_overrides_annotation"] = legacy
   961| 
   962|     # Deprecated aggregate output intentionally omitted.
   963|     # Keep split domains only: view_category_overrides_model +
   964|     # view_category_overrides_annotation.
   965| 
   966|     # view_templates split domains.
   967|     # Non-schedule domains require both phase_filters and view_filter_definitions
   968|     # (filter stack resolution). Schedules only require phase_filters — ViewSchedule
   969|     # templates do not have view filter stacks.
   970|     for _vt_domain, _vt_extractor in [
   971|         ("view_templates_floor_structural_area_plans", view_templates.extract_floor_structural_area_plans),
   972|         ("view_templates_ceiling_plans", view_templates.extract_ceiling_plans),
   973|         ("view_templates_elevations_sections_detail", view_templates.extract_elevations_sections_detail),
   974|         ("view_templates_renderings_drafting", view_templates.extract_renderings_drafting),
   975|     ]:
   976|         if not _enabled(_vt_domain):
   977|             continue
   978|         try:
   979|             require_domain(contract_domains, "phase_filters")
   980|             require_domain(contract_domains, "view_filter_definitions")
   981|             legacy = _domain_run(_vt_domain, _vt_extractor, doc, ctx,
   982|                                  contract_domains, run_diag, runner_notes)
   983|             if legacy is not None:
   984|                 fingerprint[_vt_domain] = legacy
   985|         except Blocked as b:
   986|             contract_domains[_vt_domain] = contracts.new_domain_envelope(
   987|                 domain=_vt_domain,
   988|                 domain_version=_DOMAIN_VERSION,
   989|                 status=contracts.DOMAIN_STATUS_BLOCKED,
   990|                 block_reasons=list(b.reasons),
   991|                 diag={"blocked_code": b.code, "upstream": b.upstream},
   992|                 records=None,
   993|                 hash_value=None,
   994|             )
   995|             contracts.add_bounded_error(run_diag, domain=_vt_domain,
   996|                 status=contracts.DOMAIN_STATUS_BLOCKED, code=b.code,
   997|                 message=";".join(list(b.reasons)))
   998| 
   999|     # Schedules gate: only requires phase_filters
  1000|     if _enabled("view_templates_schedules"):
  1001|         try:
  1002|             require_domain(contract_domains, "phase_filters")
  1003|             legacy = _domain_run("view_templates_schedules",
  1004|                                  view_templates.extract_schedules, doc, ctx,
  1005|                                  contract_domains, run_diag, runner_notes)
  1006|             if legacy is not None:
  1007|                 fingerprint["view_templates_schedules"] = legacy
  1008|         except Blocked as b:
  1009|             contract_domains["view_templates_schedules"] = contracts.new_domain_envelope(
  1010|                 domain="view_templates_schedules",
  1011|                 domain_version=_DOMAIN_VERSION,
  1012|                 status=contracts.DOMAIN_STATUS_BLOCKED,
  1013|                 block_reasons=list(b.reasons),
  1014|                 diag={"blocked_code": b.code, "upstream": b.upstream},
  1015|                 records=None,
  1016|                 hash_value=None,
  1017|             )
  1018|             contracts.add_bounded_error(run_diag, domain="view_templates_schedules",
  1019|                 status=contracts.DOMAIN_STATUS_BLOCKED, code=b.code,
  1020|                 message=";".join(list(b.reasons)))
  1021| 
  1022|     if _enabled("worksets"):
  1023|         legacy = _domain_run("worksets", worksets.extract_worksets, doc, ctx, contract_domains, run_diag, runner_notes)
  1024|         if legacy is not None:
  1025|             fingerprint["worksets"] = legacy
  1026|         # Cross-domain crosswalk for browser_organization (below): see
  1027|         # _build_workset_name_to_unique_id_ctx's docstring for why this is
  1028|         # built here rather than inside worksets.py.
  1029|         ctx["workset_name_to_unique_id"] = _build_workset_name_to_unique_id_ctx(legacy)
  1030| 
  1031|     if _enabled("worksets_doc"):
  1032|         legacy = _domain_run("worksets_doc", worksets.extract_worksets_doc, doc, ctx, contract_domains, run_diag, runner_notes)
  1033|         if legacy is not None:
  1034|             fingerprint["worksets_doc"] = legacy
  1035| 
  1036|     if _enabled("browser_organization"):
  1037|         legacy = _domain_run("browser_organization", browser_organization.extract_browser_organization, doc, ctx, contract_domains, run_diag, runner_notes)
  1038|         if legacy is not None:
  1039|             fingerprint["browser_organization"] = legacy
  1040| 
  1041|     # Routing completeness check: verify all view templates accounted for
  1042|     # across all 5 domains. Emits a runner note if any templates fell through.
  1043|     try:
  1044|         _vt_domains = [
  1045|             "view_templates.extract_floor_structural_area_plans",
  1046|             "view_templates.extract_ceiling_plans",
  1047|             "view_templates.extract_elevations_sections_detail",
  1048|             "view_templates.extract_renderings_drafting",
  1049|             "view_templates_schedules",
  1050|         ]
  1051|         _vt_total_kept = sum(
  1052|             fingerprint.get(d, {}).get("debug_kept", 0)
  1053|             for d in _vt_domains
  1054|         )
  1055|         _vt_raw = fingerprint.get(
  1056|             "view_templates.extract_floor_structural_area_plans", {}
  1057|         ).get("raw_count", 0)
  1058|         _vt_not_template = fingerprint.get(
  1059|             "view_templates.extract_floor_structural_area_plans", {}
  1060|         ).get("debug_not_template", 0)
  1061|         _vt_templates_total = (_vt_raw or 0) - (_vt_not_template or 0)
  1062|         _vt_unrouted = _vt_templates_total - _vt_total_kept
  1063|         if _vt_unrouted > 0:
  1064|             runner_notes.append(
  1065|                 "view_templates: {} template(s) not routed to any domain "
  1066|                 "(unrecognized viewtype)".format(_vt_unrouted)
  1067|             )
  1068|     except Exception:
  1069|         pass
  1070| 
  1071|     # End total extraction timer
  1072|     try:
  1073|         _timing.end_timer("total_extraction")
  1074|     except Exception:
  1075|         pass
  1076| 
  1077|     # Clean up hashing module timing reference
  1078|     try:
  1079|         from core import hashing as _hashing_mod
  1080|         _hashing_mod._timing_collector = None
  1081|     except Exception:
  1082|         pass
  1083| 
  1084|     # PR5: merge collector counters into contract run_diag for acceptance verification
  1085|     try:
  1086|         _c = ctx.get("_collect")
  1087|         if _c is not None and hasattr(_c, "counters"):
  1088|             for _k, _v in dict(_c.counters).items():
  1089|                 run_diag["counters"][str(_k)] = int(_v)
  1090|     except Exception:
  1091|         # Do not change run outcome if diagnostics merge fails.
  1092|         pass
  1093| 
  1094|     # Merge timing report into run_diag before contract envelope is built.
  1095|     # total_run and total_serialization are not yet ended here — they will show 0.0,
  1096|     # which is acceptable. Domain timings are complete at this point.
  1097|     try:
  1098|         timing_report = _timing.get_report()
  1099|         if isinstance(timing_report, dict):
  1100|             run_diag["timings"] = timing_report
  1101|     except Exception:
  1102|         pass
  1103| 
  1104|     # Hash mode participates in stable surfaces; timing does not.
  1105| 
  1106|     # Authoritative contract (statuses live here; legacy payloads may still exist at top-level)
  1107|     run_status, run_diag = contracts.compute_run_status(contract_domains, base_run_diag=run_diag, treat_unsupported_as_degraded=False)
  1108|     fingerprint["_contract"] = contracts.new_run_envelope(
  1109|         schema_version=contracts.SCHEMA_VERSION,
  1110|         run_status=run_status,
  1111|         run_diag=run_diag,
  1112|         domains=contract_domains,
  1113|     )
  1114| 
  1115|     # Stable comparison + cohort-analysis surfaces
  1116|     # Must never throw (runner should remain usable even if these builders fail).
  1117|     try:
  1118|         fingerprint["_manifest"] = build_manifest(fingerprint)
  1119|     except Exception as e:
  1120|         contracts.add_bounded_error(
  1121|             run_diag,
  1122|             domain="_runner",
  1123|             status=contracts.DOMAIN_STATUS_DEGRADED,
  1124|             code="manifest_build_failed",
  1125|             message=str(e),
  1126|         )
  1127|         run_status2, run_diag2 = contracts.compute_run_status(contract_domains, base_run_diag=run_diag, treat_unsupported_as_degraded=False)
  1128|         fingerprint["_contract"] = contracts.new_run_envelope(
  1129|             schema_version=contracts.SCHEMA_VERSION,
  1130|             run_status=run_status2,
  1131|             run_diag=run_diag2,
  1132|             domains=contract_domains,
  1133|         )
  1134| 
  1135|     try:
  1136|         fingerprint["_features"] = build_features(fingerprint)
  1137|     except Exception as e:
  1138|         contracts.add_bounded_error(
  1139|             run_diag,
  1140|             domain="_runner",
  1141|             status=contracts.DOMAIN_STATUS_DEGRADED,
  1142|             code="features_build_failed",
  1143|             message=str(e),
  1144|         )
  1145|         run_status3, run_diag3 = contracts.compute_run_status(contract_domains, base_run_diag=run_diag, treat_unsupported_as_degraded=False)
  1146|         fingerprint["_contract"] = contracts.new_run_envelope(
  1147|             schema_version=contracts.SCHEMA_VERSION,
  1148|             run_status=run_status3,
  1149|             run_diag=run_diag3,
  1150|             domains=contract_domains,
  1151|         )
  1152| 
  1153|     # Canonicalize all domain record payloads to flat items shape.
  1154|     fingerprint = _canonicalize_all_domain_records(fingerprint)
  1155| 
  1156|     # Preserve the collector for post-extraction serialization timing at module scope.
  1157|     # This hidden key is removed before the final payload is emitted.
  1158|     fingerprint["_timing_collector"] = _timing
  1159| 
  1160|     # Back-compat: keep a pointer to domains map (same object shape as _contract.domains)
  1161|     fingerprint["_domains"] = contract_domains
  1162|     fingerprint["_notes"] = runner_notes
  1163| 
  1164|     return fingerprint
```
