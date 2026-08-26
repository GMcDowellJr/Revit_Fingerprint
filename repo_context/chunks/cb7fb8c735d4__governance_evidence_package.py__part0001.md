# Chunk of tools/governance_evidence_package.py

- Source relative path: `tools/governance_evidence_package.py`
- Chunk: 1 of 6
- Original line range: 1-515
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _utc_now_iso, write_json, build_findings_document, build_package_manifest, comparison_type_coverage, build_package_health, _artifact
- Source SHA-256: 2fece0426163550ef83e302b52b9f002b12123e12eb35430df07c3d1f4c4b1f3
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| """
     2| governance_evidence_package.py
     3| 
     4| Package-boundary layer for tools/generate_governance_narrative.py.
     5| 
     6| Builds the JSON manifest/health/evidence-map artifacts that make the
     7| governance narrative package's provenance, coverage, and navigation
     8| structure machine-legible, without touching any of the generator's
     9| deterministic calculations, thresholds, or CSV columns. This is Phase 1
    10| ("PR1") of a broader evidence-package refactor; structured findings
    11| (governance_findings.json) and policy externalization are deferred to
    12| later phases -- see docs/governance_evidence_package.md.
    13| 
    14| Design reference only: the authority-level vocabulary below is modeled on,
    15| but independently defined from, the discovery-scaffold vocabulary in the
    16| the design-reference evidence framework repository. That repository is explicitly
    17| not a finalized standard or schema, and this module does not import from it
    18| or depend on it at runtime -- these constants are this repo's own copy,
    19| chosen for cross-tool legibility.
    20| 
    21| All text fields (known_limitations, cannot_answer, etc.) in this module are
    22| mechanical/factual statements about what the code does, citing a specific
    23| function, line, or docs/governance_narrative_scope_gap_audit.md finding ID
    24| where relevant -- never an interpretive judgment about impact or severity.
    25| Severity/impact judgment belongs to a human reader or to a future
    26| governance_findings.json (PR2), not to this deterministic layer.
    27| """
    28| from __future__ import annotations
    29| 
    30| import csv
    31| import json
    32| from datetime import datetime, timezone
    33| from pathlib import Path
    34| from typing import Optional
    35| 
    36| 
    37| # ── package identity / schema versions ──────────────────────────────────────
    38| 
    39| PACKAGE_TYPE = "governance_evidence_package"
    40| PACKAGE_SCHEMA_VERSION = "1.0"
    41| EVIDENCE_MAP_SCHEMA_VERSION = "1.1"
    42| FINDINGS_SCHEMA_VERSION = "1.0"
    43| FILE_INVENTORY_SCHEMA_VERSION = "1.0"
    44| 
    45| GENERATOR_IDENTITY = "generate_governance_narrative.py"
    46| GENERATOR_ROLE = "deterministic_governance_narrative_generator"
    47| 
    48| # ── authority-level vocabulary ───────────────────────────────────────────────
    49| 
    50| AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE = "authoritative_deterministic_evidence"
    51| AUTHORITY_CONTROLLED_INTERPRETATION = "controlled_interpretation"
    52| AUTHORITY_CONVENIENCE_SUMMARY = "convenience_summary"
    53| AUTHORITY_USER_PROVIDED_NOTE = "user_provided_note"
    54| AUTHORITY_LLM_GENERATED_PROVISIONAL_INTERPRETATION = "llm_generated_provisional_interpretation"
    55| 
    56| AUTHORITY_LEVELS = {
    57|     AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
    58|     AUTHORITY_CONTROLLED_INTERPRETATION,
    59|     AUTHORITY_CONVENIENCE_SUMMARY,
    60|     AUTHORITY_USER_PROVIDED_NOTE,
    61|     AUTHORITY_LLM_GENERATED_PROVISIONAL_INTERPRETATION,
    62| }
    63| 
    64| # ── finding provenance vocabulary (epistemic provenance: origin/fidelity/ ────
    65| # ── authority/limits) ─────────────────────────────────────────────────────────
    66| # Names match the framework's four components of epistemic provenance
    67| # (patterns/deterministic_to_llm_boundary.md in the design-reference-only
    68| # llm_evidence_framework repo). Every finding in governance_findings.json is
    69| # derived from deterministic computation over already-authoritative CSV data
    70| # (build_cascade()/build_client_summary()/assign_tier() outputs), so origin
    71| # and fidelity are constant across all findings this generator produces.
    72| 
    73| FINDING_ORIGIN_DETERMINISTIC_COMPUTATION = "deterministic_computation"
    74| FINDING_FIDELITY_EXACT = "exact"
    75| FINDING_STATUS_SUPPORTED = "supported"
    76| FINDING_STATUS_QUESTION_NOT_CLAIM = "question_not_claim"
    77| 
    78| FINDING_TYPES = {
    79|     "baseline_candidate",
    80|     "strong_baseline_candidate",
    81|     "local_review_required",
    82|     "high_fragmentation",
    83|     "active_local_practice",
    84|     "cross_client_convergence",
    85|     "low_client_coherence",
    86|     "passive_inheritance_risk",
    87|     "missing_or_degraded_evidence",
    88|     "leadership_question",
    89| }
    90| 
    91| 
    92| def _utc_now_iso() -> str:
    93|     # Matches compare_cross_segment.py's own executed_utc stamping convention
    94|     # (see its `datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")`).
    95|     return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    96| 
    97| 
    98| def write_json(path: Path, payload: dict) -> None:
    99|     path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
   100| 
   101| 
   102| def build_findings_document(findings: list, schema_version: str = FINDINGS_SCHEMA_VERSION) -> dict:
   103|     """Wrap a list of finding dicts (already built by the caller -- domain-
   104|     governance classification logic stays in generate_governance_narrative.py,
   105|     which owns TIER_*/PASSIVE_INHERITANCE_RISK_DOMAINS/assign_tier()) in the
   106|     same schema_version-tagged envelope used by the other three package
   107|     artifacts. Pure function -- no filesystem I/O, no re-derivation of the
   108|     findings themselves.
   109|     """
   110|     for f in findings:
   111|         if f.get("finding_type") not in FINDING_TYPES:
   112|             raise ValueError(f"unknown finding_type: {f.get('finding_type')!r}")
   113|     return {"schema_version": schema_version, "findings": findings}
   114| 
   115| 
   116| # ── package manifest ─────────────────────────────────────────────────────────
   117| 
   118| def build_package_manifest(
   119|     *,
   120|     generator_identity: str,
   121|     generator_role: str,
   122|     package_schema_version: str,
   123|     analysis_date: str,
   124|     input_paths: dict,          # artifact_id -> Optional[Path]
   125|     input_required: dict,       # artifact_id -> bool
   126|     input_roles: dict,          # artifact_id -> authority_level string
   127|     output_paths: dict,         # artifact_id -> Path (already written to disk)
   128|     output_types: dict,         # artifact_id -> artifact_type string
   129|     output_authority: dict,     # artifact_id -> authority_level string
   130|     output_context_role: dict,  # artifact_id -> context_role string
   131|     policy_dir: Optional[Path],
   132|     comparison_run_ids: list,
   133|     source_executed_utc: list,
   134|     policy_profiles: Optional[dict] = None,
   135| ) -> dict:
   136|     """Pure function: reads only Path.exists()/Path.stat() for already-written
   137|     output files. Does not open or parse any input CSV. Never claims a content
   138|     hash or source-run identifier that isn't actually present in the loaded
   139|     rows (comparison_run_ids/source_executed_utc are read from those rows by
   140|     the caller, not invented here).
   141| 
   142|     policy_profiles: optional {"thresholds": {"profile_id":..., "schema_version":...,
   143|     "source": "policy_file"|"built_in_default"}, "domain_policy": {...}, ...} --
   144|     the resolved profile_id/schema_version/source tools/governance_policy.py's
   145|     load_governance_policy() actually used for this run, one entry per policy
   146|     profile (thresholds, domain_policy, client_onboarding, finding_rules).
   147|     Omitted (None) reproduces PR1's original "not yet read" wording for a
   148|     caller that hasn't adopted policy loading -- callers built on
   149|     generate_governance_narrative.py's PR3 always pass this.
   150|     """
   151|     inputs = []
   152|     for artifact_id, path in input_paths.items():
   153|         present = bool(path) and path.exists()
   154|         inputs.append({
   155|             "artifact_id": artifact_id,
   156|             "path": str(path) if path else None,
   157|             "required": bool(input_required.get(artifact_id, False)),
   158|             "present": present,
   159|             "role": input_roles.get(artifact_id, AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE),
   160|         })
   161| 
   162|     outputs = []
   163|     for artifact_id, path in output_paths.items():
   164|         exists = path.exists()
   165|         outputs.append({
   166|             "artifact_id": artifact_id,
   167|             "path": str(path),
   168|             "artifact_type": output_types.get(artifact_id, "unknown"),
   169|             "authority_level": output_authority.get(artifact_id),
   170|             "context_role": output_context_role.get(artifact_id, ""),
   171|             "present": exists,
   172|             "size_bytes": path.stat().st_size if exists else None,
   173|         })
   174| 
   175|     missing_required = [i["artifact_id"] for i in inputs if i["required"] and not i["present"]]
   176| 
   177|     return {
   178|         "package_type": PACKAGE_TYPE,
   179|         "package_schema_version": package_schema_version,
   180|         "generated_at": _utc_now_iso(),
   181|         "analysis_date": analysis_date,
   182|         "generator": {
   183|             "name": generator_identity,
   184|             "role": generator_role,
   185|         },
   186|         "inputs": inputs,
   187|         "outputs": outputs,
   188|         "corpus_scope": {
   189|             "comparison_run_ids": comparison_run_ids,
   190|             "source_executed_utc": source_executed_utc,
   191|         },
   192|         "policy_profiles": {
   193|             "policy_dir": str(policy_dir) if policy_dir else None,
   194|             "profiles": policy_profiles or {},
   195|             "note": (
   196|                 "Governance thresholds, domain-governance policy (excluded/"
   197|                 "passive-inheritance-risk domains, domain guidance text), "
   198|                 "client-onboarding interpretation thresholds, anomaly/note "
   199|                 "materiality thresholds, and finding-rule documentation are "
   200|                 "loaded from --policy-dir (default: policies/governance/) via "
   201|                 "tools/governance_policy.py at run time -- see the `profiles` "
   202|                 "field above for which profile_id/schema_version/source "
   203|                 "(policy_file vs. built_in_default) was actually used for "
   204|                 "each of the five profiles this run. Most shipped values in "
   205|                 "policies/governance/*.json reproduce this generator's "
   206|                 "pre-externalization Python literals exactly; the exception "
   207|                 "is anomaly_thresholds.json's union_breadth_* keys (D-033), "
   208|                 "which are newly introduced thresholds with no prior inline "
   209|                 "literal to reproduce. Overriding --policy-dir with a "
   210|                 "different profile set changes classification output."
   211|             ) if policy_profiles else (
   212|                 "Policy externalization (thresholds, domain-governance policy, "
   213|                 "onboarding rules) is not yet implemented in this generator -- "
   214|                 "deferred to a future PR. This field records the --policy-dir "
   215|                 "value given, if any, for forward-compatibility auditing only; "
   216|                 "it is not read by this generator."
   217|             ),
   218|         },
   219|         "package_status": "incomplete" if missing_required else "complete",
   220|     }
   221| 
   222| 
   223| # ── comparison-type coverage (shared shape) ──────────────────────────────────
   224| 
   225| def comparison_type_coverage(
   226|     seen: set,
   227|     known: set,
   228|     intentionally_excluded: Optional[set] = None,
   229| ) -> dict:
   230|     """Pure classification of a set of observed comparison_type values against
   231|     a known/intentionally-excluded vocabulary. Shape matches the task spec's
   232|     package-health comparison_type_coverage schema:
   233|       seen / recognized / intentionally_excluded / unrecognized.
   234|     """
   235|     intentionally_excluded = intentionally_excluded or set()
   236|     seen_clean = {s for s in seen if s}
   237|     unrecognized = seen_clean - known
   238|     excluded_seen = seen_clean & intentionally_excluded
   239|     recognized = (seen_clean & known) - excluded_seen
   240|     return {
   241|         "seen": sorted(seen_clean),
   242|         "recognized": sorted(recognized),
   243|         "intentionally_excluded": sorted(excluded_seen),
   244|         "unrecognized": sorted(unrecognized),
   245|     }
   246| 
   247| 
   248| # ── package health ────────────────────────────────────────────────────────────
   249| 
   250| def build_package_health(
   251|     *,
   252|     schema_version: str,
   253|     schema_detection: str,               # "dual" | "single" | "none"
   254|     used_view_fallback: bool,
   255|     comparison_type_coverage_by_fn: dict,  # {"build_cascade": {...}, "build_governance_state_summary": {...}}
   256|     required_inputs: dict,               # artifact_id -> bool present
   257|     optional_inputs: dict,               # artifact_id -> bool present
   258|     client_sector_status: str,           # "explicit_path" | "default_path_resolved" |
   259|                                           # "default_path_missing" | "explicit_path_missing"
   260|     domain_csv_row_count: int,
   261|     domain_rows_excluded_no_signal: int,
   262|     client_csv_row_count: int,
   263|     corpus_project_file_count: int,
   264|     excluded_from_scoring: list,
   265|     unit_systems_seen: list,
   266|     matrix_manifest_row_count: int,
   267|     matrix_names_seen: list,
   268|     policy_load_status: Optional[dict] = None,  # tools/governance_policy.py's load_status
   269|     comparison_completeness: Optional[dict] = None,  # D-032: build_comparison_completeness()'s result
   270|     interpretation_guide_present: Optional[bool] = None,  # D-030/D-034: INTERPRETATION_GUIDE_PATH.exists()
   271| ) -> dict:
   272|     """All text below is mechanical/factual only -- see module docstring.
   273| 
   274|     policy_load_status: optional {"thresholds": {"source": "policy_file"|
   275|     "built_in_default", "path":..., "reason":...}, "domain_policy": {...}, ...}
   276|     -- load_governance_policy()'s per-profile load_status for this run.
   277|     Omitted (None, the default) adds no policy-related warning, so a caller
   278|     that hasn't adopted policy loading gets identical health output to
   279|     before this parameter existed.
   280| 
   281|     comparison_completeness: optional {domain: {"total":.., "present":..,
   282|     "missing":.., "stale":..}}, from generate_governance_narrative.py's
   283|     build_comparison_completeness() (D-032). Omitted (None, the default,
   284|     when --comparison-registry wasn't supplied) adds no
   285|     comparison_completeness key to the returned dict at all -- matching
   286|     every other optional-input field's "omit rather than blank-render"
   287|     convention in this module.
   288| 
   289|     interpretation_guide_present: optional bool, whether the checked-in
   290|     docs/governance/governance_interpretation_guide.md was found on disk for
   291|     this run. PR review finding: this artifact's evidence-map entry carries
   292|     required_before_conclusions=True (D-030), but its presence is not
   293|     otherwise tracked anywhere health inspects -- in a stripped deployment
   294|     missing docs/, the prerequisite gate becomes unsatisfiable with no
   295|     signal in the package's own health-first flow. Omitted (None, the
   296|     default) adds no warning, so a caller that hasn't threaded this through
   297|     gets identical health output to before this parameter existed; pass
   298|     False explicitly to surface the gap.
   299|     """
   300|     blocking_conditions = []
   301|     missing_required = sorted(k for k, present in required_inputs.items() if not present)
   302|     if missing_required:
   303|         blocking_conditions.append({
   304|             "condition": "missing_required_input",
   305|             "detail": f"Required input(s) not present: {missing_required}",
   306|         })
   307| 
   308|     warnings = []
   309|     fallbacks_used = []
   310|     if used_view_fallback:
   311|         fallbacks_used.append("used_view_falls_back_to_legacy")
   312|         warnings.append({
   313|             "condition": "used_view_fallback",
   314|             "detail": (
   315|                 "used_view_falls_back_to_legacy() returned True: canonical "
   316|                 "used-view columns resolved to legacy all-view column names "
   317|                 "in _SUMMARY_COL_ALIASES."
   318|             ),
   319|         })
   320|     for fn_name, coverage in comparison_type_coverage_by_fn.items():
   321|         if coverage.get("unrecognized"):
   322|             warnings.append({
   323|                 "condition": "unrecognized_comparison_type",
   324|                 "detail": (
   325|                     f"{fn_name}: comparison_type value(s) not in the known "
   326|                     f"vocabulary for that function: {coverage['unrecognized']}"
   327|                 ),
   328|             })
   329|     if client_sector_status == "default_path_missing":
   330|         warnings.append({
   331|             "condition": "client_sector_default_path_missing",
   332|             "detail": (
   333|                 "The default --client-sector path (policies/client_sector.csv) "
   334|                 "does not exist on disk; every client_label is treated as "
   335|                 "unclassified sector by load_client_sectors()."
   336|             ),
   337|         })
   338|     elif client_sector_status == "explicit_path_missing":
   339|         warnings.append({
   340|             "condition": "client_sector_explicit_path_missing",
   341|             "detail": (
   342|                 "--client-sector was given an explicit path that does not "
   343|                 "exist; every client_label is treated as unclassified sector "
   344|                 "by load_client_sectors()."
   345|             ),
   346|         })
   347| 
   348|     policy_load_status = policy_load_status or {}
   349|     policy_profiles_defaulted = sorted(
   350|         name for name, status in policy_load_status.items()
   351|         if status.get("source") == "built_in_default"
   352|     )
   353|     if policy_profiles_defaulted:
   354|         fallbacks_used.append("governance_policy_built_in_default")
   355|         warnings.append({
   356|             "condition": "governance_policy_profile_defaulted",
   357|             "detail": (
   358|                 "Governance policy profile(s) not found under --policy-dir; "
   359|                 f"this generator's own built-in default was used instead: "
   360|                 f"{policy_profiles_defaulted}. See governance_package_manifest.json's "
   361|                 "policy_profiles.profiles for the resolved profile_id/schema_version "
   362|                 "of each profile actually applied."
   363|             ),
   364|         })
   365| 
   366|     # PR review finding: governance_interpretation_guide carries
   367|     # required_before_conclusions=True, but nothing in health tracked its
   368|     # actual presence -- a stripped deployment missing docs/ could report
   369|     # overall_status "complete" while the prerequisite gate is unsatisfiable.
   370|     if interpretation_guide_present is False:
   371|         warnings.append({
   372|             "condition": "reasoning_prerequisite_absent",
   373|             "detail": (
   374|                 "governance_interpretation_guide (required_before_conclusions=true) "
   375|                 "was not found on disk for this run -- reasoning_prerequisites in "
   376|                 "governance_evidence_map.json cannot be satisfied."
   377|             ),
   378|         })
   379| 
   380|     # PR review finding: comparison_completeness was only attached to the
   381|     # returned dict AFTER overall_status was already computed, so a package
   382|     # with missing/stale comparison-registry entries still reported
   383|     # "complete" -- a consumer following the documented health-first flow
   384|     # would wrongly conclude nothing limits interpretation. Fold any
   385|     # domain with a missing/stale count into warnings before computing
   386|     # overall_status, the same as every other degrading condition above.
   387|     if comparison_completeness:
   388|         domains_with_gaps = sorted(
   389|             dom for dom, c in comparison_completeness.items()
   390|             if c.get("missing", 0) or c.get("stale", 0)
   391|         )
   392|         if domains_with_gaps:
   393|             warnings.append({
   394|                 "condition": "comparison_registry_gaps",
   395|                 "detail": (
   396|                     "comparison_completeness has missing and/or stale "
   397|                     f"comparison-registry entries for domain(s): {domains_with_gaps}. "
   398|                     "See health.comparison_completeness for per-domain counts."
   399|                 ),
   400|             })
   401| 
   402|     if missing_required:
   403|         overall_status = "invalid"
   404|     elif warnings:
   405|         overall_status = "degraded"
   406|     else:
   407|         overall_status = "complete"
   408| 
   409|     health = {
   410|         "schema_version": schema_version,
   411|         "overall_status": overall_status,
   412|         "required_inputs": required_inputs,
   413|         "optional_inputs": optional_inputs,
   414|         "schema_detection": schema_detection,
   415|         "used_view_fallback": used_view_fallback,
   416|         "fallbacks_used": fallbacks_used,
   417|         "comparison_type_coverage": comparison_type_coverage_by_fn,
   418|         "client_sector_status": client_sector_status,
   419|         "policy_load_status": policy_load_status,
   420|         "scope_coverage": {
   421|             "unit_systems_seen": unit_systems_seen,
   422|             "note": (
   423|                 "Detailed scope-blending/comparability gating (deterministic "
   424|                 "comparable/weakly_comparable/not_comparable statuses) is "
   425|                 "deferred to a future PR; this field is a factual inventory "
   426|                 "of unit_system values observed in cross_segment_summary.csv "
   427|                 "only."
   428|             ),
   429|         },
   430|         "matrix_manifest": {
   431|             "present": matrix_manifest_row_count > 0,
   432|             "row_count": matrix_manifest_row_count,
   433|             "matrix_names_seen": matrix_names_seen,
   434|             "note": (
   435|                 "matrix_output_manifest.csv (MATRIX_MANIFEST_FIELDS in "
   436|                 "compare_cross_segment.py) has no structured block/status "
   437|                 "column today -- only matrix_name/known_limitations/"
   438|                 "interpretation free-text fields. This generator does not "
   439|                 "parse or classify per-matrix blocking status; see "
   440|                 "docs/governance_generator_cross_compare_coverage.md."
   441|             ),
   442|         },
   443|         "domain_csv_row_count": domain_csv_row_count,
   444|         "domain_rows_excluded_no_signal": domain_rows_excluded_no_signal,
   445|         "client_csv_row_count": client_csv_row_count,
   446|         "corpus_project_file_count": corpus_project_file_count,
   447|         "excluded_from_scoring": excluded_from_scoring,
   448|         "blocking_conditions": blocking_conditions,
   449|         "warnings": warnings,
   450|     }
   451|     if comparison_completeness is not None:
   452|         health["comparison_completeness"] = comparison_completeness
   453|     return health
   454| 
   455| 
   456| # ── evidence map ──────────────────────────────────────────────────────────────
   457| 
   458| def _artifact(
   459|     artifact_id, path, artifact_type, required, present, producer, authority_level,
   460|     context_role, grain, key_fields, identifiers, join_keys, can_answer, cannot_answer,
   461|     known_limitations, null_semantics, related_artifacts, *, required_before_conclusions,
   462|     schema_version=None, output_local_path=None,
   463| ):
   464|     # required_before_conclusions is keyword-only with no default (D-030): every
   465|     # call site must state explicitly whether a governance conclusion drawn
   466|     # without this artifact would be unsafe, so a future artifact addition can't
   467|     # silently inherit a wrong default -- see build_evidence_map()'s
   468|     # reasoning_prerequisites and docs/governance/governance_reading_order.md.
   469|     entry = {
   470|         "artifact_id": artifact_id,
   471|         "path": path,
   472|         "artifact_type": artifact_type,
   473|         "required": required,
   474|         "present": present,
   475|         "producer": producer,
   476|         "authority_level": authority_level,
   477|         "context_role": context_role,
   478|         "grain": grain,
   479|         "key_fields": key_fields,
   480|         "identifiers": identifiers,
   481|         "join_keys": join_keys,
   482|         "can_answer": can_answer,
   483|         "cannot_answer": cannot_answer,
   484|         "known_limitations": known_limitations,
   485|         "null_semantics": null_semantics,
   486|         "related_artifacts": related_artifacts,
   487|         "required_before_conclusions": required_before_conclusions,
   488|     }
   489|     if schema_version is not None:
   490|         entry["schema_version"] = schema_version
   491|     # PR review finding: an automated consumer following reasoning_
   492|     # prerequisites needs to actually locate the artifact from a portable
   493|     # --out directory (no repo checkout) -- path/present alone describe the
   494|     # checked-in repo doc (D-034's deliberate source-of-truth choice), which
   495|     # is unresolvable from a moved --out. output_local_path names the D-034
   496|     # copy that sits beside this evidence map when present, without
   497|     # changing what path/present themselves describe. Deliberately
   498|     # package-relative (a bare filename, not out_dir-qualified) -- an
   499|     # absolute or original-machine path here would break the instant the
   500|     # whole --out directory is moved or copied, defeating the point.
   501|     if output_local_path is not None:
   502|         entry["output_local_path"] = output_local_path
   503|     return entry
   504| 
   505| 
   506| _BLANK_STRING_NULL_SEMANTICS = {
   507|     "*": (
   508|         "Blank string ('') for not-applicable/not-computed numeric fields "
   509|         "(e.g. single-view schema rows lack used_*/bundle_* columns); there "
   510|         "is no explicit null marker in this CSV -- consumers must treat '' "
   511|         "as missing, not 0."
   512|     ),
   513| }
   514| 
   515| 
```
