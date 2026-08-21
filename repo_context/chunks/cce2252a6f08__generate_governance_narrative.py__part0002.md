# Chunk of tools/generate_governance_narrative.py

- Source relative path: `tools/generate_governance_narrative.py`
- Chunk: 2 of 17
- Original line range: 495-787
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _group1_scope_pair, load_client_sectors, load_corpus_counts, _has_renderable_cascade_signal
- Source SHA-256: 7a8e1def8713100e21b852dc24c2c714a8e5330815affb3e703ed572a2829d9c
- Starts inside symbol: no
- Ends inside symbol: no

```
   495| def _group1_scope_pair(row: dict) -> tuple[str, str, str]:
   496|     """Classify a Group 1 row's two sides into (scope_a, scope_b, scope_pair_key).
   497| 
   498|     Reuses _target_scope_label() for each side's SHAPE (which dimensions are
   499|     populated) -- unlike Group 2, BOTH sides matter here, since neither side
   500|     of a Group 1 pair is gated to a fixed role population. But
   501|     _target_scope_label() alone only tells you the shape, not the VALUE: two
   502|     segments that are both e.g. "bc"-shaped (business_center_label populated,
   503|     client/discipline blank) could have DIFFERENT business_center_label
   504|     values -- discover_within_segment() in compare_cross_segment.py pairs
   505|     same-parent, same-unit Template/Container/Project segments without
   506|     checking that scope label values match, so a BC_1-scoped segment paired
   507|     against a BC_2-scoped segment is a real, producer-side-reachable shape,
   508|     not just a hypothetical. Silently bucketing that under "bc::bc" would
   509|     corrupt _has_group1_bc_pooled_evidence()'s "same business center" check
   510|     (compares mismatched-value rows as if they were one converged reading)
   511|     and mislabel the same disagreement as "business-center" evidence in
   512|     render_group1_scope_section()/detect_anomalies() when it's actually a
   513|     cross-value comparison.
   514| 
   515|     When both sides share an identical shape AND every field making up that
   516|     shape has an equal value on both sides, the pair is genuine same-value
   517|     evidence and gets the normal f"{scope_a}::{scope_b}" key (e.g. "bc::bc").
   518|     When the shapes match but any field's value differs, the pair is
   519|     captured under a distinct f"{scope_a}!cross::{scope_b}!cross" key instead
   520|     -- never discarded (this file's fail-soft-in-narrative posture), but
   521|     never conflated with same-value pooled evidence either. "!cross" cannot
   522|     collide with any of _target_scope_label()'s own outputs or the plain
   523|     "::"-joined keys (verified: it never appears in any of the 9 possible
   524|     _target_scope_label() return values).
   525|     """
   526|     scope_a = _target_scope_label(row, "a")
   527|     scope_b = _target_scope_label(row, "b")
   528|     fields = _SCOPE_DIMENSION_FIELDS.get(scope_a) if scope_a == scope_b else None
   529|     if fields and any(row.get(f"{f}_a", "") != row.get(f"{f}_b", "") for f in fields):
   530|         return scope_a, scope_b, f"{scope_a}!cross::{scope_b}!cross"
   531|     return scope_a, scope_b, f"{scope_a}::{scope_b}"
   532| 
   533| 
   534| # Default location for the optional client_sector.csv, resolved relative to this
   535| # script's own directory (tools/) rather than the CWD -- so existing invocations
   536| # that don't pass --client-sector use the shipped synthetic example mapping.
   537| # Deployments must pass their approved client-sector mapping explicitly to
   538| # classify real client labels. An explicit path (including a nonexistent one)
   539| # always overrides this default.
   540| _DEFAULT_CLIENT_SECTOR_PATH = Path(__file__).resolve().parent.parent / "policies" / "client_sector.csv"
   541| 
   542| # Interpretation-layer static reference docs (PR4 -- see D-022 and
   543| # docs/governance_evidence_package.md). Neither is written by this generator;
   544| # both are human/LLM-authored discovery-scaffold documents checked into the
   545| # repo, referenced from the evidence map/narrative as sibling artifacts
   546| # (never parsed -- presence is checked via Path.exists() only, same
   547| # convention as the never-consumed sibling CSVs cross_segment_file_pairs.csv/
   548| # comparison_registry.csv). Versions here must be bumped by hand alongside
   549| # the corresponding doc's own version header if its content changes in a way
   550| # that matters for a reader relying on a specific version. Kept in their own
   551| # docs/governance/ subfolder, separate from the rest of docs/, since these
   552| # four (and only these four) are also copied into --out at the end of a run
   553| # (see main()) so the generated package is self-contained/portable -- see
   554| # DECISIONS.md D-034.
   555| _DOCS_DIR = Path(__file__).resolve().parent.parent / "docs" / "governance"
   556| INTERPRETATION_GUIDE_PATH = _DOCS_DIR / "governance_interpretation_guide.md"
   557| QUESTION_ROUTES_PATH = _DOCS_DIR / "governance_question_routes.md"
   558| READING_ORDER_PATH = _DOCS_DIR / "governance_reading_order.md"
   559| CLASSIFICATION_RULES_PATH = _DOCS_DIR / "governance_classification_rules.md"
   560| INTERPRETATION_GUIDE_VERSION = "0.1"
   561| QUESTION_ROUTES_VERSION = "0.1"
   562| READING_ORDER_VERSION = "0.1"
   563| CLASSIFICATION_RULES_VERSION = "0.1"
   564| 
   565| 
   566| def load_client_sectors(client_sector_rows: Optional[list[dict]]) -> dict:
   567|     """Build a {client_label: sector} map from an optional client_sector.csv
   568|     (--client-sector). Sector membership is a real business fact that cannot be
   569|     derived from the fingerprint pipeline's own data -- nothing in
   570|     segment_manifest.csv/file_metadata.csv encodes "sector" -- so it lives in an
   571|     editable data file instead of a Python literal. See
   572|     docs/governance_narrative_scope_gap_audit.md C7.
   573| 
   574|     Absent input (file not supplied) returns an empty map: every client is then
   575|     "unknown" (not healthcare, not non-comparable) and falls through to normal
   576|     cross-client alignment tiering -- there is no special-cased client name.
   577|     """
   578|     if not client_sector_rows:
   579|         return {}
   580|     sector_map = {}
   581|     for row in client_sector_rows:
   582|         client = _pick(row, "client_label")
   583|         sector = _pick(row, "sector").strip().lower()
   584|         if client and sector:
   585|             sector_map[client] = sector
   586|     return sector_map
   587| 
   588| 
   589| def load_corpus_counts(
   590|     summary_rows: list[dict],
   591|     file_meta_rows: Optional[list[dict]],
   592| ) -> dict:
   593|     """Return counts of files by role, and disciplines/clients present."""
   594|     counts = {"Template": 0, "Container": 0, "Project": 0, "total": 0}
   595|     disciplines = set()
   596|     clients = set()
   597| 
   598|     if file_meta_rows:
   599|         for r in file_meta_rows:
   600|             role = r.get("governance_role", "")
   601|             if role in counts:
   602|                 counts[role] += 1
   603|             d = r.get("discipline_label", "")
   604|             if d:
   605|                 disciplines.add(d)
   606|             c = r.get("client_label", "")
   607|             if c:
   608|                 clients.add(c)
   609|         counts["total"] = counts["Template"] + counts["Container"] + counts["Project"]
   610|     else:
   611|         # Infer from within_project rows
   612|         seen = {}
   613|         for r in summary_rows:
   614|             if r["comparison_type"] == "within_project" and r["segment_id_a"] == r["segment_id_b"]:
   615|                 seg = r["segment_id_a"]
   616|                 role = seg.split("|")[1] if "|" in seg else ""
   617|                 if role in counts:
   618|                     n = int(r["n_files_a"]) if r["n_files_a"] else 0
   619|                     # take largest (most inclusive) count per role
   620|                     if role not in seen or n > seen[role]:
   621|                         seen[role] = n
   622|         for role, n in seen.items():
   623|             if role in counts:
   624|                 counts[role] = n
   625|         counts["total"] = counts["Template"] + counts["Container"] + counts["Project"]
   626|         # Disciplines and clients from segment IDs
   627|         for r in summary_rows:
   628|             d = _pick(r, "discipline_label_a", "discipline_label_b")
   629|             if d:
   630|                 disciplines.add(d)
   631|             c = _pick(r, "client_label_a", "client_label_b")
   632|             if c:
   633|                 clients.add(c)
   634| 
   635|     counts["disciplines"] = disciplines
   636|     counts["clients"] = clients
   637|     return counts
   638| 
   639| 
   640| # ── build_cascade comparison_type coverage ──────────────────────────────────────
   641| # The full comparison_type vocabulary compare_cross_segment.py can emit splits into
   642| # four groups that need different treatment (see docs/governance_narrative_scope_gap_audit.md
   643| # A1). Every comparison_type value build_cascade can see must appear in exactly one
   644| # of these — the coverage check at the end of the main loop below warns on anything
   645| # that doesn't, so a future producer addition is never silently invisible again.
   646| 
   647| # Group 1 — already handled by the explicit branches in the main loop; directed
   648| # cross-role cascade stages (Template<->Container<->Project) plus the two
   649| # self/peer-comparison shapes (sibling_projects -> xc, within_project -> wp_*).
   650| CASCADE_GROUP1_TYPES = {
   651|     "template_to_container", "container_to_project", "template_to_project",
   652|     "parent_sibling_roles", "sibling_projects", "within_project",
   653|     # cross_client: purpose-built client-vs-client peer comparison (see
   654|     # discover_cross_client() in compare_cross_segment.py) feeding the same xc
   655|     # bucket as sibling_projects's healthcare-gated cross-client accidental
   656|     # overload, without that gate or the shared-parent requirement.
   657|     "cross_client",
   658| }
   659| 
   660| # Group 2 — one level up the cascade: Generic/Generic-Host (out-of-box Revit stock
   661| # content) into Template/Container/Project. This is the literal top rung of the
   662| # "Governance Cascade" diagram already printed in render_header() ("Generic /
   663| # Enterprise Baseline down-arrow [generic -> template/container/project
   664| # containment]") — an existing promise in the narrative's own output that was
   665| # never implemented before this pass.
   666| #
   667| # Scope decision (PR #350 review, revised -- Option C): compare_cross_segment.py
   668| # intentionally emits generic_to_template/_container/_project rows for client-/
   669| # discipline-/bc-scoped targets too, not only the single broadest one -- those
   670| # scoped rows are real baseline-propagation evidence. gt/gc/gp keep the SAME
   671| # single-broadest-pair semantics as tc/cp/tp (the GENERIC/reference side must
   672| # still pass _is_unscoped_segment -- one canonical enterprise-wide Generic
   673| # population -- and gt/gc/gp themselves are populated only from target rows whose
   674| # OWN scope is "enterprise"), avoiding the blend-distinct-scope-grains anti-
   675| # pattern this audit already fixed elsewhere (A2's pool_scope filter, A3's
   676| # governance-state blending). But the target (Template/Container/Project) side is
   677| # no longer gated to broadest-only: every other target scope level is captured,
   678| # not discarded, in gt_by_scope/gc_by_scope/gp_by_scope (see _target_scope_label()
   679| # and build_cascade()'s docstring) -- Option C from the original PR #350 review
   680| # discussion, implemented as its own follow-up once the tradeoff was accepted.
   681| # collection_label is still not a SUMMARY_FIELDS column (B6 residual gap after the
   682| # business_center_label addition); a collection-only-scoped target lands in the
   683| # "other_scoped" bucket rather than being silently mislabeled "enterprise".
   684| CASCADE_GROUP2_TYPES = {
   685|     "generic_to_template", "generic_to_container", "generic_to_project",
   686| }
   687| 
   688| # Group 3 — a different axis entirely (scope level: enterprise/bc/client standards
   689| # vs. Project), not one more cascade stage. Captured into the `cascade` dict under
   690| # new keys (ep/bp/eb/ec) using the same containment-extraction pattern as Group 1/2,
   691| # but deliberately NOT rendered, tiered, or anomaly-detected in this pass — that is
   692| # a future business-center-section design decision, not an extension of this
   693| # bug-fix prompt.
   694| CASCADE_GROUP3_TYPES = {
   695|     "enterprise_to_project", "bc_to_project", "enterprise_to_bc", "enterprise_to_client",
   696| }
   697| 
   698| # Group 3b — bc_to_bc: same producer as Group 3 (discover_governance_chain()'s
   699| # scope-level fan-out) but a genuinely different comparison shape. Group 3's
   700| # ep/bp/eb/ec pairs are directed (a is always the standards/reference side by
   701| # construction of the discovery loop); bc_to_bc pairs are symmetric peers --
   702| # two real business centers' same-role populations, with segment_id_a/b order
   703| # just an artifact of combinations(sorted(sids), 2) in discover_governance_
   704| # chain(), not a reference/target assignment. Captured only -- not rendered,
   705| # tiered, or anomaly-detected in this pass, same contract as Group 3.
   706| CASCADE_GROUP3B_TYPES = {"bc_to_bc"}
   707| 
   708| # Group 4 — known comparison types intentionally excluded from cascade, one reason
   709| # each (verified against compare_cross_segment.py's actual discovery functions, not
   710| # guessed):
   711| CASCADE_GROUP4_EXCLUDED_TYPES = {
   712|     "sibling_templates": (
   713|         "Same-role peer-to-peer comparison (Template vs Template), not a cross-role "
   714|         "directed cascade measurement. build_cascade only extracts a peer-similarity "
   715|         "signal from one role today (sibling_projects -> xc, restricted to a specific "
   716|         "client-pair filter) and does not generalise that pattern to other roles. "
   717|         "Whether/how Template-vs-Template consistency should be surfaced is a design "
   718|         "decision, not resolved by this pass."
   719|     ),
   720|     "sibling_containers": (
   721|         "Same defect/reason class as sibling_templates — same-role peer comparison, "
   722|         "no directed-cascade analog implemented for this role either."
   723|     ),
   724|     "sibling_generic": (
   725|         "Same-role peer comparison among Generic/Generic-Host segments. "
   726|         "_comparison_role_semantics() in compare_cross_segment.py already documents "
   727|         "that used-view is not meaningful for these pairs (all-view is primary); no "
   728|         "cascade-shaped (cross-role containment) signal applies to peer-vs-peer "
   729|         "Generic comparisons either."
   730|     ),
   731|     "sibling_segments": (
   732|         "Fallback bucket (discover_sibling_segments()'s ctype default) for peer "
   733|         "segments whose governance_role doesn't match template/container/project/"
   734|         "generic. By construction these aren't role-typed the way the cascade's "
   735|         "role buckets are, so there's no directed-cascade slot to route them into."
   736|     ),
   737|     "governance_chain": (
   738|         "Reserved vocabulary token in compare_cross_segment.py's DIRECTED_TYPES — "
   739|         "verified (grep) that no discovery function ever actually emits the literal "
   740|         "string \"governance_chain\" as a comparison_type; discover_governance_chain() "
   741|         "itself emits concrete generic_to_*/template_to_*/container_to_project types, "
   742|         "not this string. Nothing to feed into cascade under this name; kept here "
   743|         "only so the coverage check below doesn't flag it as unrecognized."
   744|     ),
   745|     "client_cross_bc": (
   746|         "Same-client, cross-business-center peer comparison (a real client's own "
   747|         "population compared to itself across the real business centers it "
   748|         "touches) emitted by discover_client_cross_bc() — same same-role/peer-not-"
   749|         "cascade reason class as sibling_templates/sibling_containers above "
   750|         "(bc_to_bc itself is no longer in this excluded set -- it has its own "
   751|         "capture-only branch, see CASCADE_GROUP3B_TYPES -- but client_cross_bc "
   752|         "is not extended to match; that remains a separate, unresolved decision). "
   753|         "The population-union aggregation fix this exclusion used to be "
   754|         "pending on has since shipped (compare_cross_segment.py's all_union_*/"
   755|         "used_union_* fields) and is now the adopted primary metric for "
   756|         "cross_client/sibling_projects/within_project (see xc_by_client/"
   757|         "wp_by_client in build_client_summary()); client_cross_bc itself is not "
   758|         "wired into any cascade or client-summary accumulator by that adoption — "
   759|         "routing it in remains a separate, unresolved design decision, not "
   760|         "something this file does implicitly by association."
   761|     ),
   762| }
   763| 
   764| # Group 1/2 signal keys -- a domain with data in at least one of these has something
   765| # to tier/render. A domain whose ONLY data is Group 3 (ep/bp/eb/ec) has no cascade-
   766| # stage signal at all; it must stay in the `cascade` dict (captured, per Group 3's
   767| # contract) but must NOT reach render_domain_tiers()/the domain summary CSV, which
   768| # only know how to tier/render Group 1/2 fields and would otherwise show it as a
   769| # spurious "Insufficient Evidence" row with every visible column blank.
   770| _CASCADE_RENDERABLE_SIGNAL_KEYS = ("tc", "cp", "tp", "xc", "wp_all", "tw", "gt", "gc", "gp")
   771| 
   772| 
   773| def _has_renderable_cascade_signal(d: dict) -> bool:
   774|     if any(d.get(k) is not None for k in _CASCADE_RENDERABLE_SIGNAL_KEYS):
   775|         return True
   776|     # tc_by_scope/cp_by_scope/tp_by_scope are always present as (possibly
   777|     # empty) dicts, never None, so they can't reuse the "is not None" check
   778|     # above -- that would trivially return True for every domain, including
   779|     # Group-3-only ones this function exists to exclude. A domain whose ONLY
   780|     # Group 1 signal is scoped (e.g. bc::bc) evidence -- no enterprise
   781|     # tc/cp/tp and no other Group 1/2 signal -- must still be renderable, or
   782|     # its TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE classification (see
   783|     # assign_tier()/_has_group1_bc_pooled_evidence()) would be computed but
   784|     # never shown in render_domain_tiers()/the domain summary CSV.
   785|     return any(d.get(k) for k in ("tc_by_scope", "cp_by_scope", "tp_by_scope"))
   786| 
   787| 
```
