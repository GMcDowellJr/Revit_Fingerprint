# Chunk of DECISIONS.md

- Source relative path: `DECISIONS.md`
- Chunk: 2 of 5
- Original line range: 400-792
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: 8ed07306f5b9f68e40e1373d6eb567f822e92ba18bed964edfc92c80cb0cb774
- Starts inside symbol: no
- Ends inside symbol: no

```
   400| - `dimension_types` → 7 domains (linear, angular, radial, diameter, spot_elevation,
   401|   spot_coordinate, spot_slope)
   402| - `view_templates` → 5 domains (floor_structural_area_plans, ceiling_plans,
   403|   elevations_sections_detail, renderings_drafting, schedules)
   404| - `object_styles` → 4 domains (model, annotation, analytical, imported)
   405| - `fill_patterns` → 2 domains (drafting, model)
   406| - `arrowheads` — record class corrections only, no split
   407| - `line_patterns` — lp.is_import added to coordination_items, no split
   408| ### Cross-domain alignment
   409| Alignment keys — fields shared across domains within a family that governance
   410| expects to be consistent — are defined in `policies/cross_domain_alignment_keys.json`.
   411| Cross-domain alignment scoring is a BI/analysis concern, not an extraction concern.
   412| Extractor changes are not required to enable cross-domain alignment analysis.
   413| ### Consequences
   414| - All hashes from previous exports are obsolete. Full re-extraction required.
   415| - 28 domains replace 4 monolithic extractors plus corrections to 2 others.
   416| - `run_extract_all`, `phase1_probe_config`, `contracts/domain_identity_keys_v2.json`
   417|   updated with new domain names.
   418| - Power BI domain family grouping and alignment measures to be implemented separately.
   419| - Future consolidation: separate extractor files per domain will be refactored into
   420|   one file per domain family with internal routing. Deferred until all domains are
   421|   validated.
   422| 
   423| ---
   424| 
   425| ## D-016 — View Category Overrides Scope and Category Classification
   426| **Status:** Accepted
   427| **Date:** 2026-03-19
   428| ### Context
   429| View category overrides (VCO) can exist in three populations with different
   430| governance implications:
   431| 1. Template-controlled overrides: V/G checkbox checked, override differs from
   432|    object styles baseline. These are enforced on all views using the template.
   433| 2. Latent overrides: V/G checkbox unchecked, override set on the template.
   434|    Not enforced but would activate if the checkbox were checked.
   435| 3. View-local overrides: overrides set directly on individual views, either
   436|    on non-templated views or on views where the template does not control
   437|    that category.
   438| ### Decision
   439| Implement categories 1 and 2 as a domain family split:
   440| - `view_category_overrides_model` (CategoryType.Model)
   441| - `view_category_overrides_annotation` (CategoryType.Annotation)
   442| 
   443| `vco.include_controlled` is removed from VCO coordination_items. Include state is
   444| owned by view_templates via per-tab include flags:
   445| - `view_template.sig.include_vg_model`
   446| - `view_template.sig.include_vg_annotation`
   447| - `view_template.sig.include_vg_analytical`
   448| 
   449| VCO records now emit `vco.vg_tab` (`Model`/`Annotation`) and downstream tools
   450| derive category 1 vs category 2 by joining `vco.vg_tab` to the corresponding
   451| `view_template.sig.include_vg_<tab>` flag.
   452| 
   453| Category 3 is deferred.
   454| Category 2 records (latent overrides) remain included because a latent override
   455| that diverges from the standard is a governance risk: if the V/G checkbox is
   456| later checked, the non-standard override activates silently.
   457| ### Category 3 hooks
   458| When category 3 (view-local overrides) is implemented:
   459| - Add `vco.context_type = "view_local"` in coordination_items
   460|   (current records use `"template"`)
   461| - Add `vco.view_element_id` in unknown_items for traceability
   462| - No changes to category 1/2 records, join-key policy, or sig_hash
   463| ### Consequences
   464| - VCO model partition depends on `object_styles_model` ctx map; annotation
   465|   partition consumes `object_styles_annotation` ctx map when present
   466| - VCO reads view templates directly from the Revit API — it does NOT depend on
   467|   view_template_* domain extractors
   468| - Include control for governance filtering is sourced from view_templates and
   469|   joined via `vco.vg_tab` → `include_vg_<tab>` mapping
   470| - View-local overrides (category 3) may be a large population; implement with
   471|   record-count ceiling and non-default-only filter when deferred work begins
   472| 
   473| ---
   474| 
   475| ## D-017 — line_patterns Join Key Upgraded to Scale-Invariant Normalized Segments
   476| **Status:** Accepted
   477| **Date:** 2026-03-31
   478| ### Decision
   479| Upgrade `line_patterns` join identity from exact segment definition hash
   480| (`line_pattern.segments_def_hash`) to normalized segment ratio hash
   481| (`line_pattern.segments_norm_hash`) using `line_patterns.join_key.v3`.
   482| ### Rationale
   483| Governance identity for line patterns is structural type, not absolute scale.
   484| Observed outputs showed 2,083 exact-length variants where the governance-meaningful
   485| distinct population is estimated around 50–200 structural patterns. Scale variants
   486| such as Hidden 1/8 and Hidden 1/4 should resolve to one governance unit.
   487| ### Normalization rule
   488| - Preserve ordered segment kind sequence.
   489| - Normalize segment lengths by ratio relative to non-dot total length.
   490| - Dot segments use relative epsilon = 1% of non-dot total to keep dot participation
   491|   scale-invariant.
   492| - Pure-dot safeguard: if non-dot total is zero, use tiny fallback epsilon `1e-9`.
   493| ### Consequences
   494| - `line_pattern.segments_norm_hash` must be computed during flatten by default
   495|   (no opt-in flag required).
   496| - `line_pattern.segments_def_hash` remains emitted in identity evidence for forensic
   497|   analysis but is explicitly excluded from join participation.
   498| - Pattern cardinality should collapse materially for structurally equivalent,
   499|   differently-scaled line patterns.
   500| 
   501| ### Validation extension (accepted operating practice)
   502| - Precision sensitivity must be evaluated around the active normalization token
   503|   precision (currently `.6f`) using neighbor sweeps (typically ±2 decimals).
   504| - Precision selection is determined by elbow behavior: maximize collapse of
   505|   floating-noise fragmentation while preserving stable structural distinctions.
   506| - Evaluation should track not only unique hash count, but also split/merge
   507|   behavior by dominant labels and shape-sequence consistency.
   508| 
   509| ---
   510| 
   511| ## D-018 — loaded_family_types scope: loaded families only; system families deferred
   512| 
   513| ### Status
   514| Accepted (2026-05-13)
   515| 
   516| ### Context
   517| `loaded_family_types` extracts `FamilySymbol` records using the parameter-schema
   518| evidence model (`lft.*` / `lftp.*`). In practice, `FamilySymbol` collectors also
   519| surface system-family types (e.g. curtain panels, stacked walls, curtain walls,
   520| MEP system types) where `Family.IsEditable == False`. These appear in extraction
   521| output with `lft.family_is_editable: "false"` and `lft.type_name` often missing,
   522| causing `status: degraded` and `identity_quality: incomplete_missing`.
   523| 
   524| `compound_types` already covers some system families (standard wall, floor, ceiling
   525| types) via their specific `ElementType` subclasses and layer-structure semantics, but
   526| does not reach stacked walls, curtain walls, curtain systems, or MEP system types.
   527| 
   528| ### Decision
   529| `loaded_family_types` is scoped to user-loaded families only as its **governed
   530| primary audience**. System families are not filtered out at extraction time — they
   531| appear in output with `lft.family_is_editable: "false"` as a discrimination signal —
   532| but they are not governed under this domain's identity contract.
   533| 
   534| A dedicated `system_family_types` domain (or expansion of `compound_types`) to cover
   535| the remaining system families (stacked walls, curtain walls, curtain systems, MEP
   536| system types) is deferred until Phase-1 analysis establishes which categories are
   537| worth the extraction investment.
   538| 
   539| ### Consequences
   540| - No filter change to `loaded_family_types` extractor.
   541| - Downstream analysis should use `lft.family_is_editable` to segment loaded vs.
   542|   system records if needed.
   543| - `compound_types` gap (stacked walls, curtain walls) is a known open item.
   544| 
   545| ---
   546| 
   547| ## D-019 — Governance narrative evidence-package layer (Phase 1: manifest/health/evidence-map)
   548| 
   549| ### Status
   550| Accepted (2026-07-16)
   551| 
   552| ### Context
   553| `tools/generate_governance_narrative.py` produced three outputs
   554| (`governance_domain_summary.csv`, `governance_client_summary.csv`,
   555| `governance_narrative_context.md`) that conflated multiple epistemic roles:
   556| deterministic evidence, package-health/coverage reporting, interpretation
   557| guide, findings store, and executive narrative, with no explicit statement
   558| of which output carries which kind of authority. The generator's own footer
   559| also referenced a stale producer filename
   560| (`generate_governance_narrative_dod_aligned_v2.py`) that never matched the
   561| actual script name.
   562| 
   563| A companion discovery-scaffold repository (`GMcDowellJr/llm_evidence_framework`,
   564| explicitly not a finalized standard) documents a pattern for separating
   565| deterministic evidence from interpretation: an authority-level vocabulary
   566| (`authoritative_deterministic_evidence` / `controlled_interpretation` /
   567| `convenience_summary` / `user_provided_note` /
   568| `llm_generated_provisional_interpretation`) and an evidence-map shape
   569| (artifact_id/producer/authority_level/context_role/grain/can_answer/
   570| cannot_answer/known_limitations/null_semantics/related_artifacts).
   571| 
   572| ### Decision
   573| Add a package-boundary layer around the existing generator without changing
   574| any of its deterministic calculations, thresholds, or CSV columns:
   575| 
   576| - A new sibling module, `tools/governance_evidence_package.py`, defines the
   577|   authority-level vocabulary (independently, as this repo's own constants —
   578|   no import from or runtime dependency on `llm_evidence_framework`) and
   579|   builds three new JSON artifacts: `governance_package_manifest.json`
   580|   (provenance: inputs/outputs/comparison_run_ids), `governance_package_health.json`
   581|   (schema detection, used-view fallback, comparison_type coverage, blocking
   582|   conditions, warnings — all mechanical/factual text, no severity judgment),
   583|   and `governance_evidence_map.json` (one entry per artifact — the 10 CSVs
   584|   the generator reads via CLI args, 2 sibling CSVs it produces but never
   585|   reads (`cross_segment_file_pairs.csv`, `comparison_registry.csv`), and its
   586|   6 own generated artifacts, 18 total).
   587| - The narrative gains a new authority-header section stating its own
   588|   `controlled_interpretation` role and the authority ordering (package
   589|   health and source CSVs outrank rollup CSVs, which outrank narrative
   590|   prose), and the stale producer-identity footer is corrected to reference
   591|   the real script name via a shared `GENERATOR_IDENTITY` constant.
   592| - Structured findings (`governance_findings.json`) and policy externalization
   593|   (thresholds, domain-governance policy, onboarding rules into
   594|   `policies/governance/`) are explicitly deferred to later PRs — this
   595|   decision covers Phase 1 only.
   596| - `--emit-evidence-package` defaults to **on**: every existing invocation of
   597|   the generator starts producing the three new JSON files with no CLI change
   598|   required. `--no-emit-evidence-package`, `--policy-dir` (recorded but not
   599|   yet read), and `--package-schema-version` are additive, backward-compatible
   600|   CLI flags.
   601| 
   602| ### Consequences
   603| - Every run of `generate_governance_narrative.py` now writes 3 additional
   604|   JSON files by default, alongside the unchanged CSV/MD outputs.
   605| - `governance_domain_summary.csv` and `governance_client_summary.csv`'s
   606|   column sets, and all classification/scoring logic, are unchanged (locked
   607|   in by regression tests asserting the exact column lists).
   608| - `docs/governance_evidence_package.md` documents the artifact inventory,
   609|   authority ordering, and the "documented but not fixed in this phase"
   610|   limitations (the `governance_narrative_scope_gap_audit.md` A2 pool_scope
   611|   caveat, the "—" vs "" missing-value inconsistency in `governance_domain_summary.csv`,
   612|   and the C8 missing domain-label contract).
   613| - Downstream tooling that reads `generate_governance_narrative.py`'s output
   614|   directory will now find three new JSON files unless it opts out.
   615| 
   616| ---
   617| 
   618| ## D-020 — Governance narrative evidence-package layer (Phase 2: structured findings)
   619| 
   620| ### Status
   621| Accepted (2026-07-16)
   622| 
   623| ### Context
   624| D-019's package manifest/health/evidence-map layer made the governance
   625| package's provenance and coverage machine-legible, but the actual
   626| classification conclusions (which domains are baseline candidates, which
   627| show high fragmentation or passive-inheritance risk, which clients have low
   628| coherence) still existed only as prose sentences inside
   629| `governance_narrative_context.md`'s "Key Findings and Governance
   630| Recommendations" section, generated independently of any structured data
   631| model. A downstream reader (human or LLM) could not enumerate "every domain
   632| currently classified `baseline_candidate`" without parsing narrative text.
   633| 
   634| ### Decision
   635| Add `governance_findings.json`: one structured finding per (subject, rule)
   636| match, covering the ten required categories (`baseline_candidate`,
   637| `strong_baseline_candidate`, `local_review_required`, `high_fragmentation`,
   638| `active_local_practice`, `cross_client_convergence`, `low_client_coherence`,
   639| `passive_inheritance_risk`, `missing_or_degraded_evidence`,
   640| `leadership_question`). Each finding carries `finding_id`, `subject`
   641| (`type`/`id`), `finding_type`, `status`, `origin`, `fidelity`,
   642| `authority_level`, `summary`, `support[]` (`artifact_id` + `selector` +
   643| `fields`), `rule_ids[]`, and `limits[]` — the origin/fidelity/authority/
   644| limits fields are this repo's own vocabulary (`tools/governance_evidence_package.py`),
   645| modeled on but independent of the design-reference `llm_evidence_framework`
   646| repo's stated epistemic-provenance components.
   647| 
   648| A new `_classify_domains_for_findings()` in `generate_governance_narrative.py`
   649| is the single source of truth for tier-derived classification buckets,
   650| shared by `build_structured_findings()` (which produces the JSON) and
   651| `render_findings_and_recommendations()` (which now consumes the same
   652| findings list instead of recomputing an independent classification) — the
   653| two can no longer drift into disagreeing readings of the same underlying
   654| data. Leadership questions are marked `status: question_not_claim` /
   655| `authority_level: convenience_summary`, distinct from evidence findings
   656| (`status: supported`), so a suggested review question is never mistaken for
   657| an observed result.
   658| 
   659| ### Consequences
   660| - `governance_findings.json` becomes the 19th evidence-package artifact
   661|   (added to `governance_evidence_map.json`); no existing CSV column,
   662|   classification/scoring logic, or threshold changed.
   663| - A finding's `support[].artifact_id` always resolves to a real artifact and
   664|   `selector`/`fields` that exist on it — enforced by construction, since
   665|   both consumers read from the same `cascade`/`client_rows`/
   666|   `governance_state_summary` inputs used to write `governance_domain_summary.csv`/
   667|   `governance_client_summary.csv`.
   668| - No baseline finding is ever emitted for a domain whose primary metric is
   669|   unavailable: `assign_tier()` itself routes that domain to
   670|   `TIER_INSUFFICIENT` before `_classify_domains_for_findings()` runs, so
   671|   the gate is structural, not a separate check that could be forgotten.
   672| 
   673| ---
   674| 
   675| ## D-021 — Governance narrative evidence-package layer (Phase 3: policy externalization)
   676| 
   677| ### Status
   678| Accepted (2026-07-17)
   679| 
   680| ### Context
   681| D-019/D-020 made the governance package's provenance, health, and findings
   682| machine-legible, but the actual governance judgments underneath those
   683| findings — tier-assignment thresholds, reliability-band cutoffs,
   684| cross-client convergence/coherence thresholds, which domains are excluded
   685| from aggregate scoring, which are flagged as passive-inheritance risk, fixed
   686| editorial guidance text for specific domains, and client-onboarding
   687| interpretation thresholds — were still Python literals scattered across
   688| `generate_governance_narrative.py`. These are deterministic classification
   689| rules, not raw corpus observations, and the task's own framing distinguishes
   690| "authoritative deterministic evidence" from "controlled interpretation"
   691| (rule-derived classification on top of that evidence) — a rule's threshold
   692| value is itself part of the interpretation layer, not something a reader
   693| can audit or override without reading Python source.
   694| 
   695| ### Decision
   696| Move these values into four JSON policy profiles under
   697| `policies/governance/`: `governance_thresholds.json` (reliability bands,
   698| tier-assignment bands, cross-client convergence/coherence thresholds, client
   699| confidence bands), `domain_governance_policy.json`
   700| (`excluded_from_scoring`, `passive_inheritance_risk_domains`, per-domain
   701| `domain_guidance` text, and `static_findings_guidance` always rendered in
   702| the findings section), `client_onboarding_policy.json`
   703| (`_client_onboarding_profile()`'s interpretation thresholds — kept as a
   704| separate profile from `governance_thresholds.json` even where a default
   705| value numerically coincides, since these gate onboarding narrative text, not
   706| `governance_tier`), and `finding_rules.json` (documentation-only
   707| `rule_id → {finding_type, description}` metadata for D-020's `rule_ids`).
   708| 
   709| A new sibling module, `tools/governance_policy.py`, is a generic JSON
   710| policy-profile loader (mechanical load/fallback only — no governance
   711| business content of its own, mirroring `tools/governance_evidence_package.py`'s
   712| separation of the generic envelope layer from the domain-governance logic
   713| that stays in `generate_governance_narrative.py`). `--policy-dir` (accepted
   714| but inert since D-019) now defaults to `policies/governance/` and is
   715| actually read: `apply_governance_policy()` reassigns every module-level
   716| threshold/domain-policy constant this file's existing functions already
   717| read as plain globals (`EXCLUDED_FROM_SCORING`, `PASSIVE_INHERITANCE_RISK_DOMAINS`,
   718| `DOMAIN_GUIDANCE`, `STATIC_FINDINGS_GUIDANCE`, and ~25 threshold constants)
   719| from the resolved policy at the start of `main()`, so no existing function
   720| body or call site needed to change — only the *source* of each constant's
   721| value changed, from a Python literal to a policy-file-or-fallback lookup.
   722| The shipped `policies/governance/*.json` files reproduce this generator's
   723| pre-externalization Python literals value-for-value (verified by a
   724| regression test comparing on-disk JSON against the module's own
   725| `_POLICY_DEFAULTS`), so no existing invocation's output changes by default.
   726| A profile file missing from `--policy-dir` falls back, per file, to this
   727| generator's own built-in default for that profile only — reported in
   728| `governance_package_health.json`'s `policy_load_status`/`fallbacks_used`/
   729| `warnings` (a `governance_policy_profile_defaulted` warning degrades
   730| `overall_status`) and in `governance_package_manifest.json`'s
   731| `policy_profiles.profiles` (resolved `profile_id`/`schema_version`/`source`
   732| per profile).
   733| 
   734| ### Consequences
   735| - No governance classification output (tier assignment, reliability
   736|   banding, cross-client convergence/coherence tiering, onboarding narrative
   737|   text, excluded/passive-inheritance-risk domain sets, or the two
   738|   domain-specific/static findings-guidance sentences) changed for any
   739|   existing invocation — locked in by a regression test running `main()`
   740|   twice (default vs. explicit `--policy-dir policies/governance/`) and
   741|   asserting byte-identical `governance_domain_summary.csv` output.
   742| - A governance threshold, excluded/passive-inheritance-risk domain set, or
   743|   guidance sentence can now be changed by editing JSON under
   744|   `--policy-dir`, without a code change — verified with tests that override
   745|   one policy file and observe the corresponding classification/prose output
   746|   change (e.g. lowering `tier_strong_baseline_min` promotes a previously
   747|   `Investigate Before Baseline` domain to `Strong Baseline Candidate`).
   748| - Because the overridden constants are process-global module attributes
   749|   (not threaded through function signatures), every test that calls
   750|   `apply_governance_policy()` with a non-default policy must reset it
   751|   afterward (an autouse pytest fixture in
   752|   `tests/test_generate_governance_narrative_policy.py` does this) — a test
   753|   that forgot to reset could leak an overridden threshold into an unrelated
   754|   test file running later in the same pytest session. This is a known
   755|   trade-off of the "reassign existing module globals" approach chosen to
   756|   avoid threading a policy object through dozens of existing call sites in
   757|   one pass; a future phase may thread policy explicitly instead if this
   758|   proves fragile in practice.
   759| - `DOMAIN_LABELS` (human-readable domain display names) is **not**
   760|   externalized in this phase — it is a display-name contract issue (see the
   761|   evidence map's existing C8 known-limitation note), not a governance
   762|   threshold or policy rule, and remains a Python literal.
   763| 
   764| ---
   765| 
   766| ## D-022 — Governance narrative evidence-package layer (Phase 4: interpretation guide, question routes, governance brief)
   767| 
   768| ### Status
   769| Accepted (2026-07-17)
   770| 
   771| ### Context
   772| D-019/D-020/D-021 made the governance package's provenance, health,
   773| findings, and classification thresholds machine-legible and externally
   774| editable, but a reader (human or LLM) still had no dedicated place to learn
   775| *how to interpret* the package's metrics and classifications (what a tier
   776| does and doesn't mean, comparability rules, missing-value semantics,
   777| known bad inferences), no catalog of where to look for a recurring
   778| question, and no artifact shorter than the full `governance_narrative_context.md`
   779| for a quick top-line read of a specific run.
   780| 
   781| The design-reference `GMcDowellJr/llm_evidence_framework` repository
   782| (explicitly provisional, no runtime dependency) documents this gap as the
   783| "interpretation layer" and "question routing" artifact roles
   784| (`patterns/deterministic_to_llm_boundary.md`, `notes/current_thesis.md`),
   785| and a discovery scaffold for capturing question routes as they recur rather
   786| than inventing them upfront (`discovery/question_route_discovery.md`,
   787| `discovery/script_recipe_discovery.md`): "a route should not be codified
   788| just because it was imagined."
   789| 
   790| ### Decision
   791| Add three artifacts:
   792| 
```
