# Chunk of DECISIONS.md

- Source relative path: `DECISIONS.md`
- Chunk: 4 of 5
- Original line range: 1193-1591
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: 8ed07306f5b9f68e40e1373d6eb567f822e92ba18bed964edfc92c80cb0cb774
- Starts inside symbol: no
- Ends inside symbol: no

```
  1193| 
  1194| ### Status
  1195| Accepted (retroactive backfill — already-shipped, load-bearing architecture;
  1196| documented now as part of D-027's lineage-model audit)
  1197| 
  1198| ### Context
  1199| `tools/build_segment_manifest.py`'s `DIMENSION_CONFIG` (currently 5 entries:
  1200| `unit_system` as the root dimension, `governance_role` as the governance
  1201| dimension, `client_label`/`discipline_label`/`business_center_label` as cut
  1202| dimensions) and `_build_segments()` construct every segment as a subset —
  1203| via `itertools.combinations` over a `frozenset` key — of the declared
  1204| dimension values present on a row: the full powerset of non-root dimensions,
  1205| not a single-parent classification tree. Every segment carries one recorded
  1206| primary `parent_segment_id` (derived by dropping the last-declared present
  1207| field) for folder/registry bookkeeping, but a segment's true position in the
  1208| hierarchy is a lattice — it can have as many immediate structural parents as
  1209| it has non-root dimensions present, one per dropped field. This architecture
  1210| underlies every downstream segment/governance tool (`run_segment_
  1211| orchestrator.py`, `compare_cross_segment.py`, `governance_manifest.py`,
  1212| `compare_governance_populations.py`, etc.) and predates this decision-log
  1213| entry, which had no record of it.
  1214| 
  1215| ### Decision
  1216| Accepted as-is; this entry backfills documentation only; the architecture
  1217| itself is not being changed here (out of scope for this session — see
  1218| D-027). Recording it because D-027's lineage-completeness fix depends
  1219| directly on this shape: `parent_segment_id` is a single bookkeeping pointer,
  1220| not the segment's full ancestor set, which is exactly why a tree-walk over
  1221| it under-reports ancestors.
  1222| 
  1223| ### Consequences
  1224| - Any tool reasoning about segment hierarchy must treat `parent_segment_id`
  1225|   as one designated primary parent, not the segment's complete set of
  1226|   structural ancestors.
  1227| - `ancestor_segment_ids` (see D-027, D-028) is the field that actually
  1228|   carries the lattice's multi-parent structure.
  1229| 
  1230| ---
  1231| 
  1232| ## D-027 — `structural_ancestor` and `population_containment`: two independent lineage relations for comparison-discovery validity
  1233| 
  1234| ### Status
  1235| Accepted (2026-08-12)
  1236| 
  1237| ### Context
  1238| `tools/compare_cross_segment.py`'s `discover_*` functions generate segment
  1239| pairs for cross-segment comparison. A pair is invalid whenever one
  1240| segment's real file population contains the other's — comparing a segment
  1241| against data that already contains some or all of its own. Before this
  1242| decision, the only guard was `_is_lineage_related()`/`_build_ancestor_map()`,
  1243| which walked `parent_segment_id` as a single-parent chain — under-reporting
  1244| ancestors whenever a segment had more than one non-root dimension present
  1245| (D-026) — and had no empirical, non-dimensional counterpart at all.
  1246| 
  1247| A live audit against the real corpus (481 segments, `segment_manifest.csv` +
  1248| `segment_membership.csv`) found: (1) the dimension-lattice ("structural")
  1249| ancestor relation, once corrected to a full transitive closure, never
  1250| produces a false population-superset claim — 0 counterexamples across all
  1251| 115,440 segment pairs checked — but is incomplete: 1,806 of 4,080 real
  1252| population-subset pairs in the corpus have no dimensional explanation at
  1253| all; (2) `discover_sibling_segments()` carried no lineage guard of any kind
  1254| and emitted 101 real population-containment violations (`sibling_templates`/
  1255| `sibling_containers`/`sibling_projects`), via its `redundant_single_child`
  1256| resolution mechanism bucketing a structural ancestor and its own descendant
  1257| as if they were unrelated peers.
  1258| 
  1259| ### Decision
  1260| Lineage/containment is split into two explicitly separate,
  1261| independently-computed relations:
  1262| 
  1263| - **`structural_ancestor`** — the dimension-lattice relation
  1264|   (`_build_ancestor_map()`/`_is_lineage_related()`), now a full transitive
  1265|   closure computed by walking each segment's `ancestor_segment_ids` (D-028)
  1266|   as a multi-parent adjacency list, rather than a single
  1267|   `parent_segment_id` chain. Reliable (no known false positives on the
  1268|   audited corpus) but incomplete. Governs authority/provision semantics
  1269|   (e.g. `discover_governance_chain()`'s Template/Container→Project fan-out).
  1270| - **`population_containment`** — a new, empirically-derived relation
  1271|   computed directly from real `export_run_id` membership
  1272|   (`segment_membership.csv`), independent of any dimensional relationship.
  1273|   Threshold-gated by two Jenks-natural-breaks passes (`tools/jenks_utils.
  1274|   jenks_breaks()`, `n_classes=2` — the more general of the two independent
  1275|   Jenks implementations in this repo, reused rather than duplicated a
  1276|   third time) fit on real *non-structural* population-subset pairs: a
  1277|   size-noise floor (`min_population_for_containment`) and a
  1278|   containment-ratio floor (`min_containment_ratio`), so a
  1279|   materially-insignificant coincidental subset (e.g. a 1-file segment
  1280|   trivially "contained" in almost anything) is not mistaken for real
  1281|   inheritance. Computed thresholds and resulting candidate pairs are
  1282|   written to `population_containment_thresholds.csv` for human review, not
  1283|   silently baked into code — a first pass, not a locked policy.
  1284| 
  1285| `discover_sibling_segments()` checks BOTH relations before finalizing a
  1286| pair. On the audited corpus, `structural_ancestor`'s completeness fix alone
  1287| already resolves all 101 known violations — `population_containment` did
  1288| not independently flag any of them, since every one of the 101 turned out
  1289| to be dimensionally explained once the lattice closure was corrected.
  1290| `population_containment` is retained there anyway as a second, independent
  1291| guard against non-structural coincidental containment (which the corpus
  1292| audit shows exists as a general phenomenon in this data, distinct from and
  1293| not corpus-verified as an active `discover_sibling_segments` defect).
  1294| `discover_governance_chain()` keeps `structural_ancestor` only — it had
  1295| zero real violations both before and after the completeness fix, so layering
  1296| `population_containment` there too is deferred rather than speculative.
  1297| `discover_within_segment()`, `discover_cross_client()`,
  1298| `discover_client_cross_bc()`, and `discover_parent_siblings()` currently
  1299| carry neither guard; the audit found zero real violations from any of them
  1300| on the current corpus, but this is flagged as a known, currently-latent gap
  1301| (see the "Lineage/containment guard audit" comment in
  1302| `compare_cross_segment.py`) rather than fixed in this pass.
  1303| 
  1304| ### Consequences
  1305| - `discover_sibling_segments()`'s emitted pair count drops on any corpus
  1306|   where the defect fires (699 → 598 pairs on the audited corpus).
  1307| - A new output file, `population_containment_thresholds.csv`, is written by
  1308|   `compare_cross_segment.py` runs whenever `segment_membership.csv` is
  1309|   present.
  1310| - The Jenks-derived thresholds are a first-pass default, not a locked
  1311|   governance decision — the specific pairs they surface should get a human
  1312|   sanity check before being treated as final policy.
  1313| - Revisiting `discover_within_segment()` / `discover_cross_client()` /
  1314|   `discover_client_cross_bc()` / `discover_parent_siblings()` for the same
  1315|   guard is left open for a future session if corpus growth ever produces a
  1316|   real violation there.
  1317| 
  1318| ---
  1319| 
  1320| ## D-028 — `ancestor_segment_ids` serialization fix: `;`-joined, not `|`-joined
  1321| 
  1322| ### Status
  1323| Accepted (2026-08-12)
  1324| 
  1325| ### Context
  1326| `tools/build_segment_manifest.py`'s `segment_manifest.csv` column
  1327| `ancestor_segment_ids` joined a segment's list of ancestor segment_ids with
  1328| `"|"` — the same character `segment_id` itself uses internally to delimit
  1329| dimension values (e.g. `"imperial|Container|0000"`). Since each list
  1330| element is itself `"|"`-delimited, the outer `"|".join()` collided with the
  1331| inner delimiters: the resulting string cannot be losslessly split back into
  1332| the original ancestor-id list (two different real ancestor-id lists can
  1333| produce the identical serialized string). A repo-wide grep found exactly
  1334| one other reference to the column (`tools/extract_segment_subtree.py`,
  1335| which excludes the column *name* from a segment_id-endpoint heuristic — it
  1336| never reads the column's *values*) — the field was write-only, and its
  1337| serialized form had never actually been consumed by any code path.
  1338| 
  1339| ### Decision
  1340| Re-serialize `ancestor_segment_ids` with `";"` instead of `"|"` (`";"` does
  1341| not otherwise occur in a segment_id, since dimension values are themselves
  1342| only `"|"`-delimited into segment_id — one level up). `compare_cross_
  1343| segment.py`'s `_build_ancestor_map()` (D-027) is the first real consumer of
  1344| this field's values, and parses on `";"`.
  1345| 
  1346| ### Consequences
  1347| - Any `segment_manifest.csv` already on disk carries the old, unparseable
  1348|   `"|"`-joined `ancestor_segment_ids` values; these are stale and require a
  1349|   full manifest regeneration (rerunning `build_segment_manifest.py`) to
  1350|   pick up the corrected encoding. There is no migration path for old
  1351|   values, because they were never losslessly parseable in the first place.
  1352| 
  1353| ---
  1354| 
  1355| ## Notes
  1356| 
  1357| - This document is **append-only**.
  1358| - Reversals require a new decision entry that references the original.
  1359| - Implementation details belong in code, not here.
  1360| 
  1361| ## D-029 — Governance narrative evidence-package layer (Phase 6: anomaly/note threshold externalization + classification-logic legibility)
  1362| 
  1363| ### Status
  1364| Accepted (2026-08-18)
  1365| 
  1366| ### Context
  1367| D-021 externalized `assign_tier()`, `score_reliability()`, `build_client_summary()`/`build_bc_summary()`, and `_low_coherence_clients()`'s threshold literals to `policies/governance/governance_thresholds.json`, on the stated basis that "a rule's threshold value is itself part of the interpretation layer, not something a reader can audit or override without reading Python source." A Step 0 read of the rest of the file found that sweep was incomplete: `detect_anomalies()` (1997–2193) — the function that produces every `notable_anomalies` entry in `governance_domain_summary.csv` — still carries roughly fifteen bare numeric literals across eleven distinct findings (gt→tp gap `0.75`/`0.55`, dual-schema passive-indicator `0.40`/`0.20`, single-schema bundle-share `0.25`/`0.15`, Group 2 scope-divergence gap `0.25`, Group 1 by-scope spread gap `0.25`, tp>tc gap `0.25`, weak-tc `0.20`, weak-cp `0.50`, view-template zero-discipline `0.05`, phases `0.85`/`0.80`). `render_findings_and_recommendations()` (5017) duplicates the same phases `0.85`/`0.80` check independently — two literals, one governance question, no shared source. `build_governance_state_summary()` (2812) has a bare `0.85` for `provided_to_used_containment` driving `primary_governance_read`. `_passive_inheritance_risk_domains()` (4752, 4756) hardcodes `0.20` and `0.25` independently of `PASSIVE_MATERIAL_THRESHOLD` (already a named, policy-sourced constant) — so a change to that policy value today would *not* propagate to this function, an already-live drift risk rather than a hypothetical one. The portfolio section's `_shape_note()` (4302) hardcodes `0.8`/`0.3` for its "same shape, different content" note.
  1368| 
  1369| Separately, `render_header()`'s "How to Read the Analysis" block (2992–3109) restates definitions of containment, cross-client similarity, all-view/used-view, and score interpretation that substantively overlap `docs/governance_interpretation_guide.md`'s "Metric semantics" section — two independently-authored descriptions of the same concepts, one in Python, one in docs, with nothing keeping them in sync.
  1370| 
  1371| Externalizing every remaining literal to policy JSON is necessary but not sufficient for the package's stated goal (an LLM or human reader reasoning through a hypothetical threshold change from the package's own artifacts, without rediscovering logic from Python source). `assign_tier()` and `detect_anomalies()` are not single threshold comparisons; they are ordered branches with exception carve-outs (e.g., `assign_tier()`'s strong-baseline branch has two sub-exceptions that reroute to `TIER_BASELINE_LOCAL_REVIEW` before returning `TIER_STRONG_BASELINE`). A reader holding `governance_domain_summary.csv` and a fully-externalized `governance_thresholds.json` can verify whether one named value crosses one named cutoff, but cannot correctly re-derive which tier or which anomaly note fires without also knowing evaluation order — that ordering currently exists only as Python control flow.
  1372| 
  1373| ### Decision
  1374| Three changes, none altering any existing classification, tier, anomaly-note, or CSV column value for any existing invocation:
  1375| 
  1376| 1. **Threshold sweep.** Add a new policy profile, `policies/governance/anomaly_thresholds.json`, following D-021's precedent of a separate profile per conceptually distinct threshold family even where a value numerically coincides with another profile's (e.g. this profile's `passive_inheritance_risk_bundle_share_max` and `governance_thresholds.json`'s `passive_material_threshold` both default to values already in use, but gate different code paths and must be independently editable). Every literal cited above gets a named key. The two duplicate phases checks (`detect_anomalies()` line ~2187, `render_findings_and_recommendations()` line 5017) both read the same `phases_tp_extension_max`/`phases_tw_min` keys. `_passive_inheritance_risk_domains()`'s `0.20`/`0.25` are replaced with reads of `passive_material_threshold` (existing `governance_thresholds.json` key) and a new `passive_inheritance_risk_bundle_share_max` key, closing the drift gap rather than just relocating it. `apply_governance_policy()` gains a fourth profile load, mirroring the existing `governance_thresholds.json`/`domain_governance_policy.json`/`client_onboarding_policy.json` load-with-fallback pattern (per-profile default, `governance_policy_profile_defaulted` warning on fallback, `policy_profiles.profiles` entry in the manifest).
  1377| 
  1378| 2. **Classification-logic legibility.** Add `docs/governance_classification_rules.md` — a stable, non-regenerated, package-type-level artifact following the D-022 precedent set by `governance_interpretation_guide.md`/`governance_question_routes.md` (versioned via its own header, not per-run). It states, in prose/pseudocode, the branch order and exception conditions of `assign_tier()`, `score_reliability()`, `detect_anomalies()`, `build_governance_state_summary()`'s `primary_governance_read` selection, and `_passive_inheritance_risk_domains()` — referencing threshold keys by name from `governance_thresholds.json`/`anomaly_thresholds.json` rather than restating values, so the two artifacts (values in JSON, order/logic in this doc) together let a reader recreate an output instead of rediscovering it from Python. Added to `governance_evidence_map.json` alongside the two existing static docs. This is a legibility aid, not a source of truth — a known limitation (below) tracks the risk of the doc drifting from the code it describes.
  1379| 
  1380| 3. **`render_header()` trim.** Replace the "How to Read the Analysis" block's restated definitions with a pointer at `docs/governance_interpretation_guide.md`'s "Metric semantics" section. Corpus-specific content in `render_header()` that is not conceptual restatement (the file-role count table, discipline/client lists, the governance-cascade diagram) stays, since that content is per-run data, not interpretation, and has no equivalent in the static docs.
  1381| 
  1382| Out of scope: `build_segment_manifest.py` and `compare_cross_segment.py` remain untouched (protected files). No change to any threshold's *value* — this phase is externalization and documentation only, matching D-021's "no existing invocation's output changes by default" discipline.
  1383| 
  1384| ### Consequences
  1385| - No governance classification, tier assignment, `notable_anomalies` content, or CSV column changes for any existing invocation — verified by the same byte-identical regression test D-021 used (`governance_domain_summary.csv` diffed before/after, run twice: default vs. explicit `--policy-dir`).
  1386| - A threshold in `detect_anomalies()`, the phases check, `build_governance_state_summary()`'s primary-read selection, or `_passive_inheritance_risk_domains()` can now be changed via JSON without a code change, and can be tested against already-exported CSV fields without a corpus re-run — closing the gap identified in the phase 5 discussion where the phases check was the one tier-adjacent rule that couldn't be tested from provided data alone.
  1387| - The two independently-hardcoded phases literals collapse to one named source; `_passive_inheritance_risk_domains()`'s drifted `0.20`/`0.25` collapse to the same source `PASSIVE_MATERIAL_THRESHOLD` already governs elsewhere, so a future change to that policy value now propagates everywhere it should.
  1388| - `render_header()` shrinks and `docs/governance_interpretation_guide.md` becomes the sole source for metric/reading definitions — requires confirming the guide's existing "Metric semantics" section actually covers every concept currently explained inline (containment, cross-client similarity, all-view/used-view, score-range interpretation) before the inline text is removed; any gap found gets added to the guide, not left unexplained.
  1389| - `docs/governance_classification_rules.md` is a new hand-maintained artifact describing Python control flow in prose. It is not mechanically verified against the functions it describes — a future code change to `assign_tier()`'s branch order could make this document stale without anything failing. This is a known limitation, not resolved in this phase; a future phase could add a regression test that asserts specific documented example inputs produce the documented example outputs, catching drift without executing the whole file line-by-line as a spec.
  1390| - `governance_evidence_map.json` grows by one artifact (`docs/governance_classification_rules.md`), following the same `Path.exists()`-based presence check as the two existing static docs.
  1391| - `docs/governance_evidence_package.md`'s "Policy profiles and threshold profiles" table gains a fifth row for `anomaly_thresholds.json`, and its phase-log intro paragraph gains a "Phase 6 (D-029)" sentence, matching the existing Phase 1–5 narration pattern.
  1392| - `CHANGELOG.md`'s `[Unreleased]` section gains an entry, matching every prior phase's changelog discipline.
  1393| 
  1394| ## D-030 — Governance narrative evidence-package layer (Phase 7: reading-order artifact and completeness gate)
  1395| 
  1396| ### Status
  1397| Accepted (2026-08-18)
  1398| 
  1399| ### Context
  1400| The package has an authority hierarchy (which artifact wins on disagreement — `render_evidence_authority_header()`) and a topic index (`docs/governance_question_routes.md` — where to look for a recurring question), but nothing states a reading *sequence* for a cold-start reader, human or LLM. `governance_evidence_map.json`'s per-artifact fields (`context_role`, `can_answer`/`cannot_answer`, related-artifact lists) have no ordering or completeness signal at all — a reader can open any artifact first and has no structural cue about what else it depends on.
  1401| 
  1402| An ordinal field (e.g. `read_priority: 1, 2, 3...`) was considered and rejected: an ordinal invites an LLM to sort, read the top few, and reason from a partial picture while still technically "following the order." The failure mode this package needs to guard against is a reader stopping partway through a required set, not a reader reading things in a suboptimal sequence. Given D-023's confirmed finding that this package has, and will continue to have, no query/tool-calling path — a reader cannot fetch more context once reasoning starts, only work from what's already in front of it — the only available guardrail is a structural, self-checkable completeness signal in the artifacts themselves.
  1403| 
  1404| ### Decision
  1405| Two additions:
  1406| 
  1407| 1. **`docs/governance_reading_order.md`** — a new, stable, non-regenerated, package-type-level doc (versioned via its own header, following the `governance_interpretation_guide.md`/`governance_question_routes.md` precedent). States, at the top, this package's intended audience and purpose in the terms the package was actually designed for: a leadership reader who does not know Revit, who understands operational tradeoffs, and who is meant to ask governance convergence/fragmentation questions — not decide standards unassisted. Below that, an explicit ordered path through the package (health check → evidence map/interpretation guide orientation → brief → domain/client rollups → narrative prose → question routes if a specific question → file inventory if deeper drill-down is needed), and a short "read this before drawing conclusions" callout pointing at the two known-bad-inference additions from D-031.
  1408| 2. **Evidence-map completeness fields**, not an ordinal. `governance_evidence_package.py`'s `_artifact()` gains a `required_before_conclusions: bool` field (which artifacts must be incorporated before a governance conclusion is stated), and `build_evidence_map()`'s top-level output gains a `reasoning_prerequisites: [artifact_id, ...]` list — the full set of `required_before_conclusions=true` artifact_ids, exposed once at the manifest level so a reader can check it as a set to exhaust, not a sequence to sample from. `render_evidence_authority_header()` gains one line naming this field and pointing at `docs/governance_reading_order.md`.
  1409| 
  1410| ### Consequences
  1411| - No existing classification, CSV column, or finding changes — this phase adds a new static doc and two new descriptive fields to already-generated JSON; nothing recomputes.
  1412| - `governance_package_manifest.json` and `governance_evidence_map.json` schema versions bump to reflect the new field (per the existing `package_schema_version` override mechanism).
  1413| - A future artifact added to the package must have an explicit `required_before_conclusions` value at the point it's added to `build_evidence_map()` — there is no default that silently opts an artifact in or out, since either default is a real content decision about that specific artifact.
  1414| - This is a convention, not an enforcement mechanism — nothing in this package can stop a reader from ignoring `reasoning_prerequisites` and stating a conclusion anyway. The gate only works if a reader (human or LLM) actually checks it, consistent with every other guardrail in this package being self-checkable rather than enforced.
  1415| 
  1416| ---
  1417| 
  1418| ## D-031 — Governance narrative evidence-package layer (Phase 8: insufficient-evidence and single-region known-bad-inference clarifications)
  1419| 
  1420| ### Status
  1421| Accepted (2026-08-18)
  1422| 
  1423| ### Context
  1424| `docs/governance_interpretation_guide.md`'s "Known bad inferences" section (eight entries) does not address two recurring misreadings, both confirmed live in the current corpus rather than hypothetical:
  1425| 
  1426| 1. Every domain in the current corpus sits at `TIER_INSUFFICIENT` or `TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE` for its *enterprise-scoped* reading, because only business center 2014 currently has Project-role files. The tier names and intro text (`render_domain_tiers()`, ~3239) already distinguish "no enterprise evidence, but BC-level pooled evidence exists" from "no evidence at all," but nothing states that this is a *scope-specific* gap — a domain lacking enterprise-scoped evidence can still have solid client-level, discipline-level, or cross-client-convergence evidence for that same domain sitting in a different summary CSV. Left unstated, this reads as "the package has no evidence," when the accurate statement is "the package has no evidence *at the enterprise scope*."
  1427| 2. The corpus currently contains files from a single region. `render_header()` already notes region is an "unavailable... future segment dimension" (~3038), but that's a "we don't have this yet" statement, not a "and here is what will happen once we do" statement — specifically, that a future region column will read identically to the existing enterprise-level rollup until a second region's data actually exists in the corpus. This is a fact about current data coverage, not a methodology gap to be fixed.
  1428| 
  1429| ### Decision
  1430| Add two entries to `docs/governance_interpretation_guide.md`'s "Known bad inferences" section:
  1431| 
  1432| - *"Insufficient Evidence" is scope-specific, not package-wide.* A domain's enterprise-scoped tier being `Insufficient Evidence` does not mean the domain has no usable evidence anywhere in the package — check `governance_client_summary.csv`, `governance_bc_summary.csv`, and the domain's `cross_client_convergence` field before concluding nothing is known about it.
  1433| - *"Region" and "Enterprise" currently read identically, and will continue to until the corpus changes.* All corpus files currently come from one region. If/when a `region` segmentation dimension is added, region-level and enterprise-level results will be identical by construction until a second region's data exists — this reflects current data coverage, not completed cross-region standardization.
  1434| 
  1435| Both entries get first-class placement (not buried at entry 9/10) in `docs/governance_reading_order.md`'s "read this before drawing conclusions" callout, per D-030.
  1436| 
  1437| Separately, add an explicit audience/intent statement to `docs/governance_interpretation_guide.md`'s existing "What this package is for" section (which currently states subject matter but not audience): this package is written for a reader who does not need Revit domain knowledge, who is expected to ask governance convergence/fragmentation questions rather than resolve them unassisted, and for whom "what to do about it" is explicitly out of this package's scope. `docs/governance_reading_order.md` references this statement rather than restating it, per the same "point, don't duplicate" discipline used for `render_header()`'s trim in D-029.
  1438| 
  1439| ### Consequences
  1440| - Docs-only change — no code, no schema, no classification output affected.
  1441| - The next corpus expansion that adds a second business center's Project-role files or a second region should prompt revisiting whether these two known-bad-inference entries are still accurate as written, since both describe a *current* corpus-composition fact, not a permanent structural one.
  1442| 
  1443| ---
  1444| 
  1445| ## D-032 — Governance narrative evidence-package layer (Phase 9a: comparison-registry input-completeness note)
  1446| 
  1447| ### Status
  1448| Accepted (2026-08-18)
  1449| 
  1450| ### Context
  1451| `docs/governance_generator_cross_compare_coverage.md` recommended `comparison_registry.csv` be wired in as an optional input to distinguish "this domain's evidence is thin because the comparison wasn't run or is stale" from "this domain's evidence is thin because convergence is actually weak" — currently both look identical (a missing or low row) to a reader of `governance_domain_summary.csv`. At 5.5MB, the file is small enough to read once at generation time without a package-size concern; the earlier assumption that this was excluded for the same size reasons as `cross_segment_file_pairs.csv` (9.8GB) does not hold once the two are considered separately — `cross_segment_file_pairs.csv` should never be read by the generator at all (D-023's file-inventory scan already handles it via header/row-count only, never content), while `comparison_registry.csv` is a normal-sized optional input, structurally identical to `--governance-state-summary` or `--reuse-by-client`.
  1452| 
  1453| ### Decision
  1454| Add an optional `--comparison-registry` CLI argument, following the existing optional-input pattern (present-or-absent, degrades gracefully, reported in `governance_package_health.json`'s `required_inputs`/`optional_inputs` when absent). When supplied, render a small **Input Completeness / Staleness** note near Analytical Notes, per-domain, stating the count of expected segment/domain comparison pairs present vs. missing vs. stale (per the registry's own recency/run-id fields). The registry file itself is never embedded or reproduced in the output package — only the derived counts are — and its own path is exposed as a drill-down source via `governance_file_inventory.json`/`governance_evidence_map.json`, the same treatment `cross_segment_file_pairs.csv` already gets.
  1455| 
  1456| Implemented in the same PR as D-033 (shared optional-input plumbing and shared touch points in `main()`/`governance_package_health.json`), but tracked as a separate decision since the two serve different governance questions (completeness vs. reuse-breadth confidence) and could ship independently if one were descoped.
  1457| 
  1458| ### Consequences
  1459| - `governance_package_health.json` gains a new optional-input entry and, when the registry is supplied, a `comparison_completeness` field.
  1460| - No existing classification or tier output changes when the flag is absent — this is strictly additive, matching every prior optional-input phase's discipline.
  1461| - A domain currently reading `Insufficient Evidence` due to a not-run/stale comparison (rather than genuinely weak convergence) becomes distinguishable from the outside for the first time — directly closing part of the D-031 "insufficient evidence is scope-specific" caveat with an actual mechanism, not just a documented caveat.
  1462| 
  1463| ---
  1464| 
  1465| ## D-033 — Governance narrative evidence-package layer (Phase 9b: union-inventory-derived domain confidence enrichment)
  1466| 
  1467| ### Status
  1468| Accepted (2026-08-18)
  1469| 
  1470| ### Context
  1471| `docs/governance_generator_cross_compare_coverage.md` marks this item "(Still open.)" in its own implementation sequence (step 3, "Domain confidence enrichment"): `cross_segment_union_inventory.csv` is currently only partially consumed (to count blocked project domains when manifest metadata exists), and its corpus-wide/project/client/file pattern-prevalence signal — which pairwise Jaccard cannot express — is not yet surfaced per domain. The doc's own framing: this identifies domains with broad natural reuse but weak formal cascade (a natural-standard candidate the cascade metrics alone would miss), or the reverse (narrow reuse despite strong formal cascade, worth flagging as fragile).
  1472| 
  1473| ### Decision
  1474| Extend `governance_domain_summary.csv` with breadth columns derived from `cross_segment_union_inventory.csv` (corpus-wide/project-wide/client-wide/file-level reuse counts per domain, following the same naming convention as existing `_by_scope` fields). Render only the strongest narrative exceptions per the coverage doc's own guardrail — broad reuse with weak cascade, or narrow reuse with strong cascade — as a new anomaly-note category in `detect_anomalies()`, using the same policy-externalized-threshold discipline established in D-029 (thresholds for "broad," "weak," "narrow," "strong" in this context go into `anomaly_thresholds.json` alongside D-029's other additions, not as new bare literals).
  1475| 
  1476| Implemented in the same PR as D-032 (shared optional-input plumbing), tracked separately per the reasoning in D-032's Context.
  1477| 
  1478| ### Consequences
  1479| - `governance_domain_summary.csv` gains new columns; existing columns and their values are unaffected.
  1480| - New anomaly-note category is gated by the same "only render the strongest exceptions" discipline the coverage doc specifies — this is a deliberate scope limit to avoid restating every domain's raw breadth numbers as narrative prose, matching the "consume, not recompute, and don't over-render" discipline established across D-020/D-022.
  1481| - Closes the last open item in `docs/governance_generator_cross_compare_coverage.md`'s implementation sequence — that doc's table should be updated to mark this row "Done" once shipped, matching its own existing convention for the other now-complete rows.
  1482| 
  1483| ---
  1484| 
  1485| ## D-034 — Governance narrative evidence-package layer (Phase 10: static-doc subfolder + self-contained package copy)
  1486| 
  1487| ### Status
  1488| Accepted (2026-08-19)
  1489| 
  1490| ### Context
  1491| The four static, package-type-level reference docs `generate_governance_narrative.py` points readers at by hardcoded path constant (`INTERPRETATION_GUIDE_PATH`/`QUESTION_ROUTES_PATH`/`READING_ORDER_PATH`/`CLASSIFICATION_RULES_PATH`, added across D-022/D-030/D-029) lived directly under `docs/` alongside ~15 unrelated technical-documentation files, with no grouping signal that they specifically belong to the governance narrative package. Separately, these docs were only ever referenced by name/pointer from the generated package (`governance_narrative_context.md`'s authority header, `governance_evidence_map.json`'s sibling-artifact entries) — never actually present alongside a run's output — so a `--out` directory handed to someone without the repo checked out contained pointers to files that reader could not open.
  1492| 
  1493| ### Decision
  1494| Two changes, neither altering any classification, tier, or CSV/JSON field value:
  1495| 
  1496| 1. Move the four docs into a new `docs/governance/` subfolder (filenames unchanged): `governance_interpretation_guide.md`, `governance_question_routes.md`, `governance_reading_order.md`, `governance_classification_rules.md`. `_DOCS_DIR` in `generate_governance_narrative.py` now points at `docs/governance/` instead of `docs/`; the four path constants are otherwise unchanged. Every other `docs/*.md` file (`governance_evidence_package.md`, `governance_generator_cross_compare_coverage.md`, `governance_narrative_scope_gap_audit.md`, `governance_narrative_group1_scope_gap_investigation.md`) stays in `docs/` — those are developer/design documentation about the system, never pointed at by a path constant or copied into a run's output, so they don't belong in the same "package-portable" subfolder.
  1497| 2. `main()`, inside the existing `if args.emit_evidence_package:` block, now copies each of the four docs into `--out` (via `shutil.copy2`, only when the source doc is present) after building `sibling_paths`/`sibling_present`. Only these four are copied — never the CSV siblings the same block registers (`cross_segment_file_pairs.csv`, `comparison_registry.csv`, `pattern_reuse_summary_by_domain.csv`, `project_mean_file_pair_jaccard_matrix.csv`, `governance_relationships.csv`), which D-023/D-024/D-032 are explicit are never embedded or reproduced in the output package. `governance_evidence_map.json`/`governance_package_manifest.json`'s `path`/`present` fields for these four artifacts continue to describe the checked-in repo doc (the source of truth), not the copy — the copy is a portability convenience, not a second source of truth, and is silently skipped when the source doc is absent (e.g. a stripped-down deployment without `docs/`).
  1498| 
  1499| ### Consequences
  1500| - No existing classification, tier, CSV column, or JSON field value changes — this phase moves files and adds a copy step, nothing else. Verified: `governance_domain_summary.csv`/`governance_client_summary.csv`/`governance_bc_summary.csv` are byte-identical before/after: only new files appear in `--out`.
  1501| - A `--out` directory is now self-contained: a reader with only that directory (no repo checkout) can open the four static reference docs the narrative and evidence map already point them at by name.
  1502| - `docs/governance_evidence_package.md` and `CLAUDE.md`'s live path references to the four docs are updated to the new location; `DECISIONS.md`/`CHANGELOG.md`'s own historical entries (D-019 through D-033) are left describing the paths that were true at the time, per this repo's append-only convention for those two files.
  1503| - A future fifth static package-type-level doc, if added, should default to `docs/governance/` and the same copy-into-`--out` treatment unless there's a specific reason not to.
  1504| 
  1505| ## D-035 — Join-policy gate exemption for single-record-per-file domains (`units_doc`, `worksets_doc`)
  1506| 
  1507| ### Status
  1508| Accepted (2026-08-19)
  1509| 
  1510| ### Context
  1511| `units_doc` and `worksets_doc` are the two domains already flagged elsewhere as structurally mismatched to the pairwise join/comparison machinery the rest of the corpus uses (see the open backlog item: "single-record-per-file domains... right fix is a dedicated aggregate/distribution reporter; decision and DECISIONS.md entry not yet written"). `run_extract_all.py`'s join-policy gate (`_enforce_policy_gate()`, ~line 711) enforces `join_key_status == ok` across all in-scope domains before authority/patterns processing begins, and these two domains cannot satisfy that check by construction — a `record_pk` grain of one row per file has no meaningful pairwise join key to compute, so their `join_key_status` reads `blocked`/`non_ok_status` on every record, every run (confirmed 2026-08-19: 1,256 of 1,256 flagged rows across two prior diagnostics runs were both domains, 100% of their records, `join_key_status=blocked`).
  1512| 
  1513| Because `_enforce_policy_gate()` fires immediately after records load and before any authority/patterns output is written (confirmed by reading the call site, ~line 1017 — it precedes the analysis/pattern-mining work entirely), a gate failure here is a fast, silent, whole-run abort: no partial output, no `analysis/` directory, and — critically — no distinguishing console signal loud enough to notice if the run's tail isn't watched closely. This is exactly what happened: `units_doc`/`worksets_doc` were introduced and the corpus re-extracted, every subsequent Run B invocation hit this gate and exited in seconds, and the resulting absence of an `analysis/` directory was read as "hasn't gotten there yet" rather than "failing" for an extended, unknown period — only surfaced as a side effect of debugging an unrelated OneDrive relocation issue. The only reason it was caught is that a run finally succeeded (via `-AllowSigHashJoinKey`, added this session) and the runtime jump from "seconds" to "actually doing the work" made the change visible.
  1514| 
  1515| `--allow-sig-hash-join-key` unblocks the run, but it is a **global** escape hatch — it doesn't scope to the two known-exempt domains, so a real, unexpected join-key regression in an unrelated domain would be silently waved through by the same flag, undetected, for as long as the flag stays on. Relying on a human remembering to pass it by hand every Run B invocation is also exactly the failure mode that produced this incident in the first place.
  1516| 
  1517| ### Decision
  1518| Scope the gate exemption to the two known domains, rather than either (a) leaving this as a manually-remembered flag, or (b) permanently defaulting the global bypass on. At the `_enforce_policy_gate()` call site(s) in `run_extract_all.py`, introduce a new constant — distinct from `SUPPRESSED_DOWNSTREAM_DOMAINS`, since that constant's effect is broader (excludes a domain from downstream processing generally) and this exemption should apply *only* to the gate check, not to authority/patterns output for these domains:
  1519| 
  1520| ```python
  1521| # Single-record-per-file domains structurally cannot produce a pairwise join key
  1522| # (record_pk grain is one row per file) -- join_key_status=blocked is expected and
  1523| # permanent for these, not a data-quality regression. Exempt from the join-policy
  1524| # gate specifically; still fully processed by authority/patterns otherwise. See
  1525| # DECISIONS.md D-035.
  1526| JOIN_GATE_EXEMPT_SINGLE_RECORD_DOMAINS = {"units_doc", "worksets_doc"}
  1527| ```
  1528| 
  1529| and filter the `domains` list passed into `_enforce_policy_gate()` at each call site (~1017, ~1204) to exclude this set before the gate evaluates it — `_emit_join_policy_diagnostics()`'s existing `dom_filter` inclusion-list mechanism already supports this without further code changes to that function.
  1530| 
  1531| Until this lands, `-AllowSigHashJoinKey` (added to `corpus_update_runbook.ps1` this session) remains the manual interim workaround, and this decision is the record of why it exists and why it shouldn't become the permanent answer.
  1532| 
  1533| This is explicitly a narrower, faster fix than the "dedicated aggregate/distribution reporter" — it stops the gate from silently killing the whole run over a known, structural, non-actionable condition; it does not give `units_doc`/`worksets_doc` a real pairwise-comparable governance signal, which remains the open, undecided problem the reporter would actually solve.
  1534| 
  1535| ### Consequences
  1536| - Once implemented, Run B no longer requires `-AllowSigHashJoinKey` for the currently-known case, and the flag reverts to what it always should have been: a rare, visible, deliberate override for a genuinely new problem, not routine cover for a permanent one.
  1537| - A future domain that legitimately regresses into `blocked`/identity-mode status is still caught by the gate and still halts the run loudly — this fix narrows the exemption to exactly the two domains it's justified for, rather than widening tolerance generally.
  1538| - `analysis/` output for `units_doc`/`worksets_doc` continues to be produced using whatever degraded/identity-mode join behavior they've always structurally had — this decision does not change what that output means or whether it's governance-grade; that's still gated on the reporter work.
  1539| - If a third domain is ever found to have the same single-record-per-file structural property, it needs to be added to `JOIN_GATE_EXEMPT_SINGLE_RECORD_DOMAINS` explicitly — this is not auto-detected, by design, so a new case doesn't silently inherit an exemption it wasn't reviewed for.
  1540| - Worth a follow-up check once this lands: confirm `domain_patterns.csv`/`analysis/` row counts for `units_doc`/`worksets_doc` from *today's* run (the first successful one) against whatever the corpus should actually contain, since this is the first time this data has existed at all since those domains were introduced — there's no prior "last known good" version to diff against, only the extraction/export side to cross-check.
  1541| 
  1542| ## D-026 — Audit reports remain historical, not operational
  1543| 
  1544| `audit_results/` is retained as clearly historical evidence for earlier releases.
  1545| Production correctness must be justified by maintained contracts, policies, code
  1546| comments, and tests; live tools must not require an audit file at runtime. The consolidation is complete: current contract rationale and implementation
  1547| explanations are maintained here and beside the relevant code; production modules,
  1548| tests, and operator runbooks no longer depend on an audit report for correctness.
  1549| Historical changelog and audit-to-audit links remain only where their targets exist.
  1550| The deterministic tracked-reference check is `scripts/check_audit_references.py`.
  1551| 
  1552| ## D-037 — Consolidated name-projection and extractor rationale
  1553| 
  1554| ### Status
  1555| Accepted (2026-08-20)
  1556| 
  1557| ### Decision
  1558| The canonical name-identity projection has 7 native domains, 18 widened domains,
  1559| and 12 explicitly excluded domains. Native values already occur on the canonical
  1560| identity surface. Widened values come from a phase-2 bucket or undecorated
  1561| `label.display`/`label.components` value; they therefore are not evidence-equivalent
  1562| to a configuration join hash. The `phases` projection is intentionally marked
  1563| redundant. `core/name_key_coverage.py` is the maintained registry.
  1564| 
  1565| Bundle name-projection staging deliberately adapts its reduced schema at one
  1566| boundary. It emits empty CAD-import evidence, uses the synthesized pattern label as
  1567| the human label, and supplies one deterministic analysis run ID. Split export IDs
  1568| normalize from details to index names only when the known metadata IDs support that
  1569| choice. Provenance must continue to disclose that agreement evidence reconstructed
  1570| the inline calculation rather than validating a live re-extracted corpus.
  1571| 
  1572| Extractor domain differences are intentional only when encoded in maintained domain
  1573| code and covered by selector/shape-gating tests. Canonical selectors constrain broad
  1574| collector APIs; dimensions require subtype shape gates; system and import coercions
  1575| are explicit; and identity-item migration rebuilds the canonical flat item surface
  1576| rather than retaining the former monolithic representation.
  1577| 
  1578| ### Consequences
  1579| - Historical audits 6–15 record how these conclusions were reached, but are not part
  1580|   of the contract.
  1581| - Tests assert current registries, schemas, provenance, selectors, and output paths
  1582|   directly; audit prose is never needed to interpret a pass or failure.
  1583| - Operators use maintained command help and runbooks. Historical audit shorthand is
  1584|   not an operational reference.
  1585| 
  1586| ## D-036 — Enterprise artifact provenance and promotion schema v2
  1587| 
  1588| ### Status
  1589| Accepted (2026-08-20)
  1590| 
  1591| ### Decision
```
