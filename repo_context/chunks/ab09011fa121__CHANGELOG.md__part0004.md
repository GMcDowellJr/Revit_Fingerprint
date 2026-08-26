# Chunk of CHANGELOG.md

- Source relative path: `CHANGELOG.md`
- Chunk: 4 of 7
- Original line range: 1206-1605
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: 4fec943c22afdfaa820cb9077538d951922289c152ad0d6436e45f8ff6d49213
- Starts inside symbol: no
- Ends inside symbol: no

```
  1206| ### Changed
  1207| - `tools/generate_governance_narrative.py`'s cross-client/within-project
  1208|   metrics (`xc_by_client`/`wp_by_client`/`xc_dom_by_client` in
  1209|   `build_client_summary()`, `disc_domain_wp` in `render_discipline_section()`,
  1210|   and `xc`/`wp_all`/`wp_used`/`tw` in `build_cascade()`) now read
  1211|   `compare_cross_segment.py`'s population-union metrics
  1212|   (`all_union_jaccard`/`used_union_jaccard`) instead of the pairwise-file
  1213|   mean (`all_pairwise_jaccard_mean`, previously read via the canonical
  1214|   `jaccard_mean` alias). This is a genuine interpretation change, not a
  1215|   rename follow-up: union jaccard measures footprint overlap between two
  1216|   populations' full file unions, independent of `n_files_a x n_files_b`,
  1217|   which is materially different from (and more resistant to file-count
  1218|   skew than) a mean of pairwise file comparisons — the exact problem
  1219|   `all_union_jaccard`/`used_union_jaccard` were added to
  1220|   `compare_cross_segment.py` to solve.
  1221|   - For `cross_client`/`sibling_projects`/`within_project`(Project role),
  1222|     `compare_cross_segment.py`'s own `_recommended_primary_view()` states
  1223|     used-view is primary ("active practice") for these types — the
  1224|     **opposite** convention from `tc`/`cp`/`tp` (Group 1 governance chain),
  1225|     where all-view is primary and `_used` is the secondary diagnostic. So
  1226|     `xc_mean`/`wp_mean`/`d["xc"]` (the bare, tier-driving names) are now
  1227|     sourced from `used_union_jaccard`; a new secondary value (`xc_mean_all`/
  1228|     `wp_mean_all`/`d["xc_all"]`, and `cross_client_convergence_all_view`/
  1229|     `cross_client_similarity_mean_all_view`/`within_project_coherence_all_view`
  1230|     in the CSV outputs) carries the all-view union metric as context. This
  1231|     changes `CLIENT_ALIGNMENT_HIGH`/`CLIENT_ALIGNMENT_MODERATE` tier
  1232|     assignment, `XC_STRONG_CONVERGENCE`/`cross_client_convergence` findings,
  1233|     `CLIENT_COHERENCE_LOW`/`low_client_coherence` findings, and onboarding
  1234|     profile reads (`_client_onboarding_profile()`) for any client/domain with
  1235|     a real gap between pairwise-mean and used-view-union scores — real tier
  1236|     movement is expected, not a bug.
  1237|   - `wp_all[dom]`/`wp_used[dom]` in `build_cascade()` were already a genuine
  1238|     all-view/used-view pair (unlike `xc`, which had no used companion before
  1239|     this change) — only the metric family swapped (pairwise mean → union);
  1240|     which side is "all" and which is "used" is unchanged, since
  1241|     `passive_indicator`'s `(all - used)` delta depends on that assignment
  1242|     staying fixed. `tw[dom]` (Template self-comparison) shares `wp_all`'s `v`
  1243|     and is therefore also now union-sourced, still all-view.
  1244|   - `CASCADE_GROUP4_EXCLUDED_TYPES["client_cross_bc"]`'s docstring updated:
  1245|     the "provisional pending a population-union aggregation fix" text was
  1246|     stale (the fix has shipped and is now adopted for the other three
  1247|     types) — `client_cross_bc` itself remains unrouted into any
  1248|     cascade/client-summary accumulator; that is still a separate, unresolved
  1249|     design decision, not something this change does implicitly.
  1250|   - `_SUMMARY_COL_ALIASES` gains 6 new canonical entries for the union
  1251|     fields (`all_union_jaccard`, `used_union_jaccard`,
  1252|     `all_union_containment_a_in_b`, `all_union_containment_b_in_a`,
  1253|     `used_union_containment_a_in_b`, `used_union_containment_b_in_a`), read
  1254|     via `_col()` like every other field in this file rather than a raw
  1255|     `row.get()` bypass. `all_pairwise_*`/`used_pairwise_*` fields and their
  1256|     aliases are unchanged — this is an addition of what else is read, not a
  1257|     removal.
  1258| 
  1259| ### Fixed
  1260| - Six correctness bugs in the `comparison_status="blocked"` row-emission
  1261|   path added earlier in this changeset (found via code review), all in
  1262|   `tools/compare_cross_segment.py`:
  1263|   - **Blocked rows reported the populated side's bundle availability as
  1264|     false.** Both blocked-row builders (`run_pair()` and
  1265|     `_build_pooled_row()`) hardcoded `all_has_bundles_*`/
  1266|     `used_has_bundles_*` to `"false"` for every side, even when the
  1267|     populated side (or, for pooled rows, one or more pool members) actually
  1268|     had `bundle_membership.csv` output for the domain. These columns
  1269|     document per-side output *availability*, not a similarity score, so
  1270|     they're now computed from `load_bundle_join_hash_set()` per side (the
  1271|     pool side aggregated across every `pool_sids` member, same as the
  1272|     non-blocked path) — only the genuinely-empty side/pool reads `false`.
  1273|     The shared-overlap bucket counts stay at `0` either way, since there's
  1274|     no trustworthy shared set when one side has zero files.
  1275|   - **Lineage-emptied pools were reported as blocked instead of skipped.**
  1276|     `_emit_for_groups()` excludes any pool member in the focal segment's own
  1277|     `parent_segment_id` lineage before calling `_build_pooled_row()` — for a
  1278|     2-member bc/client pool group where the other member is the focal's own
  1279|     ancestor or descendant, this leaves `pool_sids` empty. The zero-inventory
  1280|     blocked-row branch doesn't distinguish "no eligible pool exists" from
  1281|     "the pool's inventory couldn't be read," so it emitted a
  1282|     `comparison_status="blocked", n_files_pool=0` row for every one of the
  1283|     focal's own domains — a comparison that was never eligible in the first
  1284|     place, inflating blocked-pool counts. Now skipped entirely (`continue`)
  1285|     as soon as lineage filtering leaves `pool_sids` empty, before any domain
  1286|     is even considered.
  1287|   - **Blocked rows corrupted the populated side's own counts.** `run_pair()`'s
  1288|     blocked-row builder hardcoded `n_patterns_a`/`n_patterns_b`/
  1289|     `n_unique_patterns_a`/`n_unique_patterns_b` to `0` for *both* sides, even
  1290|     when only one side was actually empty and `n_a`/`n_b` (the populated
  1291|     side's real counts) were already computed. Now uses the real per-side
  1292|     counts; only the genuinely-empty side reads `0`.
  1293|   - **Blocked directed references produced false delta findings.** Before
  1294|     this changeset, a directed comparison with a zero-file reference side
  1295|     returned `None` from `run_pair()`, so `main()`'s delta-generation block
  1296|     (for `DELTA_DIRECTED_TYPES`) never ran. Now that a blocked comparison
  1297|     returns a real row, that block *did* run — with an empty `ref_union`,
  1298|     `tgt_union - ref_union` equals `tgt_union`, so every target join_hash
  1299|     was written to `cross_segment_delta.csv` as if the target had invented
  1300|     it locally, when the true story is "reference unknown," not "target
  1301|     drifted." Delta generation now skips rows with
  1302|     `comparison_status == "blocked"`.
  1303|   - **Pool-only domains were never scheduled for an empty focal segment.**
  1304|     `run_pooled_comparison()` iterated only `discover_domains_for_segment
  1305|     (focal_sid)` when deciding which domains to run `_build_pooled_row()`
  1306|     for. A focal segment with zero inventory for a domain that exists only
  1307|     in its pool (`n_files_focal=0, n_files_pool>0` — precisely the case the
  1308|     blocked-row path exists to report) was therefore never scheduled at all
  1309|     for that domain, silently dropping the row instead of reporting it
  1310|     blocked. Domain discovery now unions the focal segment's domains with
  1311|     every pool member's domains (memoized per segment_id across the whole
  1312|     call, since the same segment recurs across the three pool grains).
  1313| - `tools/compare_cross_segment.py`'s `make_comparison_run_id()` now includes
  1314|   `comparison_type` in its hash input (`seg_a|seg_b|comparison_type|
  1315|   executed_utc`, was `seg_a|seg_b|executed_utc`). An enterprise (Stantec/
  1316|   `"0000"`) standard and a real-BC standard of the same role that share a
  1317|   `parent_segment_id` get paired both as `sibling_templates`/
  1318|   `sibling_containers` (`discover_sibling_segments()`, symmetric Jaccard)
  1319|   and as `enterprise_to_bc` (`discover_governance_chain()`, directed
  1320|   reference-union containment) — genuinely distinct measurements of the
  1321|   same two segments, not duplicates (unlike the `cross_client`/`bc_to_bc`/
  1322|   `client_cross_bc` case `drop_legacy_siblings_covered_by_peer_comparisons()`
  1323|   already handles, which are symmetric duplicates and correctly get the
  1324|   sibling row dropped). Because `discover_sibling_segments()`'s sorted-ID
  1325|   pairing and `discover_governance_chain()`'s enterprise-then-bc pairing can
  1326|   land on the identical `(seg_a, seg_b)` orientation (whenever the
  1327|   enterprise segment's generated ID happens to sort first, e.g. `"0000"`
  1328|   segments), both rows previously collided on the same `comparison_run_id`
  1329|   even though `cross_segment_file_pairs.csv` carries no `comparison_type`
  1330|   column to disambiguate by. `enterprise_to_client` has the identical
  1331|   structural risk (same shared-parent/same-role precondition) and is fixed
  1332|   by the same change. All callers within `compare_cross_segment.py` that
  1333|   build a `comparison_run_id` for a `run_pair()`-style comparison now pass
  1334|   their `comparison_type` through; the two `_build_pooled_row()` pooled-
  1335|   comparison call sites are unaffected (their second `make_comparison_run_id`
  1336|   argument already embeds `pool_scope`, so there is no analogous collision
  1337|   there). This changes every `comparison_run_id` value produced by the tool
  1338|   (the hash input format changed for all rows, not just the previously-
  1339|   colliding ones) — `comparison_run_id` is a per-run bookkeeping ID
  1340|   (embeds `executed_utc` already, so never reproducible across runs
  1341|   regardless), not one of the record.v2 identity/fingerprint hashes D-002
  1342|   protects, so no `DECISIONS.md` entry is needed.
  1343| 
  1344| ### Changed
  1345| - `tools/compare_cross_segment.py` cardinality and aggregation semantics are
  1346|   now explicit. Adds non-suppressive `comparison_status` (`ok`/`degraded`/
  1347|   `blocked`) computed purely from file counts on each side of a comparison
  1348|   (`blocked` = zero readable file inventory on a required side; `degraded` =
  1349|   exactly one side has a single file while the other has more; everything
  1350|   else, including a symmetric 1×1 comparison, is `ok`) to `cross_segment_
  1351|   summary.csv` and `cross_segment_pooled.csv`. This is a genuine behavior
  1352|   change: a comparison where either side has zero files previously produced
  1353|   no row at all (`run_pair()`'s shared `min_patterns` gate silently returned
  1354|   `None`); it now emits a real, schema-complete row with `comparison_status
  1355|   = "blocked"` and blank (not zero-valued) similarity fields instead.
  1356|   `n_files_a >= 1 and n_files_b >= 1` comparisons that used to be silently
  1357|   suppressed via the same path are unaffected — that "can we say anything at
  1358|   all" pattern-count gate (`min_patterns`, default 3, unrelated to file
  1359|   count) is untouched and still silently suppresses rows below it, by
  1360|   design (out of scope for this change).
  1361|   - Purely descriptive `cardinality_shape` (`single_a`/`single_b`/`balanced`/
  1362|     `imbalanced`) and `file_count_ratio` siblings added alongside — neither
  1363|     ever gates output; `balanced` classifies equal file counts on both
  1364|     sides, including 1×1, as symmetric rather than narrow.
  1365|   - `inventory_status_a`/`inventory_status_b` (populated only on `blocked`
  1366|     rows) distinguish a confirmed-empty domain (segment read succeeded,
  1367|     zero patterns — `no_patterns`) from a segment/domain that couldn't be
  1368|     read at all (`missing_domain_patterns`), reusing the existing
  1369|     `_segment_domain_source_status()` helper. Both have zero files but are
  1370|     not the same fact.
  1371|   - Adds population-union metrics for every comparison routed through
  1372|     `compare_symmetric_file()` (this now covers the `bc_to_bc` and
  1373|     `client_cross_bc` comparison types PR2/#373 left at provisional status
  1374|     specifically because of this imbalance problem): `all_union_jaccard`,
  1375|     `all_union_containment_a_in_b`, `all_union_containment_b_in_a`, and
  1376|     `used_*` counterparts — Jaccard/containment between each side's full
  1377|     file-union footprint, independent of `n_files_a × n_files_b`. These
  1378|     answer "how similar are these two populations", a different question
  1379|     from the existing pairwise mean ("what's the mean of all file pairs"),
  1380|     and are stable when a side gains an exact-duplicate file where the
  1381|     pairwise mean is not.
  1382|   - Renames `all_jaccard_mean` → `all_pairwise_jaccard_mean`,
  1383|     `used_jaccard_mean` → `used_pairwise_jaccard_mean`,
  1384|     `all_containment_a_in_b_mean` → `all_pairwise_containment_a_in_b_mean`
  1385|     (and the `b_in_a`/`used_*` counterparts) in `cross_segment_summary.csv`,
  1386|     and adds `aggregation_method = "cartesian_file_pair_mean"` (symmetric
  1387|     rows only) to label them explicitly. The underlying computation is
  1388|     unchanged for symmetric rows; directed rows now populate the same
  1389|     renamed columns via the same reference-union-vs-per-target-file-
  1390|     distribution computation they always used (unchanged) — `reference_
  1391|     aggregation`/`target_aggregation`/`n_reference_files` make that
  1392|     directed-specific meaning explicit per row instead of requiring the
  1393|     reader to already know it from `comparison_type`.
  1394|   - **Breaking for downstream consumers of the renamed fields** — this was
  1395|     a deliberate correctness-over-compatibility call, not an oversight.
  1396|     `tools/compare_governance_populations.py` imports `compare_symmetric_
  1397|     file()`/`compare_directed_file()` directly and spreads their return
  1398|     dict into its own rows (`row.update(metrics)`); it will silently read
  1399|     blank values for `all_jaccard_mean`/`all_containment_a_in_b_mean`/
  1400|     `all_containment_b_in_a_mean` until migrated. `tools/generate_
  1401|     governance_narrative.py` reads the pre-rename names at ~15 call sites
  1402|     via its `_SUMMARY_COL_ALIASES`-style alias helper; it will also
  1403|     silently read blank values for the same three field families until
  1404|     migrated. Neither is touched by this change (out of scope; migrate in
  1405|     a follow-up PR) — 40 tests across `tests/test_generate_governance_
  1406|     narrative_brief.py`, `tests/test_generate_governance_narrative_
  1407|     evidence_package.py`, `tests/test_generate_governance_narrative_
  1408|     policy.py`, and `tests/test_compare_governance_populations.py` now fail
  1409|     as a direct, documented consequence and are left failing pending that
  1410|     migration.
  1411|   - Adds directed-reference heterogeneity diagnostics:
  1412|     `reference_union_pattern_count`, `reference_intersection_pattern_count`,
  1413|     `reference_core_share` (= intersection/union across every file on the
  1414|     reference side) — reveals whether a multi-file reference (e.g. a
  1415|     Template segment backed by several files) is a coherent standard or a
  1416|     broad union of conflicting sources, independent of how well any target
  1417|     matches it. Degrades to `1.0` for a single-file reference — not an
  1418|     artificial failure.
  1419|   - Adds side-balanced summaries for symmetric comparisons:
  1420|     `all_a_file_mean_similarity_to_b_mean/min`,
  1421|     `all_b_file_mean_similarity_to_a_mean/min` — each A-file's own mean
  1422|     Jaccard to every B file, then mean/min of those per-file means (and the
  1423|     inverse for B), exposing directional population experience that a
  1424|     single pooled mean/min hides in an imbalanced comparison.
  1425|   - `docs/cross_segment_comparison.md` updated to match; also corrects two
  1426|     stale claims (a `n_pairs ≤ 50` row-count suppression threshold for
  1427|     `cross_segment_file_pairs.csv` that does not exist anywhere in the
  1428|     code).
  1429| - `tools/compare_cross_segment.py` organizational scope is now derived from
  1430|   explicit, literal `client_label`/`business_center_label` values instead of
  1431|   blank inference, matching `build_segment_manifest.py`'s explicit-metadata
  1432|   contract: **enterprise** (`client_label == "Stantec"`,
  1433|   `business_center_label == "0000"`), **business_center** (`client_label ==
  1434|   "Stantec"`, a real `business_center_label`), **client_business_center** (a
  1435|   real external `client_label`, a real `business_center_label`) via the
  1436|   rewritten `_scope_level()`. A row where either dimension isn't cut at all
  1437|   (blank) is a roll-up pooling multiple real scopes and is handled per
  1438|   comparison type (`_is_client_wide_rollup()`), not classified by
  1439|   `_scope_level()` itself.
  1440|   - `_normalize_bc_label()` no longer folds `"0000"`/`"BC_0000"` to blank —
  1441|     `"0000"` now flows through as the literal Enterprise business-center
  1442|     value everywhere this file uses `business_center_label` (`"BC_0000"`/any
  1443|     case spelling variant canonicalizes to the same literal `"0000"` rather
  1444|     than being left as a separately-fragmenting literal — see the "Fixed"
  1445|     entry below). This was a live inconsistency left in place by the
  1446|     segment-manifest explicit-contract change: since `client_label` is now
  1447|     always populated (literally
  1448|     `"Stantec"` for internal work, never blank),
  1449|     `discover_governance_chain()`'s prior blank-based scope inference meant
  1450|     `_scope_level()` could never return `"enterprise"` for real data at all
  1451|     (an internal-work row's populated `client_label` always won the old
  1452|     3-way branch before blank-derived `"bc"`/`"client"` were ever reached) —
  1453|     `enterprise_to_project`/`enterprise_to_bc`/`enterprise_to_client` pairs
  1454|     were silently produced for zero pairs against current data. Fixed.
  1455|   - `discover_governance_chain()`'s `_key()` now folds `business_center_label`
  1456|     into its client-populated bucket too — without this, an Enterprise
  1457|     Template (`Stantec`/`0000`) and a specific business center's Template
  1458|     (`Stantec`/`2270`) collapsed into one `client=="Stantec"` bucket and
  1459|     incorrectly produced `template_to_project`/`template_to_container` pairs
  1460|     against each other's downstream population.
  1461|   - `_disc_match()`'s blank-discipline wildcard is removed — discipline-gated
  1462|     comparisons now require an exact `discipline_label` match, full stop.
  1463|   - `discover_cross_client()`'s grain now includes `discipline_label`
  1464|     (previously excluded any discipline-scoped Project segment from
  1465|     `cross_client` entirely); grouping key is now `(client_label,
  1466|     unit_system, discipline_label)`.
  1467|   - `SUMMARY_FIELDS` gains `scope_level_a`/`scope_level_b`; `POOLED_FIELDS`
  1468|     gains `scope_level` (empty string for roll-up rows).
  1469|   - `run_pooled_comparison()`'s `bc_groups` pooling (`pool_scope == "bc"`)
  1470|     calls `_bc_of()`, which calls the now-fixed `_normalize_bc_label()`
  1471|     directly (no independent re-implementation) — so this same fix also
  1472|     stops silently excluding Enterprise-scoped (`"0000"`) rows from
  1473|     bc-scoped pooling entirely (previously `if bc:` was always False for
  1474|     them, since `_bc_of()` folded `"0000"` to blank; they simply never
  1475|     entered `bc_groups`). New coverage:
  1476|     `test_pooled_comparison_bc_scope_pools_enterprise_0000_segments`.
  1477| - Cardinality/aggregation semantics (`data_sufficient` gate, pairwise-mean
  1478|   computation, `jaccard_mean`/`containment_*_mean` field naming) are
  1479|   unchanged by this entry — `cross_client` and the new `client_cross_bc`
  1480|   comparison type reuse the existing metrics functions as-is and remain
  1481|   pairwise/provisional pending a population-union aggregation fix.
  1482| 
  1483| ### Design notes
  1484| - `pool_scope` (`run_pooled_comparison()`) and `scope_level` (`_scope_level()`)
  1485|   are intentionally distinct — the former describes which axis a sibling pool
  1486|   is grouped along, the latter describes a segment's organizational position.
  1487|   Both now derive from the same corrected `_normalize_bc_label()`, so they no
  1488|   longer risk drifting apart on how `business_center_label` is interpreted
  1489|   (verified: `_bc_of()` calls the shared function directly, no independent
  1490|   re-implementation). No unification needed; documented at the `pool_scope`
  1491|   definition site to prevent future confusion.
  1492| 
  1493| ### Fixed
  1494| - (PR #373 review) `_normalize_bc_label()` now canonicalizes `"BC_0000"`/any
  1495|   case spelling to the literal `"0000"` instead of leaving it as a separate,
  1496|   fragmenting literal — `_is_enterprise_bc()` only ever compared against
  1497|   `"0000"` exactly, so a row spelled `"BC_0000"` (a real spelling used
  1498|   elsewhere in the pipeline, e.g. the extraction completeness gate) was
  1499|   classified `business_center` instead of `enterprise`, omitting the
  1500|   intended `enterprise_to_project`/`enterprise_to_bc` fan-out and able to
  1501|   emit a bogus `bc_to_bc` peer pairing between the enterprise segment and a
  1502|   real business center. Reuses the shared `na_token.
  1503|   ENTERPRISE_BC_BOOKKEEPING_TOKENS` set (re-imported) rather than
  1504|   reimplementing it.
  1505| - (PR #373 review) `drop_legacy_sibling_projects_covered_by_cross_client()`
  1506|   renamed to `drop_legacy_siblings_covered_by_peer_comparisons()` and
  1507|   generalized: it previously only dropped a `sibling_projects` row covered
  1508|   by a `cross_client` pair. The new `bc_to_bc`/`client_cross_bc` types have
  1509|   the identical collision risk against `sibling_templates`/
  1510|   `sibling_containers`/`sibling_projects` (same-role BC-scoped segments, or
  1511|   a client's per-BC segments, can share an immediate `parent_segment_id`
  1512|   with what a purpose-built peer function already pairs) — both would have
  1513|   collided on `comparison_run_id` and double-counted the pair in
  1514|   `cross_segment_file_pairs.csv`, which carries no `comparison_type` column.
  1515|   The generalized function drops any `sibling_*` row for a pair any of
  1516|   `cross_client`/`bc_to_bc`/`client_cross_bc` already covers.
  1517| - (PR #373 review) `bc_to_bc` and `client_cross_bc` registered in
  1518|   `generate_governance_narrative.py`'s `CASCADE_GROUP4_EXCLUDED_TYPES`
  1519|   (same-role/same-client peer comparison, no cascade treatment designed
  1520|   yet — same reason class as `sibling_templates`/`sibling_containers`).
  1521|   Without this, any default run where these types fire fed
  1522|   `_warn_unrecognized_comparison_types()` an unrecognized value. This is a
  1523|   narrow, additive exception to keeping `generate_governance_narrative.py`
  1524|   out of scope for this PR — registering a type name in the existing
  1525|   documented-exclusion registry, not new narrative/cascade logic.
  1526| 
  1527| ### Added
  1528| - New `bc_to_bc` comparison type in `tools/compare_cross_segment.py`
  1529|   (`discover_governance_chain()`, fires under `--governance-chain`): pairs
  1530|   every combination of real business centers' same-role, same-discipline
  1531|   Template/Container/Project populations against each other (peer-to-peer,
  1532|   not routed through `parent_segment_id`/collection_label).
  1533| - New `client_cross_bc` comparison type in `tools/compare_cross_segment.py`
  1534|   (`discover_client_cross_bc()`, fires under `--cross-client`): for a real
  1535|   client whose work spans more than one real business center, pairs that
  1536|   client's per-business-center (`client_business_center` scope) populations
  1537|   against each other for every business-center pair it actually appears in
  1538|   (derived from the data, not a fixed two-BC comparison), matched by
  1539|   `client_label`, `governance_role`, `discipline_label`, `unit_system`.
  1540|   Provisional metric pending PR3's population-union aggregation fix, same as
  1541|   `cross_client`.
  1542| - New `cross_client` comparison type in `tools/compare_cross_segment.py`
  1543|   (`discover_cross_client()`, `--cross-client` CLI flag, default-on): pairs
  1544|   each client's own broadest (client-only-scoped) Project population against
  1545|   every other client's, within the same unit_system, independent of segment
  1546|   lineage. Fixes `cross_client_convergence` (governance_domain_summary.csv)
  1547|   and `cross_client_similarity_mean` (governance_client_summary.csv) being
  1548|   blank for every row -- the only prior source for those columns was
  1549|   `sibling_projects`, which only pairs Project segments sharing an immediate
  1550|   `parent_segment_id` and is additionally sector-gated (both clients must be
  1551|   tagged `healthcare` in `policies/client_sector.csv`) in
  1552|   `build_cascade()`'s `xc` accumulation. `cross_client` has no shared-parent
  1553|   requirement and no hardcoded sector gate (sector filtering, where wanted,
  1554|   is left to downstream consumers). `tools/generate_governance_narrative.py`'s
  1555|   `build_cascade()` and `build_client_summary()` now also accumulate `xc`/
  1556|   `xc_mean` from `cross_client` rows alongside the existing `sibling_projects`
  1557|   source. Jaccard-based, undirected (mirrors `sibling_projects`'s scoring
  1558|   path); no governance-state rows are written for it (not in
  1559|   `GOVERNANCE_STATE_DIRECTED_TYPES`), matching `sibling_projects`.
  1560|   `build_client_summary()`'s `xc_by_client`/`xc_dom_by_client` read
  1561|   `client_label_a`/`client_label_b` directly rather than positionally parsing
  1562|   `segment_id` (the old `len(pa) == 3` assumption only held for the
  1563|   `unit|role|client`-shaped IDs `build_segment_manifest.py` happens to emit
  1564|   for a client-only Project segment; `discover_cross_client()` places no such
  1565|   constraint on `segment_id` shape), with an explicit `ca != cb` guard to
  1566|   preserve the existing within-client-sibling exclusion the old check
  1567|   enforced incidentally. `client_files`'s `n_project_files` backfill now also
  1568|   recognizes `cross_client` rows (previously `sibling_projects`-only), so a
  1569|   client discoverable only via a `cross_client` row no longer falsely reports
  1570|   `n_project_files=0`. New `drop_legacy_sibling_projects_covered_by_cross_client()`
  1571|   in `compare_cross_segment.py` drops a `sibling_projects` pair when
  1572|   `cross_client` already covers the identical two segments (they can share an
  1573|   immediate `parent_segment_id`, since `discover_sibling_segments()` groups
  1574|   purely by parent/role/unit) -- otherwise both would double-count that one
  1575|   pair in `xc`/`xc_by_client` and collide on `comparison_run_id`
  1576|   (`make_comparison_run_id()` hashes only segment IDs + timestamp, not
  1577|   comparison_type -- a broader, pre-existing characteristic of that
  1578|   identifier, not changed here). `cross_client`'s contribution to `xc`
  1579|   (`build_cascade()`) is gated to both-healthcare pairs, matching
  1580|   `sibling_projects`'s existing gate -- `xc` is documented and consumed
  1581|   elsewhere (client-tier "Non-comparable (different sector)" logic) as a
  1582|   healthcare-cohort metric; `discover_cross_client()` itself is unaffected and
  1583|   still emits every client pair into `cross_segment_summary.csv` regardless
  1584|   of sector. `xc_by_client`/`xc_dom_by_client` (`build_client_summary()`,
  1585|   feeding `cross_client_similarity_mean`) gain a softer, consumer-appropriate
  1586|   exclusion -- a pair is dropped only when a side has a CONFIRMED
  1587|   non-healthcare sector (`sector not in ("unknown", "healthcare")`), matching
  1588|   this function's own definition of "comparable"; an unclassified client
  1589|   still counts. This closes a pre-existing gap (this rollup never filtered by
  1590|   sector for either source type) that `cross_client` being default-on and
  1591|   pairing every client made routinely consequential. `main()` in
  1592|   `compare_cross_segment.py` now applies `--segment-a`/`--segment-b`
  1593|   filtering *before*
  1594|   `drop_legacy_sibling_projects_covered_by_cross_client()` rather than after:
  1595|   `discover_sibling_segments()` orders its pair by sorted segment ID while
  1596|   `discover_cross_client()` orders by sorted client label, so the surviving
  1597|   `cross_client` row replacing a dropped `sibling_projects` row can be in the
  1598|   reverse orientation -- which the position-sensitive segment filters would
  1599|   then also reject, making a scoped run silently report zero pairs for
  1600|   segments that do have a comparison. No effect on the default (unscoped)
  1601|   path.
  1602| - `governance_domain_summary.csv` gains `container_to_project_scoped` /
  1603|   `container_to_project_scoped_pair` columns in
  1604|   `tools/generate_governance_narrative.py`. Root cause: `container_to_project`
  1605|   (`cp`) is populated only from rows where BOTH sides are the fully unscoped
```
