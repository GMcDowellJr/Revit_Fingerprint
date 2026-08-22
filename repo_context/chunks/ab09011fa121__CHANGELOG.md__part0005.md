# Chunk of CHANGELOG.md

- Source relative path: `CHANGELOG.md`
- Chunk: 5 of 7
- Original line range: 1596-2008
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4fec943c22afdfaa820cb9077538d951922289c152ad0d6436e45f8ff6d49213
- Starts inside symbol: no
- Ends inside symbol: no

```
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
  1606|   ("enterprise::enterprise") segment -- real Project segments are almost never
  1607|   fully unscoped, so `cp` stayed empty for effectively every domain even
  1608|   though real, `data_sufficient == "true"` container_to_project evidence
  1609|   existed at other scope levels (`cp_by_scope`, already computed but never
  1610|   surfaced in this CSV). The new columns report the mean of the largest
  1611|   (most rows) non-enterprise, `data_sufficient` scope_pair bucket, plus which
  1612|   scope_pair it came from, and are populated only when `container_to_project`
  1613|   itself is empty -- `container_to_project`'s own enterprise-only meaning is
  1614|   unchanged, so this never competes with or is mistaken for enterprise-level
  1615|   evidence (same posture as `TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE`).
  1616|   Sourced from a new, separate accumulator (`cp_by_scope_suff`) rather than a
  1617|   filtered view of `cp_by_scope`, so `_has_group1_bc_pooled_evidence()`/
  1618|   `render_group1_scope_section()` (existing `cp_by_scope` consumers) are
  1619|   unaffected. No other comparison type's `data_sufficient` handling changed.
  1620|   `_TIER_DRIVER_SUPPORT_FIELDS` (the shared list of `governance_domain_summary.csv`
  1621|   columns every tier-based `governance_findings.json` finding's `support[].fields`
  1622|   references) now includes both new columns, so a `missing_or_degraded_evidence`
  1623|   finding for a domain whose only evidence is the scoped fallback (i.e.
  1624|   `container_to_project` itself is blank) still points a consumer at the
  1625|   actual populated value instead of only the blank primary column.
  1626|   `build_client_summary()`'s `xc_by_client` (feeding `cross_client_similarity_mean`)
  1627|   now also skips rows for domains in `EXCLUDED_FROM_SCORING`, matching the
  1628|   gate `xc_dom_by_client` (right below it) and `build_cascade()`'s own
  1629|   per-domain `xc` already apply -- previously a `cross_client` row for a
  1630|   policy-excluded domain (e.g. `view_templates_renderings_drafting`) could
  1631|   still drive a client's overall alignment tier, disagreeing with the rest
  1632|   of the scoring policy. Pre-existing gap for `sibling_projects` too; made
  1633|   routinely reachable by `cross_client` pairing every client for every
  1634|   domain by default.
  1635| - `tools/generate_governance_narrative.py` now emits an interpretation/
  1636|   routing layer: `docs/governance_interpretation_guide.md` (stable,
  1637|   package-type-level -- what each metric/tier means, comparability rules,
  1638|   missing-value semantics, authority ordering, known bad inferences),
  1639|   `docs/governance_question_routes.md` (a candidate question-route catalog,
  1640|   all routes at "candidate" maturity, following the discovery scaffold in
  1641|   the design-reference `llm_evidence_framework` repo's
  1642|   `discovery/question_route_discovery.md`), and `governance_brief.md` (the
  1643|   one new generated, per-run artifact -- a narrower digest built by a new
  1644|   `render_governance_brief()`, which consumes the already-computed findings
  1645|   list and package health directly, computing nothing new). New CLI flags
  1646|   `--emit-interpretation-layer`/`--no-emit-interpretation-layer` (default:
  1647|   on) control `governance_brief.md` only, independently of
  1648|   `--emit-evidence-package`. `governance_evidence_map.json` grows from 19 to
  1649|   22 artifacts; `governance_narrative_context.md`'s authority header gains
  1650|   pointers to all three new artifacts. No existing classification, scoring,
  1651|   or CSV column changed. See D-022 and `docs/governance_evidence_package.md`.
  1652| - `tools/generate_governance_narrative.py`'s governance thresholds, excluded/
  1653|   passive-inheritance-risk domain lists, per-domain guidance text, and
  1654|   client-onboarding interpretation thresholds are now loaded from JSON policy
  1655|   profiles under `policies/governance/` (`governance_thresholds.json`,
  1656|   `domain_governance_policy.json`, `client_onboarding_policy.json`,
  1657|   `finding_rules.json`), via a new sibling module `tools/governance_policy.py`
  1658|   (generic load/fallback loader; no governance business content of its own).
  1659|   `--policy-dir` (accepted but inert since the Phase 1 evidence-package work)
  1660|   now defaults to `policies/governance/` and is actually read: a new
  1661|   `apply_governance_policy()` reassigns every module-level threshold/domain-
  1662|   policy constant this file's existing functions already read as plain
  1663|   globals, so no existing function body changed -- only the source of each
  1664|   constant's value did. The shipped JSON files reproduce this generator's
  1665|   pre-externalization Python literals value-for-value, so no existing
  1666|   invocation's classification output changes by default (locked in by a
  1667|   regression test running the CLI twice -- default vs. explicit
  1668|   `--policy-dir policies/governance/` -- and asserting byte-identical
  1669|   `governance_domain_summary.csv`). A profile file missing from `--policy-dir`
  1670|   falls back, per file, to this generator's own built-in default for that
  1671|   profile only, reported in `governance_package_health.json`'s new
  1672|   `policy_load_status`/a `governance_policy_profile_defaulted` warning
  1673|   (degrades `overall_status` to `degraded`) and in
  1674|   `governance_package_manifest.json`'s `policy_profiles.profiles` (resolved
  1675|   `profile_id`/`schema_version`/`source` per profile). See D-021 and
  1676|   `docs/governance_evidence_package.md`.
  1677| - `tools/generate_governance_narrative.py` now emits `governance_findings.json`:
  1678|   structured, rule-derived governance findings (`baseline_candidate`,
  1679|   `strong_baseline_candidate`, `local_review_required`, `high_fragmentation`,
  1680|   `active_local_practice`, `cross_client_convergence`, `low_client_coherence`,
  1681|   `passive_inheritance_risk`, `missing_or_degraded_evidence`,
  1682|   `leadership_question`) with epistemic provenance (`origin`/`fidelity`/
  1683|   `authority_level`/`limits`) and `support[]` references back to specific
  1684|   `governance_domain_summary.csv`/`governance_client_summary.csv` rows and
  1685|   fields, via a new `build_structured_findings()`. `render_findings_and_recommendations()`
  1686|   now consumes the same structured findings instead of independently
  1687|   recomputing the classification, via a new shared
  1688|   `_classify_domains_for_findings()`, so the narrative's prose and the JSON
  1689|   findings can no longer disagree. Leadership questions are marked
  1690|   `status: question_not_claim` / `authority_level: convenience_summary`,
  1691|   distinct from evidence findings (`status: supported`). No existing CSV
  1692|   column, classification/scoring logic, or threshold changed. See D-020 and
  1693|   `docs/governance_evidence_package.md`.
  1694| - `tools/generate_governance_narrative.py` now emits a governance evidence-package
  1695|   layer alongside its existing outputs: `governance_package_manifest.json`
  1696|   (provenance -- which inputs were provided/found, which outputs were written and
  1697|   their sizes, comparison_run_id(s)/executed_utc observed in the loaded rows),
  1698|   `governance_package_health.json` (schema detection, used-view fallback,
  1699|   comparison_type coverage, blocking conditions, warnings), and
  1700|   `governance_evidence_map.json` (one entry per artifact -- the CSVs the
  1701|   generator reads, two sibling CSVs it produces but never reads
  1702|   (`cross_segment_file_pairs.csv`, `comparison_registry.csv`), and its own six
  1703|   generated artifacts -- with authority_level/grain/can_answer/cannot_answer/
  1704|   known_limitations per the new `tools/governance_evidence_package.py` module).
  1705|   New CLI flags `--emit-evidence-package`/`--no-emit-evidence-package` (default:
  1706|   on), `--policy-dir` (recorded, not yet read), and `--package-schema-version`.
  1707|   The narrative gains a new authority-header section stating its own
  1708|   `controlled_interpretation` role, and the previously-stale producer-identity
  1709|   footer (`generate_governance_narrative_dod_aligned_v2.py`, which never matched
  1710|   the actual script) now references the real generator name. No existing CSV
  1711|   column, classification/scoring logic, or threshold changed -- see D-019 and
  1712|   `docs/governance_evidence_package.md`. Structured findings
  1713|   (`governance_findings.json`) and policy externalization are deferred to later
  1714|   work.
  1715| - `tools/generate_governance_narrative.py`'s `build_cascade()` now breaks
  1716|   `gt`/`gc`/`gp` (generic->template/container/project containment) down by the
  1717|   TARGET's own scope level, instead of discarding every row where the target
  1718|   isn't the single broadest ("enterprise") population. `compare_cross_segment.py`
  1719|   intentionally emits `generic_to_template`/`_container`/`_project` rows for
  1720|   client-/bc-/discipline-scoped targets too — real baseline-propagation evidence
  1721|   that a prior pass (PR #350) deliberately gated away to keep `gt`/`gc`/`gp` as a
  1722|   single clean enterprise-wide number (Option A, avoiding the blend-distinct-
  1723|   scope-grains anti-pattern this file's other fixes already correct for). `gt`/
  1724|   `gc`/`gp` themselves are unchanged — still the enterprise-only slice — but a
  1725|   new `gt_by_scope`/`gc_by_scope`/`gp_by_scope` (`{scope_label: mean_containment}`,
  1726|   mirroring the existing `wp_disc` per-discipline breakdown pattern) now captures
  1727|   every other scope level (`client`, `bc`, `discipline`, and combinations, via a
  1728|   new `_target_scope_label()` using the `business_center_label_a/b` columns added
  1729|   in the intervening B6 schema fix) rather than silently dropping it. The
  1730|   GENERIC (reference) side of the comparison is still required to be the one
  1731|   canonical enterprise-wide Generic population.
  1732| 
  1733|   Rendering/anomaly-detection followed as a second pass: `detect_anomalies()`
  1734|   now flags a material (≥0.25 absolute) divergence between the enterprise
  1735|   reading and the mean of a domain's scoped buckets, in either direction, per
  1736|   cascade stage (Generic→Template/Container/Project); a new
  1737|   `render_generic_baseline_scope_section()` renders one row per
  1738|   `(domain, scope)` pair actually observed (`Domain | Scope | G→Template |
  1739|   G→Container | G→Project`) — a fixed-column table doesn't fit here since scope
  1740|   buckets are combinatorial (`client`, `bc`, `discipline`, `client_discipline`,
  1741|   etc.), not a small fixed set like disciplines. The section is omitted
  1742|   entirely when no domain has any scope-breakdown data.
  1743| 
  1744| - `tools/generate_governance_narrative.py`'s Group 1 dispatch (`tc`/`cp`/`tp`
  1745|   from `template_to_container`/`container_to_project`/`template_to_project`)
  1746|   gets the same Option C treatment Group 2 (`gt`/`gc`/`gp`) got above, closing
  1747|   the gap documented in
  1748|   `docs/governance_narrative_group1_scope_gap_investigation.md`: since
  1749|   `business_center_label` became a real segmentation cut, almost no segment is
  1750|   fully unscoped anymore, so `tp`/`cp` were `None` for effectively every
  1751|   domain and `assign_tier()` always fell to `TIER_INSUFFICIENT` regardless of
  1752|   real bc-pooled evidence sitting unused in `cross_segment_summary.csv`. `tc`/
  1753|   `cp`/`tp` themselves are unchanged — still populated only from the
  1754|   `"enterprise::enterprise"` (both sides pass `_is_unscoped_segment()`) pair —
  1755|   but new `tc_by_scope`/`cp_by_scope`/`tp_by_scope` (`{scope_pair:
  1756|   mean_containment}`, keyed `f"{scope_a}::{scope_b}"` since, unlike Group 2,
  1757|   neither side of a Group 1 pair is gated to a fixed role population) now
  1758|   capture every other `(scope_a, scope_b)` pair instead of discarding it. The
  1759|   separator is `"::"`, not a bare `"_"`, because `_target_scope_label()`'s own
  1760|   multi-dimension labels (e.g. `"bc_discipline"`, `"client_bc"`) already
  1761|   contain underscores — joining two such labels with `"_"` is ambiguous
  1762|   (`("client", "bc_discipline")` and `("client_bc", "discipline")` both
  1763|   produce the literal string `"client_bc_discipline"`) and this was confirmed
  1764|   to actually occur against a real `cross_segment_summary.csv` export during
  1765|   review, not just a theoretical edge case.
  1766| 
  1767|   A same-bc-both-sides (`"bc::bc"`) pooled value gives `assign_tier()` a new,
  1768|   distinctly-named fallback tier, `TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE`
  1769|   (ordered directly before `TIER_INSUFFICIENT`, i.e. the weakest tier that
  1770|   still has *some* evidence), when `tp`/`cp` are both `None` — deliberately
  1771|   NOT blended into the existing enterprise-only `primary`/score-banded tiers,
  1772|   since bc-pooled evidence is not enterprise-level evidence. The `T→Container`/
  1773|   `T→Project`/`C→Project` columns in `render_domain_tiers()` stay `—` for
  1774|   domains in the new tier (never silently repointed at a pooled number); a new
  1775|   `render_group1_scope_section()` (mirroring `render_generic_baseline_scope_section()`)
  1776|   renders the per-`(domain, scope_pair)` detail instead. `detect_anomalies()`
  1777|   gained a Group 1 analog of the existing scope-divergence note: since Group 1
  1778|   usually has no enterprise reading to diverge from (that's the gap this fix
  1779|   closes), the check instead flags when a pooled bucket's own intra-bucket
  1780|   spread (min/max across the individual rows pooled into it) is ≥0.25
  1781|   absolute — the same materiality threshold as Group 2's check — meaning the
  1782|   pooled mean is hiding sharp disagreement rather than reflecting genuine
  1783|   convergence. The note's wording is deliberately scope-neutral rather than
  1784|   always saying "business-center": validating against a real
  1785|   `cross_segment_summary.csv` showed most divergence notes actually fire for
  1786|   scope pairs like `client_bc::client_discipline`, where the client and
  1787|   business center are held constant and only the discipline varies across the
  1788|   pooled rows — an earlier wording draft said "across individual
  1789|   business-center pairs" unconditionally, which was accurate only for the
  1790|   `"bc::bc"` case and misleading for every other scope_pair.
  1791| 
  1792| - Four PR-review findings on the Group 1 bc-pooled fallback above, all
  1793|   confirmed against the real `cross_segment_summary.csv`/`segment_manifest.csv`
  1794|   export supplied during review:
  1795|   1. **Value-mismatch guard (new `_group1_scope_pair()`)**: `_target_scope_label()`
  1796|      only records SHAPE (which dimensions are populated), not VALUE.
  1797|      `discover_within_segment()` in `compare_cross_segment.py` pairs same-parent,
  1798|      same-unit Template/Container/Project segments without checking that scope
  1799|      label VALUES match, so a `BC_1`-scoped segment paired against a
  1800|      `BC_2`-scoped segment was silently bucketed as `"bc::bc"` — the same key as
  1801|      genuine same-business-center evidence. Confirmed reachable in the real
  1802|      export: one real row (`client_bc_discipline` shape on both sides, one field
  1803|      mismatched) was landing in a merged bucket, corrupting 20 domains'
  1804|      `tc_by_scope` entries. New `_group1_scope_pair()` verifies every field
  1805|      making up a shared shape actually matches before using the plain
  1806|      `f"{scope_a}::{scope_b}"` key; a same-shape-different-value pair now gets a
  1807|      distinct `f"{scope_a}!cross::{scope_b}!cross"` key instead — captured, not
  1808|      discarded, but never conflated with same-value pooled evidence. `tc`/`cp`/`tp`
  1809|      remain byte-for-byte unchanged (re-verified: 0 mismatches across all 32 real
  1810|      domains).
  1811|   2. **`_has_renderable_cascade_signal()` scope-only gap**: a domain whose ONLY
  1812|      Group 1 signal is scoped evidence (e.g. `tp_by_scope["bc::bc"]` populated
  1813|      but no enterprise `tc`/`cp`/`tp` and no `wp_all`/Group 2 signal) would get
  1814|      `TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE` from `assign_tier()` but never
  1815|      appear in `render_domain_tiers()`/the domain CSV, since
  1816|      `_has_renderable_cascade_signal()`'s key list didn't include
  1817|      `tc_by_scope`/`cp_by_scope`/`tp_by_scope` (which are always non-`None`
  1818|      dicts, so they can't reuse the existing `is not None` check). Now also
  1819|      checks for a non-empty by-scope dict.
  1820|   3. **`render_group1_scope_section()` prose overclaimed "business-center-level"**:
  1821|      the section's intro described every non-enterprise row as "pooled
  1822|      business-center-level evidence," but it renders every scope_pair, most of
  1823|      which (`client::bc`, `client_bc::discipline`, etc.) are not business-center
  1824|      evidence at all. Reworded to name only `"bc::bc"` as business-center-level
  1825|      and tier-relevant; other scope pairs are described as real evidence in
  1826|      their own right that does not by itself grant the new tier. (The
  1827|      equivalent `detect_anomalies()` wording was already fixed in the prior
  1828|      commit.)
  1829| 
  1830| ### Fixed
  1831| - `tools/archetype/generate_archetype_candidates.py`'s `_governance_question_hint()`
  1832|   only ever inspected `target_domain`, so it couldn't distinguish a dynamic
  1833|   View Filter Definition (VFD) edge from a structural one. Dynamic VFD edges
  1834|   carry `source_domain == "view_filter_definitions"` but `target_domain` ==
  1835|   whatever element-type domain the filter scopes to (`wall_types`,
  1836|   `ceiling_types`, `floor_types`, `roof_types`); the static
  1837|   `view_filter_applications_view_templates.stack_filter__view_filter_definitions`
  1838|   chain edge instead carries `target_domain == "view_filter_definitions"`.
  1839|   Two consequences, both independently documented in
  1840|   `tools/archetype/review/archetype_dp1_prompt.md`'s known-misfire list as a
  1841|   manual correction required every Decision Point 1 cycle: (1) a VFD-to-VFD
  1842|   pair targeting `wall_types` collided with the `wall_graphics` predicate
  1843|   (`"wall_types" in target_domain`) before any VFD-aware check existed; (2) a
  1844|   VFD-to-VFD pair targeting `ceiling_types`/`floor_types`/`roof_types` matched
  1845|   none of the target-domain predicates and fell through to `"unknown"`.
  1846|   Fixed by adding `_is_vfd_related(source_domain, target_domain)` (true when
  1847|   `source_domain == "view_filter_definitions"` OR `target_domain ==
  1848|   "view_filter_definitions"`), returning `"view_filter_strategy"` when both
  1849|   sides of the pair are VFD-related, checked before the existing
  1850|   target-domain-only priority list. The first version of this fix only
  1851|   checked `source_domain_a == source_domain_b == "view_filter_definitions"`
  1852|   (VFD-to-VFD only) and still misclassified a VFD edge paired with the static
  1853|   stack_filter chain edge as `wall_graphics` — caught in PR #357 review and
  1854|   corrected to the broader `_is_vfd_related()` form above. This only affects
  1855|   auto-generated candidates in `archetype_definitions_candidates.json`; it
  1856|   does not retroactively change `governance_question` on already-promoted
  1857|   archetypes in `config/archetype/archetype_definitions.json`, which are set
  1858|   by human curation at DP1 independent of this hint.
  1859| 
  1860| - `tools/generate_governance_narrative.py` read `client_label`/`discipline_label`/
  1861|   the "is this the broadest population for its role" condition by parsing
  1862|   `segment_id` positionally (`get_client()`, `get_disc()`, `is_generic()`, a
  1863|   `"Template" in segment_id` substring check) instead of the real
  1864|   `client_label_a/b`/`discipline_label_a/b`/`governance_role_a/b` columns that
  1865|   already exist on `SUMMARY_FIELDS`. This silently misparsed segments whose
  1866|   third pipe-separated part is a `business_center_label`/`collection_label`
  1867|   rather than a client (e.g. `imperial|Template|Shared` read as
  1868|   `client="Shared"`), and `is_generic()`'s length-2 heuristic couldn't
  1869|   distinguish a genuine broadest-role segment from a blank-`governance_role`
  1870|   scope rollup that also happens to produce 2 parts (e.g. `imperial|BC_2014`).
  1871|   Replaced with direct column reads and a `_is_unscoped_segment()` helper
  1872|   (role non-blank, `client_label`/`discipline_label` both blank). Two follow-on
  1873|   refinements to that helper, both confirmed against real segment-manifest
  1874|   construction: (1) `business_center_label`/`collection_label` are not yet
  1875|   columns on `SUMMARY_FIELDS`, so a segment scoped only by one of those two
  1876|   dimensions (e.g. `imperial|Template|BC_1234`) can slip past the column checks
  1877|   — rejected via a structural check that any segment_id part beyond
  1878|   `unit_system+role` must be blank once client/discipline are confirmed blank
  1879|   via their own columns; (2) that same check initially rejected a *genuinely*
  1880|   unscoped segment whose `client_label`/`discipline_label` dimension is
  1881|   explicitly selected-but-blank in its key (`build_segment_manifest.py`'s
  1882|   `_subset_to_id()` emits a literal empty token for this, e.g.
  1883|   `imperial|Template||Shared` for a blank client alongside a real
  1884|   `business_center_label` — see that function's own code comment), which is
  1885|   not hidden scope data and must not cause rejection; fixed by requiring only
  1886|   that any extra part be *empty*, not merely that there are exactly 2 parts.
  1887| 
  1888| - `tools/generate_governance_narrative.py`'s `build_cascade()` was a bare
  1889|   `if/elif` chain recognizing 5 of the ~16 `comparison_type` values
  1890|   `compare_cross_segment.py` can emit, silently dropping every other row with
  1891|   no signal that anything was excluded — including all four new scope-level
  1892|   types (`enterprise_to_project`, `bc_to_project`, `enterprise_to_bc`,
  1893|   `enterprise_to_client`) and the `generic_to_template`/`_container`/`_project`
  1894|   triple that is the literal top rung of the "Governance Cascade" diagram the
  1895|   narrative's own header already describes but never computed. Replaced with
  1896|   an explicit dispatch naming every known type across four groups (already-
  1897|   handled cascade stages; the newly-wired generic-to-* stage, threaded through
  1898|   as new `gt`/`gc`/`gp` fields and rendered as new table columns; the four
  1899|   scope-level types, captured under new `ep`/`bp`/`eb`/`ec` keys but
  1900|   deliberately not rendered/tiered yet — a scope-level axis, not one more
  1901|   cascade stage; and an explicit "known, deliberately excluded" registry for
  1902|   `sibling_templates`/`sibling_containers`/`sibling_generic`/`sibling_segments`/
  1903|   `governance_chain`, each with a verified reason) plus a coverage-check
  1904|   warning for any comparison_type not accounted for by name in any group.
  1905| 
  1906| - `build_governance_state_summary()`'s compact-summary loop had no
  1907|   `comparison_type` filter on any of its count/share fields, so rows for the
  1908|   four new scope-level types were silently averaged into the same per-domain
  1909|   number as `template_to_project`/`container_to_project` — a scope-level axis
  1910|   blended into a cascade-stage number with no indication it happened (traced:
  1911|   a synthetic `bc_to_project` + `template_to_project` pair for one domain
  1912|   produced a blended `provided_passive_share` of 0.375 pre-fix; 0.05 —
  1913|   `template_to_project` alone — post-fix). Its detailed per-pattern loop's own
  1914|   `_DIRECTED_GOVERNANCE_TYPES` gate was a stale hand-maintained copy of
  1915|   `compare_cross_segment.py`'s `GOVERNANCE_STATE_DIRECTED_TYPES`, missing all
  1916|   four new types and carrying two entries (`generic_to_downstream`,
  1917|   `parent_sibling_roles`) confirmed to never reach a governance-state output
  1918|   file today. Fixed by keying aggregation by `(domain, comparison_type)`
  1919|   throughout and importing `GOVERNANCE_STATE_DIRECTED_TYPES` directly instead
  1920|   of hand-copying it; the two unexplained legacy entries are kept rather than
  1921|   silently dropped pending confirmation of their disposition. A domain whose
  1922|   *entire* governance-state signal is scope-level-only is now correctly
  1923|   omitted from the returned map rather than stored as an all-`None`-valued but
  1924|   still-truthy dict, which had been switching its whole tier group's rendered
  1925|   table to state-columns mode with every visible state value blank.
  1926| 
  1927| - `DISC_KEYWORDS`/`DISC_LABELS` hardcoded a 7-discipline set that `get_disc()`
  1928|   used as the sole vocabulary for discipline detection, and
  1929|   `render_discipline_section()` iterated `DISC_LABELS.keys()` to decide which
  1930|   disciplines to render a section for — so any discipline outside that set
  1931|   (confirmed real: `lighting`, `medical_equipment`, `security`, alongside the
  1932|   existing 7) was invisible in that section even though the underlying
  1933|   `discipline_label_a/b` data already had it. Discipline vocabulary is now
  1934|   computed from the data actually present (`disc_domain_wp.keys()`);
  1935|   `DISC_LABELS` is kept only as an optional display-name override, falling
  1936|   back to a humanized title-case render (e.g. `medical_equipment` ->
  1937|   `"Medical Equipment"`) for anything not in the override map.
  1938| 
  1939| - `HEALTHCARE_CLIENTS = {"Kaiser", "Sutter", "Renown", "DCMH"}` plus a
  1940|   standalone `if client == "Intel": tier = "Non-comparable (different
  1941|   sector)"` special case hardcoded a business fact (client sector membership)
  1942|   that cannot be derived from the pipeline's own data into Python literals,
  1943|   requiring a code change and redeploy for every new client. Replaced with a
  1944|   `sector_map` lookup loaded from a new optional `client_sector.csv`
  1945|   (`client_label,sector` columns, `--client-sector`, defaulting to
  1946|   `policies/client_sector.csv` so existing invocations that don't pass the
  1947|   flag still get today's classification rather than silently losing the
  1948|   cross-client-convergence signal for every domain). An unclassified client
  1949|   (absent from the file, or the file itself absent) is `sector = "unknown"`,
  1950|   which now falls through to normal alignment tiering rather than being
  1951|   treated as either "Non-comparable" (that requires an explicit, *known*
  1952|   non-healthcare sector) or a confirmed different-sector profile in the
  1953|   onboarding-implications text — both of those previously fired for any
  1954|   `is_healthcare == False`, which conflated "known different sector" with "we
  1955|   don't know."
  1956| - `tools/compare_cross_segment.py` Mode D (`within_project`) grouped files by
  1957|   `project_label` using `.strip() or eid` — a fallback that only catches a
  1958|   truly-blank string, not a populated NA placeholder like
  1959|   `"__NOT_APPLICABLE__"`, `"n/a"`, or `"NA"`. Every file in a segment whose
  1960|   project is unassigned carries the exact same placeholder string, so all of
  1961|   them collapsed into one giant fake "project" and got pairwise-compared
  1962|   against each other (`C(n,2)` spurious pairs for `n` unassigned files —
  1963|   484 files in the `imperial` segment pre-fix). Fixed at all four sites that
  1964|   used this pattern: the `discover_within_project()` pair-discovery gate,
  1965|   both grouping loops (`by_proj`/`by_proj_used`, all-view and used-view) in
  1966|   `run_pair()`'s `is_within_project` branch, and `_project_label_for_file()`
  1967|   (used by `build_union_inventory_rows()` for the `n_projects_present`/
  1968|   `n_projects_denominator` union-inventory counts). All four now use
  1969|   `na_token.is_blank_or_na()` — the same NA-recognition helper Mode E's
  1970|   `discover_governance_chain()` already uses for `client_label`/
  1971|   `collection_label` — to decide when to fall back to the per-file `eid`
  1972|   singleton key, so unassigned-project files no longer group with each
  1973|   other (each remains its own singleton, same as a truly-blank label
  1974|   already did) while real shared `project_label` values (e.g. `"Renown"`,
  1975|   41 files) are unaffected.
  1976| 
  1977| - `tools/build_segment_manifest.py` `_sanitize_folder()` collapsed consecutive
  1978|   separator characters into one `_` and trimmed leading/trailing `_`, which
  1979|   erased a real distinction in `segment_id`: a cut dimension explicitly
  1980|   selected in a subset with a blank value (today only `client_label` — see
  1981|   `_build_segments()`'s blank-client handling) renders as an empty part
  1982|   between/after separator pipes (e.g. `imperial|Template|` or
  1983|   `imperial|Container||architectural`), which is a *different, smaller*
  1984|   population than that same dimension not being selected at all (e.g.
  1985|   `imperial|Template`, which pools every value of the field, blank
  1986|   included — always a superset of the selected-blank population). Both
  1987|   forms sanitized to the identical folder name, so once enough blank-client
  1988|   rows exist for the two populations to diverge (no longer collapsible via
  1989|   the existing `redundant_single_child` dedup), both become real,
  1990|   independently `bundle`/`reference`-eligible segments competing for the
  1991|   same `output_folder` — surfaced only as an opaque `_2` collision-avoidance
  1992|   suffix rather than a clear identity. `_UNSAFE_FOLDER_CHARS` no longer uses
  1993|   a `+` quantifier (each unsafe character is replaced one-for-one, so
  1994|   consecutive separators no longer collapse to a single `_`) and the final
  1995|   `.strip("_")` was removed, so a trailing/embedded blank-selected segment
  1996|   now sanitizes to a distinguishable folder name. Each blank part is also
  1997|   rendered as the literal token `enterprise` (the same scope-level term
  1998|   `compare_cross_segment.py` already uses for "no client, no bc" rows)
  1999|   rather than a bare `_`/`__`, so e.g. `imperial|Template|` sanitizes to
  2000|   `imperial_template_enterprise` instead of `imperial_template_` — a
  2001|   self-explanatory name instead of something that reads as a naming
  2002|   mistake. `segment_id` text itself (used elsewhere — parsed positionally
  2003|   in `tools/generate_governance_narrative.py` and hardcoded across dozens
  2004|   of existing tests) is completely unchanged; only the derived folder name
  2005|   changes, and only for segments that select a blank cut-dimension value.
  2006|   Verified against a real corpus manifest: 5 `bundle`/`reference`-eligible
  2007|   folder-name collisions, all resolved.
  2008| 
```
