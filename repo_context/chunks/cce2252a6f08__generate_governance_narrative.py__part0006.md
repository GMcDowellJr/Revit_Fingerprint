# Chunk of tools/generate_governance_narrative.py

- Source relative path: `tools/generate_governance_narrative.py`
- Chunk: 6 of 17
- Original line range: 1929-2432
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: apply_governance_policy, apply_governance_policy.th, apply_governance_policy.ct, apply_governance_policy.at_, _state_value, _has_material_state_exception, _has_group1_bc_pooled_evidence, assign_tier, detect_anomalies
- Source SHA-256: 7a8e1def8713100e21b852dc24c2c714a8e5330815affb3e703ed572a2829d9c
- Starts inside symbol: no
- Ends inside symbol: no

```
  1929| def apply_governance_policy(policy: dict) -> None:
  1930|     """Reassign this module's threshold/domain-policy globals from a
  1931|     load_governance_policy() result. Every function in this file already
  1932|     reads these names as plain module globals (see the _DEFAULT_*/name
  1933|     pairs above) -- this is the one place that makes them policy-driven
  1934|     instead of hardcoded, without threading a policy object through every
  1935|     call site. Falls back to this module's own _DEFAULT_* value, per key,
  1936|     if a resolved profile is missing an expected key (e.g. a hand-edited
  1937|     --policy-dir file that only overrides some thresholds).
  1938|     """
  1939|     global LOADED_GOVERNANCE_POLICY, FINDING_RULE_DESCRIPTIONS
  1940|     global EXCLUDED_FROM_SCORING, PASSIVE_INHERITANCE_RISK_DOMAINS
  1941|     global DOMAIN_GUIDANCE, STATIC_FINDINGS_GUIDANCE
  1942|     global RELIABILITY_TIGHT_P10, RELIABILITY_CONVERGENT_P10, RELIABILITY_CONVERGENT_SPREAD_MAX
  1943|     global RELIABILITY_LOW_P10_MAX, RELIABILITY_PRESENCE_P90_MIN, RELIABILITY_SPARSE_MEAN_MAX
  1944|     global LOCAL_ACTIVE_MATERIAL_THRESHOLD, PASSIVE_MATERIAL_THRESHOLD, MISSING_MATERIAL_THRESHOLD
  1945|     global ACTIVE_USE_MIN_FOR_STRONG_BASELINE, TIER_SPARSE_PRIMARY_MAX, TIER_ACTIVE_LOCAL_PRIMARY_MAX
  1946|     global TIER_STRONG_BASELINE_MIN, TIER_CONTAINER_GAP_TC_MAX, TIER_INVESTIGATE_MIN, TIER_MODERATE_VARIATION_MIN
  1947|     global XC_STRONG_CONVERGENCE, XC_LOW_THRESHOLD, XC_LOW_TP_MIN
  1948|     global CLIENT_ALIGNMENT_HIGH, CLIENT_ALIGNMENT_MODERATE
  1949|     global CLIENT_CONFIDENCE_LOW_MAX_FILES, CLIENT_CONFIDENCE_MODERATE_MAX_FILES, CLIENT_COHERENCE_LOW
  1950|     global BC_ALIGNMENT_HIGH, BC_ALIGNMENT_MODERATE
  1951|     global BC_CONFIDENCE_LOW_MAX_FILES, BC_CONFIDENCE_MODERATE_MAX_FILES
  1952|     global ONBOARD_WP_STABLE_MIN, ONBOARD_WP_MIXED_MIN
  1953|     global ONBOARD_XC_HIGH_PORTABILITY_MIN, ONBOARD_XC_MODERATE_PORTABILITY_MIN
  1954|     global ONBOARD_N_FILES_LOW_MAX, ONBOARD_N_FILES_MODERATE_MAX
  1955|     global PROVIDED_CARRIED_DOWNSTREAM_MIN, PROVIDED_ACTIVE_USE_MAX, PRIMARY_READ_ACTIVE_USE_MIN
  1956|     global PASSIVE_INDICATOR_HIGH_MIN, PASSIVE_INDICATOR_MODERATE_MIN
  1957|     global PASSIVE_INHERITANCE_RISK_BUNDLE_SHARE_MAX, BUNDLE_SHARE_VERY_LOW_MAX
  1958|     global GT_TP_GAP_GT_MIN, GT_TP_GAP_TP_MAX
  1959|     global GROUP2_SCOPE_DIVERGENCE_GAP_MIN, GROUP1_SCOPE_SPREAD_GAP_MIN
  1960|     global TP_TC_BYPASS_GAP_MIN, WEAK_TC_MAX, WEAK_CP_MAX
  1961|     global VIEW_TEMPLATE_ZERO_DISCIPLINE_MAX, PHASES_TP_EXTENSION_MAX, PHASES_TW_MIN
  1962|     global PORTFOLIO_SHAPE_DENSITY_MIN, PORTFOLIO_SHAPE_UNION_JACCARD_MAX
  1963|     global UNION_BREADTH_CORPUS_WIDE_CLIENTS_PCT_MIN, UNION_BREADTH_CLIENT_WIDE_CLIENTS_PCT_MIN
  1964|     global UNION_BREADTH_PROJECT_WIDE_MIN_PROJECTS, UNION_BREADTH_FILE_LEVEL_MAX_FILES
  1965|     global UNION_BREADTH_BROAD_MIN_PATTERNS, UNION_BREADTH_NARROW_FILE_LEVEL_SHARE_MIN
  1966|     global UNION_BREADTH_WEAK_CASCADE_MAX, UNION_BREADTH_STRONG_CASCADE_MIN
  1967| 
  1968|     LOADED_GOVERNANCE_POLICY = policy
  1969|     profiles = policy["profiles"]
  1970| 
  1971|     t = profiles["thresholds"].get("thresholds", {})
  1972| 
  1973|     def th(key: str, default):
  1974|         return t.get(key, default)
  1975| 
  1976|     RELIABILITY_TIGHT_P10 = th("reliability_tight_p10", _DEFAULT_RELIABILITY_TIGHT_P10)
  1977|     RELIABILITY_CONVERGENT_P10 = th("reliability_convergent_p10", _DEFAULT_RELIABILITY_CONVERGENT_P10)
  1978|     RELIABILITY_CONVERGENT_SPREAD_MAX = th("reliability_convergent_spread_max", _DEFAULT_RELIABILITY_CONVERGENT_SPREAD_MAX)
  1979|     RELIABILITY_LOW_P10_MAX = th("reliability_low_p10_max", _DEFAULT_RELIABILITY_LOW_P10_MAX)
  1980|     RELIABILITY_PRESENCE_P90_MIN = th("reliability_presence_p90_min", _DEFAULT_RELIABILITY_PRESENCE_P90_MIN)
  1981|     RELIABILITY_SPARSE_MEAN_MAX = th("reliability_sparse_mean_max", _DEFAULT_RELIABILITY_SPARSE_MEAN_MAX)
  1982| 
  1983|     LOCAL_ACTIVE_MATERIAL_THRESHOLD = th("local_active_material_threshold", _DEFAULT_LOCAL_ACTIVE_MATERIAL_THRESHOLD)
  1984|     PASSIVE_MATERIAL_THRESHOLD = th("passive_material_threshold", _DEFAULT_PASSIVE_MATERIAL_THRESHOLD)
  1985|     MISSING_MATERIAL_THRESHOLD = th("missing_material_threshold", _DEFAULT_MISSING_MATERIAL_THRESHOLD)
  1986|     ACTIVE_USE_MIN_FOR_STRONG_BASELINE = th("active_use_min_for_strong_baseline", _DEFAULT_ACTIVE_USE_MIN_FOR_STRONG_BASELINE)
  1987|     TIER_SPARSE_PRIMARY_MAX = th("tier_sparse_primary_max", _DEFAULT_TIER_SPARSE_PRIMARY_MAX)
  1988|     TIER_ACTIVE_LOCAL_PRIMARY_MAX = th("tier_active_local_primary_max", _DEFAULT_TIER_ACTIVE_LOCAL_PRIMARY_MAX)
  1989|     TIER_STRONG_BASELINE_MIN = th("tier_strong_baseline_min", _DEFAULT_TIER_STRONG_BASELINE_MIN)
  1990|     TIER_CONTAINER_GAP_TC_MAX = th("tier_container_gap_tc_max", _DEFAULT_TIER_CONTAINER_GAP_TC_MAX)
  1991|     TIER_INVESTIGATE_MIN = th("tier_investigate_min", _DEFAULT_TIER_INVESTIGATE_MIN)
  1992|     TIER_MODERATE_VARIATION_MIN = th("tier_moderate_variation_min", _DEFAULT_TIER_MODERATE_VARIATION_MIN)
  1993| 
  1994|     XC_STRONG_CONVERGENCE = th("cross_client_convergence_strong", _DEFAULT_XC_STRONG_CONVERGENCE)
  1995|     XC_LOW_THRESHOLD = th("cross_client_convergence_low", _DEFAULT_XC_LOW_THRESHOLD)
  1996|     XC_LOW_TP_MIN = th("cross_client_low_tp_min", _DEFAULT_XC_LOW_TP_MIN)
  1997|     CLIENT_ALIGNMENT_HIGH = th("client_alignment_high", _DEFAULT_CLIENT_ALIGNMENT_HIGH)
  1998|     CLIENT_ALIGNMENT_MODERATE = th("client_alignment_moderate", _DEFAULT_CLIENT_ALIGNMENT_MODERATE)
  1999|     CLIENT_CONFIDENCE_LOW_MAX_FILES = th("client_confidence_low_max_files", _DEFAULT_CLIENT_CONFIDENCE_LOW_MAX_FILES)
  2000|     CLIENT_CONFIDENCE_MODERATE_MAX_FILES = th("client_confidence_moderate_max_files", _DEFAULT_CLIENT_CONFIDENCE_MODERATE_MAX_FILES)
  2001|     CLIENT_COHERENCE_LOW = th("client_coherence_low", _DEFAULT_CLIENT_COHERENCE_LOW)
  2002|     BC_ALIGNMENT_HIGH = th("bc_alignment_high", _DEFAULT_BC_ALIGNMENT_HIGH)
  2003|     BC_ALIGNMENT_MODERATE = th("bc_alignment_moderate", _DEFAULT_BC_ALIGNMENT_MODERATE)
  2004|     BC_CONFIDENCE_LOW_MAX_FILES = th("bc_confidence_low_max_files", _DEFAULT_BC_CONFIDENCE_LOW_MAX_FILES)
  2005|     BC_CONFIDENCE_MODERATE_MAX_FILES = th("bc_confidence_moderate_max_files", _DEFAULT_BC_CONFIDENCE_MODERATE_MAX_FILES)
  2006| 
  2007|     dp = profiles["domain_policy"]
  2008|     EXCLUDED_FROM_SCORING = set(dp.get("excluded_from_scoring", sorted(_DEFAULT_EXCLUDED_FROM_SCORING)))
  2009|     PASSIVE_INHERITANCE_RISK_DOMAINS = set(
  2010|         dp.get("passive_inheritance_risk_domains", sorted(_DEFAULT_PASSIVE_INHERITANCE_RISK_DOMAINS))
  2011|     )
  2012|     DOMAIN_GUIDANCE = dict(dp.get("domain_guidance", _DEFAULT_DOMAIN_GUIDANCE))
  2013|     STATIC_FINDINGS_GUIDANCE = list(dp.get("static_findings_guidance", _DEFAULT_STATIC_FINDINGS_GUIDANCE))
  2014| 
  2015|     co = profiles["client_onboarding"].get("thresholds", {})
  2016| 
  2017|     def ct(key: str, default):
  2018|         return co.get(key, default)
  2019| 
  2020|     ONBOARD_WP_STABLE_MIN = ct("wp_stable_min", _DEFAULT_ONBOARD_WP_STABLE_MIN)
  2021|     ONBOARD_WP_MIXED_MIN = ct("wp_mixed_min", _DEFAULT_ONBOARD_WP_MIXED_MIN)
  2022|     ONBOARD_XC_HIGH_PORTABILITY_MIN = ct("xc_high_portability_min", _DEFAULT_ONBOARD_XC_HIGH_PORTABILITY_MIN)
  2023|     ONBOARD_XC_MODERATE_PORTABILITY_MIN = ct("xc_moderate_portability_min", _DEFAULT_ONBOARD_XC_MODERATE_PORTABILITY_MIN)
  2024|     ONBOARD_N_FILES_LOW_MAX = ct("n_files_low_max", _DEFAULT_ONBOARD_N_FILES_LOW_MAX)
  2025|     ONBOARD_N_FILES_MODERATE_MAX = ct("n_files_moderate_max", _DEFAULT_ONBOARD_N_FILES_MODERATE_MAX)
  2026| 
  2027|     FINDING_RULE_DESCRIPTIONS = dict(profiles["finding_rules"].get("rules", {}))
  2028| 
  2029|     at = profiles["anomaly_thresholds"].get("thresholds", {})
  2030| 
  2031|     def at_(key: str, default):
  2032|         return at.get(key, default)
  2033| 
  2034|     PROVIDED_CARRIED_DOWNSTREAM_MIN = at_("provided_carried_downstream_min", _DEFAULT_PROVIDED_CARRIED_DOWNSTREAM_MIN)
  2035|     PROVIDED_ACTIVE_USE_MAX = at_("provided_active_use_max", _DEFAULT_PROVIDED_ACTIVE_USE_MAX)
  2036|     PRIMARY_READ_ACTIVE_USE_MIN = at_("primary_read_active_use_min", _DEFAULT_PRIMARY_READ_ACTIVE_USE_MIN)
  2037|     PASSIVE_INDICATOR_HIGH_MIN = at_("passive_indicator_high_min", _DEFAULT_PASSIVE_INDICATOR_HIGH_MIN)
  2038|     PASSIVE_INDICATOR_MODERATE_MIN = at_("passive_indicator_moderate_min", _DEFAULT_PASSIVE_INDICATOR_MODERATE_MIN)
  2039|     PASSIVE_INHERITANCE_RISK_BUNDLE_SHARE_MAX = at_(
  2040|         "passive_inheritance_risk_bundle_share_max", _DEFAULT_PASSIVE_INHERITANCE_RISK_BUNDLE_SHARE_MAX
  2041|     )
  2042|     BUNDLE_SHARE_VERY_LOW_MAX = at_("bundle_share_very_low_max", _DEFAULT_BUNDLE_SHARE_VERY_LOW_MAX)
  2043|     GT_TP_GAP_GT_MIN = at_("gt_tp_gap_gt_min", _DEFAULT_GT_TP_GAP_GT_MIN)
  2044|     GT_TP_GAP_TP_MAX = at_("gt_tp_gap_tp_max", _DEFAULT_GT_TP_GAP_TP_MAX)
  2045|     GROUP2_SCOPE_DIVERGENCE_GAP_MIN = at_("group2_scope_divergence_gap_min", _DEFAULT_GROUP2_SCOPE_DIVERGENCE_GAP_MIN)
  2046|     GROUP1_SCOPE_SPREAD_GAP_MIN = at_("group1_scope_spread_gap_min", _DEFAULT_GROUP1_SCOPE_SPREAD_GAP_MIN)
  2047|     TP_TC_BYPASS_GAP_MIN = at_("tp_tc_bypass_gap_min", _DEFAULT_TP_TC_BYPASS_GAP_MIN)
  2048|     WEAK_TC_MAX = at_("weak_tc_max", _DEFAULT_WEAK_TC_MAX)
  2049|     WEAK_CP_MAX = at_("weak_cp_max", _DEFAULT_WEAK_CP_MAX)
  2050|     VIEW_TEMPLATE_ZERO_DISCIPLINE_MAX = at_("view_template_zero_discipline_max", _DEFAULT_VIEW_TEMPLATE_ZERO_DISCIPLINE_MAX)
  2051|     PHASES_TP_EXTENSION_MAX = at_("phases_tp_extension_max", _DEFAULT_PHASES_TP_EXTENSION_MAX)
  2052|     PHASES_TW_MIN = at_("phases_tw_min", _DEFAULT_PHASES_TW_MIN)
  2053|     PORTFOLIO_SHAPE_DENSITY_MIN = at_("portfolio_shape_density_min", _DEFAULT_PORTFOLIO_SHAPE_DENSITY_MIN)
  2054|     PORTFOLIO_SHAPE_UNION_JACCARD_MAX = at_("portfolio_shape_union_jaccard_max", _DEFAULT_PORTFOLIO_SHAPE_UNION_JACCARD_MAX)
  2055|     UNION_BREADTH_CORPUS_WIDE_CLIENTS_PCT_MIN = at_(
  2056|         "union_breadth_corpus_wide_clients_pct_min", _DEFAULT_UNION_BREADTH_CORPUS_WIDE_CLIENTS_PCT_MIN
  2057|     )
  2058|     UNION_BREADTH_CLIENT_WIDE_CLIENTS_PCT_MIN = at_(
  2059|         "union_breadth_client_wide_clients_pct_min", _DEFAULT_UNION_BREADTH_CLIENT_WIDE_CLIENTS_PCT_MIN
  2060|     )
  2061|     UNION_BREADTH_PROJECT_WIDE_MIN_PROJECTS = at_(
  2062|         "union_breadth_project_wide_min_projects", _DEFAULT_UNION_BREADTH_PROJECT_WIDE_MIN_PROJECTS
  2063|     )
  2064|     UNION_BREADTH_FILE_LEVEL_MAX_FILES = at_(
  2065|         "union_breadth_file_level_max_files", _DEFAULT_UNION_BREADTH_FILE_LEVEL_MAX_FILES
  2066|     )
  2067|     UNION_BREADTH_BROAD_MIN_PATTERNS = at_(
  2068|         "union_breadth_broad_min_patterns", _DEFAULT_UNION_BREADTH_BROAD_MIN_PATTERNS
  2069|     )
  2070|     UNION_BREADTH_NARROW_FILE_LEVEL_SHARE_MIN = at_(
  2071|         "union_breadth_narrow_file_level_share_min", _DEFAULT_UNION_BREADTH_NARROW_FILE_LEVEL_SHARE_MIN
  2072|     )
  2073|     UNION_BREADTH_WEAK_CASCADE_MAX = at_("union_breadth_weak_cascade_max", _DEFAULT_UNION_BREADTH_WEAK_CASCADE_MAX)
  2074|     UNION_BREADTH_STRONG_CASCADE_MIN = at_("union_breadth_strong_cascade_min", _DEFAULT_UNION_BREADTH_STRONG_CASCADE_MIN)
  2075| 
  2076| 
  2077| def _state_value(state: Optional[dict], key: str) -> Optional[float]:
  2078|     if not state:
  2079|         return None
  2080|     return state.get(key)
  2081| 
  2082| 
  2083| def _has_material_state_exception(state: Optional[dict]) -> bool:
  2084|     """Return True when explicit state signals limit a baseline conclusion."""
  2085|     if not state:
  2086|         return False
  2087|     checks = (
  2088|         ("local_active_share", LOCAL_ACTIVE_MATERIAL_THRESHOLD),
  2089|         ("provided_passive_share", PASSIVE_MATERIAL_THRESHOLD),
  2090|         ("provided_missing_share", MISSING_MATERIAL_THRESHOLD),
  2091|     )
  2092|     for key, threshold in checks:
  2093|         val = state.get(key)
  2094|         if val is not None and val >= threshold:
  2095|             return True
  2096|     return False
  2097| 
  2098| 
  2099| def _has_group1_bc_pooled_evidence(d: dict) -> bool:
  2100|     """True when a same-bc-both-sides ("bc::bc") pooled containment value exists
  2101|     in tp_by_scope or cp_by_scope, even though the enterprise-only tp/cp is None.
  2102| 
  2103|     This is deliberately a presence check only, not a score-magnitude check --
  2104|     assign_tier() must not blend this pooled value into `primary` (see
  2105|     docs/governance_narrative_group1_scope_gap_investigation.md, Q2/Q3): a
  2106|     domain with bc-pooled evidence gets a distinct tier
  2107|     (TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE), not a score-banded promotion
  2108|     into TIER_STRONG_BASELINE/TIER_INVESTIGATE/etc., which would imply
  2109|     enterprise-level evidence that doesn't exist.
  2110|     """
  2111|     return (
  2112|         (d.get("tp_by_scope") or {}).get("bc::bc") is not None
  2113|         or (d.get("cp_by_scope") or {}).get("bc::bc") is not None
  2114|     )
  2115| 
  2116| 
  2117| def assign_tier(d: dict, state: Optional[dict] = None) -> str:
  2118|     """Assign a DoD-safe governance classification.
  2119| 
  2120|     The tier is an evidence/readiness classification, not an approval decision.
  2121|     High containment can create a baseline candidate, but explicit local-active,
  2122|     passive, missing, sparse, or presence-limited signals prevent the renderer
  2123|     from declaring a domain ready as a formal standard.
  2124|     """
  2125|     tc, cp, tp = d["tc"], d["cp"], d["tp"]
  2126|     primary = tp if tp is not None else cp
  2127|     reliability = score_reliability(d)
  2128| 
  2129|     if primary is None:
  2130|         if _has_group1_bc_pooled_evidence(d):
  2131|             return TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE
  2132|         return TIER_INSUFFICIENT
  2133| 
  2134|     local_active = _state_value(state, "local_active_share")
  2135|     passive = _state_value(state, "provided_passive_share")
  2136|     missing = _state_value(state, "provided_missing_share")
  2137|     provided_used = _state_value(state, "provided_to_used_containment")
  2138| 
  2139|     # Sparse or binary-presence domains are not safe to present as converged
  2140|     # standards unless they also have strong explicit active-use evidence.
  2141|     if reliability == RELIABILITY_SPARSE and primary < TIER_SPARSE_PRIMARY_MAX:
  2142|         return TIER_SPARSE_LIMITED
  2143| 
  2144|     if local_active is not None and local_active >= LOCAL_ACTIVE_MATERIAL_THRESHOLD and primary < TIER_ACTIVE_LOCAL_PRIMARY_MAX:
  2145|         return TIER_ACTIVE_LOCAL
  2146| 
  2147|     if primary >= TIER_STRONG_BASELINE_MIN:
  2148|         if _has_material_state_exception(state):
  2149|             return TIER_BASELINE_LOCAL_REVIEW
  2150|         if provided_used is not None and provided_used < ACTIVE_USE_MIN_FOR_STRONG_BASELINE:
  2151|             return TIER_BASELINE_LOCAL_REVIEW
  2152|         if tc is not None and tc < TIER_CONTAINER_GAP_TC_MAX:
  2153|             return TIER_BASELINE_CONTAINER_GAP
  2154|         return TIER_STRONG_BASELINE
  2155| 
  2156|     if primary >= TIER_INVESTIGATE_MIN:
  2157|         if _has_material_state_exception(state):
  2158|             return TIER_BASELINE_LOCAL_REVIEW
  2159|         if reliability in (RELIABILITY_PRESENCE, RELIABILITY_SPARSE):
  2160|             return TIER_INVESTIGATE
  2161|         return TIER_INVESTIGATE
  2162| 
  2163|     if primary >= TIER_MODERATE_VARIATION_MIN:
  2164|         if local_active is not None and local_active >= LOCAL_ACTIVE_MATERIAL_THRESHOLD:
  2165|             return TIER_ACTIVE_LOCAL
  2166|         if reliability == RELIABILITY_SPARSE:
  2167|             return TIER_SPARSE_LIMITED
  2168|         return TIER_MODERATE_VARIATION
  2169| 
  2170|     return TIER_HIGH_FRAGMENTATION
  2171| 
  2172| 
  2173| TIER_ORDER = {
  2174|     TIER_STRONG_BASELINE: 0,
  2175|     TIER_BASELINE_LOCAL_REVIEW: 1,
  2176|     TIER_BASELINE_CONTAINER_GAP: 2,
  2177|     TIER_INVESTIGATE: 3,
  2178|     TIER_ACTIVE_LOCAL: 4,
  2179|     TIER_MODERATE_VARIATION: 5,
  2180|     TIER_SPARSE_LIMITED: 6,
  2181|     TIER_HIGH_FRAGMENTATION: 7,
  2182|     TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE: 8,
  2183|     TIER_INSUFFICIENT: 9,
  2184| }
  2185| 
  2186| 
  2187| def detect_anomalies(dom: str, d: dict, state: Optional[dict] = None,
  2188|                       union_breadth: Optional[dict] = None) -> list[str]:
  2189|     notes = []
  2190|     tc, cp, tp = d["tc"], d["cp"], d["tp"]
  2191|     xc = d["xc"]
  2192|     reliability = score_reliability(d)
  2193| 
  2194|     # Score reliability note — only emit when it limits interpretation.
  2195|     if reliability == RELIABILITY_PRESENCE:
  2196|         mean_pct = pct(d.get("wp_all"))
  2197|         desc = RELIABILITY_DESCRIPTIONS[RELIABILITY_PRESENCE].replace("{mean_pct}", mean_pct)
  2198|         notes.append(desc)
  2199|     elif reliability == RELIABILITY_SPARSE:
  2200|         notes.append(RELIABILITY_DESCRIPTIONS[RELIABILITY_SPARSE])
  2201| 
  2202|     if state:
  2203|         provided_used = state.get("provided_to_used_containment")
  2204|         provided_configured = state.get("provided_to_configured_containment")
  2205|         passive = state.get("provided_passive_share")
  2206|         missing = state.get("provided_missing_share")
  2207|         local_active = state.get("local_active_share")
  2208| 
  2209|         if provided_configured is not None and provided_used is not None:
  2210|             if provided_configured >= PROVIDED_CARRIED_DOWNSTREAM_MIN and provided_used < PROVIDED_ACTIVE_USE_MAX:
  2211|                 notes.append(
  2212|                     f"Provided vocabulary is substantially carried downstream ({pct(provided_configured)}) "
  2213|                     f"but active-use containment is lower ({pct(provided_used)}). Treat this as a "
  2214|                     "baseline candidate needing active-use review, not an approval-ready standard."
  2215|                 )
  2216|         if passive is not None and passive >= PASSIVE_MATERIAL_THRESHOLD:
  2217|             notes.append(
  2218|                 f"Inherited-but-passive signal is material ({pct(passive)}). The domain should be "
  2219|                 "reviewed for starter-content, approved-list, or exception-governance treatment before ratification."
  2220|             )
  2221|         if missing is not None and missing >= MISSING_MATERIAL_THRESHOLD:
  2222|             notes.append(
  2223|                 f"Provided-but-missing signal is material ({pct(missing)}). Confirm whether missing downstream "
  2224|                 "content is intentional pruning, role-specific specialization, or unmanaged propagation failure."
  2225|             )
  2226|         if local_active is not None and local_active >= LOCAL_ACTIVE_MATERIAL_THRESHOLD:
  2227|             notes.append(
  2228|                 f"Active local practice is material ({pct(local_active)}). Review whether local patterns are "
  2229|                 "roll-up candidates, client/discipline playbook content, permitted variants, or project-specific exceptions."
  2230|             )
  2231| 
  2232|     # Bundle / passive inheritance signal fallback when explicit state outputs are absent.
  2233|     bundle_share = d.get("bundle_share_all")
  2234|     bundle_schema = d.get("bundle_schema", "none")
  2235|     passive_ind = d.get("passive_indicator")
  2236| 
  2237|     if not state:
  2238|         if bundle_schema == "dual" and passive_ind is not None:
  2239|             if passive_ind >= PASSIVE_INDICATOR_HIGH_MIN:
  2240|                 notes.append(
  2241|                     f"High passive inheritance signal ({passive_ind*100:.0f}% of bundled shared "
  2242|                     "patterns drop out under the used view) — a significant fraction of the "
  2243|                     "template vocabulary is present in projects but not actively exercised. "
  2244|                     "Ratification should consider an active-use threshold, not just pattern presence."
  2245|                 )
  2246|             elif passive_ind >= PASSIVE_INDICATOR_MODERATE_MIN:
  2247|                 notes.append(
  2248|                     f"Moderate passive inheritance ({passive_ind*100:.0f}% drop from all to used view). "
  2249|                     "Some template patterns are inherited but not in active use."
  2250|                 )
  2251|         elif bundle_schema == "single" and bundle_share is not None:
  2252|             if dom in PASSIVE_INHERITANCE_RISK_DOMAINS and bundle_share < PASSIVE_INHERITANCE_RISK_BUNDLE_SHARE_MAX:
  2253|                 notes.append(
  2254|                     f"Low bundle density among shared patterns ({bundle_share*100:.0f}% bundled). "
  2255|                     "This domain is in the passive inheritance risk group — shared patterns may be "
  2256|                     "inherited rather than actively configured. Used-view analysis recommended "
  2257|                     "before ratification."
  2258|                 )
  2259|             elif bundle_share is not None and bundle_share < BUNDLE_SHARE_VERY_LOW_MAX:
  2260|                 notes.append(
  2261|                     f"Very low bundle density among shared patterns ({bundle_share*100:.0f}% bundled). "
  2262|                     "Shared vocabulary is largely unstructured — consider used-view analysis "
  2263|                     "to confirm patterns are actively exercised."
  2264|                 )
  2265| 
  2266|     # Group 2 (generic->template) signal — surfaces a distinct governance question
  2267|     # from tc/cp/tp alone: the enterprise/generic baseline successfully reached
  2268|     # templates, but templates aren't cascading down to projects, so the break is
  2269|     # specifically between templates and projects rather than with the baseline
  2270|     # content itself.
  2271|     gt = d.get("gt")
  2272|     if gt is not None and gt >= GT_TP_GAP_GT_MIN and tp is not None and tp < GT_TP_GAP_TP_MAX:
  2273|         notes.append(
  2274|             f"Generic/enterprise baseline containment into templates is strong (G→T = {pct(gt)}) "
  2275|             f"but template-to-project propagation is weak (T→P = {pct(tp)}). The enterprise "
  2276|             "baseline is reaching templates; the break is between templates and projects, "
  2277|             "not with the baseline content itself."
  2278|         )
  2279| 
  2280|     # Group 2 scope-breakdown divergence (Option C) — a distinct governance question
  2281|     # from the enterprise-only gt/gc/gp values alone: does the generic/enterprise
  2282|     # baseline propagate as well into SCOPED (client-/bc-/discipline-specific)
  2283|     # templates/containers/projects as it does into the single broadest one? A
  2284|     # material gap in either direction is informative (the baseline holding at the
  2285|     # enterprise level while eroding for specific clients/disciplines, or vice
  2286|     # versa) and would otherwise stay invisible now that gt_by_scope/gc_by_scope/
  2287|     # gp_by_scope capture it instead of discarding it.
  2288|     for cascade_label, enterprise_val, by_scope in (
  2289|         ("Generic→Template", d.get("gt"), d.get("gt_by_scope") or {}),
  2290|         ("Generic→Container", d.get("gc"), d.get("gc_by_scope") or {}),
  2291|         ("Generic→Project", d.get("gp"), d.get("gp_by_scope") or {}),
  2292|     ):
  2293|         scoped_vals = {k: v for k, v in by_scope.items() if k != "enterprise"}
  2294|         if enterprise_val is None or not scoped_vals:
  2295|             continue
  2296|         scoped_mean = statistics.mean(scoped_vals.values())
  2297|         if abs(enterprise_val - scoped_mean) >= GROUP2_SCOPE_DIVERGENCE_GAP_MIN:
  2298|             direction = "weaker" if scoped_mean < enterprise_val else "stronger"
  2299|             detail = ", ".join(f"{k}={pct(v)}" for k, v in sorted(scoped_vals.items()))
  2300|             notes.append(
  2301|                 f"{cascade_label} propagation is {direction} into scoped targets than the "
  2302|                 f"enterprise-wide reading ({pct(enterprise_val)} enterprise vs. {pct(scoped_mean)} "
  2303|                 f"scoped mean — {detail}). Review whether client-/business-center-/discipline-"
  2304|                 "specific practice is diverging from or exceeding the enterprise baseline."
  2305|             )
  2306| 
  2307|     # Group 1 by-scope intra-bucket divergence — a distinct governance question
  2308|     # from Group 2's enterprise-vs-scoped check above: Group 1 (tc/cp/tp) usually
  2309|     # has NO enterprise-level reading to compare against at all (that's the gap
  2310|     # tc_by_scope/cp_by_scope/tp_by_scope exists to fill — see
  2311|     # docs/governance_narrative_group1_scope_gap_investigation.md), so the risk
  2312|     # here isn't "enterprise differs from scoped" but "the pooled MEAN itself
  2313|     # hides sharp disagreement between the individual rows pooled into it."
  2314|     # Deliberately scope-neutral wording: a scope_pair like "bc::bc" pools
  2315|     # multiple DISTINCT business centers when more than one exists, but a
  2316|     # scope_pair like "client_bc::client_discipline" pools rows that share the
  2317|     # same client/bc and vary only by discipline (confirmed against real
  2318|     # cross_segment_summary.csv data -- see docs/governance_narrative_group1_scope_gap_investigation.md
  2319|     # follow-up) -- the note must not claim "business-center" divergence when
  2320|     # the actual varying dimension for that particular scope_pair could be
  2321|     # client or discipline instead. Uses its own group1_scope_spread_gap_min
  2322|     # materiality threshold (a separately-editable key from the Group 2 check
  2323|     # above's group2_scope_divergence_gap_min, even though the two default
  2324|     # values currently coincide), applied to each scope_pair's own (min, max)
  2325|     # spread instead of an enterprise-vs-mean comparison.
  2326|     for cascade_label, by_scope_spread, by_scope_mean in (
  2327|         ("Template→Container", d.get("tc_by_scope_spread") or {}, d.get("tc_by_scope") or {}),
  2328|         ("Container→Project", d.get("cp_by_scope_spread") or {}, d.get("cp_by_scope") or {}),
  2329|         ("Template→Project", d.get("tp_by_scope_spread") or {}, d.get("tp_by_scope") or {}),
  2330|     ):
  2331|         for scope_pair, (lo, hi) in sorted(by_scope_spread.items()):
  2332|             if scope_pair == "enterprise::enterprise":
  2333|                 continue
  2334|             if hi - lo >= GROUP1_SCOPE_SPREAD_GAP_MIN:
  2335|                 notes.append(
  2336|                     f"{cascade_label} pooled evidence for scope '{scope_pair}' spans "
  2337|                     f"{pct(lo)}–{pct(hi)} across the individual rows pooled into this "
  2338|                     f"bucket (pooled mean {pct(by_scope_mean.get(scope_pair))}). This "
  2339|                     "scope level is not a single converged reading — review the "
  2340|                     "underlying per-row variation before treating the pooled mean as "
  2341|                     "one number."
  2342|                 )
  2343| 
  2344|     if tc is not None and tp is not None and tp > tc + TP_TC_BYPASS_GAP_MIN:
  2345|         notes.append(
  2346|             "Template patterns arrive in projects via direct Revit inheritance, "
  2347|             "bypassing coordination files — coordination files are not the governance "
  2348|             "vehicle for this domain."
  2349|         )
  2350|     if tc is not None and tc < WEAK_TC_MAX:
  2351|         notes.append(
  2352|             f"Templates propagate weakly into coordination files "
  2353|             f"(T→C = {pct(tc)}). Coordination files govern this domain independently."
  2354|         )
  2355|     if cp is not None and cp < WEAK_CP_MAX:
  2356|         notes.append(
  2357|             f"Coordination-file-to-project cascade is weak (C→P = {pct(cp)}). "
  2358|             "Project teams are diverging from coordination file vocabulary."
  2359|         )
  2360|     if xc is not None and xc >= XC_STRONG_CONVERGENCE:
  2361|         notes.append(
  2362|             f"Strong cross-client convergence ({pct(xc)}) — natural baseline candidate "
  2363|             "for governance review regardless of formal template propagation."
  2364|         )
  2365|     if xc is not None and xc < XC_LOW_THRESHOLD and tp is not None and tp > XC_LOW_TP_MIN:
  2366|         notes.append(
  2367|             "Template floor propagates well but cross-client convergence is low — "
  2368|             "clients are inheriting the template floor while adding client-specific vocabulary."
  2369|         )
  2370|     if "view_template" in dom:
  2371|         disc_wp = d["wp_disc"]
  2372|         zero_discs = [_disc_label(k) for k, v in disc_wp.items() if v < VIEW_TEMPLATE_ZERO_DISCIPLINE_MAX and k != "all"]
  2373|         if zero_discs:
  2374|             notes.append(
  2375|                 f"Architecturally specific — near-zero within-project coherence for: "
  2376|                 f"{', '.join(zero_discs)}. These disciplines require separate view template governance."
  2377|             )
  2378|     if dom == "phases" and "phases" in DOMAIN_GUIDANCE:
  2379|         if tp is not None and tp < PHASES_TP_EXTENSION_MAX and d["tw"] is not None and d["tw"] > PHASES_TW_MIN:
  2380|             notes.append(DOMAIN_GUIDANCE["phases"])
  2381|     if dom == "loaded_family_types" and "loaded_family_types" in DOMAIN_GUIDANCE:
  2382|         notes.append(DOMAIN_GUIDANCE["loaded_family_types"])
  2383| 
  2384|     # Union-inventory-derived domain confidence enrichment (D-033). Only the
  2385|     # strongest exceptions render -- per docs/governance_generator_cross_compare_coverage.md's
  2386|     # own guardrail, this must not become a per-domain dump of raw breadth
  2387|     # numbers. The two conditions are checked as an if/elif (mutually
  2388|     # exclusive): a domain in the gap between UNION_BREADTH_WEAK_CASCADE_MAX
  2389|     # and UNION_BREADTH_STRONG_CASCADE_MIN, or with unremarkable breadth,
  2390|     # triggers neither.
  2391|     if union_breadth:
  2392|         primary = tp if tp is not None else cp
  2393|         total = union_breadth.get("total", 0)
  2394|         broad = union_breadth.get("corpus_wide", 0) + union_breadth.get("client_wide", 0)
  2395|         file_level = union_breadth.get("file_level", 0)
  2396|         if primary is not None and broad >= UNION_BREADTH_BROAD_MIN_PATTERNS and primary < UNION_BREADTH_WEAK_CASCADE_MAX:
  2397|             # PR review finding: `broad`/`total` are summed across every
  2398|             # discipline/unit_system scope for this domain -- a single
  2399|             # narrow scope (e.g. one small discipline where all its clients
  2400|             # happen to carry a pattern) can satisfy this threshold with no
  2401|             # domain-wide breadth evidence at all. Name the qualifying
  2402|             # scope(s) explicitly instead of letting the note read as a
  2403|             # domain-wide claim.
  2404|             broad_scopes = union_breadth.get("broad_scopes") or []
  2405|             scope_labels = [
  2406|                 f"{disc or '(no discipline)'}/{unit or '(no unit system)'}"
  2407|                 for disc, unit in broad_scopes
  2408|             ]
  2409|             scope_note = (
  2410|                 f" (scope(s): {', '.join(scope_labels)})" if scope_labels else ""
  2411|             )
  2412|             notes.append(
  2413|                 f"Broad natural reuse ({broad} corpus-wide/client-wide pattern(s) of {total} "
  2414|                 f"in cross_segment_union_inventory.csv) despite weak formal cascade (primary "
  2415|                 f"containment = {pct(primary)}){scope_note}. This may indicate a natural-"
  2416|                 "standard candidate within the named scope(s) that the cascade metrics alone "
  2417|                 "would miss -- not necessarily domain-wide reuse; counts are summed across "
  2418|                 "discipline/unit_system scopes with independent denominators."
  2419|             )
  2420|         elif (
  2421|             primary is not None and total > 0
  2422|             and file_level / total >= UNION_BREADTH_NARROW_FILE_LEVEL_SHARE_MIN
  2423|             and primary >= UNION_BREADTH_STRONG_CASCADE_MIN
  2424|         ):
  2425|             notes.append(
  2426|                 f"Narrow natural reuse ({file_level} of {total} patterns are file-level/singleton "
  2427|                 f"in cross_segment_union_inventory.csv) despite strong formal cascade (primary "
  2428|                 f"containment = {pct(primary)}). Formal propagation may be fragile — review "
  2429|                 "whether reuse is broader than the union inventory currently shows."
  2430|             )
  2431|     return notes
  2432| 
```
