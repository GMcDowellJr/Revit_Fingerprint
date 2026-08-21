# Chunk of tools/generate_governance_narrative.py

- Source relative path: `tools/generate_governance_narrative.py`
- Chunk: 5 of 17
- Original line range: 1518-1928
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: score_reliability
- Source SHA-256: 7a8e1def8713100e21b852dc24c2c714a8e5330815affb3e703ed572a2829d9c
- Starts inside symbol: no
- Ends inside symbol: no

```
  1518| 
  1519| 
  1520| # Score reliability classifications
  1521| # Based on within-project p10/p90 spread and mean
  1522| RELIABILITY_TIGHT        = "Tight"          # p10 >= 0.85 — every pair agrees; mean is a firm number
  1523| RELIABILITY_CONVERGENT   = "Convergent"     # p10 >= 0.50, spread < 0.40 — strong core, modest tail
  1524| RELIABILITY_PRESENCE     = "Presence-based" # p10 near 0, p90 near 1, mean 0.4–0.8 — domain is
  1525|                                              # optional; files either fully carry it or don't
  1526| RELIABILITY_SPARSE       = "Sparse"         # p10 near 0, mean < 0.40 — minority of files carry
  1527|                                              # the domain at all; mean understates fragmentation
  1528| RELIABILITY_UNKNOWN      = "Unknown"        # no p10/p90 data
  1529| 
  1530| # Reliability-band thresholds. Defaults reproduce the comments above exactly;
  1531| # apply_governance_policy() overrides these module globals from
  1532| # governance_thresholds.json at runtime (see main()).
  1533| _DEFAULT_RELIABILITY_TIGHT_P10 = 0.85
  1534| _DEFAULT_RELIABILITY_CONVERGENT_P10 = 0.50
  1535| _DEFAULT_RELIABILITY_CONVERGENT_SPREAD_MAX = 0.40
  1536| _DEFAULT_RELIABILITY_LOW_P10_MAX = 0.20
  1537| _DEFAULT_RELIABILITY_PRESENCE_P90_MIN = 0.85
  1538| _DEFAULT_RELIABILITY_SPARSE_MEAN_MAX = 0.40
  1539| RELIABILITY_TIGHT_P10 = _DEFAULT_RELIABILITY_TIGHT_P10
  1540| RELIABILITY_CONVERGENT_P10 = _DEFAULT_RELIABILITY_CONVERGENT_P10
  1541| RELIABILITY_CONVERGENT_SPREAD_MAX = _DEFAULT_RELIABILITY_CONVERGENT_SPREAD_MAX
  1542| RELIABILITY_LOW_P10_MAX = _DEFAULT_RELIABILITY_LOW_P10_MAX
  1543| RELIABILITY_PRESENCE_P90_MIN = _DEFAULT_RELIABILITY_PRESENCE_P90_MIN
  1544| RELIABILITY_SPARSE_MEAN_MAX = _DEFAULT_RELIABILITY_SPARSE_MEAN_MAX
  1545| 
  1546| 
  1547| def score_reliability(d: dict) -> str:
  1548|     """
  1549|     Classify mean score reliability from within-project p10/p90 spread.
  1550| 
  1551|     Tight:          p10 >= reliability_tight_p10  — floor is high; mean trustworthy
  1552|     Convergent:     p10 >= reliability_convergent_p10, spread < reliability_convergent_spread_max
  1553|                     — solid core, some tail variation
  1554|     Presence-based: p10 < reliability_low_p10_max AND p90 >= reliability_presence_p90_min
  1555|                     — binary optional domain; mean reflects how many files carry it,
  1556|                     not how well they agree
  1557|     Sparse:         p10 < reliability_low_p10_max AND mean < reliability_sparse_mean_max
  1558|                     — domain rarely present at all
  1559| 
  1560|     Threshold values come from policies/governance/governance_thresholds.json
  1561|     (see apply_governance_policy()); the names above are that profile's keys.
  1562|     """
  1563|     p10 = d.get("wp_p10")
  1564|     p90 = d.get("wp_p90")
  1565|     mean = d.get("wp_all")
  1566| 
  1567|     if p10 is None or p90 is None:
  1568|         return RELIABILITY_UNKNOWN
  1569| 
  1570|     spread = p90 - p10
  1571| 
  1572|     if p10 >= RELIABILITY_TIGHT_P10:
  1573|         return RELIABILITY_TIGHT
  1574|     if p10 >= RELIABILITY_CONVERGENT_P10 and spread < RELIABILITY_CONVERGENT_SPREAD_MAX:
  1575|         return RELIABILITY_CONVERGENT
  1576|     if p10 < RELIABILITY_LOW_P10_MAX and p90 >= RELIABILITY_PRESENCE_P90_MIN:
  1577|         return RELIABILITY_PRESENCE
  1578|     if p10 < RELIABILITY_LOW_P10_MAX and (mean is None or mean < RELIABILITY_SPARSE_MEAN_MAX):
  1579|         return RELIABILITY_SPARSE
  1580|     # Moderate spread with moderate floor — convergent with meaningful tail
  1581|     return RELIABILITY_CONVERGENT
  1582| 
  1583| 
  1584| RELIABILITY_DESCRIPTIONS = {
  1585|     RELIABILITY_TIGHT: (
  1586|         "Score is highly reliable — nearly all file pairs agree. "
  1587|         "The reported mean reflects a genuine uniform standard."
  1588|     ),
  1589|     RELIABILITY_CONVERGENT: (
  1590|         "Score is reliable — a strong core of files agree, "
  1591|         "with some variation in the tail. Mean is a good governance signal."
  1592|     ),
  1593|     RELIABILITY_PRESENCE: (
  1594|         "Score reliability is limited — this domain follows a binary presence pattern. "
  1595|         "Files either fully carry the configuration or have none of it. "
  1596|         "The mean reflects adoption rate across files, not agreement between files that have it. "
  1597|         "Interpret as: roughly {mean_pct} of file pairs both carry this domain."
  1598|     ),
  1599|     RELIABILITY_SPARSE: (
  1600|         "Score reliability is low — this domain is present in only a minority of files. "
  1601|         "The mean understates fragmentation. "
  1602|         "Governance should focus on whether the domain should be mandatory before assessing convergence."
  1603|     ),
  1604|     RELIABILITY_UNKNOWN: (
  1605|         "Spread data not available for this domain."
  1606|     ),
  1607| }
  1608| 
  1609| 
  1610| 
  1611| TIER_STRONG_BASELINE = "Strong Baseline Candidate"
  1612| TIER_BASELINE_LOCAL_REVIEW = "Baseline Candidate — Local/Use Review"
  1613| TIER_BASELINE_CONTAINER_GAP = "Baseline Candidate — Container Gap"
  1614| TIER_INVESTIGATE = "Investigate Before Baseline"
  1615| TIER_ACTIVE_LOCAL = "Active Local Practice Review"
  1616| TIER_MODERATE_VARIATION = "Moderate Variation"
  1617| TIER_HIGH_FRAGMENTATION = "High Fragmentation"
  1618| TIER_SPARSE_LIMITED = "Sparse / Presence-Limited"
  1619| TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE = "Insufficient Evidence — Enterprise; BC-Level Evidence Available"
  1620| TIER_INSUFFICIENT = "Insufficient Evidence"
  1621| 
  1622| # Deterministic materiality thresholds used to keep baseline language conservative.
  1623| # These are narrative/classification thresholds, not governance policy
  1624| # approvals. They decide when the renderer must add review language rather
  1625| # than presenting a cleaner baseline read. Defaults reproduce this file's
  1626| # pre-externalization literals; apply_governance_policy() overrides these
  1627| # module globals from governance_thresholds.json at runtime (see main()).
  1628| _DEFAULT_LOCAL_ACTIVE_MATERIAL_THRESHOLD = 0.15
  1629| _DEFAULT_PASSIVE_MATERIAL_THRESHOLD = 0.20
  1630| _DEFAULT_MISSING_MATERIAL_THRESHOLD = 0.20
  1631| _DEFAULT_ACTIVE_USE_MIN_FOR_STRONG_BASELINE = 0.75
  1632| _DEFAULT_TIER_SPARSE_PRIMARY_MAX = 0.75
  1633| _DEFAULT_TIER_ACTIVE_LOCAL_PRIMARY_MAX = 0.90
  1634| _DEFAULT_TIER_STRONG_BASELINE_MIN = 0.90
  1635| _DEFAULT_TIER_CONTAINER_GAP_TC_MAX = 0.60
  1636| _DEFAULT_TIER_INVESTIGATE_MIN = 0.75
  1637| _DEFAULT_TIER_MODERATE_VARIATION_MIN = 0.55
  1638| 
  1639| LOCAL_ACTIVE_MATERIAL_THRESHOLD = _DEFAULT_LOCAL_ACTIVE_MATERIAL_THRESHOLD
  1640| PASSIVE_MATERIAL_THRESHOLD = _DEFAULT_PASSIVE_MATERIAL_THRESHOLD
  1641| MISSING_MATERIAL_THRESHOLD = _DEFAULT_MISSING_MATERIAL_THRESHOLD
  1642| ACTIVE_USE_MIN_FOR_STRONG_BASELINE = _DEFAULT_ACTIVE_USE_MIN_FOR_STRONG_BASELINE
  1643| TIER_SPARSE_PRIMARY_MAX = _DEFAULT_TIER_SPARSE_PRIMARY_MAX
  1644| TIER_ACTIVE_LOCAL_PRIMARY_MAX = _DEFAULT_TIER_ACTIVE_LOCAL_PRIMARY_MAX
  1645| TIER_STRONG_BASELINE_MIN = _DEFAULT_TIER_STRONG_BASELINE_MIN
  1646| TIER_CONTAINER_GAP_TC_MAX = _DEFAULT_TIER_CONTAINER_GAP_TC_MAX
  1647| TIER_INVESTIGATE_MIN = _DEFAULT_TIER_INVESTIGATE_MIN
  1648| TIER_MODERATE_VARIATION_MIN = _DEFAULT_TIER_MODERATE_VARIATION_MIN
  1649| 
  1650| # Cross-client convergence and client-tier/confidence thresholds, used by
  1651| # detect_anomalies(), build_client_summary(), _low_coherence_clients(), and
  1652| # _classify_domains_for_findings(). Same default/override pattern as above;
  1653| # sourced from governance_thresholds.json.
  1654| _DEFAULT_XC_STRONG_CONVERGENCE = 0.70
  1655| _DEFAULT_XC_LOW_THRESHOLD = 0.15
  1656| _DEFAULT_XC_LOW_TP_MIN = 0.70
  1657| _DEFAULT_CLIENT_ALIGNMENT_HIGH = 0.45
  1658| _DEFAULT_CLIENT_ALIGNMENT_MODERATE = 0.33
  1659| _DEFAULT_CLIENT_CONFIDENCE_LOW_MAX_FILES = 10
  1660| _DEFAULT_CLIENT_CONFIDENCE_MODERATE_MAX_FILES = 25
  1661| _DEFAULT_CLIENT_COHERENCE_LOW = 0.45
  1662| 
  1663| XC_STRONG_CONVERGENCE = _DEFAULT_XC_STRONG_CONVERGENCE
  1664| XC_LOW_THRESHOLD = _DEFAULT_XC_LOW_THRESHOLD
  1665| XC_LOW_TP_MIN = _DEFAULT_XC_LOW_TP_MIN
  1666| CLIENT_ALIGNMENT_HIGH = _DEFAULT_CLIENT_ALIGNMENT_HIGH
  1667| CLIENT_ALIGNMENT_MODERATE = _DEFAULT_CLIENT_ALIGNMENT_MODERATE
  1668| CLIENT_CONFIDENCE_LOW_MAX_FILES = _DEFAULT_CLIENT_CONFIDENCE_LOW_MAX_FILES
  1669| CLIENT_CONFIDENCE_MODERATE_MAX_FILES = _DEFAULT_CLIENT_CONFIDENCE_MODERATE_MAX_FILES
  1670| CLIENT_COHERENCE_LOW = _DEFAULT_CLIENT_COHERENCE_LOW
  1671| 
  1672| # BC-tier/confidence thresholds, used by build_bc_summary(). Kept as a separate
  1673| # set of constants from CLIENT_ALIGNMENT_HIGH/_MODERATE and CLIENT_CONFIDENCE_*
  1674| # even though the default values below numerically coincide with them -- same
  1675| # "separate profile even where a default value numerically coincides" posture
  1676| # already established for ONBOARD_XC_HIGH_PORTABILITY_MIN/etc. above, since a
  1677| # --policy-dir override tuning client tiers must not silently also retune BC
  1678| # tiers (or vice versa). These are hand-picked defaults, not data-derived: per
  1679| # Step 0 of this PR, CLIENT_ALIGNMENT_HIGH/_MODERATE were confirmed to be
  1680| # hardcoded literals (externalized to governance_thresholds.json, but not
  1681| # computed via tools/jenks_utils.py/compute_governance_thresholds.py -- that
  1682| # tool computes an unrelated split-detection alignment-rate threshold and is
  1683| # not wired into this file at all), so reusing the same hand-picked-default
  1684| # convention here is consistent with, not a departure from, how this file
  1685| # already sets classification thresholds.
  1686| _DEFAULT_BC_ALIGNMENT_HIGH = 0.45
  1687| _DEFAULT_BC_ALIGNMENT_MODERATE = 0.33
  1688| _DEFAULT_BC_CONFIDENCE_LOW_MAX_FILES = 10
  1689| _DEFAULT_BC_CONFIDENCE_MODERATE_MAX_FILES = 25
  1690| 
  1691| BC_ALIGNMENT_HIGH = _DEFAULT_BC_ALIGNMENT_HIGH
  1692| BC_ALIGNMENT_MODERATE = _DEFAULT_BC_ALIGNMENT_MODERATE
  1693| BC_CONFIDENCE_LOW_MAX_FILES = _DEFAULT_BC_CONFIDENCE_LOW_MAX_FILES
  1694| BC_CONFIDENCE_MODERATE_MAX_FILES = _DEFAULT_BC_CONFIDENCE_MODERATE_MAX_FILES
  1695| 
  1696| # Client-onboarding-interpretation thresholds (_client_onboarding_profile()).
  1697| # Kept as a separate profile (client_onboarding_policy.json) from the
  1698| # governance-tier thresholds above even where a default value numerically
  1699| # coincides, since these gate onboarding narrative text, not governance_tier.
  1700| _DEFAULT_ONBOARD_WP_STABLE_MIN = 0.75
  1701| _DEFAULT_ONBOARD_WP_MIXED_MIN = 0.55
  1702| _DEFAULT_ONBOARD_XC_HIGH_PORTABILITY_MIN = 0.45
  1703| _DEFAULT_ONBOARD_XC_MODERATE_PORTABILITY_MIN = 0.33
  1704| _DEFAULT_ONBOARD_N_FILES_LOW_MAX = 10
  1705| _DEFAULT_ONBOARD_N_FILES_MODERATE_MAX = 25
  1706| 
  1707| ONBOARD_WP_STABLE_MIN = _DEFAULT_ONBOARD_WP_STABLE_MIN
  1708| ONBOARD_WP_MIXED_MIN = _DEFAULT_ONBOARD_WP_MIXED_MIN
  1709| ONBOARD_XC_HIGH_PORTABILITY_MIN = _DEFAULT_ONBOARD_XC_HIGH_PORTABILITY_MIN
  1710| ONBOARD_XC_MODERATE_PORTABILITY_MIN = _DEFAULT_ONBOARD_XC_MODERATE_PORTABILITY_MIN
  1711| ONBOARD_N_FILES_LOW_MAX = _DEFAULT_ONBOARD_N_FILES_LOW_MAX
  1712| ONBOARD_N_FILES_MODERATE_MAX = _DEFAULT_ONBOARD_N_FILES_MODERATE_MAX
  1713| 
  1714| 
  1715| # detect_anomalies()/render_findings_and_recommendations()'s phases check/
  1716| # _passive_inheritance_risk_domains()/_shape_note() materiality thresholds
  1717| # (D-029). Defaults reproduce this file's pre-externalization Python literals
  1718| # exactly; apply_governance_policy() overrides these module globals from
  1719| # policies/governance/anomaly_thresholds.json at runtime (see main()). Kept
  1720| # as a separate profile from governance_thresholds.json even though this
  1721| # profile's passive_inheritance_risk_bundle_share_max numerically coincides
  1722| # with governance_thresholds.json's passive_material_threshold today -- the
  1723| # two gate different code paths (bundle-density fallback vs. explicit
  1724| # governance-state share) and must be independently editable. The
  1725| # _passive_inheritance_risk_domains() dual-schema branch's own passive-share
  1726| # check does NOT get a new key here -- it already reads (and continues to
  1727| # read) PASSIVE_MATERIAL_THRESHOLD directly, closing a pre-existing drift gap
  1728| # rather than relocating it into a second, parallel constant.
  1729| _DEFAULT_PROVIDED_CARRIED_DOWNSTREAM_MIN = 0.75
  1730| _DEFAULT_PROVIDED_ACTIVE_USE_MAX = 0.75
  1731| _DEFAULT_PRIMARY_READ_ACTIVE_USE_MIN = 0.85
  1732| _DEFAULT_PASSIVE_INDICATOR_HIGH_MIN = 0.40
  1733| _DEFAULT_PASSIVE_INDICATOR_MODERATE_MIN = 0.20
  1734| _DEFAULT_PASSIVE_INHERITANCE_RISK_BUNDLE_SHARE_MAX = 0.25
  1735| _DEFAULT_BUNDLE_SHARE_VERY_LOW_MAX = 0.15
  1736| _DEFAULT_GT_TP_GAP_GT_MIN = 0.75
  1737| _DEFAULT_GT_TP_GAP_TP_MAX = 0.55
  1738| _DEFAULT_GROUP2_SCOPE_DIVERGENCE_GAP_MIN = 0.25
  1739| _DEFAULT_GROUP1_SCOPE_SPREAD_GAP_MIN = 0.25
  1740| _DEFAULT_TP_TC_BYPASS_GAP_MIN = 0.25
  1741| _DEFAULT_WEAK_TC_MAX = 0.20
  1742| _DEFAULT_WEAK_CP_MAX = 0.50
  1743| _DEFAULT_VIEW_TEMPLATE_ZERO_DISCIPLINE_MAX = 0.05
  1744| _DEFAULT_PHASES_TP_EXTENSION_MAX = 0.85
  1745| _DEFAULT_PHASES_TW_MIN = 0.80
  1746| _DEFAULT_PORTFOLIO_SHAPE_DENSITY_MIN = 0.8
  1747| _DEFAULT_PORTFOLIO_SHAPE_UNION_JACCARD_MAX = 0.3
  1748| 
  1749| PROVIDED_CARRIED_DOWNSTREAM_MIN = _DEFAULT_PROVIDED_CARRIED_DOWNSTREAM_MIN
  1750| PROVIDED_ACTIVE_USE_MAX = _DEFAULT_PROVIDED_ACTIVE_USE_MAX
  1751| PRIMARY_READ_ACTIVE_USE_MIN = _DEFAULT_PRIMARY_READ_ACTIVE_USE_MIN
  1752| PASSIVE_INDICATOR_HIGH_MIN = _DEFAULT_PASSIVE_INDICATOR_HIGH_MIN
  1753| PASSIVE_INDICATOR_MODERATE_MIN = _DEFAULT_PASSIVE_INDICATOR_MODERATE_MIN
  1754| PASSIVE_INHERITANCE_RISK_BUNDLE_SHARE_MAX = _DEFAULT_PASSIVE_INHERITANCE_RISK_BUNDLE_SHARE_MAX
  1755| BUNDLE_SHARE_VERY_LOW_MAX = _DEFAULT_BUNDLE_SHARE_VERY_LOW_MAX
  1756| GT_TP_GAP_GT_MIN = _DEFAULT_GT_TP_GAP_GT_MIN
  1757| GT_TP_GAP_TP_MAX = _DEFAULT_GT_TP_GAP_TP_MAX
  1758| GROUP2_SCOPE_DIVERGENCE_GAP_MIN = _DEFAULT_GROUP2_SCOPE_DIVERGENCE_GAP_MIN
  1759| GROUP1_SCOPE_SPREAD_GAP_MIN = _DEFAULT_GROUP1_SCOPE_SPREAD_GAP_MIN
  1760| TP_TC_BYPASS_GAP_MIN = _DEFAULT_TP_TC_BYPASS_GAP_MIN
  1761| WEAK_TC_MAX = _DEFAULT_WEAK_TC_MAX
  1762| WEAK_CP_MAX = _DEFAULT_WEAK_CP_MAX
  1763| VIEW_TEMPLATE_ZERO_DISCIPLINE_MAX = _DEFAULT_VIEW_TEMPLATE_ZERO_DISCIPLINE_MAX
  1764| PHASES_TP_EXTENSION_MAX = _DEFAULT_PHASES_TP_EXTENSION_MAX
  1765| PHASES_TW_MIN = _DEFAULT_PHASES_TW_MIN
  1766| PORTFOLIO_SHAPE_DENSITY_MIN = _DEFAULT_PORTFOLIO_SHAPE_DENSITY_MIN
  1767| PORTFOLIO_SHAPE_UNION_JACCARD_MAX = _DEFAULT_PORTFOLIO_SHAPE_UNION_JACCARD_MAX
  1768| 
  1769| 
  1770| # Union-inventory-derived domain confidence enrichment thresholds (D-033).
  1771| # build_union_breadth_by_domain() classifies each cross_segment_union_inventory.csv
  1772| # pattern (join_hash) into exactly one breadth tier -- corpus_wide > client_wide
  1773| # > project_wide > file_level > unclassified, highest-qualifying tier wins --
  1774| # using the four _*_MIN/_MAX keys below; the remaining four gate the new
  1775| # detect_anomalies() exception category (broad reuse despite weak cascade, or
  1776| # narrow reuse despite strong cascade). No prior Python literal exists for any
  1777| # of these (new functionality, not an externalization of pre-existing
  1778| # behavior) -- values are this phase's own initial defaults, editable via
  1779| # policies/governance/anomaly_thresholds.json like every other key in that
  1780| # profile. Kept independent of governance_thresholds.json's tier-assignment
  1781| # thresholds (TIER_STRONG_BASELINE_MIN etc.) even where a value could
  1782| # numerically coincide, since this check gates a narrower narrative exception,
  1783| # not governance_tier itself.
  1784| _DEFAULT_UNION_BREADTH_CORPUS_WIDE_CLIENTS_PCT_MIN = 0.90
  1785| _DEFAULT_UNION_BREADTH_CLIENT_WIDE_CLIENTS_PCT_MIN = 0.50
  1786| _DEFAULT_UNION_BREADTH_PROJECT_WIDE_MIN_PROJECTS = 2
  1787| _DEFAULT_UNION_BREADTH_FILE_LEVEL_MAX_FILES = 1
  1788| _DEFAULT_UNION_BREADTH_BROAD_MIN_PATTERNS = 1
  1789| _DEFAULT_UNION_BREADTH_NARROW_FILE_LEVEL_SHARE_MIN = 0.5
  1790| _DEFAULT_UNION_BREADTH_WEAK_CASCADE_MAX = 0.40
  1791| _DEFAULT_UNION_BREADTH_STRONG_CASCADE_MIN = 0.75
  1792| 
  1793| UNION_BREADTH_CORPUS_WIDE_CLIENTS_PCT_MIN = _DEFAULT_UNION_BREADTH_CORPUS_WIDE_CLIENTS_PCT_MIN
  1794| UNION_BREADTH_CLIENT_WIDE_CLIENTS_PCT_MIN = _DEFAULT_UNION_BREADTH_CLIENT_WIDE_CLIENTS_PCT_MIN
  1795| UNION_BREADTH_PROJECT_WIDE_MIN_PROJECTS = _DEFAULT_UNION_BREADTH_PROJECT_WIDE_MIN_PROJECTS
  1796| UNION_BREADTH_FILE_LEVEL_MAX_FILES = _DEFAULT_UNION_BREADTH_FILE_LEVEL_MAX_FILES
  1797| UNION_BREADTH_BROAD_MIN_PATTERNS = _DEFAULT_UNION_BREADTH_BROAD_MIN_PATTERNS
  1798| UNION_BREADTH_NARROW_FILE_LEVEL_SHARE_MIN = _DEFAULT_UNION_BREADTH_NARROW_FILE_LEVEL_SHARE_MIN
  1799| UNION_BREADTH_WEAK_CASCADE_MAX = _DEFAULT_UNION_BREADTH_WEAK_CASCADE_MAX
  1800| UNION_BREADTH_STRONG_CASCADE_MIN = _DEFAULT_UNION_BREADTH_STRONG_CASCADE_MIN
  1801| 
  1802| 
  1803| # ── policy externalization: default profiles + runtime application ─────────
  1804| #
  1805| # _POLICY_DEFAULTS mirrors policies/governance/*.json exactly, built from the
  1806| # same _DEFAULT_* constants the module-level names above were initialized
  1807| # from -- so there is exactly one Python-side source of truth for each
  1808| # default value, not two that could drift apart. load_governance_policy()
  1809| # (tools/governance_policy.py) uses these as the per-file fallback when a
  1810| # profile file is absent from --policy-dir; apply_governance_policy() below
  1811| # then reassigns the module globals every function in this file already
  1812| # reads (EXCLUDED_FROM_SCORING, PASSIVE_INHERITANCE_RISK_DOMAINS,
  1813| # DOMAIN_GUIDANCE, STATIC_FINDINGS_GUIDANCE, and every threshold constant
  1814| # above) from whatever load_governance_policy() actually resolved.
  1815| _POLICY_DEFAULTS = {
  1816|     "thresholds": {
  1817|         "profile_id": "governance-thresholds-v1",
  1818|         "schema_version": "0.1",
  1819|         "thresholds": {
  1820|             "reliability_tight_p10": _DEFAULT_RELIABILITY_TIGHT_P10,
  1821|             "reliability_convergent_p10": _DEFAULT_RELIABILITY_CONVERGENT_P10,
  1822|             "reliability_convergent_spread_max": _DEFAULT_RELIABILITY_CONVERGENT_SPREAD_MAX,
  1823|             "reliability_low_p10_max": _DEFAULT_RELIABILITY_LOW_P10_MAX,
  1824|             "reliability_presence_p90_min": _DEFAULT_RELIABILITY_PRESENCE_P90_MIN,
  1825|             "reliability_sparse_mean_max": _DEFAULT_RELIABILITY_SPARSE_MEAN_MAX,
  1826|             "local_active_material_threshold": _DEFAULT_LOCAL_ACTIVE_MATERIAL_THRESHOLD,
  1827|             "passive_material_threshold": _DEFAULT_PASSIVE_MATERIAL_THRESHOLD,
  1828|             "missing_material_threshold": _DEFAULT_MISSING_MATERIAL_THRESHOLD,
  1829|             "active_use_min_for_strong_baseline": _DEFAULT_ACTIVE_USE_MIN_FOR_STRONG_BASELINE,
  1830|             "tier_sparse_primary_max": _DEFAULT_TIER_SPARSE_PRIMARY_MAX,
  1831|             "tier_active_local_primary_max": _DEFAULT_TIER_ACTIVE_LOCAL_PRIMARY_MAX,
  1832|             "tier_strong_baseline_min": _DEFAULT_TIER_STRONG_BASELINE_MIN,
  1833|             "tier_container_gap_tc_max": _DEFAULT_TIER_CONTAINER_GAP_TC_MAX,
  1834|             "tier_investigate_min": _DEFAULT_TIER_INVESTIGATE_MIN,
  1835|             "tier_moderate_variation_min": _DEFAULT_TIER_MODERATE_VARIATION_MIN,
  1836|             "cross_client_convergence_strong": _DEFAULT_XC_STRONG_CONVERGENCE,
  1837|             "cross_client_convergence_low": _DEFAULT_XC_LOW_THRESHOLD,
  1838|             "cross_client_low_tp_min": _DEFAULT_XC_LOW_TP_MIN,
  1839|             "client_alignment_high": _DEFAULT_CLIENT_ALIGNMENT_HIGH,
  1840|             "client_alignment_moderate": _DEFAULT_CLIENT_ALIGNMENT_MODERATE,
  1841|             "client_confidence_low_max_files": _DEFAULT_CLIENT_CONFIDENCE_LOW_MAX_FILES,
  1842|             "client_confidence_moderate_max_files": _DEFAULT_CLIENT_CONFIDENCE_MODERATE_MAX_FILES,
  1843|             "client_coherence_low": _DEFAULT_CLIENT_COHERENCE_LOW,
  1844|             "bc_alignment_high": _DEFAULT_BC_ALIGNMENT_HIGH,
  1845|             "bc_alignment_moderate": _DEFAULT_BC_ALIGNMENT_MODERATE,
  1846|             "bc_confidence_low_max_files": _DEFAULT_BC_CONFIDENCE_LOW_MAX_FILES,
  1847|             "bc_confidence_moderate_max_files": _DEFAULT_BC_CONFIDENCE_MODERATE_MAX_FILES,
  1848|         },
  1849|     },
  1850|     "domain_policy": {
  1851|         "profile_id": "domain-governance-policy-v1",
  1852|         "schema_version": "0.1",
  1853|         "excluded_from_scoring": sorted(_DEFAULT_EXCLUDED_FROM_SCORING),
  1854|         "passive_inheritance_risk_domains": sorted(_DEFAULT_PASSIVE_INHERITANCE_RISK_DOMAINS),
  1855|         "domain_guidance": dict(_DEFAULT_DOMAIN_GUIDANCE),
  1856|         "static_findings_guidance": list(_DEFAULT_STATIC_FINDINGS_GUIDANCE),
  1857|     },
  1858|     "client_onboarding": {
  1859|         "profile_id": "client-onboarding-policy-v1",
  1860|         "schema_version": "0.1",
  1861|         "thresholds": {
  1862|             "wp_stable_min": _DEFAULT_ONBOARD_WP_STABLE_MIN,
  1863|             "wp_mixed_min": _DEFAULT_ONBOARD_WP_MIXED_MIN,
  1864|             "xc_high_portability_min": _DEFAULT_ONBOARD_XC_HIGH_PORTABILITY_MIN,
  1865|             "xc_moderate_portability_min": _DEFAULT_ONBOARD_XC_MODERATE_PORTABILITY_MIN,
  1866|             "n_files_low_max": _DEFAULT_ONBOARD_N_FILES_LOW_MAX,
  1867|             "n_files_moderate_max": _DEFAULT_ONBOARD_N_FILES_MODERATE_MAX,
  1868|         },
  1869|     },
  1870|     "finding_rules": {
  1871|         "profile_id": "finding-rules-v1",
  1872|         "schema_version": "0.1",
  1873|         "rules": {},
  1874|         "note": (
  1875|             "No built-in Python default rule descriptions -- this profile is "
  1876|             "documentation-only (never drives classification logic; the "
  1877|             "rule_id constants and the classification rules themselves live "
  1878|             "in this file). See policies/governance/finding_rules.json for "
  1879|             "the shipped descriptions."
  1880|         ),
  1881|     },
  1882|     "anomaly_thresholds": {
  1883|         "profile_id": "anomaly-thresholds-v1",
  1884|         "schema_version": "0.1",
  1885|         "thresholds": {
  1886|             "provided_carried_downstream_min": _DEFAULT_PROVIDED_CARRIED_DOWNSTREAM_MIN,
  1887|             "provided_active_use_max": _DEFAULT_PROVIDED_ACTIVE_USE_MAX,
  1888|             "primary_read_active_use_min": _DEFAULT_PRIMARY_READ_ACTIVE_USE_MIN,
  1889|             "passive_indicator_high_min": _DEFAULT_PASSIVE_INDICATOR_HIGH_MIN,
  1890|             "passive_indicator_moderate_min": _DEFAULT_PASSIVE_INDICATOR_MODERATE_MIN,
  1891|             "passive_inheritance_risk_bundle_share_max": _DEFAULT_PASSIVE_INHERITANCE_RISK_BUNDLE_SHARE_MAX,
  1892|             "bundle_share_very_low_max": _DEFAULT_BUNDLE_SHARE_VERY_LOW_MAX,
  1893|             "gt_tp_gap_gt_min": _DEFAULT_GT_TP_GAP_GT_MIN,
  1894|             "gt_tp_gap_tp_max": _DEFAULT_GT_TP_GAP_TP_MAX,
  1895|             "group2_scope_divergence_gap_min": _DEFAULT_GROUP2_SCOPE_DIVERGENCE_GAP_MIN,
  1896|             "group1_scope_spread_gap_min": _DEFAULT_GROUP1_SCOPE_SPREAD_GAP_MIN,
  1897|             "tp_tc_bypass_gap_min": _DEFAULT_TP_TC_BYPASS_GAP_MIN,
  1898|             "weak_tc_max": _DEFAULT_WEAK_TC_MAX,
  1899|             "weak_cp_max": _DEFAULT_WEAK_CP_MAX,
  1900|             "view_template_zero_discipline_max": _DEFAULT_VIEW_TEMPLATE_ZERO_DISCIPLINE_MAX,
  1901|             "phases_tp_extension_max": _DEFAULT_PHASES_TP_EXTENSION_MAX,
  1902|             "phases_tw_min": _DEFAULT_PHASES_TW_MIN,
  1903|             "portfolio_shape_density_min": _DEFAULT_PORTFOLIO_SHAPE_DENSITY_MIN,
  1904|             "portfolio_shape_union_jaccard_max": _DEFAULT_PORTFOLIO_SHAPE_UNION_JACCARD_MAX,
  1905|             "union_breadth_corpus_wide_clients_pct_min": _DEFAULT_UNION_BREADTH_CORPUS_WIDE_CLIENTS_PCT_MIN,
  1906|             "union_breadth_client_wide_clients_pct_min": _DEFAULT_UNION_BREADTH_CLIENT_WIDE_CLIENTS_PCT_MIN,
  1907|             "union_breadth_project_wide_min_projects": _DEFAULT_UNION_BREADTH_PROJECT_WIDE_MIN_PROJECTS,
  1908|             "union_breadth_file_level_max_files": _DEFAULT_UNION_BREADTH_FILE_LEVEL_MAX_FILES,
  1909|             "union_breadth_broad_min_patterns": _DEFAULT_UNION_BREADTH_BROAD_MIN_PATTERNS,
  1910|             "union_breadth_narrow_file_level_share_min": _DEFAULT_UNION_BREADTH_NARROW_FILE_LEVEL_SHARE_MIN,
  1911|             "union_breadth_weak_cascade_max": _DEFAULT_UNION_BREADTH_WEAK_CASCADE_MAX,
  1912|             "union_breadth_strong_cascade_min": _DEFAULT_UNION_BREADTH_STRONG_CASCADE_MIN,
  1913|         },
  1914|     },
  1915| }
  1916| 
  1917| # Populated by apply_governance_policy() with whichever finding_rules profile
  1918| # was actually resolved -- {rule_id: {"finding_type":..., "description":...}}.
  1919| # Documentation-only: no classification logic reads this.
  1920| FINDING_RULE_DESCRIPTIONS: dict = {}
  1921| 
  1922| # Populated by main() with the raw return value of load_governance_policy() --
  1923| # {"policy_dir":..., "profiles": {...}, "load_status": {...}} -- so
  1924| # governance_package_manifest.json/governance_package_health.json can report
  1925| # exactly which profile_id/schema_version/source was used for this run.
  1926| LOADED_GOVERNANCE_POLICY: Optional[dict] = None
  1927| 
  1928| 
```
