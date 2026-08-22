# Chunk of tools/analyze_promotion_candidates.py

- Source relative path: `tools/analyze_promotion_candidates.py`
- Chunk: 2 of 3
- Original line range: 399-573
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: compute_reuse_scope, compute_reuse_scope._union_tokens
- Source SHA-256: 1e5dbc478f5d1e9f1e948261cb2b0121cc79fd2657bbc116c27e7eeea845924b
- Starts inside symbol: no
- Ends inside symbol: no

```
   399| def compute_reuse_scope(reuse: pd.DataFrame, min_enterprise_clients: int, policy: EnterprisePolicy | None = None) -> tuple:
   400|     """Broadest reuse_scope observed for a (domain, join_hash, unit_system),
   401|     restricted to Project-role rows -- see module docstring for why
   402|     Template/Container/Generic rows are excluded. Returns (classified,
   403|     unclassified): `classified` has one row per key that resolved to a real
   404|     scope value; `unclassified` carries rows whose reuse_bucket was
   405|     "unclassified" (denominators unavailable / degraded source) for their
   406|     own diagnostic output, never silently merged into "ungoverned". Keyed
   407|     on unit_system for the same reason as compute_seeded_scope -- see its
   408|     docstring.
   409| 
   410|     Prefers `view_scope == "used"` rows over `view_scope == "all"`, decided
   411|     per (domain, join_hash, unit_system) identity -- not per (client_label,
   412|     discipline_label) population. Used-view is the active-delivery-practice
   413|     signal for Project rows (docs/cross_segment_comparison.md: "Project
   414|     used-view rows can support active delivery practice reporting");
   415|     all-view mixes in passively-inherited, configured-but-never-rendered
   416|     content. Using all-view unconditionally would be inconsistent with the
   417|     base population itself, which is already restricted to `state ==
   418|     "local_active"` rows from cross_segment_governance_states.csv -- an
   419|     active-use signal on the governance side.
   420| 
   421|     All-view rows are used only as a fallback, and only for an identity
   422|     with *no* used-view row anywhere (e.g. `inventory_status=
   423|     used_view_unavailable` upstream, older exports without
   424|     membership_matrix used-view data). A per-population fallback (falling
   425|     back to all-view just for the specific client lacking used data, while
   426|     keeping used-view rows for other clients) was tried and rejected: each
   427|     row's `reuse_bucket`/`pct_clients_present` is computed upstream against
   428|     that row's own view_scope's client population -- a "used" row's
   429|     denominator is the used-view-eligible client pool, an "all" row's is
   430|     the (typically larger) all-view client pool, which can include clients
   431|     with no active-use evidence at all. Mixing rows computed against two
   432|     different denominators in the same max-rank tie-break lets one client's
   433|     passively-configured all-view corpus_wide reading dominate over other
   434|     clients' genuinely narrower used-view evidence. Falling back at the
   435|     whole-identity level instead means an identity with partial used-view
   436|     coverage is scored from used-view evidence only, never blended with an
   437|     all-view reading computed against a different population. Which source
   438|     won is recorded per output row in `reuse_view_source` ("used" /
   439|     "all_fallback") rather than left implicit.
   440|     """
   441|     policy = policy or load_enterprise_policy()
   442| 
   443|     project_rows = reuse[reuse["governance_role"] == "Project"].copy()
   444| 
   445|     identity_cols = ["domain", "join_hash", "unit_system"]
   446|     used = project_rows[project_rows["view_scope"] == "used"].copy()
   447|     used["reuse_view_source"] = "used"
   448|     identities_with_used = set(zip(*[used[c] for c in identity_cols])) if len(used) else set()
   449|     all_fallback = project_rows[
   450|         (project_rows["view_scope"] == "all")
   451|         & ~pd.Series(
   452|             list(zip(*[project_rows[c] for c in identity_cols])), index=project_rows.index
   453|         ).isin(identities_with_used)
   454|     ].copy()
   455|     all_fallback["reuse_view_source"] = "all_fallback"
   456|     r = pd.concat([used, all_fallback], ignore_index=True)
   457| 
   458|     empty_cols = [
   459|         "domain", "join_hash", "unit_system", "reuse_scope", "reuse_bucket",
   460|         "client_label", "n_clients_present", "n_clients_denominator",
   461|         "pct_clients_present", "n_projects_present", "n_projects_denominator",
   462|         "pct_projects_present", "n_files_present", "n_files_denominator",
   463|         "pct_files_present", "enterprise_evidence_downgraded",
   464|         "reuse_client_pool_is_enterprise", "reuse_view_source",
   465|     ]
   466|     if r.empty:
   467|         return pd.DataFrame(columns=empty_cols), pd.DataFrame(columns=empty_cols)
   468| 
   469|     r["reuse_scope"] = r["reuse_bucket"].map(REUSE_BUCKET_TO_SCOPE)
   470| 
   471|     downgrade_mask = (r["reuse_bucket"] == "corpus_wide") & (
   472|         r["n_clients_present"] < min_enterprise_clients
   473|     )
   474|     r["enterprise_evidence_downgraded"] = downgrade_mask
   475|     r.loc[downgrade_mask, "reuse_scope"] = "client"
   476| 
   477|     r["_is_enterprise_row"] = r["client_label"].map(policy.is_enterprise)
   478| 
   479|     unclassified = r[r["reuse_scope"].isna()].copy()
   480|     unclassified["reuse_scope"] = "unclassified"
   481|     unclassified["reuse_client_pool_is_enterprise"] = unclassified["_is_enterprise_row"]
   482| 
   483|     classified = r[r["reuse_scope"].notna()].copy()
   484|     if classified.empty:
   485|         classified["reuse_client_pool_is_enterprise"] = classified.get(
   486|             "_is_enterprise_row", pd.Series(dtype=bool)
   487|         )
   488|         return classified.reindex(columns=empty_cols), unclassified.reindex(columns=empty_cols)
   489| 
   490|     # Multiple client-scoped rows routinely tie at the same reuse_scope_rank
   491|     # -- most commonly every client-row for a join_hash hits "corpus_wide"
   492|     # together, since pct_clients_present is a shared, not client-specific,
   493|     # quantity. idxmax() alone would silently keep one arbitrary client's
   494|     # n_projects_present/n_files_present (CSV-row-order dependent) and
   495|     # discard the others.
   496|     key_cols = ["domain", "join_hash", "unit_system"]
   497|     classified["reuse_scope_rank"] = classified["reuse_scope"].map(SCOPE_RANK)
   498|     max_rank = classified.groupby(key_cols)["reuse_scope_rank"].transform("max")
   499|     tied = classified[classified["reuse_scope_rank"] == max_rank].copy()
   500| 
   501|     def _union_tokens(series):
   502|         tokens = set()
   503|         for s in series:
   504|             tokens.update(t for t in str(s).split(";") if t)
   505|         return ";".join(sorted(tokens))
   506| 
   507|     # Two-stage aggregation, because project/file counts are disjoint across
   508|     # different clients but NOT across different disciplines within the
   509|     # same client: pattern_reuse_distribution.csv computes n_projects_present
   510|     # per (client_label, discipline_label) pool, and a single project can
   511|     # legitimately have files in more than one discipline (e.g. Arch and
   512|     # Struct on the same project). Summing across tied discipline rows for
   513|     # one client would double-count that project.
   514|     #
   515|     # Stage 1 -- within one (identity, client_label), collapse any tied
   516|     # discipline rows via max(): a non-overcounting "at least this many
   517|     # projects/files" reading, since we cannot deduplicate individual
   518|     # project identities from the aggregated counts alone.
   519|     per_client = (
   520|         tied.groupby(key_cols + ["client_label"])
   521|         .agg(
   522|             reuse_scope=("reuse_scope", "first"),
   523|             reuse_bucket=("reuse_bucket", _union_tokens),
   524|             reuse_view_source=("reuse_view_source", _union_tokens),
   525|             n_clients_present=("n_clients_present", "max"),
   526|             n_clients_denominator=("n_clients_denominator", "max"),
   527|             pct_clients_present=("pct_clients_present", "max"),
   528|             n_projects_present=("n_projects_present", "max"),
   529|             n_projects_denominator=("n_projects_denominator", "max"),
   530|             n_files_present=("n_files_present", "max"),
   531|             n_files_denominator=("n_files_denominator", "max"),
   532|             enterprise_evidence_downgraded=("enterprise_evidence_downgraded", "max"),
   533|             reuse_client_pool_is_enterprise=("_is_enterprise_row", "max"),
   534|         )
   535|         .reset_index()
   536|     )
   537| 
   538|     # Stage 2 -- across distinct clients, project/file pools are genuinely
   539|     # disjoint (a project belongs to exactly one client), so summing is
   540|     # correct here, same as before.
   541|     classified = (
   542|         per_client.groupby(key_cols)
   543|         .agg(
   544|             reuse_scope=("reuse_scope", "first"),
   545|             reuse_bucket=("reuse_bucket", _union_tokens),
   546|             reuse_view_source=("reuse_view_source", _union_tokens),
   547|             client_label=("client_label", lambda s: ";".join(sorted(set(s.astype(str))))),
   548|             n_clients_present=("n_clients_present", "max"),
   549|             n_clients_denominator=("n_clients_denominator", "max"),
   550|             pct_clients_present=("pct_clients_present", "max"),
   551|             n_projects_present=("n_projects_present", "sum"),
   552|             n_projects_denominator=("n_projects_denominator", "sum"),
   553|             n_files_present=("n_files_present", "sum"),
   554|             n_files_denominator=("n_files_denominator", "sum"),
   555|             enterprise_evidence_downgraded=("enterprise_evidence_downgraded", "max"),
   556|             reuse_client_pool_is_enterprise=("reuse_client_pool_is_enterprise", "max"),
   557|         )
   558|         .reset_index()
   559|     )
   560|     classified["pct_projects_present"] = (
   561|         classified["n_projects_present"] / classified["n_projects_denominator"].replace(0, np.nan)
   562|     ).fillna(0)
   563|     classified["pct_files_present"] = (
   564|         classified["n_files_present"] / classified["n_files_denominator"].replace(0, np.nan)
   565|     ).fillna(0)
   566| 
   567|     return classified.reindex(columns=empty_cols), unclassified.reindex(columns=empty_cols)
   568| 
   569| 
   570| # ============================================================
   571| # MAIN
   572| # ============================================================
   573| 
```
