# Chunk of tools/generate_governance_narrative.py

- Source relative path: `tools/generate_governance_narrative.py`
- Chunk: 13 of 17
- Original line range: 5372-5847
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: render_findings_and_recommendations, render_findings_and_recommendations._domain_ids, build_comparison_completeness, build_comparison_completeness._key, build_comparison_completeness._state_key, render_limitations, _narrative_for_inventory_entry, render_file_inventory_brief_section, render_governance_brief
- Source SHA-256: 7a8e1def8713100e21b852dc24c2c714a8e5330815affb3e703ed572a2829d9c
- Starts inside symbol: no
- Ends inside symbol: no

```
  5372| def render_findings_and_recommendations(
  5373|     cascade: dict,
  5374|     client_rows: list[dict],
  5375|     state_summary: Optional[dict] = None,
  5376|     findings: Optional[list] = None,
  5377| ) -> str:
  5378|     state_summary = state_summary or {}
  5379|     findings = findings if findings is not None else build_structured_findings(cascade, client_rows, state_summary)
  5380| 
  5381|     def _domain_ids(finding_type: str) -> list:
  5382|         return [f["subject"]["id"] for f in findings if f["finding_type"] == finding_type]
  5383| 
  5384|     baseline_candidates = [DOMAIN_LABELS.get(dom, dom) for dom in _domain_ids("baseline_candidate")]
  5385|     clean_baseline = [DOMAIN_LABELS.get(dom, dom) for dom in _domain_ids("strong_baseline_candidate")]
  5386|     needs_review = [DOMAIN_LABELS.get(dom, dom) for dom in _domain_ids("local_review_required")]
  5387|     high_frag = [DOMAIN_LABELS.get(dom, dom) for dom in _domain_ids("high_fragmentation")]
  5388|     universal = [DOMAIN_LABELS.get(dom, dom) for dom in _domain_ids("cross_client_convergence")]
  5389|     low_coherence = [f["subject"]["id"] for f in findings if f["finding_type"] == "low_client_coherence"]
  5390| 
  5391|     lines = [
  5392|         "## Key Findings and Governance Questions\n",
  5393|         "### What appears to be working\n",
  5394|         f"**A governance floor is visible.** {len(baseline_candidates)} domains have enough propagation evidence "
  5395|         "to be treated as baseline candidates for leadership review. "
  5396|         f"{len(clean_baseline)} of those currently {'has' if len(clean_baseline) == 1 else 'have'} no material state exception in the available outputs. "
  5397|         "This is evidence of a common base, not a standards approval.\n",
  5398|     ]
  5399| 
  5400|     if baseline_candidates:
  5401|         lines.append(
  5402|             "Baseline candidate domains: " + ", ".join(baseline_candidates) + ".\n"
  5403|         )
  5404| 
  5405|     if universal:
  5406|         lines.append(
  5407|             f"**Some natural common-base candidates are visible.** "
  5408|             f"{', '.join(universal)} show strong cross-client convergence (>{pct(XC_STRONG_CONVERGENCE)}). "
  5409|             "This supports governance review, but still requires a decision about whether the convergence is intentional, portable, and worth formalising.\n"
  5410|         )
  5411| 
  5412|     lines += [
  5413|         "\n### What needs attention\n",
  5414|         "**View-template governance remains discipline-sensitive.** View-template domains with weak containment or low discipline coherence should be handled as discipline-specific governance questions, not forced into a single firmwide baseline.\n",
  5415|     ]
  5416| 
  5417|     if "phases" in cascade and cascade["phases"]["tp"] is not None:
  5418|         phases_tp = cascade["phases"]["tp"]
  5419|         phases_tw = cascade["phases"]["tw"]
  5420|         if phases_tp < PHASES_TP_EXTENSION_MAX and phases_tw is not None and phases_tw > PHASES_TW_MIN:
  5421|             lines.append(
  5422|                 "**Phases show project-level extension.** Templates are internally consistent on phase definitions, but projects carry phases not defined in templates. The governance question is whether those additions are intentional project practice, client-specific vocabulary, or unmanaged accumulation.\n"
  5423|             )
  5424| 
  5425|     for guidance in STATIC_FINDINGS_GUIDANCE:
  5426|         lines.append(f"**{guidance}**\n")
  5427| 
  5428|     if needs_review:
  5429|         lines.append(
  5430|             f"**{len(needs_review)} domains need review before baseline language is safe.** "
  5431|             f"Examples include: {', '.join(needs_review[:12])}.\n"
  5432|         )
  5433| 
  5434|     if high_frag:
  5435|         lines.append(
  5436|             f"**High-fragmentation domains are not single-standard candidates in this run.** "
  5437|             f"{', '.join(high_frag)} should be treated as governance-design questions first.\n"
  5438|         )
  5439| 
  5440|     if low_coherence:
  5441|         lines.append(
  5442|             f"**Some clients show high within-client variation ({', '.join(low_coherence)}).** "
  5443|             "Where file counts are small, treat this as a signal for further sampling rather than a definitive client judgement.\n"
  5444|         )
  5445| 
  5446|     lines += [
  5447|         "\n### Recommended Leadership Questions\n",
  5448|         "1. **Which baseline candidates should enter ratification review?** Confirm intent, portability, active-use evidence, and whether local-active variants need separate handling before approval.\n",
  5449|         "2. **Where should governance use an approved-list or starter-content model instead of full convergence?** This is especially relevant for families, materials, and domains with project-specific vocabulary.\n",
  5450|         "3. **Which active local practices deserve roll-up or documentation?** Decide whether they are firmwide candidates, client/discipline playbook content, permitted variants, or project exceptions.\n",
  5451|         "4. **Which missing or passive inherited content is intentional?** Distinguish deliberate pruning, unused starter stock, role-specific specialization, and propagation failure.\n",
  5452|         "5. **What additional segmentation is needed before stronger claims are made?** Project type, business center, region, and larger segment samples remain future enhancements unless supplied upstream.\n",
  5453|     ]
  5454| 
  5455|     return "\n".join(lines)
  5456| 
  5457| 
  5458| def build_comparison_completeness(
  5459|     summary_rows: list, comparison_registry_rows: list,
  5460|     governance_state_rows: Optional[list] = None,
  5461|     governance_state_summary_rows: Optional[list] = None,
  5462| ) -> dict:
  5463|     """Per-domain counts of expected (segment_id_a, segment_id_b,
  5464|     comparison_type) pairs -- the union of keys seen in cross_segment_summary.csv
  5465|     (proof a comparison ran and produced evidence) and comparison_registry.csv
  5466|     (proof a comparison was computed and stamped at some point) -- that have
  5467|     a matching entry in BOTH files ("present", further split "stale" when the
  5468|     registry's computed_utc predates the summary row's own executed_utc, i.e.
  5469|     the registry snapshot is older than the evidence it should describe), or
  5470|     a matching entry in only one of the two ("missing": no registry stamp for
  5471|     evidenced work, treated as stale rather than missing when the reverse --
  5472|     the registry has a stamp but the current summary snapshot has no matching
  5473|     row AND no matching governance-state evidence either -- see below).
  5474| 
  5475|     PR review finding (D-032): iterating summary_rows alone cannot surface a
  5476|     registry entry with no matching summary evidence at all. Unioning the two
  5477|     key sets catches that case, but still cannot detect a comparison that was
  5478|     NEVER run and has zero rows in EITHER file -- that would need a canonical
  5479|     inventory of expected work items (e.g. segment_manifest.csv's full
  5480|     lattice x domain list x comparison type), which this generator does not
  5481|     reconstruct. This function remains a narrower, self-contained proxy: it
  5482|     answers "is there a mismatch between what has evidence and what the
  5483|     registry has stamped," not "was every truly-expected comparison run."
  5484| 
  5485|     PR review finding (D-032): compare_cross_segment.py legitimately stamps
  5486|     comparison_registry.csv for a (pair, domain) work item that produced
  5487|     governance-state output but no cross_segment_summary.csv row at all --
  5488|     directed work below --min-patterns still needs provided_but_missing
  5489|     visibility, so `produced_output` there is True from governance-state
  5490|     rows alone (see main()'s `if ctype in GOVERNANCE_STATE_DIRECTED_TYPES`
  5491|     block). A registry-only entry that matches a governance_state_rows/
  5492|     governance_state_summary_rows key (segment_id_reference/_target mapped
  5493|     to segment_id_a/b) is therefore counted present rather than stale purely
  5494|     for reference-existing purposes -- it is not automatically assumed
  5495|     current, though: recency is still checked against that state row's own
  5496|     `executed_utc`, the same as the summary-row path, since an independently
  5497|     supplied registry and state CSV can come from different runs.
  5498| 
  5499|     Self-contained: uses only comparison_registry.csv's own identity
  5500|     (segment_id_a/b, comparison_type, domain) and recency (computed_utc)
  5501|     fields against cross_segment_summary.csv's matching identity/executed_utc
  5502|     fields, plus governance-state rows' identity fields for the exception
  5503|     above. This generator has no access to compare_cross_segment.py's live
  5504|     segment registry (run_registry.csv's population_hash/last_run_utc), so
  5505|     this is a narrower, narrative-side proxy for staleness than
  5506|     comparison_is_stale() computes there -- it answers "is this registry
  5507|     snapshot older than the evidence it should describe," not "has the
  5508|     underlying segment population changed since this pair was computed."
  5509|     """
  5510|     def _key(row: dict) -> tuple:
  5511|         return (
  5512|             row.get("segment_id_a", ""), row.get("segment_id_b", ""),
  5513|             row.get("comparison_type", ""), row.get("domain", ""),
  5514|         )
  5515| 
  5516|     def _state_key(row: dict) -> tuple:
  5517|         return (
  5518|             row.get("segment_id_reference", ""), row.get("segment_id_target", ""),
  5519|             row.get("comparison_type", ""), row.get("domain", ""),
  5520|         )
  5521| 
  5522|     registry_index: dict = {_key(row): row for row in comparison_registry_rows}
  5523|     summary_index: dict = {}
  5524|     for row in summary_rows:
  5525|         if row.get("domain", ""):
  5526|             summary_index.setdefault(_key(row), row)
  5527| 
  5528|     # PR review finding: setdefault() kept whichever source's row for a key
  5529|     # was seen FIRST, so if governance_state_rows and governance_state_
  5530|     # summary_rows carry the same key from two different runs, an older
  5531|     # raw-state timestamp could shadow newer summary evidence and hide a
  5532|     # registry stamp that should have been flagged stale. Keep the maximum
  5533|     # executed_utc seen across all matching rows from either source instead.
  5534|     state_executed_utc: dict = {}
  5535|     for row in (governance_state_rows or []) + (governance_state_summary_rows or []):
  5536|         if row.get("domain", ""):
  5537|             key = _state_key(row)
  5538|             executed_utc = row.get("executed_utc", "")
  5539|             if executed_utc and executed_utc > state_executed_utc.get(key, ""):
  5540|                 state_executed_utc[key] = executed_utc
  5541|             else:
  5542|                 state_executed_utc.setdefault(key, executed_utc)
  5543| 
  5544|     by_domain: dict = defaultdict(lambda: {"total": 0, "present": 0, "missing": 0, "stale": 0})
  5545|     # PR review finding: a directed comparison that produced governance-state
  5546|     # evidence but was excluded from comparison_registry.csv (e.g. a segment
  5547|     # not marked complete) and has no summary row either was invisible to
  5548|     # this loop -- neither registry_index nor summary_index has its key.
  5549|     # Unioning state_executed_utc's keys in makes it show up as "missing"
  5550|     # like any other unstamped work item, instead of not being counted at all.
  5551|     for key in registry_index.keys() | summary_index.keys() | state_executed_utc.keys():
  5552|         domain = key[3]
  5553|         if not domain:
  5554|             continue
  5555|         counts = by_domain[domain]
  5556|         counts["total"] += 1
  5557|         registry_row = registry_index.get(key)
  5558|         summary_row = summary_index.get(key)
  5559|         if registry_row is None:
  5560|             counts["missing"] += 1
  5561|             continue
  5562|         counts["present"] += 1
  5563|         if summary_row is None:
  5564|             if key in state_executed_utc:
  5565|                 # Valid state-only stamp: this (pair, domain) produced
  5566|                 # governance-state evidence but no summary row, and the
  5567|                 # registry correctly reflects that -- not a staleness signal
  5568|                 # on its own. Still compare recency: an independently
  5569|                 # supplied registry and state CSV can come from different
  5570|                 # runs, so a registry stamp older than the state evidence it
  5571|                 # should describe is stale the same way a summary-row match
  5572|                 # would be.
  5573|                 computed_utc = registry_row.get("computed_utc", "")
  5574|                 executed_utc = state_executed_utc[key]
  5575|                 if computed_utc and executed_utc and computed_utc < executed_utc:
  5576|                     counts["stale"] += 1
  5577|                 continue
  5578|             # Registry has a stamp, but the current summary snapshot has no
  5579|             # matching row and no matching governance-state evidence either
  5580|             # -- the registry is out of sync with this run's evidence (e.g.
  5581|             # a domain-scoped run that didn't recompute everything a
  5582|             # broader prior run did).
  5583|             counts["stale"] += 1
  5584|             continue
  5585|         # PR review finding: a key can have BOTH a summary row and state
  5586|         # evidence (from a different run) -- comparing only against the
  5587|         # summary row's executed_utc missed a registry stamp that was stale
  5588|         # relative to newer state evidence. Compare against whichever
  5589|         # evidence timestamp is newest.
  5590|         computed_utc = registry_row.get("computed_utc", "")
  5591|         executed_utc = max(summary_row.get("executed_utc", ""), state_executed_utc.get(key, ""))
  5592|         if computed_utc and executed_utc and computed_utc < executed_utc:
  5593|             counts["stale"] += 1
  5594|     return dict(by_domain)
  5595| 
  5596| 
  5597| def render_limitations(corpus: dict, legacy_used_fallback: bool = False, has_state_outputs: bool = False,
  5598|                         comparison_completeness: Optional[dict] = None) -> str:
  5599|     used_fallback_note = (
  5600|         "\n- **Used-view fallback:** Used-view columns were not found in the summary schema. Where legacy columns are reused as fallback, active-use conclusions are limited and should be confirmed with dual-view outputs."
  5601|         if legacy_used_fallback else ""
  5602|     )
  5603|     state_note = (
  5604|         "\n- **Governance-state counts:** If upstream governance-state rows are not deduplicated to unique patterns, count fields should be treated as comparison-state rows. Prefer shares/rankings for leadership claims."
  5605|         if has_state_outputs else
  5606|         "\n- **Governance-state limitation:** Governance-state outputs were not provided. Inherited-but-unused and local-active findings are inferred indirectly."
  5607|     )
  5608|     # Read from the resolved EXCLUDED_FROM_SCORING module global (set by
  5609|     # apply_governance_policy() from domain_governance_policy.json before
  5610|     # main() renders this section), not a hardcoded literal -- a --policy-dir
  5611|     # override that changes which domain(s) are excluded must be reflected
  5612|     # here, or this note would describe a different exclusion set than the
  5613|     # one that actually produced the CSV/health output for this run.
  5614|     excluded_domains = sorted(EXCLUDED_FROM_SCORING)
  5615|     if excluded_domains:
  5616|         excluded_note = (
  5617|             f"- **Excluded domain{'s' if len(excluded_domains) != 1 else ''}:** "
  5618|             f"{', '.join(f'`{d}`' for d in excluded_domains)} "
  5619|             f"{'are' if len(excluded_domains) != 1 else 'is'} excluded from "
  5620|             "aggregate governance scoring because "
  5621|             f"{'they are' if len(excluded_domains) != 1 else 'it is'} "
  5622|             "structurally anomalous in the current corpus."
  5623|         )
  5624|     else:
  5625|         excluded_note = "- **Excluded domains:** none for this run's policy profile."
  5626| 
  5627|     completeness_section = ""
  5628|     if comparison_completeness:
  5629|         total_checked = sum(c["total"] for c in comparison_completeness.values())
  5630|         completeness_lines = [
  5631|             "\n\n### Input Completeness / Staleness\n",
  5632|             "Per-domain count of segment/domain comparison pairs referenced in "
  5633|             "`cross_segment_summary.csv`, `comparison_registry.csv`, and/or "
  5634|             "governance-state evidence (`--governance-state`/`--governance-"
  5635|             "state-summary`, for directed comparisons that produce state "
  5636|             "output but no summary row), present in / missing from / stale "
  5637|             "relative to a registry stamp -- distinguishes genuinely weak "
  5638|             "evidence from a comparison that was run but not (yet) "
  5639|             "registered, or registered but stale relative to the current "
  5640|             "run. This is a proxy, not a canonical inventory: a comparison "
  5641|             "absent from ALL THREE evidence sources (never run, never "
  5642|             "registered, no state evidence either) cannot be detected here "
  5643|             "and is not counted below. Registry content itself is never "
  5644|             "reproduced here; see `governance_evidence_map.json`'s "
  5645|             "`comparison_registry` entry for a drill-down pointer.\n",
  5646|         ]
  5647|         flagged = sorted(
  5648|             ((dom, c) for dom, c in comparison_completeness.items() if c["missing"] or c["stale"]),
  5649|             key=lambda item: (-(item[1]["missing"] + item[1]["stale"]), item[0]),
  5650|         )
  5651|         if flagged:
  5652|             completeness_lines.append("| domain | present | missing | stale |")
  5653|             completeness_lines.append("|---|---:|---:|---:|")
  5654|             for dom, c in flagged[:20]:
  5655|                 completeness_lines.append(f"| {dom} | {c['present']} | {c['missing']} | {c['stale']} |")
  5656|             if len(flagged) > 20:
  5657|                 completeness_lines.append(f"\nTable limited to 20 domains; {len(flagged) - 20} domains not shown.")
  5658|         else:
  5659|             completeness_lines.append(
  5660|                 f"No missing or stale comparison pairs across the {total_checked} "
  5661|                 "segment/domain pair(s) referenced in these evidence sources -- every "
  5662|                 "pair checked has a current registry entry. This does not confirm every "
  5663|                 "comparison that should exist was run; see the note above.\n"
  5664|             )
  5665|         completeness_section = "\n".join(completeness_lines)
  5666| 
  5667|     return f"""---
  5668| 
  5669| ## Analytical Notes and Limitations
  5670| 
  5671| - **Corpus size:** {corpus['Project']} project files is a {"moderate" if corpus['Project'] >= 80 else "small"} corpus. Client-level findings carry higher uncertainty than corpus-level findings.
  5672| - **Scope boundary:** This report is discovery and classification only. It does not approve standards, assign owners, define compliance rules, or judge project teams.
  5673| - **Segment boundary:** Project type, business center, and region should be treated as future segment dimensions unless explicit upstream segment CSVs are supplied.
  5674| - **Imperial/metric split:** All project files are imperial. Metric templates and coordination files exist but metric projects are not yet represented. Metric findings are limited to template-to-container comparisons only.
  5675| - **Scores are means across file pairs.** Individual files may score substantially higher or lower than reported means.
  5676| - **Patterns are normalised configuration fingerprints** (join_hash values) capturing the behavioural identity of a configuration record, independent of Revit element IDs. Two files sharing a pattern have identical or functionally equivalent configuration for that element.
  5677| {excluded_note}{used_fallback_note}{state_note}{completeness_section}
  5678| 
  5679| ---
  5680| 
  5681| *Generated by `{GENERATOR_IDENTITY}` from cross_segment_summary.csv, cross_segment_pooled.csv, and optional governance-state outputs.*
  5682| *Supporting tables: governance_domain_summary.csv, governance_client_summary.csv, governance_bc_summary.csv.*
  5683| """
  5684| 
  5685| 
  5686| _BRIEF_FINDING_SECTIONS = (
  5687|     # (finding_type, section heading, cap)
  5688|     ("strong_baseline_candidate", "Strong baseline candidates", 15),
  5689|     ("local_review_required", "Domains needing local/use review before baseline language is safe", 15),
  5690|     ("high_fragmentation", "High-fragmentation domains", 15),
  5691|     ("passive_inheritance_risk", "Passive-inheritance risk", 15),
  5692|     ("cross_client_convergence", "Strong cross-client convergence (natural common-base candidates)", 10),
  5693|     ("missing_or_degraded_evidence", "Insufficient or degraded evidence", 15),
  5694| )
  5695| 
  5696| 
  5697| def _narrative_for_inventory_entry(entry: dict, matrix_manifest_by_name: dict) -> str:
  5698|     """One or two live-computed sentences describing a file the directory
  5699|     scan found but no artifact_id has been registered for yet (see
  5700|     inventory_export_directory_files()). Never hand-maintained per filename:
  5701| 
  5702|     - If the filename matches a matrix_name already documented in
  5703|       matrix_output_manifest.csv (compare_cross_segment.py's own
  5704|       add_manifest() calls), reuse that row's interpretation/known_limitations
  5705|       text verbatim -- the same free-text narrative field pattern already
  5706|       used for the registered project_* matrix artifacts, just applied to a
  5707|       matrix this generator hasn't wired an input flag for yet.
  5708|     - Otherwise, fall back to a structural sentence built only from the
  5709|       header/row-count this scan already computed -- honest about not
  5710|       knowing the file's meaning, rather than guessing.
  5711|     """
  5712|     filename = entry["filename"]
  5713|     manifest_row = matrix_manifest_by_name.get(filename)
  5714|     if manifest_row and manifest_row.get("interpretation"):
  5715|         text = manifest_row["interpretation"]
  5716|         if manifest_row.get("known_limitations"):
  5717|             text += " " + manifest_row["known_limitations"]
  5718|         return f"Per matrix_output_manifest.csv: {text}"
  5719|     if entry.get("parse_error"):
  5720|         return f"Could not be scanned ({entry['parse_error']})."
  5721|     if entry.get("empty_file"):
  5722|         return "Empty file (no header row)."
  5723|     columns = entry.get("columns", [])
  5724|     col_names = [c["name"] for c in columns]
  5725|     shown = ", ".join(col_names[:8]) + (", ..." if len(col_names) > 8 else "")
  5726|     return (
  5727|         f"{len(columns)} column(s) ({shown}), {entry.get('row_count', 0)} data row(s). "
  5728|         "This generator does not read this file today; grain and meaning are "
  5729|         "inferred only from its header, not from any hand-written description."
  5730|     )
  5731| 
  5732| 
  5733| def render_file_inventory_brief_section(file_inventory: Optional[dict]) -> str:
  5734|     """Renders the live drill-down-file directory as its own section of
  5735|     governance_brief.md -- separate from, and never interleaved into, the
  5736|     per-domain findings sections above it. Returns "" (section entirely
  5737|     omitted) when the scan found nothing undiscovered, matching this file's
  5738|     existing "omit rather than blank-render" convention for empty sections.
  5739|     See D-023 / docs/governance_evidence_package.md.
  5740|     """
  5741|     files = (file_inventory or {}).get("files") or []
  5742|     if not files:
  5743|         return ""
  5744|     lines = [
  5745|         "\n## Detail-Layer File Inventory\n",
  5746|         "> Files found under the scanned export directories that are not "
  5747|         "already one of the artifacts described above -- see "
  5748|         "`governance_file_inventory.json` for the full header/dtype/row-count "
  5749|         "detail. This is a directory of what exists at the detail layer, not "
  5750|         "a claim about what any of it means.\n",
  5751|     ]
  5752|     for f in sorted(files, key=lambda e: e["filename"]):
  5753|         lines.append(f"- **{f['filename']}** ({f.get('row_count', 0)} row(s)) -- {f.get('narrative', '')}")
  5754|     return "\n".join(lines)
  5755| 
  5756| 
  5757| def render_governance_brief(
  5758|     findings: list,
  5759|     health: dict,
  5760|     corpus: dict,
  5761|     package_schema_version: str,
  5762|     file_inventory: Optional[dict] = None,
  5763| ) -> str:
  5764|     """A narrower, run-specific digest: consumes the already-built structured
  5765|     findings list (built_structured_findings()) and package health -- it
  5766|     computes nothing new and cannot drift from governance_findings.json,
  5767|     the same discipline PR2's render_findings_and_recommendations() already
  5768|     established for the full narrative. Deliberately short: each finding
  5769|     category is capped and points back to governance_findings.json for the
  5770|     complete list rather than reproducing it. See D-022 and
  5771|     docs/governance_evidence_package.md.
  5772| 
  5773|     file_inventory (D-023): the already-built governance_file_inventory.json
  5774|     document (or None/omitted) -- same "consume, not recompute" discipline;
  5775|     this function does not scan any directory itself, only renders whatever
  5776|     inventory_export_directory_files() already found in main().
  5777|     """
  5778|     by_type: dict = defaultdict(list)
  5779|     for f in findings:
  5780|         by_type[f["finding_type"]].append(f)
  5781| 
  5782|     lines = [
  5783|         "# Governance Brief\n",
  5784|         "> **Artifact role:** Convenience summary -- a narrower, run-specific "
  5785|         "digest of `governance_findings.json`, not a new source of evidence.\n"
  5786|         "> **Authority:** Subordinate to `governance_package_health.json`, the "
  5787|         "source comparison CSVs, `governance_domain_summary.csv`/"
  5788|         "`governance_client_summary.csv`/`governance_bc_summary.csv`, and "
  5789|         "`governance_findings.json` -- if this brief disagrees with any of "
  5790|         "those, they win.\n"
  5791|         f"> **Metric semantics:** see `{INTERPRETATION_GUIDE_PATH.name}` "
  5792|         f"(schema {INTERPRETATION_GUIDE_VERSION}).\n"
  5793|         f"> **Where to look for a specific question:** see `{QUESTION_ROUTES_PATH.name}` "
  5794|         f"(schema {QUESTION_ROUTES_VERSION}).\n"
  5795|         "> **Full detail:** `governance_narrative_context.md`, "
  5796|         "`governance_domain_summary.csv`, `governance_client_summary.csv`, "
  5797|         "`governance_bc_summary.csv`, `governance_findings.json`.\n",
  5798|         "## Package status\n",
  5799|         f"- Package health: **{health.get('overall_status', 'unknown')}**"
  5800|         + (f" ({len(health.get('warnings', []))} warning(s))" if health.get("warnings") else "")
  5801|         + "\n",
  5802|         f"- Corpus: **{corpus.get('Project', 0)}** project files, "
  5803|         f"**{corpus.get('Template', 0)}** templates, **{corpus.get('Container', 0)}** coordination files\n",
  5804|         f"- Structured findings this run: **{len(findings)}**\n",
  5805|     ]
  5806| 
  5807|     for finding_type, heading, cap in _BRIEF_FINDING_SECTIONS:
  5808|         items = sorted(by_type.get(finding_type, []), key=lambda f: f["subject"]["id"])
  5809|         if not items:
  5810|             continue
  5811|         lines.append(f"\n## {heading} ({len(items)})\n")
  5812|         shown = items[:cap]
  5813|         for f in shown:
  5814|             label_text = DOMAIN_LABELS.get(f["subject"]["id"], f["subject"]["id"]) if f["subject"]["type"] == "domain" else f["subject"]["id"]
  5815|             lines.append(f"- **{label_text}** -- {f['summary']}")
  5816|         if len(items) > cap:
  5817|             lines.append(f"- _...and {len(items) - cap} more -- see `governance_findings.json`._")
  5818| 
  5819|     low_coherence = sorted(
  5820|         (f for f in findings if f["finding_type"] == "low_client_coherence"),
  5821|         key=lambda f: f["subject"]["id"],
  5822|     )
  5823|     if low_coherence:
  5824|         lines.append(f"\n## Clients with low internal coherence ({len(low_coherence)})\n")
  5825|         for f in low_coherence[:15]:
  5826|             lines.append(f"- **{f['subject']['id']}** -- {f['summary']}")
  5827|         if len(low_coherence) > 15:
  5828|             lines.append(f"- _...and {len(low_coherence) - 15} more -- see `governance_findings.json`._")
  5829| 
  5830|     leadership_questions = [f for f in findings if f["finding_type"] == "leadership_question"]
  5831|     if leadership_questions:
  5832|         lines.append("\n## Leadership questions\n")
  5833|         for i, f in enumerate(leadership_questions, start=1):
  5834|             lines.append(f"{i}. {f['summary']}")
  5835| 
  5836|     file_inventory_section = render_file_inventory_brief_section(file_inventory)
  5837|     if file_inventory_section:
  5838|         lines.append(file_inventory_section)
  5839| 
  5840|     lines.append(
  5841|         f"\n---\n\n*Generated by `{GENERATOR_IDENTITY}` (package schema "
  5842|         f"{package_schema_version}) as a distillation of `governance_findings.json` "
  5843|         "and `governance_package_health.json` -- not an independent source.*\n"
  5844|     )
  5845|     return "\n".join(lines)
  5846| 
  5847| 
```
