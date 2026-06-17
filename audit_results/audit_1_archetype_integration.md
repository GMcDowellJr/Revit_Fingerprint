# Audit 1 — Archetype Pipeline + Pipeline Integration
Date: 2026-06-17

## Summary Table

| Item | Description | Status | Confidence |
|------|-------------|--------|------------|
| A1 | vfd_bip_target_domain_hints exact_bip_id | NOT IMPLEMENTED | HIGH |
| A2-Fix1 | Fill pattern edge collapse | IMPLEMENTED | HIGH |
| A2-Fix2 | Dim type variant collapse | IMPLEMENTED | HIGH |
| A2-Fix3 | Edge pair scope restriction | IMPLEMENTED | HIGH |
| A2-Fix4 | Signal stub join_hash population | IMPLEMENTED | HIGH |
| A3 | signals_fired columns in assign_archetype | IMPLEMENTED | HIGH |
| B1 | export_bundle_pattern_detail in Run C | NOT IMPLEMENTED (as specified) | HIGH |
| C2 | union discovery mode in synthesize | IMPLEMENTED | HIGH |
| C3a | label_refresh_runbook.ps1 exists | IMPLEMENTED (file exists) | HIGH |
| C3b | synthesis removed from corpus_update_runbook | IMPLEMENTED (removed) | HIGH |

## Detailed Findings

### A1 — VFD BIP target domain hints

**File:** `tools/archetype/vfd_bip_target_domain_hints.json`, line 8

```json
"exact_bip_id": {},
```

The `exact_bip_id` key exists but is an **empty dict**. The required entry
`"bip:-1001007": "wall_types"` is absent. No corpus-validated BIP overrides
have been added yet — only the schema scaffold and a docstring note
(lines 2–6) explaining that exact BIP ids take precedence over
`name_contains` rules and that high-confidence overrides should be added
"as they are validated from corpus evidence."

**Verdict: PARTIAL per the literal "key exists but is empty" criterion →
classified here as NOT IMPLEMENTED** since there is zero population, which
is the substantive ask of A1.

**Additional check — `vfd_domain_gaps.csv`:** Not found anywhere in the
repository (no `Fingerprint_Out/` output committed, and no copy elsewhere).
Cannot assess `target_domain_source` distribution; this is an output
artifact that doesn't appear to be checked in, so the question is
unanswerable from the repo alone.

### A2 — compute_cross_domain_cooccurrence.py fixes

#### Fix 1 — Fill pattern edge collapse — IMPLEMENTED

`tools/archetype/_common.py:131-149`, function `build_edge_aliases()`:

```python
# Pass 1: fill_patterns_drafting / fill_patterns_model collapse.
fill_groups: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)
for edge_id, edge in edges_by_id.items():
    prefix = strip_partition_suffix(edge.get("target_domain", ""))
    ...
for edge_ids in fill_groups.values():
    if len(edge_ids) < 2:
        continue
    canonical = next(
        (e for e in edge_ids if edges_by_id[e].get("target_domain", "").endswith("_drafting")),
        sorted(edge_ids)[0],
    )
    for e in edge_ids:
        if e != canonical:
            alias_of[e] = canonical
```

Invoked from `compute_cross_domain_cooccurrence.py:154` (`alias_of, collapsed = build_edge_aliases(edges_by_id)`) and applied at line 176 (`canonical_edge_id = alias_of.get(edge_id, edge_id)`) before pair enumeration.

#### Fix 2 — Dimension type variant collapse — IMPLEMENTED

`tools/archetype/_common.py:151-170`, Pass 2 of `build_edge_aliases()`:

```python
# Pass 2: dimension_types_{variant} tick_mark collapse (skip edges already aliased).
dim_groups: Dict[Tuple[str, str], List[str]] = defaultdict(list)
for edge_id, edge in edges_by_id.items():
    if edge_id in alias_of:
        continue
    source_domain = edge.get("source_domain", "")
    if any(source_domain == f"dimension_types_{variant}" for variant in DIM_TYPE_VARIANTS):
        dim_groups[(edge.get("source_field", ""), edge.get("target_domain", ""))].append(edge_id)
...
    canonical = next(
        (e for e in edge_ids if edges_by_id[e].get("source_domain") == "dimension_types_linear"),
        sorted(edge_ids)[0],
    )
```

Groups linear/angular/radial/diameter variants sharing `(source_field, target_domain)` onto a `dimension_types_linear` representative.

#### Fix 3 — Edge pair scope restriction — IMPLEMENTED

`tools/archetype/compute_cross_domain_cooccurrence.py:110-128`, function `_eligibility_reason()`:

```python
if target_a and target_a == target_b:
    return "shared_target"
if (target_a and target_a == source_b) or (target_b and target_b == source_a):
    return "chain"
if (edge_id_a, edge_id_b) in whitelist_pairs or (edge_id_b, edge_id_a) in whitelist_pairs:
    return "whitelist"
return None
```

Enforced at lines 200-207 during pair enumeration:

```python
for edge_id_a, edge_id_b in combinations(canonical_edge_ids, 2):
    ...
    reason = _eligibility_reason(edge_a, edge_b, edge_id_a, edge_id_b, whitelist_pairs)
    if reason is None:
        n_skipped_ineligible += 1
        continue
```

This restricts the cross-product to `shared_target`, `chain`, or explicit `whitelist` pairs, preventing all-vs-all enumeration (e.g. `dimension_types_tick_mark × VCO_model.baseline` would be skipped unless one of these three relations holds).

#### Fix 4 — Signal stub join_hash population — IMPLEMENTED

`tools/archetype/generate_archetype_candidates.py:220-231` (reads `cross_domain_patterns.csv` and selects top pairs by `file_count`):

```python
sorted_rows = sorted(rows, key=lambda r: int(float(r.get("file_count") or 0)), reverse=True)
top_join_hash_pairs = [
    {
        "join_hash_a": r.get("join_hash_a", ""),
        "join_hash_b": r.get("join_hash_b", ""),
        "file_count": int(float(r.get("file_count") or 0)),
    }
    for r in sorted_rows[: args.top_n_join_hash_pairs]
]
top_row = sorted_rows[0] if sorted_rows else {}
top_join_hash_a = top_row.get("join_hash_a", "")
top_join_hash_b = top_row.get("join_hash_b", "")
```

`generate_archetype_candidates.py:267-280` (assignment into signal stub dicts):

```python
{
    "signal_id": edge_id_a,
    ...
    "join_hash": top_join_hash_a or None,
    "join_hash_populated": bool(top_join_hash_a),
    ...
},
{
    "signal_id": edge_id_b,
    ...
    "join_hash": top_join_hash_b or None,
    "join_hash_populated": bool(top_join_hash_b),
    ...
}
```

### A3 — assign_archetype_classifications.py columns

**IMPLEMENTED.** `tools/archetype/assign_archetype_classifications.py:83-103`, `CLASSIFICATIONS_FIELDS` list includes both:

```python
"signals_fired_join_hashes",
"signals_fired_labels",
```

`human_label` is read via a `DomainPatternLabelCache` that falls back through `pattern_label_human` / `human_label` / `pattern_label` (lines 144-149):

```python
label = (
    row.get("pattern_label_human", "")
    or row.get("human_label", "")
    or row.get("pattern_label", "")
)
self._label_cache[(domain, join_hash)] = label
```

Populated into output rows at lines 396-399 and 420-421:

```python
signals_fired_labels: Dict[str, str] = {}
for sid in signals_fired:
    source_join_hash, source_domain = signals_fired_sources[sid]
    signals_fired_labels[sid] = label_cache.get(source_domain, source_join_hash)
...
"signals_fired_join_hashes": SIGNAL_LIST_SEPARATOR.join(signals_fired_sources[sid][0] for sid in signals_fired),
"signals_fired_labels": SIGNAL_LIST_SEPARATOR.join(signals_fired_labels[sid] for sid in signals_fired),
```

### B1 — export_bundle_pattern_detail.py in Run C orchestrator

**NOT IMPLEMENTED per the item's defined scope (Run C step C4).**

`export_bundle_pattern_detail.py` is invoked, but **not** from
`run_segment_orchestrator.py` or from the Run C section of
`tools/corpus_update_runbook.ps1`. It is invoked from a separate file,
`tools/label_refresh_runbook.ps1`, lines 91-96, as step "L4" of a distinct
post-corpus-update label-refresh workflow:

```powershell
python tools\export_bundle_pattern_detail.py `
    --output-folder $seg.output_folder `
    --segments-root $SEGMENTS `
    --records-dir   $RECORDS `
    --purge-view    $view `
    --out-dir       $outDir
```

A repo-wide grep for `bundle_pattern_detail` confirms no other references
(no hit in `run_segment_orchestrator.py` or in `corpus_update_runbook.ps1`).
Since the item explicitly asks whether this script was wired into **Run C
as step C4** (i.e., the corpus-update orchestrator), and that integration
point does not contain the call, this item is **NOT IMPLEMENTED as
specified** — even though the script has since been wired into a different,
adjacent runbook (`label_refresh_runbook.ps1`) that runs after Run C
completes. This may represent an intentional architecture decision (label
refresh as its own phase) rather than an oversight, but it does not satisfy
the literal B1/C4 criterion.

### C2 — union discovery mode

**IMPLEMENTED.** `tools/label_synthesis/synthesize_fragmented_labels.py`:

- `--segments-root` flag, lines 938-950 (argparse), with help text describing union bundle discovery across segments and purge views.
- `--registry-file` flag, lines 951-958 (argparse), documented as required alongside `--segments-root`.
- Union-walk logic in `_collect_union_bundle_join_hashes()`, lines 53-133: iterates `Path(segments_root)`, reads `run_registry.csv` to enumerate active segments (`run_type=bundle|reference`, `status != skip|registration`), and unions `join_hash` membership across segments and both purge views (`all`, `used`).
- Activation gate, lines 162-166:
  ```python
  union_bundle_mode = (
      filter_mode in ("bundles", "governance")
      and bool(segments_root)
      and bool(registry_file)
  )
  ```
- Mutual exclusivity enforced in argparse validation, lines 986-998:
  ```python
  if args.filter_mode in ("bundles", "governance"):
      has_single = bool(args.bundle_dir)
      has_union = bool(args.segments_root) and bool(args.registry_file)
      if not has_single and not has_union:
          ap.error(...)
  ```

No stubs, `TODO`s, or `NotImplementedError`s were found in the union code path — logic appears complete and is actively used (see C3 below).

### C3 — label_refresh_runbook.ps1 + runbook cleanup

**Check A — file existence: FOUND.**
`tools/label_refresh_runbook.ps1` exists (not under a dedicated `runbooks/`
directory — runbooks live directly under `tools/`). It implements steps
L1 through L4, including the L2 synthesis call and the L4
`export_bundle_pattern_detail.py` call referenced above.

**Check B — synthesis still invoked in the main corpus-update runbook?**
NO — a grep for `synthesize_fragmented_labels` in
`tools/corpus_update_runbook.ps1` returns **zero matches**. Synthesis has
been moved entirely into `label_refresh_runbook.ps1`, where it's invoked
with union-mode flags (lines 35-64):

```powershell
$params = @(
    "--exports-dir",           $EXPORTS,
    "--analysis-dir",          "$RESULTS\label_synthesis",
    "--domain",                $dom,
    "--cache",                 $CACHE,
    "--identity-items-lookup", $LOOKUP,
    "--domain-patterns-csv",   $DP_CSV,
    "--provider",              "openrouter",
    "--filter-mode",           "bundles",
    "--segments-root",         $SEGMENTS,
    "--registry-file",         "$RECORDS\run_registry.csv",
    "--workers",               "8"
)
python -m tools.label_synthesis.synthesize_fragmented_labels @params
```

**Verdict:** Item implemented — synthesis has been removed from the main
corpus-update runbook and consolidated into the dedicated label-refresh
runbook, consistent with C2's union-discovery design.

## Files Not Found

- `tools/archetype/reference_graph.json` — not present in the repository (likely a generated output artifact rather than a committed source file; `generate_reference_graph.py` exists as the generator).
- `Fingerprint_Out/vfd_domain_gaps.csv` (or any copy elsewhere in the repo) — not found; could not verify `target_domain_source` distribution for A1.
- A dedicated `runbooks/` directory does not exist — both runbooks (`corpus_update_runbook.ps1`, `label_refresh_runbook.ps1`) live at `tools/`.
