# After Run C: cross-segment and governance steps

`tools/corpus_update_runbook.py` covers Run A, Run B and Run C. Nothing after
Run C is automated; the runbook only prints two reminders when it finishes
(refresh Power BI; run `compare_cross_segment.py` separately). This page lists
the manual steps that follow, in order.

Each command was assembled from the tool's own `--help` / module docstring.
Re-check a tool's `--help` if an argument is rejected.

## When to run this

Re-run from step 2 whenever Run C rebuilt one or more segments (the run
summary shows `complete: N` with N > 0). Run C only rebuilds segments whose
file population or run type changed (see `tools/build_segment_manifest.py`
`_build_registry()`), so cross-segment outputs go stale as soon as any segment
is rebuilt.

## Variables

Windows PowerShell; adjust paths to your layout. `$Records` is the folder
holding `segment_manifest.csv`, `run_registry.csv` and `file_metadata.csv`.

```powershell
$Repo     = "C:\path\to\Revit_Fingerprint"
$Data     = "C:\path\to\Fingerprint_Data"
$Records  = "$Data\records"
$Segments = "$Data\segments"
$XSeg     = "$Data\cross_segment"   # reuse your previous out-dir if Power BI points at it
$GovOut   = "$Data\governance"
Set-Location $Repo
```

## 1. Refresh Power BI

Open `Fingerprint_Segmented_Bundles.pbix` and hit Refresh to pick up Run C's
per-segment output.

## 2. Cross-segment comparison

```powershell
python tools\compare_cross_segment.py --segments-root $Segments --records-dir $Records --out-dir $XSeg
```

- With no mode flag, all modes run (within-segment, sibling, parent-sibling,
  within-project, governance chain).
- `--dry-run` prints the segment pairs without computing anything.
- `--no-delta` skips `cross_segment_delta.csv` on large corpora.
- Writes `cross_segment_summary.csv`, `cross_segment_pooled.csv`,
  `cross_segment_governance_states.csv`, `cross_segment_union_inventory.csv`,
  `comparison_registry.csv`, `pattern_reuse_*.csv`, `matrix_output_manifest.csv`
  and the `project_*_matrix.csv` / `project_fragmentation_diagnostic.csv` files
  into `$XSeg`.
- Methodology: `docs/cross_segment_comparison.md`.

## 3. Governance populations

Disjoint populations (Enterprise / business center / client / project /
Generic) built directly from `file_metadata.csv`, independent of the segment
lattice.

```powershell
python tools\governance_manifest.py            --records-dir $Records --out-dir $Records
python tools\compare_governance_populations.py --records-dir $Records --out-dir $Records
python tools\governance_relationships.py       --file-meta "$Records\file_metadata.csv" --out-dir $XSeg
```

- `governance_manifest.py` writes `governance_manifest.csv` and
  `governance_membership.csv`.
- `compare_governance_populations.py` writes `cross_governance_comparison.csv`.
- `governance_relationships.py` writes `governance_bc_client_matrix.csv` and
  `governance_client_bc_matrix.csv`, which step 4 reads.

## 4. Governance narrative and evidence package

```powershell
python tools\generate_governance_narrative.py `
  --summary                  "$XSeg\cross_segment_summary.csv" `
  --pooled                   "$XSeg\cross_segment_pooled.csv" `
  --segment-manifest         "$Records\segment_manifest.csv" `
  --governance-states        "$XSeg\cross_segment_governance_states.csv" `
  --governance-state-summary "$XSeg\cross_segment_governance_state_summary.csv" `
  --file-meta                "$Records\file_metadata.csv" `
  --union-inventory          "$XSeg\cross_segment_union_inventory.csv" `
  --comparison-registry      "$XSeg\comparison_registry.csv" `
  --reuse-distribution       "$XSeg\pattern_reuse_distribution.csv" `
  --matrix-manifest          "$XSeg\matrix_output_manifest.csv" `
  --reuse-by-client          "$XSeg\pattern_reuse_summary_by_client.csv" `
  --project-union-jaccard-matrix      "$XSeg\project_union_jaccard_matrix.csv" `
  --project-density-similarity-matrix "$XSeg\project_density_similarity_matrix.csv" `
  --project-pool-containment-matrix   "$XSeg\project_pool_containment_similarity_matrix.csv" `
  --project-fragmentation-diagnostic  "$XSeg\project_fragmentation_diagnostic.csv" `
  --governance-bc-client-matrix       "$XSeg\governance_bc_client_matrix.csv" `
  --governance-client-bc-matrix       "$XSeg\governance_client_bc_matrix.csv" `
  --out "$GovOut\governance_narrative_context.md"
```

- Only `--summary` and `--pooled` are required. Every other input is optional
  and enables an additional section; drop a line if that file doesn't exist.
- `--client-sector` defaults to the synthetic `policies/client_sector.csv`;
  pass your approved mapping for real sector labels.
- Writes `governance_narrative_context.md`, `governance_brief.md` and the
  `governance_*.json` evidence-package files into `$GovOut`, plus copies of the
  static interpretation docs from `docs/governance/`.
- Details: `docs/governance_evidence_package.md`.

## Optional follow-ups

- `tools\analyze_promotion_candidates.py` — reads step 2's
  `cross_segment_governance_states.csv` and `pattern_reuse_distribution.csv`;
  flags patterns reused more broadly than the scope at which they're already
  governed. See `--help`.
- `tools\compare_reference.py` — reference-vs-target comparison inside one
  already-materialized segment. See `docs/reference_comparison_tool.md`.
- `tools\export_bundle_pattern_detail.py` — bundle → pattern → identity-item
  detail for one segment as flat BI CSVs.
