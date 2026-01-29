# Deprecated / Legacy Tools (tools/)

Status date: 2026-01-29  
Scope: `tools/` only (external entrypoints / CLIs).  
Default assumption: split export surfaces exist (`*.index.json`, `*.details.json`), and legacy is opt-in (`*.legacy.json`).

---

## Deprecation rules used here

A tool is marked **DEPRECATED** if any of these are true:

- It **recursively globs `**/*.json`** (or broadly globs `*.json`) and therefore:
  - unintentionally ingests `*.index.json` and `*.legacy.json`, or
  - double-counts `index+details`, or
  - treats `index` as empty records and poisons analysis.
- It is **superseded** by Phase-1/Phase-2 runners + flat tables.
- It is an **example / one-off probe** and should not be depended on in production workflows.

A tool is marked **KEEP (Docs-only)** if it’s useful as an example but not as an operational entrypoint.

---

## DEPRECATED

### tools/phase1_semantic_sig_dimension_types.py
**Why deprecated**
- Broad JSON discovery patterns (often `**/*.json` style) are incompatible with split exports.
- Functionally superseded by Phase-2 population + candidate join-key simulation (and flat tables if needed).

**Replacement**
- `python -m tools.phase2_analysis.run_joinhash_label_population ...`
- `python -m tools.phase2_analysis.run_joinhash_parameter_population ...`
- `python -m tools.phase2_analysis.run_candidate_joinkey_simulation ...`
- `python tools/export_to_flat_tables.py ...` (when you need CSV-level analysis)

---

## CONDITIONAL / OPTIONAL (use only if you explicitly need it)

### tools/details_to_csv.py
**Why optional**
- Useful if you still consume similarity/compare JSON outputs and want quick CSV conversion.
- Not required for the Phase-2 pipeline if you already use `export_to_flat_tables.py`.

**Prefer**
- `tools/export_to_flat_tables.py` for standardized CSV surfaces.

---

## KEEP (Docs-only / Example)

### tools/example_use_split_export.py
**Why docs-only**
- Not a production dependency; keep as “how to invoke split export correctly”.
- Should not be referenced as an operational entrypoint in pipelines.

---

## Niche probes (keep only if intentionally used)

### tools/probes/probe_arrowheads.py
**Why niche**
- Domain-specific probe; not part of standard Phase-0/1/2 workflow.
- Keep only if you’re still actively probing arrowhead-related identity behavior.

---

## Notes / Warnings

- Any tool that globs `*.json` without preference ordering is considered **unsafe** under split exports unless patched to:
  1) prefer `*.details.json`,
  2) then `*.index.json`,
  3) and ignore `*.legacy.json` unless explicitly requested.

If you want, I can add a short “Deprecation Banner” to the deprecated scripts (single stderr warning + exit code 2 unless `--force`) but that *is* a behavior change, so I did not propose it under current constraints.
