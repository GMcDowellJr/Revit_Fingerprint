# Audit 2 — Label Synthesis Layer
Date: 2026-06-17

## Summary Table

| Item | Description | Status | Confidence |
|------|-------------|--------|------------|
| C1 | fill_patterns domain_prompts module | IMPLEMENTED | HIGH |
| C1-loader | Progressive strip loader fix | IMPLEMENTED | HIGH |
| G1-line_styles | line_styles domain_prompts | IMPLEMENTED | HIGH |
| G1-text_types | text_types domain_prompts | IMPLEMENTED | HIGH |
| G1-vfd | view_filter_definitions domain_prompts | NOT IMPLEMENTED | HIGH |
| G1-arrowheads | arrowheads domain_prompts | IMPLEMENTED | HIGH |
| G1-line_patterns | line_patterns domain_prompts | IMPLEMENTED | HIGH |
| G2 | fill pattern vocab framing fix in build_semantic_groups | NOT IMPLEMENTED | HIGH |
| G3 | Bundle label synthesis | PARTIAL | MEDIUM |
| bonus | --export-prompts / --import-results flags | IMPLEMENTED | HIGH |

## domain_prompts/ directory contents

```
tools/label_synthesis/domain_prompts/
  __init__.py
  arrowheads.py
  dimension_types.py
  fill_patterns.py
  line_patterns.py
  line_styles.py
  text_types.py
```

`view_filter_definitions.py` is **not present** in this directory.

## synopsis_formatters/ directory contents

```
tools/label_synthesis/synopsis_formatters/
  __init__.py
  arrowheads.py
  dimension_types.py
  fill_patterns.py
  line_patterns.py
  line_styles.py
  phase_filters.py
  text_types.py
```

All 7 domains from the expected set (arrowheads, dimension_types,
fill_patterns, line_patterns, line_styles, phase_filters, text_types) are
present. Nothing missing.

## Detailed Findings

### C1 — fill_patterns domain_prompts

**Status: IMPLEMENTED.** `tools/label_synthesis/domain_prompts/fill_patterns.py`:

- Lines 15–166: a full `SYSTEM_PROMPT` covering material-based, geometry-based, and role/application-based naming conventions for fill patterns.
- Lines 178–283: `build_prompt()` with substantive logic — reads `fill_pattern.target` to distinguish Model vs. Drafting, builds an observed-labels summary sorted by file count, and incorporates grid geometry (angle, offset, shift) when names are opaque.
- Lines 286–291: `_is_opaque_fallback()` helper detecting system-generated fallback names (e.g. `Fill Pattern 12`).
- Lines 294–350: geometry inference logic (angle normalization, diagonal vs. horizontal detection) used as the primary signal when the name itself is uninformative.
- Lines 91–112: a "sparse-evidence rule" that consolidates weak qualifiers (e.g. scale suffixes like "Small"/"Dense") when evidence is thin.

This satisfies both required behaviors: cross-project name normalization (AR-CONC / Concrete / CONC style fragmentation) and geometry-as-primary-signal fallback.

### C1-loader — progressive strip loader

**Status: IMPLEMENTED.** `tools/label_synthesis/synthesize_fragmented_labels.py:505-529`, function `_load_domain_prompt_module(domain)`:

```python
def _load_domain_prompt_module(domain: str):
    """Import domain prompt module, with progressive base-name fallback.

    Tries exact match first, then progressively strips trailing underscore
    segments to find a base module. Examples:
      dimension_types_linear  → dimension_types_linear, dimension_types
      fill_patterns_drafting  → fill_patterns_drafting, fill_patterns
      object_styles_model     → object_styles_model, object_styles
    """
    import importlib

    parts = domain.split("_")
    for n in range(len(parts), 0, -1):
        candidate = "_".join(parts[:n])
        try:
            return importlib.import_module(
                f"tools.label_synthesis.domain_prompts.{candidate}"
            )
        except ImportError:
            continue
    return None
```

This is exactly the progressive-strip strategy described in the audit
brief: it tries `fill_patterns_drafting` first, fails, then tries
`fill_patterns` and succeeds, so both `fill_patterns_drafting` and
`fill_patterns_model` resolve to the single `fill_patterns` module without
requiring an exact name match.

### G1 — remaining domain_prompts modules

#### line_styles — IMPLEMENTED

`tools/label_synthesis/domain_prompts/line_styles.py`:
- Lines 18–177: full `SYSTEM_PROMPT` with clustering rules.
- Lines 233–296: `build_prompt()` extracts pen weight, RGB color, pattern type, and subcategory path.
- Lines 218–230: `_fmt_color()` normalizes RGB to human-readable names (Black, Red, Gray, Cyan, etc.) — non-black color is treated as a strong semantic signal for discipline/phase markup.
- Lines 318–327: weight-to-role descriptor mapping (LW1–LW8).
- Lines 204–215: subcategory path stripping (removes `"Lines|"` prefix, handles self-referential paths).

#### text_types — IMPLEMENTED

`tools/label_synthesis/domain_prompts/text_types.py`:
- Lines 18–183: full `SYSTEM_PROMPT` with annotation-role context.
- Lines 190–253: `build_prompt()` with substantive logic.
- Line 40: font name only included when multiple font families exist in the corpus.
- Lines 288–294: size normalization — maps decimal inches to fractional notation (e.g. `0.125` → `1/8"`).
- Line 44: explicit statement that all text types are annotation-space (`text_type.size_in — Text height in inches (annotation space)`) — this directly satisfies the "model-space vs. annotation-space distinction" check, by establishing that text_types is always annotation-space.
- Bold/italic/border flags mapped to semantic roles; black vs. non-black color distinguished for special roles.

#### view_filter_definitions — NOT IMPLEMENTED

No `tools/label_synthesis/domain_prompts/view_filter_definitions.py` file exists. Glob/directory listing confirms it is absent from `domain_prompts/`.

#### arrowheads — IMPLEMENTED

`tools/label_synthesis/domain_prompts/arrowheads.py`:
- Lines 18–199: full `SYSTEM_PROMPT` covering three record classes (Arrow, Heavy end tick mark, SizeOnly).
- Lines 206–281: `build_prompt()` with `_detect_record_class()` (line 215) classifying records into Arrow/Tick/SizeOnly based on identity items, with class-specific prompt context for sparse-field cases (lines 232–238).
- Line 292: fill-state handling distinguishing filled vs. open arrows.
- Lines 334–342: size formatting that maps floats to fractions with a tolerance check (`abs(v - k) < 1e-5`).

#### line_patterns — IMPLEMENTED

`tools/label_synthesis/domain_prompts/line_patterns.py`:
- Lines 19–114: full `SYSTEM_PROMPT` describing scale-invariance doctrine and import-derived pattern handling.
- Lines 121–130, 155–173: `_strip_import_prefix()` strips `"IMPORT-"`, `"IMPORT "`, `"IMPORT_"` prefixes.
- Lines 133–143: `_is_opaque_name()` heuristic flags all-caps/numeric names with no semantic keywords, to be treated as `"unresolved-import"`.
- Lines 269–311: segment-sequence analysis parsing `line_pattern.seg[NNN].(kind|length)` into a readable "Dash-Space-Dot" description.
- Lines 39–47: explanation of why normalized segment hashing means scale differences should not split clusters.

### G2 — vocabulary framing fix

**Status: NOT IMPLEMENTED.** `tools/label_synthesis/build_semantic_groups.py`, fill-patterns prompt section `_prompt_fill_patterns()` (lines 384–476):

- Line 437–445 lists material-naming context (ANSI patterns, `AR-` prefixes, scale/application suffixes).
- Line 431 instructs the model to base grouping "primarily on the geometry (type and density)" *only* when the name is a detected fallback — this is guidance for one specific case (opaque names), not a general protection rule.
- Line 454 states grouping should "reflect the material or drawing convention" with no priority ordering between the two.

No code was found that explicitly ranks geometry-derived group names above
material-derived ones, that excludes material names from influencing
geometry-based vocabulary, or that documents this exact protection rule in
a comment/docstring. The scenario described in the audit brief — a pattern
named "Concrete" overriding the vocabulary for a geometrically distinct
"Diagonal Hatch" group — is not guarded against anywhere in this file.

### G3 — bundle label synthesis

**Status: PARTIAL.**

Currently defined `--filter-mode` values (`synthesize_fragmented_labels.py:915-926`):
```python
choices=["all", "candidates", "bundles", "governance"]
```
default is `"all"`.

`--filter-mode bundles` is a real, working code path, not a stub:
- Lines 162–166: union-bundle-mode gate (`filter_mode in ("bundles", "governance") and bool(segments_root) and bool(registry_file)`).
- Lines 208–235: `_load_governance_join_hashes()` reads `bundle_membership.csv` (single-directory mode) or performs union discovery across segments (multi-segment mode via `_collect_union_bundle_join_hashes()`, lines 53–133) and maps `pattern_id` → `join_hash`.
- Lines 236–241: for `"governance"` mode, bundle join_hashes are unioned with candidate join_hashes.

However, this only uses bundle membership to **filter which individual
join_hashes get synthesized** — synthesis output remains one label per
join_hash. There is no bundle-level aggregate label (a single label
covering an entire bundle of patterns), and no separate
`synthesize_bundle_labels.py` script exists (confirmed via repo-wide glob —
no match).

**Conclusion:** `--filter-mode bundles` is implemented and functional, but
it is bundle-*scoped* synthesis, not bundle-*level* label synthesis as
described in the G3 backlog item. This is graded PARTIAL rather than
IMPLEMENTED because the actual deliverable — a label that describes a
bundle as a whole — does not exist.

### Bonus — export-prompts / import-results flags

**Status: IMPLEMENTED.** `tools/label_synthesis/synthesize_fragmented_labels.py`:

`--export-prompts` (argparse at lines 900–903; logic at lines 703–727):
```python
if export_prompts:
    prompt_exports = []
    for jh in to_process:
        rows = label_pop_by_hash.get(jh, [])
        rows_sorted = sorted(rows, key=lambda r: -int(r.get("files_count", 0)))
        identity_items = _load_representative_identity_items(
            exports_dir, domain, jh, items_lookup=items_lookup
        )
        user_prompt = build_prompt_fn(
            join_hash=jh,
            observed_labels=rows_sorted,
            identity_items=identity_items,
        )
        prompt_exports.append({
            "join_hash": jh,
            "domain": domain,
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
        })
    os.makedirs(os.path.dirname(os.path.abspath(export_prompts)), exist_ok=True)
    with open(export_prompts, "w", encoding="utf-8") as f:
        json.dump(prompt_exports, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print(f"  Exported {len(prompt_exports)} prompts → {export_prompts}")
    return
```
Assembles full system+user prompt pairs per join_hash and writes them as a JSON array — real, working logic, not a placeholder.

`--import-results` (argparse at lines 905–908; logic at lines 615–631):
```python
if import_results:
    with open(import_results, "r", encoding="utf-8") as f:
        imported_entries = json.load(f)
    for entry in imported_entries:
        join_hash = entry["join_hash"]
        cache[join_hash] = {
            "domain": domain,
            "recommended": entry["recommended"],
            "candidates": entry.get("candidates", [entry["recommended"]]),
            "rationale": entry.get("rationale", ""),
            "reviewed": False,
            "generated_at": date.today().isoformat(),
            "source": "import",
        }
    save_llm_cache(cache_path, cache)
    print(f"  Imported {len(imported_entries)} results → cache written to {cache_path}")
    return
```
Reads an offline-produced JSON results file and merges entries into the LLM cache keyed by join_hash, then persists the cache — also real, working logic.

Mutual exclusivity / guard rails (lines 982–985):
```python
if args.export_prompts and args.import_results:
    ap.error("--export-prompts and --import-results are mutually exclusive.")
if args.dry_run and (args.export_prompts or args.import_results):
    ap.error("--dry-run cannot be combined with --export-prompts or --import-results.")
```

Both flags are fully implemented with functioning logic and correct guard conditions — this gap from April 2026 has been closed.

## Files Not Found

- `tools/label_synthesis/domain_prompts/view_filter_definitions.py` — does not exist (G1-vfd).
- `tools/label_synthesis/synthesize_bundle_labels.py` — does not exist; no dedicated bundle-label-synthesis script (relevant to G3).
