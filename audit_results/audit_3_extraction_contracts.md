# Audit 3 — Extraction Layer & Contracts
Date: 2026-06-17

Note on file locations: the task brief assumed extractors live under
`extractors/domains/`. Per this repo's actual layout,
domain extractors live at the repo root under `domains/`. Wall types are a
partition inside the consolidated `domains/compound_types.py` extractor
(per D-015 domain family architecture), not a standalone `wall_types.py`
file. All findings below cite the verified actual paths.

## Summary Table

| Item | Description | Status | Confidence |
|------|-------------|--------|------------|
| E1a | material.class in contract | NOT IMPLEMENTED | HIGH |
| E1b | material.class + sig_hash in extractor | PARTIAL | HIGH |
| E2a | join_hash = md5(gsh\|keynote) in contract | NOT IMPLEMENTED | HIGH |
| E2b | keynote as identity_item in extractor | PARTIAL | HIGH |
| E3 | Materials latent purgeable chain / synthetic stage | PARTIAL | HIGH |
| E4 | wt.cfpsh in wall_types extractor | IMPLEMENTED | HIGH |
| E5 | FilterInverseRule handled in VFD extractor | IMPLEMENTED | HIGH |
| I5 | fill_patterns is_import flag | NOT IMPLEMENTED | HIGH |
| I6 | line_styles pattern synopsis as coordination_item | NOT IMPLEMENTED | HIGH |

## Materials domain — current contract state

`contracts/domain_identity_keys_v2.json:517-533`:

```json
"materials": {
  "domain_family": "materials",
  "allowed_keys": [
    "material.graphics_sig_hash_v2",
    "material.uid"
  ],
  "required_keys": [
    "material.graphics_sig_hash_v2"
  ],
  "minima": {
    "block_if_any_required_not_ok": true
  },
  "notes": [
    "sig_hash anchors on material.graphics_sig_hash_v2 (visual/material properties).",
    "material.uid is for record traceability only — not a sig or join anchor.",
    "Consistent with domain_join_key_policies.json which requires graphics_sig_hash_v2."
  ]
}
```

`material.class` and `material.keynote` are absent from `allowed_keys`.
No `join_hash` composition rule of any kind appears in this section — the
sig_hash basis is still purely `material.graphics_sig_hash_v2`, not
name+class.

## Wall types domain — current contract state

`contracts/domain_identity_keys_v2.json:913-956` (key `wall_types`, domain_family `compound_types`):

```json
"wall_types": {
  "domain_family": "compound_types",
  "allowed_keys": [
    "wt.function",
    "wt.layer_count",
    "wt.total_thickness_in",
    "wt.stack_hash_loose",
    "wt.wraps_at_inserts",
    "wt.wraps_at_ends",
    "wt.kind",
    "wt.total_layer_rows",
    "wt.stack_hash_strict",
    "wt.stack_hash_function_only",
    "wt.coarse_fill_pattern_sig_hash",
    "wt.has_embedded_sweeps",
    "wt.type_name",
    "wt.coarse_fill_color_rgb"
  ],
  "required_keys": [
    "wt.layer_count",
    "wt.total_thickness_in",
    "wt.stack_hash_loose"
  ],
  "optional": [
    "wt.wraps_at_inserts",
    "wt.wraps_at_ends",
    "wt.kind",
    "wt.total_layer_rows",
    "wt.stack_hash_strict",
    "wt.stack_hash_function_only",
    "wt.coarse_fill_pattern_sig_hash",
    "wt.has_embedded_sweeps",
    "wt.type_name",
    "wt.coarse_fill_color_rgb"
  ]
}
```

`wt.coarse_fill_pattern_sig_hash` (the field referred to as `wt.cfpsh` in
the backlog item) is listed both in `allowed_keys` (line 926) and
`optional` (line 948) — it is implemented but not required.

## Detailed Findings

### E1 — material.class identity_item + sig_hash revision

**Part A (contract) — NOT IMPLEMENTED.** As shown above, `material.class`
does not appear anywhere in the materials contract section
(`contracts/domain_identity_keys_v2.json:517-533`). The sig_hash basis
remains `material.graphics_sig_hash_v2` exclusively — there is no
provision for a name+class basis (`md5(name|class)`).

**Part B (extractor) — PARTIAL.** `domains/materials.py`:
- Line 311 reads the class value: `mat_class = canon_str(_read_prop(m, "MaterialClass"))`.
- Line 418 stores it in the payload dict: `"class": mat_class`.
- However it is **not** added to `identity_items` or to `sig_basis_items`.
- `sig_basis_items` (lines 396-407) is built only from graphics properties (shading color/transparency, surface/cut pattern sig hashes and colors) — no name, no class.
- The sig_hash is computed purely from this graphics basis at lines 443-450:
  ```python
  sig_basis_items_sorted = sorted(sig_basis_items, key=lambda it: safe_str(it.get("k", "")))
  graphics_sig_hash_v2 = make_hash(serialize_identity_items(sig_basis_items_sorted))
  identity_items.append(
      make_identity_item("material.graphics_sig_hash_v2", graphics_sig_hash_v2, ITEM_Q_OK)
  )
  ```

**Verdict:** The data needed (class) is already read and stored in the
payload, but the actual semantic change — making `material.class` an
identity_item and revising sig_hash to `name+class` — has not happened in
either the contract or the extractor. Graded PARTIAL only because the raw
field is present in the payload; the contract side is flatly NOT
IMPLEMENTED.

### E2 — materials join_hash composition

**Part A (contract) — NOT IMPLEMENTED.** No `join_hash`, no `keynote`
reference anywhere in the materials contract section.

**Part B (extractor) — PARTIAL.** `domains/materials.py`:
- Lines 353-357 read keynote: `keynote = _read_param_as_string(m, bip_names=["KEYNOTE_PARAM"], lookup_names=["Keynote"])`.
- Line 426 stores it in the payload: `"keynote": keynote[0]`.
- Keynote is **not** added to `identity_items`, and there is no `join_hash` field assembled anywhere from `graphics_sig_hash_v2 | keynote`.

**Verdict:** Same pattern as E1 — keynote is already captured in the raw
payload, but the join_hash composition rule itself does not exist in
either the contract or the extractor.

### E3 — materials latent purgeable chain

**Status: PARTIAL.**

The synthetic migration script does exist:
`tools/migration/migrate_materials_identity_items.py` (full file, ~300
lines). It:
- Loads an existing fingerprint JSON export (compact or indented).
- Iterates materials records.
- Checks whether `material.graphics_sig_hash_v2` is already present in `identity_basis.items` (lines 124-126).
- If missing, injects it: `items.append({"k": "material.graphics_sig_hash_v2", "q": "ok", "v": str(sig_value)})` (line 128), sourcing the value from `rec.get("graphics_sig_hash_v2") or rec.get("sig_hash")`.
- Re-sorts items by key (line 129) and supports `--in-place`, `--out-dir`, `--dry-run`.

This is exactly the "synthetic stage that injects a new identity_item into
existing JSON exports without re-extraction" pattern described in the
backlog item — but it injects `material.graphics_sig_hash_v2` (the
existing field), **not** `material.name_class_hash` / `material.class`,
since that field doesn't exist yet per E1.

The chain-config half is explicitly **not** wired up.
`tools/compute_latent_purgeable.py:57-59` contains:
```python
# Deferred (not implemented): materials ← object_styles_model
# The obj_style.material_sig_hash and materials sig_hash live in different
# hash spaces; fix requires ctx lookup in object_styles + re-extraction.
```
The chain list (lines 62-127) currently only covers `arrowheads`,
`line_patterns`, `phase_filters`, and `view_filter_definitions` —
`materials` is not among the configured `target_domain` entries, and no
`consumer_domains=[object_styles_model]` / `ref_item_keys=[obj_style.material_sig_hash]`
entry exists.

**Verdict: PARTIAL** — the migration-script *pattern* exists and is
functional (for the existing graphics_sig_hash_v2 field), but the actual
materials↔object_styles_model latent purgeable chain referenced in the
backlog item is explicitly deferred and not configured.

### E4 — wt.cfpsh in wall_types

**Status: IMPLEMENTED.**

`domains/compound_types.py`:
- Line 624: `cfpsh_v, cfpsh_q, cfc_v, cfc_q = _coarse_fill_reads(wt, doc, fp_uid_to_sig_hash, ctx)`.
- Lines 735-805 define `_coarse_fill_reads()`, which reads `BuiltInParameter.COARSE_SCALE_FILL_PATTERN_ID_FOR_LEGEND` (falling back to `LookupParameter("Coarse Scale Fill Pattern")`).
- Line 653 emits it as an identity item: `make_identity_item("wt.coarse_fill_pattern_sig_hash", cfpsh_v, cfpsh_q)`.
- Line 658 also emits the coarse fill color: `make_identity_item("wt.coarse_fill_color_rgb", cfc_v, cfc_q)`.

Contract confirms both `wt.coarse_fill_pattern_sig_hash` and
`wt.coarse_fill_color_rgb` are listed in `allowed_keys`/`optional`
(`contracts/domain_identity_keys_v2.json:926, 935, 948`).

This satisfies the cross-domain join requirement for archetype detection
approach B (coarse-scale wall type override ↔ fill_patterns).

### E5 — FilterInverseRule

**Status: IMPLEMENTED.**

`domains/view_filter_definitions.py`, function `_walk_rules()`
(lines 175-232) and helper `_append_rule()` (lines 184-195) both contain
explicit `FilterInverseRule` handling, not a fallthrough:

```python
def _append_rule(rule_obj, prefix: str) -> Tuple[bool, Optional[str]]:
    """Append a rule, unwrapping FilterInverseRule wrappers into NOT.-prefixed inner rule."""
    try:
        if FilterInverseRule is not None and isinstance(rule_obj, FilterInverseRule):
            inner_rule = rule_obj.GetInnerRule() if hasattr(rule_obj, "GetInnerRule") else None
            if inner_rule is None:
                return False, "filter_tree.rules_unreadable"
            return _append_rule(inner_rule, "{}NOT.".format(prefix))
        out_rules.append({"rule": rule_obj, "prefix": prefix})
        return True, None
    except Exception:
        return False, "filter_tree.rules_unreadable"
```

And in `_walk_rules()` (lines 209-217), the same unwrap-and-recurse logic
appears before the leaf dispatch:

```python
# Inverse wrapper: NOT(inner_rule)
try:
    if FilterInverseRule is not None and isinstance(elem_filter, FilterInverseRule):
        inner_rule = elem_filter.GetInnerRule() if hasattr(elem_filter, "GetInnerRule") else None
        if inner_rule is None:
            return False, "filter_tree.rules_unreadable"
        return _append_rule(inner_rule, "{}NOT.".format(rule_prefix))
except Exception:
    return False, "filter_tree.rules_unreadable"
```

`FilterInverseRule` is detected via `isinstance`, its inner rule extracted
via `GetInnerRule()`, and re-dispatched with a `NOT.`-prefixed key — it
never reaches the unhandled-leaf path. `"filter_tree.leaf_unsupported"`
(line 232) is reached only for genuinely unrecognized rule/filter types,
not for `FilterInverseRule`.

### I5 — fill_patterns is_import flag

**Status: IMPLEMENTED (deferred until next Dynamo re-extraction).**

Contract (`contracts/domain_identity_keys_v2.json`, both
`fill_patterns_drafting` and `fill_patterns_model`): `allowed_keys` now
includes `fill_pattern.is_import` alongside target, grid count, and grid
definition hash fields, plus the `fill_pattern.grid[` prefix.

Extractor (`domains/fill_patterns.py`, both `extract_drafting`
and `extract_model`): the `_phase2_build_phase2()` payload builder constructs
`semantic`, `cosmetic`, `coordination`, and `unknown` item buckets. The extractor now emits `fill_pattern.is_import` as a coordination
item for both drafting and model fill patterns, using any direct API flag that
is exposed and otherwise falling back to common PAT-style name/category
heuristics. Existing materialized corpora require the next Dynamo
re-extraction cycle to populate this field.

### I6 — line_styles pattern synopsis as coordination_item

**Status: IMPLEMENTED (deferred until next Dynamo re-extraction).**

Contract (`contracts/domain_identity_keys_v2.json`): `allowed_keys` now
includes `line_style.pattern_ref.synopsis` alongside the existing projection
weight, color, cut-weight, and pattern sig-hash fields.

Extractor (`domains/line_styles.py`): pattern reference resolution still
records `pattern_ref.kind` ("solid"/"ref") and `pattern_ref.sig_hash`, and now
also resolves a human-readable segment-sequence synopsis for the referenced
line pattern.

`coordination_items` now emits `line_style.pattern_ref.synopsis` at extraction
time. The extractor prefers context-provided line pattern synopsis maps when
available and falls back to reading the referenced pattern element's segment
sequence directly, preserving the "Option B" approach instead of relying on a
runtime cross-domain join. Existing materialized corpora require the next
Dynamo re-extraction cycle to populate this field.

### migrate_materials_identity_items.py

**Exists:** `tools/migration/migrate_materials_identity_items.py` (note:
under a `tools/migration/` subdirectory, not directly under `tools/`).

It is a synthetic post-extraction migration script that:
- Reads an existing fingerprint JSON export (compact or indented format).
- Iterates over materials records.
- For each record, checks `identity_basis.items` for an existing
  `material.graphics_sig_hash_v2` entry; if absent, injects one sourced
  from `rec.get("graphics_sig_hash_v2") or rec.get("sig_hash")`, with
  `q: "ok"`.
- Re-sorts `identity_basis.items` by key after injection.
- Supports `--in-place`, `--out-dir`, and `--dry-run` modes for safe
  application across a corpus of existing exports.

This establishes the pattern E3 was looking for, but it currently targets
the already-existing `graphics_sig_hash_v2` field rather than a new
`material.class` / `name_class_hash` field (which doesn't exist yet, per
E1).

## Files Not Found

None — every file referenced in the audit brief and Step 0 inventory was
located, though several were at different paths than the brief assumed
(`domains/` instead of `extractors/domains/`; wall_types as a partition
inside `domains/compound_types.py` rather than a standalone file;
`migrate_materials_identity_items.py` under `tools/migration/` rather than
directly under `tools/`).
