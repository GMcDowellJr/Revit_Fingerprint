# Audit 6 — Step 0-within-PR1: Per-Domain `label.display` Item Mapping and Final Eligibility List

Date: 2026-07-23
Scope: Read-only, pre-implementation confirmation for PR1 (Canonical Name Identity Projection). Every domain file that emits `label.display` was read at its label-construction site to determine which flat `identity_items` key (if any) actually backs that value — not merely whether the value exists as a *label*, but whether it exists as its own addressable key in the `identity_items` list that `core/join_key_builder.build_join_key_from_policy()` operates on. This distinction turned out to matter: `label.display` is frequently built from a raw Python variable (an element's `.Name`) that is **never** turned into its own `identity_items` entry — it only reaches a phase2 bucket (`cosmetic_items`/`coordination_items`/`unknown_items`), or nothing at all, in which case `build_join_key_from_policy()` cannot see it regardless of what the join-key policy JSON's `explicitly_excluded_items` list claims.

**Method**: for each domain, the label-construction call site (`label={"display": ...}`) was read, the source variable was traced backward to its origin, and then a targeted grep confirmed whether `make_identity_item("<domain>.<key>", <that same value>, ...)` is ever appended to the `identity_items` list passed to `build_record_v2`/`build_join_key_from_policy` at that call site.

---

## Result: only 7 of 37 policy-file entries have a usable own-name identity item

| Domain / partition | `label.display` source variable | Is that value ALSO a standalone key in `identity_items`? | Verdict |
|---|---|---|---|
| **phases** | `name` (`phase.Name`) | **Yes** — `phase.name`, `domains/phases.py:230` | **ELIGIBLE** (redundant with `join_hash` — phases' sole required join item, D-010) |
| **materials** | `name` | **Yes** — `material.name`, `domains/materials.py:416` | **ELIGIBLE** |
| **text_types** | `type_name` | **Yes** — `text_type.name`, `domains/text_types.py:485` | **ELIGIBLE** |
| **wall_types** (compound_types.py) | `type_name` | **Yes** — `wt.type_name`, `domains/compound_types.py:552/657` | **ELIGIBLE** (no existing join-key call site in this file — see §3) |
| **floor_types** | `type_name` | **Yes** — `ft.type_name`, `domains/compound_types.py:875/941` | **ELIGIBLE** (same caveat) |
| **roof_types** | `type_name` | **Yes** — `rt.type_name`, `domains/compound_types.py:1060/1108` | **ELIGIBLE** (same caveat) |
| **ceiling_types** | `type_name` | **Yes** — `ct.type_name`, `domains/compound_types.py:1227/1275` | **ELIGIBLE** (same caveat) |
| identity | `info.get("project_title")` | **No** — `identity.project_title` exists only in `phase2.unknown_items` (`domains/identity.py:164`); never appended to the `identity_items` list passed to `build_record_v2`/`build_join_key_from_policy` (that list is built at lines 182-193 and contains only `is_workshared`/`revit_version_number`/`revit_version_name`/`revit_build`) | **EXCLUDED** — no addressable item |
| phase_filters | `name` (`PhaseFilter.Name`) | **No** — `phase_filter.name` exists only in `phase2_coordination_items` (`domains/phase_filters.py:242`); the policy file's own notes confirm this explicitly: *"phase_filter.name remains a phase2 coordination/label field only... replaces the provisional name-only policy, which no longer matched the exported identity_items surface."* | **EXCLUDED** — no addressable item |
| line_patterns | `getattr(e, "Name", None)` | **No** — `line_pattern.name` exists only in `cosmetic_items` (`domains/line_patterns.py:408-413`); not in the `identity_items_sorted` list used for hashing/join | **EXCLUDED** — no addressable item |
| fill_patterns (both partitions) | raw `name` | **No** — `fill_pattern.name` exists only in the `cosmetic` bucket (`domains/fill_patterns.py:408`, `:1293`), never in `identity_items` | **EXCLUDED** — no addressable item |
| arrowheads | `nm` (`get_type_display_name(t)`) | **No** — no `arrowhead.name` (or equivalent) item is ever constructed anywhere in the file; `nm` feeds only `label.display`/`label.components.type_name`. Code comment at `domains/arrowheads.py:518`: *"Label is not identity; keep human if possible."* The join-key policy's `explicitly_excluded_items: ["arrowhead.name", ...]` is documentation of intent, not a real key — no such item is ever emitted. | **EXCLUDED** — no addressable item |
| loaded_family_types | `fam_name_v` (`Family.Name`) | **No** — explicitly, deliberately never added. Code comment at `domains/loaded_family_types.py:317-318`: *"family_name is label-only — excluded so sig_hash is name-independent and two families with the same behavioral definition compare equal."* | **EXCLUDED** — no addressable item (by design) |
| view_templates (all 5 partitions) | `name` | **No** — `identity_items` for every partition come from `_canonical_identity_items_from_signature(def_hash, sig_final)` (e.g. `domains/view_templates.py:668`), a structured def-hash signature that explicitly strips `"name="`-prefixed entries before hashing (`view_templates.py:659`). No `view_template.name` key ever reaches `identity_items`, despite being listed in the join-key policy's `explicitly_excluded_items` (documentation only, not a real key). | **EXCLUDED** — no addressable item |
| view_filter_definitions | `name_v` (synthetic label: `"View Filter Definition ({name})".format(...)`) | **No** — `identity_items` = `vf.categories`, `vf.logic_root`, `vf.rule_count`, `vf.def_hash` only (`domains/view_filter_definitions.py:320-435`). The join-key policy lists `label.components.name` as excluded, confirming a name value exists *inside the nested `label` dict* (`domains/view_filter_definitions.py:280`) — but `build_join_key_from_policy()` only ever looks up flat `identity_items` keys by `k`, and cannot address a path inside the `label` dict at all. There is no flat item to reference. | **EXCLUDED** — no addressable item (mechanism can't reach `label.*` even if desired) |
| view_filter_applications_view_templates (vfa) | synthetic label built from the referenced view template's `Name` | **Partial / unsuitable** — `vfa.template_uid_or_namekey` *is* a real `identity_items` key (`domains/view_filter_applications_view_templates.py:159`), but it is a **UID-preferring composite**: `uid_v, uid_q = canonicalize_str(uid_raw)`; only if `uid_v is None` does it fall back to the template's name (`view_filter_applications_view_templates.py:156-159`). Referencing it as a name-identity required item would hash a **UniqueId** for the overwhelming majority of real records (UIDs are essentially always present), not a stable cross-file name string — the opposite of what a name-identity projection needs. | **EXCLUDED** — the only candidate item is UID-shaped, not name-shaped |
| dimension_types (all 7 partitions: linear/angular/radial/diameter/spot_elevation/spot_coordinate/spot_slope) | `type_name` (assigned to `label_str`) | **No** — zero occurrences of `dim_type.name` (or any name-keyed item) anywhere in `domains/dimension_types.py`. `type_name`/`label_str` feeds `label.display` only. (For `spot_coordinate`/`spot_elevation` specifically, the *other* name-shaped required item, `dim_type.symbol_name`, does exist in `identity_items` — but it names a different, referenced element (a leader/tick-mark symbol), not this record's own label, per Step 0 §A.4 — this is the task's originally-cited exclusion reason for those two partitions specifically.) | **EXCLUDED, all 7** — 5 partitions for lack of any own-name item at all (a **broader** reason than the task's brief anticipated — it assumed only the 2 spot partitions needed exclusion and that linear/angular/radial/diameter/spot_slope had a usable own-name item); spot_coordinate/spot_elevation additionally for the referenced-element-name reason already identified in Step 0 |
| units | derived `label_display = "Units ({})".format(label)` | **No** — `label` is the unit-spec loop key (e.g. "Area"/"Length"), a category label, not a Revit-authored name; confirmed no name item, per Step 0 §A.2. | **EXCLUDED** (per task's stated list) |
| line_styles | `sc_name` (`GraphicsStyle.Name`) | **No standalone key** — `sc_name` is embedded only inside the composite `line_style.path = "Lines|{sc_name}"` item (`domains/line_styles.py:255-261`); there is no bare `line_style.name` item. | **EXCLUDED** (per task's stated list; confirmed the name is present only in composite form, never standalone) |
| object_styles (4 partitions), view_category_overrides (2 partitions + coordinator) | `row_key` (category/subcategory path) | **No** — `row_key` is a category path, not a name; confirmed no name-like item exists at all. | **EXCLUDED** (per task's stated list) |

---

## 1. Final eligibility list (7 entries)

```
phases            → required_items: ["phase.name"]      (join_key_schema: "phases.name_identity.join_key.v1.redundant")
materials         → required_items: ["material.name"]
text_types        → required_items: ["text_type.name"]
wall_types        → required_items: ["wt.type_name"]
floor_types       → required_items: ["ft.type_name"]
roof_types        → required_items: ["rt.type_name"]
ceiling_types     → required_items: ["ct.type_name"]
```

All other domains/partitions (30 of 37 policy-file entries) are excluded — each for one of four reasons, cited per-row above:
1. No name-like key at all (units, line_styles, object_styles ×4, view_category_overrides ×3) — matches the task brief's original exclusion list.
2. Referenced-element name, not own label (dimension_types_spot_coordinate, dimension_types_spot_elevation) — matches the task brief's original exclusion list.
3. **Newly discovered in this pass**: the domain's own name is a real Revit-authored string and does feed `label.display`, but it is **never captured as its own `identity_items` key** — it lives only in a phase2 bucket (`cosmetic_items`/`coordination_items`/`unknown_items`) that is *not* merged into the `identity_items` list passed to `build_join_key_from_policy()`, or it is deliberately omitted entirely (identity, phase_filters, line_patterns, fill_patterns ×2, arrowheads, loaded_family_types, view_templates ×5, view_filter_definitions). Referencing a name that isn't in `identity_items` as a `required_items` entry would not error — `build_join_key_from_policy()` maps items by key and would just find nothing — but it would produce a `join_key_status` of `missing_required` (or `blocked`) for **every** record, unconditionally, which is not a usable name-identity key.
4. **Newly discovered**: the only real candidate item is a UID-preferring composite, not a name string (view_filter_applications_view_templates).

This is a materially smaller and differently-shaped list than the task brief anticipated (which expected roughly 19 of 21 domains eligible, minus the 2 explicit dimension_types exclusions). The gap is entirely explained by reason (3): most domains' own name is real, human-authored, and does reach `label.display` — but was deliberately kept out of the hashable `identity_items` surface by earlier domain-specific design decisions (several with code comments or policy notes saying exactly that: "excluded so sig_hash is name-independent," "no longer matched the exported identity_items surface," "must not participate in joins"). A `label.display`-keyed name-identity projection, if it is to reuse `build_join_key_from_policy()` unmodified against the existing `identity_items` snapshot (per the task's item 4 wiring instruction), can only surface for domains where that decision was never made in the first place.

## 2. `materials`/`compound_types` gap (task item 2 point 3) — resolved

Per Step 0 §A.1, `materials.py` and `compound_types.py` never call `build_join_key_from_policy()` inline today (`loaded_family_types.py` also doesn't, but it's excluded above for lack of a name item regardless). Confirmed during this pass: `materials.py`'s `material.name` and `compound_types.py`'s `{wt,ft,rt,ct}.type_name` **are** genuine, already-present keys in each domain's `identity_items` list — so the name-key builder call can be added as a **new** call site in these files (there being no existing join-key call site to "mirror," contra the task's item 4 phrasing for the other 16 files). `loaded_family_types.py` is excluded on its own (no name item at all, by design), so its gap does not need resolving for this PR.

## 3. `phases` redundancy marker (task item 3) — confirmed, no new finding

`phase.name` is genuinely the sole required item in both `phases`' existing `join_hash` policy and would be the sole required item in the new name-key policy — full agreement with Step 0's addendum §A.4. The `join_key_schema` string for this one entry will be stamped `"phases.name_identity.join_key.v1.redundant"` per the task's explicit instruction (item 3), rather than the bare `"name_identity.join_key.v1"` used for the other 6 eligible entries.

## 4. Decisions (confirmed by Greg, implemented)

1. **Eligibility scope — widen.** Greg chose to widen the 8 phase2-bucket-only domains
   (`identity`, `phase_filters`, `line_patterns`, `fill_patterns` x2, `arrowheads`,
   `loaded_family_types`, `view_templates` x5, `view_filter_definitions`) rather than
   ship only the 7 fully-native entries. Implemented via a *locally widened* items list
   built only at each name-key call site (the domain's existing `identity_items` plus one
   freshly-`make_identity_item`-wrapped value read from the phase2 bucket, or from
   `label.display` for domains where no bucket item exists either) — `identity_basis.items`,
   `sig_hash`, and the existing `join_key` are computed from the original, unwidened list
   and are byte-identical before/after this PR. The same widening principle was also
   applied to `dimension_types_{linear,angular,radial,diameter,spot_slope}` (5 more
   partitions where `dim_type.name` likewise exists nowhere in `identity_items`, only as
   the raw `type_name` variable feeding `label.display`) — the task brief's own working
   assumption expected these 5 partitions to be natively eligible, so this closes that gap
   the same way as the other 8. `view_filter_applications_view_templates` was
   **deliberately left excluded**: its only in-scope candidate item
   (`vfa.template_uid_or_namekey`) is a UID-preferring composite (falls back to name only
   when UID is absent), not a clean name string, and widening it would mean synthesizing a
   value from a different code path than the rest of this PR's mechanism — left as a named
   open follow-up rather than folded in speculatively. Final eligible set: 25 of 37
   `domain_join_key_policies.json` entries (7 native + 18 widened).

2. **Case-folding / normalization — no.** Confirmed: no case-folding or Unicode
   normalization exists anywhere in the codebase today (Step 0 §1.1/§1.6). Greg confirmed
   the task's stated default: PR1 reuses `canonicalize_str`/`canonicalize_str_allow_empty`
   exactly as-is (trim + missing-check only) for every widened item and every inline call.
   Case-only name variants (e.g. "Acme Wall Type" vs. "ACME WALL TYPE") produce distinct
   `join_key_name_identity.join_hash` values under this PR; this is flagged, not silently
   resolved, for PR4/5's later false-fragmentation analysis to interpret.

3. **Delivery approach — both.** Mid-implementation, Greg raised that every value this
   projection reads (`label.display`, phase2 bucket items) is already present in today's
   exported `*.details.json` files — meaning inline extractor wiring alone would produce
   zero usable data until every model in the corpus is re-extracted through Revit/Dynamo.
   Per the precedent already in this codebase (`core/sig_hash_builder.py` reconstructs
   `sig_hash` analysis-side from already-flattened data, alongside the inline extractor
   computation), this PR ships **both**: the inline `domains/*.py` wiring described above
   (for future-export correctness and to avoid drift between what the two join_hash
   variants see), and a new analysis-side reconstruction path
   (`core/name_key_builder.py` + `tools/apply_name_key_policy.py`) that computes the
   identical `join_key_name_identity` value directly from already-exported
   `*.details.json` records — usable immediately against the existing corpus, no
   re-extraction required. The two paths are expected to agree by construction: both read
   the same underlying `identity_basis.items`/phase2 bucket/`label.display` data, just at
   different points in the pipeline (export-time vs. read-time).
