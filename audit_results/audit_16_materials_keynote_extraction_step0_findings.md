# Audit 16 — PR 4 (`materials.keynote` extraction fix): Step 0 findings

Date: 2026-08-26
Scope: Read-only Step 0 investigation for PR 4 of the Audit 16 corpus re-extraction batch (`domains/materials.py:360-363`'s `material.keynote` read path). No extractor, test, or policy file was modified as part of this pass.

## Premise being tested

The PR brief assumed a real extractor bug: `_read_param_as_string(m, bip_names=["KEYNOTE_PARAM"], lookup_names=["Keynote"])` was reported to resolve on only ~6% of materials (20/362) in a prior probe run, with the implication that `BuiltInParameter.KEYNOTE_PARAM` and the `"Keynote"`-named parameter are two different bindings and the helper's BIP-first fallback logic was silently preferring an empty/wrong one over a populated real one. The brief explicitly required confirming this via direct inspection of live Material elements before writing any fix.

## Investigation

1. Read `domains/materials.py:88-139` (`_read_param_as_string`) and the `keynote = _read_param_as_string(...)` call site (`domains/materials.py:360-363`) in full. Identified a real structural property of the helper worth checking empirically: once any `bip_name` resolves to a non-`None` `Parameter`, the function commits to that parameter's `HasValue`/`AsString()` and never falls through to the `lookup_names` loop below — a plausible mechanism for exactly this failure mode *if* `KEYNOTE_PARAM` and `"Keynote"` are in fact different, independently-bound parameters on `Material`.
2. Ran `tools/probes/probe_materials.py` (write_json=True) against a live document (`FVOT_ BLDG 1_ Arch25_Greg.McDowellJr.rvt`, Revit 2025, 286 materials). Its generic `GetOrderedParameters()` breadth sweep confirmed a bound parameter with display name exactly `"Keynote"`, `StorageType.String`, with real distinct values present across the sampled materials — ruling out "the field doesn't exist" or "it's under a different display name" as the root cause, and ruling in that ordinary parameter access can read it.
3. That probe's aggregated inventory format only records de-duplicated `(storage, value)` signatures per parameter, not a per-material resolution breakdown, so it could not by itself confirm or rule out the BIP-vs-name divergence hypothesis. Wrote a small standalone diagnostic (`diagnose_material_keynote.py`, not part of this repo — ad hoc, run once) that, for every `Material` in the same live document, captured side-by-side: `get_Parameter(BuiltInParameter.KEYNOTE_PARAM)` (is it `None`? `HasValue`? `AsString()`?) vs. `LookupParameter("Keynote")` (same three), plus whether the two calls return the identical `Parameter` object (`bip_p.Id == name_p.Id`).

   ```python
   bip_p = m.get_Parameter(BuiltInParameter.KEYNOTE_PARAM)
   name_p = m.LookupParameter("Keynote")
   same_object = (bip_p is not None and name_p is not None
                  and bip_p.Id.IntegerValue == name_p.Id.IntegerValue)
   ```

4. Ran it against the full material collection (`FilteredElementCollector(doc).OfClass(Material)`, no sampling) in the same document.

## Result

Across every material returned (all classes: Ceramic, Concrete, Glass, Metal, Wood, Life Safety, Generic, Unassigned, etc.):

- `bip.KEYNOTE_PARAM.is_none` was `false` for every single material — the BIP is bound on all of them, so the "BIP unbound on most materials" branch of the original hypothesis (Step 0.2's first bullet) does not apply here.
- `same_parameter_object` was `true` for every single material, with no exceptions.
- `HasValue` and `AsString()` matched exactly between the BIP access and the name-based lookup on every row, again with no exceptions.

In other words: on this document, `BuiltInParameter.KEYNOTE_PARAM` and `LookupParameter("Keynote")` are **the same underlying `Parameter` object** on every `Material`. There is no second, hidden, differently-bound parameter for the helper's fallback order to skip past. The premise that a BIP-vs-name divergence is silently swallowing real values does not reproduce here.

The low resolution rate is explained by the data itself, not by the code:
- The large majority of materials have `HasValue: true` but `AsString()` returns `""` (empty string) — a parameter that has been touched/initialized but never actually given text. `core/record_v2.py`'s `canonicalize_str("")` already correctly maps this to `v: None, q: "missing"` (per `test_keynote_blank_emits_missing_item_not_omitted`), which is the intended sentinel behavior, not a bug.
- A small number of materials have `HasValue: false` (parameter present, never set at all) — correctly `q: "missing"` today (`test_keynote_unset_param_emits_missing_item_not_omitted`).
- Only a handful of materials carry genuine text: two distinct real keynote codes were observed — `"08 41 13"` (on 4 aluminum-related materials: `METAL_ ALUMINUM`, `METAL_ ALUMINUM_ BLACK`, `METAL_ ALUMINUM_ 180º`, `METAL_ EIFS-01`) and `"MP-02"` (on `Metal Panel` and `MP-02_Wall panels...`). Both resolve correctly today through the existing code path. One additional material (`Clearance - Code`) carries a string of invisible/BOM whitespace characters rather than real content — not a keynote value, not something this PR's scope covers.

This matches the magnitude of the originally reported ~6% (20/362) figure: on this document the genuinely-populated fraction is smaller still (~2%), and it is populated correctly.

## Conclusion

**No extraction bug found.** `domains/materials.py`'s existing `keynote = _read_param_as_string(m, bip_names=["KEYNOTE_PARAM"], lookup_names=["Keynote"])` call already reads the correct, single, canonical parameter and correctly distinguishes "genuinely blank" from "has real text" via the shared sentinel/canonicalization logic. The low observed resolution rate reflects that most materials in this corpus simply were never given a Keynote value — real-world data sparsity, not a capture defect. Per user direction (2026-08-26), this is accepted as-is: "if the corpus only shows 6% of materials have a value in keynote and the rest are blank that's okay -- it's the data doing what its doing."

## Disposition

- **No code change made** to `domains/materials.py`, `_read_param_as_string`, or any policy file. The acceptance criteria's "resolution rate increases materially above baseline" is not pursued because there is no confirmed defect to fix — forcing a code change against unconfirmed root cause would risk exactly the kind of unverified, non-reproducible fix Step 0 was designed to prevent.
- `patch_json_materials_keynote.py` (external, not in this repo) was not consulted as ground truth for this conclusion, per the PR's own out-of-scope instruction; this finding rests solely on live-document inspection.
- This finding is scoped to the one document tested (`FVOT_ BLDG 1_ Arch25_Greg.McDowellJr.rvt`, Revit 2025). If a future document in the broader corpus is found where `same_parameter_object` is `false` for some materials (i.e., a genuine second, differently-bound `"Keynote"` parameter exists, e.g. from a firm/project-level shared parameter colliding in display name with the built-in one), that would reopen this investigation with a concrete, reproducible divergence to fix — the diagnostic script above is the tool to re-run against such a document.
- PR 4 is closed out as a verified non-issue for the Audit 16 batch; no re-extraction is required on account of this domain/field.
