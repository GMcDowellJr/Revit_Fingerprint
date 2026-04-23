from dataclasses import dataclass

from tools.lib.domain_profile import DomainProfile, ResolutionSpec

VT_DOMAINS = [
    "view_templates_floor_structural_area_plans",
    "view_templates_ceiling_plans",
    "view_templates_elevations_sections_detail",
    "view_templates_renderings_drafting",
    "view_templates_schedules",
]

VT_DEFERRED_DOMAINS = [
    "view_category_overrides",
    "view_filter_applications_view_templates",
]

VT_SUPPRESS_KEYS = {"view_template.def_hash"}

VT_RESOLUTION_SPECS = [
    ResolutionSpec(
        key_exact="view_template.sig.phase_filter",
        source_domain="phase_filters",
        name_path="label.display",
    ),
    ResolutionSpec(
        key_prefix="view_template.sig.filter[",
        key_suffix="].def_sig",
        source_domain="view_filter_definitions",
        name_path="label.components.name",
    ),
]

VT_VALID_KEYS_BY_DOMAIN = {
    "view_templates_floor_structural_area_plans": None,
    "view_templates_ceiling_plans": None,
    "view_templates_elevations_sections_detail": None,
    "view_templates_renderings_drafting": None,
    "view_templates_schedules": None,
}

# Property suffixes shared by obj_style.* and vco.* graphic fields.
# Used to identify which identity_basis items are graphic properties
# vs. identity/reference items (row_key, baseline_sig_hash, etc.).
_GRAPHIC_SUFFIXES = frozenset(
    {
        "projection.line_weight",
        "projection.color.rgb",
        "projection.pattern_ref.sig_hash",
        "projection.fill_pattern_ref.sig_hash",
        "projection.fill_color.rgb",
        "cut.line_weight",
        "cut.color.rgb",
        "cut.pattern_ref.sig_hash",
        "cut.fill_pattern_ref.sig_hash",
        "cut.fill_color.rgb",
        "halftone",
        "transparency",
    }
)


def _load_domain_records(raw, domain_key):
    """Return records list from raw[domain_key]["records"], or [] if absent/malformed."""
    payload = raw.get(domain_key)
    if not isinstance(payload, dict):
        return []
    records = payload.get("records")
    if not isinstance(records, list):
        return []
    return records


def _load_vco_records(raw):
    """
    Load VCO records from raw, preferring split partition keys over legacy aggregate.

    Tries view_category_overrides_model and view_category_overrides_annotation first.
    Falls back to view_category_overrides (legacy aggregate) only if both split
    keys return empty. Never double-counts: if split keys produce records the
    legacy key is ignored even if present.
    """
    model = _load_domain_records(raw, "view_category_overrides_model")
    annot = _load_domain_records(raw, "view_category_overrides_annotation")
    if model or annot:
        return model + annot
    return _load_domain_records(raw, "view_category_overrides")


def _get_phase2_cosmetic_value(rec, key):
    """Return the value of a named item from record phase2.cosmetic_items, or None."""
    for it in (rec.get("phase2") or {}).get("cosmetic_items") or []:
        if isinstance(it, dict) and it.get("k") == key:
            return it.get("v")
    return None


def _get_identity_item_value(rec, key):
    """Return the value of a named item from record identity_basis.items, or None."""
    for it in (rec.get("identity_basis") or {}).get("items") or []:
        if isinstance(it, dict) and it.get("k") == key:
            return it.get("v")
    return None


def _index_vco_by_template(records):
    """
    Build a two-level index: {template_name: {category_path: record}}.

    template_name sourced from phase2.cosmetic_items["vco.template_name"].
    category_path sourced from identity_basis.items["vco.baseline_category_path"].

    Records missing either value are skipped. On (template_name, category_path)
    collision the first record wins — collisions should not occur for non-degraded
    records but are handled defensively.
    """
    index = {}
    for rec in records:
        tpl_name = _get_phase2_cosmetic_value(rec, "vco.template_name")
        if not tpl_name:
            continue
        cat_path = _get_identity_item_value(rec, "vco.baseline_category_path")
        if not cat_path:
            continue
        tpl_entry = index.setdefault(tpl_name, {})
        if cat_path not in tpl_entry:
            tpl_entry[cat_path] = rec
    return index


def _index_object_styles_by_row_key(raw):
    """
    Load object_styles_model + object_styles_annotation records from raw and
    index by obj_style.row_key → record.

    model partition records take precedence on row_key collision (model runs
    first in the runner and is the canonical baseline source for VCO).
    """
    records = _load_domain_records(raw, "object_styles_model") + _load_domain_records(raw, "object_styles_annotation")
    index = {}
    for rec in records:
        row_key = _get_identity_item_value(rec, "obj_style.row_key")
        if row_key and row_key not in index:
            index[row_key] = rec
    return index


def _extract_graphic_fields(record, prefix):
    """
    Extract graphic property items from record identity_basis.items.

    Returns {suffix: (value, quality)} where suffix is the part of the key
    after the given prefix, filtered to _GRAPHIC_SUFFIXES only.

    prefix is either "obj_style." (for baseline records) or "vco." (for
    VCO override records). The suffix space is identical for both.
    """
    result = {}
    for it in (record.get("identity_basis") or {}).get("items") or []:
        if not isinstance(it, dict):
            continue
        k = it.get("k", "")
        if not k.startswith(prefix):
            continue
        suffix = k[len(prefix) :]
        if suffix in _GRAPHIC_SUFFIXES:
            result[suffix] = (it.get("v"), it.get("q", ""))
    return result


def _reconstruct_effective(baseline_record, vco_record):
    """
    Reconstruct the effective rendered graphics for one (template, category) pair.

    Algorithm:
      1. Seed from baseline (obj_style.* → suffix map). If baseline_record is
         None the seed is empty — only override fields will be present.
      2. Overwrite with VCO override fields (vco.* → same suffix map).
         VCO records only store items that differ from OverrideGraphicSettings()
         default, so every field present in VCO identity_basis IS an active
         override and takes precedence over the baseline value.

    Returns {suffix: (value, quality)}.
    """
    effective = {}
    if baseline_record is not None:
        effective.update(_extract_graphic_fields(baseline_record, "obj_style."))
    effective.update(_extract_graphic_fields(vco_record, "vco."))
    return effective


def _build_synthetic_items_for_pair(tpl_vco_a, tpl_vco_b, os_index_a, os_index_b):
    """
    Build parallel synthetic item lists for one matched template pair.

    For each category present in either file's VCO records, reconstructs
    effective graphics for both sides and emits one item per property suffix.

    item_key format: "category_path > property_suffix"
    e.g. "Walls|self > projection.color.rgb"

    Returns (items_a, items_b) — parallel lists of {"k", "v", "q"} dicts
    ready for compare_entries(). Lists are parallel: items_a[i] and
    items_b[i] share the same "k" value.
    """
    items_a = []
    items_b = []

    all_cat_paths = sorted(set(tpl_vco_a) | set(tpl_vco_b))

    for cat_path in all_cat_paths:
        rec_a = tpl_vco_a.get(cat_path)
        rec_b = tpl_vco_b.get(cat_path)

        os_rec_a = os_index_a.get(cat_path)
        os_rec_b = os_index_b.get(cat_path)

        eff_a = _reconstruct_effective(os_rec_a, rec_a) if rec_a is not None else {}
        eff_b = _reconstruct_effective(os_rec_b, rec_b) if rec_b is not None else {}

        all_suffixes = sorted(set(eff_a) | set(eff_b))

        for suffix in all_suffixes:
            item_key = "{} > {}".format(cat_path, suffix)

            if suffix in eff_a:
                val_a, q_a = eff_a[suffix]
            else:
                val_a, q_a = None, "missing"

            if suffix in eff_b:
                val_b, q_b = eff_b[suffix]
            else:
                val_b, q_b = None, "missing"

            items_a.append({"k": item_key, "v": val_a, "q": q_a})
            items_b.append({"k": item_key, "v": val_b, "q": q_b})

    return items_a, items_b


@dataclass
class ViewTemplateDomainProfile(DomainProfile):
    """Domain profile for view_templates_* partitions."""

    def __post_init__(self):
        if not self.domains:
            self.domains = list(VT_DOMAINS)
        if not self.suppress_keys:
            self.suppress_keys = set(VT_SUPPRESS_KEYS)
        if not self.resolution_specs:
            self.resolution_specs = list(VT_RESOLUTION_SPECS)
        if not self.valid_keys_by_domain:
            self.valid_keys_by_domain = dict(VT_VALID_KEYS_BY_DOMAIN)
        self.bucket_strategy = "sig_basis"
        self.match_strategy = "label_display"

    def get_deferred_domains(self):
        return list(VT_DEFERRED_DOMAINS)

    def get_hash_resolution_meta(self, maps_a, maps_b):
        return {
            "phase_filter_map_a_size": len(maps_a.get("phase_filters", {})),
            "phase_filter_map_b_size": len(maps_b.get("phase_filters", {})),
            "vf_def_map_a_size": len(maps_a.get("view_filter_definitions", {})),
            "vf_def_map_b_size": len(maps_b.get("view_filter_definitions", {})),
        }

    def classify_bucket(self, item_key, record_a, record_b):
        """
        For VCO synthetic items (item_key contains " > "), classify by property
        suffix. All other keys delegate to the base class.
        """
        if " > " in item_key:
            suffix = item_key.split(" > ", 1)[1]
            if (
                suffix.startswith("projection.")
                or suffix.startswith("cut.")
                or suffix in ("halftone", "transparency")
            ):
                return "cosmetic"
            return "unknown"
        return super().classify_bucket(item_key, record_a, record_b)

    def reconstruct(self, matched_pairs, raw_a, raw_b):
        """
        For each matched template pair, inject reconstructed VCO effective-diff
        items as synthetic extra_entries. The engine's secondary loop in
        run_comparison() processes these to produce detail rows with
        domain = "view_category_overrides".

        Returns the mutated matched_pairs list. matched_pairs is modified in place
        but also returned for consistency with the base class contract.
        """
        os_index_a = _index_object_styles_by_row_key(raw_a)
        os_index_b = _index_object_styles_by_row_key(raw_b)

        if not os_index_a and not os_index_b:
            return matched_pairs

        vco_records_a = _load_vco_records(raw_a)
        vco_records_b = _load_vco_records(raw_b)

        if not vco_records_a and not vco_records_b:
            return matched_pairs

        vco_index_a = _index_vco_by_template(vco_records_a)
        vco_index_b = _index_vco_by_template(vco_records_b)

        for pair in matched_pairs:
            template_name = pair["entry_a"]["display_name"]

            tpl_vco_a = vco_index_a.get(template_name, {})
            tpl_vco_b = vco_index_b.get(template_name, {})

            if not tpl_vco_a and not tpl_vco_b:
                continue

            syn_items_a, syn_items_b = _build_synthetic_items_for_pair(tpl_vco_a, tpl_vco_b, os_index_a, os_index_b)

            if not syn_items_a and not syn_items_b:
                continue

            base_record = pair["entry_a"]["record"]
            syn_entry_a = {
                "domain": "view_category_overrides",
                "display_name": template_name,
                "norm_name": pair["entry_a"]["norm_name"],
                "record": base_record,
                "record_id": pair["entry_a"]["record_id"],
                "sig_hash": "",
                "status": "ok",
                "label_quality": "synthetic_vco",
                "items": syn_items_a,
            }
            syn_entry_b = {
                "domain": "view_category_overrides",
                "display_name": template_name,
                "norm_name": pair["entry_b"]["norm_name"],
                "record": pair["entry_b"]["record"],
                "record_id": pair["entry_b"]["record_id"],
                "sig_hash": "",
                "status": "ok",
                "label_quality": "synthetic_vco",
                "items": syn_items_b,
            }

            pair.setdefault("extra_entries", []).append((syn_entry_a, syn_entry_b))

        return matched_pairs


def make_vt_profile():
    return ViewTemplateDomainProfile(name="view_templates")
