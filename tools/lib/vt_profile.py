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


def make_vt_profile():
    return ViewTemplateDomainProfile(name="view_templates")
