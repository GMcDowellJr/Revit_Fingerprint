# Chunk of policies/domain_join_key_policies.json

- Source relative path: `policies/domain_join_key_policies.json`
- Chunk: 3 of 3
- Original line range: 781-1046
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: c2197eb74a16316f88e902f8d66eca20eb7d7030b0f0e452980b93cd31d80964
- Starts inside symbol: no
- Ends inside symbol: no

```
   781|       ]
   782|     },
   783|     "view_category_overrides_model": {
   784|       "join_key_schema": "view_category_overrides_model.join_key.v1",
   785|       "hash_alg": "md5_utf8_join_pipe",
   786|       "required_items": [
   787|         "vco.baseline_category_path",
   788|         "vco.baseline_sig_hash",
   789|         "vco.override_properties_hash"
   790|       ],
   791|       "optional_items": [],
   792|       "explicitly_excluded_items": [
   793|         "vco.category_path",
   794|         "vco.projection.line_weight",
   795|         "vco.projection.line_pattern_ref.sig_hash",
   796|         "vco.projection.line_color.rgb",
   797|         "vco.cut.line_weight",
   798|         "vco.cut.line_pattern_ref.sig_hash",
   799|         "vco.cut.line_color.rgb",
   800|         "vco.halftone",
   801|         "vco.transparency"
   802|       ],
   803|       "notes": [
   804|         "Domain family: view_category_overrides.",
   805|         "Contains: CategoryType.Model categories and subcategories.",
   806|         "include_controlled removed — include state sourced from view_templates.include_vg_model.",
   807|         "vco.vg_tab = Model in coordination_items for downstream join."
   808|       ]
   809|     },
   810|     "view_filter_applications_view_templates": {
   811|       "join_key_schema": "view_filter_applications_view_templates.join_key.v2",
   812|       "hash_alg": "md5_utf8_join_pipe",
   813|       "required_items": [
   814|         "vfa.stack_def_hash"
   815|       ],
   816|       "optional_items": [],
   817|       "explicitly_excluded_items": [
   818|         "vfa.template_uid_or_namekey",
   819|         "vfa.template_elem_id",
   820|         "vfa.filter_stack_count",
   821|         "vfa.stack[].filter_sig_hash",
   822|         "vfa.stack[].visible",
   823|         "vfa.stack[].enabled"
   824|       ],
   825|       "notes": [
   826|         "Structured-sequence domain: join identity is the ordered filter-stack definition collapsed into vfa.stack_def_hash.",
   827|         "Leaf stack members and counts are authoritative identity (identity_basis) but must not participate in joins.",
   828|         "Template identifiers are labels/linkage only and are prohibited from joins."
   829|       ]
   830|     },
   831|     "view_filter_definitions": {
   832|       "join_key_schema": "view_filter_definitions.join_key.v2",
   833|       "hash_alg": "md5_utf8_join_pipe",
   834|       "required_items": [
   835|         "vf.def_hash"
   836|       ],
   837|       "optional_items": [],
   838|       "explicitly_excluded_items": [
   839|         "vf.uid_or_namekey",
   840|         "vf.elem_id",
   841|         "vf.categories",
   842|         "vf.logic_root",
   843|         "vf.rule_count",
   844|         "vf.rules[].sig",
   845|         "label.components.name",
   846|         "label.display"
   847|       ],
   848|       "notes": [
   849|         "Structured domain: join is performed on vf.def_hash only (definition bundle hash).",
   850|         "vf.def_hash is computed from canonical vf.categories + vf.logic_root + vf.rule_count + ordered vf.rule[###].sig, but those leaf inputs remain identity-only for forensics.",
   851|         "Names/labels and any UID-like identifiers are prohibited from joins for standards governance."
   852|       ]
   853|     },
   854|     "view_templates_ceiling_plans": {
   855|       "join_key_schema": "view_templates.join_key.v1",
   856|       "hash_alg": "md5_utf8_join_pipe",
   857|       "required_items": [
   858|         "view_template.def_hash"
   859|       ],
   860|       "optional_items": [],
   861|       "explicitly_excluded_items": [
   862|         "view_template.name",
   863|         "view_template.uid",
   864|         "view_template.element_id",
   865|         "view_template.category_overrides_def_hash"
   866|       ],
   867|       "notes": [
   868|         "Split domain: same join key policy as monolithic view_templates.",
   869|         "view_template.def_hash captures complete behavioral definition.",
   870|         "Domain family routing is handled by coordination_items.vt.view_type_family, not by the join key.",
   871|         "Contains: ViewType.CeilingPlan."
   872|       ]
   873|     },
   874|     "view_templates_elevations_sections_detail": {
   875|       "join_key_schema": "view_templates.join_key.v1",
   876|       "hash_alg": "md5_utf8_join_pipe",
   877|       "required_items": [
   878|         "view_template.def_hash"
   879|       ],
   880|       "optional_items": [],
   881|       "explicitly_excluded_items": [
   882|         "view_template.name",
   883|         "view_template.uid",
   884|         "view_template.element_id",
   885|         "view_template.category_overrides_def_hash"
   886|       ],
   887|       "notes": [
   888|         "Split domain: same join key policy as monolithic view_templates.",
   889|         "view_template.def_hash captures complete behavioral definition.",
   890|         "Domain family routing is handled by coordination_items.vt.view_type_family, not by the join key.",
   891|         "Contains: ViewType.Elevation, ViewType.Section, ViewType.Detail."
   892|       ]
   893|     },
   894|     "view_templates_floor_structural_area_plans": {
   895|       "join_key_schema": "view_templates.join_key.v1",
   896|       "hash_alg": "md5_utf8_join_pipe",
   897|       "required_items": [
   898|         "view_template.def_hash"
   899|       ],
   900|       "optional_items": [],
   901|       "explicitly_excluded_items": [
   902|         "view_template.name",
   903|         "view_template.uid",
   904|         "view_template.element_id",
   905|         "view_template.category_overrides_def_hash"
   906|       ],
   907|       "notes": [
   908|         "Split domain: same join key policy as monolithic view_templates.",
   909|         "view_template.def_hash captures complete behavioral definition.",
   910|         "Domain family routing is handled by coordination_items.vt.view_type_family, not by the join key.",
   911|         "Contains: ViewType.FloorPlan, ViewType.EngineeringPlan (Structural), ViewType.AreaPlan."
   912|       ]
   913|     },
   914|     "view_templates_renderings_drafting": {
   915|       "join_key_schema": "view_templates.join_key.v1",
   916|       "hash_alg": "md5_utf8_join_pipe",
   917|       "required_items": [
   918|         "view_template.def_hash"
   919|       ],
   920|       "optional_items": [],
   921|       "explicitly_excluded_items": [
   922|         "view_template.name",
   923|         "view_template.uid",
   924|         "view_template.element_id",
   925|         "view_template.category_overrides_def_hash"
   926|       ],
   927|       "notes": [
   928|         "Split domain: same join key policy as monolithic view_templates.",
   929|         "view_template.def_hash captures complete behavioral definition.",
   930|         "Domain family routing is handled by coordination_items.vt.view_type_family, not by the join key.",
   931|         "Contains: ViewType.ThreeD, ViewType.Rendering, ViewType.DraftingView."
   932|       ]
   933|     },
   934|     "view_templates_schedules": {
   935|       "join_key_schema": "view_templates.join_key.v1",
   936|       "hash_alg": "md5_utf8_join_pipe",
   937|       "required_items": [
   938|         "view_template.def_hash"
   939|       ],
   940|       "optional_items": [],
   941|       "explicitly_excluded_items": [
   942|         "view_template.name",
   943|         "view_template.uid",
   944|         "view_template.element_id",
   945|         "view_template.category_overrides_def_hash"
   946|       ],
   947|       "notes": [
   948|         "Split domain: same join key policy as monolithic view_templates.",
   949|         "view_template.def_hash captures complete behavioral definition.",
   950|         "Domain family routing is handled by coordination_items.vt.view_type_family, not by the join key.",
   951|         "Contains: ViewType.Schedule, ViewType.PanelSchedule."
   952|       ]
   953|     },
   954|     "wall_types": {
   955|       "join_key_schema": "wall_types.join_key.v1",
   956|       "hash_alg": "md5_utf8_join_pipe",
   957|       "required_items": [
   958|         "wt.function",
   959|         "wt.layer_count",
   960|         "wt.total_thickness_in",
   961|         "wt.stack_hash_loose"
   962|       ],
   963|       "optional_items": [
   964|         "wt.wraps_at_inserts",
   965|         "wt.wraps_at_ends"
   966|       ],
   967|       "explicitly_excluded_items": [
   968|         "wt.type_name",
   969|         "wt.coarse_fill_color_rgb"
   970|       ],
   971|       "notes": [
   972|         "Domain family: compound_types.",
   973|         "stack_hash_loose is both sig_hash input and primary join discriminator.",
   974|         "stack_hash_strict and stack_hash_function_only are coordination items for pareto evaluation.",
   975|         "Wrapping params are optional — degrade gracefully if unreadable.",
   976|         "wt.type_name excluded per D-001 — names are labels only."
   977|       ]
   978|     },
   979|     "worksets": {
   980|       "join_key_schema": "worksets.join_key.v1",
   981|       "hash_alg": "md5_utf8_join_pipe",
   982|       "required_items": [
   983|         "workset.name"
   984|       ],
   985|       "optional_items": [
   986|         "workset.kind",
   987|         "workset.is_default_workset"
   988|       ],
   989|       "explicitly_excluded_items": [
   990|         "workset.owner",
   991|         "workset.is_active_workset",
   992|         "workset.is_editable",
   993|         "workset.unique_id"
   994|       ],
   995|       "notes": [
   996|         "workset.name is the definition-bearing identifier (Revit does not allow duplicate workset names within a document).",
   997|         "workset.owner, workset.is_active_workset, and workset.is_editable are live editing-session/checkout state, not behavior — excluded from joins per the same rule that excludes them from sig_hash. IsEditable reflects whether the *current user* can edit the workset right now, not a fixed property of the workset.",
   998|         "workset.unique_id excluded per D-004 — Workset is not Element-backed, so UniqueId is traceability only, never identity."
   999|       ]
  1000|     },
  1001|     "worksets_doc": {
  1002|       "join_key_schema": "worksets_doc.join_key.v1",
  1003|       "hash_alg": "md5_utf8_join_pipe",
  1004|       "required_items": [],
  1005|       "optional_items": [
  1006|         "worksets_doc.is_workshared",
  1007|         "worksets_doc.active_workset_name",
  1008|         "worksets_doc.count_user_workset",
  1009|         "worksets_doc.count_standard_workset",
  1010|         "worksets_doc.count_view_workset",
  1011|         "worksets_doc.count_family_workset",
  1012|         "worksets_doc.count_other_workset"
  1013|       ],
  1014|       "explicitly_excluded_items": [],
  1015|       "notes": [
  1016|         "Single synthetic document-level summary record — no per-record required discriminator; all fields optional to match the record's own minima (nothing blocks it)."
  1017|       ]
  1018|     },
  1019|     "browser_organization": {
  1020|       "join_key_schema": "browser_organization.join_key.v1",
  1021|       "hash_alg": "md5_utf8_join_pipe",
  1022|       "required_items": [
  1023|         "bo.category"
  1024|       ],
  1025|       "optional_items": [
  1026|         "bo.sorting_order",
  1027|         "bo.sorting_parameter_id",
  1028|         "bo.filter_has_value"
  1029|       ],
  1030|       "explicitly_excluded_items": [
  1031|         "bo.family_name",
  1032|         "bo.org_id",
  1033|         "bo.unique_id",
  1034|         "bo.workset_id",
  1035|         "bo.workset_name",
  1036|         "bo.workset_unique_id"
  1037|       ],
  1038|       "notes": [
  1039|         "bo.category is the definition-bearing identifier — Revit exposes at most one BrowserOrganization per category (views/sheets/schedules) per document.",
  1040|         "bo.family_name is a Revit-internal display label, not behavior — excluded from joins per the naming-is-metadata rule.",
  1041|         "bo.org_id/bo.unique_id excluded — BrowserOrganization ids are not expected to be stable/comparable across documents.",
  1042|         "bo.workset_id/bo.workset_name/bo.workset_unique_id are coordination items (cross-model, name-based resolution) — excluded from joins per the same rule that excludes them from sig_hash."
  1043|       ]
  1044|     }
  1045|   }
  1046| }
```
