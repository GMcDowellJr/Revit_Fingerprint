# Chunk of policies/domain_join_key_policies.json

- Source relative path: `policies/domain_join_key_policies.json`
- Chunk: 2 of 3
- Original line range: 391-790
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: c2197eb74a16316f88e902f8d66eca20eb7d7030b0f0e452980b93cd31d80964
- Starts inside symbol: no
- Ends inside symbol: no

```
   391|         "Dot segments use relative epsilon (1% of non_dot_total) to preserve scale invariance.",
   392|         "segments_def_hash moved to explicitly_excluded — retains exact identity in identity_basis for forensics.",
   393|         "Names are labels only and must not participate in joins.",
   394|         "lp.is_import added to coordination_items in extractor v2.",
   395|         "sig_hash intentionally uses segments_def_hash (exact scale identity); see domain_sig_hash_policies.json.",
   396|         "join_hash (this policy) uses segments_norm_hash (governance equivalence). These are different questions."
   397|       ]
   398|     },
   399|     "line_styles": {
   400|       "join_key_schema": "line_styles.join_key.v1",
   401|       "hash_alg": "md5_utf8_join_pipe",
   402|       "required_items": [
   403|         "line_style.weight.projection",
   404|         "line_style.color.rgb",
   405|         "line_style.pattern_ref.sig_hash"
   406|       ],
   407|       "optional_items": [],
   408|       "explicitly_excluded_items": [
   409|         "line_style.path",
   410|         "line_style.pattern_ref.kind"
   411|       ],
   412|       "notes": [
   413|         "Identity is definition-based: projection weight + color + referenced line pattern definition via pattern_ref.sig_hash.",
   414|         "pattern_ref.kind is too coarse and produces high collisions; it must not be used for joins."
   415|       ]
   416|     },
   417|     "loaded_family_types": {
   418|       "join_key_schema": "loaded_family_types.join_key.v2",
   419|       "hash_alg": "md5_utf8_join_pipe",
   420|       "required_items": [
   421|         "lft.shape_gate.category",
   422|         "lft.type_parameter_schema_hash"
   423|       ],
   424|       "optional_items": [
   425|         "lft.family_is_in_place",
   426|         "lft.family_is_editable"
   427|       ],
   428|       "explicitly_excluded_items": [
   429|         "lft.shape_gate.category_id",
   430|         "lft.type_parameter_count",
   431|         "lft.family_symbol_count",
   432|         "lft.type_count",
   433|         "lft.structural_material_type",
   434|         "lft.is_active"
   435|       ],
   436|       "notes": [
   437|         "Records are at family granularity (one per Family element, not per type).",
   438|         "family_name is excluded from join: including it prevents within-project matches for",
   439|         "functionally identical families with different names. Two families that share category +",
   440|         "parameter schema are the same behavioral concept.",
   441|         "",
   442|         "lft.type_parameter_schema_hash IS the behavioral anchor for join: it encodes the full",
   443|         "parameter definition set for the family, making it a precise identity discriminator.",
   444|         "Note: schema_hash is opaque to greedy/Pareto discovery tools — those tools cannot",
   445|         "decompose it into sub-fields. For finer-grained join discovery, individual parameter",
   446|         "values from parameter_rows would need to be promoted as identity items (future work,",
   447|         "see domain future-steps item 8).",
   448|         "",
   449|         "lft.family_is_in_place / lft.family_is_editable optionally narrow matches to",
   450|         "loaded-only or in-place-only subsets.",
   451|         "",
   452|         "lft.structural_material_type / lft.is_active (Area 12) are governed sig_hash inputs but",
   453|         "are explicitly excluded from join-key discovery: is_active is an operational activation",
   454|         "state (per-symbol usage history, not a definitional property of the family), and",
   455|         "structural_material_type, while stable per family in the probed corpus, has no probe",
   456|         "evidence yet establishing it as a cross-project join discriminator. Without this exclusion",
   457|         "tools/discover_join_policy.py's default discover/harsh modes would nominate both as",
   458|         "candidates from every emitted identity-item key.",
   459|         "",
   460|         "Excluded from any parameter-row (lftp.*) discovery promotion:",
   461|         "  bip:-1002001 Type Name      -- circular alias of lft.type_name (removed field)",
   462|         "  bip:-1002002 Family Name    -- circular alias of family_name (label-only)",
   463|         "  bip:-1140362/3 Category     -- circular alias of lft.shape_gate.category",
   464|         "  bip:-1002053 Workset        -- operational",
   465|         "  bip:-1002067 Edited by      -- operational",
   466|         "  bip:-1019001/13/15/17 IFC   -- file-local export config",
   467|         "  bip:-1152384 Type Image     -- presentation only",
   468|         "  bip:-1001205 Cost           -- project-local financial",
   469|         "  bip:-1013201 Design Option  -- file-local context",
   470|         "  guid:* pattern              -- project-specific shared params (e.g. Revision) not portable",
   471|         "  Classification group        -- Type Mark, Assembly Code/Description, OmniClass Number/Title,",
   472|         "                                 Code Name, Description, URL, Manufacturer, Model, Keynote;",
   473|         "                                 taxonomy labels useful for label synthesis only"
   474|       ]
   475|     },
   476|     "materials": {
   477|       "join_key_schema": "materials.join_key.v3",
   478|       "hash_alg": "md5_utf8_join_pipe",
   479|       "required_items": [
   480|         "material.graphics_sig_hash_v2",
   481|         "material.keynote"
   482|       ],
   483|       "optional_items": [
   484|         "material.class",
   485|         "material.name_class_hash",
   486|         "material.manufacturer",
   487|         "material.model"
   488|       ],
   489|       "explicitly_excluded_items": [
   490|         "material.uid",
   491|         "material.id_local",
   492|         "material.name",
   493|         "material.description",
   494|         "material.comments",
   495|         "material.keywords",
   496|         "material.cost",
   497|         "material.url",
   498|         "material.mark",
   499|         "material.use_render_appearance",
   500|         "material.shading_color_rgb",
   501|         "material.shading_transparency",
   502|         "material.surface_foreground_pattern.sig_hash",
   503|         "material.surface_foreground_pattern_color_rgb",
   504|         "material.surface_background_pattern.sig_hash",
   505|         "material.surface_background_pattern_color_rgb",
   506|         "material.cut_foreground_pattern.sig_hash",
   507|         "material.cut_foreground_pattern_color_rgb",
   508|         "material.cut_background_pattern.sig_hash",
   509|         "material.cut_background_pattern_color_rgb",
   510|         "material.appearance_asset_capture_status",
   511|         "material.physical_asset_capture_status",
   512|         "material.thermal_asset_capture_status"
   513|       ],
   514|       "notes": [
   515|         "Domain family: materials.",
   516|         "join_hash basis: md5(material.graphics_sig_hash_v2 | material.keynote). Keynote is the governance anchor for join equivalence; graphics_sig_hash_v2 remains the visual/rendering anchor.",
   517|         "Subset policy decision: recognized exception. materials.sig_hash.v2 is pinned to material.name_class_hash while materials.join_key.v3 intentionally uses a disjoint governance/graphics preimage.",
   518|         "material.class and material.name_class_hash are available as identity items for discovery and diagnostics, but are not required join anchors in v3.",
   519|         "material.manufacturer and material.model are over-captured as optional discovery candidates; they are not required join anchors.",
   520|         "material.uid remains traceability-only and excluded from join equivalence.",
   521|         "use_render_appearance is recorded for diagnostics/context but excluded from default signature.",
   522|         "Appearance/Physical/Thermal assets are explicitly deferred."
   523|       ]
   524|     },
   525|     "object_styles_analytical": {
   526|       "join_key_schema": "object_styles_analytical.join_key.v1",
   527|       "hash_alg": "md5_utf8_join_pipe",
   528|       "required_items": [
   529|         "obj_style.row_key",
   530|         "obj_style.weight.projection",
   531|         "obj_style.color.rgb",
   532|         "obj_style.pattern_ref.sig_hash"
   533|       ],
   534|       "optional_items": [],
   535|       "explicitly_excluded_items": [
   536|         "obj_style.weight.cut",
   537|         "obj_style.material_sig_hash"
   538|       ],
   539|       "notes": [
   540|         "Domain family: object_styles.",
   541|         "Contains: CategoryType.AnalyticalModel categories.",
   542|         "Same structure as annotation: cut weight and material not applicable."
   543|       ]
   544|     },
   545|     "object_styles_annotation": {
   546|       "join_key_schema": "object_styles_annotation.join_key.v1",
   547|       "hash_alg": "md5_utf8_join_pipe",
   548|       "required_items": [
   549|         "obj_style.row_key",
   550|         "obj_style.weight.projection",
   551|         "obj_style.color.rgb",
   552|         "obj_style.pattern_ref.sig_hash"
   553|       ],
   554|       "optional_items": [],
   555|       "explicitly_excluded_items": [
   556|         "obj_style.weight.cut",
   557|         "obj_style.material_sig_hash"
   558|       ],
   559|       "notes": [
   560|         "Domain family: object_styles.",
   561|         "Contains: CategoryType.Annotation categories.",
   562|         "Cut weight and material do not exist for Annotation categories in Revit API.",
   563|         "These fields must not be emitted in identity_items for this domain."
   564|       ]
   565|     },
   566|     "object_styles_imported": {
   567|       "join_key_schema": "object_styles_imported.join_key.v1",
   568|       "hash_alg": "md5_utf8_join_pipe",
   569|       "required_items": [
   570|         "obj_style.row_key",
   571|         "obj_style.weight.projection",
   572|         "obj_style.color.rgb",
   573|         "obj_style.pattern_ref.sig_hash"
   574|       ],
   575|       "optional_items": [],
   576|       "explicitly_excluded_items": [
   577|         "obj_style.weight.cut",
   578|         "obj_style.material_sig_hash"
   579|       ],
   580|       "notes": [
   581|         "Domain family: object_styles.",
   582|         "Contains: CategoryType.Imported categories (CAD import artifacts).",
   583|         "Material present but is CAD render material, not governed Revit material — excluded from identity."
   584|       ]
   585|     },
   586|     "object_styles_model": {
   587|       "join_key_schema": "object_styles_model.join_key.v1",
   588|       "hash_alg": "md5_utf8_join_pipe",
   589|       "required_items": [
   590|         "obj_style.row_key",
   591|         "obj_style.weight.projection",
   592|         "obj_style.weight.cut",
   593|         "obj_style.color.rgb",
   594|         "obj_style.pattern_ref.sig_hash"
   595|       ],
   596|       "optional_items": [
   597|         "obj_style.material_sig_hash"
   598|       ],
   599|       "explicitly_excluded_items": [],
   600|       "notes": [
   601|         "Domain family: object_styles.",
   602|         "Contains: CategoryType.Model categories and subcategories.",
   603|         "Cut weight and material are applicable to Model categories.",
   604|         "pattern_ref gate removed — pattern_ref.sig_hash is optional (present for ref patterns, q=missing for solid/none).",
   605|         "pattern_ref.kind NOT in identity_items — only needed for the old gate which is removed.",
   606|         "row_key encodes category path and provides primary identity."
   607|       ]
   608|     },
   609|     "phase_filters": {
   610|       "join_key_schema": "phase_filters.join_key.v2",
   611|       "hash_alg": "md5_utf8_join_pipe",
   612|       "required_items": [
   613|         "phase_filter.demolished.presentation_id",
   614|         "phase_filter.existing.presentation_id",
   615|         "phase_filter.new.presentation_id",
   616|         "phase_filter.temporary.presentation_id"
   617|       ],
   618|       "optional_items": [],
   619|       "explicitly_excluded_items": [
   620|         "phase_filter.name"
   621|       ],
   622|       "notes": [
   623|         "Policy aligned to canonical identity evidence emitted by domains/phase_filters.py.",
   624|         "Join identity is defined by the four per-status presentation ids; phase_filter.name remains a phase2 coordination/label field only.",
   625|         "This replaces the provisional name-only policy, which no longer matched the exported identity_items surface."
   626|       ]
   627|     },
   628|     "phases": {
   629|       "join_key_schema": "phases.join_key.v1",
   630|       "hash_alg": "md5_utf8_join_pipe",
   631|       "required_items": [
   632|         "phase.name"
   633|       ],
   634|       "optional_items": [
   635|         "phase.seq"
   636|       ],
   637|       "explicitly_excluded_items": [],
   638|       "notes": [
   639|         "Policy is provisional pending broader samples; current Pareto evidence shows zero fragmentation and zero collisions for both phase.name and phase.seq.",
   640|         "phase.name is treated as the definition-bearing identifier in current evidence; phase.seq may be added only if future datasets exhibit name collisions."
   641|       ]
   642|     },
   643|     "roof_types": {
   644|       "join_key_schema": "roof_types.join_key.v1",
   645|       "hash_alg": "md5_utf8_join_pipe",
   646|       "required_items": [
   647|         "rt.layer_count",
   648|         "rt.total_thickness_in",
   649|         "rt.stack_hash_loose"
   650|       ],
   651|       "optional_items": [],
   652|       "explicitly_excluded_items": [
   653|         "rt.type_name",
   654|         "rt.coarse_fill_color_rgb"
   655|       ],
   656|       "notes": [
   657|         "Domain family: compound_types.",
   658|         "Basic Roof only — non-basic types blocked."
   659|       ]
   660|     },
   661|     "text_types": {
   662|       "join_key_schema": "text_types.join_key.v2",
   663|       "hash_alg": "md5_utf8_join_pipe",
   664|       "required_items": [
   665|         "text_type.font",
   666|         "text_type.size_in",
   667|         "text_type.bold",
   668|         "text_type.italic",
   669|         "text_type.underline",
   670|         "text_type.color_rgb",
   671|         "text_type.width_factor"
   672|       ],
   673|       "optional_items": [
   674|         "text_type.leader_arrowhead_sig_hash",
   675|         "text_type.background_raw",
   676|         "text_type.line_weight",
   677|         "text_type.show_border",
   678|         "text_type.tab_size_in",
   679|         "text_type.leader_border_offset_in"
   680|       ],
   681|       "explicitly_excluded_items": [
   682|         "text_type.name",
   683|         "text_type.type_id",
   684|         "text_type.type_uid",
   685|         "text_type.leader_arrowhead_uid",
   686|         "text_type.leader_arrowhead_name",
   687|         "text_type.color_int"
   688|       ],
   689|       "notes": [
   690|         "Policy: color_rgb is definition-bearing.",
   691|         "Policy: arrowhead is definition-bearing only via arrowheads-domain signature hash (text_type.leader_arrowhead_sig_hash), not uid/name/id.",
   692|         "If text_type.leader_arrowhead_sig_hash is unavailable for some records, treat as lower confidence rather than substituting uid/name."
   693|       ]
   694|     },
   695|     "units": {
   696|       "join_key_schema": "units.join_key.v1",
   697|       "hash_alg": "md5_utf8_join_pipe",
   698|       "required_items": [
   699|         "units.spec",
   700|         "units.unit_type_id",
   701|         "units.rounding_method"
   702|       ],
   703|       "optional_items": [
   704|         "units.accuracy"
   705|       ],
   706|       "explicitly_excluded_items": [],
   707|       "notes": [
   708|         "Accuracy moved to optional - display precision preference, not behavioral identity.",
   709|         "Spec + type + rounding define the unit system (feet-inches, metric, etc.).",
   710|         "Accuracy variance (0.01 vs 1.0) is display formatting, not calculation behavior.",
   711|         "DECISION: Accuracy affects display only, not technical behavior - treat as refinement."
   712|       ]
   713|     },
   714|     "units_doc": {
   715|       "join_key_schema": "units_doc.join_key.v1",
   716|       "hash_alg": "md5_utf8_join_pipe",
   717|       "required_items": [],
   718|       "optional_items": [
   719|         "units_doc.decimal_symbol",
   720|         "units_doc.digit_grouping_amount",
   721|         "units_doc.digit_grouping_symbol"
   722|       ],
   723|       "explicitly_excluded_items": [],
   724|       "notes": [
   725|         "Single synthetic document-level summary record -- no per-record required discriminator; all fields optional to match the record's own minima (nothing blocks it)."
   726|       ]
   727|     },
   728|     "view_category_overrides": {
   729|       "join_key_schema": "view_category_overrides.join_key.v1",
   730|       "hash_alg": "md5_utf8_join_pipe",
   731|       "required_items": [
   732|         "vco.baseline_category_path",
   733|         "vco.baseline_sig_hash",
   734|         "vco.override_properties_hash"
   735|       ],
   736|       "optional_items": [],
   737|       "explicitly_excluded_items": [
   738|         "vco.category_path",
   739|         "vco.projection.line_weight",
   740|         "vco.projection.line_pattern_ref.sig_hash",
   741|         "vco.projection.line_color.rgb",
   742|         "vco.cut.line_weight",
   743|         "vco.cut.line_pattern_ref.sig_hash",
   744|         "vco.cut.line_color.rgb",
   745|         "vco.halftone",
   746|         "vco.transparency"
   747|       ],
   748|       "notes": [
   749|         "Join-key identity captures the override pattern, not where it's used.",
   750|         "vco.baseline_category_path identifies WHICH category is being overridden.",
   751|         "vco.baseline_sig_hash identifies the object_styles baseline definition being overridden.",
   752|         "vco.override_properties_hash collapses all delta properties into a single behavioral signature.",
   753|         "Individual delta properties (line_weight, color, etc.) are forensic detail, excluded from joins.",
   754|         "Deprecated aggregate domain. Use view_category_overrides_model and view_category_overrides_annotation."
   755|       ]
   756|     },
   757|     "view_category_overrides_annotation": {
   758|       "join_key_schema": "view_category_overrides_annotation.join_key.v1",
   759|       "hash_alg": "md5_utf8_join_pipe",
   760|       "required_items": [
   761|         "vco.baseline_category_path",
   762|         "vco.baseline_sig_hash",
   763|         "vco.override_properties_hash"
   764|       ],
   765|       "optional_items": [],
   766|       "explicitly_excluded_items": [
   767|         "vco.category_path",
   768|         "vco.projection.line_weight",
   769|         "vco.projection.line_pattern_ref.sig_hash",
   770|         "vco.projection.line_color.rgb",
   771|         "vco.halftone",
   772|         "vco.transparency"
   773|       ],
   774|       "notes": [
   775|         "Domain family: view_category_overrides.",
   776|         "Contains: CategoryType.Annotation categories.",
   777|         "Cut weight not applicable to annotation categories — excluded.",
   778|         "include_controlled removed — include state sourced from view_templates.include_vg_annotation.",
   779|         "vco.vg_tab = Annotation in coordination_items for downstream join.",
   780|         "Baseline: object_styles_annotation ctx map."
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
```
