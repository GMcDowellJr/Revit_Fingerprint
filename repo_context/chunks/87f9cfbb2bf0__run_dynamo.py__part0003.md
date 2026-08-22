# Chunk of runner/run_dynamo.py

- Source relative path: `runner/run_dynamo.py`
- Chunk: 3 of 5
- Original line range: 524-923
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: run_fingerprint
- Source SHA-256: ab34a2ab032f21677da5f5b1b79a0089611b4fad01f19f048caf0e2893056e4b
- Starts inside symbol: no
- Ends inside symbol: run_fingerprint

```
   524| def run_fingerprint(doc, timing=None):
   525|     """
   526|     Execute fingerprint extraction on the given document.
   527| 
   528|     Args:
   529|         doc: Revit Document
   530| 
   531|     Returns:
   532|         Dictionary with all domain fingerprints
   533|     """
   534|     start_ts = time.time()
   535| 
   536|     # Context dictionary for cross-domain references
   537|     # Populated by global domains, consumed by contextual domains
   538|     # Environment is read only here; construction is importable without Revit assemblies.
   539|     # Loading and all validation finish before collectors or domains can write output.
   540|     ctx = build_extraction_context(_REPO_ROOT, operator_deployment_config_path())
   541| 
   542|     # PR5: per-run collector cache + counters
   543|     ctx["_collect"] = CollectCtx()
   544| 
   545|     # Timing instrumentation: create collector and wire into subsystems
   546|     _timing = timing if timing is not None else TimingCollector()
   547|     ctx["_timing"] = _timing
   548|     ctx["_collect"].timing = _timing
   549| 
   550|     build_purgeable_id_set(doc, ctx)
   551| 
   552|     # Wire timing into hashing module (module-level ref, never affects hash output)
   553|     try:
   554|         from core import hashing as _hashing_mod
   555|         _hashing_mod._timing_collector = _timing
   556|     except Exception:
   557|         pass
   558| 
   559|     _timing.start_timer("total_extraction")
   560| 
   561|     # PR6: shared document + view context (domains can use for consistent view reads)
   562|     ctx["_doc_view"] = DocViewContext(doc)
   563| 
   564|     # Assemble fingerprint by calling each domain extractor (legacy payloads)
   565|     fingerprint = {}
   566| 
   567|     # Contract envelope (authoritative for statuses)
   568|     contract_domains = {}
   569|     run_diag = contracts.new_run_diag()
   570|     runner_notes = []
   571|     
   572|     # Expose authoritative domain envelopes to extractors for dependency gating.
   573|     # contract_domains is mutated as domains run; ctx sees the live dict.
   574|     ctx["_domains"] = contract_domains
   575| 
   576|     # Metadata domains (no behavioral hash)
   577|     if _enabled("identity"):
   578|         legacy = _domain_run("identity", identity.extract, doc, ctx, contract_domains, run_diag, runner_notes, require_v2_hash=False)
   579|         if legacy is not None:
   580|             fingerprint["identity"] = legacy
   581| 
   582|     if _enabled("units"):
   583|         legacy = _domain_run("units", units.extract, doc, ctx, contract_domains, run_diag, runner_notes)
   584|         if legacy is not None:
   585|             fingerprint["units"] = legacy
   586| 
   587|     if _enabled("units_doc"):
   588|         legacy = _domain_run("units_doc", units.extract_units_doc, doc, ctx, contract_domains, run_diag, runner_notes)
   589|         if legacy is not None:
   590|             fingerprint["units_doc"] = legacy
   591| 
   592|     # Global style domains (locked semantics)
   593|     # NOTE: line_patterns must run first to populate ctx mappings consumed by object_styles/line_styles.
   594|     if _enabled("line_patterns"):
   595|         legacy = _domain_run("line_patterns", line_patterns.extract, doc, ctx, contract_domains, run_diag, runner_notes)
   596|         if legacy is not None:
   597|             fingerprint["line_patterns"] = legacy
   598| 
   599|     # fill_patterns split domains
   600|     if _enabled("fill_patterns_drafting"):
   601|         legacy = _domain_run("fill_patterns_drafting", fill_patterns.extract_drafting, doc, ctx, contract_domains, run_diag, runner_notes)
   602|         if legacy is not None:
   603|             fingerprint["fill_patterns_drafting"] = legacy
   604| 
   605|     if _enabled("fill_patterns_model"):
   606|         legacy = _domain_run("fill_patterns_model", fill_patterns.extract_model, doc, ctx, contract_domains, run_diag, runner_notes)
   607|         if legacy is not None:
   608|             fingerprint["fill_patterns_model"] = legacy
   609| 
   610|     if _enabled("materials"):
   611|         legacy = _domain_run(
   612|             "materials",
   613|             materials.extract,
   614|             doc,
   615|             ctx,
   616|             contract_domains,
   617|             run_diag,
   618|             runner_notes,
   619|         )
   620|         if legacy is not None:
   621|             fingerprint["materials"] = legacy
   622| 
   623|     # object_styles split domains (model must run first to export baseline map to ctx)
   624|     if _enabled("object_styles_model"):
   625|         legacy = _domain_run("object_styles_model", object_styles.extract_model, doc, ctx, contract_domains, run_diag, runner_notes)
   626|         if legacy is not None:
   627|             fingerprint["object_styles_model"] = legacy
   628| 
   629|     if _enabled("object_styles_annotation"):
   630|         legacy = _domain_run("object_styles_annotation", object_styles.extract_annotation, doc, ctx, contract_domains, run_diag, runner_notes)
   631|         if legacy is not None:
   632|             fingerprint["object_styles_annotation"] = legacy
   633| 
   634|     if _enabled("object_styles_analytical"):
   635|         legacy = _domain_run("object_styles_analytical", object_styles.extract_analytical, doc, ctx, contract_domains, run_diag, runner_notes)
   636|         if legacy is not None:
   637|             fingerprint["object_styles_analytical"] = legacy
   638| 
   639|     if _enabled("object_styles_imported"):
   640|         legacy = _domain_run("object_styles_imported", object_styles.extract_imported, doc, ctx, contract_domains, run_diag, runner_notes)
   641|         if legacy is not None:
   642|             fingerprint["object_styles_imported"] = legacy
   643| 
   644|     if _enabled("line_styles"):
   645|         legacy = _domain_run("line_styles", line_styles.extract, doc, ctx, contract_domains, run_diag, runner_notes)
   646|         if legacy is not None:
   647|             fingerprint["line_styles"] = legacy
   648| 
   649| 
   650|     # compound_types: wall_types partition
   651|     # Hard dependencies: materials (layer material resolution) and
   652|     # fill_patterns (coarse fill pattern sig_hash resolution).
   653|     if _enabled("wall_types"):
   654|         try:
   655|             require_domain(contract_domains, "materials")
   656|             require_domain(contract_domains, "fill_patterns_drafting")
   657|             require_domain(contract_domains, "fill_patterns_model")
   658|             legacy = _domain_run(
   659|                 "wall_types",
   660|                 wall_types.extract_wall_types,
   661|                 doc, ctx, contract_domains, run_diag, runner_notes,
   662|             )
   663|             if legacy is not None:
   664|                 fingerprint["wall_types"] = legacy
   665|         except Blocked as b:
   666|             contract_domains["wall_types"] = contracts.new_domain_envelope(
   667|                 domain="wall_types",
   668|                 domain_version=_DOMAIN_VERSION,
   669|                 status=contracts.DOMAIN_STATUS_BLOCKED,
   670|                 block_reasons=list(b.reasons),
   671|                 diag={"blocked_code": b.code, "upstream": b.upstream},
   672|                 records=None,
   673|                 hash_value=None,
   674|             )
   675|             contracts.add_bounded_error(
   676|                 run_diag, domain="wall_types",
   677|                 status=contracts.DOMAIN_STATUS_BLOCKED,
   678|                 code=b.code, message=";".join(list(b.reasons))
   679|             )
   680| 
   681|     if _enabled("floor_types"):
   682|         try:
   683|             require_domain(contract_domains, "materials")
   684|             require_domain(contract_domains, "fill_patterns_drafting")
   685|             require_domain(contract_domains, "fill_patterns_model")
   686|             legacy = _domain_run(
   687|                 "floor_types",
   688|                 floor_types.extract_floor_types,
   689|                 doc, ctx, contract_domains, run_diag, runner_notes,
   690|             )
   691|             if legacy is not None:
   692|                 fingerprint["floor_types"] = legacy
   693|         except Blocked as b:
   694|             contract_domains["floor_types"] = contracts.new_domain_envelope(
   695|                 domain="floor_types",
   696|                 domain_version=_DOMAIN_VERSION,
   697|                 status=contracts.DOMAIN_STATUS_BLOCKED,
   698|                 block_reasons=list(b.reasons),
   699|                 diag={"blocked_code": b.code, "upstream": b.upstream},
   700|                 records=None,
   701|                 hash_value=None,
   702|             )
   703|             contracts.add_bounded_error(
   704|                 run_diag, domain="floor_types",
   705|                 status=contracts.DOMAIN_STATUS_BLOCKED,
   706|                 code=b.code, message=";".join(list(b.reasons))
   707|             )
   708| 
   709|     if _enabled("roof_types"):
   710|         try:
   711|             require_domain(contract_domains, "materials")
   712|             require_domain(contract_domains, "fill_patterns_drafting")
   713|             require_domain(contract_domains, "fill_patterns_model")
   714|             legacy = _domain_run(
   715|                 "roof_types",
   716|                 roof_types.extract_roof_types,
   717|                 doc, ctx, contract_domains, run_diag, runner_notes,
   718|             )
   719|             if legacy is not None:
   720|                 fingerprint["roof_types"] = legacy
   721|         except Blocked as b:
   722|             contract_domains["roof_types"] = contracts.new_domain_envelope(
   723|                 domain="roof_types",
   724|                 domain_version=_DOMAIN_VERSION,
   725|                 status=contracts.DOMAIN_STATUS_BLOCKED,
   726|                 block_reasons=list(b.reasons),
   727|                 diag={"blocked_code": b.code, "upstream": b.upstream},
   728|                 records=None,
   729|                 hash_value=None,
   730|             )
   731|             contracts.add_bounded_error(
   732|                 run_diag, domain="roof_types",
   733|                 status=contracts.DOMAIN_STATUS_BLOCKED,
   734|                 code=b.code, message=";".join(list(b.reasons))
   735|             )
   736| 
   737|     if _enabled("ceiling_types"):
   738|         try:
   739|             require_domain(contract_domains, "materials")
   740|             require_domain(contract_domains, "fill_patterns_drafting")
   741|             require_domain(contract_domains, "fill_patterns_model")
   742|             legacy = _domain_run(
   743|                 "ceiling_types",
   744|                 ceiling_types.extract_ceiling_types,
   745|                 doc, ctx, contract_domains, run_diag, runner_notes,
   746|             )
   747|             if legacy is not None:
   748|                 fingerprint["ceiling_types"] = legacy
   749|         except Blocked as b:
   750|             contract_domains["ceiling_types"] = contracts.new_domain_envelope(
   751|                 domain="ceiling_types",
   752|                 domain_version=_DOMAIN_VERSION,
   753|                 status=contracts.DOMAIN_STATUS_BLOCKED,
   754|                 block_reasons=list(b.reasons),
   755|                 diag={"blocked_code": b.code, "upstream": b.upstream},
   756|                 records=None,
   757|                 hash_value=None,
   758|             )
   759|             contracts.add_bounded_error(
   760|                 run_diag, domain="ceiling_types",
   761|                 status=contracts.DOMAIN_STATUS_BLOCKED,
   762|                 code=b.code, message=";".join(list(b.reasons))
   763|             )
   764| 
   765|     if _enabled("arrowheads"):
   766|         legacy = _domain_run("arrowheads", arrowheads.extract, doc, ctx, contract_domains, run_diag, runner_notes)
   767|         if legacy is not None:
   768|             fingerprint["arrowheads"] = legacy
   769| 
   770|     if _enabled("text_types"):
   771|         legacy = _domain_run("text_types", text_types.extract, doc, ctx, contract_domains, run_diag, runner_notes)
   772|         if legacy is not None:
   773|             fingerprint["text_types"] = legacy
   774| 
   775|     # dimension_types split domains
   776|     if _enabled("dimension_types_linear"):
   777|         legacy = _domain_run("dimension_types_linear", dimension_types.extract_linear, doc, ctx, contract_domains, run_diag, runner_notes)
   778|         if legacy is not None:
   779|             fingerprint["dimension_types_linear"] = legacy
   780| 
   781|     if _enabled("dimension_types_angular"):
   782|         legacy = _domain_run("dimension_types_angular", dimension_types.extract_angular, doc, ctx, contract_domains, run_diag, runner_notes)
   783|         if legacy is not None:
   784|             fingerprint["dimension_types_angular"] = legacy
   785| 
   786|     if _enabled("dimension_types_radial"):
   787|         legacy = _domain_run("dimension_types_radial", dimension_types.extract_radial, doc, ctx, contract_domains, run_diag, runner_notes)
   788|         if legacy is not None:
   789|             fingerprint["dimension_types_radial"] = legacy
   790| 
   791|     if _enabled("dimension_types_diameter"):
   792|         legacy = _domain_run("dimension_types_diameter", dimension_types.extract_diameter, doc, ctx, contract_domains, run_diag, runner_notes)
   793|         if legacy is not None:
   794|             fingerprint["dimension_types_diameter"] = legacy
   795| 
   796|     if _enabled("dimension_types_spot_elevation"):
   797|         legacy = _domain_run("dimension_types_spot_elevation", dimension_types.extract_spot_elevation, doc, ctx, contract_domains, run_diag, runner_notes)
   798|         if legacy is not None:
   799|             fingerprint["dimension_types_spot_elevation"] = legacy
   800| 
   801|     if _enabled("dimension_types_spot_coordinate"):
   802|         legacy = _domain_run("dimension_types_spot_coordinate", dimension_types.extract_spot_coordinate, doc, ctx, contract_domains, run_diag, runner_notes)
   803|         if legacy is not None:
   804|             fingerprint["dimension_types_spot_coordinate"] = legacy
   805| 
   806|     if _enabled("dimension_types_spot_slope"):
   807|         legacy = _domain_run("dimension_types_spot_slope", dimension_types.extract_spot_slope, doc, ctx, contract_domains, run_diag, runner_notes)
   808|         if legacy is not None:
   809|             fingerprint["dimension_types_spot_slope"] = legacy
   810| 
   811|     # New global domains (M4) - run before contextual domains
   812|     # These populate ctx with mappings for views/templates to reference
   813|     if _enabled("view_filter_definitions"):
   814|         legacy = _domain_run(
   815|             "view_filter_definitions",
   816|             view_filter_definitions.extract,
   817|             doc,
   818|             ctx,
   819|             contract_domains,
   820|             run_diag,
   821|             runner_notes,
   822|         )
   823|         if legacy is not None:
   824|             fingerprint["view_filter_definitions"] = legacy
   825| 
   826|     if _enabled("phases"):
   827|         legacy = _domain_run("phases", phases.extract, doc, ctx, contract_domains, run_diag, runner_notes)
   828|         if legacy is not None:
   829|             fingerprint["phases"] = legacy
   830| 
   831|     if _enabled("phase_filters"):
   832|         legacy = _domain_run("phase_filters", phase_filters.extract, doc, ctx, contract_domains, run_diag, runner_notes)
   833|         if legacy is not None:
   834|             fingerprint["phase_filters"] = legacy
   835| 
   836|     # Phase graphics are not exposed via the Revit API (as of 2021–2025).
   837|     # Domain intentionally disabled to avoid misleading fingerprints.
   838|     # if _enabled("phase_graphics"):
   839|     #     fingerprint["phase_graphics"] = phase_graphics.extract(doc, ctx)
   840| 
   841|     # Contract-only emission: phase graphics are known API-unreachable in supported versions.
   842|     # Do not produce a semantic hash.
   843|     if _enabled("phase_graphics"):
   844|         contract_domains["phase_graphics"] = contracts.new_domain_envelope(
   845|             domain="phase_graphics",
   846|             domain_version=_DOMAIN_VERSION,
   847|             status=contracts.DOMAIN_STATUS_UNSUPPORTED,
   848|             block_reasons=["api_unreachable:phase_graphics"],
   849|             diag={
   850|                 "api_reachable": False,
   851|                 "reason": "Phase graphics are not reachable via Revit API in supported versions.",
   852|             },
   853|             records=None,
   854|             hash_value=None,
   855|         )
   856| 
   857|     if _enabled("loaded_family_types"):
   858|         legacy = _domain_run("loaded_family_types", loaded_family_types.extract, doc, ctx, contract_domains, run_diag, runner_notes)
   859|         if legacy is not None:
   860|             fingerprint["loaded_family_types"] = legacy
   861| 
   862|     # Contextual domains (can reference global domains via ctx)
   863|     # Cache pre-warm: populate shared View instances cache before any view-related domain runs.
   864|     # view_filter_applications_view_templates, view_category_overrides, and all
   865|     # view_templates_* domains use _VIEW_INSTANCES_CACHE_KEY so FEC executes exactly once.
   866|     # This block must remain the first view-domain operation in the runner.
   867|     try:
   868|         from domains.view_templates import _VIEW_INSTANCES_CACHE_KEY as _VT_CACHE_KEY
   869|         from core.collect import collect_instances as _ci
   870|         from Autodesk.Revit.DB import View as _View
   871|         _ci(doc, of_class=_View, cctx=ctx["_collect"], cache_key=_VT_CACHE_KEY)
   872|     except Exception:
   873|         pass
   874| 
   875|     if _enabled("view_filter_applications_view_templates"):
   876|         legacy = _domain_run(
   877|             "view_filter_applications_view_templates",
   878|             view_filter_applications_view_templates.extract,
   879|             doc,
   880|             ctx,
   881|             contract_domains,
   882|             run_diag,
   883|             runner_notes,
   884|         )
   885|         if legacy is not None:
   886|             fingerprint["view_filter_applications_view_templates"] = legacy
   887| 
   888|     if _enabled("view_category_overrides_model"):
   889|         # Hard dependencies: must run after object_styles_model + line_patterns.
   890|         # VCO cannot produce meaningful output without the model baseline or
   891|         # line pattern refs, so these remain hard requirements.
   892|         # fill_patterns are soft — graphic_overrides.py degrades gracefully
   893|         # when a fill pattern ref can't be resolved (q=missing on the field).
   894|         try:
   895|             require_domain(contract_domains, "object_styles_model")
   896|             require_domain(contract_domains, "line_patterns")
   897|             # Soft requirements for fill pattern partitions
   898|             for _fp_dep in ["fill_patterns_drafting", "fill_patterns_model"]:
   899|                 if _fp_dep not in contract_domains:
   900|                     runner_notes.append(
   901|                         "view_category_overrides_model: {} not run; "
   902|                         "fill pattern refs in overrides will degrade to q=missing".format(
   903|                             _fp_dep)
   904|                     )
   905|             # Soft requirements — emit note if absent but do not block VCO
   906|             # (these partitions are legitimately empty in most files)
   907|             for _soft_dep in ["object_styles_annotation"]:
   908|                 if _soft_dep not in contract_domains:
   909|                     runner_notes.append(
   910|                         "view_category_overrides_model: {} not run; "
   911|                         "{} category overrides will have no baseline".format(
   912|                             _soft_dep,
   913|                             _soft_dep.replace("object_styles_", "")
   914|                         )
   915|                     )
   916| 
   917|             legacy = _domain_run(
   918|                 "view_category_overrides_model",
   919|                 view_category_overrides_model.extract,
   920|                 doc,
   921|                 ctx,
   922|                 contract_domains,
   923|                 run_diag,
```
