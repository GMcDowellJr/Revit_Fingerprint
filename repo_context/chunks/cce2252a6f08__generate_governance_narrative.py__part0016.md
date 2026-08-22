# Chunk of tools/generate_governance_narrative.py

- Source relative path: `tools/generate_governance_narrative.py`
- Chunk: 16 of 17
- Original line range: 6648-6886
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: main
- Source SHA-256: 7a8e1def8713100e21b852dc24c2c714a8e5330815affb3e703ed572a2829d9c
- Starts inside symbol: main
- Ends inside symbol: no

```
  6648|         _reuse_domain_anchor = (
  6649|             Path(args.reuse_by_client) if args.reuse_by_client
  6650|             else Path(args.reuse_distribution) if args.reuse_distribution
  6651|             # --union-inventory (cross_segment_union_inventory.csv) is written by the
  6652|             # same compare_cross_segment.py invocation to the same --out-dir as the
  6653|             # reuse-distribution family, so it is an equally valid anchor when neither
  6654|             # of the two more specific reuse flags above was supplied.
  6655|             else Path(args.union_inventory) if args.union_inventory
  6656|             else Path(args.summary)
  6657|         )
  6658|         _project_mean_pair_anchor = (
  6659|             Path(args.project_fragmentation_diagnostic) if args.project_fragmentation_diagnostic
  6660|             else Path(args.project_union_jaccard_matrix) if args.project_union_jaccard_matrix
  6661|             else Path(args.project_density_similarity_matrix) if args.project_density_similarity_matrix
  6662|             else Path(args.project_pool_containment_matrix) if args.project_pool_containment_matrix
  6663|             # --matrix-manifest (matrix_output_manifest.csv) is written by the same
  6664|             # compare_cross_segment.py invocation to the same --out-dir as
  6665|             # project_mean_file_pair_jaccard_matrix.csv and every other project_*
  6666|             # matrix (see the single `if matrix_outputs or fragmentation_rows or
  6667|             # matrix_manifest_rows:` write block), so a run that supplies only
  6668|             # --matrix-manifest without any individual --project-* flag still
  6669|             # anchors correctly instead of falling through to --summary's directory.
  6670|             else Path(args.matrix_manifest) if args.matrix_manifest
  6671|             else Path(args.summary)
  6672|         )
  6673|         # D-032: --comparison-registry, when explicitly supplied, overrides the
  6674|         # auto-detected sibling path below -- the same "explicit override, else
  6675|         # fall back to the anchor default" pattern _reuse_domain_anchor/
  6676|         # _project_mean_pair_anchor already use above. Shared by both
  6677|         # input_paths (drives governance_package_health.json's
  6678|         # required_inputs/optional_inputs "present" signal) and sibling_paths
  6679|         # (drives governance_evidence_map.json's artifact entry) below, so the
  6680|         # two never disagree about which file "comparison_registry" means for
  6681|         # this run.
  6682|         _comparison_registry_path = (
  6683|             Path(args.comparison_registry) if args.comparison_registry
  6684|             else Path(args.summary).parent / "comparison_registry.csv"
  6685|         )
  6686|         sibling_paths = {
  6687|             "file_pairs": Path(args.summary).parent / "cross_segment_file_pairs.csv",
  6688|             "comparison_registry": _comparison_registry_path,
  6689|             # D-024: the other two files this generator's own module docstring
  6690|             # names as "not yet consumed directly" (see above) -- both written
  6691|             # by compare_cross_segment.py's main(), anchored beside whichever
  6692|             # related optional input was actually supplied (see _reuse_domain_anchor/
  6693|             # _project_mean_pair_anchor above), falling back to --summary's directory.
  6694|             # Registering them here (rather than leaving them for
  6695|             # inventory_export_directory_files() to discover generically below)
  6696|             # gives each its own governance_evidence_map.json can_answer/
  6697|             # cannot_answer entry instead of a structural-only sentence.
  6698|             "pattern_reuse_summary_by_domain": _reuse_domain_anchor.parent / "pattern_reuse_summary_by_domain.csv",
  6699|             "project_mean_file_pair_jaccard_matrix": _project_mean_pair_anchor.parent / "project_mean_file_pair_jaccard_matrix.csv",
  6700|             "interpretation_guide": INTERPRETATION_GUIDE_PATH,
  6701|             "question_routes": QUESTION_ROUTES_PATH,
  6702|             "reading_order": READING_ORDER_PATH,
  6703|             "classification_rules": CLASSIFICATION_RULES_PATH,
  6704|             "governance_relationships": _relationships_anchor.parent / "governance_relationships.csv",
  6705|         }
  6706|         sibling_present = {k: v.exists() for k, v in sibling_paths.items()}
  6707| 
  6708|         # ── D-034: copy the four static docs/governance/ reference docs into
  6709|         # --out, so a governance package handed to someone without the repo
  6710|         # checked out is still self-contained -- narrative sections/pointers
  6711|         # name these docs by filename, which is meaningless without the file
  6712|         # actually present alongside the rest of the run's output. Only these
  6713|         # four (never the CSV siblings above, e.g. comparison_registry.csv --
  6714|         # D-032 is explicit those are never embedded/reproduced). governance_
  6715|         # evidence_map.json/governance_package_manifest.json's own path/present
  6716|         # fields for these artifacts still describe the checked-in repo doc
  6717|         # (the source of truth), not this copy -- the copy is a convenience,
  6718|         # not a new source of truth, and is silently skipped if the source
  6719|         # doc is missing (e.g. a stripped-down deployment without docs/).
  6720|         for _doc_key in ("interpretation_guide", "question_routes", "reading_order", "classification_rules"):
  6721|             _doc_src = sibling_paths[_doc_key]
  6722|             _doc_dst = out_dir / _doc_src.name
  6723|             if not sibling_present[_doc_key]:
  6724|                 # Source doc absent this run (e.g. a stripped-down deployment
  6725|                 # without docs/) -- remove a copy left by an earlier run over
  6726|                 # this same --out directory so a reader can't pair a stale
  6727|                 # doc with this run's fresh narrative/CSVs.
  6728|                 if _doc_dst.exists():
  6729|                     _doc_dst.unlink()
  6730|                 continue
  6731|             if _doc_src.resolve() != _doc_dst.resolve():
  6732|                 shutil.copy2(_doc_src, _doc_dst)
  6733| 
  6734|         # ── D-023: live file-availability inventory ─────────────────────────────
  6735|         # Scans the cross_segment export directory (--summary's parent) and,
  6736|         # when it differs, the relationship-layer output directory
  6737|         # (_relationships_anchor's parent) and the pattern-reuse/project-matrix
  6738|         # anchor directories (D-024's _reuse_domain_anchor/_project_mean_pair_anchor)
  6739|         # for *.csv files this generator has no artifact_id for yet -- every path
  6740|         # already known as an input, output, or sibling artifact above is
  6741|         # excluded. See D-023 and docs/governance_evidence_package.md. Written
  6742|         # before governance_brief.md so the brief can render a pointer/summary
  6743|         # section from the same already-computed data (no second scan).
  6744|         _export_scan_dirs = []
  6745|         for _d in (Path(args.summary).parent, _relationships_anchor.parent,
  6746|                    _reuse_domain_anchor.parent, _project_mean_pair_anchor.parent):
  6747|             _rd = _d.resolve()
  6748|             if _rd not in {sd.resolve() for sd in _export_scan_dirs}:
  6749|                 _export_scan_dirs.append(_d)
  6750|         _known_artifact_paths = {
  6751|             p for p in list(input_paths.values()) + list(output_paths.values()) + list(sibling_paths.values())
  6752|             if p
  6753|         }
  6754|         _matrix_manifest_by_name = {r["matrix_name"]: r for r in matrix_manifest_rows if r.get("matrix_name")}
  6755|         file_inventory_entries = inventory_export_directory_files(_export_scan_dirs, _known_artifact_paths)
  6756|         for _entry in file_inventory_entries:
  6757|             _entry["narrative"] = _narrative_for_inventory_entry(_entry, _matrix_manifest_by_name)
  6758|         file_inventory_document = build_file_inventory_document(
  6759|             schema_version=FILE_INVENTORY_SCHEMA_VERSION,
  6760|             scanned_directories=_export_scan_dirs,
  6761|             files=file_inventory_entries,
  6762|         )
  6763|         write_json(out_dir / "governance_file_inventory.json", file_inventory_document)
  6764|         if file_inventory_entries:
  6765|             print(f"  → governance_file_inventory.json: {len(file_inventory_entries)} "
  6766|                   f"undiscovered file(s) found under {[str(d) for d in _export_scan_dirs]}")
  6767| 
  6768|         if args.emit_interpretation_layer:
  6769|             print("Writing governance brief...")
  6770|             brief_md = render_governance_brief(
  6771|                 findings=findings, health=health, corpus=corpus,
  6772|                 package_schema_version=args.package_schema_version,
  6773|                 file_inventory=file_inventory_document,
  6774|             )
  6775|             brief_path.write_text(brief_md, encoding="utf-8")
  6776|             print(f"  → {brief_path}")
  6777|         elif (out_dir / "governance_brief.md").exists():
  6778|             # Same staleness-prevention rationale as the emit_evidence_package
  6779|             # opt-out branch below: a prior run may have written governance_brief.md
  6780|             # with --emit-interpretation-layer (the default); if this run turned
  6781|             # only that layer off, the stale brief must not survive alongside a
  6782|             # freshly-written governance_findings.json/package_health.json that
  6783|             # it was never actually built from.
  6784|             (out_dir / "governance_brief.md").unlink()
  6785|             print("  → removed stale governance_brief.md from a prior run "
  6786|                   "(this run used --no-emit-interpretation-layer)")
  6787| 
  6788|         evidence_map = build_evidence_map(
  6789|             schema_version=EVIDENCE_MAP_SCHEMA_VERSION,
  6790|             input_paths=input_paths,
  6791|             input_present=input_present,
  6792|             output_paths=output_paths,
  6793|             sibling_paths=sibling_paths,
  6794|             sibling_present=sibling_present,
  6795|             package_schema_version=args.package_schema_version,
  6796|             file_inventory_schema_version=FILE_INVENTORY_SCHEMA_VERSION,
  6797|             out_dir=out_dir,
  6798|         )
  6799|         write_json(out_dir / "governance_evidence_map.json", evidence_map)
  6800| 
  6801|         # Built and written last, now that governance_package_health.json and
  6802|         # governance_evidence_map.json are actually on disk and stat correctly.
  6803|         # Excludes "governance_package_manifest" from the paths it stats about
  6804|         # itself -- see the comment above for why.
  6805|         manifest_output_paths = {k: v for k, v in output_paths.items() if k != "governance_package_manifest"}
  6806|         manifest_output_types = {k: v for k, v in output_types.items() if k != "governance_package_manifest"}
  6807|         manifest_output_authority = {k: v for k, v in output_authority.items() if k != "governance_package_manifest"}
  6808|         manifest_output_context_role = {k: v for k, v in output_context_role.items() if k != "governance_package_manifest"}
  6809|         policy_profile_ids = {
  6810|             profile_key: {
  6811|                 "profile_id": profile.get("profile_id"),
  6812|                 "schema_version": profile.get("schema_version"),
  6813|                 "source": governance_policy["load_status"][profile_key]["source"],
  6814|             }
  6815|             for profile_key, profile in governance_policy["profiles"].items()
  6816|         }
  6817|         manifest = build_package_manifest(
  6818|             generator_identity=GENERATOR_IDENTITY,
  6819|             generator_role=GENERATOR_ROLE,
  6820|             package_schema_version=args.package_schema_version,
  6821|             analysis_date=args.date,
  6822|             input_paths=input_paths,
  6823|             input_required=input_required,
  6824|             input_roles=input_roles,
  6825|             output_paths=manifest_output_paths,
  6826|             output_types=manifest_output_types,
  6827|             output_authority=manifest_output_authority,
  6828|             output_context_role=manifest_output_context_role,
  6829|             policy_dir=Path(args.policy_dir) if args.policy_dir else None,
  6830|             comparison_run_ids=comparison_run_ids,
  6831|             source_executed_utc=source_executed_utc,
  6832|             policy_profiles=policy_profile_ids,
  6833|         )
  6834|         write_json(out_dir / "governance_package_manifest.json", manifest)
  6835| 
  6836|         print(f"  → wrote governance_package_health.json, governance_findings.json, "
  6837|               f"governance_file_inventory.json, governance_evidence_map.json, "
  6838|               f"governance_package_manifest.json"
  6839|               f"{', governance_brief.md' if args.emit_interpretation_layer else ''} to {out_dir}")
  6840|     else:
  6841|         # A previous run over this same --out directory may have written
  6842|         # package JSONs with --emit-evidence-package (the default). The
  6843|         # narrative just rendered above states plainly that no package
  6844|         # health/findings/evidence-map file exists for this run (see
  6845|         # render_evidence_authority_header's emit_evidence_package gating) --
  6846|         # leaving stale files from an earlier run in place would contradict
  6847|         # that claim and let a downstream reader pick up out-of-date
  6848|         # provenance/health/findings data alongside the freshly-written CSV/MD.
  6849|         stale_names = (
  6850|             "governance_package_manifest.json",
  6851|             "governance_package_health.json",
  6852|             "governance_evidence_map.json",
  6853|             "governance_findings.json",
  6854|             "governance_file_inventory.json",
  6855|             "governance_brief.md",
  6856|             # D-034's copied static docs -- also written only inside the
  6857|             # emit_evidence_package branch above, so they go stale the same
  6858|             # way the JSON artifacts do when a later run opts out.
  6859|             INTERPRETATION_GUIDE_PATH.name,
  6860|             QUESTION_ROUTES_PATH.name,
  6861|             READING_ORDER_PATH.name,
  6862|             CLASSIFICATION_RULES_PATH.name,
  6863|         )
  6864|         # PR review finding: if --out IS docs/governance/ (or any directory
  6865|         # the four static docs actually live in), these names resolve to the
  6866|         # checked-in source documents themselves, not a D-034 copy -- deleting
  6867|         # them would destroy tracked repo docs, not clean up a stale copy.
  6868|         # Same resolve()-comparison guard the copy loop above already uses.
  6869|         _doc_source_paths = {
  6870|             INTERPRETATION_GUIDE_PATH.name, QUESTION_ROUTES_PATH.name,
  6871|             READING_ORDER_PATH.name, CLASSIFICATION_RULES_PATH.name,
  6872|         }
  6873|         _doc_sources_resolved = {
  6874|             p.resolve() for p in (
  6875|                 INTERPRETATION_GUIDE_PATH, QUESTION_ROUTES_PATH,
  6876|                 READING_ORDER_PATH, CLASSIFICATION_RULES_PATH,
  6877|             )
  6878|         }
  6879|         removed = [
  6880|             name for name in stale_names if (out_dir / name).exists()
  6881|             and not (name in _doc_source_paths and (out_dir / name).resolve() in _doc_sources_resolved)
  6882|         ]
  6883|         for name in removed:
  6884|             (out_dir / name).unlink()
  6885|         if removed:
  6886|             print(f"  → removed stale evidence-package file(s) from a prior run: {', '.join(removed)}")
```
