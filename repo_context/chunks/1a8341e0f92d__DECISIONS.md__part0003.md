# Chunk of DECISIONS.md

- Source relative path: `DECISIONS.md`
- Chunk: 3 of 5
- Original line range: 793-1192
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: 8ed07306f5b9f68e40e1373d6eb567f822e92ba18bed964edfc92c80cb0cb774
- Starts inside symbol: no
- Ends inside symbol: no

```
   793| - `docs/governance_interpretation_guide.md` — a **stable, package-type-level**
   794|   document (not regenerated per run, versioned via its own
   795|   `interpretation_guide_version` header) explaining cascade-field and
   796|   `governance_tier`/`score_reliability` semantics, comparability rules
   797|   (sector, unit system, all-view/used-view), missing-value conventions,
   798|   authority ordering, and a "known bad inferences" section specific to this
   799|   package type.
   800| - `docs/governance_question_routes.md` — a **candidate** question-route
   801|   catalog (all routes at "candidate" maturity per the reference framework's
   802|   own maturity scale — none has a proven history of repeated use for this
   803|   package type yet), following that framework's discovery scaffold
   804|   (Status / Question forms / Intent / Primary+Secondary artifacts /
   805|   Relevant fields / Evidence type / Supported+Unsupported conclusion types /
   806|   Comparability requirements / Common traps / Escalation). Seeded from
   807|   questions this generator already treats as recurring (the leadership
   808|   questions rendered in the narrative, and the ten `governance_findings.json`
   809|   finding types) rather than invented from nothing.
   810| - `governance_brief.md` — the one new **generated, per-run** artifact:
   811|   built by `render_governance_brief()`, which consumes the already-computed
   812|   `findings` list and `governance_package_health.json` directly (no new
   813|   classification logic — the same "consume, not recompute" discipline
   814|   D-020 established), rendering package status, corpus counts, each
   815|   finding category capped at 10–15 items with a pointer to
   816|   `governance_findings.json` for the full list, and the leadership
   817|   questions as a distinctly-marked numbered list. `authority_level:
   818|   convenience_summary`, subordinate to package health, the source CSVs,
   819|   the rollup CSVs, and `governance_findings.json`.
   820| 
   821| A new `--emit-interpretation-layer`/`--no-emit-interpretation-layer` CLI
   822| flag (default: on) controls `governance_brief.md` only, independently of
   823| `--emit-evidence-package` (but only takes effect when that flag is also
   824| on, since the brief depends on findings/health). The two static docs are
   825| unaffected by either flag — they are always listed in
   826| `governance_evidence_map.json` with real `Path.exists()`-based presence,
   827| since they are checked-in repo docs, not per-run outputs.
   828| `governance_narrative_context.md` is retained unchanged as a compatibility
   829| artifact; its authority header gains pointers to all three new artifacts.
   830| 
   831| ### Consequences
   832| - `governance_evidence_map.json` grows from 19 to 22 artifacts.
   833| - No existing classification, scoring, CSV column, or narrative section
   834|   changed — `governance_brief.md` computes nothing new, and the two static
   835|   docs are pure documentation.
   836| - Unlike every other "generated this run" evidence-map entry (whose
   837|   `present` is asserted `True` by construction), `governance_brief.md`'s
   838|   `present` is a genuine per-run check, since `--no-emit-interpretation-layer`
   839|   can suppress it while the rest of the package still generates normally —
   840|   a consumer must check this artifact's own `present` field, not just
   841|   package-level flags, before assuming it exists for a given run.
   842| - Script recipes and deterministic extractors (the next two rungs on the
   843|   reference framework's promotion ladder: ad hoc question → candidate route
   844|   → active route → recipe → extractor) are explicitly out of scope for this
   845|   phase — no route in `docs/governance_question_routes.md` has earned
   846|   promotion past "candidate" yet.
   847| 
   848| ---
   849| 
   850| ## D-023 — Governance narrative evidence-package layer (Phase 5: live file-availability inventory)
   851| 
   852| ### Status
   853| Accepted (2026-07-22)
   854| 
   855| ### Context
   856| The evidence package (D-019 through D-022) describes, in detail, every
   857| artifact this generator itself reads or writes. It has no way to describe
   858| a file it does *not* read: `compare_cross_segment.py` writes several CSVs
   859| (`pattern_reuse_summary_by_domain.csv`, `project_mean_file_pair_jaccard_matrix.csv`)
   860| that this generator's own code comments already note are "deliberately not
   861| consumed," but that note lived only in Python — no package artifact told a
   862| reader (human or LLM) that these files exist at all. When a question needs
   863| more detail than the rollups carry, the LLM reading this package has no way
   864| to know a candidate drill-down file exists; it either guesses or stonewalls
   865| with "I need more data" and no path forward.
   866| 
   867| Step 0 for this phase confirmed two facts that shape the whole design:
   868| (1) this package has, and will continue to have, no query/tool-calling
   869| path — `generate_governance_narrative.py`'s outputs are consumed single-shot
   870| by a reader that cannot fetch anything itself, so *naming* a file is the
   871| only lever available, not fetching it; (2) no `csv_inventory.md`-style
   872| utility already existed anywhere in this repo to reuse.
   873| 
   874| ### Decision
   875| Add `governance_file_inventory.json`, built fresh on every run by
   876| `inventory_export_directory_files()` (`tools/governance_evidence_package.py`):
   877| a `Path.glob("*.csv")` scan of the cross_segment export directory
   878| (`--summary`'s parent) and, when it differs, the relationship-layer output
   879| directory, excluding every path already tracked as an input, output, or
   880| sibling artifact elsewhere in the package. For each undiscovered file it
   881| records the column header, an inferred per-column dtype (`integer`/`float`/
   882| `boolean`/`string`/`empty`), and the row count — **never a sample row or
   883| cell value**, matching the "type of data, not shape of values" scope
   884| decision for this phase. A short narrative sentence per file is attached in
   885| `generate_governance_narrative.py`: when the filename matches a `matrix_name`
   886| already documented in `matrix_output_manifest.csv`, it reuses that row's own
   887| `interpretation`/`known_limitations` text verbatim (the same free-text
   888| narrative field pattern `compare_cross_segment.py`'s `add_manifest()`
   889| already uses for the registered `project_*` matrix artifacts); otherwise it
   890| falls back to a structural sentence built only from the header/row-count
   891| the scan already computed. Neither path hand-maintains a per-filename
   892| description, so a brand-new future export (a promotion-candidates output,
   893| a future PR) is picked up automatically the next time this generator runs,
   894| with no follow-up edit to this code.
   895| 
   896| `governance_brief.md` renders the same already-scanned data as its own new
   897| `## Detail-Layer File Inventory` section — a directory of what exists at
   898| the detail layer, appended after the leadership questions and deliberately
   899| not interleaved into the per-domain findings sections above it. The
   900| section is entirely omitted (not blank-rendered) when the scan found
   901| nothing undiscovered. `governance_file_inventory.json` is gated by
   902| `--emit-evidence-package` (matching manifest/health/evidence-map/findings);
   903| the `governance_brief.md` section additionally requires
   904| `--emit-interpretation-layer`, matching the rest of the brief's existing
   905| gating. `governance_narrative_context.md` itself is unchanged — this phase
   906| adds no section there, preserving that document's existing documented
   907| guarantee that `--no-emit-evidence-package` leaves CSV/MD outputs
   908| unaffected.
   909| 
   910| `governance_file_inventory` is registered as a 33rd `governance_evidence_map.json`
   911| artifact: `authority_level: authoritative_deterministic_evidence` (the
   912| header/dtype/row-count facts are directly observed, not interpreted), with
   913| an intentionally empty `related_artifacts` — unlike every other entry, the
   914| files this artifact lists vary run to run, so no fixed relationship list
   915| would stay accurate.
   916| 
   917| ### Consequences
   918| - `governance_evidence_map.json` grows to 33 artifacts.
   919| - No existing classification, scoring, CSV column, or narrative section
   920|   changed — the scan only ever describes files nothing else in the package
   921|   already reads, and `governance_brief.md`'s new section is additive,
   922|   omitted entirely when there is nothing to report.
   923| - The design-reference `GMcDowellJr/llm_evidence_framework` repository's
   924|   `discovery/evidence_map_discovery.md` scaffold (candidate evidence-map
   925|   field list, already cited by D-019) covers "what files exist" as an
   926|   explicit evidence-map purpose; this phase is the first to close that gap
   927|   for files this generator does not itself consume, still with no runtime
   928|   dependency on that repository.
   929| - Explicitly out of scope, per this phase's own boundary: any query, fetch,
   930|   or tool-calling mechanism that would let an LLM actually retrieve a named
   931|   file's contents. That remains a different initiative, if it ever happens
   932|   — this phase only makes the file's *existence and shape* discoverable
   933|   within the single-shot package.
   934| - `compare_cross_segment.py` and `build_segment_manifest.py` are unchanged
   935|   (read-only dependency, per this phase's own scope boundary).
   936| 
   937| ---
   938| 
   939| ## D-024 — Governance narrative evidence-package layer (Phase 6: escalation-target file coverage)
   940| 
   941| ### Status
   942| Accepted (2026-07-22)
   943| 
   944| ### Context
   945| `docs/governance_interpretation_guide.md`'s "What to do when a pre-built
   946| route isn't enough" section tells a reader escalating past the compact layer
   947| to "name which large source file is needed" and names
   948| `cross_segment_file_pairs.csv`/`comparison_registry.csv` as examples,
   949| gesturing at "another large sibling artifact the generator never parses."
   950| Before this phase, that gesture was unresolved: a reader had no way to
   951| learn the exhaustive list of such files, and the two named files'
   952| `governance_evidence_map.json` entries carried only hand-written
   953| `context_role`/`can_answer`/`cannot_answer` text — no real column header or
   954| row count — so escalating still meant opening a multi-GB file cold to learn
   955| its schema before the interpretation guide's own step 2 (write a filtered
   956| extraction script) was possible.
   957| 
   958| Step 0 for this phase confirmed two facts against the code, not assumed:
   959| (1) `generate_governance_narrative.py`'s own module docstring already lists
   960| the exhaustive set of files it writes no code path to read: `comparison_
   961| registry.csv`, `cross_segment_file_pairs.csv`, `pattern_reuse_summary_by_
   962| domain.csv`, and `project_mean_file_pair_jaccard_matrix.csv` — four files,
   963| not two; (2) no `csv_inventory.md`-style utility needed rebuilding —
   964| D-023's `_scan_csv_file()` already does exactly the header/dtype/row-count
   965| scan this phase needs, just scoped to files with no artifact_id at all.
   966| 
   967| ### Decision
   968| All four files above are now registered as `sibling_paths` in
   969| `generate_governance_narrative.py`'s `main()` and get a full
   970| `governance_evidence_map.json` artifact entry
   971| (`build_evidence_map()` in `tools/governance_evidence_package.py`) with
   972| `context_role`, `grain`, `can_answer`, `cannot_answer`, and
   973| `known_limitations` text in the same voice as every other archive_only
   974| sibling entry.
   975| 
   976| A new helper, `_sibling_scan_fields(path, present)`, wraps D-023's
   977| `_scan_csv_file()` — no second scanning implementation — to populate each of
   978| the four entries' `columns` (column name + inferred dtype) and `row_count`
   979| fields when the file is present on disk; both fields are simply absent from
   980| the entry when the file is not present, since scanning a nonexistent path is
   981| meaningless, not an all-zeros result. No sample row or cell value is ever
   982| retained, the same "type of data, not shape of values" scope decision D-023
   983| made.
   984| 
   985| Because `pattern_reuse_summary_by_domain.csv` and `project_mean_file_pair_
   986| jaccard_matrix.csv` are now registered `sibling_paths`, they are
   987| automatically excluded from `inventory_export_directory_files()`'s generic
   988| undiscovered-file scan (the same `_known_artifact_paths` exclusion set
   989| D-023 already built) — each file now has exactly one narrative home, not
   990| two competing descriptions of the same file. This is `can_answer`/
   991| `cannot_answer` doing the job it already does, not a second per-file
   992| narrative layer beside it.
   993| 
   994| `pattern_reuse_distribution`'s and `project_fragmentation_diagnostic`'s
   995| `related_artifacts` lists gained a reverse link to their newly-registered
   996| by-domain/mean-file-pair siblings, matching the bidirectional linking
   997| already used elsewhere in the evidence map (e.g.
   998| `pattern_reuse_summary_by_client` already named `pattern_reuse_distribution`
   999| as related).
  1000| 
  1001| **PR-review fix (anchor point):** `pattern_reuse_summary_by_domain.csv` and
  1002| `project_mean_file_pair_jaccard_matrix.csv`'s sibling paths are not
  1003| hard-coded beside `--summary`'s directory. `compare_cross_segment.py`
  1004| writes both files to the same `--out-dir` as already-optional, already-
  1005| CLI-supplied siblings (`pattern_reuse_summary_by_domain.csv` alongside
  1006| `pattern_reuse_summary_by_client.csv`/`pattern_reuse_distribution.csv`;
  1007| `project_mean_file_pair_jaccard_matrix.csv` alongside
  1008| `project_fragmentation_diagnostic.csv` and the other `project_*` matrices),
  1009| so each is anchored to whichever of those related optional flags
  1010| (`--reuse-by-client`/`--reuse-distribution`; `--project-fragmentation-
  1011| diagnostic`/`--project-union-jaccard-matrix`/`--project-density-similarity-
  1012| matrix`/`--project-pool-containment-matrix`) was actually supplied, falling
  1013| back to `--summary`'s directory when none were — the identical pattern
  1014| `_relationships_anchor` already established for `governance_relationships.csv`
  1015| in the prior relationship-layer phase. Without this, a caller running a
  1016| mixed-directory pipeline (optional reuse/project outputs living somewhere
  1017| other than `--summary`'s directory, which the CLI already allows) would get
  1018| a permanently `present: false` entry for these two escalation targets even
  1019| though the real files sit right beside the input they did supply. The
  1020| D-023 live-scan directories (`_export_scan_dirs`) grew to include both new
  1021| anchor directories too, for the same reason `_relationships_anchor.parent`
  1022| was already scanned.
  1023| 
  1024| **Second PR-review follow-up (anchor completeness):** the two anchor chains
  1025| above also fall back to `--union-inventory` (`cross_segment_union_inventory.csv`,
  1026| for `pattern_reuse_summary_by_domain.csv`) and `--matrix-manifest`
  1027| (`matrix_output_manifest.csv`, for `project_mean_file_pair_jaccard_matrix.csv`)
  1028| before falling back to `--summary`'s directory — both are written by the
  1029| same `compare_cross_segment.py` invocation to the same `--out-dir` as their
  1030| respective escalation target (`matrix_output_manifest.csv` in particular
  1031| shares the exact same write block as every `project_*` matrix), so a run
  1032| that supplies only that broader optional input, without any of the more
  1033| specific reuse/project-matrix flags, still anchors correctly instead of
  1034| silently falling through to `--summary`'s directory.
  1035| 
  1036| ### Consequences
  1037| - `governance_evidence_map.json` grows from 33 to 35 artifacts.
  1038| - Three existing tests that used `pattern_reuse_summary_by_domain.csv`/
  1039|   `project_mean_file_pair_jaccard_matrix.csv` as stand-ins for "a generic
  1040|   undiscovered file" (`test_file_inventory_surfaces_an_undiscovered_sibling_csv`,
  1041|   `test_file_inventory_borrows_interpretation_from_matrix_output_manifest`,
  1042|   `test_file_inventory_surfaces_regardless_of_interpretation_layer_flag`)
  1043|   were updated to use fictitious filenames instead — those two real
  1044|   filenames are no longer valid "undiscovered" examples now that they carry
  1045|   their own artifact_id, which is the intended effect of this phase, not a
  1046|   regression.
  1047| - No existing classification, scoring, CSV column, or narrative content
  1048|   changed — this phase only adds evidence-map metadata (context_role,
  1049|   can_answer/cannot_answer, columns, row_count) for files this generator
  1050|   already declared it never reads.
  1051| - `compare_cross_segment.py` and `build_segment_manifest.py` are unchanged
  1052|   (read-only dependency, per this phase's own scope boundary).
  1053| - No query/fetch/tool-calling mechanism was added — the package remains
  1054|   single-shot; a reader still cannot fetch a named file's contents through
  1055|   this package, only see its existence and real shape ahead of writing their
  1056|   own extraction script.
  1057| 
  1058| ---
  1059| 
  1060| ## D-025 — `identity` domain expansion: `project_info.*` fields, included in sig_hash
  1061| 
  1062| ### Status
  1063| Accepted (2026-08-10)
  1064| 
  1065| ### Context
  1066| `domains/identity.py` captured only worksharing status, Revit version/build,
  1067| and file-local lineage signals (central path, filename, project title). It
  1068| never read `doc.ProjectInformation` at all. Step 0 for this area (`audit_
  1069| results/audit_11_domain_extractor_delta_step0_findings.md` §5) confirmed: (1)
  1070| zero overlap with `tools/build_segment_manifest.py`, which never reads Revit
  1071| and sources its governance labels (`client_label`/`discipline_label`/etc.)
  1072| from a hand-curated `file_metadata.csv`, not from `ProjectInformation`; (2)
  1073| the requested built-in fields (Project Number/Status/Address/Issue Date,
  1074| Client Name, Building Name, Organization Name/Description) are confirmed
  1075| Revit built-ins, present on every project's `ProjectInformation` element
  1076| regardless of template; (3) `Office` is a confirmed Stantec-authored shared
  1077| parameter (GUID `6b61afc7-13eb-4af5-8b65-889f978af4f3`), null by design on
  1078| any non-Stantec-template project.
  1079| 
  1080| This creates real tension with this project's own non-negotiable rule
  1081| ("Names are metadata only — never included in behavior hashes unless
  1082| explicitly stated", with D-010's phase-name hash inclusion as the only prior
  1083| exception): most of these new fields (project name, client name, building
  1084| name, organization name/description, address) are literally human-entered
  1085| naming/labeling metadata, not technical behavior. `identity.py`'s existing
  1086| design, however, hashes every item it puts in `identity_items` indiscriminately
  1087| (no separate non-hashed identity_basis slot exists in record.v2 today — the
  1088| precedent for "descriptive but non-hashed" signals, e.g. `project_title`, is
  1089| to live only in `phase2.unknown_items`, entirely outside `identity_basis.items`
  1090| and `sig_hash`).
  1091| 
  1092| ### Decision
  1093| `project_info.*` items ARE included in `identity_items` / `identity_basis.items`
  1094| / `sig_hash` for the `identity` domain — a second explicit, documented
  1095| exception to the "names are metadata only" default rule, alongside D-010.
  1096| This was a deliberate scope call for this domain (the identity domain's own
  1097| module docstring already described itself as "metadata only", and its
  1098| sig_hash was already a thin worksharing/version/build fingerprint, not a
  1099| behavioral one in the sense other domains use) rather than something forced
  1100| by the record.v2 schema.
  1101| 
  1102| To limit the blast radius of that call:
  1103| - `project_info.*` is explicitly excluded from the identity `join_key` policy
  1104|   (`policies/domain_join_key_policies.json`) — it does not participate in
  1105|   cross-project join-key matching, the same treatment `identity.project_title`
  1106|   already got.
  1107| - `status`/`status_reasons`/`identity_quality` remain driven only by the
  1108|   pre-existing core items (`is_workshared`/`revit_version_number`/
  1109|   `revit_version_name`/`revit_build`) — project_info.* fields being blank or
  1110|   legitimately not-applicable (both extremely common: many real projects leave
  1111|   Client Name/Project Status blank, and `Office` is absent on every
  1112|   non-Stantec project by design) must not flip this domain's record status to
  1113|   degraded on every ordinary export.
  1114| - Built-ins use `q=unreadable` only if the `Parameter` object itself is
  1115|   missing (an unexpected API/document gap); `Office` — the only remaining
  1116|   shared/custom field — uses `q=unsupported.not_applicable` when its
  1117|   definition isn't found at all, distinct from `q=missing` (definition
  1118|   present, value blank) and `q=unreadable` (read exception).
  1119| - Built-ins are read via `BuiltInParameter` enum (`pi.get_Parameter(...)`),
  1120|   not `LookupParameter` by display name, so behavior does not depend on
  1121|   Revit's UI display-language locale. **Second PR-review follow-up:** the
  1122|   three IFC GUID fields turned out to be real `BuiltInParameter` members too
  1123|   (`IFC_BUILDING_GUID`/`IFC_PROJECT_GUID`/`IFC_SITE_GUID`, confirmed via
  1124|   `tools/archetype/bip_lookup.json`, a generated BuiltInParameter id→name
  1125|   registry already consumed elsewhere in this repo, e.g.
  1126|   `domains/browser_organization.py`) — not custom/shared parameters as
  1127|   originally assumed — and were moved from the named/shared-field table into
  1128|   the built-in table, so they now follow the same unreadable/missing
  1129|   semantics as the other built-ins rather than Office's not_applicable
  1130|   semantics. `Office` itself is read via its confirmed shared-parameter GUID
  1131|   (`Element.get_Parameter(Guid(...))`, GUID
  1132|   `6b61afc7-13eb-4af5-8b65-889f978af4f3`) rather than `LookupParameter("Office")`
  1133|   by display name, which the Revit API can resolve to an arbitrary same-named
  1134|   parameter if a project happens to contain more than one "Office" definition
  1135|   (first PR-review follow-up); it is the only field left using
  1136|   `LookupParameter`-style name resolution (by GUID, not display name).
  1137| - `identity.py`'s `sig_basis.schema` is bumped `identity.sig_basis.v1` →
  1138|   `.v2`, and the hand-patched `policies/domain_sig_hash_policies.json` entry's
  1139|   `sig_hash_schema` likewise `identity.sig_hash.v1` → `.v2` (PR review
  1140|   follow-up), so a consumer comparing `sig_hash` across a pre-D-025 export and
  1141|   a post-D-025 export can tell the two hash definitions apart instead of
  1142|   reading the resulting hash mismatch as fingerprint drift. **Third PR-review
  1143|   follow-up:** the `.v2` override is also recorded as `sig_hash_schema` in
  1144|   `contracts/domain_identity_keys_v2.json`'s `identity` block itself, not just
  1145|   the derived `policies/domain_sig_hash_policies.json` file — the generator
  1146|   (`tools/generate_sig_hash_policy.py`'s `build_policy()`) defaults any domain
  1147|   lacking that key to `<domain>.sig_hash.v1`, so without this the next
  1148|   regeneration would have silently reverted the version bump.
  1149| - Office's Address/City/State/Zip/Country/Telephone/Fax/Legal Entity
  1150|   sub-fields are deliberately NOT implemented: their exact parameter names
  1151|   need confirmation against a real Stantec-template project, which this
  1152|   change's environment had no live Revit/Dynamo access to do — guessing names
  1153|   that might not match real Stantec parameters was rejected in favor of
  1154|   leaving them for a follow-up once that confirmation is possible.
  1155| - `identity.py`'s `sig_basis.keys_used` (previously a hardcoded 3-of-4-item
  1156|   list that had already drifted from what `sig_hash` actually hashes) is now
  1157|   computed dynamically as every `identity_items` key, fixing that drift as a
  1158|   side effect rather than as a second, separate hash-algorithm change.
  1159|   **Fourth PR-review follow-up:** that fix was initially applied by having
  1160|   `phase2.semantic_keys` share the same dynamically-computed list as
  1161|   `sig_basis.keys_used` — which was wrong, because it made every newly-hashed
  1162|   `project_info.*` naming/label field (and `identity.revit_version_name`,
  1163|   previously cosmetic-only) look like Phase-2 "semantic" (behavior-defining)
  1164|   content, contradicting this same decision's framing of `project_info.*` as
  1165|   metadata whose hash inclusion is an explicit exception, not a behavioral
  1166|   reclassification. `sig_basis_keys_used` (all `identity_items` keys, what
  1167|   `sig_hash` actually hashes) and `semantic_keys` (unchanged from pre-D-025:
  1168|   just `is_workshared`/`revit_version_number`/`revit_build`, what Phase-2
  1169|   calls behavior-defining) are now two separate variables/selectors.
  1170| 
  1171| ### Consequences
  1172| - `identity` domain `sig_hash` values change for every export going forward —
  1173|   expected and intentional, since real project-metadata content is now part
  1174|   of the hash input, not a change to *how* hashes are computed. Previously
  1175|   captured `identity` sig_hash values are not comparable to post-D-025 values.
  1176| - `contracts/domain_identity_keys_v2.json`'s `identity.allowed_keys` and the
  1177|   hand-patched `identity` entry in `policies/domain_sig_hash_policies.json`
  1178|   both grow to include the 13 new `project_info.*` keys (the latter was
  1179|   hand-patched rather than regenerated via `tools/generate_sig_hash_policy.py`,
  1180|   since a full regen would also clobber unrelated hand-tuned `notes` on other
  1181|   domains that have already drifted from a strict mechanical regen).
  1182| - `file_metadata.csv` / `tools/build_segment_manifest.py` / the governance
  1183|   narrative pipeline are unchanged — `project_info.*` are raw Revit reads and
  1184|   are not assumed to reconcile with `file_metadata.csv`'s separately-curated
  1185|   `client_label`/etc.; a later PR may find they frequently disagree, which is
  1186|   worth flagging, not resolved here.
  1187| - Office's 8 sub-fields remain a known gap pending live-Revit confirmation of
  1188|   their exact parameter names.
  1189| 
  1190| ---
  1191| 
  1192| ## D-026 — Backfill: segment lattice is a dimensional powerset, not a single-parent tree
```
