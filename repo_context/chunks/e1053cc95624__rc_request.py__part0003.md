# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 3 of 3
- Original line range: 662-1092
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: generate_packet_from_request, generate_packet_from_request.note_focus_file, generate_packet_from_request._spend_header, _res_to_dict
- Source SHA-256: 9afae857c0fde058a6c80995f21f1d847647d7b327a1e3e9e5318ec40212b388
- Starts inside symbol: no
- Ends inside symbol: no

```
   662| def generate_packet_from_request(root: Path, output_dir: Path, request_path: Path,
   663|                                   name_override: Optional[str] = None) -> tuple:
   664|     """Returns (packet_path_or_None, resolution_report, error_message_or_None).
   665| 
   666|     error_message_or_None is set (and packet_path is None) for a
   667|     structurally invalid request, or when strict mode / an unresolvable
   668|     explicit-selector budget conflict blocks generation -- no partial
   669|     packet is written in either case.
   670|     """
   671|     try:
   672|         request_text = request_path.read_text(encoding="utf-8")
   673|     except OSError as exc:
   674|         return None, [], f"could not read request file {request_path}: {exc}"
   675| 
   676|     request_hash = sha256_text(request_text)
   677|     resolved, errors = parse_and_validate_request(request_text)
   678|     if errors:
   679|         return None, [], "invalid packet_request.json:\n" + "\n".join(f"  - {e}" for e in errors)
   680| 
   681|     files_rows = _load_csv(output_dir / "file_inventory.csv")
   682|     symbols_rows = _load_csv(output_dir / "python_symbols.csv")
   683|     imports_rows = _load_csv(output_dir / "python_imports.csv")
   684|     calls_rows = _load_csv(output_dir / "python_calls.csv")
   685|     files_by_path = {r["relative_path"]: r for r in files_rows}
   686|     symbols_by_file: dict = {}
   687|     for r in symbols_rows:
   688|         symbols_by_file.setdefault(r["relative_path"], []).append(r)
   689| 
   690|     file_resolutions = resolve_files(resolved.files, files_by_path)
   691|     symbol_resolutions = resolve_symbols(resolved.symbols, symbols_rows)
   692|     line_resolutions = resolve_lines(resolved.lines, files_by_path)
   693|     search_resolutions, search_matches_by_term, stale_search_files = resolve_search_terms(
   694|         root, resolved.search_terms, resolved.search_as_regex, files_rows,
   695|     )
   696|     all_resolutions = file_resolutions + symbol_resolutions + line_resolutions + search_resolutions
   697| 
   698|     unresolved_explicit = [r for r in all_resolutions if r.status != "resolved"]
   699|     if resolved.strict and unresolved_explicit:
   700|         lines = [f"  - {r.selector_type} '{r.requested}': {r.status} — {r.detail}" for r in unresolved_explicit]
   701|         return None, [_res_to_dict(r) for r in all_resolutions], (
   702|             "strict mode: aborting because the following selector(s) did not resolve cleanly:\n" + "\n".join(lines)
   703|         )
   704| 
   705|     budget = Budget(max_lines=1_000_000, max_characters=resolved.max_estimated_tokens * 4)
   706|     out: list = []
   707|     focus_files: list = []
   708| 
   709|     communities_by_file: dict = {}
   710|     if resolved.include_graphify:
   711|         _git_for_graphify = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
   712|         communities_by_file, graphify_warnings_for_request = rc_graphify.load_graphify_communities(
   713|             root, _git_for_graphify.get("commit") if _git_for_graphify.get("available") else None,
   714|             current_dirty=_git_for_graphify.get("dirty") if _git_for_graphify.get("available") else None,
   715|         )
   716|         for w in graphify_warnings_for_request:
   717|             budget.omissions.append(f"expansion.include_graphify was requested but unavailable: {w}")
   718| 
   719|     def note_focus_file(rel: str) -> bool:
   720|         if rel in focus_files:
   721|             return True
   722|         if len(focus_files) >= resolved.max_files:
   723|             return False
   724|         focus_files.append(rel)
   725|         return True
   726| 
   727|     # --- Tier 1: explicit selectors (never silently dropped) ---
   728|     # explicit_conflicts collects only "the explicit excerpt itself doesn't
   729|     # fit the budget" -- a hard, must-be-reported-not-truncated conflict.
   730|     # Freshness withholding and non-mandatory expansions (callers/callees/
   731|     # imports/tests) are still recorded as ordinary, non-fatal omissions on
   732|     # `budget` and must NOT abort generation.
   733|     #
   734|     # Two passes, deliberately in this order:
   735|     #   1. Render every explicit selector's own header/excerpt first (and
   736|     #      only once per distinct target -- two selectors naming the same
   737|     #      file/symbol/line render it a single time). None of this may be
   738|     #      pre-empted by expansion content.
   739|     #   2. Only once every explicit item has had its guaranteed shot at the
   740|     #      budget do optional expansions (callers/callees/imports/tests/
   741|     #      Graphify) spend whatever budget remains. Interleaving expansion
   742|     #      spend between explicit items (the previous structure) let one
   743|     #      selector's expansions manufacture a budget conflict for a later,
   744|     #      otherwise-fitting explicit selector.
   745|     explicit_conflicts: list = []
   746|     rendered_files: set = set()
   747|     rendered_symbols: set = set()
   748|     rendered_lines: set = set()
   749|     file_expansion_items: list = []    # [(rel, top_level_rows)]
   750|     symbol_expansion_items: list = []  # [row]
   751| 
   752|     def _spend_header(header: str) -> bool:
   753|         if not budget.allow(header, 2):
   754|             return False
   755|         out.append(header)
   756|         budget.spend(header, 2)
   757|         return True
   758| 
   759|     for res in file_resolutions:
   760|         if res.status != "resolved":
   761|             continue
   762|         rel = res.requested
   763|         if rel in rendered_files:
   764|             # Duplicate selector for a file already attempted -- its first
   765|             # occurrence's outcome (rendered, or a recorded conflict) is
   766|             # final; re-attempting an identical selector would just repeat
   767|             # the same header/budget work (or the same conflict message)
   768|             # once per repeat, which is itself an unbounded-output shape
   769|             # for a request naming the same selector many times over.
   770|             continue
   771|         rendered_files.add(rel)
   772|         if not note_focus_file(rel):
   773|             explicit_conflicts.append(
   774|                 f"explicit file selector `{rel}` does not fit: limits.max_files ({resolved.max_files}) reached"
   775|             )
   776|             continue
   777|         row = res.resolved_rows[0]
   778|         header = _render_origin_header(f"File: `{rel}`", ["explicit_file_selector"])
   779|         if not _spend_header(header):
   780|             explicit_conflicts.append(f"explicit file selector `{rel}` does not fit (header alone exceeds budget)")
   781|             continue
   782|         # Render the mandatory excerpt (the actual content the selector asked
   783|         # for) before the optional top-level-symbols inventory, so the
   784|         # inventory can never spend the shared budget ahead of the excerpt
   785|         # itself and force it into an "explicit_conflicts" abort.
   786|         try:
   787|             line_count = int(row.get("line_count") or 0)
   788|         except ValueError:
   789|             line_count = 0
   790|         if line_count:
   791|             status = _render_excerpt_block(root, rel, 1, line_count, budget, out, row.get("sha256", ""))
   792|             if status == "too_large":
   793|                 explicit_conflicts.append(f"explicit file selector `{rel}` ({line_count} lines) does not fit")
   794|                 continue
   795|         top_level = sorted(
   796|             [r for r in symbols_by_file.get(rel, []) if r["parent_symbol"] == "<module>" and r["symbol_type"] != "module"],
   797|             key=lambda r: int(r["start_line"]),
   798|         )
   799|         # The "Top-level symbols:" inventory itself is optional metadata,
   800|         # same as this symbol's caller/callee/etc. expansions -- deferred
   801|         # to the pass below (after every explicit file/symbol/line
   802|         # selector has had its guaranteed shot at the budget), so file A's
   803|         # inventory can never spend budget that file B's own explicit
   804|         # excerpt needed.
   805|         file_expansion_items.append((rel, top_level))
   806| 
   807|     for res in symbol_resolutions:
   808|         if res.status != "resolved":
   809|             continue
   810|         row = res.resolved_rows[0]
   811|         rel = row["relative_path"]
   812|         symbol_key = (rel, row["qualified_name"])
   813|         if symbol_key in rendered_symbols:
   814|             continue  # duplicate selector; first occurrence's outcome is final
   815|         rendered_symbols.add(symbol_key)
   816|         if not note_focus_file(rel):
   817|             explicit_conflicts.append(
   818|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit: "
   819|                 f"limits.max_files ({resolved.max_files}) reached"
   820|             )
   821|             continue
   822|         header = _render_origin_header(f"Symbol: `{row['qualified_name']}` — `{rel}`", ["explicit_symbol_selector"])
   823|         if not _spend_header(header):
   824|             explicit_conflicts.append(
   825|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit (header alone exceeds budget)"
   826|             )
   827|             continue
   828|         status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
   829|                                         files_by_path.get(rel, {}).get("sha256", ""))
   830|         if status == "too_large":
   831|             explicit_conflicts.append(f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit")
   832|         symbol_expansion_items.append(row)
   833| 
   834|     for res in line_resolutions:
   835|         if res.status != "resolved":
   836|             continue
   837|         info = res.resolved_rows[0]
   838|         rel = info["file"]
   839|         line_key = (rel, info["start"], info["end"])
   840|         if line_key in rendered_lines:
   841|             continue  # duplicate selector; first occurrence's outcome is final
   842|         rendered_lines.add(line_key)
   843|         if not note_focus_file(rel):
   844|             explicit_conflicts.append(
   845|                 f"explicit line selector `{res.requested}` does not fit: limits.max_files ({resolved.max_files}) reached"
   846|             )
   847|             continue
   848|         enclosing = [
   849|             r for r in symbols_by_file.get(rel, [])
   850|             if int(r["start_line"]) <= info["start"] <= int(r["end_line"]) and r["symbol_type"] != "module"
   851|         ]
   852|         header = _render_origin_header(f"Line selector: `{res.requested}`", ["explicit_line_selector"])
   853|         if not _spend_header(header):
   854|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit (header alone exceeds budget)")
   855|             continue
   856|         if enclosing:
   857|             enclosing.sort(key=lambda r: int(r["end_line"]) - int(r["start_line"]))
   858|             row = enclosing[0]
   859|             out.append(f"Enclosing symbol: `{row['qualified_name']}` ({row['symbol_type']}, "
   860|                        f"lines {row['start_line']}-{row['end_line']})\n")
   861|             status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
   862|                                             files_by_path.get(rel, {}).get("sha256", ""))
   863|         else:
   864|             start, end = max(1, info["start"] - 10), info["end"] + 10
   865|             status = _render_excerpt_block(root, rel, start, end, budget, out, files_by_path.get(rel, {}).get("sha256", ""))
   866|         if status == "too_large":
   867|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit")
   868| 
   869|     if explicit_conflicts:
   870|         # An explicit selection itself didn't fit -- either the token
   871|         # budget or limits.max_files -- per contract this is a hard
   872|         # conflict, not something to truncate silently.
   873|         return None, [_res_to_dict(r) for r in all_resolutions], (
   874|             "the requested explicit selector(s) do not fit within limits.max_estimated_tokens "
   875|             f"({resolved.max_estimated_tokens}) / limits.max_files ({resolved.max_files}); increase the "
   876|             f"relevant limit or narrow the request. Conflicts:\n"
   877|             + "\n".join(f"  - {o}" for o in explicit_conflicts)
   878|         )
   879| 
   880|     # Every explicit selector fit -- now spend whatever budget remains on
   881|     # optional metadata/expansions (callers/callees/imports/tests/
   882|     # Graphify, plus each explicit file selector's own "Top-level
   883|     # symbols:" inventory). Done only now, not interleaved with tier-1's
   884|     # rendering above, so a symbol's expansions -- or one file's inventory
   885|     # -- can never manufacture a budget conflict for a later explicit
   886|     # selector.
   887|     for rel, top_level in file_expansion_items:
   888|         if top_level:
   889|             header = "Top-level symbols:"
   890|             if budget.allow(header, 1):
   891|                 out.append(header)
   892|                 budget.spend(header, 1)
   893|                 for idx, r in enumerate(top_level):
   894|                     line = f"- `{r['qualified_name']}` ({r['symbol_type']}, lines {r['start_line']}-{r['end_line']})"
   895|                     if not budget.allow(line, 1):
   896|                         budget.omissions.append(
   897|                             f"{len(top_level) - idx} more top-level symbol(s) in `{rel}` omitted from the "
   898|                             f"listing (packet size limit reached); see python_symbols.csv."
   899|                         )
   900|                         break
   901|                     out.append(line)
   902|                     budget.spend(line, 1)
   903|             else:
   904|                 budget.omissions.append(
   905|                     f"Top-level symbol listing for `{rel}` omitted entirely (packet size limit reached); "
   906|                     f"see python_symbols.csv."
   907|                 )
   908|     for rel, top_level in file_expansion_items:
   909|         for r in top_level:
   910|             _symbol_expansion(r, calls_rows, imports_rows, files_by_path, resolved, budget, out, note_focus_file,
   911|                               communities_by_file)
   912|     for row in symbol_expansion_items:
   913|         _symbol_expansion(row, calls_rows, imports_rows, files_by_path, resolved, budget, out, note_focus_file,
   914|                           communities_by_file)
   915| 
   916|     # --- Tier 2: exact search-term matches ---
   917|     # Matches were already computed by resolve_search_terms() above (before
   918|     # the strict-mode gate) -- reused here rather than re-scanning the
   919|     # repository a second time.
   920|     for term in resolved.search_terms:
   921|         matches = search_matches_by_term.get(term, [])
   922|         term_status = next((r.status for r in search_resolutions if r.requested == term), None)
   923|         if term_status == "invalid":
   924|             notice = f"\n_Search term `{term}` is not a valid regex; skipped._\n"
   925|             if not budget.allow(notice, 1):
   926|                 # Unbounded per-term notices (e.g. a request with hundreds
   927|                 # of invalid regex terms) would otherwise bypass
   928|                 # limits.max_estimated_tokens entirely, same failure shape
   929|                 # as the earlier unbudgeted resolution-report finding.
   930|                 budget.omissions.append(
   931|                     f"Invalid-regex notice for `{term}` omitted (packet size limit reached); "
   932|                     f"see the resolution report."
   933|                 )
   934|                 continue
   935|             out.append(notice)
   936|             budget.spend(notice, 1)
   937|             continue
   938|         header = f"\n### Search: `{term}` ({len(matches)} match(es))\n_Included because: exact_search_match._\n"
   939|         if not budget.allow(header, 1):
   940|             budget.omissions.append(f"Search results for `{term}` omitted entirely (packet size limit reached).")
   941|             continue
   942|         out.append(header); budget.spend(header, 1)
   943|         max_files_note_emitted = False
   944|         for rel, ln, text in matches:
   945|             line_text = redact_secrets(text.strip()[:200])
   946|             line = f"- `{rel}:{ln}` — `{line_text}`"
   947|             # Check the budget *before* reserving a focus-file slot for
   948|             # this match -- otherwise a match that ultimately doesn't fit
   949|             # (and is never rendered) could still consume the one
   950|             # remaining limits.max_files slot, starving a later, shorter
   951|             # match from a different file that would have fit.
   952|             if not budget.allow(line, 1):
   953|                 budget.omissions.append(f"Additional `{term}` matches omitted (packet size limit reached).")
   954|                 break
   955|             if not note_focus_file(rel):
   956|                 # note_focus_file enforces limits.max_files against the
   957|                 # *global* focus-file set shared across every selector/tier
   958|                 # in this packet, not just this one search term -- so the
   959|                 # cap holds even when different terms match different files.
   960|                 # A match in a file beyond the cap doesn't mean every
   961|                 # *later* match is unreachable too -- a later match may be
   962|                 # in a file already in focus_files (e.g. the selected
   963|                 # file), which costs no new slot. Skip this one match and
   964|                 # keep checking the rest instead of abandoning the term.
   965|                 if not max_files_note_emitted:
   966|                     budget.omissions.append(
   967|                         f"Additional `{term}` match(es) beyond limits.max_files ({resolved.max_files}) omitted."
   968|                     )
   969|                     max_files_note_emitted = True
   970|                 continue
   971|             out.append(line); budget.spend(line, 1)
   972|     if stale_search_files:
   973|         budget.omissions.append(f"{stale_search_files} file(s) changed on disk since the last scan and were "
   974|                                  f"skipped for search terms; re-run scan.")
   975| 
   976|     if budget.omissions:
   977|         # The omissions *list* itself is unbounded in memory (a request
   978|         # that triggers hundreds of distinct omission reasons -- e.g. many
   979|         # invalid regex search terms -- can produce hundreds of entries),
   980|         # so rendering it must be budgeted the same way the resolution
   981|         # report is: otherwise this section alone could bypass
   982|         # limits.max_estimated_tokens. The full list is always available,
   983|         # unbudgeted, in the packet_*.resolution.json sidecar's
   984|         # "omissions" field.
   985|         header = "\n## Omitted / unresolved\n"
   986|         if budget.allow(header, 1):
   987|             out.append(header)
   988|             budget.spend(header, 1)
   989|             omitted_omissions = 0
   990|             for idx, o in enumerate(budget.omissions):
   991|                 line = f"- {o}"
   992|                 if not budget.allow(line, 1):
   993|                     omitted_omissions = len(budget.omissions) - idx
   994|                     break
   995|                 out.append(line)
   996|                 budget.spend(line, 1)
   997|             if omitted_omissions:
   998|                 note = (f"- ... and {omitted_omissions} more omission(s) not listed here (packet size limit "
   999|                         f"reached); see the accompanying packet_*.resolution.json's \"omissions\" field for "
  1000|                         f"the complete list.")
  1001|                 if budget.allow(note, 1):
  1002|                     out.append(note)
  1003|                     budget.spend(note, 1)
  1004| 
  1005|     # (Unresolved/ambiguous selectors are reported once, in the "Selector
  1006|     # resolution report" section built into the header below -- not
  1007|     # repeated here.)
  1008| 
  1009|     git_info = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
  1010|     header_lines = [
  1011|         "# Repo Context Packet (from packet_request.json)\n",
  1012|         f"- Root: `{root.resolve().name}`",
  1013|         f"- Question: {resolved.question}",
  1014|         f"- schema_version: {resolved.schema_version}",
  1015|         f"- Tool version: {TOOL_VERSION}",
  1016|         f"- Request file: `{request_path.name}` (sha256: `{request_hash[:16]}…`)",
  1017|     ]
  1018|     if git_info.get("available"):
  1019|         dirty = "dirty" if git_info.get("dirty") else ("clean" if git_info.get("dirty") is False else "unknown")
  1020|         header_lines.append(f"- Repository revision: `{git_info['commit']}` ({dirty} worktree)")
  1021|     else:
  1022|         header_lines.append("- Repository revision: not available (not a git repository, or git is not installed)")
  1023|     header_lines.append(
  1024|         f"- Limits: max_estimated_tokens={resolved.max_estimated_tokens}, max_files={resolved.max_files}, "
  1025|         f"max_hops={resolved.max_hops}"
  1026|     )
  1027| 
  1028|     # The resolution report scales with the *request*, not the source
  1029|     # repository (a request naming hundreds of missing/ambiguous
  1030|     # selectors could otherwise render an unbounded report regardless of
  1031|     # limits.max_estimated_tokens) -- charge it against the same budget
  1032|     # as everything else, with a count-of-omitted note rather than an
  1033|     # unbounded listing. The full, untruncated report is always available
  1034|     # in the accompanying packet_<name>.resolution.json sidecar.
  1035|     resolution_lines = ["## Selector resolution report\n"]
  1036|     omitted_selector_count = 0
  1037|     for idx, r in enumerate(all_resolutions):
  1038|         entry = [f"- {r.selector_type} `{r.requested}`: **{r.status}** — {r.detail}"]
  1039|         entry.extend(f"  - candidate: `{c}`" for c in r.candidates)
  1040|         entry_text = "\n".join(entry)
  1041|         if not budget.allow(entry_text, len(entry)):
  1042|             omitted_selector_count = len(all_resolutions) - idx
  1043|             break
  1044|         resolution_lines.extend(entry)
  1045|         budget.spend(entry_text, len(entry))
  1046|     if omitted_selector_count:
  1047|         note = (f"- ... and {omitted_selector_count} more selector(s) omitted from this report (packet size "
  1048|                 f"limit reached); see the accompanying packet_*.resolution.json for the complete report.")
  1049|         if budget.allow(note, 1):
  1050|             resolution_lines.append(note)
  1051|             budget.spend(note, 1)
  1052|     resolution_lines.append("")
  1053| 
  1054|     # Computed last so it reflects the resolution report's own budget spend too.
  1055|     header_lines.append(
  1056|         f"- Estimated tokens used: ~{round(budget.chars_used / 4)} "
  1057|         f"(chars_used={budget.chars_used}/{budget.max_characters})\n"
  1058|     )
  1059|     header_lines.extend(resolution_lines)
  1060| 
  1061|     out.append("\n_Static analysis only. Call/import relationships above are candidates, not proof of runtime "
  1062|                 "dispatch. See README.md in this output directory for full limitations._\n")
  1063| 
  1064|     text = "\n".join(header_lines) + "\n".join(out) + "\n"
  1065| 
  1066|     stem = sanitize_stem(name_override) if name_override else sanitize_stem(request_path.stem)
  1067|     packet_path = output_dir / "packets" / f"packet_{stem}.md"
  1068|     atomic_write_text(packet_path, text)
  1069| 
  1070|     resolution_report = [_res_to_dict(r) for r in all_resolutions]
  1071|     sidecar = {
  1072|         "tool_version": TOOL_VERSION,
  1073|         "schema_version": resolved.schema_version,
  1074|         "question": resolved.question,
  1075|         "request_file_sha256": request_hash,
  1076|         "git": git_info,
  1077|         "estimated_tokens_used": round(budget.chars_used / 4),
  1078|         "focus_files": focus_files,
  1079|         "omissions": budget.omissions,
  1080|         "resolution_report": resolution_report,
  1081|     }
  1082|     atomic_write_text(output_dir / "packets" / f"packet_{stem}.resolution.json",
  1083|                        json.dumps(sidecar, indent=2, sort_keys=True) + "\n")
  1084| 
  1085|     return packet_path, resolution_report, None
  1086| 
  1087| 
  1088| def _res_to_dict(r: SelectorResolution) -> dict:
  1089|     return {
  1090|         "selector_type": r.selector_type, "requested": r.requested,
  1091|         "status": r.status, "detail": r.detail, "candidates": r.candidates,
  1092|     }
```
