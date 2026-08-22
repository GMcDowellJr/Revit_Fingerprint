# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 3 of 3
- Original line range: 716-1221
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: generate_packet_from_request, generate_packet_from_request.note_focus_file, generate_packet_from_request._spend_header, generate_packet_from_request._maybe_file_expansion, _res_to_dict
- Source SHA-256: 6c748c6dae6fe757e70a4e79cf617c64dd69f78347f47fb2e3913ec88c0b6a25
- Starts inside symbol: no
- Ends inside symbol: no

```
   716| def generate_packet_from_request(root: Path, output_dir: Path, request_path: Path,
   717|                                   name_override: Optional[str] = None) -> tuple:
   718|     """Returns (packet_path_or_None, resolution_report, error_message_or_None).
   719| 
   720|     error_message_or_None is set (and packet_path is None) for a
   721|     structurally invalid request, or when strict mode / an unresolvable
   722|     explicit-selector budget conflict blocks generation -- no partial
   723|     packet is written in either case.
   724|     """
   725|     try:
   726|         request_text = request_path.read_text(encoding="utf-8")
   727|     except OSError as exc:
   728|         return None, [], f"could not read request file {request_path}: {exc}"
   729| 
   730|     request_hash = sha256_text(request_text)
   731|     resolved, errors = parse_and_validate_request(request_text)
   732|     if errors:
   733|         return None, [], "invalid packet_request.json:\n" + "\n".join(f"  - {e}" for e in errors)
   734| 
   735|     files_rows = _load_csv(output_dir / "file_inventory.csv")
   736|     symbols_rows = _load_csv(output_dir / "python_symbols.csv")
   737|     imports_rows = _load_csv(output_dir / "python_imports.csv")
   738|     calls_rows = _load_csv(output_dir / "python_calls.csv")
   739|     files_by_path = {r["relative_path"]: r for r in files_rows}
   740|     symbols_by_file: dict = {}
   741|     for r in symbols_rows:
   742|         symbols_by_file.setdefault(r["relative_path"], []).append(r)
   743| 
   744|     file_resolutions = resolve_files(resolved.files, files_by_path)
   745|     symbol_resolutions = resolve_symbols(resolved.symbols, symbols_rows)
   746|     line_resolutions = resolve_lines(resolved.lines, files_by_path)
   747|     search_resolutions, search_matches_by_term, stale_search_files = resolve_search_terms(
   748|         root, resolved.search_terms, resolved.search_as_regex, files_rows, resolved.max_files,
   749|     )
   750|     all_resolutions = file_resolutions + symbol_resolutions + line_resolutions + search_resolutions
   751| 
   752|     unresolved_explicit = [r for r in all_resolutions if r.status != "resolved"]
   753|     if resolved.strict and unresolved_explicit:
   754|         lines = [f"  - {r.selector_type} '{r.requested}': {r.status} — {r.detail}" for r in unresolved_explicit]
   755|         return None, [_res_to_dict(r) for r in all_resolutions], (
   756|             "strict mode: aborting because the following selector(s) did not resolve cleanly:\n" + "\n".join(lines)
   757|         )
   758| 
   759|     budget = Budget(max_lines=1_000_000, max_characters=resolved.max_estimated_tokens * 4)
   760|     out: list = []
   761|     focus_files: list = []
   762| 
   763|     communities_by_file: dict = {}
   764|     if resolved.include_graphify:
   765|         _git_for_graphify = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
   766|         communities_by_file, graphify_warnings_for_request = rc_graphify.load_graphify_communities(
   767|             root, _git_for_graphify.get("commit") if _git_for_graphify.get("available") else None,
   768|             current_dirty=_git_for_graphify.get("dirty") if _git_for_graphify.get("available") else None,
   769|         )
   770|         for w in graphify_warnings_for_request:
   771|             budget.omissions.append(f"expansion.include_graphify was requested but unavailable: {w}")
   772| 
   773|     def note_focus_file(rel: str) -> bool:
   774|         if rel in focus_files:
   775|             return True
   776|         if len(focus_files) >= resolved.max_files:
   777|             return False
   778|         focus_files.append(rel)
   779|         return True
   780| 
   781|     # --- Reserve the fixed framing's budget cost up front ---
   782|     # The header, the selector-resolution report, and the footer are
   783|     # essential, always-rendered provenance -- not optional content -- so
   784|     # none of them can be dropped to make room. But charging their cost
   785|     # only after Tier-1/Tier-2 content had already spent against the full,
   786|     # unreserved budget (as a prior version did) let Tier-1/Tier-2 content
   787|     # spend as if framing were free: a request could "succeed" with
   788|     # Tier-1 content that, combined with the framing charged on top
   789|     # afterward, made the packet's *actual* rendered size exceed
   790|     # limits.max_estimated_tokens even though generation reported success.
   791|     # Reserving framing's cost first means a too-tight budget now
   792|     # correctly surfaces as an explicit_conflicts abort (an explicit
   793|     # selector's excerpt no longer fits once framing's real cost is
   794|     # subtracted) instead of a "successful" packet whose true size is
   795|     # larger than what was requested.
   796|     git_info = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
   797|     header_lines = [
   798|         "# Repo Context Packet (from packet_request.json)\n",
   799|         f"- Root: `{root.resolve().name}`",
   800|         f"- Question: {resolved.question}",
   801|         f"- schema_version: {resolved.schema_version}",
   802|         f"- Tool version: {TOOL_VERSION}",
   803|         f"- Request file: `{request_path.name}` (sha256: `{request_hash[:16]}…`)",
   804|     ]
   805|     if git_info.get("available"):
   806|         dirty = "dirty" if git_info.get("dirty") else ("clean" if git_info.get("dirty") is False else "unknown")
   807|         header_lines.append(f"- Repository revision: `{git_info['commit']}` ({dirty} worktree)")
   808|     else:
   809|         header_lines.append("- Repository revision: not available (not a git repository, or git is not installed)")
   810|     header_lines.append(
   811|         f"- Limits: max_estimated_tokens={resolved.max_estimated_tokens}, max_files={resolved.max_files}, "
   812|         f"max_hops={resolved.max_hops}"
   813|     )
   814|     budget.spend("\n".join(header_lines) + "\n", len(header_lines))
   815| 
   816|     # The resolution report scales with the *request*, not the source
   817|     # repository (a request naming hundreds of missing/ambiguous
   818|     # selectors could otherwise render an unbounded report regardless of
   819|     # limits.max_estimated_tokens) -- charge it against the same budget as
   820|     # everything else, with a count-of-omitted note rather than an
   821|     # unbounded listing. The full, untruncated report is always available
   822|     # in the accompanying packet_<name>.resolution.json sidecar. Computed
   823|     # here (reserved up front, alongside the header) rather than after
   824|     # Tier-1/Tier-2 render, since it depends only on `all_resolutions`
   825|     # (already resolved above), not on anything Tier-1/Tier-2 produce.
   826|     resolution_lines = ["## Selector resolution report\n"]
   827|     omitted_selector_count = 0
   828|     for idx, r in enumerate(all_resolutions):
   829|         entry = [f"- {r.selector_type} `{r.requested}`: **{r.status}** — {r.detail}"]
   830|         entry.extend(f"  - candidate: `{c}`" for c in r.candidates)
   831|         entry_text = "\n".join(entry)
   832|         if not budget.allow(entry_text, len(entry)):
   833|             omitted_selector_count = len(all_resolutions) - idx
   834|             break
   835|         resolution_lines.extend(entry)
   836|         budget.spend(entry_text, len(entry))
   837|     if omitted_selector_count:
   838|         note = (f"- ... and {omitted_selector_count} more selector(s) omitted from this report (packet size "
   839|                 f"limit reached); see the accompanying packet_*.resolution.json for the complete report.")
   840|         if budget.allow(note, 1):
   841|             resolution_lines.append(note)
   842|             budget.spend(note, 1)
   843|     resolution_lines.append("")
   844| 
   845|     # Same fixed-framing reasoning as the header above -- always rendered
   846|     # in full, reserved up front so Tier-1/Tier-2 content can't spend
   847|     # against budget this footer will also need.
   848|     footer = ("\n_Static analysis only. Call/import relationships above are candidates, not proof of runtime "
   849|               "dispatch. See README.md in this output directory for full limitations._\n")
   850|     budget.spend(footer, 1)
   851| 
   852|     # --- Tier 1: explicit selectors (never silently dropped) ---
   853|     # explicit_conflicts collects only "the explicit excerpt itself doesn't
   854|     # fit the budget" -- a hard, must-be-reported-not-truncated conflict.
   855|     # Freshness withholding and non-mandatory expansions (callers/callees/
   856|     # imports/tests) are still recorded as ordinary, non-fatal omissions on
   857|     # `budget` and must NOT abort generation.
   858|     #
   859|     # Two passes, deliberately in this order:
   860|     #   1. Render every explicit selector's own header/excerpt first (and
   861|     #      only once per distinct target -- two selectors naming the same
   862|     #      file/symbol/line render it a single time). None of this may be
   863|     #      pre-empted by expansion content.
   864|     #   2. Only once every explicit item has had its guaranteed shot at the
   865|     #      budget do optional expansions (callers/callees/imports/tests/
   866|     #      Graphify) spend whatever budget remains. Interleaving expansion
   867|     #      spend between explicit items (the previous structure) let one
   868|     #      selector's expansions manufacture a budget conflict for a later,
   869|     #      otherwise-fitting explicit selector.
   870|     explicit_conflicts: list = []
   871|     rendered_files: set = set()
   872|     rendered_symbols: set = set()
   873|     rendered_lines: set = set()
   874|     file_expansion_items: list = []    # [(rel, top_level_rows)]
   875|     symbol_expansion_items: list = []  # [row]
   876| 
   877|     def _spend_header(header: str) -> bool:
   878|         if not budget.allow(header, 2):
   879|             return False
   880|         out.append(header)
   881|         budget.spend(header, 2)
   882|         return True
   883| 
   884|     for res in file_resolutions:
   885|         if res.status != "resolved":
   886|             continue
   887|         rel = res.requested
   888|         if rel in rendered_files:
   889|             # Duplicate selector for a file already attempted -- its first
   890|             # occurrence's outcome (rendered, or a recorded conflict) is
   891|             # final; re-attempting an identical selector would just repeat
   892|             # the same header/budget work (or the same conflict message)
   893|             # once per repeat, which is itself an unbounded-output shape
   894|             # for a request naming the same selector many times over.
   895|             continue
   896|         rendered_files.add(rel)
   897|         if not note_focus_file(rel):
   898|             explicit_conflicts.append(
   899|                 f"explicit file selector `{rel}` does not fit: limits.max_files ({resolved.max_files}) reached"
   900|             )
   901|             continue
   902|         row = res.resolved_rows[0]
   903|         header = _render_origin_header(f"File: `{rel}`", ["explicit_file_selector"])
   904|         if not _spend_header(header):
   905|             explicit_conflicts.append(f"explicit file selector `{rel}` does not fit (header alone exceeds budget)")
   906|             continue
   907|         # Render the mandatory excerpt (the actual content the selector asked
   908|         # for) before the optional top-level-symbols inventory, so the
   909|         # inventory can never spend the shared budget ahead of the excerpt
   910|         # itself and force it into an "explicit_conflicts" abort.
   911|         try:
   912|             line_count = int(row.get("line_count") or 0)
   913|         except ValueError:
   914|             line_count = 0
   915|         if line_count:
   916|             status = _render_excerpt_block(root, rel, 1, line_count, budget, out, row.get("sha256", ""))
   917|             if status == "too_large":
   918|                 explicit_conflicts.append(f"explicit file selector `{rel}` ({line_count} lines) does not fit")
   919|                 continue
   920|         top_level = sorted(
   921|             [r for r in symbols_by_file.get(rel, []) if r["parent_symbol"] == "<module>" and r["symbol_type"] != "module"],
   922|             key=lambda r: int(r["start_line"]),
   923|         )
   924|         # The "Top-level symbols:" inventory itself is optional metadata,
   925|         # same as this symbol's caller/callee/etc. expansions -- deferred
   926|         # to the pass below (after every explicit file/symbol/line
   927|         # selector has had its guaranteed shot at the budget), so file A's
   928|         # inventory can never spend budget that file B's own explicit
   929|         # excerpt needed.
   930|         file_expansion_items.append((rel, top_level))
   931| 
   932|     for res in symbol_resolutions:
   933|         if res.status != "resolved":
   934|             continue
   935|         row = res.resolved_rows[0]
   936|         rel = row["relative_path"]
   937|         symbol_key = (rel, row["qualified_name"])
   938|         if symbol_key in rendered_symbols:
   939|             continue  # duplicate selector; first occurrence's outcome is final
   940|         rendered_symbols.add(symbol_key)
   941|         if not note_focus_file(rel):
   942|             explicit_conflicts.append(
   943|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit: "
   944|                 f"limits.max_files ({resolved.max_files}) reached"
   945|             )
   946|             continue
   947|         header = _render_origin_header(f"Symbol: `{row['qualified_name']}` — `{rel}`", ["explicit_symbol_selector"])
   948|         if not _spend_header(header):
   949|             explicit_conflicts.append(
   950|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit (header alone exceeds budget)"
   951|             )
   952|             continue
   953|         status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
   954|                                         files_by_path.get(rel, {}).get("sha256", ""))
   955|         if status == "too_large":
   956|             explicit_conflicts.append(f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit")
   957|         symbol_expansion_items.append(row)
   958| 
   959|     for res in line_resolutions:
   960|         if res.status != "resolved":
   961|             continue
   962|         info = res.resolved_rows[0]
   963|         rel = info["file"]
   964|         line_key = (rel, info["start"], info["end"])
   965|         if line_key in rendered_lines:
   966|             continue  # duplicate selector; first occurrence's outcome is final
   967|         rendered_lines.add(line_key)
   968|         if not note_focus_file(rel):
   969|             explicit_conflicts.append(
   970|                 f"explicit line selector `{res.requested}` does not fit: limits.max_files ({resolved.max_files}) reached"
   971|             )
   972|             continue
   973|         # Only substitute a smaller enclosing symbol when it contains
   974|         # *both* endpoints of the requested range -- a symbol containing
   975|         # just the start line (the old check) could be smaller than the
   976|         # actual request (e.g. lines 2-7 where line 2's enclosing function
   977|         # ends at line 2), silently truncating the rendered excerpt to
   978|         # that symbol's own bounds while the resolution report still
   979|         # claimed the full range was resolved.
   980|         enclosing = [
   981|             r for r in symbols_by_file.get(rel, [])
   982|             if int(r["start_line"]) <= info["start"] and info["end"] <= int(r["end_line"])
   983|             and r["symbol_type"] != "module"
   984|         ]
   985|         header = _render_origin_header(f"Line selector: `{res.requested}`", ["explicit_line_selector"])
   986|         if not _spend_header(header):
   987|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit (header alone exceeds budget)")
   988|             continue
   989|         if enclosing:
   990|             enclosing.sort(key=lambda r: int(r["end_line"]) - int(r["start_line"]))
   991|             row = enclosing[0]
   992|             enclosing_line = (f"Enclosing symbol: `{row['qualified_name']}` ({row['symbol_type']}, "
   993|                                f"lines {row['start_line']}-{row['end_line']})\n")
   994|             # Metadata, not the mandatory excerpt itself -- budgeted like
   995|             # every other optional line, with a non-fatal skip if it
   996|             # doesn't fit rather than silently rendering it unbudgeted.
   997|             if budget.allow(enclosing_line, 1):
   998|                 out.append(enclosing_line)
   999|                 budget.spend(enclosing_line, 1)
  1000|             else:
  1001|                 budget.omissions.append(
  1002|                     f"Enclosing-symbol note for `{res.requested}` omitted (packet size limit reached)."
  1003|                 )
  1004|             status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
  1005|                                             files_by_path.get(rel, {}).get("sha256", ""))
  1006|         else:
  1007|             start, end = max(1, info["start"] - 10), info["end"] + 10
  1008|             status = _render_excerpt_block(root, rel, start, end, budget, out, files_by_path.get(rel, {}).get("sha256", ""))
  1009|         if status == "too_large":
  1010|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit")
  1011| 
  1012|     if explicit_conflicts:
  1013|         # An explicit selection itself didn't fit -- either the token
  1014|         # budget or limits.max_files -- per contract this is a hard
  1015|         # conflict, not something to truncate silently.
  1016|         return None, [_res_to_dict(r) for r in all_resolutions], (
  1017|             "the requested explicit selector(s) do not fit within limits.max_estimated_tokens "
  1018|             f"({resolved.max_estimated_tokens}) / limits.max_files ({resolved.max_files}); increase the "
  1019|             f"relevant limit or narrow the request. Conflicts:\n"
  1020|             + "\n".join(f"  - {o}" for o in explicit_conflicts)
  1021|         )
  1022| 
  1023|     # Every explicit selector fit -- now spend whatever budget remains on
  1024|     # optional metadata/expansions (callers/callees/imports/tests/
  1025|     # Graphify, plus each explicit file selector's own "Top-level
  1026|     # symbols:" inventory). Done only now, not interleaved with tier-1's
  1027|     # rendering above, so a symbol's expansions -- or one file's inventory
  1028|     # -- can never manufacture a budget conflict for a later explicit
  1029|     # selector.
  1030|     for rel, top_level in file_expansion_items:
  1031|         if top_level:
  1032|             header = "Top-level symbols:"
  1033|             if budget.allow(header, 1):
  1034|                 out.append(header)
  1035|                 budget.spend(header, 1)
  1036|                 for idx, r in enumerate(top_level):
  1037|                     line = f"- `{r['qualified_name']}` ({r['symbol_type']}, lines {r['start_line']}-{r['end_line']})"
  1038|                     if not budget.allow(line, 1):
  1039|                         budget.omissions.append(
  1040|                             f"{len(top_level) - idx} more top-level symbol(s) in `{rel}` omitted from the "
  1041|                             f"listing (packet size limit reached); see python_symbols.csv."
  1042|                         )
  1043|                         break
  1044|                     out.append(line)
  1045|                     budget.spend(line, 1)
  1046|             else:
  1047|                 budget.omissions.append(
  1048|                     f"Top-level symbol listing for `{rel}` omitted entirely (packet size limit reached); "
  1049|                     f"see python_symbols.csv."
  1050|                 )
  1051|     file_level_expansion_done: set = set()
  1052| 
  1053|     def _maybe_file_expansion(rel: str) -> None:
  1054|         # Once per file regardless of how many symbols in it get
  1055|         # expanded, and regardless of whether that file is reached via a
  1056|         # file selector's own top-level symbols or a distinct explicit
  1057|         # symbol selector in the same file.
  1058|         if rel in file_level_expansion_done:
  1059|             return
  1060|         file_level_expansion_done.add(rel)
  1061|         _file_expansion(rel, imports_rows, calls_rows, files_by_path, resolved, budget, out, note_focus_file,
  1062|                          communities_by_file)
  1063| 
  1064|     for rel, top_level in file_expansion_items:
  1065|         for r in top_level:
  1066|             _symbol_expansion(r, calls_rows, resolved, budget, out, note_focus_file)
  1067|         # File-level expansion (imports/related-tests/Graphify peers) must
  1068|         # run for every explicitly selected file, not just ones with
  1069|         # top-level symbols -- a symbol-free file (e.g. an __init__.py
  1070|         # re-export shim, or a plain config module) previously never got
  1071|         # its imports/related-tests/Graphify-peer expansion at all, since
  1072|         # this loop used to skip straight past it when `top_level` was
  1073|         # empty. Those expansions describe the file, not any symbol in
  1074|         # it, so they apply regardless of whether the file happens to
  1075|         # define any top-level symbols.
  1076|         _maybe_file_expansion(rel)
  1077|     for row in symbol_expansion_items:
  1078|         _symbol_expansion(row, calls_rows, resolved, budget, out, note_focus_file)
  1079|         _maybe_file_expansion(row["relative_path"])
  1080| 
  1081|     # --- Tier 2: exact search-term matches ---
  1082|     # Matches were already computed by resolve_search_terms() above (before
  1083|     # the strict-mode gate) -- reused here rather than re-scanning the
  1084|     # repository a second time.
  1085|     for term in resolved.search_terms:
  1086|         matches = search_matches_by_term.get(term, [])
  1087|         term_status = next((r.status for r in search_resolutions if r.requested == term), None)
  1088|         if term_status == "invalid":
  1089|             notice = f"\n_Search term `{term}` is not a valid regex; skipped._\n"
  1090|             if not budget.allow(notice, 1):
  1091|                 # Unbounded per-term notices (e.g. a request with hundreds
  1092|                 # of invalid regex terms) would otherwise bypass
  1093|                 # limits.max_estimated_tokens entirely, same failure shape
  1094|                 # as the earlier unbudgeted resolution-report finding.
  1095|                 budget.omissions.append(
  1096|                     f"Invalid-regex notice for `{term}` omitted (packet size limit reached); "
  1097|                     f"see the resolution report."
  1098|                 )
  1099|                 continue
  1100|             out.append(notice)
  1101|             budget.spend(notice, 1)
  1102|             continue
  1103|         header = f"\n### Search: `{term}` ({len(matches)} match(es))\n_Included because: exact_search_match._\n"
  1104|         if not budget.allow(header, 1):
  1105|             budget.omissions.append(f"Search results for `{term}` omitted entirely (packet size limit reached).")
  1106|             continue
  1107|         out.append(header); budget.spend(header, 1)
  1108|         max_files_note_emitted = False
  1109|         for rel, ln, text in matches:
  1110|             # Redact the full line before truncating it, not after -- a
  1111|             # secret-shaped value whose closing quote falls beyond
  1112|             # character 200 would otherwise have that quote cut off
  1113|             # first, breaking _SECRET_ASSIGNMENT_PATTERN's closing-quote
  1114|             # backreference so redact_secrets() never matches and the
  1115|             # (truncated) secret prefix leaks into the packet.
  1116|             line_text = redact_secrets(text.strip())[:200]
  1117|             line = f"- `{rel}:{ln}` — `{line_text}`"
  1118|             # Check the budget *before* reserving a focus-file slot for
  1119|             # this match -- otherwise a match that ultimately doesn't fit
  1120|             # (and is never rendered) could still consume the one
  1121|             # remaining limits.max_files slot, starving a later, shorter
  1122|             # match from a different file that would have fit.
  1123|             if not budget.allow(line, 1):
  1124|                 budget.omissions.append(f"Additional `{term}` matches omitted (packet size limit reached).")
  1125|                 break
  1126|             if not note_focus_file(rel):
  1127|                 # note_focus_file enforces limits.max_files against the
  1128|                 # *global* focus-file set shared across every selector/tier
  1129|                 # in this packet, not just this one search term -- so the
  1130|                 # cap holds even when different terms match different files.
  1131|                 # A match in a file beyond the cap doesn't mean every
  1132|                 # *later* match is unreachable too -- a later match may be
  1133|                 # in a file already in focus_files (e.g. the selected
  1134|                 # file), which costs no new slot. Skip this one match and
  1135|                 # keep checking the rest instead of abandoning the term.
  1136|                 if not max_files_note_emitted:
  1137|                     budget.omissions.append(
  1138|                         f"Additional `{term}` match(es) beyond limits.max_files ({resolved.max_files}) omitted."
  1139|                     )
  1140|                     max_files_note_emitted = True
  1141|                 continue
  1142|             out.append(line); budget.spend(line, 1)
  1143|     if stale_search_files:
  1144|         budget.omissions.append(f"{stale_search_files} file(s) changed on disk since the last scan and were "
  1145|                                  f"skipped for search terms; re-run scan.")
  1146| 
  1147|     if budget.omissions:
  1148|         # The omissions *list* itself is unbounded in memory (a request
  1149|         # that triggers hundreds of distinct omission reasons -- e.g. many
  1150|         # invalid regex search terms -- can produce hundreds of entries),
  1151|         # so rendering it must be budgeted the same way the resolution
  1152|         # report is: otherwise this section alone could bypass
  1153|         # limits.max_estimated_tokens. The full list is always available,
  1154|         # unbudgeted, in the packet_*.resolution.json sidecar's
  1155|         # "omissions" field.
  1156|         header = "\n## Omitted / unresolved\n"
  1157|         if budget.allow(header, 1):
  1158|             out.append(header)
  1159|             budget.spend(header, 1)
  1160|             omitted_omissions = 0
  1161|             for idx, o in enumerate(budget.omissions):
  1162|                 line = f"- {o}"
  1163|                 if not budget.allow(line, 1):
  1164|                     omitted_omissions = len(budget.omissions) - idx
  1165|                     break
  1166|                 out.append(line)
  1167|                 budget.spend(line, 1)
  1168|             if omitted_omissions:
  1169|                 note = (f"- ... and {omitted_omissions} more omission(s) not listed here (packet size limit "
  1170|                         f"reached); see the accompanying packet_*.resolution.json's \"omissions\" field for "
  1171|                         f"the complete list.")
  1172|                 if budget.allow(note, 1):
  1173|                     out.append(note)
  1174|                     budget.spend(note, 1)
  1175| 
  1176|     # (Unresolved/ambiguous selectors are reported once, in the "Selector
  1177|     # resolution report" section built into the header below -- not
  1178|     # repeated here. header_lines/resolution_lines/footer were built and
  1179|     # charged against the budget up front, before Tier-1/Tier-2 rendering
  1180|     # -- see the "Reserve the fixed framing's budget cost up front"
  1181|     # comment above.)
  1182| 
  1183|     # Computed last so it reflects Tier-1/Tier-2's and the resolution
  1184|     # report's/footer's own budget spend too.
  1185|     header_lines.append(
  1186|         f"- Estimated tokens used: ~{round(budget.chars_used / 4)} "
  1187|         f"(chars_used={budget.chars_used}/{budget.max_characters})\n"
  1188|     )
  1189|     header_lines.extend(resolution_lines)
  1190| 
  1191|     out.append(footer)
  1192| 
  1193|     text = "\n".join(header_lines) + "\n".join(out) + "\n"
  1194| 
  1195|     stem = sanitize_stem(name_override) if name_override else sanitize_stem(request_path.stem)
  1196|     packet_path = output_dir / "packets" / f"packet_{stem}.md"
  1197|     atomic_write_text(packet_path, text)
  1198| 
  1199|     resolution_report = [_res_to_dict(r) for r in all_resolutions]
  1200|     sidecar = {
  1201|         "tool_version": TOOL_VERSION,
  1202|         "schema_version": resolved.schema_version,
  1203|         "question": resolved.question,
  1204|         "request_file_sha256": request_hash,
  1205|         "git": git_info,
  1206|         "estimated_tokens_used": round(budget.chars_used / 4),
  1207|         "focus_files": focus_files,
  1208|         "omissions": budget.omissions,
  1209|         "resolution_report": resolution_report,
  1210|     }
  1211|     atomic_write_text(output_dir / "packets" / f"packet_{stem}.resolution.json",
  1212|                        json.dumps(sidecar, indent=2, sort_keys=True) + "\n")
  1213| 
  1214|     return packet_path, resolution_report, None
  1215| 
  1216| 
  1217| def _res_to_dict(r: SelectorResolution) -> dict:
  1218|     return {
  1219|         "selector_type": r.selector_type, "requested": r.requested,
  1220|         "status": r.status, "detail": r.detail, "candidates": r.candidates,
  1221|     }
```
