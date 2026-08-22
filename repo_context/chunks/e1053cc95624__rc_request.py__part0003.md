# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 3 of 3
- Original line range: 638-1059
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: generate_packet_from_request, generate_packet_from_request.note_focus_file, generate_packet_from_request._spend_header, _res_to_dict
- Source SHA-256: 358ab4a29afbd578ab28fbd6572c753c17a6c386fa593b10e9bd256e75df92a2
- Starts inside symbol: no
- Ends inside symbol: no

```
   638| def generate_packet_from_request(root: Path, output_dir: Path, request_path: Path,
   639|                                   name_override: Optional[str] = None) -> tuple:
   640|     """Returns (packet_path_or_None, resolution_report, error_message_or_None).
   641| 
   642|     error_message_or_None is set (and packet_path is None) for a
   643|     structurally invalid request, or when strict mode / an unresolvable
   644|     explicit-selector budget conflict blocks generation -- no partial
   645|     packet is written in either case.
   646|     """
   647|     try:
   648|         request_text = request_path.read_text(encoding="utf-8")
   649|     except OSError as exc:
   650|         return None, [], f"could not read request file {request_path}: {exc}"
   651| 
   652|     request_hash = sha256_text(request_text)
   653|     resolved, errors = parse_and_validate_request(request_text)
   654|     if errors:
   655|         return None, [], "invalid packet_request.json:\n" + "\n".join(f"  - {e}" for e in errors)
   656| 
   657|     files_rows = _load_csv(output_dir / "file_inventory.csv")
   658|     symbols_rows = _load_csv(output_dir / "python_symbols.csv")
   659|     imports_rows = _load_csv(output_dir / "python_imports.csv")
   660|     calls_rows = _load_csv(output_dir / "python_calls.csv")
   661|     files_by_path = {r["relative_path"]: r for r in files_rows}
   662|     symbols_by_file: dict = {}
   663|     for r in symbols_rows:
   664|         symbols_by_file.setdefault(r["relative_path"], []).append(r)
   665| 
   666|     file_resolutions = resolve_files(resolved.files, files_by_path)
   667|     symbol_resolutions = resolve_symbols(resolved.symbols, symbols_rows)
   668|     line_resolutions = resolve_lines(resolved.lines, files_by_path)
   669|     search_resolutions, search_matches_by_term, stale_search_files = resolve_search_terms(
   670|         root, resolved.search_terms, resolved.search_as_regex, files_rows,
   671|     )
   672|     all_resolutions = file_resolutions + symbol_resolutions + line_resolutions + search_resolutions
   673| 
   674|     unresolved_explicit = [r for r in all_resolutions if r.status != "resolved"]
   675|     if resolved.strict and unresolved_explicit:
   676|         lines = [f"  - {r.selector_type} '{r.requested}': {r.status} — {r.detail}" for r in unresolved_explicit]
   677|         return None, [_res_to_dict(r) for r in all_resolutions], (
   678|             "strict mode: aborting because the following selector(s) did not resolve cleanly:\n" + "\n".join(lines)
   679|         )
   680| 
   681|     budget = Budget(max_lines=1_000_000, max_characters=resolved.max_estimated_tokens * 4)
   682|     out: list = []
   683|     focus_files: list = []
   684| 
   685|     communities_by_file: dict = {}
   686|     if resolved.include_graphify:
   687|         _git_for_graphify = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
   688|         communities_by_file, graphify_warnings_for_request = rc_graphify.load_graphify_communities(
   689|             root, _git_for_graphify.get("commit") if _git_for_graphify.get("available") else None,
   690|             current_dirty=_git_for_graphify.get("dirty") if _git_for_graphify.get("available") else None,
   691|         )
   692|         for w in graphify_warnings_for_request:
   693|             budget.omissions.append(f"expansion.include_graphify was requested but unavailable: {w}")
   694| 
   695|     def note_focus_file(rel: str) -> bool:
   696|         if rel in focus_files:
   697|             return True
   698|         if len(focus_files) >= resolved.max_files:
   699|             return False
   700|         focus_files.append(rel)
   701|         return True
   702| 
   703|     # --- Tier 1: explicit selectors (never silently dropped) ---
   704|     # explicit_conflicts collects only "the explicit excerpt itself doesn't
   705|     # fit the budget" -- a hard, must-be-reported-not-truncated conflict.
   706|     # Freshness withholding and non-mandatory expansions (callers/callees/
   707|     # imports/tests) are still recorded as ordinary, non-fatal omissions on
   708|     # `budget` and must NOT abort generation.
   709|     #
   710|     # Two passes, deliberately in this order:
   711|     #   1. Render every explicit selector's own header/excerpt first (and
   712|     #      only once per distinct target -- two selectors naming the same
   713|     #      file/symbol/line render it a single time). None of this may be
   714|     #      pre-empted by expansion content.
   715|     #   2. Only once every explicit item has had its guaranteed shot at the
   716|     #      budget do optional expansions (callers/callees/imports/tests/
   717|     #      Graphify) spend whatever budget remains. Interleaving expansion
   718|     #      spend between explicit items (the previous structure) let one
   719|     #      selector's expansions manufacture a budget conflict for a later,
   720|     #      otherwise-fitting explicit selector.
   721|     explicit_conflicts: list = []
   722|     rendered_files: set = set()
   723|     rendered_symbols: set = set()
   724|     rendered_lines: set = set()
   725|     file_expansion_items: list = []    # [(top_level_rows)]
   726|     symbol_expansion_items: list = []  # [row]
   727| 
   728|     def _spend_header(header: str) -> bool:
   729|         if not budget.allow(header, 2):
   730|             return False
   731|         out.append(header)
   732|         budget.spend(header, 2)
   733|         return True
   734| 
   735|     for res in file_resolutions:
   736|         if res.status != "resolved":
   737|             continue
   738|         rel = res.requested
   739|         if rel in rendered_files:
   740|             # Duplicate selector for a file already attempted -- its first
   741|             # occurrence's outcome (rendered, or a recorded conflict) is
   742|             # final; re-attempting an identical selector would just repeat
   743|             # the same header/budget work (or the same conflict message)
   744|             # once per repeat, which is itself an unbounded-output shape
   745|             # for a request naming the same selector many times over.
   746|             continue
   747|         rendered_files.add(rel)
   748|         if not note_focus_file(rel):
   749|             explicit_conflicts.append(
   750|                 f"explicit file selector `{rel}` does not fit: limits.max_files ({resolved.max_files}) reached"
   751|             )
   752|             continue
   753|         row = res.resolved_rows[0]
   754|         header = _render_origin_header(f"File: `{rel}`", ["explicit_file_selector"])
   755|         if not _spend_header(header):
   756|             explicit_conflicts.append(f"explicit file selector `{rel}` does not fit (header alone exceeds budget)")
   757|             continue
   758|         # Render the mandatory excerpt (the actual content the selector asked
   759|         # for) before the optional top-level-symbols inventory, so the
   760|         # inventory can never spend the shared budget ahead of the excerpt
   761|         # itself and force it into an "explicit_conflicts" abort.
   762|         try:
   763|             line_count = int(row.get("line_count") or 0)
   764|         except ValueError:
   765|             line_count = 0
   766|         if line_count:
   767|             status = _render_excerpt_block(root, rel, 1, line_count, budget, out, row.get("sha256", ""))
   768|             if status == "too_large":
   769|                 explicit_conflicts.append(f"explicit file selector `{rel}` ({line_count} lines) does not fit")
   770|                 continue
   771|         top_level = sorted(
   772|             [r for r in symbols_by_file.get(rel, []) if r["parent_symbol"] == "<module>" and r["symbol_type"] != "module"],
   773|             key=lambda r: int(r["start_line"]),
   774|         )
   775|         if top_level:
   776|             header = "Top-level symbols:"
   777|             if budget.allow(header, 1):
   778|                 out.append(header)
   779|                 budget.spend(header, 1)
   780|                 for idx, r in enumerate(top_level):
   781|                     line = f"- `{r['qualified_name']}` ({r['symbol_type']}, lines {r['start_line']}-{r['end_line']})"
   782|                     if not budget.allow(line, 1):
   783|                         budget.omissions.append(
   784|                             f"{len(top_level) - idx} more top-level symbol(s) in `{rel}` omitted from the "
   785|                             f"listing (packet size limit reached); see python_symbols.csv."
   786|                         )
   787|                         break
   788|                     out.append(line)
   789|                     budget.spend(line, 1)
   790|             else:
   791|                 budget.omissions.append(
   792|                     f"Top-level symbol listing for `{rel}` omitted entirely (packet size limit reached); "
   793|                     f"see python_symbols.csv."
   794|                 )
   795|         file_expansion_items.append(top_level)
   796| 
   797|     for res in symbol_resolutions:
   798|         if res.status != "resolved":
   799|             continue
   800|         row = res.resolved_rows[0]
   801|         rel = row["relative_path"]
   802|         symbol_key = (rel, row["qualified_name"])
   803|         if symbol_key in rendered_symbols:
   804|             continue  # duplicate selector; first occurrence's outcome is final
   805|         rendered_symbols.add(symbol_key)
   806|         if not note_focus_file(rel):
   807|             explicit_conflicts.append(
   808|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit: "
   809|                 f"limits.max_files ({resolved.max_files}) reached"
   810|             )
   811|             continue
   812|         header = _render_origin_header(f"Symbol: `{row['qualified_name']}` — `{rel}`", ["explicit_symbol_selector"])
   813|         if not _spend_header(header):
   814|             explicit_conflicts.append(
   815|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit (header alone exceeds budget)"
   816|             )
   817|             continue
   818|         status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
   819|                                         files_by_path.get(rel, {}).get("sha256", ""))
   820|         if status == "too_large":
   821|             explicit_conflicts.append(f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit")
   822|         symbol_expansion_items.append(row)
   823| 
   824|     for res in line_resolutions:
   825|         if res.status != "resolved":
   826|             continue
   827|         info = res.resolved_rows[0]
   828|         rel = info["file"]
   829|         line_key = (rel, info["start"], info["end"])
   830|         if line_key in rendered_lines:
   831|             continue  # duplicate selector; first occurrence's outcome is final
   832|         rendered_lines.add(line_key)
   833|         if not note_focus_file(rel):
   834|             explicit_conflicts.append(
   835|                 f"explicit line selector `{res.requested}` does not fit: limits.max_files ({resolved.max_files}) reached"
   836|             )
   837|             continue
   838|         enclosing = [
   839|             r for r in symbols_by_file.get(rel, [])
   840|             if int(r["start_line"]) <= info["start"] <= int(r["end_line"]) and r["symbol_type"] != "module"
   841|         ]
   842|         header = _render_origin_header(f"Line selector: `{res.requested}`", ["explicit_line_selector"])
   843|         if not _spend_header(header):
   844|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit (header alone exceeds budget)")
   845|             continue
   846|         if enclosing:
   847|             enclosing.sort(key=lambda r: int(r["end_line"]) - int(r["start_line"]))
   848|             row = enclosing[0]
   849|             out.append(f"Enclosing symbol: `{row['qualified_name']}` ({row['symbol_type']}, "
   850|                        f"lines {row['start_line']}-{row['end_line']})\n")
   851|             status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
   852|                                             files_by_path.get(rel, {}).get("sha256", ""))
   853|         else:
   854|             start, end = max(1, info["start"] - 10), info["end"] + 10
   855|             status = _render_excerpt_block(root, rel, start, end, budget, out, files_by_path.get(rel, {}).get("sha256", ""))
   856|         if status == "too_large":
   857|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit")
   858| 
   859|     if explicit_conflicts:
   860|         # An explicit selection itself didn't fit -- either the token
   861|         # budget or limits.max_files -- per contract this is a hard
   862|         # conflict, not something to truncate silently.
   863|         return None, [_res_to_dict(r) for r in all_resolutions], (
   864|             "the requested explicit selector(s) do not fit within limits.max_estimated_tokens "
   865|             f"({resolved.max_estimated_tokens}) / limits.max_files ({resolved.max_files}); increase the "
   866|             f"relevant limit or narrow the request. Conflicts:\n"
   867|             + "\n".join(f"  - {o}" for o in explicit_conflicts)
   868|         )
   869| 
   870|     # Every explicit selector fit -- now spend whatever budget remains on
   871|     # optional expansions (callers/callees/imports/tests/Graphify). Done
   872|     # only now, not interleaved with tier-1's rendering above, so a
   873|     # symbol's expansions can never manufacture a budget conflict for a
   874|     # later explicit selector.
   875|     for top_level in file_expansion_items:
   876|         for r in top_level:
   877|             _symbol_expansion(r, calls_rows, imports_rows, files_by_path, resolved, budget, out, note_focus_file,
   878|                               communities_by_file)
   879|     for row in symbol_expansion_items:
   880|         _symbol_expansion(row, calls_rows, imports_rows, files_by_path, resolved, budget, out, note_focus_file,
   881|                           communities_by_file)
   882| 
   883|     # --- Tier 2: exact search-term matches ---
   884|     # Matches were already computed by resolve_search_terms() above (before
   885|     # the strict-mode gate) -- reused here rather than re-scanning the
   886|     # repository a second time.
   887|     for term in resolved.search_terms:
   888|         matches = search_matches_by_term.get(term, [])
   889|         term_status = next((r.status for r in search_resolutions if r.requested == term), None)
   890|         if term_status == "invalid":
   891|             notice = f"\n_Search term `{term}` is not a valid regex; skipped._\n"
   892|             if not budget.allow(notice, 1):
   893|                 # Unbounded per-term notices (e.g. a request with hundreds
   894|                 # of invalid regex terms) would otherwise bypass
   895|                 # limits.max_estimated_tokens entirely, same failure shape
   896|                 # as the earlier unbudgeted resolution-report finding.
   897|                 budget.omissions.append(
   898|                     f"Invalid-regex notice for `{term}` omitted (packet size limit reached); "
   899|                     f"see the resolution report."
   900|                 )
   901|                 continue
   902|             out.append(notice)
   903|             budget.spend(notice, 1)
   904|             continue
   905|         header = f"\n### Search: `{term}` ({len(matches)} match(es))\n_Included because: exact_search_match._\n"
   906|         if not budget.allow(header, 1):
   907|             budget.omissions.append(f"Search results for `{term}` omitted entirely (packet size limit reached).")
   908|             continue
   909|         out.append(header); budget.spend(header, 1)
   910|         max_files_note_emitted = False
   911|         for rel, ln, text in matches:
   912|             line_text = redact_secrets(text.strip()[:200])
   913|             line = f"- `{rel}:{ln}` — `{line_text}`"
   914|             # Check the budget *before* reserving a focus-file slot for
   915|             # this match -- otherwise a match that ultimately doesn't fit
   916|             # (and is never rendered) could still consume the one
   917|             # remaining limits.max_files slot, starving a later, shorter
   918|             # match from a different file that would have fit.
   919|             if not budget.allow(line, 1):
   920|                 budget.omissions.append(f"Additional `{term}` matches omitted (packet size limit reached).")
   921|                 break
   922|             if not note_focus_file(rel):
   923|                 # note_focus_file enforces limits.max_files against the
   924|                 # *global* focus-file set shared across every selector/tier
   925|                 # in this packet, not just this one search term -- so the
   926|                 # cap holds even when different terms match different files.
   927|                 # A match in a file beyond the cap doesn't mean every
   928|                 # *later* match is unreachable too -- a later match may be
   929|                 # in a file already in focus_files (e.g. the selected
   930|                 # file), which costs no new slot. Skip this one match and
   931|                 # keep checking the rest instead of abandoning the term.
   932|                 if not max_files_note_emitted:
   933|                     budget.omissions.append(
   934|                         f"Additional `{term}` match(es) beyond limits.max_files ({resolved.max_files}) omitted."
   935|                     )
   936|                     max_files_note_emitted = True
   937|                 continue
   938|             out.append(line); budget.spend(line, 1)
   939|     if stale_search_files:
   940|         budget.omissions.append(f"{stale_search_files} file(s) changed on disk since the last scan and were "
   941|                                  f"skipped for search terms; re-run scan.")
   942| 
   943|     if budget.omissions:
   944|         # The omissions *list* itself is unbounded in memory (a request
   945|         # that triggers hundreds of distinct omission reasons -- e.g. many
   946|         # invalid regex search terms -- can produce hundreds of entries),
   947|         # so rendering it must be budgeted the same way the resolution
   948|         # report is: otherwise this section alone could bypass
   949|         # limits.max_estimated_tokens. The full list is always available,
   950|         # unbudgeted, in the packet_*.resolution.json sidecar's
   951|         # "omissions" field.
   952|         header = "\n## Omitted / unresolved\n"
   953|         if budget.allow(header, 1):
   954|             out.append(header)
   955|             budget.spend(header, 1)
   956|             omitted_omissions = 0
   957|             for idx, o in enumerate(budget.omissions):
   958|                 line = f"- {o}"
   959|                 if not budget.allow(line, 1):
   960|                     omitted_omissions = len(budget.omissions) - idx
   961|                     break
   962|                 out.append(line)
   963|                 budget.spend(line, 1)
   964|             if omitted_omissions:
   965|                 note = (f"- ... and {omitted_omissions} more omission(s) not listed here (packet size limit "
   966|                         f"reached); see the accompanying packet_*.resolution.json's \"omissions\" field for "
   967|                         f"the complete list.")
   968|                 if budget.allow(note, 1):
   969|                     out.append(note)
   970|                     budget.spend(note, 1)
   971| 
   972|     # (Unresolved/ambiguous selectors are reported once, in the "Selector
   973|     # resolution report" section built into the header below -- not
   974|     # repeated here.)
   975| 
   976|     git_info = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
   977|     header_lines = [
   978|         "# Repo Context Packet (from packet_request.json)\n",
   979|         f"- Root: `{root.resolve().name}`",
   980|         f"- Question: {resolved.question}",
   981|         f"- schema_version: {resolved.schema_version}",
   982|         f"- Tool version: {TOOL_VERSION}",
   983|         f"- Request file: `{request_path.name}` (sha256: `{request_hash[:16]}…`)",
   984|     ]
   985|     if git_info.get("available"):
   986|         dirty = "dirty" if git_info.get("dirty") else ("clean" if git_info.get("dirty") is False else "unknown")
   987|         header_lines.append(f"- Repository revision: `{git_info['commit']}` ({dirty} worktree)")
   988|     else:
   989|         header_lines.append("- Repository revision: not available (not a git repository, or git is not installed)")
   990|     header_lines.append(
   991|         f"- Limits: max_estimated_tokens={resolved.max_estimated_tokens}, max_files={resolved.max_files}, "
   992|         f"max_hops={resolved.max_hops}"
   993|     )
   994| 
   995|     # The resolution report scales with the *request*, not the source
   996|     # repository (a request naming hundreds of missing/ambiguous
   997|     # selectors could otherwise render an unbounded report regardless of
   998|     # limits.max_estimated_tokens) -- charge it against the same budget
   999|     # as everything else, with a count-of-omitted note rather than an
  1000|     # unbounded listing. The full, untruncated report is always available
  1001|     # in the accompanying packet_<name>.resolution.json sidecar.
  1002|     resolution_lines = ["## Selector resolution report\n"]
  1003|     omitted_selector_count = 0
  1004|     for idx, r in enumerate(all_resolutions):
  1005|         entry = [f"- {r.selector_type} `{r.requested}`: **{r.status}** — {r.detail}"]
  1006|         entry.extend(f"  - candidate: `{c}`" for c in r.candidates)
  1007|         entry_text = "\n".join(entry)
  1008|         if not budget.allow(entry_text, len(entry)):
  1009|             omitted_selector_count = len(all_resolutions) - idx
  1010|             break
  1011|         resolution_lines.extend(entry)
  1012|         budget.spend(entry_text, len(entry))
  1013|     if omitted_selector_count:
  1014|         note = (f"- ... and {omitted_selector_count} more selector(s) omitted from this report (packet size "
  1015|                 f"limit reached); see the accompanying packet_*.resolution.json for the complete report.")
  1016|         if budget.allow(note, 1):
  1017|             resolution_lines.append(note)
  1018|             budget.spend(note, 1)
  1019|     resolution_lines.append("")
  1020| 
  1021|     # Computed last so it reflects the resolution report's own budget spend too.
  1022|     header_lines.append(
  1023|         f"- Estimated tokens used: ~{round(budget.chars_used / 4)} "
  1024|         f"(chars_used={budget.chars_used}/{budget.max_characters})\n"
  1025|     )
  1026|     header_lines.extend(resolution_lines)
  1027| 
  1028|     out.append("\n_Static analysis only. Call/import relationships above are candidates, not proof of runtime "
  1029|                 "dispatch. See README.md in this output directory for full limitations._\n")
  1030| 
  1031|     text = "\n".join(header_lines) + "\n".join(out) + "\n"
  1032| 
  1033|     stem = sanitize_stem(name_override) if name_override else sanitize_stem(request_path.stem)
  1034|     packet_path = output_dir / "packets" / f"packet_{stem}.md"
  1035|     atomic_write_text(packet_path, text)
  1036| 
  1037|     resolution_report = [_res_to_dict(r) for r in all_resolutions]
  1038|     sidecar = {
  1039|         "tool_version": TOOL_VERSION,
  1040|         "schema_version": resolved.schema_version,
  1041|         "question": resolved.question,
  1042|         "request_file_sha256": request_hash,
  1043|         "git": git_info,
  1044|         "estimated_tokens_used": round(budget.chars_used / 4),
  1045|         "focus_files": focus_files,
  1046|         "omissions": budget.omissions,
  1047|         "resolution_report": resolution_report,
  1048|     }
  1049|     atomic_write_text(output_dir / "packets" / f"packet_{stem}.resolution.json",
  1050|                        json.dumps(sidecar, indent=2, sort_keys=True) + "\n")
  1051| 
  1052|     return packet_path, resolution_report, None
  1053| 
  1054| 
  1055| def _res_to_dict(r: SelectorResolution) -> dict:
  1056|     return {
  1057|         "selector_type": r.selector_type, "requested": r.requested,
  1058|         "status": r.status, "detail": r.detail, "candidates": r.candidates,
  1059|     }
```
