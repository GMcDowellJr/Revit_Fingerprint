# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 2 of 4
- Original line range: 521-765
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _render_origin_header, _render_excerpt_block, _symbol_expansion, _file_expansion
- Source SHA-256: e5af5f07850e5e55e3c59afdede5ca2e2d0d048df70a923234802e541f9466b2
- Starts inside symbol: no
- Ends inside symbol: no

```
   521| 
   522| 
   523| # --- Rendering ------------------------------------------------------------
   524| 
   525| def _render_origin_header(title: str, origins: list) -> str:
   526|     return f"\n### {title}\n_Included because: {', '.join(origins)}._\n"
   527| 
   528| 
   529| def _render_excerpt_block(root: Path, rel_path: str, start: int, end: int, budget: Budget, out: list,
   530|                            expected_sha256: str) -> str:
   531|     """Returns "rendered", "stale" (withheld -- source changed since scan),
   532|     "unavailable" (file missing/unreadable), or "too_large" (would not fit
   533|     within the remaining budget). Only "too_large" is a hard, must-not-be-
   534|     silently-dropped conflict for an *explicit* selector -- "stale" and
   535|     "unavailable" are reported as ordinary (non-fatal) omissions, matching
   536|     the direct --file/--symbol/--line packet path's existing behavior."""
   537|     if not _file_is_fresh(root, rel_path, expected_sha256):
   538|         msg = (f"_Source excerpt withheld: `{rel_path}` has changed on disk since the last `scan` "
   539|                f"(SHA-256 mismatch); re-run `scan` for an up-to-date packet._\n")
   540|         if budget.allow(msg, 1):
   541|             out.append(msg)
   542|             budget.spend(msg, 1)
   543|         budget.omissions.append(f"`{rel_path}` changed since the last scan; excerpt withheld. Re-run scan.")
   544|         return "stale"
   545|     lines = _iter_safe_lines(root, rel_path, start, end)
   546|     if lines is None:
   547|         out.append("_Source excerpt unavailable (file missing or unreadable)._\n")
   548|         budget.omissions.append(f"`{rel_path}` excerpt unavailable (file missing or unreadable).")
   549|         return "unavailable"
   550|     # Stream the requested range rather than materializing it whole up
   551|     # front -- `end` can be an oversized file's real line_count (the
   552|     # scanner deliberately keeps files over MAX_TEXT_READ_BYTES in the
   553|     # inventory, with a real streamed-counted line_count, without ever
   554|     # reading them into memory whole -- see rc_scan.py), so an explicit
   555|     # file selector naming such a file could otherwise allocate hundreds
   556|     # of megabytes or more just to learn the excerpt is "too_large".
   557|     # Track the raw (pre-redaction) size as lines come in and bail out as
   558|     # soon as it clearly exceeds the remaining budget, without reading
   559|     # the rest of the file. redact_secrets() can grow a line slightly (a
   560|     # short matched secret replaced by the fixed-length placeholder), so
   561|     # this early check is deliberately against the raw size, not a
   562|     # substitute for the exact post-redaction budget.allow() check below
   563|     # -- it only ever *skips* reading further, never changes what content
   564|     # that does get read is judged against.
   565|     body_lines = []
   566|     # Reserve room for the "```\n"/"\n```\n" fence markers up front too --
   567|     # they're charged along with `body` below (see `fragment`).
   568|     remaining_chars = budget.max_characters - budget.chars_used - len("```\n\n```\n")
   569|     raw_chars = 0
   570|     too_large = False
   571|     try:
   572|         for ln, text in lines:
   573|             rendered = f"{ln:>6}| {text}"
   574|             body_lines.append(rendered)
   575|             raw_chars += len(rendered) + 1  # +1 for the joining newline
   576|             if raw_chars > remaining_chars:
   577|                 too_large = True
   578|                 break
   579|     finally:
   580|         lines.close()
   581|     if too_large:
   582|         return "too_large"
   583|     # Redact *before* the budget check, not after -- redact_secrets()
   584|     # replaces a matched secret with a placeholder that can be longer
   585|     # than the original text, so checking budget.allow() against the raw
   586|     # body and only redacting afterward let the actually-written content
   587|     # end up bigger than what was verified to fit.
   588|     body = redact_secrets("\n".join(body_lines))
   589|     # Charge the *rendered fragment actually appended to `out`* -- the
   590|     # fenced code block, not just its inner `body` -- or the "```\n"/
   591|     # "\n```\n" fence markers (9 chars) ride along uncounted on every
   592|     # excerpt, letting the packet's true size creep past
   593|     # limits.max_estimated_tokens by a few characters per excerpt.
   594|     fragment = "```\n" + body + "\n```\n"
   595|     if not budget.allow(fragment, len(body_lines)):
   596|         return "too_large"
   597|     out.append(fragment)
   598|     budget.spend(fragment, len(body_lines))
   599|     return "rendered"
   600| 
   601| 
   602| def _symbol_expansion(row: dict, calls_rows: list, req: ResolvedRequest, budget: Budget, out: list,
   603|                        note_focus_file) -> None:
   604|     """Callers/callees are inherently per-symbol (each symbol has its own
   605|     call graph neighborhood), unlike imports/related-tests/Graphify peers
   606|     below in _file_expansion(), which describe the containing file and
   607|     must not be repeated once per symbol in it."""
   608|     rel, qn = row["relative_path"], row["qualified_name"]
   609| 
   610|     if req.include_callers:
   611|         callers = [c for c in _bfs_callers(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
   612|         if callers:
   613|             header = f"\nCallers of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
   614|             if budget.allow(header, 1):
   615|                 out.append(header); budget.spend(header, 1)
   616|                 max_files_note_emitted = False
   617|                 for c in callers:
   618|                     line = (f"- `{c['caller_symbol']}` in `{c['caller_file']}`:{c['line']} "
   619|                             f"— `{c['call_expression']}` ({c['confidence']}: {c['explanation']}) "
   620|                             f"[origin: caller_expansion]\n")
   621|                     # Budget-check before reserving a focus-file slot -- a
   622|                     # caller entry that ultimately doesn't fit must not
   623|                     # consume the slot on behalf of content that was never
   624|                     # actually rendered.
   625|                     if not budget.allow(line, 1):
   626|                         budget.omissions.append(f"More callers of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
   627|                         break
   628|                     if not note_focus_file(c["caller_file"]):
   629|                         # A file beyond limits.max_files doesn't mean every
   630|                         # *later* caller is unreachable too -- a later one
   631|                         # may be in a file already in focus_files, which
   632|                         # note_focus_file accepts for free. Skip this entry
   633|                         # and keep checking the rest instead of abandoning
   634|                         # the whole listing.
   635|                         if not max_files_note_emitted:
   636|                             budget.omissions.append(
   637|                                 f"Caller(s) of `{qn}` beyond limits.max_files ({req.max_files}) omitted."
   638|                             )
   639|                             max_files_note_emitted = True
   640|                         continue
   641|                     out.append(line); budget.spend(line, 1)
   642|             else:
   643|                 budget.omissions.append(f"Callers listing for `{qn}` omitted entirely (packet size limit reached).")
   644| 
   645|     if req.include_callees:
   646|         callees = [c for c in _bfs_callees(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
   647|         if callees:
   648|             header = f"\nCallees of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
   649|             if budget.allow(header, 1):
   650|                 out.append(header); budget.spend(header, 1)
   651|                 max_files_note_emitted = False
   652|                 for c in callees:
   653|                     line = (f"- `{c['call_expression']}` at line {c['line']} -> `{c['candidate_symbol']}` "
   654|                             f"in `{c['candidate_file']}` ({c['confidence']}: {c['explanation']}) "
   655|                             f"[origin: callee_expansion]\n")
   656|                     if not budget.allow(line, 1):
   657|                         budget.omissions.append(f"More callees of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
   658|                         break
   659|                     if not note_focus_file(c["candidate_file"]):
   660|                         if not max_files_note_emitted:
   661|                             budget.omissions.append(
   662|                                 f"Callee(s) of `{qn}` beyond limits.max_files ({req.max_files}) omitted."
   663|                             )
   664|                             max_files_note_emitted = True
   665|                         continue
   666|                     out.append(line); budget.spend(line, 1)
   667|             else:
   668|                 budget.omissions.append(f"Callees listing for `{qn}` omitted entirely (packet size limit reached).")
   669| 
   670| 
   671| def _file_expansion(rel: str, imports_rows: list, calls_rows: list, files_by_path: dict,
   672|                      req: ResolvedRequest, budget: Budget, out: list, note_focus_file,
   673|                      communities_by_file: Optional[dict] = None) -> None:
   674|     """Internal imports, related tests, and Graphify community peers all
   675|     describe the *file*, not any one symbol in it -- unlike
   676|     callers/callees above, which are inherently per-symbol. Must be
   677|     called at most once per file regardless of how many of that file's
   678|     symbols are being expanded (see the caller: it used to call this
   679|     content once per top-level symbol, rendering identical "Internal
   680|     imports of X"/"Related tests for X"/"Graphify community peers of X"
   681|     sections over and over for a multi-symbol file)."""
   682|     if req.include_imports:
   683|         file_imports = [i for i in imports_rows if i["source_file"] == rel and i["resolved_file"]]
   684|         if file_imports:
   685|             header = f"\nInternal imports of `{rel}` (import_expansion):\n"
   686|             if budget.allow(header, 1):
   687|                 out.append(header); budget.spend(header, 1)
   688|                 max_files_note_emitted = False
   689|                 for i in file_imports[:20]:
   690|                     line = f"- line {i['line']}: `{i['imported_name'] or i['imported_module']}` -> `{i['resolved_file']}`\n"
   691|                     if not budget.allow(line, 1):
   692|                         break
   693|                     if not note_focus_file(i["resolved_file"]):
   694|                         if not max_files_note_emitted:
   695|                             budget.omissions.append(
   696|                                 f"Import(s) of `{rel}` beyond limits.max_files ({req.max_files}) omitted."
   697|                             )
   698|                             max_files_note_emitted = True
   699|                         continue
   700|                     out.append(line); budget.spend(line, 1)
   701| 
   702|     if req.include_related_tests:
   703|         tests = _candidate_tests_for_file(rel, imports_rows, calls_rows, files_by_path)
   704|         if tests:
   705|             header = f"\nRelated tests for `{rel}` (related_test_expansion):\n"
   706|             if budget.allow(header, 1):
   707|                 out.append(header); budget.spend(header, 1)
   708|                 max_files_note_emitted = False
   709|                 for t in tests:
   710|                     line = f"- `{t}`\n"
   711|                     if not budget.allow(line, 1):
   712|                         break
   713|                     if not note_focus_file(t):
   714|                         # Route through the same global-focus-file gate as
   715|                         # every other tier -- a hard-coded high ceiling here
   716|                         # would let related-test expansion silently bypass
   717|                         # limits.max_files.
   718|                         if not max_files_note_emitted:
   719|                             budget.omissions.append(
   720|                                 f"Related test(s) for `{rel}` beyond limits.max_files ({req.max_files}) omitted."
   721|                             )
   722|                             max_files_note_emitted = True
   723|                         continue
   724|                     out.append(line); budget.spend(line, 1)
   725| 
   726|     if req.include_graphify and communities_by_file:
   727|         my_communities = communities_by_file.get(rel, [])
   728|         if my_communities:
   729|             comm_ids = {cid for cid, _ in my_communities}
   730|             peers = sorted(
   731|                 f for f, comms in communities_by_file.items()
   732|                 if f != rel and any(cid in comm_ids for cid, _ in comms)
   733|             )[:10]
   734|             if peers:
   735|                 header = f"\nGraphify community peers of `{rel}` ({rc_graphify.format_communities(my_communities)}):\n"
   736|                 if budget.allow(header, 1):
   737|                     out.append(header); budget.spend(header, 1)
   738|                     max_files_note_emitted = False
   739|                     for p in peers:
   740|                         line = f"- `{p}` [origin: graphify_expansion]\n"
   741|                         if not budget.allow(line, 1):
   742|                             budget.omissions.append(
   743|                                 f"More Graphify community peers of `{rel}` omitted (packet size limit reached)."
   744|                             )
   745|                             break
   746|                         if not note_focus_file(p):
   747|                             # Route through the same global focus-file gate
   748|                             # as every other expansion tier -- otherwise a
   749|                             # Graphify peer could silently exceed
   750|                             # limits.max_files while the resolution
   751|                             # sidecar's focus_files list stayed under it.
   752|                             if not max_files_note_emitted:
   753|                                 budget.omissions.append(
   754|                                     f"Graphify community peer(s) of `{rel}` beyond limits.max_files "
   755|                                     f"({req.max_files}) omitted."
   756|                                 )
   757|                                 max_files_note_emitted = True
   758|                             continue
   759|                         out.append(line); budget.spend(line, 1)
   760|                 else:
   761|                     budget.omissions.append(
   762|                         f"Graphify community peers listing for `{rel}` omitted entirely (packet size limit reached)."
   763|                     )
   764| 
   765| 
```
