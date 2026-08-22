# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 2 of 3
- Original line range: 517-715
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _render_excerpt_block, _symbol_expansion, _file_expansion
- Source SHA-256: 523991ef4ebd1a10bc2dd1b35f7956c36be078362fffaf1203e4cefd40337091
- Starts inside symbol: no
- Ends inside symbol: no

```
   517| def _render_excerpt_block(root: Path, rel_path: str, start: int, end: int, budget: Budget, out: list,
   518|                            expected_sha256: str) -> str:
   519|     """Returns "rendered", "stale" (withheld -- source changed since scan),
   520|     "unavailable" (file missing/unreadable), or "too_large" (would not fit
   521|     within the remaining budget). Only "too_large" is a hard, must-not-be-
   522|     silently-dropped conflict for an *explicit* selector -- "stale" and
   523|     "unavailable" are reported as ordinary (non-fatal) omissions, matching
   524|     the direct --file/--symbol/--line packet path's existing behavior."""
   525|     if not _file_is_fresh(root, rel_path, expected_sha256):
   526|         msg = (f"_Source excerpt withheld: `{rel_path}` has changed on disk since the last `scan` "
   527|                f"(SHA-256 mismatch); re-run `scan` for an up-to-date packet._\n")
   528|         if budget.allow(msg, 1):
   529|             out.append(msg)
   530|             budget.spend(msg, 1)
   531|         budget.omissions.append(f"`{rel_path}` changed since the last scan; excerpt withheld. Re-run scan.")
   532|         return "stale"
   533|     excerpt = _safe_excerpt(root, rel_path, start, end)
   534|     if excerpt is None:
   535|         out.append("_Source excerpt unavailable (file missing or unreadable)._\n")
   536|         budget.omissions.append(f"`{rel_path}` excerpt unavailable (file missing or unreadable).")
   537|         return "unavailable"
   538|     body_lines = [f"{ln:>6}| {text}" for ln, text in excerpt]
   539|     # Redact *before* the budget check, not after -- redact_secrets()
   540|     # replaces a matched secret with a placeholder that can be longer
   541|     # than the original text, so checking budget.allow() against the raw
   542|     # body and only redacting afterward let the actually-written content
   543|     # end up bigger than what was verified to fit.
   544|     body = redact_secrets("\n".join(body_lines))
   545|     if not budget.allow(body, len(body_lines)):
   546|         return "too_large"
   547|     out.append("```\n" + body + "\n```\n")
   548|     budget.spend(body, len(body_lines))
   549|     return "rendered"
   550| 
   551| 
   552| def _symbol_expansion(row: dict, calls_rows: list, req: ResolvedRequest, budget: Budget, out: list,
   553|                        note_focus_file) -> None:
   554|     """Callers/callees are inherently per-symbol (each symbol has its own
   555|     call graph neighborhood), unlike imports/related-tests/Graphify peers
   556|     below in _file_expansion(), which describe the containing file and
   557|     must not be repeated once per symbol in it."""
   558|     rel, qn = row["relative_path"], row["qualified_name"]
   559| 
   560|     if req.include_callers:
   561|         callers = [c for c in _bfs_callers(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
   562|         if callers:
   563|             header = f"\nCallers of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
   564|             if budget.allow(header, 1):
   565|                 out.append(header); budget.spend(header, 1)
   566|                 max_files_note_emitted = False
   567|                 for c in callers:
   568|                     line = (f"- `{c['caller_symbol']}` in `{c['caller_file']}`:{c['line']} "
   569|                             f"— `{c['call_expression']}` ({c['confidence']}: {c['explanation']}) "
   570|                             f"[origin: caller_expansion]")
   571|                     # Budget-check before reserving a focus-file slot -- a
   572|                     # caller entry that ultimately doesn't fit must not
   573|                     # consume the slot on behalf of content that was never
   574|                     # actually rendered.
   575|                     if not budget.allow(line, 1):
   576|                         budget.omissions.append(f"More callers of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
   577|                         break
   578|                     if not note_focus_file(c["caller_file"]):
   579|                         # A file beyond limits.max_files doesn't mean every
   580|                         # *later* caller is unreachable too -- a later one
   581|                         # may be in a file already in focus_files, which
   582|                         # note_focus_file accepts for free. Skip this entry
   583|                         # and keep checking the rest instead of abandoning
   584|                         # the whole listing.
   585|                         if not max_files_note_emitted:
   586|                             budget.omissions.append(
   587|                                 f"Caller(s) of `{qn}` beyond limits.max_files ({req.max_files}) omitted."
   588|                             )
   589|                             max_files_note_emitted = True
   590|                         continue
   591|                     out.append(line); budget.spend(line, 1)
   592|             else:
   593|                 budget.omissions.append(f"Callers listing for `{qn}` omitted entirely (packet size limit reached).")
   594| 
   595|     if req.include_callees:
   596|         callees = [c for c in _bfs_callees(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
   597|         if callees:
   598|             header = f"\nCallees of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
   599|             if budget.allow(header, 1):
   600|                 out.append(header); budget.spend(header, 1)
   601|                 max_files_note_emitted = False
   602|                 for c in callees:
   603|                     line = (f"- `{c['call_expression']}` at line {c['line']} -> `{c['candidate_symbol']}` "
   604|                             f"in `{c['candidate_file']}` ({c['confidence']}: {c['explanation']}) "
   605|                             f"[origin: callee_expansion]")
   606|                     if not budget.allow(line, 1):
   607|                         budget.omissions.append(f"More callees of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
   608|                         break
   609|                     if not note_focus_file(c["candidate_file"]):
   610|                         if not max_files_note_emitted:
   611|                             budget.omissions.append(
   612|                                 f"Callee(s) of `{qn}` beyond limits.max_files ({req.max_files}) omitted."
   613|                             )
   614|                             max_files_note_emitted = True
   615|                         continue
   616|                     out.append(line); budget.spend(line, 1)
   617|             else:
   618|                 budget.omissions.append(f"Callees listing for `{qn}` omitted entirely (packet size limit reached).")
   619| 
   620| 
   621| def _file_expansion(rel: str, imports_rows: list, calls_rows: list, files_by_path: dict,
   622|                      req: ResolvedRequest, budget: Budget, out: list, note_focus_file,
   623|                      communities_by_file: Optional[dict] = None) -> None:
   624|     """Internal imports, related tests, and Graphify community peers all
   625|     describe the *file*, not any one symbol in it -- unlike
   626|     callers/callees above, which are inherently per-symbol. Must be
   627|     called at most once per file regardless of how many of that file's
   628|     symbols are being expanded (see the caller: it used to call this
   629|     content once per top-level symbol, rendering identical "Internal
   630|     imports of X"/"Related tests for X"/"Graphify community peers of X"
   631|     sections over and over for a multi-symbol file)."""
   632|     if req.include_imports:
   633|         file_imports = [i for i in imports_rows if i["source_file"] == rel and i["resolved_file"]]
   634|         if file_imports:
   635|             header = f"\nInternal imports of `{rel}` (import_expansion):\n"
   636|             if budget.allow(header, 1):
   637|                 out.append(header); budget.spend(header, 1)
   638|                 max_files_note_emitted = False
   639|                 for i in file_imports[:20]:
   640|                     line = f"- line {i['line']}: `{i['imported_name'] or i['imported_module']}` -> `{i['resolved_file']}`"
   641|                     if not budget.allow(line, 1):
   642|                         break
   643|                     if not note_focus_file(i["resolved_file"]):
   644|                         if not max_files_note_emitted:
   645|                             budget.omissions.append(
   646|                                 f"Import(s) of `{rel}` beyond limits.max_files ({req.max_files}) omitted."
   647|                             )
   648|                             max_files_note_emitted = True
   649|                         continue
   650|                     out.append(line); budget.spend(line, 1)
   651| 
   652|     if req.include_related_tests:
   653|         tests = _candidate_tests_for_file(rel, imports_rows, calls_rows, files_by_path)
   654|         if tests:
   655|             header = f"\nRelated tests for `{rel}` (related_test_expansion):\n"
   656|             if budget.allow(header, 1):
   657|                 out.append(header); budget.spend(header, 1)
   658|                 max_files_note_emitted = False
   659|                 for t in tests:
   660|                     line = f"- `{t}`"
   661|                     if not budget.allow(line, 1):
   662|                         break
   663|                     if not note_focus_file(t):
   664|                         # Route through the same global-focus-file gate as
   665|                         # every other tier -- a hard-coded high ceiling here
   666|                         # would let related-test expansion silently bypass
   667|                         # limits.max_files.
   668|                         if not max_files_note_emitted:
   669|                             budget.omissions.append(
   670|                                 f"Related test(s) for `{rel}` beyond limits.max_files ({req.max_files}) omitted."
   671|                             )
   672|                             max_files_note_emitted = True
   673|                         continue
   674|                     out.append(line); budget.spend(line, 1)
   675| 
   676|     if req.include_graphify and communities_by_file:
   677|         my_communities = communities_by_file.get(rel, [])
   678|         if my_communities:
   679|             comm_ids = {cid for cid, _ in my_communities}
   680|             peers = sorted(
   681|                 f for f, comms in communities_by_file.items()
   682|                 if f != rel and any(cid in comm_ids for cid, _ in comms)
   683|             )[:10]
   684|             if peers:
   685|                 header = f"\nGraphify community peers of `{rel}` ({rc_graphify.format_communities(my_communities)}):\n"
   686|                 if budget.allow(header, 1):
   687|                     out.append(header); budget.spend(header, 1)
   688|                     max_files_note_emitted = False
   689|                     for p in peers:
   690|                         line = f"- `{p}` [origin: graphify_expansion]"
   691|                         if not budget.allow(line, 1):
   692|                             budget.omissions.append(
   693|                                 f"More Graphify community peers of `{rel}` omitted (packet size limit reached)."
   694|                             )
   695|                             break
   696|                         if not note_focus_file(p):
   697|                             # Route through the same global focus-file gate
   698|                             # as every other expansion tier -- otherwise a
   699|                             # Graphify peer could silently exceed
   700|                             # limits.max_files while the resolution
   701|                             # sidecar's focus_files list stayed under it.
   702|                             if not max_files_note_emitted:
   703|                                 budget.omissions.append(
   704|                                     f"Graphify community peer(s) of `{rel}` beyond limits.max_files "
   705|                                     f"({req.max_files}) omitted."
   706|                                 )
   707|                                 max_files_note_emitted = True
   708|                             continue
   709|                         out.append(line); budget.spend(line, 1)
   710|                 else:
   711|                     budget.omissions.append(
   712|                         f"Graphify community peers listing for `{rel}` omitted entirely (packet size limit reached)."
   713|                     )
   714| 
   715| 
```
