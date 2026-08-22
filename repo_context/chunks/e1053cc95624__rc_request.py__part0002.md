# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 2 of 3
- Original line range: 489-637
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _symbol_expansion
- Source SHA-256: 358ab4a29afbd578ab28fbd6572c753c17a6c386fa593b10e9bd256e75df92a2
- Starts inside symbol: no
- Ends inside symbol: no

```
   489| def _symbol_expansion(row: dict, calls_rows: list, imports_rows: list, files_by_path: dict,
   490|                        req: ResolvedRequest, budget: Budget, out: list, note_focus_file,
   491|                        communities_by_file: Optional[dict] = None) -> None:
   492|     rel, qn = row["relative_path"], row["qualified_name"]
   493| 
   494|     if req.include_callers:
   495|         callers = [c for c in _bfs_callers(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
   496|         if callers:
   497|             header = f"\nCallers of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
   498|             if budget.allow(header, 1):
   499|                 out.append(header); budget.spend(header, 1)
   500|                 max_files_note_emitted = False
   501|                 for c in callers:
   502|                     line = (f"- `{c['caller_symbol']}` in `{c['caller_file']}`:{c['line']} "
   503|                             f"— `{c['call_expression']}` ({c['confidence']}: {c['explanation']}) "
   504|                             f"[origin: caller_expansion]")
   505|                     # Budget-check before reserving a focus-file slot -- a
   506|                     # caller entry that ultimately doesn't fit must not
   507|                     # consume the slot on behalf of content that was never
   508|                     # actually rendered.
   509|                     if not budget.allow(line, 1):
   510|                         budget.omissions.append(f"More callers of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
   511|                         break
   512|                     if not note_focus_file(c["caller_file"]):
   513|                         # A file beyond limits.max_files doesn't mean every
   514|                         # *later* caller is unreachable too -- a later one
   515|                         # may be in a file already in focus_files, which
   516|                         # note_focus_file accepts for free. Skip this entry
   517|                         # and keep checking the rest instead of abandoning
   518|                         # the whole listing.
   519|                         if not max_files_note_emitted:
   520|                             budget.omissions.append(
   521|                                 f"Caller(s) of `{qn}` beyond limits.max_files ({req.max_files}) omitted."
   522|                             )
   523|                             max_files_note_emitted = True
   524|                         continue
   525|                     out.append(line); budget.spend(line, 1)
   526|             else:
   527|                 budget.omissions.append(f"Callers listing for `{qn}` omitted entirely (packet size limit reached).")
   528| 
   529|     if req.include_callees:
   530|         callees = [c for c in _bfs_callees(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
   531|         if callees:
   532|             header = f"\nCallees of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
   533|             if budget.allow(header, 1):
   534|                 out.append(header); budget.spend(header, 1)
   535|                 max_files_note_emitted = False
   536|                 for c in callees:
   537|                     line = (f"- `{c['call_expression']}` at line {c['line']} -> `{c['candidate_symbol']}` "
   538|                             f"in `{c['candidate_file']}` ({c['confidence']}: {c['explanation']}) "
   539|                             f"[origin: callee_expansion]")
   540|                     if not budget.allow(line, 1):
   541|                         budget.omissions.append(f"More callees of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
   542|                         break
   543|                     if not note_focus_file(c["candidate_file"]):
   544|                         if not max_files_note_emitted:
   545|                             budget.omissions.append(
   546|                                 f"Callee(s) of `{qn}` beyond limits.max_files ({req.max_files}) omitted."
   547|                             )
   548|                             max_files_note_emitted = True
   549|                         continue
   550|                     out.append(line); budget.spend(line, 1)
   551|             else:
   552|                 budget.omissions.append(f"Callees listing for `{qn}` omitted entirely (packet size limit reached).")
   553| 
   554|     if req.include_imports:
   555|         file_imports = [i for i in imports_rows if i["source_file"] == rel and i["resolved_file"]]
   556|         if file_imports:
   557|             header = f"\nInternal imports of `{rel}` (import_expansion):\n"
   558|             if budget.allow(header, 1):
   559|                 out.append(header); budget.spend(header, 1)
   560|                 max_files_note_emitted = False
   561|                 for i in file_imports[:20]:
   562|                     line = f"- line {i['line']}: `{i['imported_name'] or i['imported_module']}` -> `{i['resolved_file']}`"
   563|                     if not budget.allow(line, 1):
   564|                         break
   565|                     if not note_focus_file(i["resolved_file"]):
   566|                         if not max_files_note_emitted:
   567|                             budget.omissions.append(
   568|                                 f"Import(s) of `{rel}` beyond limits.max_files ({req.max_files}) omitted."
   569|                             )
   570|                             max_files_note_emitted = True
   571|                         continue
   572|                     out.append(line); budget.spend(line, 1)
   573| 
   574|     if req.include_related_tests:
   575|         tests = _candidate_tests_for_file(rel, imports_rows, calls_rows, files_by_path)
   576|         if tests:
   577|             header = f"\nRelated tests for `{rel}` (related_test_expansion):\n"
   578|             if budget.allow(header, 1):
   579|                 out.append(header); budget.spend(header, 1)
   580|                 max_files_note_emitted = False
   581|                 for t in tests:
   582|                     line = f"- `{t}`"
   583|                     if not budget.allow(line, 1):
   584|                         break
   585|                     if not note_focus_file(t):
   586|                         # Route through the same global-focus-file gate as
   587|                         # every other tier -- a hard-coded high ceiling here
   588|                         # would let related-test expansion silently bypass
   589|                         # limits.max_files.
   590|                         if not max_files_note_emitted:
   591|                             budget.omissions.append(
   592|                                 f"Related test(s) for `{rel}` beyond limits.max_files ({req.max_files}) omitted."
   593|                             )
   594|                             max_files_note_emitted = True
   595|                         continue
   596|                     out.append(line); budget.spend(line, 1)
   597| 
   598|     if req.include_graphify and communities_by_file:
   599|         my_communities = communities_by_file.get(rel, [])
   600|         if my_communities:
   601|             comm_ids = {cid for cid, _ in my_communities}
   602|             peers = sorted(
   603|                 f for f, comms in communities_by_file.items()
   604|                 if f != rel and any(cid in comm_ids for cid, _ in comms)
   605|             )[:10]
   606|             if peers:
   607|                 header = f"\nGraphify community peers of `{rel}` ({rc_graphify.format_communities(my_communities)}):\n"
   608|                 if budget.allow(header, 1):
   609|                     out.append(header); budget.spend(header, 1)
   610|                     max_files_note_emitted = False
   611|                     for p in peers:
   612|                         line = f"- `{p}` [origin: graphify_expansion]"
   613|                         if not budget.allow(line, 1):
   614|                             budget.omissions.append(
   615|                                 f"More Graphify community peers of `{rel}` omitted (packet size limit reached)."
   616|                             )
   617|                             break
   618|                         if not note_focus_file(p):
   619|                             # Route through the same global focus-file gate
   620|                             # as every other expansion tier -- otherwise a
   621|                             # Graphify peer could silently exceed
   622|                             # limits.max_files while the resolution
   623|                             # sidecar's focus_files list stayed under it.
   624|                             if not max_files_note_emitted:
   625|                                 budget.omissions.append(
   626|                                     f"Graphify community peer(s) of `{rel}` beyond limits.max_files "
   627|                                     f"({req.max_files}) omitted."
   628|                                 )
   629|                                 max_files_note_emitted = True
   630|                             continue
   631|                         out.append(line); budget.spend(line, 1)
   632|                 else:
   633|                     budget.omissions.append(
   634|                         f"Graphify community peers listing for `{rel}` omitted entirely (packet size limit reached)."
   635|                     )
   636| 
   637| 
```
